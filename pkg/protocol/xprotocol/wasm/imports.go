/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wasm

import (
	"encoding/binary"
	"runtime/debug"

	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/protocol/xprotocol"
	"mosn.io/mosn/pkg/types"
	"mosn.io/pkg/buffer"
	"mosn.io/proxy-wasm-go-host/proxywasm"
)

func proxySetBufferBytes(instance types.WasmInstance, bufferType int32, start int32, length int32, dataPtr int32, dataSize int32) int32 {

	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Errorf("[wasm protocol] %s panic %v\n%s", r, string(debug.Stack()))
		}
	}()

	bt := proxywasm.BufferType(bufferType)
	switch bt {
	case BufferTypeDecodeData:
		return proxySetDecodeCommand(instance, bufferType, start, length, dataPtr, dataSize)
	case BufferTypeEncodeData:
		return proxySetEncodeCommand(instance, bufferType, start, length, dataPtr, dataSize)
	default:
		return proxywasm.ProxySetBufferBytes(instance, bufferType, start, length, dataPtr, dataSize)
	}
}

func proxySetDecodeCommand(instance types.WasmInstance, bufferType int32, start int32, length int32, ptr int32, size int32) int32 {
	// buffer format:
	// encoded header map | Flag | Id | (Timeout|GetStatus) | drain length | raw bytes length | raw bytes
	content, err := instance.GetMemory(uint64(ptr), uint64(size))
	if err != nil {
		return proxywasm.WasmResultInvalidMemoryAccess.Int32()
	}

	headerBytes := binary.BigEndian.Uint32(content[0:4])
	headers := xprotocol.Header{}
	if headerBytes > 0 {
		xprotocol.DecodeHeader(content[4:], &headers)
	}

	flag := content[4+headerBytes]
	id := binary.BigEndian.Uint64(content[5+headerBytes:])

	cmdType := flag >> 6
	switch cmdType {
	case RequestType, RequestOneWayType:
		decodeWasmRequest(instance, bufferType, content, headerBytes, id, &headers, flag)
	case ResponseType:
		decodeWasmResponse(instance, bufferType, content, headerBytes, id, &headers, flag)
	default:
		log.DefaultLogger.Errorf("[wasm] failed to decode buffer, type = %s, value = %d", UnKnownRpcFlagType, flag)
		return proxywasm.WasmResultBadArgument.Int32()
	}

	return proxywasm.WasmResultOk.Int32()
}

func proxySetEncodeCommand(instance types.WasmInstance, bufferType int32, start int32, length int32, ptr int32, size int32) int32 {
	// buffer format:
	// encoded header map | Flag | Id | (Timeout|GetStatus) | drain length | raw bytes
	content, err := instance.GetMemory(uint64(ptr), uint64(size))
	if err != nil {
		return proxywasm.WasmResultInvalidMemoryAccess.Int32()
	}

	headerBytes := binary.BigEndian.Uint32(content[0:4])
	headers := xprotocol.Header{}
	if headerBytes > 0 {
		xprotocol.DecodeHeader(content[4:], &headers)
	}

	var (
		timeoutIndex = 13 + headerBytes
		drainIndex   = timeoutIndex + 4
		byteIndex    = drainIndex + 4
	)

	// encoded buffer length
	drainLen := binary.BigEndian.Uint32(content[drainIndex:])
	// command encode buffer
	payload := make([]byte, drainLen)
	// wasm shared linear memory cannot be used here,
	// otherwise it will be  modified by other data.
	copy(payload, content[byteIndex:byteIndex+drainLen])
	buf := buffer.NewIoBufferBytes(payload)
	//fmt.Fprintf(os.Stdout, "==>encode buf(%d): %v", buf.Len(), buf.Bytes())

	ctx := getInstanceCallback(instance)
	ctx.SetEncodeBuffer(buf)

	return proxywasm.WasmResultOk.Int32()
}

func decodeWasmRequest(instance types.WasmInstance, bufferType int32,
	content []byte, headerBytes uint32, id uint64,
	headers *xprotocol.Header, flag byte) {

	// buffer format:
	// encoded header map | Flag | Id | (Timeout|GetStatus) | drain length | raw bytes length | raw bytes

	var (
		timeoutIndex = 13 + headerBytes
		drainIndex   = timeoutIndex + 4
		rawIndex     = drainIndex + 4
		byteIndex    = rawIndex + 4
	)

	// decode wasm request timeout
	timeout := binary.BigEndian.Uint32(content[timeoutIndex:])
	// decode buffer should drain length
	drainLen := binary.BigEndian.Uint32(content[drainIndex:])
	// content byte length
	rawBytesLen := binary.BigEndian.Uint32(content[rawIndex:])

	// create proxy wasm request
	payload := make([]byte, rawBytesLen)
	// wasm shared linear memory cannot be used here,
	// otherwise it will be  modified by other data.
	copy(payload, content[byteIndex:byteIndex+rawBytesLen])
	req := NewWasmRequestWithId(uint32(id), headers, buffer.NewIoBufferBytes(payload))
	req.Timeout = timeout

	// check heartbeat command
	if flag&HeartBeatFlag != 0 {
		req.Flag = req.Flag | HeartBeatFlag
	}
	// check oneway request
	if flag&RpcOneWayRequestFlag == RpcOneWayRequestFlag {
		req.Flag = req.Flag | RpcOneWayRequestFlag
	}
	buf := GetBuffer(instance, proxywasm.BufferType(bufferType))
	// if data without change, direct encode forward
	req.Data = buffer.GetIoBuffer(int(drainLen))
	req.Data.Write(buf.Bytes()[:drainLen])

	//fmt.Fprintf(os.Stdout, "==>decode buf(%d): %v", req.Data.Len(), req.Data.Bytes())

	ctx := getInstanceCallback(instance)
	// we need to drain decode buffer
	if drainLen > 0 {
		ctx.GetDecodeBuffer().Drain(int(drainLen))
	}
	ctx.SetDecodeCmd(req)
}

func decodeWasmResponse(instance types.WasmInstance, bufferType int32,
	content []byte, headerBytes uint32, id uint64,
	headers *xprotocol.Header, flag byte) {
	// buffer format:
	// encoded header map | Flag | Id | (Timeout|GetStatus) | drain length | raw bytes length | raw bytes

	var (
		timeoutIndex = 13 + headerBytes
		drainIndex   = timeoutIndex + 4
		rawIndex     = drainIndex + 4
		byteIndex    = rawIndex + 4
	)

	// decode wasm response status
	status := binary.BigEndian.Uint32(content[timeoutIndex:])
	// decode buffer should drain length
	drainLen := binary.BigEndian.Uint32(content[drainIndex:])
	// content byte length
	rawBytesLen := binary.BigEndian.Uint32(content[rawIndex:])
	payload := make([]byte, rawBytesLen)
	// wasm shared linear memory cannot be used here,
	// otherwise it will be  modified by other data.
	copy(payload, content[byteIndex:byteIndex+rawBytesLen])
	resp := NewWasmResponseWithId(uint32(id), headers, buffer.NewIoBufferBytes(payload))
	resp.Status = status

	// check heartbeat command
	if flag&HeartBeatFlag != 0 {
		resp.Flag = resp.Flag | HeartBeatFlag
	}
	buf := GetBuffer(instance, proxywasm.BufferType(bufferType))
	// if data without change, direct encode forward
	resp.Data = buffer.GetIoBuffer(int(drainLen))
	resp.Data.Write(buf.Bytes()[:drainLen])

	ctx := getInstanceCallback(instance)
	// we need to drain decode buffer
	if drainLen > 0 {
		ctx.GetDecodeBuffer().Drain(int(drainLen))
	}
	ctx.SetDecodeCmd(resp)
}

func GetBuffer(instance types.WasmInstance, bufferType proxywasm.BufferType) buffer.IoBuffer {
	callback := getInstanceCallback(instance)
	switch bufferType {
	case BufferTypeDecodeData:
		return callback.GetDecodeBuffer()
	}
	return nil
}

func getInstanceCallback(instance types.WasmInstance) ContextCallback {
	v := instance.GetData()
	if v == nil {
		return &Context{}
	}

	cb, ok := v.(types.ABI)
	if !ok {
		log.DefaultLogger.Errorf("[proxywasm_0_2_0][imports] getInstanceCallback return type is not *AbiContext")
		return &Context{}
	}

	imports := cb.GetABIImports()
	if imports == nil {
		log.DefaultLogger.Errorf("[proxywasm_0_2_0][imports] getInstanceCallback imports not set")
		return &Context{}
	}

	if ctx, ok := imports.(ContextCallback); ok {
		return ctx
	}

	return &Context{}
}
