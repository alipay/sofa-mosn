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
	v2 "mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/types"
	v1 "mosn.io/mosn/pkg/wasm/abi/proxywasm_0_1_0"
)

const (
	ResponseType              = 0
	RequestType               = 1
	RequestOneWayType         = 2
	HeartBeatFlag        byte = 1 << 5
	RpcHeaderLength           = 16
	RpcMagic                  = 0xAF
	RpcVersion                = 1
	RpcRequestFlag       byte = 1 << 6
	RpcOneWayRequestFlag byte = 1<<6 | 1<<7
	RpcResponseFlag      byte = 0
	RpcIdIndex                = 4
	ProtocolName              = "wasm"
	RpcTimeout                = "timeout"
	RpcResponseStatus         = 0
	UnKnownMagicType          = "unknown magic type"
	UnKnownProtocolType       = "unknown protocol code type"
	UnKnownRpcFlagType        = "unknown protocol flag type"
)

type ProtocolConfig struct {
	FromWasmPlugin string                 `json:"from_wasm_plugin,omitempty"`
	VmConfig       *v2.WasmVmConfig       `json:"vm_config,omitempty"`
	InstanceNum    int                    `json:"instance_num,omitempty"`
	RootContextID  int32                  `json:"root_context_id,omitempty"`
	ExtendConfig   *v2.XProxyExtendConfig `json:"extend_config,omitempty"`
	// protocol feature field
	PoolMode          string `json:"pool_mode,omitempty"`
	DisableWorkerPool bool   `json:"disable_worker_pool,omitempty"`
	PluginGenerateID  bool   `json:"plugin_generate_id,omitempty"`
	poolMode          types.PoolMode
}

// extension for protocol
const (
	BufferTypeDecodeData v1.BufferType = 13
	BufferTypeEncodeData v1.BufferType = 14
	//
	StatusNeedMoreData v1.WasmResult = 99
)