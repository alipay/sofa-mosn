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

package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"mosn.io/api"
	"mosn.io/mosn/pkg/admin/store"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/mtls"
	"mosn.io/mosn/pkg/types"
	"mosn.io/pkg/buffer"
	"mosn.io/pkg/utils"
)

const (
	transferErr    = 0
	transferNotify = 1
)

// TransferTimeout is the total transfer time
var TransferTimeout = time.Second * 30 //default 30s

func SetTransferTimeout(time time.Duration) {
	if time != 0 {
		TransferTimeout = time
	}
}

// TransferServer is called on new mosn start
func TransferServer(handler types.ConnectionHandler) {
	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Errorf("[network] [transfer] [server] transferServer panic %v", r)
		}
	}()

	defer store.SetMosnState(store.Running)

	syscall.Unlink(types.TransferConnDomainSocket)
	l, err := net.Listen("unix", types.TransferConnDomainSocket)
	if err != nil {
		log.DefaultLogger.Errorf("[network] [transfer] [server] transfer net listen error %v", err)
		return
	}
	defer l.Close()

	log.DefaultLogger.Infof("[network] [transfer] [server] TransferServer start")

	var transferMap sync.Map

	utils.GoWithRecover(func() {
		for {
			c, err := l.Accept()
			if err != nil {
				if ope, ok := err.(*net.OpError); ok && (ope.Op == "accept") {
					log.DefaultLogger.Infof("[network] [transfer] [server] TransferServer listener %s closed", l.Addr())
				} else {
					log.DefaultLogger.Errorf("[network] [transfer] [server] TransferServer Accept error :%v", err)
				}
				return
			}
			log.DefaultLogger.Infof("[network] [transfer] [server] transfer Accept")
			go transferHandler(c, handler, &transferMap)

		}
	}, nil)

	<-time.After(2*TransferTimeout + 2*buffer.ConnReadTimeout + 10*time.Second)
	log.DefaultLogger.Infof("[network] [transfer] [server] TransferServer exit")
}

// transferHandler is called on recv transfer request
func transferHandler(c net.Conn, handler types.ConnectionHandler, transferMap *sync.Map) {
	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Errorf("[network] [transfer] [handler] transferHandler panic %v", r)
		}
	}()

	defer c.Close()

	uc, ok := c.(*net.UnixConn)
	if !ok {
		log.DefaultLogger.Errorf("[network] [transfer] [handler] unexpected FileConn type; expected UnixConn, got %T", c)
		return
	}
	// recv type
	conn, err := transferRecvType(uc)
	if err != nil {
		log.DefaultLogger.Errorf("[network] [transfer] [handler] transferRecvType error :%v", err)
		return
	}

	if conn != nil {
		// transfer read
		// recv header + buffer
		dataBuf, tlsBuf, err := transferReadRecvData(uc)
		if err != nil {
			log.DefaultLogger.Errorf("[network] [transfer] [handler] transferRecvData error :%v", err)
			return
		}
		connection := transferNewConn(conn, dataBuf, tlsBuf, handler, transferMap)
		if connection != nil {
			transferSendID(uc, connection.id)
		} else {
			transferSendID(uc, transferErr)
		}
	} else {
		// transfer write
		// recv header + buffer
		id, buf, err := transferWriteRecvData(uc)
		if err != nil {
			log.DefaultLogger.Errorf("[network] [transfer] [handler] transferRecvData error :%v", err)
		}
		connection := transferFindConnection(transferMap, uint64(id))
		if connection == nil {
			log.DefaultLogger.Errorf("[network] [transfer] [handler] transferFindConnection failed, id = %d", id)
			return
		}
		err = transferWriteBuffer(connection, buf)
		if err != nil {
			log.DefaultLogger.Errorf("[network] [transfer] [handler] transferWriteBuffer error :%v", err)
			return
		}
	}
}

// old mosn transfer readloop
func transferRead(c *connection) (uint64, error) {
	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Errorf("[network] [transfer] [read] panic %v\n%s", r, string(debug.Stack()))
		}
	}()
	unixConn, err := net.Dial("unix", types.TransferConnDomainSocket)
	if err != nil {
		log.DefaultLogger.Errorf("[network] [transfer] [read] net Dial unix failed c:%p, id:%d, err:%v", c, c.id, err)
		return transferErr, err
	}
	defer unixConn.Close()

	file, tlsConn, err := transferGetFile(c)
	if err != nil {
		log.DefaultLogger.Errorf("[network] [transfer] [read] transferRead failed: %v", err)
		return transferErr, err
	}

	uc := unixConn.(*net.UnixConn)
	// send type and TCP FD
	err = transferSendType(uc, file)
	if err != nil {
		log.DefaultLogger.Errorf("[network] [transfer] [read] transferRead failed: %v", err)
		return transferErr, err
	}
	// send header + buffer + TLS
	err = transferReadSendData(uc, tlsConn, c.readBuffer)
	if err != nil {
		log.DefaultLogger.Errorf("[network] [transfer] [read] transferRead failed: %v", err)
		return transferErr, err
	}
	// recv ID
	id := transferRecvID(uc)
	log.DefaultLogger.Infof("[network] [transfer] [read] TransferRead NewConn Id = %d, oldId = %d, %p, addrass = %s", id, c.id, c, c.RemoteAddr().String())

	return id, nil
}

// old mosn transfer writeloop
func transferWrite(c *connection, id uint64) error {
	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Errorf("[network] [transfer] [write] transferWrite panic %v", r)
		}
	}()
	unixConn, err := net.Dial("unix", types.TransferConnDomainSocket)
	if err != nil {
		log.DefaultLogger.Errorf("[network] [transfer] [write] net Dial unix failed %v", err)
		return err
	}
	defer unixConn.Close()

	log.DefaultLogger.Infof("[network] [transfer] [write] TransferWrite id = %d, dataBuf = %d", id, c.writeBufLen())
	uc := unixConn.(*net.UnixConn)
	err = transferSendType(uc, nil)
	if err != nil {
		log.DefaultLogger.Errorf("[network] [transfer] [write] transferWrite failed: %v", err)
		return err
	}
	// build net.Buffers to IoBuffer
	buf := transferBuildIoBuffer(c)
	// send header + buffer
	err = transferWriteSendData(uc, int(id), buf)
	if err != nil {
		log.DefaultLogger.Errorf("[network] [transfer] [write] transferWrite failed: %v", err)
		return err
	}
	return nil
}

func transferGetFile(c *connection) (file *os.File, tlsConn *mtls.TLSConn, err error) {
	switch conn := c.rawConnection.(type) {
	case *net.TCPConn:
		file, err = conn.File()
		if err != nil {
			return nil, nil, fmt.Errorf("TCP File failed %v", err)
		}
	case *net.UnixConn:
		file, err = conn.File()
		if err != nil {
			return nil, nil, fmt.Errorf("Unix File failed %v", err)
		}
	case *mtls.Conn:
		mtlsConn, ok := conn.Conn.(*net.TCPConn)
		if !ok {
			return nil, nil, errors.New("unexpected Conn type")
		}
		file, err = mtlsConn.File()
		if err != nil {
			return nil, nil, fmt.Errorf("mtls.Conn File failed %v", err)
		}
	case *mtls.TLSConn:
		tlsConn = conn
		netConn := conn.GetRawConn()
		switch conn := netConn.(type) {
		case *mtls.Conn:
			mtlsConn, ok := conn.Conn.(*net.TCPConn)
			if !ok {
				return nil, nil, errors.New("unexpected Conn type")
			}
			file, err = mtlsConn.File()
			if err != nil {
				return nil, nil, fmt.Errorf("mtls.Conn File failed %v", err)
			}
		case *net.TCPConn:
			file, err = conn.File()
			if err != nil {
				return nil, nil, fmt.Errorf("TCPConn File failed %v", err)
			}
		default:
			return nil, nil, errors.New("unexpected Conn type")
		}
	default:
		return nil, nil, fmt.Errorf("unexpected net.Conn type; expected TCPConn or mtls.TLSConn, got %T", conn)
	}
	return
}

func transferBuildIoBuffer(c *connection) types.IoBuffer {
	buf := buffer.GetIoBuffer(c.writeBufLen())
	for _, b := range c.writeBuffers {
		buf.Write(b)
	}
	c.writeBuffers = c.writeBuffers[:0]
	c.ioBuffers = c.ioBuffers[:0]
	return buf
}

func transferWriteBuffer(conn *connection, buf []byte) error {
	iobuf := buffer.NewIoBufferBytes(buf)
	return conn.Write(iobuf)
}

func transferFindConnection(transferMap *sync.Map, id uint64) *connection {
	conn, ok := transferMap.Load(id)
	if !ok {
		return nil
	}
	return conn.(*connection)
}

/**
 *  transfer read protocol
 *  header (8 bytes) + (readBuffer data) + TLS
 *
 * 0                       4                       8
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 * |      data length      |     TLS length        |
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 * |                     data                      |
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 * |                     TLS                       |
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 *
*
 *  transfer write protocol
 *  header (8 bytes) + (writeBuffer data)
 *
 * 0                       4                       8
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 * |      data length      |    connection  ID     |
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 * |                     data                      |
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 *
**/
func transferBuildHead(s1 uint32, s2 uint32) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:], s1)
	binary.BigEndian.PutUint32(buf[4:], s2)
	return buf
}

func transferSendHead(uc *net.UnixConn, s1 uint32, s2 uint32) error {
	buf := transferBuildHead(s1, s2)
	return transferSendMsg(uc, buf)
}

/**
 * type (1 bytes)
 *  0 : transfer read and FD
 *  1 : transfer write
 **/
func transferSendType(uc *net.UnixConn, file *os.File) error {
	buf := make([]byte, 1)
	// transfer write
	if file == nil {
		buf[0] = 1
		return transferSendMsg(uc, buf)
	}
	// transfer read, send FD
	return transferSendFD(uc, file)
}

func transferSendFD(uc *net.UnixConn, file *os.File) error {
	buf := make([]byte, 1)
	// transfer read
	buf[0] = 0
	if file == nil {
		return errors.New("transferSendFD conn is nil")
	}
	defer file.Close()
	rights := syscall.UnixRights(int(file.Fd()))
	n, oobn, err := uc.WriteMsgUnix(buf, rights, nil)
	if err != nil {
		return fmt.Errorf("WriteMsgUnix: %v", err)
	}
	if n != len(buf) || oobn != len(rights) {
		return fmt.Errorf("WriteMsgUnix = %d, %d; want 1, %d", n, oobn, len(rights))
	}
	return nil
}

func transferRecvFD(oob []byte) (net.Conn, error) {
	scms, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, fmt.Errorf("ParseSocketControlMessage: %v", err)
	}
	if len(scms) != 1 {
		return nil, fmt.Errorf("expected 1 SocketControlMessage; got scms = %#v", scms)
	}
	scm := scms[0]
	gotFds, err := unix.ParseUnixRights(&scm)
	if err != nil {
		return nil, fmt.Errorf("unix.ParseUnixRights: %v", err)
	}
	if len(gotFds) != 1 {
		return nil, fmt.Errorf("wanted 1 fd; got %#v", gotFds)
	}
	f := os.NewFile(uintptr(gotFds[0]), "fd-from-old")
	defer f.Close()
	conn, err := net.FileConn(f)
	if err != nil {
		return nil, fmt.Errorf("FileConn error :%v", gotFds)
	}
	return conn, nil
}

func transferRecvType(uc *net.UnixConn) (net.Conn, error) {
	buf := make([]byte, 1)
	oob := make([]byte, 32)
	_, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, fmt.Errorf("ReadMsgUnix error: %v", err)
	}
	// transfer write
	if buf[0] == 1 {
		return nil, nil
	}
	// transfer read, recv FD
	conn, err := transferRecvFD(oob[0:oobn])
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func transferReadSendData(uc *net.UnixConn, c *mtls.TLSConn, buf buffer.IoBuffer) error {
	// send header
	s1 := buf.Len()
	s2 := c.GetTLSInfo(buf)
	err := transferSendHead(uc, uint32(s1), uint32(s2))
	if err != nil {
		return err
	}
	log.DefaultLogger.Infof("TransferRead dataBuf = %d, tlsBuf = %d", s1, s2)
	// send read/write buffer
	return transferSendIoBuffer(uc, buf)
}

func transferWriteSendData(uc *net.UnixConn, id int, buf types.IoBuffer) error {
	// send header
	err := transferSendHead(uc, uint32(buf.Len()), uint32(id))
	if err != nil {
		return err
	}
	// send read/write buffer
	return transferSendIoBuffer(uc, buf)
}

func transferReadRecvData(uc *net.UnixConn) ([]byte, []byte, error) {
	// recv header
	dataSize, tlsSize, err := transferRecvHead(uc)
	if err != nil {
		return nil, nil, err
	}
	// recv read buffer and TLS
	buf, err := transferRecvMsg(uc, dataSize+tlsSize)
	if err != nil {
		return nil, nil, err
	}

	return buf[0:dataSize], buf[dataSize:], nil
}

func transferWriteRecvData(uc *net.UnixConn) (int, []byte, error) {
	// recv header
	size, id, err := transferRecvHead(uc)
	if err != nil {
		return 0, nil, err
	}
	// recv write buffer
	buf, err := transferRecvMsg(uc, size)
	if err != nil {
		return 0, nil, err
	}

	return id, buf, nil
}

func transferRecvHead(uc *net.UnixConn) (int, int, error) {
	buf, err := transferRecvMsg(uc, 8)
	if err != nil {
		return 0, 0, fmt.Errorf("ReadMsgUnix error: %v", err)
	}
	size := int(binary.BigEndian.Uint32(buf[0:]))
	id := int(binary.BigEndian.Uint32(buf[4:]))
	return size, id, nil
}

func transferSendIoBuffer(uc *net.UnixConn, buf types.IoBuffer) error {
	if buf.Len() == 0 {
		return nil
	}
	_, err := buf.WriteTo(uc)
	if err != nil {
		return fmt.Errorf("transferSendIobuffer failed: %v", err)
	}
	return nil
}

func transferSendMsg(uc *net.UnixConn, b []byte) error {
	if len(b) == 0 {
		return nil
	}
	buf := buffer.NewIoBufferBytes(b)
	err := transferSendIoBuffer(uc, buf)
	if err != nil {
		return fmt.Errorf("transferSendMsg failed: %v", err)
	}
	return nil
}

func transferRecvMsg(uc *net.UnixConn, size int) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	b := make([]byte, size)
	var n, off int
	var err error
	for {
		n, err = uc.Read(b[off:])
		if err != nil {
			return nil, fmt.Errorf("transferRecvMsg failed: %v", err)
		}
		off += n
		if off == size {
			return b, nil
		}
	}
}

func transferSendID(uc *net.UnixConn, id uint64) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(id))
	return transferSendMsg(uc, b)
}

func transferRecvID(uc *net.UnixConn) uint64 {
	b, err := transferRecvMsg(uc, 4)
	if err != nil {
		return transferErr
	}
	return uint64(binary.BigEndian.Uint32(b))
}

func transferNewConn(conn net.Conn, dataBuf, tlsBuf []byte, handler types.ConnectionHandler, transferMap *sync.Map) *connection {

	listener := transferFindListen(conn.LocalAddr(), handler)
	if listener == nil {
		return nil
	}

	log.DefaultLogger.Infof("[network] [transfer] [new conn] transferNewConn dataBuf = %d, tlsBuf = %d", len(dataBuf), len(tlsBuf))

	var err error
	if len(tlsBuf) != 0 {
		conn, err = mtls.GetTLSConn(conn, tlsBuf)
		if err != nil {
			log.DefaultLogger.Errorf("[network] [transfer] [new conn] transferTLSConn error: %v", err)
			return nil
		}
	}

	ch := make(chan api.Connection, 1)
	// new connection
	utils.GoWithRecover(func() {
		listener.GetListenerCallbacks().OnAccept(conn, listener.UseOriginalDst(), nil, ch, dataBuf, nil)
	}, nil)

	select {
	// recv connection
	case rch := <-ch:
		conn, ok := rch.(*connection)
		if !ok {
			log.DefaultLogger.Errorf("[network] [transfer] [new conn] transfer NewConn failed %+v", conn.LocalAddr())
			return nil
		}
		log.DefaultLogger.Infof("[network] [transfer] [new conn] transfer NewConn id: %d", conn.id)
		transferMap.Store(conn.id, conn)
		return conn
	case <-time.After(3000 * time.Millisecond):
		log.DefaultLogger.Errorf("[network] [transfer] [new conn] transfer NewConn timeout, localAddress %+v, remoteAddress %+v", conn.LocalAddr(), conn.RemoteAddr())
		return nil
	}
}

func transferFindListen(addr net.Addr, handler types.ConnectionHandler) types.Listener {
	switch address := addr.(type) {
	case *net.TCPAddr:
		port := strconv.FormatInt(int64(address.Port), 10)
		ipv4, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:"+port)
		ipv6, _ := net.ResolveTCPAddr("tcp", "[::]:"+port)

		var listener types.Listener
		if listener = handler.FindListenerByAddress(address); listener != nil {
			return listener
		}
		if listener = handler.FindListenerByAddress(ipv4); listener != nil {
			return listener
		}
		if listener = handler.FindListenerByAddress(ipv6); listener != nil {
			return listener
		}
	case *net.UnixAddr:
		var listener types.Listener
		if listener = handler.FindListenerByAddress(address); listener != nil {
			return listener
		}
	default:
		log.DefaultLogger.Errorf("unexpected net.Addr type; expected TCPAddr or UnixAddr, got %T", address)
		return nil
	}

	log.DefaultLogger.Errorf("[network] [transfer] Find Listener failed %v", addr)
	return nil
}
