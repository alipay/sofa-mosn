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
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rcrowley/go-metrics"
	"mosn.io/api"
	mosnctx "mosn.io/mosn/pkg/context"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/mtls"
	"mosn.io/mosn/pkg/types"
	"mosn.io/pkg/buffer"
	"mosn.io/pkg/utils"
)

// Network related const
const (
	DefaultBufferReadCapacity = 1 << 7

	NetBufferDefaultSize     = 0
	NetBufferDefaultCapacity = 1 << 4

	DefaultConnectTimeout = 10 * time.Second
)

var idCounter uint64 = 1

type connection struct {
	id         uint64
	file       *os.File //copy of origin connection fd
	localAddr  net.Addr
	remoteAddr net.Addr

	nextProtocol         string
	noDelay              bool
	readEnabled          bool
	readEnabledChan      chan bool
	readDisableCount     int
	localAddressRestored bool
	bufferLimit          uint32 // todo: support soft buffer limit
	rawConnection        net.Conn
	tlsMng               types.TLSClientContextManager
	closeWithFlush       bool
	connCallbacks        []api.ConnectionEventListener
	bytesReadCallbacks   []func(bytesRead uint64)
	bytesSendCallbacks   []func(bytesSent uint64)
	transferCallbacks    func() bool
	filterManager        api.FilterManager
	idleEventListener    api.ConnectionEventListener
	network              string

	stopChan           chan struct{}
	curWriteBufferData []buffer.IoBuffer
	readBuffer         buffer.IoBuffer
	writeBuffers       net.Buffers
	ioBuffers          []buffer.IoBuffer
	writeBufferChan    chan *[]buffer.IoBuffer
	transferChan       chan uint64

	// readLoop/writeLoop goroutine fields:
	internalLoopStarted bool
	internalStopChan    chan struct{}
	// eventLoop fields:
	writeSchedChan chan bool // writable if not scheduled yet.

	stats              *types.ConnectionStats
	readCollector      metrics.Counter
	writeCollector     metrics.Counter
	lastBytesSizeRead  int64
	lastWriteSizeWrite int64

	closed    uint32
	connected uint32
	startOnce sync.Once

	tryMutex     *utils.Mutex
	needTransfer bool
	useWriteLoop bool

	// eventloop related
	poll struct {
		eventLoop        *eventLoop
		ev               *connEvent
		readTimeoutTimer *time.Timer
		readBufferMux    sync.Mutex
	}
}

// NewServerConnection new server-side connection, rawc is the raw connection from go/net
func NewServerConnection(ctx context.Context, rawc net.Conn, stopChan chan struct{}) api.Connection {
	id := atomic.AddUint64(&idCounter, 1)

	conn := &connection{
		id:               id,
		rawConnection:    rawc,
		localAddr:        rawc.LocalAddr(),
		remoteAddr:       rawc.RemoteAddr(),
		stopChan:         stopChan,
		readEnabled:      true,
		connected:        1,
		readEnabledChan:  make(chan bool, 1),
		internalStopChan: make(chan struct{}),
		writeBufferChan:  make(chan *[]buffer.IoBuffer, 8),
		writeSchedChan:   make(chan bool, 1),
		transferChan:     make(chan uint64),
		network:          rawc.LocalAddr().Network(),
		stats: &types.ConnectionStats{
			ReadTotal:     metrics.NewCounter(),
			ReadBuffered:  metrics.NewGauge(),
			WriteTotal:    metrics.NewCounter(),
			WriteBuffered: metrics.NewGauge(),
		},
		readCollector:  metrics.NilCounter{},
		writeCollector: metrics.NilCounter{},
		tryMutex:       utils.NewMutex(),
	}

	// store fd
	if val := mosnctx.Get(ctx, types.ContextKeyConnectionFd); val != nil {
		conn.file = val.(*os.File)
	}

	if conn.network == "udp" {
		if val := mosnctx.Get(ctx, types.ContextKeyAcceptBuffer); val != nil {
			buf := val.([]byte)
			conn.readBuffer = buffer.GetIoBuffer(UdpPacketMaxSize)
			conn.readBuffer.Write(buf)
			conn.updateReadBufStats(int64(conn.readBuffer.Len()), int64(conn.readBuffer.Len()))
		}
	}

	// transfer old mosn connection
	if val := mosnctx.Get(ctx, types.ContextKeyAcceptChan); val != nil {
		if val := mosnctx.Get(ctx, types.ContextKeyAcceptBuffer); val != nil {
			buf := val.([]byte)
			conn.readBuffer = buffer.GetIoBuffer(len(buf))
			conn.readBuffer.Write(buf)
		}

		ch := val.(chan api.Connection)
		ch <- conn
		if log.DefaultLogger.GetLogLevel() >= log.INFO {
			log.DefaultLogger.Infof("[network] [new server connection] NewServerConnection id = %d, buffer = %d", conn.id, conn.readBuffer.Len())
		}
	}

	conn.filterManager = newFilterManager(conn)

	return conn
}

// basic

func (c *connection) ID() uint64 {
	return c.id
}

func (c *connection) Start(lctx context.Context) {
	// udp downstream connection do not use read/write loop
	if c.network == "udp" && c.rawConnection.RemoteAddr() == nil {
		return
	}
	c.startOnce.Do(func() {
		if UseNetpollMode {
			c.attachEventLoop(lctx)
		} else {
			c.startRWLoop(lctx)
		}
	})
}

func (c *connection) SetIdleTimeout(readTimeout time.Duration, idleTimeout time.Duration) {
	c.newIdleChecker(readTimeout, idleTimeout)
}

func (conn *connection) OnConnectionEvent(event api.ConnectionEvent) {
	if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
		log.DefaultLogger.Debugf("[network] receive new connection event %s, try to handle", event)
	}
	for _, listener := range conn.connCallbacks {
		listener.OnEvent(event)
	}
}

func (c *connection) attachEventLoop(lctx context.Context) {
	// Choose one event loop to register, the implement is platform-dependent(epoll for linux and kqueue for bsd)
	c.poll.eventLoop = attach()

	// create a new timer and bind it to connection
	c.poll.readTimeoutTimer = time.AfterFunc(buffer.ConnReadTimeout, func() {
		// run read timeout callback, for keep alive if configured
		c.OnConnectionEvent(api.OnReadTimeout)

		c.poll.readBufferMux.Lock()
		defer c.poll.readBufferMux.Unlock()

		// shrink read buffer
		// this shrink logic may happen concurrent with read callback
		// so we should protect this under readBufferMux
		if c.network == "tcp" && c.readBuffer != nil && c.readBuffer.Len() == 0 && c.readBuffer.Cap() > DefaultBufferReadCapacity {
			c.readBuffer.Free()
			c.readBuffer.Alloc(DefaultBufferReadCapacity)
		}

		// if connection is not closed, timer should be reset
		if !c.poll.ev.stopped.Load() {
			c.poll.readTimeoutTimer.Reset(buffer.ConnReadTimeout)
		}
	})

	// Register read only, write is supported now because it is more complex than read.
	// We need to write our own code based on syscall.write to deal with the EAGAIN and writable epoll event
	err := c.poll.eventLoop.registerRead(c, &connEventHandler{
		onRead: func() bool {
			if c.readEnabled {
				// read buffer is used during the doRead and timeout process
				// we should protect it from concurrent access from the timer
				c.poll.readBufferMux.Lock()
				defer c.poll.readBufferMux.Unlock()

				c.poll.readTimeoutTimer.Stop()

				var err error
				for {
					err = c.doRead()
					if err != nil {
						break
					}

					if c, ok := c.rawConnection.(*mtls.TLSConn); ok && c.Conn.HasMoreData() {
						continue
					}

					break
				}

				// reset read timeout timer
				// mainly for heartbeat request
				if err == nil {
					// read err is nil, start timer
					c.poll.readTimeoutTimer.Reset(buffer.ConnReadTimeout)
					return true
				}

				// err != nil
				if te, ok := err.(net.Error); ok && te.Timeout() {
					if c.network == "tcp" && c.readBuffer != nil && c.readBuffer.Len() == 0 && c.readBuffer.Cap() > DefaultBufferReadCapacity {
						c.readBuffer.Free()
						c.readBuffer.Alloc(DefaultBufferReadCapacity)
					}

					// should reset timer
					c.poll.readTimeoutTimer.Reset(buffer.ConnReadTimeout)
					return true
				}

				if err == io.EOF {
					if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
						log.DefaultLogger.Debugf("[network] [event loop] [onRead] Error on read. Connection = %d, Remote Address = %s, err = %s",
							c.id, c.RemoteAddr().String(), err)
					}
					c.Close(api.NoFlush, api.RemoteClose)
				} else {
					log.DefaultLogger.Errorf("[network] [event loop] [onRead] Error on read. Connection = %d, Remote Address = %s, err = %s",
						c.id, c.RemoteAddr().String(), err)
					c.Close(api.NoFlush, api.OnReadErrClose)
				}

				return false
			} else {
				select {
				case <-c.readEnabledChan:
				case <-time.After(100 * time.Millisecond):
				}
			}
			return true
		},

		onHup: func() bool {
			log.DefaultLogger.Errorf("[network] [event loop] [onHup] ReadHup error. Connection = %d, Remote Address = %s", c.id, c.RemoteAddr().String())
			c.Close(api.NoFlush, api.RemoteClose)
			return false
		},
	})

	if err != nil {
		log.DefaultLogger.Errorf("[network] [event loop] [register] conn %d register read failed:%s", c.id, err.Error())
		c.Close(api.NoFlush, api.LocalClose)
		return
	}
}

var OptimizeLocalWrite = false

func SetOptimizeLocalWrite(b bool) {
	OptimizeLocalWrite = b
}

func (c *connection) checkUseWriteLoop() bool {
	// if OptimizeLocalWrite is false, connection just use write directly.
	// if OptimizeLocalWrite is true, and connection remote address is loopback
	// connection will start a goroutine for write
	if !OptimizeLocalWrite {
		return false
	}
	var ip net.IP
	switch c.network {
	case "udp":
		if udpAddr, ok := c.remoteAddr.(*net.UDPAddr); ok {
			ip = udpAddr.IP
		} else {
			return false
		}
	case "unix":
		return false
	case "tcp":
		if tcpAddr, ok := c.remoteAddr.(*net.TCPAddr); ok {
			ip = tcpAddr.IP
		} else {
			return false
		}
	}

	if ip.IsLoopback() {
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[network] [check use writeloop] Connection = %d, Local Address = %+v, Remote Address = %+v",
				c.id, c.rawConnection.LocalAddr(), c.RemoteAddr())
		}
		return true
	}
	return false
}

func (c *connection) startRWLoop(lctx context.Context) {
	c.internalLoopStarted = true

	utils.GoWithRecover(func() {
		c.startReadLoop()
	}, func(r interface{}) {
		c.Close(api.NoFlush, api.LocalClose)
	})

	if c.checkUseWriteLoop() {
		c.useWriteLoop = true
		utils.GoWithRecover(func() {
			c.startWriteLoop()
		}, func(r interface{}) {
			c.Close(api.NoFlush, api.LocalClose)
		})
	}
}

func (c *connection) startReadLoop() {
	var transferTime time.Time
	for {
		// exit loop asap. one receive & one default block will be optimized by go compiler
		select {
		case <-c.internalStopChan:
			return
		default:
		}

		select {
		case <-c.stopChan:
			if transferTime.IsZero() {
				if c.transferCallbacks != nil && c.transferCallbacks() {
					randTime := time.Duration(rand.Intn(int(TransferTimeout.Nanoseconds())))
					transferTime = time.Now().Add(TransferTimeout).Add(randTime)
					log.DefaultLogger.Infof("[network] [read loop] transferTime: Wait %d Second", (TransferTimeout+randTime)/1e9)
				} else {
					// set a long time, not transfer connection, wait mosn exit.
					transferTime = time.Now().Add(10 * TransferTimeout)
					log.DefaultLogger.Infof("[network] [read loop] not support transfer connection, Connection = %d, Local Address = %+v, Remote Address = %+v",
						c.id, c.rawConnection.LocalAddr(), c.RemoteAddr())
				}
			} else {
				if transferTime.Before(time.Now()) {
					c.transfer()
					return
				}
			}
		default:
		}

		select {
		case <-c.internalStopChan:
			return
		case <-c.readEnabledChan:
		default:
			if c.readEnabled {
				err := c.doRead()
				if err != nil {
					if te, ok := err.(net.Error); ok && te.Timeout() {
						if c.network == "tcp" && c.readBuffer != nil && c.readBuffer.Len() == 0 && c.readBuffer.Cap() > DefaultBufferReadCapacity {
							c.readBuffer.Free()
							c.readBuffer.Alloc(DefaultBufferReadCapacity)
						}
						continue
					}

					// normal close or health check, modify log level
					if c.lastBytesSizeRead == 0 || err == io.EOF {
						if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
							log.DefaultLogger.Debugf("[network] [read loop] Error on read. Connection = %d, Local Address = %+v, Remote Address = %+v, err = %v",
								c.id, c.rawConnection.LocalAddr(), c.RemoteAddr(), err)
						}
					} else {
						log.DefaultLogger.Errorf("[network] [read loop] Error on read. Connection = %d, Local Address = %+v, Remote Address = %+v, err = %v",
							c.id, c.rawConnection.LocalAddr(), c.RemoteAddr(), err)
					}

					if err == io.EOF {
						c.Close(api.NoFlush, api.RemoteClose)
					} else {
						c.Close(api.NoFlush, api.OnReadErrClose)
					}

					return
				}
			} else {
				select {
				case <-c.readEnabledChan:
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}
}

func (c *connection) transfer() {
	c.notifyTransfer()
	id, _ := transferRead(c)
	c.transferWrite(id)
}

func (c *connection) notifyTransfer() {
	if c.useWriteLoop {
		c.transferChan <- transferNotify
	} else {
		locked := c.tryMutex.TryLock(types.DefaultConnTryTimeout)
		if locked {
			c.needTransfer = true
			c.tryMutex.Unlock()
		}
	}
}

func (c *connection) transferWrite(id uint64) {
	log.DefaultLogger.Infof("[network] TransferWrite begin")
	for {
		select {
		case <-c.internalStopChan:
			return
		case buf, ok := <-c.writeBufferChan:
			if !ok {
				return
			}
			c.appendBuffer(buf)
			transferWrite(c, id)
		}
	}
}

func (c *connection) setReadDeadline() {
	switch c.network {
	case "udp":
		c.rawConnection.SetReadDeadline(time.Now().Add(types.DefaultUDPReadTimeout))
	default:
		c.rawConnection.SetReadDeadline(time.Now().Add(buffer.ConnReadTimeout))
	}
}

func (c *connection) doRead() (err error) {
	if c.readBuffer == nil {
		switch c.network {
		case "udp":
			// A UDP socket will Read up to the size of the receiving buffer and will discard the rest
			c.readBuffer = buffer.GetIoBuffer(UdpPacketMaxSize)
		default: // unix or tcp
			c.readBuffer = buffer.GetIoBuffer(DefaultBufferReadCapacity)
		}
	}

	var bytesRead int64
	c.setReadDeadline()
	bytesRead, err = c.readBuffer.ReadOnce(c.rawConnection)

	if err != nil {
		if atomic.LoadUint32(&c.closed) == 1 {
			return err
		}
		if te, ok := err.(net.Error); ok && te.Timeout() {
			for _, cb := range c.connCallbacks {
				cb.OnEvent(api.OnReadTimeout) // run read timeout callback, for keep alive if configured
			}
			if bytesRead == 0 {
				return err
			}
		} else if err != io.EOF {
			return err
		}
	}

	//todo: ReadOnce maybe always return (0, nil) and causes dead loop (hack)
	if bytesRead == 0 && err == nil {
		err = io.EOF
		log.DefaultLogger.Errorf("[network] ReadOnce maybe always return (0, nil) and causes dead loop, Connection = %d, Local Address = %+v, Remote Address = %+v",
			c.id, c.rawConnection.LocalAddr(), c.RemoteAddr())
	}

	c.onRead(bytesRead)
	return
}

func (c *connection) updateReadBufStats(bytesRead int64, bytesBufSize int64) {
	if c.stats == nil {
		return
	}

	if bytesRead > 0 {
		c.stats.ReadTotal.Inc(bytesRead)
		c.readCollector.Inc(bytesRead)
	}

	if bytesBufSize != c.lastBytesSizeRead {
		// todo: fix: when read blocks, ReadCurrent is out-of-date
		c.stats.ReadBuffered.Update(bytesBufSize)
		c.lastBytesSizeRead = bytesBufSize
	}
}

func (c *connection) OnRead(b buffer.IoBuffer) {
	c.readBuffer = b
	bytesRead := b.Len()
	c.onRead(int64(bytesRead))
}

func (c *connection) onRead(bytesRead int64) {
	for _, cb := range c.bytesReadCallbacks {
		cb(uint64(bytesRead))
	}

	if !c.readEnabled {
		return
	}

	if c.readBuffer.Len() == 0 {
		return
	}

	c.filterManager.OnRead()
	c.updateReadBufStats(bytesRead, int64(c.readBuffer.Len()))
}

func (c *connection) Write(buffers ...buffer.IoBuffer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Errorf("[network] [write] connection has closed. Connection = %d, Local Address = %+v, Remote Address = %+v, err = %+v",
				c.id, c.LocalAddr(), c.RemoteAddr(), r)
			err = types.ErrConnectionHasClosed
		}
	}()

	fs := c.filterManager.OnWrite(buffers)

	if fs == api.Stop {
		return nil
	}

	if UseNetpollMode {
		err = c.writeDirectly(&buffers)
		return
	}

	// non netpoll mode
	if c.useWriteLoop {
		select {
		case c.writeBufferChan <- &buffers:
			return
		default:
		}

		// fail after 60s
		t := acquireTimer(types.DefaultConnTryTimeout)
		select {
		case c.writeBufferChan <- &buffers:
		case <-t.C:
			err = types.ErrWriteBufferChanTimeout
		}
		releaseTimer(t)
	} else {
		err = c.writeDirectly(&buffers)
	}

	return
}

func (c *connection) setWriteDeadline() {
	switch c.network {
	case "udp":
		c.rawConnection.SetWriteDeadline(time.Now().Add(types.DefaultUDPIdleTimeout))
	default:
		c.rawConnection.SetWriteDeadline(time.Now().Add(types.DefaultConnWriteTimeout))
	}
}

func (c *connection) writeDirectly(buf *[]buffer.IoBuffer) (err error) {
	select {
	case <-c.internalStopChan:
		return types.ErrConnectionHasClosed
	default:
	}

	locked := c.tryMutex.TryLock(types.DefaultConnTryTimeout)

	if locked {
		defer c.tryMutex.Unlock()
		if c.needTransfer {
			c.writeBufferChan <- buf
			return
		}

		c.appendBuffer(buf)

		c.setWriteDeadline()
		_, err = c.doWrite()
	} else {
		// trylock timeouted
		err = types.ErrWriteTryLockTimeout
	}

	if err != nil {

		if err == buffer.EOF {
			if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
				log.DefaultLogger.Debugf("[network] [write directly] Error on write. Connection = %d, Remote Address = %s, err = %s, conn = %p",
					c.id, c.RemoteAddr().String(), err, c)
			}
			c.Close(api.NoFlush, api.LocalClose)
		} else {
			log.DefaultLogger.Errorf("[network] [write directly] Error on write. Connection = %d, Remote Address = %s, err = %s, conn = %p",
				c.id, c.RemoteAddr().String(), err, c)
		}

		if te, ok := err.(net.Error); ok && te.Timeout() {
			c.Close(api.NoFlush, api.OnWriteTimeout)
		}

		//other write errs not close connection, beacause readbuffer may have unread data, wait for readloop close connection,

		return
	}

	return nil
}

func (c *connection) startWriteLoop() {
	var needTransfer bool
	defer func() {
		if !needTransfer {
			close(c.writeBufferChan)
		}
	}()

	var err error
	for {
		// exit loop asap. one receive & one default block will be optimized by go compiler
		select {
		case <-c.internalStopChan:
			return
		default:
		}

		select {
		case <-c.internalStopChan:
			return
		case <-c.transferChan:
			needTransfer = true
			return
		case buf, ok := <-c.writeBufferChan:
			if !ok {
				return
			}
			c.appendBuffer(buf)

			//todo: dynamic set loop nums
		OUTER:
			for i := 0; i < 10; i++ {
				select {
				case buf, ok := <-c.writeBufferChan:
					if !ok {
						return
					}
					c.appendBuffer(buf)
				default:
					break OUTER
				}
			}

			c.setWriteDeadline()
			_, err = c.doWrite()
		}

		if err != nil {

			if err == buffer.EOF {
				if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
					log.DefaultLogger.Debugf("[network] [write loop] Error on write. Connection = %d, Remote Address = %s, err = %s, conn = %p",
						c.id, c.RemoteAddr().String(), err, c)
				}
				c.Close(api.NoFlush, api.LocalClose)
			} else {
				log.DefaultLogger.Errorf("[network] [write loop] Error on write. Connection = %d, Remote Address = %s, err = %s, conn = %p",
					c.id, c.RemoteAddr().String(), err, c)
			}

			if te, ok := err.(net.Error); ok && te.Timeout() {
				c.Close(api.NoFlush, api.OnWriteTimeout)
			}

			if c.network == "udp" && strings.Contains(err.Error(), "connection refused") {
				c.Close(api.NoFlush, api.RemoteClose)
			}
			//other write errs not close connection, beacause readbuffer may have unread data, wait for readloop close connection,

			return
		}
	}
}

func (c *connection) appendBuffer(iobuffers *[]buffer.IoBuffer) {
	if iobuffers == nil {
		return
	}
	for _, buf := range *iobuffers {
		if buf == nil {
			continue
		}
		c.ioBuffers = append(c.ioBuffers, buf)
		c.writeBuffers = append(c.writeBuffers, buf.Bytes())
	}
}

func (c *connection) doWrite() (int64, error) {
	bytesSent, err := c.doWriteIo()
	if err != nil && atomic.LoadUint32(&c.closed) == 1 {
		return 0, nil
	}

	c.updateWriteBuffStats(bytesSent, int64(c.writeBufLen()))

	for _, cb := range c.bytesSendCallbacks {
		cb(uint64(bytesSent))
	}

	return bytesSent, err
}

func (c *connection) doWriteIo() (bytesSent int64, err error) {
	buffers := c.writeBuffers
	if tlsConn, ok := c.rawConnection.(*mtls.TLSConn); ok {
		bytesSent, err = tlsConn.WriteTo(&buffers)
	} else {
		//todo: writev(runtime) has memroy leak.
		switch c.network {
		case "unix":
			bytesSent, err = buffers.WriteTo(c.rawConnection)
		case "tcp":
			bytesSent, err = buffers.WriteTo(c.rawConnection)
		case "udp":
			addr := c.RemoteAddr().(*net.UDPAddr)
			n := 0
			bytesSent = 0
			for _, buf := range c.ioBuffers {
				if c.rawConnection.RemoteAddr() == nil {
					n, err = c.rawConnection.(*net.UDPConn).WriteToUDP(buf.Bytes(), addr)
				} else {
					n, err = c.rawConnection.Write(buf.Bytes())
				}
				if err != nil {
					break
				}
				bytesSent += int64(n)
			}
		}
	}
	if err != nil {
		return bytesSent, err
	}
	for i, buf := range c.ioBuffers {
		c.ioBuffers[i] = nil
		c.writeBuffers[i] = nil
		if buf.EOF() {
			err = buffer.EOF
		}
		if e := buffer.PutIoBuffer(buf); e != nil {
			log.DefaultLogger.Errorf("[network] [do write] PutIoBuffer error: %v", e)
		}
	}
	c.ioBuffers = c.ioBuffers[:0]
	c.writeBuffers = c.writeBuffers[:0]
	return
}

func (c *connection) updateWriteBuffStats(bytesWrite int64, bytesBufSize int64) {
	if c.stats == nil {
		return
	}

	if bytesWrite > 0 {
		c.stats.WriteTotal.Inc(bytesWrite)
		c.writeCollector.Inc(bytesWrite)
	}

	if bytesBufSize != c.lastWriteSizeWrite {
		c.stats.WriteBuffered.Update(bytesBufSize)
		c.lastWriteSizeWrite = bytesBufSize
	}
}

func (c *connection) writeBufLen() (bufLen int) {
	for _, buf := range c.writeBuffers {
		bufLen += len(buf)
	}
	return
}

func (c *connection) Close(ccType api.ConnectionCloseType, eventType api.ConnectionEvent) error {
	defer func() {
		if p := recover(); p != nil {
			log.DefaultLogger.Errorf("[network] [close connection] panic %v\n%s", p, string(debug.Stack()))
		}
	}()

	if ccType == api.FlushWrite {
		c.Write(buffer.NewIoBufferEOF())
		return nil
	}

	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return nil
	}

	// connection failed in client mode
	if c.rawConnection == nil || reflect.ValueOf(c.rawConnection).IsNil() {
		return nil
	}

	// shutdown read first
	if rawc, ok := c.rawConnection.(*net.TCPConn); ok {
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[network] [close connection] Close TCP Conn, Remote Address is = %s, eventType is = %s", rawc.RemoteAddr(), eventType)
		}
		rawc.CloseRead()
	}

	if c.network == "udp" && c.RawConn().RemoteAddr() == nil {
		key := GetProxyMapKey(c.localAddr.String(), c.remoteAddr.String())
		DelUDPProxyMap(key)
	}

	// wait for io loops exit, ensure single thread operate streams on the connection
	// because close function must be called by one io loop thread, notify another loop here
	close(c.internalStopChan)
	if c.poll.eventLoop != nil {
		// unregister events while connection close
		c.poll.eventLoop.unregisterRead(c)
		// close copied fd
		err := c.file.Close()
		if err != nil {
			log.DefaultLogger.Errorf("[network] [close connection] error, %v", err)
		}

		c.poll.readTimeoutTimer.Stop()
	}

	c.rawConnection.Close()

	if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
		log.DefaultLogger.Debugf("[network] [close connection] Close connection %d, event %s, type %s", c.id, eventType, ccType)
	}

	c.updateReadBufStats(0, 0)
	c.updateWriteBuffStats(0, 0)

	c.OnConnectionEvent(eventType)

	return nil
}

func (c *connection) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *connection) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *connection) SetRemoteAddr(address net.Addr) {
	c.remoteAddr = address
}

func (c *connection) AddConnectionEventListener(cb api.ConnectionEventListener) {
	c.connCallbacks = append(c.connCallbacks, cb)
}

func (c *connection) AddBytesReadListener(cb func(bytesRead uint64)) {
	c.bytesReadCallbacks = append(c.bytesReadCallbacks, cb)
}

func (c *connection) AddBytesSentListener(cb func(bytesSent uint64)) {
	c.bytesSendCallbacks = append(c.bytesSendCallbacks, cb)
}

func (c *connection) NextProtocol() string {
	// TODO
	return ""
}

func (c *connection) SetNoDelay(enable bool) {
	if c.rawConnection != nil {

		if rawc, ok := c.rawConnection.(*net.TCPConn); ok {
			rawc.SetNoDelay(enable)
		}
	}
}

func (c *connection) SetReadDisable(disable bool) {
	if disable {
		if !c.readEnabled {
			c.readDisableCount++
			return
		}

		c.readEnabled = false
	} else {
		if c.readDisableCount > 0 {
			c.readDisableCount--
			return
		}

		c.readEnabled = true
		// only on read disable status, we need to trigger chan to wake read loop up
		c.readEnabledChan <- true
	}
}

func (c *connection) ReadEnabled() bool {
	return c.readEnabled
}

func (c *connection) TLS() net.Conn {
	return nil
}

func (c *connection) SetBufferLimit(limit uint32) {
	if limit > 0 {
		c.bufferLimit = limit
	}
}

func (c *connection) BufferLimit() uint32 {
	return c.bufferLimit
}

func (c *connection) SetLocalAddress(localAddress net.Addr, restored bool) {
	// TODO
	c.localAddressRestored = restored
}

func (c *connection) SetCollector(read, write metrics.Counter) {
	c.readCollector = read
	c.writeCollector = write
}

func (c *connection) LocalAddressRestored() bool {
	return c.localAddressRestored
}

// BufferSource
func (c *connection) GetWriteBuffer() []buffer.IoBuffer {
	return c.curWriteBufferData
}

func (c *connection) GetReadBuffer() buffer.IoBuffer {
	return c.readBuffer
}

func (c *connection) FilterManager() api.FilterManager {
	return c.filterManager
}

func (c *connection) RawConn() net.Conn {
	return c.rawConnection
}

func (c *connection) SetTransferEventListener(listener func() bool) {
	c.transferCallbacks = listener
}

func (c *connection) State() api.ConnState {
	if atomic.LoadUint32(&c.closed) == 1 {
		return api.ConnClosed
	}
	if atomic.LoadUint32(&c.connected) == 1 {
		return api.ConnActive
	}
	return api.ConnInit
}

type clientConnection struct {
	connection

	connectTimeout time.Duration

	connectOnce sync.Once
}

// NewClientConnection new client-side connection
func NewClientConnection(connectTimeout time.Duration, tlsMng types.TLSClientContextManager, remoteAddr net.Addr, stopChan chan struct{}) types.ClientConnection {
	id := atomic.AddUint64(&idCounter, 1)

	conn := &clientConnection{
		connection: connection{
			id:               id,
			remoteAddr:       remoteAddr,
			stopChan:         stopChan,
			readEnabled:      true,
			readEnabledChan:  make(chan bool, 1),
			internalStopChan: make(chan struct{}),
			writeBufferChan:  make(chan *[]buffer.IoBuffer, 8),
			writeSchedChan:   make(chan bool, 1),
			stats: &types.ConnectionStats{
				ReadTotal:     metrics.NewCounter(),
				ReadBuffered:  metrics.NewGauge(),
				WriteTotal:    metrics.NewCounter(),
				WriteBuffered: metrics.NewGauge(),
			},
			readCollector:  metrics.NilCounter{},
			writeCollector: metrics.NilCounter{},
			tlsMng:         tlsMng,
			tryMutex:       utils.NewMutex(),
		},
		connectTimeout: connectTimeout,
	}

	conn.filterManager = newFilterManager(conn)

	if conn.remoteAddr != nil {
		conn.network = conn.remoteAddr.Network()
	}

	return conn
}

func (cc *clientConnection) connect() (event api.ConnectionEvent, err error) {
	timeout := cc.connectTimeout
	if timeout == 0 {
		timeout = DefaultConnectTimeout
	}
	addr := cc.RemoteAddr()
	if addr == nil {
		return api.ConnectFailed, errors.New("ClientConnection RemoteAddr is nil")
	}
	cc.rawConnection, err = net.DialTimeout(cc.network, cc.RemoteAddr().String(), timeout)
	if err != nil {
		if err == io.EOF {
			// remote conn closed
			event = api.RemoteClose
		} else if err, ok := err.(net.Error); ok && err.Timeout() {
			event = api.ConnectTimeout
		} else {
			event = api.ConnectFailed
		}
		return
	}
	atomic.StoreUint32(&cc.connected, 1)
	event = api.Connected
	cc.localAddr = cc.rawConnection.LocalAddr()

	// ensure ioEnabled and UseNetpollMode
	if !UseNetpollMode {
		return
	}
	// store fd
	switch cc.network {
	case "udp":
		if tc, ok := cc.rawConnection.(*net.UDPConn); ok {
			cc.file, err = tc.File()
		}
	case "unix":
		if tc, ok := cc.rawConnection.(*net.UnixConn); ok {
			cc.file, err = tc.File()
		}
	case "tcp":
		if tc, ok := cc.rawConnection.(*net.TCPConn); ok {
			cc.file, err = tc.File()
		}
	}
	return
}

func (cc *clientConnection) tryConnect() (event api.ConnectionEvent, err error) {
	event, err = cc.connect()
	if err != nil {
		return
	}
	if cc.tlsMng == nil {
		return
	}
	cc.rawConnection, err = cc.tlsMng.Conn(cc.rawConnection)
	if err == nil {
		return
	}
	if !cc.tlsMng.Fallback() {
		event = api.ConnectFailed
		return
	}
	log.DefaultLogger.Alertf(types.ErrorKeyTLSFallback, "tls handshake fallback, local addr %v, remote addr %v, error: %v",
		cc.localAddr, cc.remoteAddr, err)
	return cc.connect()
}

func (cc *clientConnection) Connect() (err error) {
	cc.connectOnce.Do(func() {
		var event api.ConnectionEvent
		event, err = cc.tryConnect()
		if err == nil {
			cc.Start(context.TODO())
		}
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[network] [client connection connect] connect raw %s, remote address = %s ,event = %+v, error = %+v", cc.network, cc.remoteAddr, event, err)
		}

		for _, cccb := range cc.connCallbacks {
			cccb.OnEvent(event)
		}
	})
	return
}
