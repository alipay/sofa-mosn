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
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/metrics"
	"mosn.io/mosn/pkg/types"
	"mosn.io/pkg/utils"
)

type ListenerState int

// listener state
// ListenerInited means listener is inited, a inited listener can be started or stopped
// ListenerRunning means listener is running, start a running listener will be ignored.
// ListenerStopped means listener is stopped, start a stopped listener without restart flag will be ignored.
const (
	ListenerInited ListenerState = iota
	ListenerRunning
	ListenerStopped
)

// listener impl based on golang net package
type listener struct {
	name                    string
	localAddress            net.Addr
	bindToPort              bool
	listenerTag             uint64
	perConnBufferLimitBytes uint32
	useOriginalDst          bool
	network                 string
	cb                      types.ListenerEventListener
	packetConn              net.PacketConn
	rawl                    net.Listener
	config                  *v2.Listener
	mutex                   sync.Mutex
	// listener state indicates the listener's running state. The listener state effects if a listener binded to a port
	state ListenerState
}

func NewListener(lc *v2.Listener) types.Listener {

	l := &listener{
		name:                    lc.Name,
		localAddress:            lc.Addr,
		bindToPort:              lc.BindToPort,
		listenerTag:             lc.ListenerTag,
		perConnBufferLimitBytes: lc.PerConnBufferLimitBytes,
		useOriginalDst:          lc.UseOriginalDst,
		network:                 lc.Network,
		config:                  lc,
	}

	if lc.InheritListener != nil {
		//inherit old process's listener
		l.rawl = lc.InheritListener
	}

	if lc.InheritPacketConn != nil {
		l.packetConn = *lc.InheritPacketConn
	}

	if lc.Network == "" {
		l.network = "tcp"
	}
	lc.Network = strings.ToLower(lc.Network)
	return l
}

func (l *listener) IsBindToPort() bool {
	return l.bindToPort
}

func (l *listener) Config() *v2.Listener {
	return l.config
}

func (l *listener) SetConfig(config *v2.Listener) {
	l.config = config
}

func (l *listener) Name() string {
	return l.name
}

func (l *listener) Addr() net.Addr {
	return l.localAddress
}

func (l *listener) Start(lctx context.Context, restart bool) {
	if l.bindToPort {
		ignore := func() bool {
			l.mutex.Lock()
			defer l.mutex.Unlock()
			switch l.state {
			case ListenerRunning:
				// if listener is running, ignore start
				log.DefaultLogger.Debugf("[network] [listener start] %s is running", l.name)
				return true
			case ListenerStopped:
				if !restart {
					return true
				}
				log.DefaultLogger.Infof("[network] [listener start] %s restart listener ", l.name)
				if err := l.listen(lctx); err != nil {
					// TODO: notify listener callbacks
					log.DefaultLogger.Alertf("listener.start", "[network] [listener start] [listen] %s listen failed, %v", l.name, err)
					return true
				}
			default:
				// try start listener
				//call listen if not inherit
				if l.rawl == nil && l.packetConn == nil {
					if err := l.listen(lctx); err != nil {
						// TODO: notify listener callbacks
						log.StartLogger.Fatalf("[network] [listener start] [listen] %s listen failed, %v", l.name, err)
					}
				}
			}
			l.state = ListenerRunning
			// add metrics for listener if bind port
			switch l.network {
			case "udp":
				metrics.AddListenerAddr(l.packetConn.(*net.UDPConn).LocalAddr().String() + l.network)
			default:
				metrics.AddListenerAddr(l.rawl.Addr().String())
			}

			return false
		}()
		if ignore {
			return
		}
		switch l.network {
		case "udp":
			l.readMsgEventLoop(lctx)
		default:
			l.acceptEventLoop(lctx)
		}
	}
}

func (l *listener) acceptEventLoop(lctx context.Context) {
	for {
		if err := l.accept(lctx); err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				log.DefaultLogger.Infof("[network] [listener start] [accept] listener %s stop accepting connections by deadline", l.name)
				return
			}
			if ope, ok := err.(*net.OpError); ok {
				// not timeout error and not temporary, which means the error is non-recoverable
				// stop accepting loop and log the event
				if !(ope.Timeout() && ope.Temporary()) {
					// accept error raised by sockets closing
					if ope.Op == "accept" {
						log.DefaultLogger.Infof("[network] [listener start] [accept] listener %s %s closed", l.name, l.Addr())
					} else {
						log.DefaultLogger.Alertf("listener.accept", "[network] [listener start] [accept] listener %s occurs non-recoverable error, stop listening and accepting:%s", l.name, err.Error())
					}
					return
				}
			} else {
				log.DefaultLogger.Errorf("[network] [listener start] [accept] listener %s occurs unknown error while accepting:%s", l.name, err.Error())
			}
		}
	}
}

func (l *listener) readMsgEventLoop(lctx context.Context) {
	utils.GoWithRecover(func() {
		readMsgLoop(lctx, l)
	}, func(r interface{}) {
		l.readMsgEventLoop(lctx)
	})
}

func (l *listener) Stop() error {
	if !l.bindToPort {
		return nil
	}

	var err error
	switch l.network {
	case "udp":
		err = l.packetConn.SetDeadline(time.Now())
	case "unix":
		err = l.rawl.(*net.UnixListener).SetDeadline(time.Now())
	case "tcp":
		err = l.rawl.(*net.TCPListener).SetDeadline(time.Now())
	}
	return err
}

func (l *listener) ListenerTag() uint64 {
	return l.listenerTag
}

func (l *listener) SetListenerTag(tag uint64) {
	l.listenerTag = tag
}

func (l *listener) ListenerFile() (*os.File, error) {
	if !l.bindToPort {
		return nil, syscall.EINVAL
	}

	switch l.network {
	case "udp":
		return l.packetConn.(*net.UDPConn).File()
	case "unix":
		return l.rawl.(*net.UnixListener).File()
	case "tcp":
		return l.rawl.(*net.TCPListener).File()
	}
	return nil, errors.New("not support this network " + l.network)
}

func (l *listener) PerConnBufferLimitBytes() uint32 {
	return l.perConnBufferLimitBytes
}

func (l *listener) SetPerConnBufferLimitBytes(limitBytes uint32) {
	l.perConnBufferLimitBytes = limitBytes
}

func (l *listener) SetListenerCallbacks(cb types.ListenerEventListener) {
	l.cb = cb
}

func (l *listener) GetListenerCallbacks() types.ListenerEventListener {
	return l.cb
}

func (l *listener) SetUseOriginalDst(use bool) {
	l.useOriginalDst = use
}

func (l *listener) UseOriginalDst() bool {
	return l.useOriginalDst
}

func (l *listener) Close(lctx context.Context) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.state = ListenerStopped

	if !l.bindToPort {
		return nil
	}

	if l.rawl != nil {
		l.cb.OnClose()
		return l.rawl.Close()
	}
	if l.packetConn != nil {
		l.cb.OnClose()
		return l.packetConn.Close()
	}
	return nil
}

func (l *listener) listen(lctx context.Context) error {
	var err error
	var rawl net.Listener
	var rconn net.PacketConn

	if l.localAddress == nil {
		return errors.New("listener local addr is nil")
	}

	switch l.network {
	case "udp":
		lc := net.ListenConfig{}
		if rconn, err = lc.ListenPacket(context.Background(), l.network, l.localAddress.String()); err != nil {
			return err
		}
		l.packetConn = rconn
	case "unix":
		if rawl, err = net.Listen("unix", l.localAddress.String()); err != nil {
			return err
		}
		l.rawl = rawl
	case "tcp":
		if rawl, err = net.Listen("tcp", l.localAddress.String()); err != nil {
			return err
		}
		l.rawl = rawl
	}

	return nil
}

func (l *listener) accept(lctx context.Context) error {
	rawc, err := l.rawl.Accept()

	if err != nil {
		return err
	}

	// TODO: use thread pool
	utils.GoWithRecover(func() {
		l.cb.OnAccept(rawc, l.useOriginalDst, nil, nil, nil, nil)
	}, nil)

	return nil
}
