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

package tunnel

import (
	"errors"
	"fmt"
	"net"
	"time"

	"go.uber.org/atomic"
	"mosn.io/api"
	"mosn.io/mosn/pkg/filter/network/tunnel/ext"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/types"
	"mosn.io/pkg/buffer"
	"mosn.io/pkg/utils"
)

type BaseRawConnection struct {
	readBuffer buffer.IoBuffer
	rawc       net.Conn
	close      *atomic.Bool
	closeChan  chan struct{}
	listener   types.Listener
	init       func() error

	connectRetryTimes      int
	reconnectBaseDuration  time.Duration
	address                string
	network                string
	connectTimeoutDuration time.Duration
	readTimeoutDuration    time.Duration
}

func (a *BaseRawConnection) doConnect() error {
	rawc, err := net.DialTimeout(a.network, a.address, a.connectTimeoutDuration)
	if err != nil {
		log.DefaultLogger.Errorf("[agent] failed to connect remote server, address: %v, err: %+v", a.address, err)
		return err
	}
	rawc.SetReadDeadline(time.Now().Add(a.readTimeoutDuration))
	a.rawc = rawc
	return nil
}

func (a *BaseRawConnection) ReadOneMessage() (interface{}, error) {
	for {
		select {
		case <-a.closeChan:
			return nil, errors.New("agent connection closed")
		default:
			_, err := a.readBuffer.ReadOnce(a.rawc)
			// Timout or EOF
			if err != nil {
				log.DefaultLogger.Errorf("[agent] read response failed, err: %+v", a.address, err)
				a.Close()
				return nil, err
			}
			ret, err := DecodeFromBuffer(a.readBuffer)
			if err != nil {
				log.DefaultLogger.Warnf("[agent] decode from buffer failed, err: %+v", err)
				return nil, err
			}
			// Data is not enough, continue to read
			if ret == nil {
				continue
			}
			return ret, nil
		}
	}
}

func (a *BaseRawConnection) Write(request interface{}) error {
	b, err := Encode(request)
	if err != nil {
		return err
	}
	// Write connection init request
	_, err = a.rawc.Write(b.Bytes())
	if err != nil {
		log.DefaultLogger.Errorf("[agent] failed to write data to remote server: %v, err: %+v", a.address, err)
	}
	return err
}

func (a *BaseRawConnection) Close() error {
	if a.close.CAS(false, true) {
		close(a.closeChan)
		if a.rawc == nil {
			return nil
		}
		err := a.rawc.Close()
		if err != nil {
			log.DefaultLogger.Errorf("[agent] failed to close raw connection, remote address: %v, err: %+v", a.address, err)
			return err
		}

	}
	return nil
}

func (a *BaseRawConnection) initConnection() error {
	var err error
	backoffConnectDuration := a.reconnectBaseDuration

	for i := 0; i < a.connectRetryTimes || a.connectRetryTimes == -1; i++ {
		if a.close.Load() {
			return fmt.Errorf("connection closed, don't attempt to connect, address: %v", a.address)
		}
		err = a.init()
		if err == nil {
			break
		}
		log.DefaultLogger.Errorf("[agent] failed to connect remote server, try again after %v seconds, address: %v, err: %+v", backoffConnectDuration, a.address, err)
		time.Sleep(backoffConnectDuration)
		backoffConnectDuration *= 2
	}
	if err != nil {
		return err
	}
	// Hosting new connection
	utils.GoWithRecover(func() {
		ch := make(chan api.Connection, 1)
		a.listener.GetListenerCallbacks().OnAccept(a.rawc, a.listener.UseOriginalDst(), nil, ch, a.readBuffer.Bytes(), []api.ConnectionEventListener{a})
	}, nil)

	return nil
}

func (a *BaseRawConnection) OnEvent(event api.ConnectionEvent) {
	switch {
	case event.IsClose(), event.ConnectFailure():
		break
	default:
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[agent] receive %s event, ignore it", event)
		}
		return
	}

	utils.GoWithRecover(func() {
		log.DefaultLogger.Infof("[agent] receive reconnect event, and try to reconnect remote server %v", a.address)
		err := a.initConnection()
		if err != nil {
			log.DefaultLogger.Errorf("[agent] failed to reconnect remote server: %v", a.address)
			return
		}
		log.DefaultLogger.Infof("[agent] reconnect remote server: %v success", a.address)
	}, nil)
}

// AgentCoreConnection
type AgentCoreConnection struct {
	ConnectionConfig
	BaseRawConnection
	readBuffer buffer.IoBuffer
	rawc       net.Conn
	listener   types.Listener
	close      *atomic.Bool
	closeChan  chan struct{}
	initInfo   *ConnectionInitInfo
}

func NewAgentCoreConnection(config ConnectionConfig, listener types.Listener) *AgentCoreConnection {

	initInfo := &ConnectionInitInfo{
		ClusterName:      config.ClusterName,
		Weight:           config.Weight,
		CredentialPolicy: config.CredentialPolicy,
	}

	if config.CredentialPolicy != "" {
		credentialGetter := ext.GetConnectionCredentialGetter(config.CredentialPolicy)
		if credentialGetter == nil {
			log.DefaultLogger.Fatalf("[agent] credential %v getter not found", config.CredentialPolicy)
		}
		initInfo.Credential = credentialGetter(config.ClusterName)
	}

	coreConn := &AgentCoreConnection{
		ConnectionConfig: config,
		listener:         listener,
		initInfo:         initInfo,
		readBuffer:       buffer.GetIoBuffer(1024),
		close:            atomic.NewBool(false),
	}
	base := BaseRawConnection{
		readBuffer:             buffer.NewIoBuffer(1024),
		close:                  atomic.NewBool(false),
		closeChan:              make(chan struct{}),
		listener:               listener,
		connectRetryTimes:      config.ConnectRetryTimes,
		reconnectBaseDuration:  config.ReconnectBaseDuration,
		address:                config.Address,
		network:                config.Network,
		connectTimeoutDuration: config.ConnectTimeoutDuration,
		readTimeoutDuration:    config.ConnectTimeoutDuration,
		init:                   coreConn.initAgentCoreConnection,
	}
	coreConn.BaseRawConnection = base
	return coreConn
}

func (a *AgentCoreConnection) initAgentCoreConnection() error {
	if err := a.doConnect(); err != nil {
		return err
	}
	if err := a.Write(a.initInfo); err != nil {
		return err
	}

	ret, err := a.ReadOneMessage()
	if err != nil {
		return err
	}
	resp := ret.(*ConnectionInitResponse)
	if resp.Status != ConnectSuccess {
		// Reconnect and write again
		log.DefaultLogger.Errorf("[agent] failed to write connection info to remote server, address: %v, status: %v", a.Address, resp.Status)
		// Close connection and reconnect again
		return a.Close()
	}
	return nil
}

type AgentAsideConnection struct {
	BaseRawConnection
}

func NewAgentAsideConnection(config ConnectionConfig, listener types.Listener) *AgentAsideConnection {
	asideConn := &AgentAsideConnection{}
	base := BaseRawConnection{
		readBuffer:             buffer.NewIoBuffer(1024),
		close:                  atomic.NewBool(false),
		closeChan:              make(chan struct{}),
		listener:               listener,
		connectRetryTimes:      config.ConnectRetryTimes,
		reconnectBaseDuration:  config.ReconnectBaseDuration,
		address:                config.Address,
		network:                config.Network,
		connectTimeoutDuration: config.ConnectTimeoutDuration,
		readTimeoutDuration:    config.ConnectTimeoutDuration,
		init:                   asideConn.initAsideConnection,
	}
	asideConn.BaseRawConnection = base
	return asideConn
}

func (a *AgentAsideConnection) initAsideConnection() error {
	return a.doConnect()
}
