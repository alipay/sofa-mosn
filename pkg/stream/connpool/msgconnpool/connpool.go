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
package msgconnpool

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	v2 "mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/upstream/cluster"

	"mosn.io/api"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/types"
	"mosn.io/pkg/buffer"
	"mosn.io/pkg/utils"
)

// types.ConnectionPool
type connpool struct {
	client *activeClient

	host      types.Host
	clientMux sync.RWMutex

	autoReconnectWhenClose bool
	heartBeatCreator       func() KeepAlive
	connTryTimes           int
	readFilters            []api.ReadFilter

	destroyed uint64
}

func (p *connpool) Host() types.Host {
	return p.host
}

// Destroy the pool
func (p *connpool) Destroy() {
	atomic.StoreUint64(&p.destroyed, 1)
	p.client.getConnData().Connection.Close(api.NoFlush, api.LocalClose)
}

var reconnectBackoff = []time.Duration{
	0, time.Second, time.Second * 2, time.Second * 5, time.Second * 10,
}

// types.StreamEventListener
// types.ConnectionEventListener
// types.StreamConnectionEventListener
type activeClient struct {
	pool            *connpool
	connData        atomic.Value // *types.CreateConnectionData
	keepAlive       *keepAliveListener
	reconnectLock   sync.Mutex
	connectTryTimes int
}

// get connData
func (ac *activeClient) getConnData() *types.CreateConnectionData {
	c := ac.connData.Load()
	if cd, ok := c.(*types.CreateConnectionData); ok {
		return cd
	}

	return nil
}

func (ac *activeClient) OnEvent(event api.ConnectionEvent) {
	//  all close event:
	//  ce == LocalClose || ce == RemoteClose ||
	//	ce == OnReadErrClose || ce == OnWriteErrClose || ce == OnWriteTimeout
	switch event {
	case api.RemoteClose:
		goto RECONN
	case api.OnReadErrClose, api.OnWriteErrClose, api.OnWriteTimeout,
		api.ConnectTimeout, api.ConnectFailed, api.LocalClose:
		// RemoteClose when read/write error
		// LocalClose when there is a panic
		// OnReadErrClose when read failed
		goto RECONN

	default:
		return
	}

RECONN:
	if ac.pool.autoReconnectWhenClose {
		// auto reconnect when close
		log.DefaultLogger.Warnf("[connpool] reconnect after event : %v,  connData : %v", event, ac)
		ac.reconnect()
	} else {
		ac.removeFromPool()
	}
}

// generate the client, and set it to the connpool
func (p *connpool) initActiveClient() {
	p.client = &activeClient{
		pool: p,
	}

	p.client.initConnection()
}

func (ac *activeClient) initConnection() {
	// build new conn
	// must create this new conn, the same conn can only be connected once
	createConnData := ac.pool.Host().CreateConnection(context.Background())

	createConnData.Connection.AddConnectionEventListener(ac)

	// connect the new connection
	err := createConnData.Connection.Connect()

	if err != nil {
		if ac.getConnData() == nil { // the first time
			// atomic store, avoid partial write
			// atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&ac.connData)), unsafe.Pointer(&createConnData))
			ac.connData.Store(&createConnData)
			log.DefaultLogger.Warnf("[connpool] connect failed %v times, connData : %v",
				ac.connectTryTimes, ac.pool.Host().Address())
		} else {
			log.DefaultLogger.Warnf("[connpool] reconnect failed %v times, ac : %p",
				ac.connectTryTimes, ac)
		}

		return
	}

	// atomic store, avoid partial write
	ac.connData.Store(&createConnData)

	log.DefaultLogger.Infof("[connpool] reconnect succeed after %v tries, connData %v, ac %p, connData: %v",
		ac.connectTryTimes, ac.getConnData().Host.AddressString(), ac, ac.getConnData())

	// if pool was destroyed, but connection was connected
	// we need to close it
	if atomic.LoadUint64(&ac.pool.destroyed) == 1 {
		ac.getConnData().Connection.Close(api.NoFlush, api.LocalClose)
		return
	}

	// read filters
	for _, rf := range ac.pool.readFilters {
		ac.getConnData().Connection.FilterManager().AddReadFilter(rf)
	}

	// set the new heartbeat
	ac.setHeartBeater(ac.pool.heartBeatCreator())

	// clear retry times
	ac.connectTryTimes = 0
}

// reconnect triggers connection to reconnect
func (ac *activeClient) reconnect() {
	if atomic.LoadUint64(&ac.pool.destroyed) == 1 {
		return
	}

	if ac.connectTryTimes >= ac.pool.connTryTimes {
		log.DefaultLogger.Warnf("[connpool] retry time exceed pool config %v", ac.pool.connTryTimes)
		return
	}

	var idx = ac.connectTryTimes
	if idx >= len(reconnectBackoff) {
		idx = len(reconnectBackoff) - 1
	}

	utils.GoWithRecover(func() {
		time.Sleep(reconnectBackoff[idx])
		ac.reconnectLock.Lock()
		defer ac.reconnectLock.Unlock()

		ac.connectTryTimes++

		if atomic.LoadUint64(&ac.pool.destroyed) == 1 {
			// if pool was exited, then stop
			return
		}

		ac.initConnection()

	}, func(r interface{}) {
		log.DefaultLogger.Warnf("[connpool] reconnect failed, %v, connData: %v", r, ac.getConnData().Host.AddressString())
	})

}

// removeFromPool removes this client from connection pool
func (ac *activeClient) removeFromPool() {
	p := ac.pool
	p.clientMux.Lock()
	defer p.clientMux.Unlock()

	p.client = nil
}

// setHeartBeater set the heart beat for an active client
func (ac *activeClient) setHeartBeater(hb KeepAlive) {
	// clear the previous keepAlive
	if ac.keepAlive != nil && ac.keepAlive.keepAlive != nil {
		ac.keepAlive.keepAlive.Stop()
		ac.keepAlive = nil
	}

	ac.keepAlive = &keepAliveListener{
		keepAlive: hb,
		conn:      ac.getConnData().Connection,
	}

	// this should be equal to
	// ac.codecClient.AddConnectionEventListener(ac.keepAlive)
	ac.getConnData().Connection.AddConnectionEventListener(ac.keepAlive)
}

// keepAliveListener is a types.ConnectionEventListener
type keepAliveListener struct {
	keepAlive KeepAlive
	conn      api.Connection
}

// OnEvent impl types.ConnectionEventListener
func (l *keepAliveListener) OnEvent(event api.ConnectionEvent) {
	if event == api.OnReadTimeout && l.keepAlive != nil {
		heartbeatFailCreator := func() {
			l.conn.Close(api.NoFlush, api.OnReadErrClose)
		}

		l.conn.Write(buffer.NewIoBufferBytes(l.keepAlive.GetKeepAliveData(heartbeatFailCreator)))
	}
}

type KeepAlive interface {
	Stop()

	GetKeepAliveData(failCallback func()) []byte
}

type Connection interface {
	Write(buf ...buffer.IoBuffer) error
	Destroy()
	Available() bool
}

// NewConn returns a simplified connpool
func NewConn(hostAddr string, connectTryTimes int, heartBeatCreator func() KeepAlive, readFilters []api.ReadFilter, autoReconnectWhenClose bool) Connection {
	// use connData addr as cluster name, for the count of metrics
	cl := basicCluster(hostAddr, []string{hostAddr})
	host := cluster.NewSimpleHost(cl.Hosts[0], cluster.NewCluster(cl).Snapshot().ClusterInfo())

	// if user configure this to -1, then retry is unlimited
	if connectTryTimes == -1 {
		connectTryTimes = math.MaxInt32
	}

	p := &connpool{
		host:                   host,
		heartBeatCreator:       heartBeatCreator,
		autoReconnectWhenClose: autoReconnectWhenClose,
		connTryTimes:           connectTryTimes,
		readFilters:            readFilters,
	}

	p.initActiveClient()

	return p
}

func (p *connpool) isActive() (*types.CreateConnectionData, bool) {
	// if pool was destroyed
	if atomic.LoadUint64(&p.destroyed) == 1 {
		return nil, false
	}

	cli := p.client
	// use atomic to load pointer, avoid partial pointer read
	if cli == nil {
		return nil, false
	}

	h := cli.getConnData()
	if h != nil && h.Connection.State() != api.ConnActive {
		return h, false
	}
	return h, true
}

// write to client
func (p *connpool) Write(buf ...buffer.IoBuffer) error {
	if h, ok := p.isActive(); ok {
		return h.Connection.Write(buf...)
	}
	return errors.New("[msgconnpool] connection not ready")
}

// Available current available to send request
func (p *connpool) Available() bool {
	_, avail := p.isActive()
	return avail
}

func basicCluster(name string, hosts []string) v2.Cluster {
	var vhosts []v2.Host
	for _, addr := range hosts {
		vhosts = append(vhosts, v2.Host{
			HostConfig: v2.HostConfig{
				Address: addr,
			},
		})
	}
	return v2.Cluster{
		Name:        name,
		ClusterType: v2.SIMPLE_CLUSTER,
		LbType:      v2.LB_ROUNDROBIN,
		Hosts:       vhosts,
	}
}