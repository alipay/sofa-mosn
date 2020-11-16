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

package http2

import (
	"context"
	"sync"
	"sync/atomic"

	"mosn.io/api"
	mosnctx "mosn.io/mosn/pkg/context"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/network"
	"mosn.io/mosn/pkg/protocol"
	str "mosn.io/mosn/pkg/stream"
	"mosn.io/mosn/pkg/types"
)

func init() {
	network.RegisterNewPoolFactory(protocol.HTTP2, NewConnPool)
	types.RegisterConnPoolFactory(protocol.HTTP2, true)
}

// ConnPool activeClient used as connected client
// host is the upstream
type ConnPool struct {
	activeClient *activeClient
	host         atomic.Value
	tlsHash      *types.HashValue

	mux sync.Mutex
}

// NewConnPool newConnPool
func NewConnPool(ctx context.Context, host types.Host) types.ConnectionPool {
	pool := &ConnPool{
		tlsHash: host.TLSHashValue(),
	}
	pool.host.Store(host)
	return pool
}

// TLSHashValue return tlsHash
func (p *ConnPool) TLSHashValue() *types.HashValue {
	return p.tlsHash
}

// Protocol return protocol (Http2)
func (p *ConnPool) Protocol() types.ProtocolName {
	return protocol.HTTP2
}

// Host return host
func (p *ConnPool) Host() types.Host {
	h := p.host.Load()
	if host, ok := h.(types.Host); ok {
		return host
	}

	return nil
}

// UpdateHost updateHost
func (p *ConnPool) UpdateHost(h types.Host) {
	p.host.Store(h)
}

// CheckAndInit always return true
func (p *ConnPool) CheckAndInit(ctx context.Context) bool {
	return true
}

// NewStream new stream
func (p *ConnPool) NewStream(ctx context.Context, responseDecoder types.StreamReceiveListener) (types.Host, types.StreamSender, types.PoolFailureReason) {
	activeClient := func() *activeClient {
		p.mux.Lock()
		defer p.mux.Unlock()
		if p.activeClient != nil && atomic.LoadUint32(&p.activeClient.goaway) == 1 {
			p.activeClient = nil
		}
		if p.activeClient == nil {
			p.activeClient = newActiveClient(ctx, p)
		}
		return p.activeClient
	}()

	host := p.Host()
	if activeClient == nil {
		return host, nil, types.ConnectionFailure
	}

	if !host.ClusterInfo().ResourceManager().Requests().CanCreate() {
		host.HostStats().UpstreamRequestPendingOverflow.Inc(1)
		host.ClusterInfo().Stats().UpstreamRequestPendingOverflow.Inc(1)
		return host, nil, types.Overflow
	}

	atomic.AddUint64(&activeClient.totalStream, 1)
	host.HostStats().UpstreamRequestTotal.Inc(1)
	host.HostStats().UpstreamRequestActive.Inc(1)
	host.ClusterInfo().Stats().UpstreamRequestTotal.Inc(1)
	host.ClusterInfo().Stats().UpstreamRequestActive.Inc(1)
	host.ClusterInfo().ResourceManager().Requests().Increase()
	streamEncoder := activeClient.client.NewStream(ctx, responseDecoder)
	streamEncoder.GetStream().AddEventListener(activeClient)

	return host, streamEncoder, ""
}

// Close close activeClient
func (p *ConnPool) Close() {
	activeClient := p.activeClient
	if activeClient != nil {
		activeClient.client.Close()
	}
}
// Shutdown http2 connpool do nothing for shutdown
func (p *ConnPool) Shutdown() {
	//TODO: http2 connpool do nothing for shutdown
}

func (p *ConnPool) onConnectionEvent(client *activeClient, event api.ConnectionEvent) {
	// event.ConnectFailure() contains types.ConnectTimeout and types.ConnectTimeout
	log.DefaultLogger.Debugf("http2 connPool onConnectionEvent: %v", event)
	host := p.Host()
	if event.IsClose() {
		if client.closeWithActiveReq {
			if event == api.LocalClose {
				host.HostStats().UpstreamConnectionLocalCloseWithActiveRequest.Inc(1)
				host.ClusterInfo().Stats().UpstreamConnectionLocalCloseWithActiveRequest.Inc(1)
			} else if event == api.RemoteClose {
				host.HostStats().UpstreamConnectionRemoteCloseWithActiveRequest.Inc(1)
				host.ClusterInfo().Stats().UpstreamConnectionRemoteCloseWithActiveRequest.Inc(1)
			}
		}
		if atomic.LoadUint32(&client.goaway) == 1 {
			return
		}
		p.mux.Lock()
		p.activeClient = nil
		p.mux.Unlock()
	} else if event == api.ConnectTimeout {
		host.HostStats().UpstreamRequestTimeout.Inc(1)
		host.ClusterInfo().Stats().UpstreamRequestTimeout.Inc(1)
	} else if event == api.ConnectFailed {
		host.HostStats().UpstreamConnectionConFail.Inc(1)
		host.ClusterInfo().Stats().UpstreamConnectionConFail.Inc(1)
	}
}

func (p *ConnPool) onStreamDestroy(client *activeClient) {
	host := p.Host()
	host.HostStats().UpstreamRequestActive.Dec(1)
	host.ClusterInfo().Stats().UpstreamRequestActive.Dec(1)
	host.ClusterInfo().ResourceManager().Requests().Decrease()
}

func (p *ConnPool) onStreamReset(client *activeClient, reason types.StreamResetReason) {
	host := p.Host()
	if reason == types.StreamConnectionTermination || reason == types.StreamConnectionFailed {
		host.HostStats().UpstreamRequestFailureEject.Inc(1)
		host.ClusterInfo().Stats().UpstreamRequestFailureEject.Inc(1)
		client.closeWithActiveReq = true
	} else if reason == types.StreamLocalReset {
		host.HostStats().UpstreamRequestLocalReset.Inc(1)
		host.ClusterInfo().Stats().UpstreamRequestLocalReset.Inc(1)
	} else if reason == types.StreamRemoteReset {
		host.HostStats().UpstreamRequestRemoteReset.Inc(1)
		host.ClusterInfo().Stats().UpstreamRequestRemoteReset.Inc(1)
	}
}

func (p *ConnPool) createStreamClient(context context.Context, connData types.CreateConnectionData) str.Client {
	return str.NewStreamClient(context, protocol.HTTP2, connData.Connection, connData.Host)
}

// types.StreamEventListener
// types.ConnectionEventListener
// types.StreamConnectionEventListener
type activeClient struct {
	pool               *ConnPool
	client             str.Client
	host               types.CreateConnectionData
	closeWithActiveReq bool
	totalStream        uint64
	goaway             uint32
}

func newActiveClient(ctx context.Context, pool *ConnPool) *activeClient {
	ac := &activeClient{
		pool: pool,
	}

	host := pool.Host()
	data := host.CreateConnection(ctx)
	data.Connection.AddConnectionEventListener(ac)
	if err := data.Connection.Connect(); err != nil {
		log.DefaultLogger.Debugf("http2 underlying connection error: %v", err)
		return nil
	}

	connCtx := mosnctx.WithValue(ctx, types.ContextKeyConnectionID, data.Connection.ID())
	codecClient := pool.createStreamClient(connCtx, data)
	codecClient.SetStreamConnectionEventListener(ac)

	ac.client = codecClient
	ac.host = data

	host.HostStats().UpstreamConnectionTotal.Inc(1)
	host.HostStats().UpstreamConnectionActive.Inc(1)
	host.ClusterInfo().Stats().UpstreamConnectionTotal.Inc(1)
	host.ClusterInfo().Stats().UpstreamConnectionActive.Inc(1)

	// bytes total adds all connections data together, but buffered data not
	codecClient.SetConnectionCollector(host.ClusterInfo().Stats().UpstreamBytesReadTotal, host.ClusterInfo().Stats().UpstreamBytesWriteTotal)

	return ac
}

func (ac *activeClient) OnEvent(event api.ConnectionEvent) {
	ac.pool.onConnectionEvent(ac, event)
}

// types.StreamEventListener
func (ac *activeClient) OnDestroyStream() {
	ac.pool.onStreamDestroy(ac)
}

func (ac *activeClient) OnResetStream(reason types.StreamResetReason) {
	ac.pool.onStreamReset(ac, reason)
}

// types.StreamConnectionEventListener
func (ac *activeClient) OnGoAway() {
	atomic.StoreUint32(&ac.goaway, 1)
}
