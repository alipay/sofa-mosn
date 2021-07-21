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
	"context"
	"net"

	v2 "mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/types"
	"mosn.io/mosn/pkg/upstream/cluster"
)

var _ types.Host = (*host)(nil)

type host struct {
	types.Host
	conn types.ClientConnection
}

func (t host) AddressString() string {
	return t.conn.RemoteAddr().String()
}

func (t host) CreateConnection(context context.Context) types.CreateConnectionData {
	return types.CreateConnectionData{
		Connection: t.conn,
		Host:       t.Host,
	}
}

func (t host) CreateUDPConnection(context context.Context) types.CreateConnectionData {
	return types.CreateConnectionData{
		Connection: t.conn,
		Host:       t.Host,
	}
}

func (t host) Address() net.Addr {
	return t.conn.RemoteAddr()
}

func NewTunnelHost(config v2.Host, clusterInfo types.ClusterInfo, connection types.ClientConnection) *host {
	return &host{
		Host: cluster.NewSimpleHost(config, clusterInfo),
		conn: connection,
	}
}
