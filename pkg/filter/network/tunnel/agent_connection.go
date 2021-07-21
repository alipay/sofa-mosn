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
	"sync"

	"mosn.io/api"
	"mosn.io/mosn/pkg/types"
)

var _ types.ClientConnection = (*agentConnection)(nil)

// agentConnection is a implementation for ClientConnection, provides a mechanism to bind an existing api.Connection
type agentConnection struct {
	api.Connection
	once sync.Once
}

// Connect is a fake operation, it only checks the status of the bound connection
// and overrides the Connect operation of the api.Connection
func (cc *agentConnection) Connect() (err error) {
	if cc.State() == api.ConnClosed {
		return errors.New("tunnel channel has been closed")
	}
	cc.once.Do(func() {
		cc.OnConnectionEvent(api.Connected)
	})
	return nil
}

func CreateTunnelAgentConnection(conn api.Connection) *agentConnection {
	return &agentConnection{
		Connection: conn,
	}
}
