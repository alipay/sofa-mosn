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

	"mosn.io/api"
	v2 "mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/filter/network/tunnel/ext"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/types"
)

var _ api.ReadFilter = (*tunnelFilter)(nil)

// tunnelFilter handles the reverse connection sent by the edge node，
// mainly used in scenarios where cloud nodes and edge nodes may be located in separate and isolated network region.
type tunnelFilter struct {
	clusterManager types.ClusterManager
	readCallbacks  api.ReadFilterCallbacks
	// connInitialized is used as a flag to be processed only once
	connInitialized bool
}

func (t *tunnelFilter) OnData(buffer api.IoBuffer) api.FilterStatus {
	if t.connInitialized {
		// hand it over to the next filter directly
		return api.Continue
	}
	if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
		log.DefaultLogger.Debugf("[tunnel server] [ondata] read data , len: %v", buffer.Len())
	}
	data, err := DecodeFromBuffer(buffer)
	if data == nil && err == nil {
		// Not enough data was read.
		return api.Stop
	}
	if err != nil {
		log.DefaultLogger.Errorf("[tunnel server] [ondata] failed to read from buffer, close connection err: %+v", err)
		t.readCallbacks.Connection().Close(api.NoFlush, api.OnReadErrClose)
		return api.Stop
	}

	return t.handleRequest(data)
}

func (t *tunnelFilter) handleRequest(request interface{}) api.FilterStatus {
	switch request.(type) {
	case *ConnectionInitInfo:
		return t.handleConnectionInit(request.(*ConnectionInitInfo))
	case *GracefulCloseOnewayRequest:
		return t.handleGracefulClose(request.(*GracefulCloseOnewayRequest))
	}
	return api.Continue
}

func (t *tunnelFilter) handleConnectionInit(info *ConnectionInitInfo) api.FilterStatus {
	// Auth the connection
	conn := t.readCallbacks.Connection()
	if info.CredentialPolicy != "" {
		validator := ext.GetConnectionValidator(info.CredentialPolicy)
		if validator == nil {
			writeConnectResponse(ConnectValidatorNotFound, conn)
			return api.Stop
		}
		res := validator.Validate(info.Credential, info.HostName, info.ClusterName)
		if !res {
			writeConnectResponse(ConnectAuthFailed, conn)
			return api.Stop
		}
	}
	if !t.clusterManager.ClusterExist(info.ClusterName) {
		writeConnectResponse(ConnectClusterNotExist, conn)
		return api.Stop
	}
	// Set the flag that has been initialized, subsequent data processing skips this filter
	err := writeConnectResponse(ConnectSuccess, conn)
	if err != nil {
		return api.Stop
	}
	conn.AddConnectionEventListener(NewHostRemover(conn.RemoteAddr().String(), info.ClusterName))
	tunnelHostMutex.Lock()
	defer tunnelHostMutex.Unlock()
	snapshot := t.clusterManager.GetClusterSnapshot(context.Background(), info.ClusterName)
	_ = t.clusterManager.AppendClusterTypesHosts(info.ClusterName, []types.Host{NewTunnelHost(v2.Host{
		HostConfig: v2.HostConfig{
			Address:    conn.RemoteAddr().String(),
			Hostname:   info.HostName,
			Weight:     uint32(info.Weight),
			TLSDisable: false,
		}}, snapshot.ClusterInfo(), CreateTunnelAgentConnection(conn))})
	t.connInitialized = true
	return api.Stop
}

func (t *tunnelFilter) handleGracefulClose(request *GracefulCloseOnewayRequest) api.FilterStatus {
	addresses := request.Addresses
	for _, address := range addresses {
		removeHost(address, request.ClusterName)
	}
	return api.Stop
}

func (t *tunnelFilter) OnNewConnection() api.FilterStatus {
	return api.Stop
}

func (t *tunnelFilter) InitializeReadFilterCallbacks(cb api.ReadFilterCallbacks) {
	t.readCallbacks = cb
}

func writeConnectResponse(status ConnectStatus, conn api.Connection) error {
	log.DefaultLogger.Debugf("[tunnel server] try to write response, connect status: %v", status)
	buffer, err := Encode(&ConnectionInitResponse{Status: status})
	if err != nil {
		log.DefaultLogger.Errorf("[tunnel server] failed to encode response, err: %+v", err)
		conn.Close(api.NoFlush, api.LocalClose)
		return err
	}
	err = conn.Write(buffer)
	if err != nil {
		log.DefaultLogger.Errorf("[tunnel server] failed to write response, err: %+v", err)
		conn.Close(api.NoFlush, api.OnWriteErrClose)
		return err
	}
	return nil
}
