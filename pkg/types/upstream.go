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

package types

import (
	"context"
	"net"
	"sort"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"mosn.io/api"
	v2 "mosn.io/mosn/pkg/config/v2"
)

//   Below is the basic relation between clusterManager, cluster, hostSet, and hosts:
//
//           1              * | 1                          1 | 1          *
//   clusterManager --------- cluster  --------- --------- hostSet------hosts

// ClusterManager manages connection pools and load balancing for upstream clusters.
type ClusterManager interface {
	// Add or update a cluster via API.
	AddOrUpdatePrimaryCluster(cluster v2.Cluster) error

	// Add Cluster health check callbacks
	AddClusterHealthCheckCallbacks(name string, cb HealthCheckCb) error

	// Get, use to get the snapshot of a cluster
	GetClusterSnapshot(context context.Context, cluster string) ClusterSnapshot

	// Deprecated: PutClusterSnapshot exists for historical compatibility and should not be used.
	PutClusterSnapshot(ClusterSnapshot)

	// UpdateClusterHosts used to update cluster's hosts
	// temp interface todo: remove it
	UpdateClusterHosts(cluster string, hosts []v2.Host) error

	// AppendClusterHosts used to add cluster's hosts
	AppendClusterHosts(clusterName string, hostConfigs []v2.Host) error

	// AppendClusterTypesHosts used to add cluster's by passing the `types.Host` type
	AppendClusterTypesHosts(clusterName string, typesHost []Host) error

	// Get or Create tcp conn pool for a cluster
	TCPConnForCluster(balancerContext LoadBalancerContext, snapshot ClusterSnapshot) CreateConnectionData

	// Get or Create tcp conn pool for a cluster
	UDPConnForCluster(balancerContext LoadBalancerContext, snapshot ClusterSnapshot) CreateConnectionData

	// ConnPoolForCluster used to get protocol related conn pool
	ConnPoolForCluster(balancerContext LoadBalancerContext, snapshot ClusterSnapshot, protocol api.ProtocolName) (ConnectionPool, Host)

	// RemovePrimaryCluster used to remove cluster from set
	RemovePrimaryCluster(clusters ...string) error

	// ClusterExist, used to check whether 'clusterName' exist or not
	ClusterExist(clusterName string) bool

	// RemoveClusterHosts, remove the host by address string
	RemoveClusterHosts(clusterName string, hosts []string) error

	// TLSManager is used to cluster tls config
	GetTLSManager() TLSClientContextManager
	// UpdateTLSManager updates the tls manager which is used to cluster tls config
	UpdateTLSManager(*v2.TLSConfig)

	// ShutdownConnectionPool shutdown the connection pool by address and ProtocolName
	// If ProtocolName is not specified, remove the addr's connection pool of all protocols
	ShutdownConnectionPool(proto ProtocolName, addr string)

	// Destroy the cluster manager
	Destroy()
}

// ClusterSnapshot is a thread-safe cluster snapshot
type ClusterSnapshot interface {
	// HostSet returns the cluster snapshot's host set
	HostSet() HostSet

	// ClusterInfo returns the cluster snapshot's cluster info
	ClusterInfo() ClusterInfo

	// LoadBalancer returns the cluster snapshot's load balancer
	LoadBalancer() LoadBalancer

	// IsExistsHosts checks whether the metadata's subset contains host or not
	// if metadata is nil, check the cluster snapshot contains host or not
	IsExistsHosts(metadata api.MetadataMatchCriteria) bool

	HostNum(metadata api.MetadataMatchCriteria) int
}

// Cluster is a group of upstream hosts
type Cluster interface {
	// Snapshot returns the cluster snapshot, which contains cluster info, hostset and load balancer
	Snapshot() ClusterSnapshot

	// UpdateHosts updates the host set's hosts
	UpdateHosts([]Host)

	// Add health check callbacks in health checker
	AddHealthCheckCallbacks(cb HealthCheckCb)

	// Shutdown the healthcheck routine, if exists
	StopHealthChecking()
}

// HostPredicate checks wether the host is matched the metadata
type HostPredicate func(Host) bool

// HostSet is as set of hosts that contains all of the endpoints for a given
type HostSet interface {
	// Hosts returns all hosts that make up the set at the current time.
	Hosts() []Host
}

// Host is an upstream host
type Host interface {
	api.HostInfo

	// HostStats returns the host stats metrics
	HostStats() HostStats

	// ClusterInfo returns the cluster info
	ClusterInfo() ClusterInfo
	// SetClusterInfo updates the host's cluster info
	SetClusterInfo(info ClusterInfo)

	// TLSHashValue TLS HashValue effects the host support tls state
	TLSHashValue() *HashValue
	// CreateConnection a connection for this host.
	CreateConnection(context context.Context) CreateConnectionData

	// CreateUDPConnection a udp connection for this host.
	CreateUDPConnection(context context.Context) CreateConnectionData

	// Address returns the host's Addr structure
	Address() net.Addr
	// Config creates a host config by the host attributes
	Config() v2.Host
}

// ClusterInfo defines a cluster's information
type ClusterInfo interface {
	// Name returns the cluster name
	Name() string

	// ClusterType returns the cluster type
	ClusterType() v2.ClusterType

	// LbType returns the cluster's load balancer type
	LbType() LoadBalancerType

	// ConnBufferLimitBytes returns the connection buffer limits
	ConnBufferLimitBytes() uint32

	// MaxRequestsPerConn returns a connection's max request
	MaxRequestsPerConn() uint32

	// Stats returns the cluster's stats metrics
	Stats() ClusterStats

	// ResourceManager returns the ResourceManager
	ResourceManager() ResourceManager

	// TLSMng returns the tls manager
	TLSMng() TLSClientContextManager

	// LbSubsetInfo returns the load balancer subset's config
	LbSubsetInfo() LBSubsetInfo

	// ConectTimeout returns the connect timeout
	ConnectTimeout() time.Duration

	// LbOriDstInfo returns the load balancer oridst config
	LbOriDstInfo() LBOriDstInfo

	// Optional configuration for the load balancing algorithm selected by
	LbConfig() v2.IsCluster_LbConfig

	//  Optional configuration for some cluster description
	SubType() string
}

// ResourceManager manages different types of Resource
type ResourceManager interface {
	// Connections resource to count connections in pool. Only used by protocol which has a connection pool which has multiple connections.
	Connections() Resource

	// Pending request resource to count pending requests. Only used by protocol which has a connection pool and pending requests to assign to connections.
	PendingRequests() Resource

	// Request resource to count requests
	Requests() Resource

	// Retries resource to count retries
	Retries() Resource
}

// Resource is a interface to statistics information
type Resource interface {
	CanCreate() bool
	Increase()
	Decrease()
	Max() uint64
	Cur() int64
	UpdateCur(int64)
}

// HostStats defines a host's statistics information
type HostStats struct {
	UpstreamConnectionTotal                        metrics.Counter
	UpstreamConnectionClose                        metrics.Counter
	UpstreamConnectionActive                       metrics.Counter
	UpstreamConnectionConFail                      metrics.Counter
	UpstreamConnectionLocalClose                   metrics.Counter
	UpstreamConnectionRemoteClose                  metrics.Counter
	UpstreamConnectionLocalCloseWithActiveRequest  metrics.Counter
	UpstreamConnectionRemoteCloseWithActiveRequest metrics.Counter
	UpstreamConnectionCloseNotify                  metrics.Counter
	UpstreamRequestTotal                           metrics.Counter
	UpstreamRequestActive                          metrics.Counter
	UpstreamRequestLocalReset                      metrics.Counter
	UpstreamRequestRemoteReset                     metrics.Counter
	UpstreamRequestTimeout                         metrics.Counter
	UpstreamRequestFailureEject                    metrics.Counter
	UpstreamRequestPendingOverflow                 metrics.Counter
	UpstreamRequestDuration                        metrics.Histogram
	UpstreamRequestDurationTotal                   metrics.Counter
	UpstreamResponseSuccess                        metrics.Counter
	UpstreamResponseFailed                         metrics.Counter
}

// ClusterStats defines a cluster's statistics information
type ClusterStats struct {
	UpstreamConnectionTotal                        metrics.Counter
	UpstreamConnectionClose                        metrics.Counter
	UpstreamConnectionActive                       metrics.Counter
	UpstreamConnectionConFail                      metrics.Counter
	UpstreamConnectionRetry                        metrics.Counter
	UpstreamConnectionLocalClose                   metrics.Counter
	UpstreamConnectionRemoteClose                  metrics.Counter
	UpstreamConnectionLocalCloseWithActiveRequest  metrics.Counter
	UpstreamConnectionRemoteCloseWithActiveRequest metrics.Counter
	UpstreamConnectionCloseNotify                  metrics.Counter
	UpstreamBytesReadTotal                         metrics.Counter
	UpstreamBytesWriteTotal                        metrics.Counter
	UpstreamRequestTotal                           metrics.Counter
	UpstreamRequestActive                          metrics.Counter
	UpstreamRequestLocalReset                      metrics.Counter
	UpstreamRequestRemoteReset                     metrics.Counter
	UpstreamRequestRetry                           metrics.Counter
	UpstreamRequestRetryOverflow                   metrics.Counter
	UpstreamRequestTimeout                         metrics.Counter
	UpstreamRequestFailureEject                    metrics.Counter
	UpstreamRequestPendingOverflow                 metrics.Counter
	UpstreamRequestDuration                        metrics.Histogram
	UpstreamRequestDurationTotal                   metrics.Counter
	UpstreamResponseSuccess                        metrics.Counter
	UpstreamResponseFailed                         metrics.Counter
	LBSubSetsFallBack                              metrics.Counter
	LBSubsetsCreated                               metrics.Gauge
}

type CreateConnectionData struct {
	Connection ClientConnection
	Host       Host
}

// SimpleCluster is a simple cluster in memory
type SimpleCluster interface {
	UpdateHosts(newHosts []Host)
}

// ClusterConfigFactoryCb is a callback interface
type ClusterConfigFactoryCb interface {
	UpdateClusterConfig(configs []v2.Cluster) error
}

type ClusterHostFactoryCb interface {
	UpdateClusterHost(cluster string, hosts []v2.Host) error
}

type ClusterManagerFilter interface {
	OnCreated(cccb ClusterConfigFactoryCb, chcb ClusterHostFactoryCb)
}

// RegisterUpstreamUpdateMethodCb is a callback interface
type RegisterUpstreamUpdateMethodCb interface {
	TriggerClusterUpdate(clusterName string, hosts []v2.Host)
	GetClusterNameByServiceName(serviceName string) string
}

type LBSubsetInfo interface {
	// IsEnabled represents whether the subset load balancer is configured or not
	IsEnabled() bool

	// FallbackPolicy returns the fallback policy
	FallbackPolicy() FallBackPolicy

	// DefaultSubset returns the default subset's metadata configure
	// it takes effects when the fallback policy is default subset
	DefaultSubset() SubsetMetadata

	// SubsetKeys returns the sorted subset keys
	SubsetKeys() []SortedStringSetType
}

type LBOriDstInfo interface {
	// Check use host header
	IsEnabled() bool

	// GET header name
	GetHeader() string
}

// SortedHosts is an implementation of sort.Interface
// a slice of host can be sorted as address string
type SortedHosts []Host

func (s SortedHosts) Len() int {
	return len(s)
}

func (s SortedHosts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortedHosts) Less(i, j int) bool {
	return s[i].AddressString() < s[j].AddressString()
}

// SortedStringSetType is a sorted key collection with no duplicate
type SortedStringSetType struct {
	keys []string
}

// InitSet returns a SortedStringSetType
// The input key will be sorted and deduplicated
func InitSet(input []string) SortedStringSetType {
	var ssst SortedStringSetType
	var keys []string

	for _, keyInput := range input {
		exist := false

		for _, keyIn := range keys {
			if keyIn == keyInput {
				exist = true
				break
			}
		}

		if !exist {
			keys = append(keys, keyInput)
		}
	}
	ssst.keys = keys
	sort.Sort(&ssst)

	return ssst
}

// Keys is the keys in the collection
func (ss *SortedStringSetType) Keys() []string {
	return ss.keys
}

// Len is the number of elements in the collection.
func (ss *SortedStringSetType) Len() int {
	return len(ss.keys)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (ss *SortedStringSetType) Less(i, j int) bool {
	return ss.keys[i] < ss.keys[j]
}

// Swap swaps the elements with indexes i and j.
func (ss *SortedStringSetType) Swap(i, j int) {
	ss.keys[i], ss.keys[j] = ss.keys[j], ss.keys[i]
}

func init() {
	ConnPoolFactories = make(map[api.ProtocolName]bool)
}

var ConnPoolFactories map[api.ProtocolName]bool

func RegisterConnPoolFactory(protocol api.ProtocolName, registered bool) {
	//other
	ConnPoolFactories[protocol] = registered
}
