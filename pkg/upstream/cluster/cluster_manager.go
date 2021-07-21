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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	v2 "mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/configmanager"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/mtls"
	"mosn.io/mosn/pkg/network"
	"mosn.io/mosn/pkg/types"
)

var errNilCluster = errors.New("cannot update nil cluster")

// refreshHostsConfig refresh the stored config for admin api
func refreshHostsConfig(c types.Cluster) {
	// use new cluster snapshot to get new cluster config
	name := c.Snapshot().ClusterInfo().Name()
	hosts := c.Snapshot().HostSet().Hosts()
	hostsConfig := make([]v2.Host, 0, len(hosts))
	for _, h := range hosts {
		hostsConfig = append(hostsConfig, h.Config())
	}
	configmanager.SetHosts(name, hostsConfig)
	if log.DefaultLogger.GetLogLevel() >= log.INFO {
		log.DefaultLogger.Infof("[cluster] [primaryCluster] [UpdateHosts] cluster %s update hosts: %d", name, len(hosts))
	}
	if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
		log.DefaultLogger.Debugf("[cluster] [primaryCluster] [UpdateHosts] cluster %s update hosts: %v", name, hosts)
	}
}

const globalTLSMetrics = "global"

// types.ClusterManager
type clusterManager struct {
	clustersMap      sync.Map
	protocolConnPool sync.Map
	tlsMetrics       *mtls.TLSStats
	tlsMng           atomic.Value // store types.TLSClientContextManager
	mux              sync.Mutex
}

type clusterManagerSingleton struct {
	instanceMutex sync.Mutex
	*clusterManager
}

func (singleton *clusterManagerSingleton) Destroy() {
	clusterManagerInstance.instanceMutex.Lock()
	defer clusterManagerInstance.instanceMutex.Unlock()
	clusterManagerInstance.clusterManager = nil
}

var clusterManagerInstance = &clusterManagerSingleton{}

func NewClusterManagerSingleton(clusters []v2.Cluster, clusterMap map[string][]v2.Host, tls *v2.TLSConfig) types.ClusterManager {
	clusterManagerInstance.instanceMutex.Lock()
	defer clusterManagerInstance.instanceMutex.Unlock()
	if clusterManagerInstance.clusterManager != nil {
		return clusterManagerInstance
	}

	clusterManagerInstance.clusterManager = &clusterManager{
		tlsMetrics: mtls.NewStats(globalTLSMetrics),
	}
	// set global tls
	clusterManagerInstance.clusterManager.UpdateTLSManager(tls)
	// add conn pool
	for k := range types.ConnPoolFactories {
		clusterManagerInstance.protocolConnPool.Store(k, &sync.Map{})
	}

	//Add cluster to cm
	for _, cluster := range clusters {
		if err := clusterManagerInstance.AddOrUpdatePrimaryCluster(cluster); err != nil {
			log.DefaultLogger.Alertf("cluster.config", "[upstream] [cluster manager] NewClusterManager: AddOrUpdatePrimaryCluster failure, cluster name = %s, error: %v", cluster.Name, err)
		}
	}
	// Add cluster host
	for clusterName, hosts := range clusterMap {
		if err := clusterManagerInstance.UpdateClusterHosts(clusterName, hosts); err != nil {
			log.DefaultLogger.Alertf("cluster.config", "[upstream] [cluster manager] NewClusterManager: UpdateClusterHosts failure, cluster name = %s, error: %v", clusterName, err)
		}
	}
	return clusterManagerInstance
}


// AddOrUpdatePrimaryCluster will always create a new cluster without the hosts config
// if the same name cluster is already exists, we will keep the exists hosts.
func (cm *clusterManager) AddOrUpdatePrimaryCluster(cluster v2.Cluster) error {
	// new cluster
	newCluster := NewCluster(cluster)
	if newCluster == nil || reflect.ValueOf(newCluster).IsNil() {
		log.DefaultLogger.Errorf("[cluster] [cluster manager] [AddOrUpdatePrimaryCluster] update cluster %s failed", cluster.Name)
		return errNilCluster
	}
	// check update or new
	clusterName := cluster.Name
	// set config
	configmanager.SetClusterConfig(cluster)
	// add or update
	ci, exists := cm.clustersMap.Load(clusterName)
	if exists {
		c := ci.(types.Cluster)
		hosts := c.Snapshot().HostSet().Hosts()

		newSnap := newCluster.Snapshot()

		oldResourceManager := c.Snapshot().ClusterInfo().ResourceManager()
		newResourceManager := newSnap.ClusterInfo().ResourceManager()

		// sync oldResourceManager to new cluster ResourceManager
		updateClusterResourceManager(newSnap.ClusterInfo(), oldResourceManager)

		// sync newResourceManager value to oldResourceManager value
		updateResourceValue(oldResourceManager, newResourceManager)
		for _, host := range hosts {
			host.SetClusterInfo(newSnap.ClusterInfo()) // update host cluster info
		}

		// update hosts, refresh
		newCluster.UpdateHosts(hosts)
		refreshHostsConfig(c)
	}
	cm.clustersMap.Store(clusterName, newCluster)
	if log.DefaultLogger.GetLogLevel() >= log.INFO {
		log.DefaultLogger.Infof("[cluster] [cluster manager] [AddOrUpdatePrimaryCluster] cluster %s updated", clusterName)
	}
	return nil
}

// AddClusterHealthCheckCallbacks adds a health check callback function into cluster
func (cm *clusterManager) AddClusterHealthCheckCallbacks(name string, cb types.HealthCheckCb) error {
	ci, ok := cm.clustersMap.Load(name)
	if ok {
		c := ci.(types.Cluster)
		c.AddHealthCheckCallbacks(cb)
		return nil
	}
	return fmt.Errorf("cluster %s is not exists", name)
}

func (cm *clusterManager) ClusterExist(clusterName string) bool {
	_, ok := cm.clustersMap.Load(clusterName)
	return ok
}

// RemovePrimaryCluster removes clusters from cluster manager
// If the cluster is more than one, all of them should be exists, or no one will be deleted
func (cm *clusterManager) RemovePrimaryCluster(clusterNames ...string) error {
	// check all clutsers in cluster manager
	for _, clusterName := range clusterNames {
		if _, ok := cm.clustersMap.Load(clusterName); !ok {
			log.DefaultLogger.Errorf("[upstream] [cluster manager] Remove Primary Cluster,  cluster %s not exists", clusterName)
			return fmt.Errorf("remove cluster failed, cluster %s is not exists", clusterName)
		}
	}
	// delete all of them
	for _, clusterName := range clusterNames {
		v, ok := cm.clustersMap.Load(clusterName)
		if !ok {
			// In theory there's still a chance that cluster was just removed by another routine after the upper for-loop check.
			continue
		}
		c := v.(types.Cluster)
		c.StopHealthChecking()

		cm.clustersMap.Delete(clusterName)
		configmanager.SetRemoveClusterConfig(clusterName)
		if log.DefaultLogger.GetLogLevel() >= log.INFO {
			log.DefaultLogger.Infof("[upstream] [cluster manager] Remove Primary Cluster, Cluster Name = %s", clusterName)
		}
	}
	return nil
}

// UpdateClusterHosts update all hosts in the cluster
func (cm *clusterManager) UpdateClusterHosts(clusterName string, hostConfigs []v2.Host) error {
	ci, ok := cm.clustersMap.Load(clusterName)
	if !ok {
		log.DefaultLogger.Errorf("[upstream] [cluster manager] UpdateClusterHosts cluster %s not found", clusterName)
		return fmt.Errorf("cluster %s is not exists", clusterName)
	}
	c := ci.(types.Cluster)
	snap := c.Snapshot()
	hosts := make([]types.Host, 0, len(hostConfigs))
	for _, hc := range hostConfigs {
		hosts = append(hosts, NewSimpleHost(hc, snap.ClusterInfo()))
	}
	c.UpdateHosts(hosts)
	refreshHostsConfig(c)
	return nil
}

// AppendClusterHosts adds new hosts into cluster
func (cm *clusterManager) AppendClusterHosts(clusterName string, hostConfigs []v2.Host) error {
	ci, ok := cm.clustersMap.Load(clusterName)
	if !ok {
		log.DefaultLogger.Errorf("[upstream] [cluster manager] AppendClusterHosts cluster %s not found", clusterName)
		return fmt.Errorf("cluster %s is not exists", clusterName)
	}
	c := ci.(types.Cluster)
	snap := c.Snapshot()
	hosts := make([]types.Host, 0, len(hostConfigs))
	for _, hc := range hostConfigs {
		hosts = append(hosts, NewSimpleHost(hc, snap.ClusterInfo()))
	}
	hosts = append(hosts, snap.HostSet().Hosts()...)
	c.UpdateHosts(hosts)
	refreshHostsConfig(c)
	return nil
}

// AppendClusterTypesHosts used to adds new hosts into cluster by passing `types.Host`
func (cm *clusterManager) AppendClusterTypesHosts(clusterName string, typesHost []types.Host) error {
	ci, ok := cm.clustersMap.Load(clusterName)
	if !ok {
		log.DefaultLogger.Errorf("[upstream] [cluster manager] AppendClusterHosts cluster %s not found", clusterName)
		return fmt.Errorf("cluster %s is not exists", clusterName)
	}
	c := ci.(types.Cluster)
	snap := c.Snapshot()
	hosts := append(snap.HostSet().Hosts(), typesHost...)
	c.UpdateHosts(hosts)
	refreshHostsConfig(c)
	return nil
}

// RemoveClusterHosts removes hosts from cluster by address string
func (cm *clusterManager) RemoveClusterHosts(clusterName string, addrs []string) error {
	ci, ok := cm.clustersMap.Load(clusterName)
	if !ok {
		log.DefaultLogger.Errorf("[upstream] [cluster manager] RemoveClusterHosts cluster %s not found", clusterName)
		return fmt.Errorf("cluster %s is not exists", clusterName)
	}
	c := ci.(types.Cluster)
	snap := c.Snapshot()
	hosts := snap.HostSet().Hosts()
	newHosts := make([]types.Host, len(hosts))
	copy(newHosts, hosts)
	sortedHosts := types.SortedHosts(newHosts)
	sort.Sort(sortedHosts)
	for _, addr := range addrs {
		i := sort.Search(sortedHosts.Len(), func(i int) bool {
			return sortedHosts[i].AddressString() >= addr
		})
		// found it, delete it
		if i < sortedHosts.Len() && sortedHosts[i].AddressString() == addr {
			sortedHosts = append(sortedHosts[:i], sortedHosts[i+1:]...)
		}
	}
	c.UpdateHosts(sortedHosts)
	refreshHostsConfig(c)
	return nil
}

// GetClusterSnapshot returns cluster snap
func (cm *clusterManager) GetClusterSnapshot(ctx context.Context, clusterName string) types.ClusterSnapshot {
	ci, ok := cm.clustersMap.Load(clusterName)
	if !ok {
		return nil
	}
	return ci.(types.Cluster).Snapshot()
}

func (cm *clusterManager) PutClusterSnapshot(snap types.ClusterSnapshot) {
}

func (cm *clusterManager) TCPConnForCluster(lbCtx types.LoadBalancerContext, snapshot types.ClusterSnapshot) types.CreateConnectionData {
	if snapshot == nil || reflect.ValueOf(snapshot).IsNil() {
		return types.CreateConnectionData{}
	}
	host := snapshot.LoadBalancer().ChooseHost(lbCtx)
	if host == nil {
		return types.CreateConnectionData{}
	}
	return host.CreateConnection(context.Background())
}

func (cm *clusterManager) UDPConnForCluster(lbCtx types.LoadBalancerContext, snapshot types.ClusterSnapshot) types.CreateConnectionData {
	if snapshot == nil || reflect.ValueOf(snapshot).IsNil() {
		return types.CreateConnectionData{}
	}
	host := snapshot.LoadBalancer().ChooseHost(lbCtx)
	if host == nil {
		return types.CreateConnectionData{}
	}
	return host.CreateUDPConnection(context.Background())
}

func (cm *clusterManager) ConnPoolForCluster(balancerContext types.LoadBalancerContext, snapshot types.ClusterSnapshot, protocol types.ProtocolName) (types.ConnectionPool, types.Host) {
	if snapshot == nil || reflect.ValueOf(snapshot).IsNil() {
		log.DefaultLogger.Errorf("[upstream] [cluster manager]  %s ConnPool For Cluster is nil", protocol)
		return nil, nil
	}
	pool, host, err := cm.getActiveConnectionPool(balancerContext, snapshot, protocol)
	if err != nil {
		log.DefaultLogger.Errorf("[upstream] [cluster manager] ConnPoolForCluster Failed; %v", err)
	}
	return pool, host
}

func (cm *clusterManager) GetTLSManager() types.TLSClientContextManager {
	v := cm.tlsMng.Load()
	tlsmng, _ := v.(types.TLSClientContextManager)
	return tlsmng
}

func (cm *clusterManager) UpdateTLSManager(tls *v2.TLSConfig) {
	if tls == nil {
		tls = &v2.TLSConfig{} // use a disabled config instead
	}
	mng, err := mtls.NewTLSClientContextManager(tls)
	if err != nil {
		log.DefaultLogger.Alertf("cluster.config", "[upstream] [cluster manager] NewClusterManager: Add TLS Manager failed, error: %v", err)
		return
	}
	cm.tlsMng.Store(mng)
	configmanager.SetClusterManagerTLS(*tls)
}

const (
	maxHostsCounts  = 3
	intervalStep    = 5
	maxTryConnTimes = 15
)

var tryConnTimes = [maxTryConnTimes]time.Duration{}

func init() {
	index := 0
	// total duration is 535ms
	for _, interval := range []time.Duration{
		2 * time.Millisecond,
		5 * time.Millisecond,
		100 * time.Millisecond,
	} {
		for i := 0; i < intervalStep; i++ {
			tryConnTimes[index] = interval
			index++
		}
	}
}

var (
	errNilHostChoose   = errors.New("cluster snapshot choose host is nil")
	errUnknownProtocol = errors.New("protocol pool can not found protocol")
	errNoHealthyHost   = errors.New("no health hosts")
)

func (cm *clusterManager) getActiveConnectionPool(balancerContext types.LoadBalancerContext, clusterSnapshot types.ClusterSnapshot, protocol types.ProtocolName) (types.ConnectionPool, types.Host, error) {
	factory, ok := network.ConnNewPoolFactories[protocol]
	if !ok {
		return nil, nil, fmt.Errorf("protocol %v is not registered is pool factory", protocol)
	}

	// for pool to know, whether this is a multiplex or pingpong pool
	var (
		pools [maxHostsCounts]types.ConnectionPool
		hosts [maxHostsCounts]types.Host
	)

	try := clusterSnapshot.HostNum(balancerContext.MetadataMatchCriteria())
	if try == 0 {
		return nil, nil, errNilHostChoose
	}
	if try > maxHostsCounts {
		try = maxHostsCounts
	}
	for i := 0; i < try; i++ {
		host := clusterSnapshot.LoadBalancer().ChooseHost(balancerContext)
		if host == nil {
			return nil, nil, errNilHostChoose
		}

		addr := host.AddressString()
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[upstream] [cluster manager] clusterSnapshot.loadbalancer.ChooseHost result is %s, cluster name = %s", addr, clusterSnapshot.ClusterInfo().Name())
		}
		value, ok := cm.protocolConnPool.Load(protocol)
		if !ok {
			return nil, nil, errUnknownProtocol
		}

		connectionPool := value.(*sync.Map)
		// we cannot use sync.Map.LoadOrStore directly, becasue we do not want to new a connpool every time
		loadOrStoreConnPool := func() (types.ConnectionPool, bool) {
			// avoid locking if it is already exists
			if connPool, ok := connectionPool.Load(addr); ok {
				pool := connPool.(types.ConnectionPool)
				return pool, true
			}
			cm.mux.Lock()
			defer cm.mux.Unlock()
			if connPool, ok := connectionPool.Load(addr); ok {
				pool := connPool.(types.ConnectionPool)
				return pool, true
			}
			pool := factory(balancerContext.DownstreamContext(), host)
			connectionPool.Store(addr, pool)
			return pool, false
		}
		pool, loaded := loadOrStoreConnPool()
		if loaded {
			if !pool.TLSHashValue().Equal(host.TLSHashValue()) {
				if log.DefaultLogger.GetLogLevel() >= log.INFO {
					log.DefaultLogger.Infof("[upstream] [cluster manager] %s tls state changed", addr)
				}
				func() {
					// lock the load and delete
					cm.mux.Lock()
					defer cm.mux.Unlock()
					// recheck whether the pool is changed
					if connPool, ok := connectionPool.Load(addr); ok {
						pool = connPool.(types.ConnectionPool)
						if pool.TLSHashValue().Equal(host.TLSHashValue()) {
							return
						}
						connectionPool.Delete(addr)
						pool.Shutdown()
						pool = factory(balancerContext.DownstreamContext(), host)
						connectionPool.Store(addr, pool)
						cm.tlsMetrics.TLSConnpoolChanged.Inc(1)
					}
				}()

			}
		}
		if pool.CheckAndInit(balancerContext.DownstreamContext()) {
			return pool, host, nil
		}
		pools[i] = pool
		hosts[i] = host
	}

	// perhaps the first request, wait for tcp handshaking.
	for t := 0; t < maxTryConnTimes; t++ {
		for i := 0; i < try; i++ {
			if pools[i] == nil {
				continue
			}
			if pools[i].CheckAndInit(balancerContext.DownstreamContext()) {
				return pools[i], hosts[i], nil
			}
		}
		waitTime := tryConnTimes[t]
		time.Sleep(waitTime)
	}
	return nil, nil, errNoHealthyHost
}

func (cm *clusterManager) ShutdownConnectionPool(proto types.ProtocolName, addr string) {
	shutdown := func(value interface{}) {
		connectionPool := value.(*sync.Map)
		if connPool, ok := connectionPool.Load(addr); ok {
			pool := connPool.(types.ConnectionPool)
			connectionPool.Delete(addr)
			pool.Shutdown()
			if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
				log.DefaultLogger.Debugf("[upstream] [cluster manager] protocol %s address %s connections shutdown", proto, addr)
			}
		}
	}
	if proto == "" {
		cm.protocolConnPool.Range(func(_, value interface{}) bool {
			shutdown(value)
			return true
		})
	} else {
		value, ok := cm.protocolConnPool.Load(proto)
		if !ok {
			if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
				log.DefaultLogger.Debugf("[upstream] [cluster manager] unknown protocol when shutdown, protocol:%s, address: %s", proto, addr)
			}
			return
		}
		shutdown(value)
	}
}
