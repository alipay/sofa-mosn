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

package config

import (
	"fmt"

	"github.com/alipay/sofa-mosn/pkg/api/v2"
	"github.com/alipay/sofa-mosn/pkg/filter"
	"github.com/alipay/sofa-mosn/pkg/log"
	"github.com/alipay/sofa-mosn/pkg/server"
	"github.com/alipay/sofa-mosn/pkg/types"
	clusterAdapter "github.com/alipay/sofa-mosn/pkg/upstream/cluster"
	pb "github.com/envoyproxy/go-control-plane/envoy/api/v2"
)

// OnAddOrUpdateListeners called by XdsClient when listeners config refresh
func (config *MOSNConfig) OnAddOrUpdateListeners(listeners []*pb.Listener) error {
	for _, listener := range listeners {
		mosnListener := convertListenerConfig(listener)
		if mosnListener == nil {
			continue
		}

		var streamFilters []types.StreamFilterChainFactory
		var networkFilters []types.NetworkFilterChainFactory

		if !mosnListener.HandOffRestoredDestinationConnections {
			for _, filterChain := range mosnListener.FilterChains {
				for _, f := range filterChain.Filters {
					nfcf, err := filter.CreateNetworkFilterChainFactory(f.Name, f.Config)
					if err != nil {
						log.DefaultLogger.Errorf("parse network filter failed %v", err)
						return err
					}
					networkFilters = append(networkFilters, nfcf)
				}
			}

			streamFilters = GetStreamFilters(mosnListener.StreamFilters)

			if len(networkFilters) == 0 {
				errMsg := "xds client update listener error: proxy needed in network filters"
				log.DefaultLogger.Errorf(errMsg)
				return fmt.Errorf(errMsg)
			}
		}

		if listenerAdapter := server.GetListenerAdapterInstance(); listenerAdapter == nil {
			return fmt.Errorf("listenerAdapter is nil and hasn't been initiated at this time")
		} else {
			if err := listenerAdapter.AddOrUpdateListener("", mosnListener, networkFilters, streamFilters); err == nil {
				log.DefaultLogger.Debugf("xds AddOrUpdateListener success,listener address = %s", mosnListener.Addr.String())
			} else {
				log.DefaultLogger.Errorf("xds AddOrUpdateListener failure,listener address = %s, mag = %s ",
					mosnListener.Addr.String(), err.Error())
				return err
			}
		}
	}

	return nil
}

func (config *MOSNConfig) OnDeleteListeners(listeners []*pb.Listener) error {
	for _, listener := range listeners {
		mosnListener := convertListenerConfig(listener)
		if mosnListener == nil {
			continue
		}

		if listenerAdapter := server.GetListenerAdapterInstance(); listenerAdapter == nil {
			return fmt.Errorf("listenerAdapter is nil and hasn't been initiated at this time")
		} else {
			if err := listenerAdapter.DeleteListener("", mosnListener.Name); err == nil {
				log.DefaultLogger.Debugf("xds OnDeleteListeners success,listener address = %s", mosnListener.Addr.String())
			} else {
				log.DefaultLogger.Errorf("xds OnDeleteListeners failure,listener address = %s, mag = %s ",
					mosnListener.Addr.String(), err.Error())
				return err
			}
		}
	}

	return nil
}

// OnUpdateClusters called by XdsClient when clusters config refresh
// Can be used to update and add clusters
func (config *MOSNConfig) OnUpdateClusters(clusters []*pb.Cluster) error {
	mosnClusters := convertClustersConfig(clusters)

	for _, cluster := range mosnClusters {
		log.DefaultLogger.Debugf("cluster: %+v\n", cluster)
		var err error
		if cluster.ClusterType == v2.EDS_CLUSTER {
			err = clusterAdapter.GetClusterMngAdapterInstance().TriggerClusterAddOrUpdate(*cluster)
		} else {
			err = clusterAdapter.GetClusterMngAdapterInstance().TriggerClusterAndHostsAddOrUpdate(*cluster, cluster.Hosts)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// OnDeleteClusters called by XdsClient when need to delete clusters
func (config *MOSNConfig) OnDeleteClusters(clusters []*pb.Cluster) error {
	mosnClusters := convertClustersConfig(clusters)

	for _, cluster := range mosnClusters {
		log.DefaultLogger.Debugf("delete cluster: %+v\n", cluster)
		var err error
		if cluster.ClusterType == v2.EDS_CLUSTER {
			err = clusterAdapter.GetClusterMngAdapterInstance().TriggerClusterDel(cluster.Name)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// OnUpdateEndpoints called by XdsClient when ClusterLoadAssignment config refresh
func (config *MOSNConfig) OnUpdateEndpoints(loadAssignments []*pb.ClusterLoadAssignment) error {

	for _, loadAssignment := range loadAssignments {
		clusterName := loadAssignment.ClusterName

		for _, endpoints := range loadAssignment.Endpoints {
			hosts := convertEndpointsConfig(&endpoints)

			for _, host := range hosts {
				log.DefaultLogger.Debugf("xds client update endpoint: cluster: %s, priority: %d, %+v\n", loadAssignment.ClusterName, endpoints.Priority, host)
			}

			if err := clusterAdapter.GetClusterMngAdapterInstance().TriggerClusterHostUpdate(clusterName, hosts); err != nil {
				log.DefaultLogger.Errorf("xds client update Error = %s", err.Error())
				return err
			}
			log.DefaultLogger.Debugf("xds client update host success")

		}
	}

	return nil
}
