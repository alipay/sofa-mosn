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
	"reflect"
	"sort"

	"mosn.io/api"
	v2 "mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/types"
)

type subsetLoadBalancer struct {
	lbType         types.LoadBalancerType
	stats          types.ClusterStats
	subSets        types.LbSubsetMap  // final trie-like structure used to stored easily searched subset
	fallbackSubset *LBSubsetEntryImpl // subset entry generated according to fallback policy
	hostSet        types.HostSet
	fullLb         types.LoadBalancer // a loadbalancer for all hosts
}

func NewSubsetLoadBalancer(info types.ClusterInfo, hostSet types.HostSet) types.LoadBalancer {
	subsetInfo := info.LbSubsetInfo()
	subsetLB := &subsetLoadBalancer{
		lbType:  info.LbType(),
		stats:   info.Stats(),
		subSets: make(map[string]types.ValueSubsetMap),
		hostSet: hostSet,
		fullLb:  NewLoadBalancer(info, hostSet),
	}
	// create fallback
	subsetLB.createFallbackSubset(info, subsetInfo.FallbackPolicy(), subsetInfo.DefaultSubset())
	// create subsets
	subsetLB.createSubsets(info, subsetInfo.SubsetKeys())
	return subsetLB
}

func (sslb *subsetLoadBalancer) ChooseHost(ctx types.LoadBalancerContext) types.Host {
	if ctx != nil {
		host, hostChoosen := sslb.tryChooseHostFromContext(ctx)
		// if a subset's hosts are all deleted, it will return a nil host and a true flag
		if hostChoosen && host != nil {
			if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
				log.DefaultLogger.Debugf("[upstream] [subset lb] subset load balancer: match subset entry success, "+
					"choose hostaddr = %s", host.AddressString())
			}
			return host
		}
	}
	if sslb.fallbackSubset == nil {
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[upstream] [subset lb] subset load balancer: failure, fallback subset is nil")
		}
		return nil
	}
	sslb.stats.LBSubSetsFallBack.Inc(1)
	return sslb.fallbackSubset.LoadBalancer().ChooseHost(ctx)
}

// if metadata is nil, use all hosts as results
func (sslb *subsetLoadBalancer) IsExistsHosts(metadata api.MetadataMatchCriteria) bool {
	if metadata == nil || reflect.ValueOf(metadata).IsNil() {
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[upstream] [subset lb] subset load balancer: metadata match criteria is nil")
		}
		return sslb.fullLb.IsExistsHosts(metadata)
	}
	matchCriteria := metadata.MetadataMatchCriteria()
	entry := sslb.findSubset(matchCriteria)
	empty := (entry == nil || !entry.Active())
	return !empty
}

// if metadata is nil, use all hosts as results
func (sslb *subsetLoadBalancer) HostNum(metadata api.MetadataMatchCriteria) int {
	if metadata == nil || reflect.ValueOf(metadata).IsNil() {
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[upstream] [subset lb] subset load balancer: metadata match criteria is nil")
		}
		return sslb.fullLb.HostNum(metadata)
	}
	matchCriteria := metadata.MetadataMatchCriteria()
	entry := sslb.findSubset(matchCriteria)
	if entry == nil {
		return 0
	}
	return entry.HostNum()
}

// if metadata is nil, use all hosts as results
func (sslb *subsetLoadBalancer) tryChooseHostFromContext(ctx types.LoadBalancerContext) (types.Host, bool) {
	metadata := ctx.MetadataMatchCriteria()
	if metadata == nil || reflect.ValueOf(metadata).IsNil() {
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[upstream] [subset lb] subset load balancer: metadata match criteria is nil")
		}
		return sslb.fullLb.ChooseHost(ctx), true
	}
	matchCriteria := metadata.MetadataMatchCriteria()
	entry := sslb.findSubset(matchCriteria)
	if entry == nil || !entry.Active() {
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[upstream] [subset lb] subset load balancer: match entry failure")
		}
		return nil, false
	}
	return entry.LoadBalancer().ChooseHost(ctx), true
}

// createSubsets creates the sslb.subSets
func (sslb *subsetLoadBalancer) createSubsets(info types.ClusterInfo, subSetKeys []types.SortedStringSetType) {
	hosts := sslb.hostSet.Hosts()
	var subsSetCount int64 = 0
	for _, host := range hosts {
		for _, subSetKey := range subSetKeys {
			// one keys will create one subset
			kvs := ExtractSubsetMetadata(subSetKey.Keys(), host.Metadata())
			if len(kvs) > 0 {
				entry := sslb.findOrCreateSubset(sslb.subSets, kvs, 0)
				if !entry.Initialized() {
					subHostset := CreateSubset(sslb.hostSet.Hosts(), func(host types.Host) bool {
						return HostMatches(kvs, host)
					})
					subsSetCount += 1
					entry.CreateLoadBalancer(info, subHostset)
				}
			}
		}
	}
	sslb.stats.LBSubsetsCreated.Update(subsSetCount)
}

// createFallbackSubset creates a LBSubsetEntryImpl as fallbackSubset
func (sslb *subsetLoadBalancer) createFallbackSubset(info types.ClusterInfo, policy types.FallBackPolicy, meta types.SubsetMetadata) {
	hostSet := sslb.hostSet
	switch policy {
	case types.NoFallBack:
		if log.DefaultLogger.GetLogLevel() >= log.DEBUG {
			log.DefaultLogger.Debugf("[upstream] [subset lb] subset load balancer: fallback is disabled")
		}
		return
	case types.AnyEndPoint:
		sslb.fallbackSubset = &LBSubsetEntryImpl{
			children: nil,         // no child
			lb:       sslb.fullLb, // reuse the full loadbalancer
			hostSet:  hostSet,
		}
	case types.DefaultSubset:
		sslb.fallbackSubset = &LBSubsetEntryImpl{
			children: nil, // no child
		}
		subHostset := CreateSubset(sslb.hostSet.Hosts(), func(host types.Host) bool {
			return HostMatches(meta, host)
		})
		sslb.fallbackSubset.CreateLoadBalancer(info, subHostset)
	}
}

func (sslb *subsetLoadBalancer) findSubset(matchCriteria []api.MetadataMatchCriterion) types.LBSubsetEntry {
	subSets := sslb.subSets
	for i, mcCriterion := range matchCriteria {
		vsMap, ok := subSets[mcCriterion.MetadataKeyName()]
		if !ok {
			return nil
		}
		vsEntry, ok := vsMap[mcCriterion.MetadataValue()]
		if !ok {
			return nil
		}
		if i+1 == len(matchCriteria) {
			return vsEntry
		}
		subSets = vsEntry.Children()
	}
	return nil
}

func (sslb *subsetLoadBalancer) findOrCreateSubset(subsets types.LbSubsetMap, kvs types.SubsetMetadata, idx uint32) types.LBSubsetEntry {
	name := kvs[idx].T1
	value := kvs[idx].T2
	var entry types.LBSubsetEntry

	if vsMap, ok := subsets[name]; ok {
		lbEntry, ok := vsMap[value]
		if !ok {
			lbEntry = &LBSubsetEntryImpl{
				children: make(map[string]types.ValueSubsetMap),
			}
			vsMap[value] = lbEntry
			subsets[name] = vsMap
		}
		entry = lbEntry
	} else {
		entry = &LBSubsetEntryImpl{
			children: make(map[string]types.ValueSubsetMap),
		}
		subsets[name] = types.ValueSubsetMap{
			value: entry,
		}
	}
	idx++
	if idx == uint32(len(kvs)) {
		return entry
	}
	return sslb.findOrCreateSubset(entry.Children(), kvs, idx)
}

// if subsetKeys are all contained in the host metadata
func ExtractSubsetMetadata(subsetKeys []string, metadata api.Metadata) types.SubsetMetadata {
	var kvs types.SubsetMetadata
	for _, key := range subsetKeys {
		value, ok := metadata[key]
		if !ok {
			return nil
		}
		kvs = append(kvs, types.Pair{
			T1: key,
			T2: value,
		})
	}
	return kvs
}

func CreateSubset(hosts []types.Host, predicate types.HostPredicate) types.HostSet {
	var subHosts []types.Host
	for _, h := range hosts {
		if predicate(h) {
			subHosts = append(subHosts, h)
		}
	}
	sub := &hostSet{}
	sub.setFinalHost(subHosts)
	return sub
}

func HostMatches(kvs types.SubsetMetadata, host types.Host) bool {
	meta := host.Metadata()
	for _, kv := range kvs {
		value, ok := meta[kv.T1]
		if !ok || value != kv.T2 {
			return false
		}
	}
	return true
}

type LBSubsetEntryImpl struct {
	children types.LbSubsetMap
	hostSet  types.HostSet
	lb       types.LoadBalancer
}

func (entry *LBSubsetEntryImpl) Initialized() bool {
	return entry.lb != nil
}

func (entry *LBSubsetEntryImpl) Active() bool {
	return entry.hostSet != nil && len(entry.hostSet.Hosts()) != 0
}

func (entry *LBSubsetEntryImpl) HostNum() int {
	if entry.hostSet != nil {
		return len(entry.hostSet.Hosts())
	}
	return 0
}

func (entry *LBSubsetEntryImpl) Children() types.LbSubsetMap {
	return entry.children
}

func (entry *LBSubsetEntryImpl) CreateLoadBalancer(info types.ClusterInfo, hosts types.HostSet) {
	lb := NewLoadBalancer(info, hosts)
	entry.lb = lb
	entry.hostSet = hosts
}

func (entry *LBSubsetEntryImpl) LoadBalancer() types.LoadBalancer {
	return entry.lb
}

type LBSubsetInfoImpl struct {
	enabled        bool
	fallbackPolicy types.FallBackPolicy
	defaultSubSet  types.SubsetMetadata
	subSetKeys     []types.SortedStringSetType // sorted subset selectors
}

func (info *LBSubsetInfoImpl) IsEnabled() bool {
	return info.enabled
}

func (info *LBSubsetInfoImpl) FallbackPolicy() types.FallBackPolicy {
	return info.fallbackPolicy
}

func (info *LBSubsetInfoImpl) DefaultSubset() types.SubsetMetadata {
	return info.defaultSubSet
}

func (info *LBSubsetInfoImpl) SubsetKeys() []types.SortedStringSetType {
	return info.subSetKeys
}

func NewLBSubsetInfo(subsetCfg *v2.LBSubsetConfig) types.LBSubsetInfo {
	lbSubsetInfo := &LBSubsetInfoImpl{
		fallbackPolicy: types.FallBackPolicy(subsetCfg.FallBackPolicy),
		subSetKeys:     GenerateSubsetKeys(subsetCfg.SubsetSelectors),
		defaultSubSet:  make(types.SubsetMetadata, 0, len(subsetCfg.DefaultSubset)),
		enabled:        len(subsetCfg.SubsetSelectors) != 0,
	}
	keys := make([]string, 0, len(subsetCfg.DefaultSubset))
	for key := range subsetCfg.DefaultSubset {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		lbSubsetInfo.defaultSubSet = append(lbSubsetInfo.defaultSubSet, types.Pair{
			T1: key,
			T2: subsetCfg.DefaultSubset[key],
		})
	}
	return lbSubsetInfo
}

func GenerateSubsetKeys(keysArray [][]string) []types.SortedStringSetType {
	var subSetKeys []types.SortedStringSetType

	for _, keys := range keysArray {
		sortedStringSet := types.InitSet(keys)
		dup := false
		for _, subset := range subSetKeys {
			// sorted keys can compare directly
			if reflect.DeepEqual(sortedStringSet.Keys(), subset.Keys()) {
				dup = true
			}
		}
		if !dup {
			subSetKeys = append(subSetKeys, sortedStringSet)
		}
	}

	return subSetKeys
}
