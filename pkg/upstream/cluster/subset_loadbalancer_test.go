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
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"mosn.io/api"
	"mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/types"
)

// we test following cases form envoy's example
// see https://github.com/envoyproxy/envoy/blob/master/source/docs/subset_load_balancer.md
// Host List
/*
| host  | stage | version |     type     | xlarge
| e1    | prod  | 1.0     |     std      | true
| e2    | prod  | 1.0     |     std
| e3    | prod  | 1.1     |     std
| e4    | prod  | 1.1     |     std
| e5    | prod  | 1.0     |     bigmem
| e6    | prod  | 1.1     |     bigmem
| e7    | dev   | 1.2-pre |     std
*/
func exampleHostConfigs() (hosts []v2.Host) {
	metaDatas := []map[string]string{
		map[string]string{
			"stage":   "prod",
			"version": "1.0",
			"type":    "std",
			"xlarge":  "true", // value should be string, not a bool
		},
		map[string]string{
			"stage":   "prod",
			"version": "1.0",
			"type":    "std",
		},
		map[string]string{
			"stage":   "prod",
			"version": "1.1",
			"type":    "std",
		},
		map[string]string{
			"stage":   "prod",
			"version": "1.1",
			"type":    "std",
		},
		map[string]string{
			"stage":   "prod",
			"version": "1.0",
			"type":    "bigmem",
		},
		map[string]string{
			"stage":   "prod",
			"version": "1.1",
			"type":    "bigmem",
		},
		map[string]string{
			"stage":   "dev",
			"version": "1.2-pre",
			"type":    "std",
		},
	}
	for i, md := range metaDatas {
		hostName := fmt.Sprintf("e%d", i+1)
		addr := fmt.Sprintf("127.0.0.1:808%d", i+1)
		cfg := v2.Host{
			HostConfig: v2.HostConfig{
				Hostname: hostName,
				Address:  addr,
				Weight:   uint32(i) + 1,
			},
			MetaData: md,
		}
		hosts = append(hosts, cfg)
	}
	return
}

func createHostset(cfg []v2.Host) *hostSet {
	// clear healthy flag
	healthStore = sync.Map{}
	// create
	var hosts []types.Host
	for _, h := range cfg {
		host := &mockHost{
			name: h.Hostname,
			addr: h.Address,
			meta: h.MetaData,
		}
		hosts = append(hosts, host)
	}
	hs := &hostSet{}
	hs.setFinalHost(hosts)
	return hs
}
func createHostsetWithStats(cfg []v2.Host, clusterName string) *hostSet {
	var hosts []types.Host
	for _, h := range cfg {
		host := &mockHost{
			name: h.Hostname,
			addr: h.Address,
			meta: h.MetaData,
			w:    h.Weight,
		}
		host.stats = newHostStats(clusterName, host.addr)
		hosts = append(hosts, host)
	}
	hs := &hostSet{}
	hs.setFinalHost(hosts)
	return hs
}

// Selector Config
/*
"subset_selectors": [
      { "keys": [ "stage", "type" ] },
      { "keys": [ "stage", "version" ] },
      { "keys": [ "version" ] },
      { "keys": [ "xlarge", "version" ] },
    ]
*/
// mosn's config format is different from envoy
func exampleSubsetConfig() *v2.LBSubsetConfig {
	return &v2.LBSubsetConfig{
		SubsetSelectors: [][]string{
			[]string{
				"stage", "type",
			},
			[]string{
				"stage", "version",
			},
			[]string{
				"version",
			},
			[]string{
				"xlarge", "version",
			},
		},
	}
}

// LbSubsetMap Result, show as string chain
// stage->dev->type->std->[e7]
// stage->dev->version->1.2-pre->[e7]
// stage->prod->type->std->[e1,e2,e3,e4]
// stage->prod->type->bigmem->[e5,e6]
// stage->prod->version->1.0->[e1,e2,e5]
// stage->prod->version->1.1->[e3,e4,e6]
// version->1.0->[e1,e2,e5]
// version->1.0->xlarge->true->[e1]
// version->1.1->[e3,e4,e6]
// version->1.2-pre->[e7]
var exampleResult = map[string][]string{
	"stage->dev->type->std->":        []string{"e7"},
	"stage->dev->version->1.2-pre->": []string{"e7"},
	"stage->prod->type->std->":       []string{"e1", "e2", "e3", "e4"},
	"stage->prod->type->bigmem->":    []string{"e5", "e6"},
	"stage->prod->version->1.0->":    []string{"e1", "e2", "e5"},
	"stage->prod->version->1.1->":    []string{"e3", "e4", "e6"},
	"version->1.0->":                 []string{"e1", "e2", "e5"},
	"version->1.0->xlarge->true->":   []string{"e1"},
	"version->1.1->":                 []string{"e3", "e4", "e6"},
	"version->1.2-pre->":             []string{"e7"},
}

func resultMapEqual(m1, m2 map[string][]string) bool {
	if len(m1) != len(m2) {
		return false
	}
	for key, node := range m1 {
		node2, ok := m2[key]
		if !ok {
			return false
		}
		if !strSliceEqual(node, node2) {
			return false
		}
	}
	return true
}

func strSliceEqual(s1, s2 []string) bool {
	sort.Strings(s1)
	sort.Strings(s2)
	return reflect.DeepEqual(s1, s2)
}

func strInSlice(s string, list []string) bool {
	for _, ls := range list {
		if s == ls {
			return true
		}
	}
	return false
}

type subSetMapResult struct {
	// key is prefix like: stage->dev->type->std->
	// value is hostname's string like: [e7]
	result map[string][]string
}

// parse a LbSubsetMap as a readable map
func (r *subSetMapResult) RangeSubsetMap(prefix string, subsetMap types.LbSubsetMap) {
	for key, value := range subsetMap {
		for v, entry := range value {
			p := prefix + key + "->" + string(v) + "->"
			if entry.Children() != nil {
				r.RangeSubsetMap(p, entry.Children())
			}
			if entry.Initialized() {
				e := entry.(*LBSubsetEntryImpl)
				hosts := e.hostSet.Hosts()
				hostsNode := []string{}
				for _, h := range hosts {
					hostsNode = append(hostsNode, h.Hostname())
				}
				r.result[p] = hostsNode
			}
		}
	}
}

func newSubsetLoadBalancer(lbType types.LoadBalancerType, hosts *hostSet, stats types.ClusterStats, subsets types.LBSubsetInfo) *subsetLoadBalancer {
	info := &clusterInfo{
		lbType:       lbType,
		stats:        stats,
		lbSubsetInfo: subsets,
	}
	lb := NewSubsetLoadBalancer(info, hosts)
	return lb.(*subsetLoadBalancer)
}

// create a subset as expected, see example
func TestNewSubsetLoadBalancer(t *testing.T) {
	ps := createHostset(exampleHostConfigs())
	lb := newSubsetLoadBalancer(types.RoundRobin, ps, newClusterStats("TestNewSubsetLoadBalancer"), NewLBSubsetInfo(exampleSubsetConfig()))
	subSet := lb.subSets
	result := &subSetMapResult{
		result: map[string][]string{},
	}
	result.RangeSubsetMap("", subSet)
	if !resultMapEqual(result.result, exampleResult) {
		t.Errorf("subset tree created is not expected, %v", result.result)
	}
}

// MetadataMatchCriteria should exactly matches subset
// case1: stage:prod, version:1.0 shoud find e1,e2,e5
// case2: stage:prod: should find nil
func TestNewSubsetChooseHost(t *testing.T) {
	ps := createHostset(exampleHostConfigs())
	lb := newSubsetLoadBalancer(types.RoundRobin, ps, newClusterStats("TestNewSubsetChooseHost"), NewLBSubsetInfo(exampleSubsetConfig()))
	ctx1 := newMockLbContext(map[string]string{
		"stage":   "prod",
		"version": "1.0",
	})
	ctx2 := newMockLbContext(map[string]string{
		"stage": "prod",
	})
	h, ok := lb.tryChooseHostFromContext(ctx1)
	if !ok || h == nil {
		t.Fatal("choose host failed, expected success")
	}
	switch h.Hostname() {
	case "e1", "e2", "e5":
	default:
		t.Fatal("host found, but not the expected subset", h.Hostname())
	}
	if h, ok := lb.tryChooseHostFromContext(ctx2); ok || h != nil {
		t.Fatalf("expected choose failed, but returns a host, host: %v, ok: %v", h, ok)
	}
}

// If selectors not configured the host label, the host will not be put in any subset
func TestNoSubsetHost(t *testing.T) {
	ps := createHostset(exampleHostConfigs())
	cfg := &v2.LBSubsetConfig{
		SubsetSelectors: [][]string{
			[]string{
				"xlarge", "version",
			},
		},
	}
	// only one host will put in subset (e1)
	// others cannot be found in subset even if version is matched
	lb := newSubsetLoadBalancer(types.RoundRobin, ps, newClusterStats("TestNoSubsetHost"), NewLBSubsetInfo(cfg))
	// found no host
	ctx1 := newMockLbContext(map[string]string{
		"version": "1.0",
	})
	// found host
	ctx2 := newMockLbContext(map[string]string{
		"version": "1.0",
		"xlarge":  "true",
	})
	// test HostNum
	if lb.HostNum(ctx1.MetadataMatchCriteria()) != 0 {
		t.Fatalf("expected hosts is 0")
	}
	if lb.HostNum(ctx2.MetadataMatchCriteria()) != 1 {
		t.Fatalf("expected hosts is 1")
	}
	if h, ok := lb.tryChooseHostFromContext(ctx1); ok || h != nil {
		t.Fatalf("expected choose failed, but returns a host, host: %v, ok: %v", h, ok)
	}
	for i := 0; i < 10; i++ {
		h, ok := lb.tryChooseHostFromContext(ctx2)
		if !ok || h == nil {
			t.Fatal("choose host failed, expected success")
		}
		if h.Hostname() != "e1" {
			t.Fatalf("host found not expected, got: %s", h.Hostname())
		}
	}
}

// TestFallbackWithDefaultSubset configure default subset as fallback
// if a ctx is not matched the subset, use the fallback instead
func TestFallbackWithDefaultSubset(t *testing.T) {
	ps := createHostset(exampleHostConfigs())
	// only create subset with version and xlarge
	// if not matched, use default subset: stage:dev
	cfg := &v2.LBSubsetConfig{
		FallBackPolicy: uint8(types.DefaultSubset),
		DefaultSubset: map[string]string{
			"stage": "dev", // only contain e7
		},
		SubsetSelectors: [][]string{
			[]string{
				"version", "xlarge",
			},
		},
	}
	// ctx1: version:1.0, xlarge: true. match the selector, find e1
	// ctx2: version:1.0, xlarge: false. not matched, find is fallback, e7
	// ctx3: version:1.2, xlarge: true. not matched, find is fallabck, e7
	// ctx4: stage: prod. not matched, find is fallback, e7
	// ctx5~7: nil(mmc is nil/no value). not matched, find is fallback e7
	lb := newSubsetLoadBalancer(types.RoundRobin, ps, newClusterStats("TestFallbackWithDefaultSubset"), NewLBSubsetInfo(cfg))
	testCases := []struct {
		ctx          types.LoadBalancerContext
		expectedHost string
	}{
		{
			ctx: newMockLbContext(map[string]string{
				"version": "1.0",
				"xlarge":  "true",
			}),
			expectedHost: "e1",
		},
		{
			ctx: newMockLbContext(map[string]string{
				"version": "1.0",
				"xlarge":  "false",
			}),
			expectedHost: "e7",
		},
		{
			ctx: newMockLbContext(map[string]string{
				"version": "1.2",
				"xlarge":  "true",
			}),
			expectedHost: "e7",
		},
		{
			ctx: newMockLbContext(map[string]string{
				"stage": "prod",
			}),
			expectedHost: "e7",
		},
		{
			ctx:          nil,
			expectedHost: "e7",
		},
		{
			ctx:          newMockLbContext(map[string]string{}),
			expectedHost: "e7",
		},
	}
	for i, tc := range testCases {
		h := lb.ChooseHost(tc.ctx)
		if h == nil {
			t.Errorf("#%d choose host failed", i)
			continue
		}
		if h.Hostname() != tc.expectedHost {
			t.Errorf("#%d choose host is not expected, expected %s, got %s", i, tc.expectedHost, h.Hostname())
		}
	}
}

// TestFallbackWithAllHosts configure all hosts as fallback, without default subset
func TestFallbackWithAllHosts(t *testing.T) {
	// fallback policy is any point
	// all host (no matter what lable it is) is used to fallback subset
	// for test simple, we use some simple host configs
	hosts := []v2.Host{
		{
			HostConfig: v2.HostConfig{
				Hostname: "host1",
				Address:  "127.0.0.1:8080",
			},
			MetaData: map[string]string{
				"zone": "zone0",
				"room": "room0",
			},
		},
		{
			HostConfig: v2.HostConfig{
				Hostname: "host2",
				Address:  "127.0.0.1:8081",
			},
			MetaData: map[string]string{
				"zone": "zone1",
			},
		},
		{
			HostConfig: v2.HostConfig{
				Hostname: "host3",
				Address:  "127.0.0.1:8082",
			},
			MetaData: map[string]string{
				"room": "room1",
			},
		},
	}
	ps := createHostset(hosts)
	// only create subset with version and xlarge
	// if not matched, use default subset: stage:dev
	cfg := &v2.LBSubsetConfig{
		FallBackPolicy: uint8(types.AnyEndPoint),
		SubsetSelectors: [][]string{
			[]string{
				"zone", "room",
			},
		},
	}
	//
	expectedResult := map[string][]string{
		"room->room0->zone->zone0->": []string{"host1"},
	}
	// New
	lb := newSubsetLoadBalancer(types.RoundRobin, ps, newClusterStats("TestFallbackWithAllHosts"), NewLBSubsetInfo(cfg))
	subSet := lb.subSets
	result := &subSetMapResult{
		result: map[string][]string{},
	}
	// Verify subset created
	result.RangeSubsetMap("", subSet)
	if !resultMapEqual(result.result, expectedResult) {
		t.Errorf("subset tree created is not expected, got: %v, expected: %v", result.result, expectedResult)
	}
	// choose host test
	testCases := []struct {
		ctx           types.LoadBalancerContext
		expectedHosts []string
	}{
		{
			ctx: newMockLbContext(map[string]string{
				"zone": "zone0",
				"room": "room0",
			}),
			expectedHosts: []string{
				"host1", // matched subset
			},
		},
		// fallback
		{
			ctx: newMockLbContext(map[string]string{
				"zone": "zone0",
			}),
			expectedHosts: []string{
				"host1", "host2", "host3",
			},
		},
		{
			ctx: newMockLbContext(map[string]string{
				"room": "room0",
			}),
			expectedHosts: []string{
				"host1", "host2", "host3",
			},
		},
		{
			ctx: newMockLbContext(map[string]string{
				"zone": "zone0",
				"room": "room1",
			}),
			expectedHosts: []string{
				"host1", "host2", "host3",
			},
		},
		{
			ctx: newMockLbContext(map[string]string{
				"zone": "zone1",
			}),
			expectedHosts: []string{
				"host1", "host2", "host3",
			},
		},
		{
			ctx: newMockLbContext(map[string]string{
				"room": "room1",
			}),
			expectedHosts: []string{
				"host1", "host2", "host3",
			},
		},
		{
			ctx: newMockLbContext(map[string]string{}),
			expectedHosts: []string{
				"host1", "host2", "host3",
			},
		},
		{
			ctx: nil,
			expectedHosts: []string{
				"host1", "host2", "host3",
			},
		},
		{
			ctx: newMockLbContext(nil),
			expectedHosts: []string{
				"host1", "host2", "host3",
			},
		},
	}
RUNCASE:
	for i, tc := range testCases {
		for j := 0; j < 3; j++ { // choose multi times
			h := lb.ChooseHost(tc.ctx)
			if h == nil {
				t.Errorf("#%d choose host failed", i)
				continue RUNCASE
			}
			if !strInSlice(h.Hostname(), tc.expectedHosts) {
				t.Errorf("#%d choose host not expected, expected: %v, got: %s", i, tc.expectedHosts, h.Hostname())
				continue RUNCASE
			}
		}
	}
}

// TestDynamicSubsetHost with subset
// If a new host with label is added, a new subset map will be created
// If a exists host label is changed, the host should be moved into new subset(maybe needs create a new one)
func TestDynamicSubsetHost(t *testing.T) {
	// use cluster manager to register dynamic host changed
	clusterName := "TestSubset"
	hostA := &mockHost{
		addr: "127.0.0.1:8080",
		name: "A",
		meta: api.Metadata{
			"zone":  "zone0",
			"group": "a",
		},
	}
	clusterConfig := v2.Cluster{
		Name:                 clusterName,
		ClusterType:          v2.SIMPLE_CLUSTER,
		LbType:               v2.LB_RANDOM,
		MaxRequestPerConn:    1024,
		ConnBufferLimitBytes: 1024,
		LBSubSetConfig: v2.LBSubsetConfig{
			FallBackPolicy: uint8(types.AnyEndPoint),
			SubsetSelectors: [][]string{
				[]string{
					"zone", "group",
				},
				[]string{
					"zone",
				},
			},
		},
	}
	cluster := newSimpleCluster(clusterConfig).(*simpleCluster)
	// create a subset
	{
		cluster.UpdateHosts([]types.Host{hostA})
		expectedResult := map[string][]string{
			"group->a->zone->zone0->": []string{"A"},
			"zone->zone0->":           []string{"A"},
		}
		result := &subSetMapResult{
			result: map[string][]string{},
		}
		lb := cluster.lbInstance.(*subsetLoadBalancer)
		result.RangeSubsetMap("", lb.subSets)
		if !resultMapEqual(result.result, expectedResult) {
			t.Fatal("create subset is not expected", result.result)
		}
		// try to choose host, found A
		ctx := newMockLbContext(map[string]string{
			"zone":  "zone0",
			"group": "a",
		})
		// no fallback choose
		if h, ok := lb.tryChooseHostFromContext(ctx); !ok || h == nil || h.Hostname() != "A" {
			t.Fatal("choose host not expected")
		}
	}
	// remove a host
	{
		cluster.UpdateHosts([]types.Host{})
		result := &subSetMapResult{
			result: map[string][]string{},
		}
		lb := cluster.lbInstance.(*subsetLoadBalancer)
		result.RangeSubsetMap("", lb.subSets)
		if len(result.result) != 0 {
			t.Fatal("create subset is not expected", result.result)
		}
		// try to choose host, found nil
		ctx := newMockLbContext(map[string]string{
			"zone":  "zone0",
			"group": "a",
		})
		// with fallback choose, also get a nil
		if h := lb.ChooseHost(ctx); h != nil {
			t.Fatal("choost host not expected")
		}
	}
	// add a new host with new label, create a new subset
	{
		hostB := &mockHost{
			addr: "127.0.0.1:8080",
			name: "B",
			meta: api.Metadata{
				"zone":  "zone0",
				"group": "b",
			},
		}
		cluster.UpdateHosts([]types.Host{hostB})
		expectedResult := map[string][]string{
			"zone->zone0->":           []string{"B"},
			"group->b->zone->zone0->": []string{"B"},
		}
		lb := cluster.lbInstance.(*subsetLoadBalancer)
		result := &subSetMapResult{
			result: map[string][]string{},
		}
		result.RangeSubsetMap("", lb.subSets)
		if !resultMapEqual(result.result, expectedResult) {
			t.Fatal("create subset is not expected", result.result)
		}
		// try to choose host
		ctx := newMockLbContext(map[string]string{
			"zone":  "zone0",
			"group": "a",
		})
		// no fallback, found nothing
		if h, ok := lb.tryChooseHostFromContext(ctx); ok || h != nil {
			t.Fatal("choost host not expected")
		}
		// with fallback, found host
		if h := lb.ChooseHost(ctx); h == nil || h.Hostname() != "B" {
			t.Fatal("choost host not expected")
		}
		ctx2 := newMockLbContext(map[string]string{
			"zone":  "zone0",
			"group": "b",
		})
		// no fallback, found host
		if h, ok := lb.tryChooseHostFromContext(ctx2); !ok || h == nil || h.Hostname() != "B" {
			t.Fatal("choose host not expected")
		}
	}
	// update label
	{
		hostB := &mockHost{
			addr: "127.0.0.1:8080",
			name: "B",
			meta: api.Metadata{
				"zone":  "zone0",
				"group": "a",
			},
		}
		cluster.UpdateHosts([]types.Host{hostB})
		expectedResult := map[string][]string{
			"zone->zone0->":           []string{"B"},
			"group->a->zone->zone0->": []string{"B"},
		}
		lb := cluster.lbInstance.(*subsetLoadBalancer)
		result := &subSetMapResult{
			result: map[string][]string{},
		}
		result.RangeSubsetMap("", lb.subSets)
		if !resultMapEqual(result.result, expectedResult) {
			t.Fatal("create subset is not expected", result.result)
		}
	}

}

func TestNoFallbackWithEmpty(t *testing.T) {
	// allback policy is no fallback
	// but empty meta data can be used as any point fallback
	ps := createHostset(exampleHostConfigs())
	cfg := exampleSubsetConfig()
	cfg.FallBackPolicy = uint8(types.NoFallBack) // NoFallBack
	lb := newSubsetLoadBalancer(types.RoundRobin, ps, newClusterStats("TestNewSubsetChooseHost"), NewLBSubsetInfo(cfg))

	cases := []struct {
		ctx   types.LoadBalancerContext
		hosts int
	}{
		{
			ctx:   newMockLbContext(map[string]string{"stage": "prod", "version": "1.0"}),
			hosts: 3,
		},
		{
			ctx:   newMockLbContext(map[string]string{"stage": "prod"}),
			hosts: 0,
		},
		{
			ctx:   newMockLbContext(nil),
			hosts: 7, // all hosts
		},
	}
	for _, c := range cases {
		hnum := lb.HostNum(c.ctx.MetadataMatchCriteria())
		require.Equal(t, c.hosts, hnum)
		for i := 0; i < 7; i++ {
			h := lb.ChooseHost(c.ctx)
			if c.hosts > 0 {
				require.True(t, lb.IsExistsHosts(c.ctx.MetadataMatchCriteria()))
				require.NotNil(t, h)
			} else {
				require.False(t, lb.IsExistsHosts(c.ctx.MetadataMatchCriteria()))
				require.Nil(t, h)
			}
		}
	}

}
