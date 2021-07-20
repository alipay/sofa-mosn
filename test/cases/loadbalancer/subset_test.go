// +build MOSNTest

package loadbalancer

import (
	"testing"

	. "mosn.io/mosn/test/framework"
	"mosn.io/mosn/test/lib"
	"mosn.io/mosn/test/lib/xprotocol/boltv1"
)

func TestSubsetLoadBalancer(t *testing.T) {
	Scenario(t, "request with subset", func() {
		_, _ = lib.InitMosn(ConfigSubsetLoadBalancer, lib.CreateConfig(GreenServerConfig), lib.CreateConfig(BlueServerConfig))
		Case("request green server", func() {
			client := lib.CreateClient("bolt", &boltv1.BoltClientConfig{
				TargetAddr: "127.0.0.1:2045",
				Request: &boltv1.RequestConfig{
					Header: map[string]string{
						"service": "service.green",
					},
				},
				Verify: &boltv1.VerifyConfig{
					ExpectedStatusCode: 0,
					ExpectedHeader: map[string]string{
						"server_flag": "green",
					},
				},
			})
			Verify(client.SyncCall(), Equal, true)
		})
		Case("request blue server", func() {
			client := lib.CreateClient("bolt", &boltv1.BoltClientConfig{
				TargetAddr: "127.0.0.1:2045",
				Request: &boltv1.RequestConfig{
					Header: map[string]string{
						"service": "service.blue",
					},
				},
				Verify: &boltv1.VerifyConfig{
					ExpectedStatusCode: 0,
					ExpectedHeader: map[string]string{
						"server_flag": "blue",
					},
				},
			})
			Verify(client.SyncCall(), Equal, true)
		})
		Case("request fail server", func() {
			client := lib.CreateClient("bolt", &boltv1.BoltClientConfig{
				TargetAddr: "127.0.0.1:2045",
				Request: &boltv1.RequestConfig{
					Header: map[string]string{
						"service": "service.fail",
					},
				},
				Verify: &boltv1.VerifyConfig{
					ExpectedStatusCode: 16,
				},
			})
			Verify(client.SyncCall(), Equal, true)
		})
		Case("request default server", func() {
			client := lib.CreateClient("bolt", &boltv1.BoltClientConfig{
				TargetAddr: "127.0.0.1:2045",
				Request: &boltv1.RequestConfig{
					Header: map[string]string{
						"service": "service.any",
					},
				},
				Verify: &boltv1.VerifyConfig{
					ExpectedStatusCode: 0,
				},
			})
			Verify(client.SyncCall(), Equal, true)
		})

	})
}

const GreenServerConfig = `{
	"protocol":"bolt",
	"config": {
		"address": "127.0.0.1:8080",
		"mux_config": {
			".*":{
				"common_builder": {
					"status_code": 0,
					"header": {
						"server_flag": "green"
					}
				},
				"error_buidler": {
					"status_code": 16
				}
			}
		}
	}
}`

const BlueServerConfig = `{
	"protocol":"bolt",
	"config": {
		"address": "127.0.0.1:8081",
		"mux_config": {
			".*":{
				"common_builder": {
					"status_code": 0,
					"header": {
						"server_flag": "blue"
					}
				},
				"error_buidler": {
					"status_code": 16
				}
			}
		}
	}
}`

const ConfigSubsetLoadBalancer = `{
	"servers":[
		{
			"default_log_path":"stdout",
			"default_log_level": "ERROR",
			"routers":[
				{
					"router_config_name":"router_to_server",
					"virtual_hosts":[{
						"name":"subset",
						"domains": ["*"],
						"routers": [
							{
								 "match":{"headers":[{"name":"service","value":"service.green"}]},
								 "route":{
									 "cluster_name": "cluster_subset",
									 "metadata_match": {
										 "filter_metadata": {
											 "mosn.lb": {
												 "subset":"green"
											 }
										 }
									 }
								 }
							},
							{
								 "match":{"headers":[{"name":"service","value":"service.blue"}]},
								 "route":{
									 "cluster_name": "cluster_subset",
									 "metadata_match": {
										 "filter_metadata": {
											 "mosn.lb": {
												 "subset":"blue"
											 }
										 }
									 }
								 }
							},
							{
								 "match":{"headers":[{"name":"service","value":"service.fail"}]},
								 "route":{
									 "cluster_name": "cluster_subset",
									 "metadata_match": {
										 "filter_metadata": {
											 "mosn.lb": {
												 "subset":"fail"
											 }
										 }
									 }
								 }
							},
							{
								 "match":{"headers":[{"name":"service","value":".*"}]},
								 "route":{
									 "cluster_name": "cluster_subset"
								 }
							}


						]
					}]
				}
			],
			"listeners":[
				{
					"address":"127.0.0.1:2045",
					"bind_port": true,
					"filter_chains": [{
						"filters": [
							{
								"type": "proxy",
								"config": {
									"downstream_protocol": "X",
									"upstream_protocol": "X",
									"extend_config": {
										"sub_protocol": "bolt"
									},
									"router_config_name":"router_to_server"
								}
							}
						]
					}]
				}
			]
		}
	],
	"cluster_manager":{
		"clusters":[
			{
				"name":"cluster_subset",
				"type": "SIMPLE",
				"lb_type": "LB_RANDOM",
				"lb_subset_config": {
					"fall_back_policy": 0,
					"subset_selectors": [
						[
							"subset"
						]
					]
				},
				"hosts": [
					{
						"address": "127.0.0.1:8080",
						"metadata": {
							"filter_metadata": {
								"mosn.lb": {
									"subset": "green"
								}
							}
						}
					},
					{
						"address": "127.0.0.1:8081",
						"metadata": {
							"filter_metadata": {
								"mosn.lb": {
									"subset": "blue"
								}
							}
						}
					}
				]
			}
		]
	}
}`
