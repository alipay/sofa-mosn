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

package v3

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"time"

	envoy_config_bootstrap_v3 "github.com/envoyproxy/go-control-plane/envoy/config/bootstrap/v3"
	envoy_config_cluster_v3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoy_config_core_v3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_config_listener_v3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	envoy_config_route_v3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	envoy_extensions_filters_network_http_connection_manager_v3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	envoy_extensions_transport_sockets_tls_v3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	envoy_service_discovery_v3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	wellknown "github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"mosn.io/mosn/pkg/featuregate"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/xds/v3/conv"
)

const connectionManager = "envoy.filters.network.http_connection_manager"

var (
	typeFactoryMapping = map[string]func() proto.Message{
		connectionManager: func() proto.Message {
			return &envoy_extensions_filters_network_http_connection_manager_v3.HttpConnectionManager{}
		},
	}
)

//  Init parsed ds and clusters config for xds
func (c *XDSConfig) Init(dynamicResources *envoy_config_bootstrap_v3.Bootstrap_DynamicResources, staticResources *envoy_config_bootstrap_v3.Bootstrap_StaticResources) error {
	if staticResources == nil {
		log.DefaultLogger.Errorf("StaticResources is null")
		err := errors.New("null point exception")
		return err
	}
	err := staticResources.Validate()
	if err != nil {
		log.DefaultLogger.Errorf("Invalid StaticResources")
		return err
	}

	err = c.loadStaticResource(staticResources)
	if err != nil {
		return err
	}
	err = c.loadADSConfig(dynamicResources)
	if err != nil {
		return err
	}
	return nil
}

func (c *XDSConfig) loadStaticResource(staticResources *envoy_config_bootstrap_v3.Bootstrap_StaticResources) error {
	err := c.loadClusters(staticResources.Clusters)
	if err != nil {
		return err
	}
	err = c.loadLisener(staticResources.Listeners)
	if err != nil {
		return err
	}
	err = c.loadRoutes(staticResources.Listeners)
	if err != nil {
		return err
	}
	return nil
}

func (c *XDSConfig) loadADSConfig(dynamicResources *envoy_config_bootstrap_v3.Bootstrap_DynamicResources) error {
	if dynamicResources == nil || dynamicResources.AdsConfig == nil {
		log.DefaultLogger.Errorf("DynamicResources is null")
		err := errors.New("null point exception")
		return err
	}
	err := dynamicResources.AdsConfig.Validate()
	if err != nil {
		log.DefaultLogger.Errorf("Invalid DynamicResources")
		return err
	}
	config, err := c.getAPISourceEndpoint(dynamicResources.AdsConfig)
	if err != nil {
		log.DefaultLogger.Errorf("fail to get api source endpoint")
		return err
	}
	c.ADSConfig = config
	return nil
}

func (c *XDSConfig) getAPISourceEndpoint(source *envoy_config_core_v3.ApiConfigSource) (*ADSConfig, error) {
	config := &ADSConfig{}
	if source.ApiType != envoy_config_core_v3.ApiConfigSource_GRPC {
		log.DefaultLogger.Errorf("unsupported api type: %v", source.ApiType)
		err := errors.New("only support GRPC api type yet")
		return nil, err
	}
	config.APIType = source.ApiType
	if source.RefreshDelay == nil || source.RefreshDelay.GetSeconds() <= 0 {
		duration := time.Duration(time.Second * 10) // default refresh delay
		config.RefreshDelay = &duration
	} else {
		duration := conv.ConvertDuration(source.RefreshDelay)
		config.RefreshDelay = &duration
	}

	config.Services = make([]*ServiceConfig, 0, len(source.GrpcServices))
	for _, service := range source.GrpcServices {
		t := service.TargetSpecifier
		if target, ok := t.(*envoy_config_core_v3.GrpcService_EnvoyGrpc_); ok {
			serviceConfig := ServiceConfig{}
			if service.Timeout == nil || (service.Timeout.GetSeconds() <= 0 && service.Timeout.GetNanos() <= 0) {
				duration := time.Duration(time.Second) // default connection timeout
				serviceConfig.Timeout = &duration
			} else {
				var nanos = service.Timeout.Seconds*int64(time.Second) + int64(service.Timeout.Nanos)
				duration := time.Duration(nanos)
				serviceConfig.Timeout = &duration
			}
			clusterName := target.EnvoyGrpc.ClusterName
			serviceConfig.ClusterConfig = c.Clusters[clusterName]
			if serviceConfig.ClusterConfig == nil {
				log.DefaultLogger.Errorf("cluster not found: %s", clusterName)
				return nil, fmt.Errorf("cluster not found: %s", clusterName)
			}
			config.Services = append(config.Services, &serviceConfig)
		} else if _, ok := t.(*envoy_config_core_v3.GrpcService_GoogleGrpc_); ok {
			log.DefaultLogger.Warnf("GrpcService_GoogleGrpc_ not support yet")
			continue
		}
	}
	return config, nil
}

func (c *XDSConfig) loadClusters(clusters []*envoy_config_cluster_v3.Cluster) error {
	if len(clusters) == 0 {
		return errors.New("static_resources cluster is empty")
	}
	conv.ConvertUpdateClusters(clusters)

	c.Clusters = make(map[string]*ClusterConfig)
	for _, cluster := range clusters {
		name := cluster.Name
		config := ClusterConfig{}

		if cluster.TransportSocket != nil && cluster.TransportSocket.Name == wellknown.TransportSocketTls {
			config.TlsContext = cluster.TransportSocket
		}

		if cluster.LbPolicy != envoy_config_cluster_v3.Cluster_RANDOM {
			log.DefaultLogger.Warnf("only random lbPoliy supported, convert to random")
		}

		config.LbPolicy = envoy_config_cluster_v3.Cluster_RANDOM
		if cluster.ConnectTimeout.GetSeconds() <= 0 {
			duration := time.Second * 10
			config.ConnectTimeout = &duration // default connect timeout
		} else {
			duration := conv.ConvertDuration(cluster.ConnectTimeout)
			config.ConnectTimeout = &duration
		}

		if len(cluster.LoadAssignment.Endpoints) == 0 {
			log.DefaultLogger.Fatalf("xds v3 cluster.loadassignment is empty")
		}

		config.Address = make([]string, 0, len(cluster.LoadAssignment.GetEndpoints()[0].LbEndpoints))
		for _, host := range cluster.LoadAssignment.GetEndpoints()[0].LbEndpoints {
			endpoint := host.GetEndpoint()

			// Istio 1.8+ use istio-agent proxy request Istiod
			if endpoint.Address.GetPipe() != nil {
				config.Address = []string{fmt.Sprintf("unix://%s", endpoint.Address.GetPipe().Path)}
				break
			}

			if endpoint.Address.GetSocketAddress() == nil {
				log.DefaultLogger.Fatalf("xds v3 cluster.loadassignment pipe and socket both empty")
			}
			if port, ok := endpoint.Address.GetSocketAddress().PortSpecifier.(*envoy_config_core_v3.SocketAddress_PortValue); ok {
				newAddress := fmt.Sprintf("%s:%d", endpoint.Address.GetSocketAddress().Address, port.PortValue)
				config.Address = append(config.Address, newAddress)
			} else {
				log.DefaultLogger.Warnf("only PortValue supported")
				continue
			}
		}
		c.Clusters[name] = &config
	}
	return nil
}

func (c *XDSConfig) loadLisener(listeners []*envoy_config_listener_v3.Listener) error {
	conv.ConvertAddOrUpdateListeners(listeners)
	return nil
}

func (c *XDSConfig) loadRoutes(listeners []*envoy_config_listener_v3.Listener) error {
	if len(listeners) == 0 {
		return nil
	}
	routes := make([]*envoy_config_route_v3.RouteConfiguration, 0, len(listeners))
	for _, listener := range listeners {
		if len(listener.FilterChains) == 0 {
			continue
		}
		for _, fc := range listener.FilterChains {
			if len(fc.Filters) == 0 {
				continue
			}
			for _, filter := range fc.Filters {
				if factory, exist := typeFactoryMapping[filter.Name]; exist {
					typedConfig := factory()
					if err := ptypes.UnmarshalAny(filter.GetTypedConfig(), typedConfig); err != nil {
						return err
					}
					switch typedConfig.(type) {
					case *envoy_extensions_filters_network_http_connection_manager_v3.HttpConnectionManager:
						manager := typedConfig.(*envoy_extensions_filters_network_http_connection_manager_v3.HttpConnectionManager)
						if routerConfig := manager.GetRouteConfig(); routerConfig != nil {
							routes = append(routes, routerConfig)
						}
					default:
						log.DefaultLogger.Warnf("cannot handle route config type, listener: %s, name: %s",
							listener.Name, filter.Name)
					}
				} else {
					log.DefaultLogger.Warnf("cannot handle route type, listener: %s, filter: %s",
						listener.Name, filter.Name)
				}
			}
		}
	}
	if len(routes) > 0 {
		conv.ConvertAddOrUpdateRouters(routes)
	}
	return nil
}

// GetEndpoint return an endpoint address by random
func (c *ClusterConfig) GetEndpoint() (string, *time.Duration) {
	if c.LbPolicy != envoy_config_cluster_v3.Cluster_RANDOM || len(c.Address) < 1 {
		// never happen
		return "", nil
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	idx := r.Intn(len(c.Address))

	return c.Address[idx], c.ConnectTimeout
}

// GetStreamClient return a grpc stream client that connected to ads
func (c *ADSConfig) GetStreamClient() envoy_service_discovery_v3.AggregatedDiscoveryService_StreamAggregatedResourcesClient {
	if c.StreamClient != nil && c.StreamClient.Client != nil {
		return c.StreamClient.Client
	}

	sc := &StreamClient{
		Conn: c.buildClient(),
	}

	if sc.Conn == nil {
		return nil
	}
	client := envoy_service_discovery_v3.NewAggregatedDiscoveryServiceClient(sc.Conn)

	ctx, cancel := context.WithCancel(context.Background())
	sc.Cancel = cancel
	streamClient, err := client.StreamAggregatedResources(ctx)
	if err != nil {
		log.DefaultLogger.Infof("fail to create stream client: %v", err)
		if sc.Conn != nil {
			sc.Conn.Close()
		}
		return nil
	}
	sc.Client = streamClient
	c.StreamClient = sc
	return streamClient
}

func (c *ADSConfig) buildClient() *grpc.ClientConn {
	if c.Services == nil {
		log.DefaultLogger.Errorf("no available ads service")
		return nil
	}

	var endpoint string
	var tlsContext *envoy_config_core_v3.TransportSocket
	for _, service := range c.Services {
		if service.ClusterConfig == nil {
			continue
		}
		endpoint, _ = service.ClusterConfig.GetEndpoint()
		if len(endpoint) > 0 {
			tlsContext = service.ClusterConfig.TlsContext
			break
		}
	}
	if len(endpoint) == 0 {
		log.DefaultLogger.Errorf("no available ads endpoint")
		return nil
	}

	if tlsContext == nil || !featuregate.Enabled(featuregate.XdsMtlsEnable) {
		conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), generateDialOption())
		if err != nil {
			log.DefaultLogger.Errorf("did not connect: %v", err)
			return nil
		}
		log.DefaultLogger.Infof("mosn estab grpc connection to pilot with address at %v", endpoint)
		return conn
	}

	// Grpc with mTls support
	creds, err := c.getTLSCreds(tlsContext)
	if err != nil {
		log.DefaultLogger.Errorf("xds-grpc get tls creds fail: err= %v", err)
		return nil
	}
	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(creds), generateDialOption())
	if err != nil {
		log.DefaultLogger.Errorf("did not connect: %v", err)
		return nil
	}
	log.DefaultLogger.Infof("mosn estab grpc connection to pilot with address and mTls at %v", endpoint)
	return conn
}

func (c *ADSConfig) getTLSCreds(tlsContextConfig *envoy_config_core_v3.TransportSocket) (credentials.TransportCredentials, error) {

	tlsContext := &envoy_extensions_transport_sockets_tls_v3.UpstreamTlsContext{}
	if err := ptypes.UnmarshalAny(tlsContextConfig.GetTypedConfig(), tlsContext); err != nil {
		return nil, err
	}

	if tlsContext.CommonTlsContext.GetValidationContext() == nil ||
		tlsContext.CommonTlsContext.GetValidationContext().GetTrustedCa() == nil {
		return nil, errors.New("can't find trusted ca ")
	}
	rootCAPath := tlsContext.CommonTlsContext.GetValidationContext().GetTrustedCa().GetFilename()
	if len(tlsContext.CommonTlsContext.GetTlsCertificates()) <= 0 {
		return nil, errors.New("can't find client certificates")
	}
	if tlsContext.CommonTlsContext.GetTlsCertificates()[0].GetCertificateChain() == nil ||
		tlsContext.CommonTlsContext.GetTlsCertificates()[0].GetPrivateKey() == nil {
		return nil, errors.New("can't read client certificates fail")
	}
	certChainPath := tlsContext.CommonTlsContext.GetTlsCertificates()[0].GetCertificateChain().GetFilename()
	privateKeyPath := tlsContext.CommonTlsContext.GetTlsCertificates()[0].GetPrivateKey().GetFilename()
	log.DefaultLogger.Infof("mosn start with tls context,root ca certificate path = %v\n cert chain path = %v\n private key path = %v\n",
		rootCAPath, certChainPath, privateKeyPath)
	certPool := x509.NewCertPool()
	bs, err := ioutil.ReadFile(rootCAPath)
	if err != nil {
		return nil, err
	}
	ok := certPool.AppendCertsFromPEM(bs)
	if !ok {
		return nil, errors.New("failed to append certs")
	}
	certificate, err := tls.LoadX509KeyPair(
		certChainPath,
		privateKeyPath,
	)
	creds := credentials.NewTLS(&tls.Config{
		ServerName:   "",
		Certificates: []tls.Certificate{certificate},
		RootCAs:      certPool,
	})
	return creds, nil
}

func (c *ADSConfig) getADSRefreshDelay() *time.Duration {
	return c.RefreshDelay
}

func (c *ADSConfig) closeADSStreamClient() {
	if c.StreamClient == nil {
		return
	}
	c.StreamClient.Cancel()
	if c.StreamClient.Conn != nil {
		c.StreamClient.Conn.Close()
		c.StreamClient.Conn = nil
	}
	c.StreamClient.Client = nil
	c.StreamClient = nil
}

// [xds] [ads client] get resp timeout: rpc error: code = ResourceExhausted desc = grpc: received message larger than max (5193322 vs. 4194304), retry after 1s
// https://github.com/istio/istio/blob/9686754643d0939c1f4dd0ee20443c51183f3589/pilot/pkg/bootstrap/server.go#L662
// Istio xDS DiscoveryServer not set grpc MaxSendMsgSize. If this is not set, gRPC uses the default `math.MaxInt32`.
func generateDialOption() grpc.DialOption {
	return grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
	)
}
