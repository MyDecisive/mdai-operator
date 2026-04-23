package xds

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	upstreamhttp "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	envoytypev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"github.com/go-logr/logr"
	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const httpProtocolOptionsTypedExtension = "envoy.extensions.upstreams.http.v3.HttpProtocolOptions"

const (
	defaultOTLPGRPCPort = 4317
	defaultOTLPHTTPPort = 4318
)

type Manager struct {
	cache   cache.SnapshotCache
	version atomic.Int64
	mu      sync.Mutex
}

func NewXDSManager() *Manager {
	return &Manager{
		cache: cache.NewSnapshotCache(true, cache.IDHash{}, nil),
	}
}

func (m *Manager) GetCache() cache.Cache {
	return m.cache
}

type collectorPort struct {
	port        uint32
	svc         string
	name        string
	ns          string
	enableHTTP2 bool
}

type routeTarget struct {
	clusterName string
	address     string
	port        uint32
}

func (m *Manager) UpdateSnapshot(ctx context.Context, nodeID string, collectors []otelv1beta1.OpenTelemetryCollector, validations []hubv1.TelemetryValidation) error {
	log := logr.FromContextOrDiscard(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()

	upstreamProtocolOptionsAny, err := anypb.New(&upstreamhttp.HttpProtocolOptions{
		UpstreamProtocolOptions: &upstreamhttp.HttpProtocolOptions_ExplicitHttpConfig_{
			ExplicitHttpConfig: &upstreamhttp.HttpProtocolOptions_ExplicitHttpConfig{
				ProtocolConfig: &upstreamhttp.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{
					Http2ProtocolOptions: &core.Http2ProtocolOptions{
						MaxConcurrentStreams:         wrapperspb.UInt32(50),                    //nolint:mnd
						InitialStreamWindowSize:     wrapperspb.UInt32(1024 * 1024),            //nolint:mnd
						InitialConnectionWindowSize: wrapperspb.UInt32(4 * 1024 * 1024),        //nolint:mnd
						ConnectionKeepalive: &core.KeepaliveSettings{
							Interval: durationpb.New(30 * time.Second), //nolint:mnd
							Timeout:  durationpb.New(5 * time.Second),  //nolint:mnd
						},
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// Group all ports found across all collectors
	portMap := make(map[uint32][]collectorPort)

	for _, c := range collectors {
		if IsShadowCollector(c) {
			continue
		}

		// The OTEL Operator creates a service named <collector-name>-collector
		svcName := fmt.Sprintf("%s-collector.%s.svc.cluster.local", c.Name, c.Namespace)

		ports := m.extractPortsFromConfig(c.Spec.Config)
		if len(ports) == 0 {
			ports = []collectorProtocolPort{
				{port: defaultOTLPGRPCPort, enableHTTP2: true},
				{port: defaultOTLPHTTPPort, enableHTTP2: false},
			}
		}

		log.Info("Identified ports for collector", "collector", c.Name, "ports", ports)

		for _, p := range ports {
			portMap[p.port] = append(portMap[p.port], collectorPort{
				port:        p.port,
				svc:         svcName,
				name:        c.Name,
				ns:          c.Namespace,
				enableHTTP2: p.enableHTTP2,
			})
		}
	}

	var clusters []types.Resource
	var listeners []types.Resource
	seenClusters := make(map[string]struct{})

	for port, cpList := range portMap {
		var virtualHosts []*route.VirtualHost

		for _, cp := range cpList {
			// Cluster name is based on the service it forwards to
			clusterName := fmt.Sprintf("%s_%d", cp.name, cp.port)
			appendCluster(&clusters, seenClusters, newDNSCluster(clusterName, cp.svc, cp.port, protocolOptionsForCollectorPort(cp, upstreamProtocolOptionsAny), false))

			mirrorTargets := validationTargetsForCollectorPort(log, cp, port, validations)
			for _, target := range mirrorTargets {
				log.Info(
					"xDS mirror forwarding target",
					"collector", cp.name,
					"collector_namespace", cp.ns,
					"listener_port", port,
					"target_cluster", target.clusterName,
					"target_address", target.address,
					"target_port", target.port,
				)
			}
			for _, target := range mirrorTargets {
				appendCluster(&clusters, seenClusters, newDNSCluster(target.clusterName, target.address, target.port, protocolOptionsForCollectorPort(cp, upstreamProtocolOptionsAny), true))
			}

			vHost := &route.VirtualHost{
				Name: fmt.Sprintf("vhost_%s_%d", cp.name, cp.port),
				Domains: []string{
					cp.name + ".mdai.hub",
					fmt.Sprintf("%s:%d", cp.name, cp.port),
					cp.svc,
					fmt.Sprintf("%s:%d", cp.svc, cp.port),
				},
				Routes: []*route.Route{buildRoute(clusterName, mirrorTargets)},
			}
			virtualHosts = append(virtualHosts, vHost)
		}

		if len(cpList) == 1 {
			clusterName := fmt.Sprintf("%s_%d", cpList[0].name, cpList[0].port)
			mirrorTargets := validationTargetsForCollectorPort(log, cpList[0], port, validations)
			virtualHosts = append(virtualHosts, &route.VirtualHost{
				Name:    fmt.Sprintf("vhost_%s_%d_default", cpList[0].name, cpList[0].port),
				Domains: []string{"*"},
				Routes:  []*route.Route{buildRoute(clusterName, mirrorTargets)},
			})
		} else if len(cpList) > 1 {
			defaultClusterName := fmt.Sprintf("%s_%d", cpList[0].name, cpList[0].port)
			mirrorTargets := validationTargetsForCollectorPort(log, cpList[0], port, validations)
			virtualHosts = append(virtualHosts, &route.VirtualHost{
				Name:    fmt.Sprintf("vhost_default_%d", port),
				Domains: []string{"*"},
				Routes:  []*route.Route{buildRoute(defaultClusterName, mirrorTargets)},
			})
		}

		routerAny, err := anypb.New(&router.Router{})
		if err != nil {
			return err
		}

		manager := &hcm.HttpConnectionManager{
			StatPrefix:             fmt.Sprintf("ingress_%d", port),
			GenerateRequestId:      wrapperspb.Bool(true),
			StreamIdleTimeout:      durationpb.New(10 * time.Minute),  //nolint:mnd
			RequestTimeout:         durationpb.New(30 * time.Second),  //nolint:mnd
			RequestHeadersTimeout:  durationpb.New(10 * time.Second),  //nolint:mnd
			RouteSpecifier: &hcm.HttpConnectionManager_RouteConfig{
				RouteConfig: &route.RouteConfiguration{
					Name:         fmt.Sprintf("route_%d", port),
					VirtualHosts: virtualHosts,
				},
			},
			HttpFilters: []*hcm.HttpFilter{{
				Name: wellknown.Router,
				ConfigType: &hcm.HttpFilter_TypedConfig{
					TypedConfig: routerAny,
				},
			}},
		}

		log.Info("Creating listener", "port", port)

		pbst, err := anypb.New(manager)
		if err != nil {
			return err
		}

		uListener := &listener.Listener{
			Name: fmt.Sprintf("listener_%d", port),
			Address: &core.Address{
				Address: &core.Address_SocketAddress{
					SocketAddress: &core.SocketAddress{
						Protocol: core.SocketAddress_TCP,
						Address:  "0.0.0.0",
						PortSpecifier: &core.SocketAddress_PortValue{
							PortValue: port,
						},
					},
				},
			},
			PerConnectionBufferLimitBytes: wrapperspb.UInt32(32 * 1024 * 1024), //nolint:mnd
			FilterChains: []*listener.FilterChain{{
				Filters: []*listener.Filter{{
					Name: wellknown.HTTPConnectionManager,
					ConfigType: &listener.Filter_TypedConfig{
						TypedConfig: pbst,
					},
				}},
			}},
		}
		listeners = append(listeners, uListener)
	}

	v := m.version.Add(1)
	versionStr := strconv.FormatInt(v, 10)

	snapshot, err := cache.NewSnapshot(versionStr, map[resource.Type][]types.Resource{
		resource.ClusterType:  clusters,
		resource.ListenerType: listeners,
		resource.RouteType:    {},
		resource.EndpointType: {},
	})
	if err != nil {
		return err
	}

	return m.cache.SetSnapshot(ctx, nodeID, snapshot)
}

func buildRoute(clusterName string, mirrorTargets []routeTarget) *route.Route {
	requestMirrorPolicies := make([]*route.RouteAction_RequestMirrorPolicy, 0, len(mirrorTargets))
	for _, target := range mirrorTargets {
		requestMirrorPolicies = append(requestMirrorPolicies, &route.RouteAction_RequestMirrorPolicy{
			Cluster: target.clusterName,
			RuntimeFraction: &core.RuntimeFractionalPercent{
				DefaultValue: &envoytypev3.FractionalPercent{
					Numerator:   100, //nolint:mnd
					Denominator: envoytypev3.FractionalPercent_HUNDRED,
				},
			},
		})
	}

	return &route.Route{
		Match: &route.RouteMatch{
			PathSpecifier: &route.RouteMatch_Prefix{
				Prefix: "/",
			},
		},
		RequestHeadersToAdd: []*core.HeaderValueOption{{
			Header: &core.HeaderValue{
				Key:   "x-correlation-id",
				Value: "%REQ(X-REQUEST-ID)%",
			},
			AppendAction: core.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		}},
		Action: &route.Route_Route{
			Route: &route.RouteAction{
				ClusterSpecifier: &route.RouteAction_Cluster{
					Cluster: clusterName,
				},
				// Timeout is disabled (0) so the upstream sender (DataDog agent, OTEL SDK) governs
				// the overall deadline. Envoy retries only on connection-level failures where
				// the request provably never reached the collector, avoiding duplicate delivery.
				Timeout:     durationpb.New(0),
				IdleTimeout: durationpb.New(10 * time.Minute), //nolint:mnd
				RetryPolicy: &route.RetryPolicy{
					RetryOn:       "connect-failure,refused-stream,reset",
					NumRetries:    wrapperspb.UInt32(1), //nolint:mnd
					PerTryTimeout: durationpb.New(5 * time.Second), //nolint:mnd
					RetryBackOff: &route.RetryPolicy_RetryBackOff{
						BaseInterval: durationpb.New(100 * time.Millisecond), //nolint:mnd
						MaxInterval:  durationpb.New(1 * time.Second),        //nolint:mnd
					},
				},
				RequestMirrorPolicies: requestMirrorPolicies,
			},
		},
	}
}

// newDNSCluster creates a STRICT_DNS cluster for the given address and port.
// isMirror should be true for validation mirror targets (shadow collector, fidelity validator)
// to apply tighter circuit breaker limits and shorter connect timeouts, preventing mirror
// traffic from competing with primary collector traffic.
func newDNSCluster(name, address string, port uint32, upstreamProtocolOptionsAny *anypb.Any, isMirror bool) *cluster.Cluster {
	isGRPC := upstreamProtocolOptionsAny != nil

	connectTimeout := 5 * time.Second
	if isGRPC {
		connectTimeout = 15 * time.Second //nolint:mnd
		if isMirror {
			connectTimeout = 10 * time.Second //nolint:mnd
		}
	}

	bufferLimitBytes := uint32(32 * 1024 * 1024) //nolint:mnd
	if isMirror {
		bufferLimitBytes = 10 * 1024 * 1024 //nolint:mnd
	}

	maxConns := uint32(100) //nolint:mnd
	maxReqs := uint32(500)  //nolint:mnd
	if isMirror {
		maxConns = 50  //nolint:mnd
		maxReqs = 200  //nolint:mnd
	}

	c := &cluster.Cluster{
		Name:           name,
		ConnectTimeout: durationpb.New(connectTimeout),
		ClusterDiscoveryType: &cluster.Cluster_Type{
			Type: cluster.Cluster_STRICT_DNS,
		},
		LbPolicy:                      cluster.Cluster_ROUND_ROBIN,
		PerConnectionBufferLimitBytes: wrapperspb.UInt32(bufferLimitBytes),
		CircuitBreakers: &cluster.CircuitBreakers{
			Thresholds: []*cluster.CircuitBreakers_Thresholds{{
				Priority:           core.RoutingPriority_DEFAULT,
				MaxConnections:     wrapperspb.UInt32(maxConns),
				MaxPendingRequests: wrapperspb.UInt32(maxConns),
				MaxRequests:        wrapperspb.UInt32(maxReqs),
				MaxRetries:         wrapperspb.UInt32(3), //nolint:mnd
			}},
		},
		OutlierDetection: &cluster.OutlierDetection{
			Consecutive_5Xx:                wrapperspb.UInt32(5),                    //nolint:mnd
			ConsecutiveGatewayFailure:      wrapperspb.UInt32(5),                    //nolint:mnd
			Interval:                       durationpb.New(10 * time.Second),        //nolint:mnd
			BaseEjectionTime:               durationpb.New(30 * time.Second),        //nolint:mnd
			MaxEjectionPercent:             wrapperspb.UInt32(50),                   //nolint:mnd
			SplitExternalLocalOriginErrors: true,
			ConsecutiveLocalOriginFailure:  wrapperspb.UInt32(5),                    //nolint:mnd
		},
		LoadAssignment: &endpoint.ClusterLoadAssignment{
			ClusterName: name,
			Endpoints: []*endpoint.LocalityLbEndpoints{{
				LbEndpoints: []*endpoint.LbEndpoint{{
					HostIdentifier: &endpoint.LbEndpoint_Endpoint{
						Endpoint: &endpoint.Endpoint{
							Address: &core.Address{
								Address: &core.Address_SocketAddress{
									SocketAddress: &core.SocketAddress{
										Protocol: core.SocketAddress_TCP,
										Address:  address,
										PortSpecifier: &core.SocketAddress_PortValue{
											PortValue: port,
										},
									},
								},
							},
						},
					},
				}},
			}},
		},
	}
	if upstreamProtocolOptionsAny != nil {
		c.TypedExtensionProtocolOptions = map[string]*anypb.Any{
			httpProtocolOptionsTypedExtension: upstreamProtocolOptionsAny,
		}
	}
	return c
}

func protocolOptionsForCollectorPort(cp collectorPort, upstreamProtocolOptionsAny *anypb.Any) *anypb.Any {
	if cp.enableHTTP2 {
		return upstreamProtocolOptionsAny
	}
	return nil
}

func appendCluster(clusters *[]types.Resource, seen map[string]struct{}, c *cluster.Cluster) {
	if _, ok := seen[c.GetName()]; ok {
		return
	}
	seen[c.GetName()] = struct{}{}
	*clusters = append(*clusters, c)
}

func validationTargetsForCollectorPort(log logr.Logger, cp collectorPort, listenerPort uint32, validations []hubv1.TelemetryValidation) []routeTarget {
	targets := make([]routeTarget, 0)
	for _, validation := range validations {
		if !validation.Spec.Enabled || validation.Spec.CollectorRef.Name != cp.name || validation.Namespace != cp.ns {
			continue
		}

		validatorService := validation.Status.ValidatorService
		if strings.TrimSpace(validatorService) == "" {
			log.Info(
				"xDS ingress capture target skipped; validator service not ready",
				"telemetry_validation", validation.Name,
				"namespace", validation.Namespace,
				"collector", cp.name,
				"listener_port", listenerPort,
			)
			continue
		}
		targets = append(targets, routeTarget{
			clusterName: fmt.Sprintf("%s_%s_%s_validator_%d", cp.name, validation.Namespace, validation.Name, listenerPort),
			address:     fmt.Sprintf("%s.%s.svc.cluster.local", validatorService, validation.Namespace),
			port:        listenerPort,
		})

		shadowName := shadowCollectorName(cp.name)
		targets = append(targets, routeTarget{
			clusterName: fmt.Sprintf("%s_%s_%s_shadow_%d", cp.name, validation.Namespace, validation.Name, listenerPort),
			address:     fmt.Sprintf("%s-collector.%s.svc.cluster.local", shadowName, validation.Namespace),
			port:        listenerPort,
		})
	}

	return targets
}

func shadowCollectorName(collectorName string) string {
	return collectorName + "-shadow"
}

func IsShadowCollector(c otelv1beta1.OpenTelemetryCollector) bool {
	if c.Labels["hub.mydecisive.ai/shadow"] == "true" {
		return true
	}
	if c.Annotations["hub.mydecisive.ai/shadow"] == "true" {
		return true
	}
	return strings.HasSuffix(c.Name, "-shadow")
}

type collectorProtocolPort struct {
	port        uint32
	enableHTTP2 bool
}

func (*Manager) extractPortsFromConfig(config otelv1beta1.Config) []collectorProtocolPort {
	portProtocols := make(map[uint32]bool)
	receivers := config.Receivers.Object

	for _, r := range receivers {
		rm, ok := r.(map[string]any)
		if !ok {
			continue
		}

		if receiverEndpoint, ok := rm["endpoint"].(string); ok {
			if port := extractPort(receiverEndpoint); port != 0 {
				if _, exists := portProtocols[port]; !exists {
					portProtocols[port] = false
				}
			}
		}

		if protocols, ok := rm["protocols"].(map[string]any); ok {
			for protocolName, p := range protocols {
				pm, ok := p.(map[string]any)
				if !ok {
					continue
				}
				if protocolEndpoint, ok := pm["endpoint"].(string); ok {
					if port := extractPort(protocolEndpoint); port != 0 {
						portProtocols[port] = portProtocols[port] || strings.EqualFold(protocolName, "grpc")
					}
				}
			}
		}
	}

	ports := make([]collectorProtocolPort, 0, len(portProtocols))
	for port, enableHTTP2 := range portProtocols {
		ports = append(ports, collectorProtocolPort{
			port:        port,
			enableHTTP2: enableHTTP2,
		})
	}
	slices.SortFunc(ports, func(a, b collectorProtocolPort) int {
		if a.port < b.port {
			return -1
		}
		if a.port > b.port {
			return 1
		}
		return 0
	})
	return ports
}

func extractPort(addr string) uint32 {
	var port uint32
	_, _ = fmt.Sscanf(addr, "0.0.0.0:%d", &port)
	if port == 0 {
		_, _ = fmt.Sscanf(addr, ":%d", &port)
	}
	if port == 0 {
		_, _ = fmt.Sscanf(addr, "%d", &port)
	}
	return port
}
