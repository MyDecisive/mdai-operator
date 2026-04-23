package xds

import (
	"context"
	"fmt"
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
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"github.com/go-logr/logr"
	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Manager struct {
	cache   cache.SnapshotCache
	version int64
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
	port uint32
	svc  string
	name string
	ns   string
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

	// Group all ports found across all collectors
	portMap := make(map[uint32][]collectorPort)

	for _, c := range collectors {
		if isShadowCollector(c) {
			continue
		}

		// The OTEL Operator creates a service named <collector-name>-collector
		svcName := fmt.Sprintf("%s-collector.%s.svc.cluster.local", c.Name, c.Namespace)

		ports := m.extractPortsFromConfig(c.Spec.Config)
		if len(ports) == 0 {
			ports = []uint32{4317, 4318} // Default OTLP fallback
		}

		log.Info("Identified ports for collector", "collector", c.Name, "ports", ports)

		for _, p := range ports {
			portMap[p] = append(portMap[p], collectorPort{
				port: p,
				svc:  svcName,
				name: c.Name,
				ns:   c.Namespace,
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
			appendCluster(&clusters, seenClusters, newDNSCluster(clusterName, cp.svc, cp.port))

			mirrorTargets := validationTargetsForCollectorPort(cp, port, validations)
			for _, target := range mirrorTargets {
				appendCluster(&clusters, seenClusters, newDNSCluster(target.clusterName, target.address, target.port))
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
			mirrorTargets := validationTargetsForCollectorPort(cpList[0], port, validations)
			virtualHosts = append(virtualHosts, &route.VirtualHost{
				Name:    fmt.Sprintf("vhost_%s_%d_default", cpList[0].name, cpList[0].port),
				Domains: []string{"*"},
				Routes:  []*route.Route{buildRoute(clusterName, mirrorTargets)},
			})
		} else if len(cpList) > 1 {
			defaultClusterName := fmt.Sprintf("%s_%d", cpList[0].name, cpList[0].port)
			mirrorTargets := validationTargetsForCollectorPort(cpList[0], port, validations)
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
			StatPrefix: fmt.Sprintf("ingress_%d", port),
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

	v := atomic.AddInt64(&m.version, 1)
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
		})
	}

	return &route.Route{
		Match: &route.RouteMatch{
			PathSpecifier: &route.RouteMatch_Prefix{
				Prefix: "/",
			},
		},
		Action: &route.Route_Route{
			Route: &route.RouteAction{
				ClusterSpecifier: &route.RouteAction_Cluster{
					Cluster: clusterName,
				},
				RequestMirrorPolicies: requestMirrorPolicies,
			},
		},
	}
}

func newDNSCluster(name, address string, port uint32) *cluster.Cluster {
	return &cluster.Cluster{
		Name:           name,
		ConnectTimeout: durationpb.New(5 * time.Second), //nolint:mnd
		ClusterDiscoveryType: &cluster.Cluster_Type{
			Type: cluster.Cluster_STRICT_DNS,
		},
		LbPolicy: cluster.Cluster_ROUND_ROBIN,
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
}

func appendCluster(clusters *[]types.Resource, seen map[string]struct{}, c *cluster.Cluster) {
	if _, ok := seen[c.GetName()]; ok {
		return
	}
	seen[c.GetName()] = struct{}{}
	*clusters = append(*clusters, c)
}

func validationTargetsForCollectorPort(cp collectorPort, listenerPort uint32, validations []hubv1.TelemetryValidation) []routeTarget {
	targets := make([]routeTarget, 0)
	for _, validation := range validations {
		if !validation.Spec.Enabled || validation.Spec.CollectorRef.Name != cp.name || validation.Namespace != cp.ns {
			continue
		}

		if validation.Spec.IngressCapture.Enabled {
			targets = append(targets, routeTarget{
				clusterName: fmt.Sprintf("%s_validator_%d", cp.name, listenerPort),
				address:     "mdai-fidelity-validator.mdai.svc.cluster.local",
				port:        listenerPort,
			})
		}

		if validation.Spec.ShadowCollector.Enabled {
			shadowName := shadowCollectorName(cp.name)
			targets = append(targets, routeTarget{
				clusterName: fmt.Sprintf("%s_shadow_%d", cp.name, listenerPort),
				address:     fmt.Sprintf("%s-collector.%s.svc.cluster.local", shadowName, validation.Namespace),
				port:        listenerPort,
			})
		}
	}

	return targets
}

func shadowCollectorName(collectorName string) string {
	return collectorName + "-shadow"
}

func isShadowCollector(c otelv1beta1.OpenTelemetryCollector) bool {
	if c.Labels["hub.mydecisive.ai/shadow"] == "true" {
		return true
	}
	if c.Annotations["hub.mydecisive.ai/shadow"] == "true" {
		return true
	}
	return strings.HasSuffix(c.Name, "-shadow")
}

func (*Manager) extractPortsFromConfig(config otelv1beta1.Config) []uint32 {
	ports := make([]uint32, 0)
	receivers := config.Receivers.Object

	for _, r := range receivers {
		rm, ok := r.(map[string]any)
		if !ok {
			continue
		}

		if receiverEndpoint, ok := rm["endpoint"].(string); ok {
			if port := extractPort(receiverEndpoint); port != 0 {
				ports = append(ports, port)
			}
		}

		if protocols, ok := rm["protocols"].(map[string]any); ok {
			for _, p := range protocols {
				pm, ok := p.(map[string]any)
				if !ok {
					continue
				}
				if protocolEndpoint, ok := pm["endpoint"].(string); ok {
					if port := extractPort(protocolEndpoint); port != 0 {
						ports = append(ports, port)
					}
				}
			}
		}
	}

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
