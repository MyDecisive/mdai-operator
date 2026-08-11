package xds

import (
	"fmt"
	"testing"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	upstreamhttp "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestUpdateSnapshotAddsWildcardFallbackForSharedPort(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollector("alpha", "mdai", 4317),
		newCollector("beta", "mdai", 4317),
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, nil)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)

	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok, "snapshot type = %T, want *cachev3.Snapshot", snapshot)

	listeners := concreteSnapshot.GetResources(resource.ListenerType)
	rawListener, ok := listeners["listener_4317"]
	require.True(t, ok, "listener_4317 not found in snapshot")

	l, ok := rawListener.(*listener.Listener)
	require.True(t, ok, "listener type = %T, want *listener.Listener", rawListener)

	require.NotEmpty(t, l.GetFilterChains(), "listener is missing filter chains")
	require.NotEmpty(t, l.GetFilterChains()[0].GetFilters(), "listener is missing filters")

	typedConfig := l.GetFilterChains()[0].GetFilters()[0].GetTypedConfig()
	require.NotNil(t, typedConfig, "listener is missing typed config")

	managerConfig := &hcm.HttpConnectionManager{}
	require.NoError(t, typedConfig.UnmarshalTo(managerConfig))

	routeConfig := managerConfig.GetRouteConfig()
	require.NotNil(t, routeConfig, "route config is nil")

	wildcardFound := false
	for _, virtualHost := range routeConfig.GetVirtualHosts() {
		if proto.Equal(virtualHost, &route.VirtualHost{}) {
			continue
		}
		for _, domain := range virtualHost.GetDomains() {
			if domain == "*" {
				wildcardFound = true
				require.NotEmpty(t, virtualHost.GetRoutes(), "wildcard virtual host has no routes")
				gotCluster := virtualHost.GetRoutes()[0].GetRoute().GetCluster()
				assert.Equal(t, "alpha_4317", gotCluster)
			}
		}
	}

	assert.True(t, wildcardFound, "expected wildcard virtual host for shared port")
	assert.True(t, managerConfig.GetGenerateRequestId().GetValue(), "expected generate_request_id=true")
	wildcardRoute := routeConfig.GetVirtualHosts()[len(routeConfig.GetVirtualHosts())-1].GetRoutes()[0]
	require.NotEmpty(t, wildcardRoute.GetRequestHeadersToAdd(), "expected request_headers_to_add on route")
	correlationHeader := wildcardRoute.GetRequestHeadersToAdd()[0]
	assert.Equal(t, "x-correlation-id", correlationHeader.GetHeader().GetKey())
	assert.Equal(t, "%REQ(X-REQUEST-ID)%", correlationHeader.GetHeader().GetValue())
	assert.Equal(t, "OVERWRITE_IF_EXISTS_OR_ADD", correlationHeader.GetAppendAction().String())
	require.NotNil(t, wildcardRoute.GetRoute().GetRetryPolicy(), "expected retry policy to be set")
	assert.Equal(t, uint32(1), wildcardRoute.GetRoute().GetRetryPolicy().GetNumRetries().GetValue())
}

func TestUpdateSnapshotUsesValidatorServiceFromTelemetryValidationStatus(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollector("gateway", "mdai", 4317),
	}
	validations := []hubv1.TelemetryValidation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample",
				Namespace: "mdai",
			},
			Spec: hubv1.TelemetryValidationSpec{
				Enabled: true,
				CollectorRef: hubv1.TelemetryValidationCollectorRef{
					Name: "gateway",
				},
			},
			Status: hubv1.TelemetryValidationStatus{
				ValidatorService: "sample-fidelity-validator",
			},
		},
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, validations)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	rawCluster, ok := clusters["gateway_mdai_sample_validator_4317"]
	require.True(t, ok, "gateway_mdai_sample_validator_4317 cluster not found")
	c, ok := rawCluster.(*cluster.Cluster)
	require.True(t, ok)
	lbEndpoints := c.GetLoadAssignment().GetEndpoints()
	require.NotEmpty(t, lbEndpoints)
	endpoints := lbEndpoints[0].GetLbEndpoints()
	require.NotEmpty(t, endpoints)
	socket := endpoints[0].GetEndpoint().GetAddress().GetSocketAddress()
	require.NotNil(t, socket)
	assert.Equal(t, "sample-fidelity-validator.mdai.svc.cluster.local", socket.GetAddress())
	assert.Equal(t, uint32(4317), socket.GetPortValue())
}

func TestUpdateSnapshotSkipsValidatorMirrorUntilValidatorServiceReady(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollector("gateway", "mdai", 4317),
	}
	validations := []hubv1.TelemetryValidation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample",
				Namespace: "mdai",
			},
			Spec: hubv1.TelemetryValidationSpec{
				Enabled: true,
				CollectorRef: hubv1.TelemetryValidationCollectorRef{
					Name: "gateway",
				},
			},
		},
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, validations)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	_, exists := clusters["gateway_mdai_sample_validator_4317"]
	assert.False(t, exists, "validator cluster should not exist before validatorService is populated")
}

func TestUpdateSnapshotUsesCollectorListenerPortForValidatorMirror(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollector("gateway", "mdai", 4317),
	}
	validations := []hubv1.TelemetryValidation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample",
				Namespace: "mdai",
			},
			Spec: hubv1.TelemetryValidationSpec{
				Enabled: true,
				CollectorRef: hubv1.TelemetryValidationCollectorRef{
					Name: "gateway",
				},
			},
			Status: hubv1.TelemetryValidationStatus{
				ValidatorService:     "sample-fidelity-validator",
				ValidatorIngressPort: 8126,
			},
		},
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, validations)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	rawCluster, ok := clusters["gateway_mdai_sample_validator_4317"]
	require.True(t, ok, "gateway_mdai_sample_validator_4317 cluster not found")
	c, ok := rawCluster.(*cluster.Cluster)
	require.True(t, ok)
	lbEndpoints := c.GetLoadAssignment().GetEndpoints()
	require.NotEmpty(t, lbEndpoints)
	endpoints := lbEndpoints[0].GetLbEndpoints()
	require.NotEmpty(t, endpoints)
	socket := endpoints[0].GetEndpoint().GetAddress().GetSocketAddress()
	require.NotNil(t, socket)
	assert.Equal(t, "sample-fidelity-validator.mdai.svc.cluster.local", socket.GetAddress())
	assert.Equal(t, uint32(4317), socket.GetPortValue())
}

func TestUpdateSnapshotUsesUniqueMirrorClusterNamesPerValidation(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollector("gateway", "mdai", 4317),
	}
	validations := []hubv1.TelemetryValidation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample-a",
				Namespace: "mdai",
			},
			Spec: hubv1.TelemetryValidationSpec{
				Enabled: true,
				CollectorRef: hubv1.TelemetryValidationCollectorRef{
					Name: "gateway",
				},
			},
			Status: hubv1.TelemetryValidationStatus{
				ValidatorService: "sample-a-fidelity-validator",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample-b",
				Namespace: "mdai",
			},
			Spec: hubv1.TelemetryValidationSpec{
				Enabled: true,
				CollectorRef: hubv1.TelemetryValidationCollectorRef{
					Name: "gateway",
				},
			},
			Status: hubv1.TelemetryValidationStatus{
				ValidatorService: "sample-b-fidelity-validator",
			},
		},
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, validations)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	clusterA, ok := clusters["gateway_mdai_sample-a_validator_4317"]
	require.True(t, ok, "gateway_mdai_sample-a_validator_4317 cluster not found")
	clusterB, ok := clusters["gateway_mdai_sample-b_validator_4317"]
	require.True(t, ok, "gateway_mdai_sample-b_validator_4317 cluster not found")

	clusterAResource, ok := clusterA.(*cluster.Cluster)
	require.True(t, ok)
	clusterBResource, ok := clusterB.(*cluster.Cluster)
	require.True(t, ok)
	clusterAEndpoint := clusterAResource.GetLoadAssignment().GetEndpoints()[0].GetLbEndpoints()[0].GetEndpoint().GetAddress().GetSocketAddress()
	clusterBEndpoint := clusterBResource.GetLoadAssignment().GetEndpoints()[0].GetLbEndpoints()[0].GetEndpoint().GetAddress().GetSocketAddress()
	assert.Equal(t, "sample-a-fidelity-validator.mdai.svc.cluster.local", clusterAEndpoint.GetAddress())
	assert.Equal(t, "sample-b-fidelity-validator.mdai.svc.cluster.local", clusterBEndpoint.GetAddress())
}

func TestUpdateSnapshotSetsHTTP2UpstreamProtocolOptionsOnClusters(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollector("gateway", "mdai", 4317),
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, nil)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	rawCluster, ok := clusters["mdai_gateway_4317"]
	require.True(t, ok, "mdai_gateway_4317 cluster not found")
	c, ok := rawCluster.(*cluster.Cluster)
	require.True(t, ok)

	typedProtocolOptions := c.GetTypedExtensionProtocolOptions()
	require.Contains(t, typedProtocolOptions, httpProtocolOptionsTypedExtension)

	httpProtocolOptionsAny := typedProtocolOptions[httpProtocolOptionsTypedExtension]
	httpProtocolOptions := &upstreamhttp.HttpProtocolOptions{}
	require.NoError(t, httpProtocolOptionsAny.UnmarshalTo(httpProtocolOptions))
	require.NotNil(t, httpProtocolOptions.GetExplicitHttpConfig())
	require.NotNil(t, httpProtocolOptions.GetExplicitHttpConfig().GetHttp2ProtocolOptions())
}

func TestUpdateSnapshotDoesNotForceHTTP2ForNonGRPCPorts(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollectorWithProtocol("gateway", "mdai", "http", 4318),
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, nil)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	rawCluster, ok := clusters["mdai_gateway_4318"]
	require.True(t, ok, "mdai_gateway_4318 cluster not found")
	c, ok := rawCluster.(*cluster.Cluster)
	require.True(t, ok)

	typedProtocolOptions := c.GetTypedExtensionProtocolOptions()
	_, exists := typedProtocolOptions[httpProtocolOptionsTypedExtension]
	assert.False(t, exists, "non-gRPC cluster should not force HTTP/2 upstream protocol options")
}

func TestPrefixClusterName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "myconn__ns_name_4317", prefixClusterName("myconn", "ns_name_4317"))
	assert.Equal(t, "ns_name_4317", prefixClusterName("", "ns_name_4317"))
}

func TestClusterNameForPort(t *testing.T) {
	t.Parallel()

	cp := collectorPort{ns: "mdai", name: "gateway", port: 4317, mdaiConnection: ""}
	assert.Equal(t, "mdai_gateway_4317", clusterNameForPort(cp))

	cp.mdaiConnection = "my-hub"
	assert.Equal(t, "my-hub__mdai_gateway_4317", clusterNameForPort(cp))
}

func TestUpdateSnapshotPrefixesClusterNamesWithMdaiConnection(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollectorWithApp("gateway", "mdai", "my-hub"),
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, nil)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	_, ok = clusters["my-hub__mdai_gateway_4317"]
	assert.True(t, ok, "expected cluster my-hub__mdai_gateway_4317")
	_, unprefixed := clusters["mdai_gateway_4317"]
	assert.False(t, unprefixed, "unprefixed cluster mdai_gateway_4317 should not exist when app label is set")
}

func TestUpdateSnapshotPrefixesValidatorAndShadowClusterNamesWithMdaiConnection(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollectorWithApp("gateway", "mdai", "my-hub"),
	}
	validations := []hubv1.TelemetryValidation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample",
				Namespace: "mdai",
			},
			Spec: hubv1.TelemetryValidationSpec{
				Enabled: true,
				CollectorRef: hubv1.TelemetryValidationCollectorRef{
					Name: "gateway",
				},
			},
			Status: hubv1.TelemetryValidationStatus{
				ValidatorService: "sample-fidelity-validator",
			},
		},
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, validations)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)

	_, ok = clusters["my-hub__gateway_mdai_sample_validator_4317"]
	assert.True(t, ok, "expected prefixed validator cluster my-hub__gateway_mdai_sample_validator_4317")

	_, ok = clusters["my-hub__gateway_mdai_sample_shadow_4317"]
	assert.True(t, ok, "expected prefixed shadow cluster my-hub__gateway_mdai_sample_shadow_4317")
}

func TestUpdateSnapshotPrefixesWildcardDefaultClusterWithMdaiConnection(t *testing.T) {
	t.Parallel()

	manager := NewXDSManager()
	collectors := []otelv1beta1.OpenTelemetryCollector{
		newCollectorWithApp("alpha", "mdai", "my-hub"),
		newCollectorWithApp("beta", "mdai", "my-hub"),
	}

	err := manager.UpdateSnapshot(t.Context(), "envoy-hub-proxy", collectors, nil)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	listeners := concreteSnapshot.GetResources(resource.ListenerType)
	rawListener, ok := listeners["listener_4317"]
	require.True(t, ok, "listener_4317 not found")

	l, ok := rawListener.(*listener.Listener)
	require.True(t, ok)

	typedConfig := l.GetFilterChains()[0].GetFilters()[0].GetTypedConfig()
	require.NotNil(t, typedConfig)

	managerConfig := &hcm.HttpConnectionManager{}
	require.NoError(t, typedConfig.UnmarshalTo(managerConfig))

	wildcardFound := false
	for _, vHost := range managerConfig.GetRouteConfig().GetVirtualHosts() {
		for _, domain := range vHost.GetDomains() {
			if domain == "*" {
				wildcardFound = true
				gotCluster := vHost.GetRoutes()[0].GetRoute().GetCluster()
				assert.Equal(t, "my-hub__alpha_4317", gotCluster)
			}
		}
	}
	assert.True(t, wildcardFound, "expected wildcard virtual host")
}

func newCollector(name, namespace string, _ uint32) otelv1beta1.OpenTelemetryCollector {
	return newCollectorWithProtocol(name, namespace, "grpc", 4317)
}

func newCollectorWithApp(name, namespace, appLabel string) otelv1beta1.OpenTelemetryCollector {
	c := newCollectorWithProtocol(name, namespace, "grpc", 4317)
	c.Labels = map[string]string{"app": appLabel}
	return c
}

func newCollectorWithProtocol(name, namespace, protocol string, port uint32) otelv1beta1.OpenTelemetryCollector {
	return otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"otlp": map[string]any{
							"protocols": map[string]any{
								protocol: map[string]any{
									"endpoint": fmt.Sprintf(":%d", port),
								},
							},
						},
					},
				},
			},
		},
	}
}
