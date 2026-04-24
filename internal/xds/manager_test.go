package xds

import (
	"context"
	"testing"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
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

	err := manager.UpdateSnapshot(context.Background(), "envoy-hub-proxy", collectors, nil)
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
				IngressCapture: hubv1.TelemetryValidationIngressCaptureSpec{
					Enabled: true,
				},
			},
			Status: hubv1.TelemetryValidationStatus{
				ValidatorService: "sample-fidelity-validator",
			},
		},
	}

	err := manager.UpdateSnapshot(context.Background(), "envoy-hub-proxy", collectors, validations)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	rawCluster, ok := clusters["gateway_validator_4317"]
	require.True(t, ok, "gateway_validator_4317 cluster not found")
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
				IngressCapture: hubv1.TelemetryValidationIngressCaptureSpec{
					Enabled: true,
				},
			},
		},
	}

	err := manager.UpdateSnapshot(context.Background(), "envoy-hub-proxy", collectors, validations)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	_, exists := clusters["gateway_validator_4317"]
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
				IngressCapture: hubv1.TelemetryValidationIngressCaptureSpec{
					Enabled: true,
				},
			},
			Status: hubv1.TelemetryValidationStatus{
				ValidatorService:     "sample-fidelity-validator",
				ValidatorIngressPort: 8126,
			},
		},
	}

	err := manager.UpdateSnapshot(context.Background(), "envoy-hub-proxy", collectors, validations)
	require.NoError(t, err)

	snapshot, err := manager.cache.GetSnapshot("envoy-hub-proxy")
	require.NoError(t, err)
	concreteSnapshot, ok := snapshot.(*cachev3.Snapshot)
	require.True(t, ok)

	clusters := concreteSnapshot.GetResources(resource.ClusterType)
	rawCluster, ok := clusters["gateway_validator_4317"]
	require.True(t, ok, "gateway_validator_4317 cluster not found")
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

func newCollector(name, namespace string, _ uint32) otelv1beta1.OpenTelemetryCollector {
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
								"grpc": map[string]any{
									"endpoint": ":4317",
								},
							},
						},
					},
				},
			},
		},
	}
}
