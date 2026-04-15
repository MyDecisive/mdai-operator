package controller

import (
	"context"
	"testing"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeXDSManager struct {
	called     bool
	collectors []otelv1beta1.OpenTelemetryCollector
}

func (f *fakeXDSManager) UpdateSnapshot(_ context.Context, _ string, collectors []otelv1beta1.OpenTelemetryCollector, _ []hubv1.TelemetryValidation) error {
	f.called = true
	f.collectors = append([]otelv1beta1.OpenTelemetryCollector(nil), collectors...)
	return nil
}

func TestXDSReconcileUpdatesManagedProxyServicePorts(t *testing.T) {
	t.Setenv(xdsProxyServiceNameEnv, "envoy-hub-proxy")
	t.Setenv(xdsProxyServiceNamespaceEnv, "mdai")

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))

	manager := &fakeXDSManager{}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "envoy-hub-proxy",
			Namespace: "mdai",
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "metrics",
					Port:       8443,
					TargetPort: intstr.FromInt32(8443),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "xds-9999",
					Port:       9999,
					TargetPort: intstr.FromInt32(9999),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	collector := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway",
			Namespace: "mdai",
			Labels: map[string]string{
				xdsRoleLabelKey: connectionCollectorRole,
			},
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"otlp": map[string]any{
							"protocols": map[string]any{
								"grpc": map[string]any{"endpoint": ":4317"},
								"http": map[string]any{"endpoint": "0.0.0.0:4318"},
							},
						},
					},
				},
			},
		},
	}
	collectorEndpoints := readyEndpointSlice("gateway-collector", "mdai", 4317, 4318)
	shadowCollector := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway-shadow",
			Namespace: "mdai",
			Labels: map[string]string{
				"hub.mydecisive.ai/shadow": "true",
			},
		},
		Spec: collector.Spec,
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(service, collector, shadowCollector, collectorEndpoints).
		Build()

	reconciler := &XDSReconciler{
		Client:     cl,
		Scheme:     scheme,
		XDSManager: manager,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called")

	updated := &corev1.Service{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Name:      "envoy-hub-proxy",
		Namespace: "mdai",
	}, updated))

	assert.Equal(t, []corev1.ServicePort{
		{
			Name:       "xds-4317",
			Port:       4317,
			TargetPort: intstr.FromInt32(4317),
			Protocol:   corev1.ProtocolTCP,
		},
		{
			Name:       "xds-4318",
			Port:       4318,
			TargetPort: intstr.FromInt32(4318),
			Protocol:   corev1.ProtocolTCP,
		},
		{
			Name:       "metrics",
			Port:       8443,
			TargetPort: intstr.FromInt32(8443),
			Protocol:   corev1.ProtocolTCP,
		},
	}, updated.Spec.Ports)
}

func TestXDSReconcileCreatesProxyServiceFromMarkedDeployment(t *testing.T) {
	t.Setenv(xdsProxyServiceNameEnv, "")
	t.Setenv(xdsProxyServiceNamespaceEnv, "")

	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))

	manager := &fakeXDSManager{}
	collector := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway",
			Namespace: "mdai",
			Labels: map[string]string{
				xdsRoleLabelKey: connectionCollectorRole,
			},
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"otlp": map[string]any{
							"protocols": map[string]any{
								"grpc": map[string]any{"endpoint": ":4317"},
							},
						},
					},
				},
			},
		},
	}
	collectorEndpoints := readyEndpointSlice("gateway-collector", "mdai", 4317)
	envoyDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "envoy-hub",
			Namespace: "mdai",
			Labels: map[string]string{
				xdsProxyMarkerKey: "true",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "envoy-hub",
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(collector, envoyDeployment, collectorEndpoints).
		Build()

	reconciler := &XDSReconciler{
		Client:     cl,
		Scheme:     scheme,
		XDSManager: manager,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called")

	created := &corev1.Service{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Name:      "envoy-hub-xds-proxy",
		Namespace: "mdai",
	}, created))

	assert.Equal(t, map[string]string{"app": "envoy-hub"}, created.Spec.Selector)
	assert.Equal(t, []corev1.ServicePort{
		{
			Name:       "xds-4317",
			Port:       4317,
			TargetPort: intstr.FromInt32(4317),
			Protocol:   corev1.ProtocolTCP,
		},
	}, created.Spec.Ports)
}

func TestXDSReconcileCreatesProxyServiceFromEnvWhenMissing(t *testing.T) {
	t.Setenv(xdsProxyServiceNameEnv, "envoy-hub-proxy")
	t.Setenv(xdsProxyServiceNamespaceEnv, "mdai")
	t.Setenv(xdsProxyServiceSelectorEnv, "app=envoy,component=proxy")

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))

	manager := &fakeXDSManager{}
	collector := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway",
			Namespace: "mdai",
			Labels: map[string]string{
				xdsRoleLabelKey: connectionCollectorRole,
			},
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"otlp": map[string]any{
							"protocols": map[string]any{
								"grpc": map[string]any{"endpoint": ":4317"},
							},
						},
					},
				},
			},
		},
	}
	collectorEndpoints := readyEndpointSlice("gateway-collector", "mdai", 4317)

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(collector, collectorEndpoints).
		Build()

	reconciler := &XDSReconciler{
		Client:     cl,
		Scheme:     scheme,
		XDSManager: manager,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called")

	created := &corev1.Service{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Name:      "envoy-hub-proxy",
		Namespace: "mdai",
	}, created))
	assert.Equal(t, map[string]string{"app": "envoy", "component": "proxy"}, created.Spec.Selector)
	assert.Equal(t, []corev1.ServicePort{
		{
			Name:       "xds-4317",
			Port:       4317,
			TargetPort: intstr.FromInt32(4317),
			Protocol:   corev1.ProtocolTCP,
		},
	}, created.Spec.Ports)
}

func TestDiscoveredCollectorPortsFallsBackToDefaultPorts(t *testing.T) {
	collector := otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway",
			Namespace: "mdai",
			Labels: map[string]string{
				xdsRoleLabelKey: connectionCollectorRole,
			},
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"nop": map[string]any{},
					},
				},
			},
		},
	}

	assert.Equal(t, []uint32{4317, 4318}, discoveredCollectorPorts([]otelv1beta1.OpenTelemetryCollector{collector}))
}

func TestXDSReconcileReconcilesServicePortsWithoutEndpointReadiness(t *testing.T) {
	t.Setenv(xdsProxyServiceNameEnv, "envoy-hub-proxy")
	t.Setenv(xdsProxyServiceNamespaceEnv, "mdai")
	t.Setenv(xdsProxyServiceSelectorEnv, "app=envoy,component=proxy")

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))

	manager := &fakeXDSManager{}
	collector := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway",
			Namespace: "mdai",
			Labels: map[string]string{
				xdsRoleLabelKey: connectionCollectorRole,
			},
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"otlp": map[string]any{
							"protocols": map[string]any{
								"grpc": map[string]any{"endpoint": ":4317"},
							},
						},
					},
				},
			},
		},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(collector).
		Build()

	reconciler := &XDSReconciler{
		Client:     cl,
		Scheme:     scheme,
		XDSManager: manager,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called")

	service := &corev1.Service{}
	err = cl.Get(context.Background(), types.NamespacedName{Name: "envoy-hub-proxy", Namespace: "mdai"}, service)
	require.NoError(t, err, "service should be created even without endpoint readiness checks")
	require.Len(t, service.Spec.Ports, 1)
	assert.Equal(t, int32(4317), service.Spec.Ports[0].Port)
}

func TestXDSReconcileIgnoresCollectorsWithoutConnectionCollectorRole(t *testing.T) {
	t.Setenv(xdsProxyServiceNameEnv, "envoy-hub-proxy")
	t.Setenv(xdsProxyServiceNamespaceEnv, "mdai")
	t.Setenv(xdsProxyServiceSelectorEnv, "app=envoy,component=proxy")

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))

	manager := &fakeXDSManager{}
	collector := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway",
			Namespace: "mdai",
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"otlp": map[string]any{
							"protocols": map[string]any{
								"grpc": map[string]any{"endpoint": ":4317"},
							},
						},
					},
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(collector).
		Build()

	reconciler := &XDSReconciler{
		Client:     cl,
		Scheme:     scheme,
		XDSManager: manager,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called even with no eligible collectors")
	assert.Empty(t, manager.collectors, "unlabeled collectors should not be included in xDS snapshot updates")

	service := &corev1.Service{}
	err = cl.Get(context.Background(), types.NamespacedName{Name: "envoy-hub-proxy", Namespace: "mdai"}, service)
	require.True(t, apierrors.IsNotFound(err), "proxy service should not be created when no eligible collectors exist")
}

func readyEndpointSlice(serviceName, namespace string, ports ...int32) *discoveryv1.EndpointSlice {
	ready := true
	slicePorts := make([]discoveryv1.EndpointPort, 0, len(ports))
	for _, port := range ports {
		p := port
		slicePorts = append(slicePorts, discoveryv1.EndpointPort{Port: &p, Protocol: ptrTo(corev1.ProtocolTCP)})
	}
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName + "-slice",
			Namespace: namespace,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: serviceName,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		}},
		Ports: slicePorts,
	}
}

func notReadyEndpointSlice(serviceName, namespace string, ports ...int32) *discoveryv1.EndpointSlice {
	ready := false
	slicePorts := make([]discoveryv1.EndpointPort, 0, len(ports))
	for _, port := range ports {
		p := port
		slicePorts = append(slicePorts, discoveryv1.EndpointPort{Port: &p, Protocol: ptrTo(corev1.ProtocolTCP)})
	}
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName + "-slice-not-ready",
			Namespace: namespace,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: serviceName,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		}},
		Ports: slicePorts,
	}
}

func ptrTo[T any](v T) *T {
	return &v
}
