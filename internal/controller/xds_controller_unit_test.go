package controller

import (
	"context"
	"errors"
	"testing"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/mydecisive/mdai-operator/internal/collectorconfig"
	"github.com/mydecisive/mdai-operator/internal/xds"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type fakeXDSManager struct {
	called     bool
	collectors []xds.CollectorPorts
}

func (f *fakeXDSManager) UpdateSnapshot(
	_ context.Context,
	_ string,
	collectors []xds.CollectorPorts,
	_ []hubv1.TelemetryValidation,
) error {
	f.called = true
	f.collectors = append([]xds.CollectorPorts(nil), collectors...)
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

	ctx := t.Context()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called")

	updated := &corev1.Service{}
	require.NoError(t, cl.Get(ctx, types.NamespacedName{
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

func TestXDSReconcileDoesNotAddManagedPortWhenServiceAlreadyExposesSamePort(t *testing.T) {
	t.Setenv(xdsProxyServiceNameEnv, "envoy-proxy")
	t.Setenv(xdsProxyServiceNamespaceEnv, "mdai")

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))

	manager := &fakeXDSManager{}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "envoy-proxy",
			Namespace: "mdai",
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port:       8126,
					TargetPort: intstr.FromInt32(8126),
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
						"datadog": map[string]any{
							"endpoint": ":8126",
						},
					},
				},
			},
		},
	}
	collectorEndpoints := readyEndpointSlice("gateway-collector", "mdai", 8126)

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(service, collector, collectorEndpoints).
		Build()

	reconciler := &XDSReconciler{
		Client:     cl,
		Scheme:     scheme,
		XDSManager: manager,
	}

	ctx := t.Context()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called")

	updated := &corev1.Service{}
	require.NoError(t, cl.Get(ctx, types.NamespacedName{
		Name:      "envoy-proxy",
		Namespace: "mdai",
	}, updated))

	require.Len(t, updated.Spec.Ports, 1)
	assert.Equal(t, int32(8126), updated.Spec.Ports[0].Port)
	assert.Empty(t, updated.Spec.Ports[0].Name)
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

	ctx := t.Context()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called")

	created := &corev1.Service{}
	require.NoError(t, cl.Get(ctx, types.NamespacedName{
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

	ctx := t.Context()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called")

	created := &corev1.Service{}
	require.NoError(t, cl.Get(ctx, types.NamespacedName{
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

func TestResolveCollectorPortsFallsBackToDefaultPorts(t *testing.T) {
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

	resolved, err := resolveCollectorPorts(t.Context(), nil, []otelv1beta1.OpenTelemetryCollector{collector})
	require.NoError(t, err)
	assert.Equal(t, []uint32{4317, 4318}, discoveredCollectorPorts(resolved))
}

func TestResolveCollectorPortsResolvesEnvironmentEndpointPorts(t *testing.T) {
	tests := []struct {
		name    string
		env     []corev1.EnvVar
		envFrom []corev1.EnvFromSource
		objects []client.Object
		want    []uint32
	}{
		{
			name: "spec env",
			env:  []corev1.EnvVar{{Name: "PORT", Value: "8126"}},
			want: []uint32{8126},
		},
		{
			name: "spec envFrom configmap",
			envFrom: []corev1.EnvFromSource{{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "collector-ports"},
				},
			}},
			objects: []client.Object{&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "collector-ports", Namespace: "mdai"},
				Data:       map[string]string{"PORT": "14268"},
			}},
			want: []uint32{14268},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()
			collector := otelv1beta1.OpenTelemetryCollector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "gateway",
					Namespace: "mdai",
					Labels: map[string]string{
						xdsRoleLabelKey: connectionCollectorRole,
					},
				},
				Spec: otelv1beta1.OpenTelemetryCollectorSpec{
					OpenTelemetryCommonFields: otelv1beta1.OpenTelemetryCommonFields{
						Env:     tt.env,
						EnvFrom: tt.envFrom,
					},
					Config: otelv1beta1.Config{
						Receivers: otelv1beta1.AnyConfig{
							Object: map[string]any{
								"datadog": map[string]any{
									"endpoint": "0.0.0.0:${env:PORT}",
								},
							},
						},
					},
				},
			}

			resolved, err := resolveCollectorPorts(t.Context(), cl, []otelv1beta1.OpenTelemetryCollector{collector})
			require.NoError(t, err)
			assert.Equal(t, tt.want, discoveredCollectorPorts(resolved))
		})
	}
}

func TestResolveCollectorPortsIsolatesUnresolvableCollector(t *testing.T) {
	good := xdsCollectorWithReceivers("good", nil, map[string]any{
		"otlp": map[string]any{"protocols": map[string]any{
			"grpc": map[string]any{"endpoint": ":4317"},
		}},
	})
	partial := xdsCollectorWithReceivers("partial", nil, map[string]any{
		"datadog": map[string]any{"endpoint": "0.0.0.0:${env:MISSING_PORT}"},
		"zipkin":  map[string]any{"endpoint": ":9411"},
	})
	broken := xdsCollectorWithReceivers("broken", nil, map[string]any{
		"datadog": map[string]any{"endpoint": "0.0.0.0:${env:MISSING_PORT}"},
	})

	resolved, err := resolveCollectorPorts(t.Context(), nil, []otelv1beta1.OpenTelemetryCollector{good, partial, broken})
	require.ErrorIs(t, err, collectorconfig.ErrUnresolvedEndpoint)
	require.ErrorContains(t, err, "collector mdai/partial")
	require.ErrorContains(t, err, "collector mdai/broken")
	require.NotContains(t, err.Error(), "collector mdai/good")
	require.Len(t, resolved, 3)

	assert.Equal(t, "good", resolved[0].Collector.Name)
	assert.Equal(t, []uint32{4317}, collectorconfig.ReceiverPortNumbers(resolved[0].Ports))
	assert.Equal(t, "partial", resolved[1].Collector.Name)
	assert.Equal(t, []uint32{9411}, collectorconfig.ReceiverPortNumbers(resolved[1].Ports),
		"resolvable endpoints are kept when another endpoint cannot be resolved")
	assert.Equal(t, "broken", resolved[2].Collector.Name)
	assert.Equal(t, collectorconfig.DefaultReceiverPorts(), resolved[2].Ports,
		"a collector with no resolvable endpoints falls back to the OTLP defaults")
}

func TestResolveCollectorPortsReturnsTransientReadErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	readErr := errors.New("apiserver unavailable")
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return readErr
			},
		}).
		Build()
	collector := xdsCollectorWithReceivers("gateway", []corev1.EnvFromSource{{
		ConfigMapRef: &corev1.ConfigMapEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "collector-ports"},
		},
	}}, map[string]any{
		"datadog": map[string]any{"endpoint": "0.0.0.0:${env:PORT}"},
	})

	_, err := resolveCollectorPorts(t.Context(), cl, []otelv1beta1.OpenTelemetryCollector{collector})
	require.ErrorIs(t, err, readErr)
	assert.NotErrorIs(t, err, collectorconfig.ErrUnresolvedEndpoint)
}

func TestXDSReconcileUpdatesSnapshotWhenOneCollectorIsUnresolvable(t *testing.T) {
	t.Setenv(xdsProxyServiceNameEnv, "envoy-hub-proxy")
	t.Setenv(xdsProxyServiceNamespaceEnv, "mdai")
	t.Setenv(xdsProxyServiceSelectorEnv, "app=envoy,component=proxy")

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))

	good := xdsCollectorWithReceivers("good", nil, map[string]any{
		"otlp": map[string]any{"protocols": map[string]any{
			"grpc": map[string]any{"endpoint": ":4317"},
		}},
	})
	broken := xdsCollectorWithReceivers("broken", nil, map[string]any{
		"datadog": map[string]any{"endpoint": "0.0.0.0:${env:MISSING_PORT}"},
	})
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&good, &broken).
		Build()

	manager := &fakeXDSManager{}
	reconciler := &XDSReconciler{
		Client:     cl,
		Scheme:     scheme,
		XDSManager: manager,
	}

	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "broken", Namespace: "mdai"},
	})
	require.NoError(t, err)
	assert.Equal(t, unresolvedEndpointRetryInterval, result.RequeueAfter,
		"reconcile should be retried until the missing env source exists")
	require.True(t, manager.called, "xDS manager should be called")

	names := make([]string, 0, len(manager.collectors))
	for _, c := range manager.collectors {
		names = append(names, c.Collector.Name)
	}
	assert.ElementsMatch(t, []string{"good", "broken"}, names)
}

func xdsCollectorWithReceivers(name string, envFrom []corev1.EnvFromSource, receivers map[string]any) otelv1beta1.OpenTelemetryCollector {
	return otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "mdai",
			Labels: map[string]string{
				xdsRoleLabelKey: connectionCollectorRole,
			},
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			OpenTelemetryCommonFields: otelv1beta1.OpenTelemetryCommonFields{
				EnvFrom: envFrom,
			},
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{Object: receivers},
			},
		},
	}
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

	ctx := t.Context()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called")

	service := &corev1.Service{}
	err = cl.Get(ctx, types.NamespacedName{Name: "envoy-hub-proxy", Namespace: "mdai"}, service)
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

	ctx := t.Context()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "gateway", Namespace: "mdai"},
	})
	require.NoError(t, err)
	require.True(t, manager.called, "xDS manager should be called even with no eligible collectors")
	assert.Empty(t, manager.collectors, "unlabeled collectors should not be included in xDS snapshot updates")

	service := &corev1.Service{}
	err = cl.Get(ctx, types.NamespacedName{Name: "envoy-hub-proxy", Namespace: "mdai"}, service)
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

func ptrTo[T any](v T) *T {
	return new(v)
}
