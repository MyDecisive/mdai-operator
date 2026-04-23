package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	prometheusv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

func TestReconcileValidatorLifecycle(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))
	require.NoError(t, prometheusv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	tv := &hubv1.TelemetryValidation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample",
			Namespace: "mdai",
		},
		Spec: hubv1.TelemetryValidationSpec{
			Enabled: true,
			CollectorRef: hubv1.TelemetryValidationCollectorRef{
				Name: "gateway",
			},
			Validator: hubv1.TelemetryValidationValidatorSpec{
				Image:            "validator:test",
				Port:             19081,
				RulesYAML:        "rules: []",
				FieldMappingYAML: "mappings: []",
			},
		},
	}
	collector := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway",
			Namespace: "mdai",
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"datadog": map[string]any{
							"endpoint": ":18126",
						},
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

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tv, collector).Build()
	r := &TelemetryValidationReconciler{Client: c, Scheme: scheme}

	validatorName, validatorService, validatorEndpoint, err := r.reconcileValidator(context.Background(), tv)
	require.NoError(t, err)
	assert.Equal(t, "sample-fidelity-validator", validatorName)
	assert.Equal(t, "sample-fidelity-validator", validatorService)
	assert.Equal(t, "http://sample-fidelity-validator.mdai.svc.cluster.local:19081", validatorEndpoint)

	cfg := &corev1.ConfigMap{}
	assertObjectExists(t, c, cfg, types.NamespacedName{Name: "sample-fidelity-validator-config", Namespace: "mdai"})
	assert.Equal(t, telemetryValidationRoleValidator, cfg.Labels["hub.mydecisive.ai/role"])
	assert.Equal(t, "sample", cfg.Labels[telemetryValidationLabelKey])
	svc := &corev1.Service{}
	assertObjectExists(t, c, svc, types.NamespacedName{Name: "sample-fidelity-validator", Namespace: "mdai"})
	assert.Equal(t, telemetryValidationRoleValidator, svc.Labels["hub.mydecisive.ai/role"])
	assert.Equal(t, "sample", svc.Labels[telemetryValidationLabelKey])
	assertServicePort(t, svc.Spec.Ports, "receiver-18126", 18126)
	assertServicePort(t, svc.Spec.Ports, "receiver-4317", 4317)
	assertServicePort(t, svc.Spec.Ports, "exporter-intake", 19081)
	assertServicePort(t, svc.Spec.Ports, otelMetricsName, otelMetricsPort)
	monitor := &prometheusv1.ServiceMonitor{}
	assertObjectExists(t, c, monitor, types.NamespacedName{Name: "sample-fidelity-validator-metrics", Namespace: "mdai"})
	assert.Equal(t, telemetryValidationRoleValidator, monitor.Labels["hub.mydecisive.ai/role"])
	assert.Equal(t, "sample", monitor.Labels[telemetryValidationLabelKey])
	require.Len(t, monitor.Spec.Endpoints, 1)
	assert.Equal(t, otelMetricsName, monitor.Spec.Endpoints[0].Port)
	assert.Equal(t, "/metrics", monitor.Spec.Endpoints[0].Path)
	assert.True(t, monitor.Spec.Endpoints[0].HonorLabels)
	require.Len(t, monitor.Spec.Endpoints[0].RelabelConfigs, 1)
	assert.Equal(t, "mdai_connection", monitor.Spec.Endpoints[0].RelabelConfigs[0].TargetLabel)
	assert.Equal(t, "replace", monitor.Spec.Endpoints[0].RelabelConfigs[0].Action)
	assert.Equal(t, []prometheusv1.LabelName{"__meta_kubernetes_service_label_hub_mydecisive_ai_telemetry_validation"}, monitor.Spec.Endpoints[0].RelabelConfigs[0].SourceLabels)
	assert.Equal(t, []string{"mdai"}, monitor.Spec.NamespaceSelector.MatchNames)
	assert.Equal(t, map[string]string{
		telemetryValidationLabelKey: "sample",
		"hub.mydecisive.ai/role":    telemetryValidationRoleValidator,
	}, monitor.Spec.Selector.MatchLabels)
	deploy := &appsv1.Deployment{}
	assertObjectExists(t, c, deploy, types.NamespacedName{Name: "sample-fidelity-validator", Namespace: "mdai"})
	assert.Equal(t, telemetryValidationRoleValidator, deploy.Labels["hub.mydecisive.ai/role"])
	assert.Equal(t, "sample", deploy.Labels[telemetryValidationLabelKey])
	assertDeploymentContainerPort(t, deploy, otelMetricsName, otelMetricsPort)
	assertDeploymentEnvVar(t, deploy, "MDAI_DATADOG_AGENT_INGEST_ADDR", ":18126")

	tv.Spec.Enabled = false
	validatorName, validatorService, validatorEndpoint, err = r.reconcileValidator(context.Background(), tv)
	require.NoError(t, err)
	assert.Empty(t, validatorName)
	assert.Empty(t, validatorService)
	assert.Empty(t, validatorEndpoint)

	assertObjectNotFound(t, c, &corev1.ConfigMap{}, types.NamespacedName{Name: "sample-fidelity-validator-config", Namespace: "mdai"})
	assertObjectNotFound(t, c, &corev1.Service{}, types.NamespacedName{Name: "sample-fidelity-validator", Namespace: "mdai"})
	assertObjectNotFound(t, c, &prometheusv1.ServiceMonitor{}, types.NamespacedName{Name: "sample-fidelity-validator-metrics", Namespace: "mdai"})
	assertObjectNotFound(t, c, &appsv1.Deployment{}, types.NamespacedName{Name: "sample-fidelity-validator", Namespace: "mdai"})
}

func TestReconcileValidatorLifecycleUsesEmbeddedDefaultsWhenValidatorConfigEmpty(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))
	require.NoError(t, prometheusv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	tv := &hubv1.TelemetryValidation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample",
			Namespace: "mdai",
		},
		Spec: hubv1.TelemetryValidationSpec{
			Enabled: true,
			CollectorRef: hubv1.TelemetryValidationCollectorRef{
				Name: "gateway",
			},
			Validator: hubv1.TelemetryValidationValidatorSpec{},
		},
	}
	collector := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway",
			Namespace: "mdai",
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"datadog": map[string]any{"endpoint": ":8126"},
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tv, collector).Build()
	r := &TelemetryValidationReconciler{Client: c, Scheme: scheme}

	_, _, validatorEndpoint, err := r.reconcileValidator(context.Background(), tv)
	require.NoError(t, err)
	assert.NotEmpty(t, validatorEndpoint)

	cfg := &corev1.ConfigMap{}
	assertObjectExists(t, c, cfg, types.NamespacedName{Name: "sample-fidelity-validator-config", Namespace: "mdai"})
	rulesYAML := cfg.Data["rules.yaml"]
	traceRulesSection := rulesYAML
	tracesIdx := strings.Index(traceRulesSection, "\n  traces:")
	require.NotEqual(t, -1, tracesIdx)
	metricsRulesSection := traceRulesSection[:tracesIdx]
	logsIdx := strings.Index(traceRulesSection, "\n  logs:")
	require.NotEqual(t, -1, logsIdx)
	traceRulesSection = traceRulesSection[tracesIdx:logsIdx]
	assert.Contains(t, rulesYAML, "signals:")
	assert.Contains(t, rulesYAML, "required_attributes:")
	assert.Contains(t, metricsRulesSection, "- metric_name")
	assert.Contains(t, metricsRulesSection, "- point_timestamp")
	assert.Contains(t, metricsRulesSection, "- point_value")
	assert.Contains(t, metricsRulesSection, "- service")
	assert.Contains(t, metricsRulesSection, "- env")
	assert.NotContains(t, metricsRulesSection, "\n      - host\n")
	assert.NotContains(t, metricsRulesSection, "\n      - metric\n")
	assert.NotContains(t, metricsRulesSection, "\n      - tags\n")
	assert.NotContains(t, metricsRulesSection, "\n      - type\n")
	assert.Contains(t, traceRulesSection, "- env")
	assert.Contains(t, traceRulesSection, "- operation_name")
	assert.Contains(t, traceRulesSection, "- resource_name")
	assert.NotContains(t, traceRulesSection, "- status")
	assert.NotContains(t, traceRulesSection, "- ingestion_reason")
	assert.NotContains(t, traceRulesSection, "span_count")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "signals:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "correlation_id:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "\"contains:.tags[|tag:correlation_id\"")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "operation_name:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "resource_name:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "ingestion_reason:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "exporters:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "datadog:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "operation_name:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "\"suffix:.name\"")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "\"suffix:.resource\"")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "\"suffix:.meta.dd.span.Resource\"")
}

func TestReconcileCreatesShadowCollectorLabels(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))
	require.NoError(t, prometheusv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	tv := &hubv1.TelemetryValidation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample",
			Namespace: "mdai",
		},
		Spec: hubv1.TelemetryValidationSpec{
			Enabled: true,
			CollectorRef: hubv1.TelemetryValidationCollectorRef{
				Name: "gateway",
			},
			Validator: hubv1.TelemetryValidationValidatorSpec{
				Port: 19081,
			},
		},
	}
	collector := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gateway",
			Namespace: "mdai",
			Labels: map[string]string{
				"existing-label": "kept",
			},
			Annotations: map[string]string{
				"existing-annotation": "kept",
			},
		},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			Config: otelv1beta1.Config{
				Receivers: otelv1beta1.AnyConfig{
					Object: map[string]any{
						"datadog": map[string]any{
							"endpoint": ":18126",
						},
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(tv).
		WithObjects(tv, collector).
		Build()
	r := &TelemetryValidationReconciler{Client: c, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: tv.Name, Namespace: tv.Namespace},
	})
	require.NoError(t, err)

	shadow := &otelv1beta1.OpenTelemetryCollector{}
	assertObjectExists(t, c, shadow, types.NamespacedName{Name: shadowCollectorName("gateway"), Namespace: "mdai"})
	assert.Equal(t, telemetryValidationRoleShadow, shadow.Labels["hub.mydecisive.ai/role"])
	assert.Equal(t, "sample", shadow.Labels[telemetryValidationLabelKey])
	assert.Equal(t, "true", shadow.Labels["hub.mydecisive.ai/shadow"])
	assert.Equal(t, "gateway", shadow.Labels["hub.mydecisive.ai/source"])
	assert.Equal(t, "kept", shadow.Labels["existing-label"])
	assert.Equal(t, "true", shadow.Annotations["hub.mydecisive.ai/shadow"])
	assert.Equal(t, "kept", shadow.Annotations["existing-annotation"])
}

func assertObjectExists(t *testing.T, c client.Client, obj client.Object, key types.NamespacedName) {
	t.Helper()
	err := c.Get(context.Background(), key, obj)
	require.NoError(t, err)
}

func assertObjectNotFound(t *testing.T, c client.Client, obj client.Object, key types.NamespacedName) {
	t.Helper()
	err := c.Get(context.Background(), key, obj)
	require.Error(t, err)
}

func assertServicePort(t *testing.T, ports []corev1.ServicePort, name string, port int32) {
	t.Helper()
	for _, p := range ports {
		if p.Name == name {
			assert.Equal(t, port, p.Port)
			return
		}
	}
	t.Fatalf("service port %q not found", name)
}

func assertDeploymentEnvVar(t *testing.T, deploy *appsv1.Deployment, name, expected string) {
	t.Helper()
	for _, c := range deploy.Spec.Template.Spec.Containers {
		for _, env := range c.Env {
			if env.Name == name {
				assert.Equal(t, expected, env.Value)
				return
			}
		}
	}
	t.Fatalf("deployment env var %q not found", name)
}

func assertDeploymentContainerPort(t *testing.T, deploy *appsv1.Deployment, name string, port int32) {
	t.Helper()
	for _, c := range deploy.Spec.Template.Spec.Containers {
		for _, p := range c.Ports {
			if p.Name == name {
				assert.Equal(t, port, p.ContainerPort)
				return
			}
		}
	}
	t.Fatalf("deployment container port %q not found", name)
}
