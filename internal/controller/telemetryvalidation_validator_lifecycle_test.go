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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileValidatorLifecycle(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))
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

	assertObjectExists(t, c, &corev1.ConfigMap{}, types.NamespacedName{Name: "sample-fidelity-validator-config", Namespace: "mdai"})
	svc := &corev1.Service{}
	assertObjectExists(t, c, svc, types.NamespacedName{Name: "sample-fidelity-validator", Namespace: "mdai"})
	assertServicePort(t, svc.Spec.Ports, "receiver-18126", 18126)
	assertServicePort(t, svc.Spec.Ports, "receiver-4317", 4317)
	assertServicePort(t, svc.Spec.Ports, "exporter-intake", 19081)
	deploy := &appsv1.Deployment{}
	assertObjectExists(t, c, deploy, types.NamespacedName{Name: "sample-fidelity-validator", Namespace: "mdai"})
	assertDeploymentEnvVar(t, deploy, "MDAI_DATADOG_AGENT_INGEST_ADDR", ":18126")

	tv.Spec.Enabled = false
	validatorName, validatorService, validatorEndpoint, err = r.reconcileValidator(context.Background(), tv)
	require.NoError(t, err)
	assert.Empty(t, validatorName)
	assert.Empty(t, validatorService)
	assert.Empty(t, validatorEndpoint)

	assertObjectNotFound(t, c, &corev1.ConfigMap{}, types.NamespacedName{Name: "sample-fidelity-validator-config", Namespace: "mdai"})
	assertObjectNotFound(t, c, &corev1.Service{}, types.NamespacedName{Name: "sample-fidelity-validator", Namespace: "mdai"})
	assertObjectNotFound(t, c, &appsv1.Deployment{}, types.NamespacedName{Name: "sample-fidelity-validator", Namespace: "mdai"})
}

func TestReconcileValidatorLifecycleUsesEmbeddedDefaultsWhenValidatorConfigEmpty(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, hubv1.AddToScheme(scheme))
	require.NoError(t, otelv1beta1.AddToScheme(scheme))
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
	assert.Contains(t, cfg.Data["rules.yaml"], "signals:")
	assert.Contains(t, cfg.Data["rules.yaml"], "required_attributes:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "signals:")
	assert.Contains(t, cfg.Data["field-mapping.yaml"], "correlation_id:")
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
