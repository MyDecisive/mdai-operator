package controller

import (
	"context"
	"maps"
	"slices"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type TelemetryValidationReconciler struct {
	client.Client

	Scheme *runtime.Scheme
}

const (
	correlationProcessorName               = "attributes/correlation_id"
	correlationResourceProcessorName       = "resource/correlation_id"
	correlationDDTagsProcessorName         = "transform/correlation_ddtags"
	correlationMetricsProcessorName        = "transform/metrics_correlation_id"
	correlationAttributeKey                = "correlation_id"
	correlationHeaderFromCtxKey            = "metadata.x-correlation-id"
	correlationDDTagKey                    = "correlation_id:"
	setDDTagsOnlyStatement                 = `set(attributes["ddtags"], Concat(["%s", attributes["%s"]], "")) where attributes["%s"] != nil and attributes["ddtags"] == nil`
	appendDDTagsStatement                  = `set(attributes["ddtags"], Concat([attributes["ddtags"], ",", "%s", attributes["%s"]], "")) where attributes["%s"] != nil and attributes["ddtags"] != nil`
	setMetricCorrelationStatement          = `set(attributes["%s"], resource.attributes["%s"]) where attributes["%s"] == nil and resource.attributes["%s"] != nil`
	deleteMetricDDTagsStatement            = `delete_key(attributes, "ddtags") where attributes["ddtags"] != nil`
	deleteMetricTagsStatement              = `delete_key(attributes, "tags") where attributes["tags"] != nil`
	defaultValidatorImage                  = "ghcr.io/mydecisive/mdai-fidelity-validator:0.1.0"
	defaultValidatorPort             int32 = 18081
	defaultValidatorReceiverPort     int32 = 8126
	defaultValidatorReplicas         int32 = 1
	telemetryValidationLabelKey            = "hub.mydecisive.ai/telemetry-validation"
	telemetryValidationRoleShadow          = "telemetry-validation-shadow-collector"
	telemetryValidationRoleValidator       = "telemetry-validation-validator"
)

// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations/finalizers,verbs=update
// +kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete

func (r *TelemetryValidationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var validation hubv1.TelemetryValidation
	if err := r.Get(ctx, req.NamespacedName, &validation); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	validatorName, validatorServiceName, resolvedValidatorEndpoint, err := r.reconcileValidator(ctx, &validation)
	if err != nil {
		return ctrl.Result{}, err
	}
	validatorIngressPort, _, err := r.resolveValidatorIngressPorts(ctx, &validation)
	if err != nil {
		return ctrl.Result{}, err
	}

	validatorIngressPortStatus := int32(0)
	if validation.Spec.Enabled {
		validatorIngressPortStatus = validatorIngressPort
	}

	return r.reconcileShadowCollector(
		ctx,
		&validation,
		validatorName,
		validatorServiceName,
		resolvedValidatorEndpoint,
		validatorIngressPortStatus,
	)
}

func (r *TelemetryValidationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hubv1.TelemetryValidation{}).
		Owns(&otelv1beta1.OpenTelemetryCollector{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}

func activeSignals(signals []hubv1.TelemetrySignal) []hubv1.TelemetrySignal {
	if len(signals) == 0 {
		return []hubv1.TelemetrySignal{
			hubv1.TelemetrySignalMetrics,
			hubv1.TelemetrySignalLogs,
			hubv1.TelemetrySignalTraces,
		}
	}

	unique := make([]hubv1.TelemetrySignal, 0, len(signals))
	for _, signal := range signals {
		if !slices.Contains(unique, signal) {
			unique = append(unique, signal)
		}
	}

	return unique
}

func setValidationCondition(conditions *[]metav1.Condition, generation int64, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               typeAvailableHub,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

func mergeMaps(a, b map[string]string) map[string]string {
	if a == nil && b == nil {
		return nil
	}

	out := make(map[string]string, len(a)+len(b))
	maps.Copy(out, a)
	maps.Copy(out, b)
	return out
}
