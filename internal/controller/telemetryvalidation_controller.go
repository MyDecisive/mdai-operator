package controller

import (
	"context"
	"crypto/rand"
	"fmt"
	"maps"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logger "sigs.k8s.io/controller-runtime/pkg/log"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

type TelemetryValidationReconciler struct {
	client.Client

	Scheme *runtime.Scheme
}

const (
	correlationProcessorName                       = "attributes/correlation_id"
	correlationResourceProcessorName               = "resource/correlation_id"
	correlationDDTagsProcessorName                 = "transform/correlation_ddtags"
	correlationMetricsProcessorName                = "transform/metrics_correlation_id"
	correlationAttributeKey                        = "correlation_id"
	correlationHeaderFromCtxKey                    = "metadata.x-correlation-id"
	correlationDDTagKey                            = "correlation_id:"
	setDDTagsOnlyStatement                         = `set(attributes["ddtags"], Concat(["%s", attributes["%s"]], "")) where attributes["%s"] != nil and attributes["ddtags"] == nil`
	appendDDTagsStatement                          = `set(attributes["ddtags"], Concat([attributes["ddtags"], ",", "%s", attributes["%s"]], "")) where attributes["%s"] != nil and attributes["ddtags"] != nil`
	setMetricCorrelationStatement                  = `set(attributes["%s"], resource.attributes["%s"]) where attributes["%s"] == nil and resource.attributes["%s"] != nil`
	deleteMetricDDTagsStatement                    = `delete_key(attributes, "ddtags") where attributes["ddtags"] != nil`
	deleteMetricTagsStatement                      = `delete_key(attributes, "tags") where attributes["tags"] != nil`
	defaultValidatorImage                          = "ghcr.io/mydecisive/mdai-fidelity-validator:0.1.0"
	defaultValidatorPort                     int32 = 18081
	defaultValidatorReceiverPort             int32 = 8126
	defaultValidatorReplicas                 int32 = 1
	telemetryValidationLabelKey                    = "hub.mydecisive.ai/telemetry-validation"
	telemetryValidationRunIDAnnotationKey          = "hub.mydecisive.ai/telemetry-validation-run-id"
	telemetryValidationRunIDMetricLabel            = "telemetry_validation_run_id"
	telemetryValidationPrometheusSourceLabel       = "__meta_kubernetes_service_label_hub_mydecisive_ai_telemetry_validation"
	telemetryValidationRunIDPrometheusSource       = "__meta_kubernetes_service_annotation_hub_mydecisive_ai_telemetry_validation_run_id"
	telemetryValidationRoleShadow                  = "telemetry-validation-shadow-collector"
	telemetryValidationRoleValidator               = "telemetry-validation-validator"
)

// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations/finalizers,verbs=update
// +kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete

func (r *TelemetryValidationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)
	log.Info("-- Starting TelemetryValidation reconciliation --", "namespace", req.NamespacedName, "name", req.Name)

	var validation hubv1.TelemetryValidation
	if err := r.Get(ctx, req.NamespacedName, &validation); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "unable to fetch TelemetryValidation CR:"+req.Namespace+" : "+req.Name)
		}
		log.Info("-- Exiting TelemetryValidation reconciliation, CR is deleted already --")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	result, err := r.ReconcileHandler(ctx, NewTelemetryValidationAdapter(&validation, log, r.Client, r.Scheme, r))
	if err != nil {
		return result, err
	}

	log.Info("-- Finished TelemetryValidation reconciliation --")

	return result, nil
}

func (*TelemetryValidationReconciler) ReconcileHandler(ctx context.Context, adapter Adapter) (ctrl.Result, error) {
	telemetryValidationAdapter, ok := adapter.(*TelemetryValidationAdapter)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("unexpected adapter type: %T", adapter)
	}

	operations := []ReconcileOperation{
		telemetryValidationAdapter.ensureDeletionProcessed,
		telemetryValidationAdapter.ensureRunIDResolved,
		telemetryValidationAdapter.ensureStatusInitialized,
		telemetryValidationAdapter.ensureFinalizerInitialized,
		telemetryValidationAdapter.ensureSynchronized,
		telemetryValidationAdapter.ensureStatusSetToDone,
	}
	return RunReconcileOperations(ctx, operations)
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

func resolveTelemetryValidationRunID(validation *hubv1.TelemetryValidation) (string, bool, error) {
	specified := strings.TrimSpace(validation.Spec.RunID)
	if specified != "" {
		return specified, specified != validation.Status.RunID, nil
	}
	if strings.TrimSpace(validation.Status.RunID) != "" {
		return validation.Status.RunID, false, nil
	}

	generated, err := generateTelemetryValidationRunID()
	if err != nil {
		return "", false, err
	}
	return generated, true, nil
}

func generateTelemetryValidationRunID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate telemetry validation run id: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	), nil
}
