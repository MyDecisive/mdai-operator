package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	logger "sigs.k8s.io/controller-runtime/pkg/log"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

type TelemetryValidationReconciler struct {
	client.Client

	Scheme *runtime.Scheme
}

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
