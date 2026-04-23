package controller

import (
	"context"
	"strings"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logger "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// XDSManager defines the interface for updating the xDS snapshot
type XDSManager interface {
	UpdateSnapshot(ctx context.Context, nodeID string, collectors []otelv1beta1.OpenTelemetryCollector, validations []hubv1.TelemetryValidation) error
}

// XDSReconciler reconciles xDS snapshots based on OpenTelemetryCollector objects
type XDSReconciler struct {
	client.Client

	Scheme     *runtime.Scheme
	XDSManager XDSManager
}

// +kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors,verbs=get;list;watch

// Reconcile handles xDS snapshot updates
func (r *XDSReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)
	log.Info("-- Starting xDS reconciliation (OpenTelemetryCollector) --", "trigger", req.NamespacedName)

	if r.XDSManager == nil {
		log.V(1).Info("xDS Manager not initialized, skipping reconciliation")
		return ctrl.Result{}, nil
	}

	var collectors otelv1beta1.OpenTelemetryCollectorList
	if err := r.List(ctx, &collectors); err != nil {
		log.Error(err, "unable to list OpenTelemetryCollectors for xDS update")
		return ctrl.Result{}, err
	}

	var validations hubv1.TelemetryValidationList
	if err := r.List(ctx, &validations); err != nil {
		log.Error(err, "unable to list TelemetryValidations for xDS update")
		return ctrl.Result{}, err
	}

	// For now, we use a fixed nodeID as configured in the hub chart
	if err := r.XDSManager.UpdateSnapshot(ctx, "envoy-hub-proxy", collectors.Items, validations.Items); err != nil {
		log.Error(err, "failed to update xDS snapshot")
		return ctrl.Result{}, err
	}

	log.Info("-- Finished xDS reconciliation (OpenTelemetryCollector) --")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *XDSReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&otelv1beta1.OpenTelemetryCollector{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
			if obj == nil {
				return false
			}
			if obj.GetLabels()["hub.mydecisive.ai/shadow"] == "true" {
				return false
			}
			if obj.GetAnnotations()["hub.mydecisive.ai/shadow"] == "true" {
				return false
			}
			return !strings.HasSuffix(obj.GetName(), "-shadow")
		}))).
		Watches(
			&hubv1.TelemetryValidation{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				tv, ok := obj.(*hubv1.TelemetryValidation)
				if !ok || tv.Spec.CollectorRef.Name == "" {
					return nil
				}
				return []reconcile.Request{{
					NamespacedName: types.NamespacedName{
						Name:      tv.Spec.CollectorRef.Name,
						Namespace: tv.Namespace,
					},
				}}
			}),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Named("xds-reconciler").
		Complete(r)
}
