package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	logger "sigs.k8s.io/controller-runtime/pkg/log"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
)

// MdaiDalReconciler reconciles a MdaiDal object
type MdaiDalReconciler struct {
	client.Client

	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaidals,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaidals/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaidals/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *MdaiDalReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)
	log.Info("-- Starting MDAI DAL reconciliation --", "namespace", req.NamespacedName, "name", req.Name)

	fetchedCR := &mdaiv1.MdaiDal{}
	if err := r.Get(ctx, req.NamespacedName, fetchedCR); err != nil {
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		if !apierrors.IsNotFound(err) {
			log.Error(err, "unable to fetch MDAI DAL CR: "+req.Namespace+" : "+req.Name)
		}
		log.Info("-- Exiting MDAI DAL reconciliation, CR is deleted already --")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	result, err := r.ReconcileHandler(ctx, *NewMdaiDalAdapter(fetchedCR, log, r.Client, r.Scheme))
	if err != nil {
		return result, err
	}

	log.Info("-- Finished MDAI DAL reconciliation --")

	return result, nil
}

func (*MdaiDalReconciler) ReconcileHandler(ctx context.Context, adapter Adapter) (ctrl.Result, error) {
	mdaiDalAdapter, ok := adapter.(MdaiDalAdapter)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("unexpected adapter type: %T", adapter)
	}

	operations := []ReconcileOperation{
		mdaiDalAdapter.ensureDeletionProcessed,
		mdaiDalAdapter.ensureStatusInitialized,
		mdaiDalAdapter.ensureFinalizerInitialized,
		mdaiDalAdapter.ensureSynchronized,
		mdaiDalAdapter.ensureStatusSetToDone,
	}
	return RunReconcileOperations(ctx, operations)
}

// SetupWithManager sets up the controller with the Manager.
func (r *MdaiDalReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mdaiv1.MdaiDal{}).
		Named("mdaidal").
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
