package controller

import (
	"context"
	"fmt"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logger "sigs.k8s.io/controller-runtime/pkg/log"
)

var _ Controller = (*MdaiObserverReconciler)(nil)

// MdaiObserverReconciler reconciles a MdaiObserver object
type MdaiObserverReconciler struct {
	client.Client

	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaiobservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaiobservers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaiobservers/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.2/pkg/reconcile
func (r *MdaiObserverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)
	log.Info("-- Starting MdaiObserver reconciliation --", "namespace", req.NamespacedName, "name", req.Name)

	fetchedCR := &mdaiv1.MdaiObserver{}
	if err := r.Get(ctx, req.NamespacedName, fetchedCR); err != nil {
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		if !apierrors.IsNotFound(err) {
			log.Error(err, "unable to fetch MdaiObserver CR:"+req.Namespace+" : "+req.Name)
		}
		log.Info("-- Exiting MdaiObserver reconciliation, CR is deleted already --")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	result, err := r.ReconcileHandler(ctx, *NewObserverAdapter(fetchedCR, log, r.Client, r.Recorder, r.Scheme))
	if err != nil {
		return result, err
	}

	log.Info("-- Finished MdaiObserver reconciliation --")

	return result, nil
}

// ReconcileHandler processes the MdaiObserver CR and performs the necessary operations.
func (*MdaiObserverReconciler) ReconcileHandler(ctx context.Context, adapter Adapter) (ctrl.Result, error) {
	observerAdapter, ok := adapter.(ObserverAdapter)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("unexpected adapter type: %T", adapter)
	}

	operations := []ReconcileOperation{
		observerAdapter.ensureDeletionProcessed,
		observerAdapter.ensureStatusInitialized,
		observerAdapter.ensureFinalizerInitialized,
		observerAdapter.ensureSynchronized,
		observerAdapter.ensureStatusSetToDone,
	}
	for _, operation := range operations {
		result, err := operation(ctx)
		if err != nil || result.RequeueRequest {
			return ctrl.Result{RequeueAfter: result.RequeueDelay}, err
		}
		if result.CancelRequest {
			return ctrl.Result{}, nil
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MdaiObserverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mdaiv1.MdaiObserver{}).
		Named("mdaiobserver").
		Complete(r)
}
