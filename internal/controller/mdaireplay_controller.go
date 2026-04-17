package controller

import (
	"context"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	logger "sigs.k8s.io/controller-runtime/pkg/log"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
)

var _ Controller = (*MdaiReplayReconciler)(nil)

// MdaiReplayReconciler reconciles a MdaiReplay object
type MdaiReplayReconciler struct {
	client.Client

	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaireplays,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaireplays/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaireplays/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MdaiReplay object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/reconcile
func (r *MdaiReplayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)
	log.Info("-- Starting MdaiReplay reconciliation --", "namespace", req.NamespacedName, "name", req.Name)

	fetchedCR := &mdaiv1.MdaiReplay{}
	if err := r.Get(ctx, req.NamespacedName, fetchedCR); err != nil {
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		if !apierrors.IsNotFound(err) {
			log.Error(err, "unable to fetch MdaiReplay CR:"+req.Namespace+" : "+req.Name)
		}
		log.Info("-- Exiting MdaiReplay reconciliation, CR is deleted already --")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	result, err := r.ReconcileHandler(ctx, *NewMdaiReplayAdapter(fetchedCR, log, r.Client, r.Recorder, r.Scheme, http.DefaultClient, nil))
	if err != nil {
		return result, err
	}

	log.Info("-- Finished MdaiReplay reconciliation --")

	return result, nil
}

// ReconcileHandler processes the MdaiReplay CR and performs the necessary operations.
func (*MdaiReplayReconciler) ReconcileHandler(ctx context.Context, adapter Adapter) (ctrl.Result, error) {
	replayAdapter, ok := adapter.(MdaiReplayAdapter)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("unexpected adapter type: %T", adapter)
	}

	operations := []ReconcileOperation{
		replayAdapter.ensureDeletionProcessed,
		replayAdapter.ensureStatusInitialized,
		replayAdapter.ensureFinalizerInitialized,
		replayAdapter.ensureSynchronized,
		replayAdapter.ensureStatusSetToDone,
	}
	return RunReconcileOperations(ctx, operations)
}

// SetupWithManager sets up the controller with the Manager.
func (r *MdaiReplayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mdaiv1.MdaiReplay{}).
		Named("mdaireplay").
		Complete(r)
}
