package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	logger "sigs.k8s.io/controller-runtime/pkg/log"
)

var _ Controller = (*MdaiCollectorReconciler)(nil)

// MdaiCollectorReconciler reconciles a MdaiCollector object
type MdaiCollectorReconciler struct {
	client.Client

	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaicollectors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaicollectors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaicollectors/finalizers,verbs=update
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events;namespaces;namespaces/status;nodes;nodes/spec;pods;pods/status;replicationcontrollers;replicationcontrollers/status;resourcequotas;services,verbs=get;list;watch
// +kubebuilder:rbac:groups="apps",resources=daemonsets;deployments;replicasets;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="extensions",resources=daemonsets;deployments;replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups="batch",resources=jobs;cronjobs,verbs=get;list;watch
// +kubebuilder:rbac:groups="autoscaling",resources=horizontalpodautoscalers,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *MdaiCollectorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)
	log.Info("-- Starting MdaiCollector reconciliation --", "namespace", req.NamespacedName, "name", req.Name)

	fetchedCR := &mdaiv1.MdaiCollector{}
	if err := r.Get(ctx, req.NamespacedName, fetchedCR); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "unable to fetch MdaiCollector CR:"+req.Namespace+" : "+req.Name)
		}
		log.Info("-- Exiting MdaiCollector reconciliation, CR is deleted already --")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	_, err := r.ReconcileHandler(ctx, *NewMdaiCollectorAdapter(fetchedCR, log, r.Client, r.Recorder, r.Scheme))
	if err != nil {
		return ctrl.Result{}, err
	}

	log.Info("-- Finished MdaiCollector reconciliation --")

	return ctrl.Result{}, nil
}

func (*MdaiCollectorReconciler) ReconcileHandler(ctx context.Context, adapter Adapter) (ctrl.Result, error) {
	mdaiCollectorAdapter, ok := adapter.(MdaiCollectorAdapter)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("unexpected adapter type: %T", adapter)
	}

	operations := []ReconcileOperation{
		mdaiCollectorAdapter.ensureDeletionProcessed,
		mdaiCollectorAdapter.ensureStatusInitialized,
		mdaiCollectorAdapter.ensureFinalizerInitialized,
		mdaiCollectorAdapter.ensureSynchronized,
		mdaiCollectorAdapter.ensureStatusSetToDone,
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
func (r *MdaiCollectorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mdaiv1.MdaiCollector{}).
		Named("mdaicollector").
		Complete(r)
}
