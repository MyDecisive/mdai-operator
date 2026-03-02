package controller

import (
	"context"
	"errors"
	"maps"
	"reflect"

	"github.com/mydecisive/mdai-operator/internal/manifests"
	"github.com/mydecisive/mdai-operator/internal/manifests/collector"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logger "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

const (
	resourceOwnerKey            = "metadata.ownerReferences.name"
	MdaiIngressOtelColLookupKey = "spec.otelCol.lookupKey"
)

// MdaiIngressReconciler reconciles a MdaiIngress object
type MdaiIngressReconciler struct {
	client.Client

	Scheme *runtime.Scheme
	Logger *zap.Logger
}

var ErrIncorrectOtelcolRef = errors.New("incorrect Otelcol reference")

var IndexerOtelCol = func(obj client.Object) []string {
	a, ok := obj.(*hubv1.MdaiIngress)
	if !ok {
		return nil
	}
	otelCol := a.Spec.OtelCollector
	if otelCol.Name != "" {
		return []string{otelCol.Name}
	}
	return nil
}

//+kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaiingresses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaiingresses/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hub.mydecisive.ai,resources=mdaiingresses/finalizers,verbs=update
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors,verbs=get;list;watch

func (r *MdaiIngressReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)
	log.Info("-- Starting MDAI Ingress reconciliation --", "name", req.Name)

	var instanceMdaiIngress hubv1.MdaiIngress
	if err := r.Get(ctx, req.NamespacedName, &instanceMdaiIngress); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "unable to fetch MdaiIngress")
		}

		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	otelcolName := instanceMdaiIngress.Spec.OtelCollector.Name
	if otelcolName == "" {
		log.Error(ErrIncorrectOtelcolRef, "No OtelCollector reference in MdaiIngress", "name", req.Name, "namespace", req.Namespace)
		return ctrl.Result{}, ErrIncorrectOtelcolRef
	}
	otelcolNsName := types.NamespacedName{Name: otelcolName, Namespace: req.Namespace}

	var instanceOtel v1beta1.OpenTelemetryCollector
	if err := r.Get(ctx, otelcolNsName, &instanceOtel); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "unable to fetch OpenTelemetryCollector")
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}

	otelMdaiComb := hubv1.NewOtelIngressConfig(instanceOtel, instanceMdaiIngress)

	params := r.GetParams(*otelMdaiComb)

	desiredObjects, buildErr := BuildCollector(params)
	if buildErr != nil {
		return ctrl.Result{}, buildErr
	}

	ownedObjects, err := r.findMdaiIngressOwnedObjects(ctx, params)
	if err != nil {
		return ctrl.Result{}, err
	}

	err = reconcileDesiredObjects(ctx, r.Client, log, &instanceMdaiIngress, params.Scheme, desiredObjects, ownedObjects)
	if err != nil {
		return ctrl.Result{}, err
	}

	log.Info("-- Finished MDAI Ingress reconciliation --", "name", req.Name)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MdaiIngressReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ctx := context.Background()
	// Index for Service
	if err := mgr.GetFieldIndexer().IndexField(
		ctx,
		&corev1.Service{},
		resourceOwnerKey,
		func(rawObj client.Object) []string {
			service, ok := rawObj.(*corev1.Service)
			if !ok {
				return nil
			}
			owners := service.GetOwnerReferences()
			if len(owners) == 0 {
				return nil
			}
			return []string{owners[0].Name}
		},
	); err != nil {
		return err
	}
	// Index for Ingress
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&networkingv1.Ingress{},
		resourceOwnerKey,
		func(rawObj client.Object) []string {
			ing, ok := rawObj.(*networkingv1.Ingress)
			if !ok {
				return nil
			}
			var ownerNames []string
			for _, owner := range ing.GetOwnerReferences() {
				ownerNames = append(ownerNames, owner.Name)
			}
			return ownerNames
		},
	); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&hubv1.MdaiIngress{}, builder.WithPredicates(r.mdaiIngressPredicates(ctx))).
		Watches(
			&v1beta1.OpenTelemetryCollector{},
			handler.EnqueueRequestsFromMapFunc(r.requeueByCollectorRef),
			builder.WithPredicates(r.otelcolPredicates()),
		).
		Complete(r)
}

func (r *MdaiIngressReconciler) GetParams(otelMdaiComb hubv1.OtelMdaiIngressComb) manifests.Params {
	p := manifests.Params{
		Client:              r.Client,
		OtelMdaiIngressComb: otelMdaiComb,
		Log:                 r.Logger,
		Scheme:              r.Scheme,
	}
	return p
}

func (r *MdaiIngressReconciler) requeueByCollectorRef(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logger.FromContext(ctx)
	log.Info("-- Requeueing MdaiIngress triggered by otel collector", "collector name", obj.GetName(), "namespace", obj.GetNamespace())

	cr, ok := obj.(*v1beta1.OpenTelemetryCollector)
	if !ok {
		return []ctrl.Request{}
	}

	mdaiIngressName, found := r.findMdaiIngress(ctx, cr.Name, cr.Namespace)
	if !found {
		return []ctrl.Request{}
	}

	// As soon as this method is called _after_ predicated applied, and therefore, we know that a linked pair MdaiIngress -> Otelcol exists,
	// we dont need to check anything here, just requeue the MdaiIngress
	return []ctrl.Request{
		{
			NamespacedName: types.NamespacedName{
				Name:      mdaiIngressName,
				Namespace: cr.Namespace,
			},
		},
	}
}

// BuildCollector returns the generation and collected errors of all manifests for a given instance.
func BuildCollector(params manifests.Params) ([]client.Object, error) {
	mBuilders := []manifests.Builder[manifests.Params]{
		collector.Build,
	}
	var resources []client.Object
	for _, mBuilder := range mBuilders {
		objs, err := mBuilder(params)
		if err != nil {
			return nil, err
		}
		resources = append(resources, objs...)
	}
	return resources, nil
}

func (r *MdaiIngressReconciler) findMdaiIngressOwnedObjects(ctx context.Context, params manifests.Params) (map[types.UID]client.Object, error) {
	ownedObjects := map[types.UID]client.Object{}
	ownedObjectTypes := GetOwnedResourceTypes()
	listOpts := []client.ListOption{
		client.InNamespace(params.OtelMdaiIngressComb.MdaiIngress.Namespace),
		client.MatchingFields{resourceOwnerKey: params.OtelMdaiIngressComb.MdaiIngress.Name},
	}
	for _, objectType := range ownedObjectTypes {
		objs, err := getList(ctx, r.Client, objectType, listOpts...)
		if err != nil {
			return nil, err
		}
		maps.Copy(ownedObjects, objs)
	}

	return ownedObjects, nil
}

// otelColExists returns true if the Otelcol instance with the referenced name and namespace is found
func (r *MdaiIngressReconciler) otelColExists(ctx context.Context, name string, namespace string) bool {
	log := logger.FromContext(ctx)

	otelcolNsName := types.NamespacedName{Name: name, Namespace: namespace}
	otelCollector := v1beta1.OpenTelemetryCollector{}
	if err := r.Get(ctx, otelcolNsName, &otelCollector); err != nil {
		log.Error(err, "Failed to get OpentelemetryCollector", "name", name, "namespace", namespace)
		return false
	}

	return true
}

// findMdaiIngress returns a Name of the MdaiIngress instance with the given name and namespace.
func (r *MdaiIngressReconciler) findMdaiIngress(ctx context.Context, name string, namespace string) (string, bool) {
	log := logger.FromContext(ctx)

	mdaiIngresses := hubv1.MdaiIngressList{}
	if err := r.List(ctx, &mdaiIngresses,
		client.InNamespace(namespace),
		client.MatchingFields{MdaiIngressOtelColLookupKey: name},
	); err != nil {
		log.Error(err, "Failed to list MdaiIngresses", "namespace", namespace)
		return "", false
	}

	if len(mdaiIngresses.Items) == 0 {
		log.Info("No MdaiIngress found", "namespace", namespace)
		return "", false
	}

	result := mdaiIngresses.Items[0].Name

	return result, true
}

// GetOwnedResourceTypes returns all the resource types the controller can own. Even though this method returns an array
// of client.Object, these are (empty) example structs rather than actual resources.
func GetOwnedResourceTypes() []client.Object {
	ownedResources := []client.Object{
		&corev1.Service{},
		&networkingv1.Ingress{},
	}

	return ownedResources
}

func (r *MdaiIngressReconciler) mdaiIngressPredicates(ctx context.Context) predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			cr, ok := e.Object.(*hubv1.MdaiIngress)
			if !ok {
				return false
			}
			return r.otelColExists(ctx, cr.Spec.OtelCollector.Name, cr.Namespace)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			newCr, ok := e.ObjectNew.(*hubv1.MdaiIngress)
			if !ok {
				return false
			}
			oldCr, ok := e.ObjectOld.(*hubv1.MdaiIngress)
			if !ok {
				return false
			}
			if reflect.DeepEqual(oldCr.Spec, newCr.Spec) {
				return false
			}
			return r.otelColExists(ctx, newCr.Spec.OtelCollector.Name, newCr.Namespace) || r.otelColExists(ctx, oldCr.Spec.OtelCollector.Name, oldCr.Namespace)
		},
		DeleteFunc:  func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

// nolint:revive
func (r *MdaiIngressReconciler) otelcolPredicates() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldCR, ok := e.ObjectOld.(*v1beta1.OpenTelemetryCollector)
			if !ok {
				return false
			}
			newCR, ok := e.ObjectNew.(*v1beta1.OpenTelemetryCollector)
			if !ok {
				return false
			}
			return !reflect.DeepEqual(oldCR.Spec.Config, newCR.Spec.Config)
		},
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

func SetMdaiIngressIndexers(ctx context.Context, mgr manager.Manager) error {
	return mgr.GetFieldIndexer().IndexField(
		ctx,
		&hubv1.MdaiIngress{},
		MdaiIngressOtelColLookupKey,
		IndexerOtelCol,
	)
}
