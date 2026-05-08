// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/mydecisive/mdai-operator/internal/manifests"
	rbacv1 "k8s.io/api/rbac/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// typeAvailableHub represents the status of the Deployment reconciliation
	typeAvailableHub = "Available"
	// typeDegradedHub represents the status used when the custom resource is deleted and the finalizer operations are must occur.
	typeDegradedHub = "Degraded"

	hubFinalizer = "mydecisive.ai/finalizer"

	ObjectModified          ObjectState = true
	ObjectUnchanged         ObjectState = false
	LabelManagedByMdaiKey               = "app.kubernetes.io/managed-by"
	LabelManagedByMdaiValue             = "mdai-operator"
	LabelAppNameKey                     = "app.kubernetes.io/name"
	LabelAppInstanceKey                 = "app.kubernetes.io/instance"

	otlpGRPCPort        = 4317
	otlpHTTPPort        = 4318
	promHTTPPort        = 8899
	otelMetricsPort     = 8888
	observerMetricsPort = 8899

	otlpGRPCName        = "otlp-grpc"
	otlpHTTPName        = "otlp-http"
	promHTTPName        = "prom-http"
	otelMetricsName     = "otel-metrics"
	observerMetricsName = "observe-metrics"
)

type ObjectState bool

// getList queries the Kubernetes API to list the requested resource, setting the list l of type T.
func getList[T client.Object](ctx context.Context, cl client.Client, l T, options ...client.ListOption) (map[types.UID]client.Object, error) {
	ownedObjects := map[types.UID]client.Object{}
	gvk, err := apiutil.GVKForObject(l, cl.Scheme())
	if err != nil {
		return nil, err
	}
	// nolint:perfsprint
	gvk.Kind = fmt.Sprintf("%sList", gvk.Kind)
	list, err := cl.Scheme().New(gvk)
	if err != nil {
		return nil, fmt.Errorf("unable to list objects of type %s: %w", gvk.Kind, err)
	}

	objList, ok := list.(client.ObjectList)
	if !ok {
		return ownedObjects, fmt.Errorf("unable to cast %v to ObjectList", list)
	}

	err = cl.List(ctx, objList, options...)
	if err != nil {
		return ownedObjects, fmt.Errorf("error listing %T: %w", l, err)
	}
	objs, err := apimeta.ExtractList(objList)
	if err != nil {
		return ownedObjects, fmt.Errorf("error listing %T: %w", l, err)
	}
	for i := range objs {
		typedObj, ok := objs[i].(T)
		if !ok {
			return ownedObjects, fmt.Errorf("error listing %T: %w", l, err)
		}
		ownedObjects[typedObj.GetUID()] = typedObj
	}

	return ownedObjects, nil
}

func isNamespaceScoped(obj client.Object) bool {
	switch obj.(type) {
	case *rbacv1.ClusterRole, *rbacv1.ClusterRoleBinding:
		return false
	default:
		return true
	}
}

// reconcileDesiredObjects runs the reconcile process using the mutateFn over the given list of objects.
func reconcileDesiredObjects(ctx context.Context, kubeClient client.Client, logger logr.Logger, owner metav1.Object, scheme *runtime.Scheme, desiredObjects []client.Object, ownedObjects map[types.UID]client.Object) error {
	var errs []error
	for _, desired := range desiredObjects {
		l := logger.WithValues(
			"object_name", desired.GetName(),
			"object_kind", desired.GetObjectKind(),
		)
		if isNamespaceScoped(desired) {
			if setErr := ctrl.SetControllerReference(owner, desired, scheme); setErr != nil {
				l.Error(setErr, "failed to set controller owner reference to desired")
				errs = append(errs, setErr)
				continue
			}
		}
		// existing is an object the controller runtime will hydrate for us
		// we obtain the existing object by deep copying the desired object because it's the most convenient way
		existing, ok := desired.DeepCopyObject().(client.Object)
		if !ok {
			return errors.New("failed to convert desired object to client.Object")
		}
		mutateFn := manifests.MutateFuncFor(existing, desired)
		var op controllerutil.OperationResult
		crudErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			result, createOrUpdateErr := ctrl.CreateOrUpdate(ctx, kubeClient, existing, mutateFn)
			op = result
			return createOrUpdateErr
		})
		if crudErr != nil && errors.As(crudErr, &manifests.ErrImmutableChange) {
			l.Error(crudErr, "detected immutable field change, trying to delete, new object will be created on next reconcile", "existing", existing.GetName())
			delErr := kubeClient.Delete(ctx, existing)
			if delErr != nil {
				return delErr
			}
			continue
		} else if crudErr != nil {
			l.Error(crudErr, "failed to configure desired")
			errs = append(errs, crudErr)
			continue
		}

		l.V(1).Info(fmt.Sprintf("desired has been %s", op))
		// This object is still managed by the operator, remove it from the list of objects to prune
		delete(ownedObjects, existing.GetUID())
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to create objects for %s: %w", owner.GetName(), errors.Join(errs...))
	}
	// Pruning owned objects in the cluster which are not should not be present after the reconciliation.
	err := deleteObjects(ctx, kubeClient, logger, ownedObjects)
	if err != nil {
		return fmt.Errorf("failed to prune objects for %s: %w", owner.GetName(), err)
	}
	return nil
}

func deleteObjects(ctx context.Context, kubeClient client.Client, logger logr.Logger, objects map[types.UID]client.Object) error {
	// Pruning owned objects in the cluster which are not should not be present after the reconciliation.
	var pruneErrs []error
	for _, obj := range objects {
		l := logger.WithValues(
			"object_name", obj.GetName(),
			"object_kind", obj.GetObjectKind().GroupVersionKind(),
		)

		l.Info("pruning unmanaged resource")
		err := kubeClient.Delete(ctx, obj)
		if err != nil {
			l.Error(err, "failed to delete resource")
			pruneErrs = append(pruneErrs, err)
		}
	}
	return errors.Join(pruneErrs...)
}
