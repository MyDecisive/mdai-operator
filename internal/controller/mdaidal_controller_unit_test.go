package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeMdaiDalAdapter struct{}

func (fakeMdaiDalAdapter) ensureDeletionProcessed(context.Context) (OperationResult, error) {
	return ContinueProcessing()
}

func (fakeMdaiDalAdapter) ensureFinalizerInitialized(context.Context) (OperationResult, error) {
	return ContinueProcessing()
}

func (fakeMdaiDalAdapter) ensureFinalizerDeleted(context.Context) error {
	return nil
}

func (fakeMdaiDalAdapter) ensureStatusInitialized(context.Context) (OperationResult, error) {
	return ContinueProcessing()
}

func (fakeMdaiDalAdapter) ensureStatusSetToDone(context.Context) (OperationResult, error) {
	return ContinueProcessing()
}

func (fakeMdaiDalAdapter) ensureSynchronized(context.Context) (OperationResult, error) {
	return ContinueProcessing()
}

func (fakeMdaiDalAdapter) finalize(context.Context) (ObjectState, error) {
	return ObjectModified, nil
}

func (fakeMdaiDalAdapter) deleteFinalizer(context.Context, client.Object, string) error {
	return nil
}

func TestMdaiDalReconciler_ReconcileHandlerRejectsUnknownAdapter(t *testing.T) {
	t.Parallel()

	reconciler := &MdaiDalReconciler{}

	_, err := reconciler.ReconcileHandler(t.Context(), fakeMdaiDalAdapter{})
	require.Error(t, err)
}

func TestMdaiDalReconciler_ReconcileHandlerSuccess(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	dalCR := &mdaiv1.MdaiDal{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mdai-dal",
			Namespace:  "default",
			Finalizers: []string{hubFinalizer},
		},
		Spec: mdaiv1.MdaiDalSpec{
			ImageSpec: &mdaiv1.MdaiDalImageSpec{Repository: "repo"},
			S3: mdaiv1.MdaiDalS3Config{
				Bucket: "bucket",
				Prefix: "prefix",
			},
			AWS: mdaiv1.MdaiDalAWSConfig{
				Region: "us-east-1",
				Credentials: mdaiv1.MdaiDalAWSCredentials{
					SecretName:     "aws-creds",
					AccessKeyField: "access",
					SecretKeyField: "secret",
				},
			},
			Granularity:  "hourly",
			UsePayloadTS: true,
			Marshaler:    "json",
		},
		Status: mdaiv1.MdaiDalStatus{
			Conditions: []metav1.Condition{{Type: typeAvailableHub, Status: metav1.ConditionUnknown}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(dalCR).
		WithObjects(dalCR).
		Build()

	adapter := *NewMdaiDalAdapter(dalCR, logr.Discard(), k8sClient, scheme)
	reconciler := &MdaiDalReconciler{Client: k8sClient, Scheme: scheme}

	ctx := t.Context()
	result, err := reconciler.ReconcileHandler(ctx, adapter)

	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	updated := &mdaiv1.MdaiDal{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{
		Name:      "mdai-dal",
		Namespace: "default",
	}, updated))

	assert.True(t, meta.IsStatusConditionTrue(updated.Status.Conditions, typeAvailableHub))
	assert.Contains(t, updated.Status.Endpoint, "svc.cluster.local")
}
