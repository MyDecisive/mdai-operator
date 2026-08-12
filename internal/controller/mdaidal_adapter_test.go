package controller

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestImageRef(t *testing.T) {
	testCases := []struct {
		name     string
		spec     mdaiv1.MdaiDalImageSpec
		expected string
	}{
		{
			name: "repository and tag",
			spec: mdaiv1.MdaiDalImageSpec{
				Repository: "my-repo",
				Tag:        "v1.0",
			},
			expected: "my-repo:v1.0",
		},
		{
			name: "repository only",
			spec: mdaiv1.MdaiDalImageSpec{
				Repository: "my-repo",
			},
			expected: "my-repo",
		},
		{
			name:     "empty spec",
			spec:     mdaiv1.MdaiDalImageSpec{},
			expected: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, imageRef(tc.spec))
		})
	}
}

func TestMdaiDalAdapter_createOrUpdateConfigMap(t *testing.T) {
	const namespace = "default"
	const dalName = "test-dal"

	dalCR := &mdaiv1.MdaiDal{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dalName,
			Namespace: namespace,
		},
		Spec: mdaiv1.MdaiDalSpec{
			S3: mdaiv1.MdaiDalS3Config{
				Bucket: "test-bucket",
				Prefix: "test-prefix",
			},
			AWS: mdaiv1.MdaiDalAWSConfig{
				Region: "us-east-1",
			},
			Granularity:  "hourly",
			UsePayloadTS: true,
			Marshaler:    "json",
		},
	}

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dalCR).Build()

	adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), k8sClient, scheme)

	ctx := t.Context()
	err := adapter.createOrUpdateConfigMap(ctx)
	require.NoError(t, err)

	cm := &corev1.ConfigMap{}
	err = k8sClient.Get(ctx, types.NamespacedName{
		Name:      "test-dal-mdai-dal-config",
		Namespace: namespace,
	}, cm)
	require.NoError(t, err)

	assert.Equal(t, "mdai-dal", cm.Labels["app.kubernetes.io/name"])
	assert.Equal(t, dalName, cm.Labels["app.kubernetes.io/instance"])

	expectedData := map[string]string{
		"MDAI_DAL_S3_BUCKET":      "test-bucket",
		"MDAI_DAL_AWS_REGION":     "us-east-1",
		"MDAI_DAL_S3_PREFIX":      "test-prefix",
		"MDAI_DAL_GRANULARITY":    "hourly",
		"MDAI_DAL_USE_PAYLOAD_TS": "true",
		"MDAI_DAL_MARSHALER":      "json",
	}

	assert.Equal(t, expectedData, cm.Data)
}

func TestMdaiDalAdapter_createOrUpdateService(t *testing.T) {
	const namespace = "default"
	const dalName = "test-dal"

	dalCR := &mdaiv1.MdaiDal{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dalName,
			Namespace: namespace,
		},
	}

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dalCR).Build()
	adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), k8sClient, scheme)

	ctx := t.Context()
	err := adapter.createOrUpdateService(ctx)
	require.NoError(t, err)

	svc := &corev1.Service{}
	err = k8sClient.Get(ctx, types.NamespacedName{
		Name:      dalName,
		Namespace: namespace,
	}, svc)
	require.NoError(t, err)

	assert.Equal(t, "mdai-dal", svc.Labels[LabelAppNameKey])
	assert.Equal(t, dalName, svc.Labels[LabelAppInstanceKey])
	assert.Equal(t, "mdai-operator", svc.Labels[LabelManagedByMdaiKey])
	assert.Equal(t, "mdai-dal", svc.Labels[HubComponentLabel])

	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	assert.Equal(t, "mdai-dal", svc.Spec.Selector[LabelAppNameKey])
	assert.Equal(t, dalName, svc.Spec.Selector[LabelAppInstanceKey])

	require.Len(t, svc.Spec.Ports, 2)
	assert.Equal(t, int32(4317), svc.Spec.Ports[0].Port)
	assert.Equal(t, "otlp-grpc", svc.Spec.Ports[0].Name)
	assert.Equal(t, int32(4318), svc.Spec.Ports[1].Port)
	assert.Equal(t, "otlp-http", svc.Spec.Ports[1].Name)
}

func TestMdaiDalAdapter_createOrUpdateDeployment(t *testing.T) {
	const namespace = "default"
	const dalName = "test-dal"

	dalCR := &mdaiv1.MdaiDal{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dalName,
			Namespace: namespace,
		},
		Spec: mdaiv1.MdaiDalSpec{
			ImageSpec: &mdaiv1.MdaiDalImageSpec{
				Repository: "my-dal-repo",
				Tag:        "v1.2.3",
				PullPolicy: "Always",
			},
			AWS: mdaiv1.MdaiDalAWSConfig{
				Credentials: mdaiv1.MdaiDalAWSCredentials{
					SecretName:     "aws-creds",
					AccessKeyField: "aws-key",
					SecretKeyField: "aws-secret",
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dalCR).Build()
	adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), k8sClient, scheme)

	ctx := t.Context()
	err := adapter.createOrUpdateDeployment(ctx)
	require.NoError(t, err)

	dep := &appsv1.Deployment{}
	err = k8sClient.Get(ctx, types.NamespacedName{
		Name:      dalName,
		Namespace: namespace,
	}, dep)
	require.NoError(t, err)

	assert.Equal(t, "mdai-dal", dep.Labels["app.kubernetes.io/name"])
	assert.Equal(t, dalName, dep.Labels["app.kubernetes.io/instance"])

	require.NotNil(t, dep.Spec.Replicas)
	assert.Equal(t, int32(1), *dep.Spec.Replicas)

	require.Len(t, dep.Spec.Template.Spec.Containers, 1)
	container := dep.Spec.Template.Spec.Containers[0]

	assert.Equal(t, "my-dal-repo:v1.2.3", container.Image)
	assert.Equal(t, corev1.PullAlways, container.ImagePullPolicy)

	expectedEnv := []corev1.EnvVar{
		{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "aws-creds"}, Key: "aws-key"}}},
		{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "aws-creds"}, Key: "aws-secret"}}},
	}
	assert.Subset(t, container.Env, expectedEnv)

	require.NotNil(t, container.SecurityContext)
	require.NotNil(t, container.SecurityContext.AllowPrivilegeEscalation)
	assert.False(t, *container.SecurityContext.AllowPrivilegeEscalation)

	require.NotNil(t, container.LivenessProbe)
	assert.Equal(t, "/healthz", container.LivenessProbe.HTTPGet.Path)
	assert.Equal(t, int32(4318), container.LivenessProbe.HTTPGet.Port.IntVal)

	require.NotNil(t, container.ReadinessProbe)
	assert.Equal(t, "/healthz", container.ReadinessProbe.HTTPGet.Path)
	assert.Equal(t, int32(4318), container.ReadinessProbe.HTTPGet.Port.IntVal)
}

func TestMdaiDalAdapter_ensureDeletionProcessed(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	t.Run("continue when not deleting", func(t *testing.T) {
		t.Parallel()

		dalCR := &mdaiv1.MdaiDal{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mdai-dal",
				Namespace: "default",
			},
		}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dalCR).Build()

		adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), k8sClient, scheme)
		ctx := t.Context()
		result, err := adapter.ensureDeletionProcessed(ctx)

		require.NoError(t, err)
		assert.Equal(t, ContinueOperationResult(), result)
	})

	t.Run("finalizes when deleting", func(t *testing.T) {
		t.Parallel()

		deletionTime := metav1.NewTime(time.Now())
		dalCR := &mdaiv1.MdaiDal{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "mdai-dal",
				Namespace:         "default",
				Finalizers:        []string{hubFinalizer},
				DeletionTimestamp: &deletionTime,
			},
		}
		k8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(dalCR).
			WithObjects(dalCR).
			Build()

		adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), k8sClient, scheme)
		result, err := adapter.ensureDeletionProcessed(t.Context())

		require.NoError(t, err)
		assert.Equal(t, StopOperationResult(), result)

		persisted := &mdaiv1.MdaiDal{}
		err = k8sClient.Get(t.Context(), types.NamespacedName{
			Name:      "mdai-dal",
			Namespace: "default",
		}, persisted)
		if apierrors.IsNotFound(err) {
			persisted = dalCR
		} else {
			require.NoError(t, err)
		}

		assert.Empty(t, persisted.Finalizers)
		condition := meta.FindStatusCondition(persisted.Status.Conditions, typeDegradedHub)
		require.NotNil(t, condition)
		assert.Equal(t, metav1.ConditionTrue, condition.Status)
	})
}

func TestMdaiDalAdapter_ensureStatusSetToDone(t *testing.T) {
	t.Parallel()

	const (
		dalName   = "mdai-dal"
		namespace = "default"
	)

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	dalCR := &mdaiv1.MdaiDal{
		ObjectMeta: metav1.ObjectMeta{
			Name:       dalName,
			Namespace:  namespace,
			Generation: 2,
		},
	}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dalName,
			Namespace: namespace,
		},
		Status: appsv1.DeploymentStatus{
			Replicas:      3,
			ReadyReplicas: 2,
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dalName,
			Namespace: namespace,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(dalCR).
		WithObjects(dalCR, deployment, service).
		Build()

	adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), k8sClient, scheme)
	ctx := t.Context()
	result, err := adapter.ensureStatusSetToDone(ctx)

	require.NoError(t, err)
	assert.Equal(t, ContinueOperationResult(), result)

	updated := &mdaiv1.MdaiDal{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{
		Name:      dalName,
		Namespace: namespace,
	}, updated))

	assert.Equal(t, updated.Generation, updated.Status.ObservedGeneration)
	assert.Equal(t, int32(3), updated.Status.Replicas)
	assert.Equal(t, int32(2), updated.Status.ReadyReplicas)
	assert.False(t, updated.Status.LastSyncedTime.IsZero())
	assert.True(t, meta.IsStatusConditionTrue(updated.Status.Conditions, typeAvailableHub))
	assert.Equal(t, fmt.Sprintf("%s.%s.svc.cluster.local:%d", dalName, namespace, otlpHTTPPort), updated.Status.Endpoint)
}

func TestMdaiDalAdapter_ensureFinalizerInitialized(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))

	t.Run("skips when already present", func(t *testing.T) {
		t.Parallel()

		dalCR := &mdaiv1.MdaiDal{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "mdai-dal",
				Namespace:  "default",
				Finalizers: []string{hubFinalizer},
			},
		}
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dalCR).Build()

		adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), client, scheme)
		result, err := adapter.ensureFinalizerInitialized(t.Context())

		require.NoError(t, err)
		assert.Equal(t, ContinueOperationResult(), result)
	})

	t.Run("adds finalizer", func(t *testing.T) {
		t.Parallel()

		dalCR := &mdaiv1.MdaiDal{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mdai-dal",
				Namespace: "default",
			},
		}
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dalCR).Build()

		adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), client, scheme)
		ctx := t.Context()
		result, err := adapter.ensureFinalizerInitialized(ctx)

		require.NoError(t, err)
		assert.Equal(t, StopOperationResult(), result)

		updated := &mdaiv1.MdaiDal{}
		require.NoError(t, client.Get(t.Context(), types.NamespacedName{
			Name:      "mdai-dal",
			Namespace: "default",
		}, updated))
		assert.Contains(t, updated.Finalizers, hubFinalizer)
	})
}

func TestMdaiDalAdapter_ensureStatusInitialized(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))

	t.Run("already initialized", func(t *testing.T) {
		t.Parallel()

		dalCR := &mdaiv1.MdaiDal{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mdai-dal",
				Namespace: "default",
			},
			Status: mdaiv1.MdaiDalStatus{
				Conditions: []metav1.Condition{
					{Type: typeAvailableHub, Status: metav1.ConditionTrue},
				},
			},
		}
		client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(dalCR).WithObjects(dalCR).Build()

		adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), client, scheme)
		result, err := adapter.ensureStatusInitialized(t.Context())

		require.NoError(t, err)
		assert.Equal(t, ContinueOperationResult(), result)
	})

	t.Run("sets initial status", func(t *testing.T) {
		t.Parallel()

		dalCR := &mdaiv1.MdaiDal{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mdai-dal",
				Namespace: "default",
			},
		}
		client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(dalCR).WithObjects(dalCR).Build()

		adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), client, scheme)
		ctx := t.Context()
		result, err := adapter.ensureStatusInitialized(ctx)

		require.NoError(t, err)
		assert.Equal(t, StopOperationResult(), result)

		updated := &mdaiv1.MdaiDal{}
		require.NoError(t, client.Get(t.Context(), types.NamespacedName{
			Name:      "mdai-dal",
			Namespace: "default",
		}, updated))

		condition := meta.FindStatusCondition(updated.Status.Conditions, typeAvailableHub)
		require.NotNil(t, condition)
		assert.Equal(t, metav1.ConditionUnknown, condition.Status)
	})
}

func TestMdaiDalAdapter_finalize(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))

	dalCR := &mdaiv1.MdaiDal{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mdai-dal",
			Namespace:  "default",
			Finalizers: []string{hubFinalizer},
		},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(dalCR).
		WithObjects(dalCR).
		Build()

	adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), client, scheme)
	ctx := t.Context()
	state, err := adapter.finalize(ctx)

	require.NoError(t, err)
	assert.Equal(t, ObjectModified, state)

	updated := &mdaiv1.MdaiDal{}
	require.NoError(t, client.Get(ctx, types.NamespacedName{
		Name:      "mdai-dal",
		Namespace: "default",
	}, updated))

	assert.Empty(t, updated.Finalizers)
	assert.True(t, meta.IsStatusConditionTrue(updated.Status.Conditions, typeDegradedHub))
}

func TestMdaiDalAdapter_ensureFinalizerDeleted(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))

	dalCR := &mdaiv1.MdaiDal{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "mdai-dal",
			Namespace:  "default",
			Finalizers: []string{hubFinalizer},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dalCR).Build()

	adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), client, scheme)
	ctx := t.Context()
	require.NoError(t, adapter.ensureFinalizerDeleted(ctx))

	updated := &mdaiv1.MdaiDal{}
	require.NoError(t, client.Get(ctx, types.NamespacedName{
		Name:      "mdai-dal",
		Namespace: "default",
	}, updated))
	assert.Empty(t, updated.Finalizers)
}

func TestMdaiDalAdapter_deleteFinalizer_noop(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, mdaiv1.AddToScheme(scheme))

	dalCR := &mdaiv1.MdaiDal{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdai-dal",
			Namespace: "default",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dalCR).Build()

	adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), client, scheme)
	require.NoError(t, adapter.deleteFinalizer(t.Context(), dalCR, hubFinalizer))

	assert.Empty(t, dalCR.Finalizers)
}

func TestMdaiDalAdapter_ensureSynchronized(t *testing.T) {
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
			ImageSpec: &mdaiv1.MdaiDalImageSpec{
				Repository: "repo",
			},
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
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dalCR).WithStatusSubresource(dalCR).Build()

	adapter := NewMdaiDalAdapter(dalCR, logr.Discard(), client, scheme)
	ctx := t.Context()
	result, err := adapter.ensureSynchronized(ctx)

	require.NoError(t, err)
	assert.Equal(t, ContinueOperationResult(), result)

	configMap := &corev1.ConfigMap{}
	require.NoError(t, client.Get(ctx, types.NamespacedName{
		Name:      "mdai-dal-mdai-dal-config",
		Namespace: "default",
	}, configMap))

	service := &corev1.Service{}
	require.NoError(t, client.Get(ctx, types.NamespacedName{
		Name:      "mdai-dal",
		Namespace: "default",
	}, service))

	deployment := &appsv1.Deployment{}
	require.NoError(t, client.Get(ctx, types.NamespacedName{
		Name:      "mdai-dal",
		Namespace: "default",
	}, deployment))

	assert.Equal(t, "bucket", configMap.Data["MDAI_DAL_S3_BUCKET"])
	assert.Equal(t, int32(otlpHTTPPort), service.Spec.Ports[1].Port)
	assert.Equal(t, "mdai-dal", deployment.Labels[LabelAppNameKey])
}
