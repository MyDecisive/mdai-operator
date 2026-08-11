package v1

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestMdaiDalDefaultsAndValidation(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	k8sClient, stopEnv := startMdaiDalEnv(t)
	t.Cleanup(stopEnv)

	t.Run("defaults are applied", func(t *testing.T) {
		t.Parallel()

		obj := &MdaiDal{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "dal-defaults",
				Namespace: "default",
			},
			Spec: MdaiDalSpec{
				Granularity: "daily",
				S3: MdaiDalS3Config{
					Bucket: "my-bucket",
				},
				AWS: MdaiDalAWSConfig{
					Region: "us-west-2",
				},
			},
		}

		require.NoError(t, k8sClient.Create(ctx, obj))

		var created MdaiDal
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: obj.Name, Namespace: obj.Namespace}, &created))

		require.Equal(t, "json", created.Spec.Marshaler)
		require.False(t, created.Spec.UsePayloadTS)

		require.NotNil(t, created.Spec.ImageSpec)
		require.Equal(t, "public.ecr.aws/decisiveai/mdai-dal", created.Spec.ImageSpec.Repository)
		require.Equal(t, "0.0.1", created.Spec.ImageSpec.Tag)
		require.Equal(t, ImagePullPolicy("IfNotPresent"), created.Spec.ImageSpec.PullPolicy)

		require.Equal(t, "aws-credentials", created.Spec.AWS.Credentials.SecretName)
		require.Equal(t, "AWS_ACCESS_KEY_ID", created.Spec.AWS.Credentials.AccessKeyField)
		require.Equal(t, "AWS_SECRET_ACCESS_KEY", created.Spec.AWS.Credentials.SecretKeyField)
	})

	t.Run("validation rejects invalid specs", func(t *testing.T) {
		tests := []struct {
			name string
			spec MdaiDalSpec
		}{
			{
				name: "invalid granularity enum",
				spec: MdaiDalSpec{
					Granularity: "yearly",
					S3:          MdaiDalS3Config{Bucket: "my-bucket"},
					AWS:         MdaiDalAWSConfig{Region: "us-west-2"},
				},
			},
			{
				name: "bucket too short",
				spec: MdaiDalSpec{
					Granularity: "daily",
					S3:          MdaiDalS3Config{Bucket: "ab"},
					AWS:         MdaiDalAWSConfig{Region: "us-west-2"},
				},
			},
			{
				name: "missing aws region",
				spec: MdaiDalSpec{
					Granularity: "daily",
					S3:          MdaiDalS3Config{Bucket: "my-bucket"},
				},
			},
			{
				name: "missing s3 bucket",
				spec: MdaiDalSpec{
					Granularity: "daily",
					AWS:         MdaiDalAWSConfig{Region: "us-west-2"},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				obj := &MdaiDal{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "dal-invalid-" + tt.name,
						Namespace: "default",
					},
					Spec: tt.spec,
				}

				err := k8sClient.Create(ctx, obj)
				require.Error(t, err)
				require.True(t, errors.IsInvalid(err), "expected validation error, got: %v", err)
			})
		}
	})
}

// startMdaiDalEnv spins up envtest with the CRDs and returns a client and a cleanup function.
func startMdaiDalEnv(t *testing.T) (client.Client, func()) {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, AddToScheme(scheme))

	env := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"),
		},
		ErrorIfCRDPathMissing: true,
	}

	if dir := getFirstFoundEnvTestBinaryDir(); dir != "" {
		env.BinaryAssetsDirectory = dir
	}

	cfg, err := env.Start()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	return k8sClient, func() {
		require.NoError(t, env.Stop())
	}
}

// getFirstFoundEnvTestBinaryDir locates the first binary in the specified path to
// avoid failures when running tests outside the Makefile targets.
func getFirstFoundEnvTestBinaryDir() string {
	basePath := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}
