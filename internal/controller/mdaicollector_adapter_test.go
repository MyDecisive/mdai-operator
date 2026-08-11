package controller

import (
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/mydecisive/mdai-operator/internal/builder"
	prometheusv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1core "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newFakeClientForCollectorCR(cr *mdaiv1.MdaiCollector, scheme *runtime.Scheme) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
}

func createTestSchemeForMdaiCollector() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = v1core.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = prometheusv1.AddToScheme(scheme)
	_ = mdaiv1.AddToScheme(scheme)
	return scheme
}

func TestGetS3ExporterForLogstream(t *testing.T) {
	testCases := []struct {
		hubName                string
		logstream              mdaiv1.MDAILogStream
		s3LogsConfig           mdaiv1.S3LogsConfig
		expectedExporterName   string
		expectedExporterConfig s3ExporterConfig
	}{
		{
			hubName:   "test-hub",
			logstream: mdaiv1.CollectorLogstream,
			s3LogsConfig: mdaiv1.S3LogsConfig{
				S3Region: "uesc-marathon-7",
				S3Bucket: "whoa-bucket",
			},
			expectedExporterName: "awss3/collector",
			expectedExporterConfig: s3ExporterConfig{
				S3Uploader: s3UploaderConfig{
					Region:            "uesc-marathon-7",
					S3Bucket:          "whoa-bucket",
					S3Prefix:          "test-hub-collector-logs",
					FilePrefix:        "collector-",
					S3PartitionFormat: S3PartitionFormat,
					DisableSSL:        true,
				},
			},
		}, {
			hubName:   "inf",
			logstream: mdaiv1.HubLogstream,
			s3LogsConfig: mdaiv1.S3LogsConfig{
				S3Region: "aeiou-meh-99",
				S3Bucket: "qwerty",
			},
			expectedExporterName: "awss3/hub",
			expectedExporterConfig: s3ExporterConfig{
				S3Uploader: s3UploaderConfig{
					Region:            "aeiou-meh-99",
					S3Bucket:          "qwerty",
					S3Prefix:          "inf-hub-logs",
					FilePrefix:        "hub-",
					S3PartitionFormat: S3PartitionFormat,
					DisableSSL:        true,
				},
			},
		}, {
			hubName:   "whoa",
			logstream: mdaiv1.AuditLogstream,
			s3LogsConfig: mdaiv1.S3LogsConfig{
				S3Region: "splat",
				S3Bucket: "hey",
			},
			expectedExporterName: "awss3/audit",
			expectedExporterConfig: s3ExporterConfig{
				S3Uploader: s3UploaderConfig{
					Region:            "splat",
					S3Bucket:          "hey",
					S3Prefix:          "whoa-audit-logs",
					FilePrefix:        "audit-",
					S3PartitionFormat: S3PartitionFormat,
					DisableSSL:        true,
				},
			},
		}, {
			hubName:   "heh",
			logstream: mdaiv1.OtherLogstream,
			s3LogsConfig: mdaiv1.S3LogsConfig{
				S3Region: "okay",
				S3Bucket: "ytho",
			},
			expectedExporterName: "awss3/other",
			expectedExporterConfig: s3ExporterConfig{
				S3Uploader: s3UploaderConfig{
					Region:            "okay",
					S3Bucket:          "ytho",
					S3Prefix:          "heh-other-logs",
					FilePrefix:        "other-",
					S3PartitionFormat: S3PartitionFormat,
					DisableSSL:        true,
				},
			},
		},
	}
	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("Case %d %s %s %s", idx, testCase.hubName, testCase.logstream, testCase.expectedExporterName), func(t *testing.T) {
			actualExporterName, actualExporterConfig := getS3ExporterForLogstream(testCase.hubName, testCase.logstream, testCase.s3LogsConfig)
			assert.Equal(t, testCase.expectedExporterName, actualExporterName)
			assert.Equal(t, testCase.expectedExporterConfig, actualExporterConfig)
		})
	}
}

func TestGetPipelineWithS3Exporter(t *testing.T) {
	testCases := []struct {
		receiverName  string
		severityLevel mdaiv1.SeverityLevel
		exporterName  string
		expected      builder.ConfigBlock
	}{
		{
			receiverName:  "routing/logstream",
			severityLevel: mdaiv1.WarnSeverityLevel,
			exporterName:  "awss3/collector",
			expected: builder.ConfigBlock{
				"receivers":  []any{"routing/logstream"},
				"processors": []any{severityFilterMap[mdaiv1.WarnSeverityLevel], "batch"},
				"exporters":  []any{"awss3/collector"},
			},
		},
		{
			receiverName:  "foobaz",
			severityLevel: mdaiv1.InfoSeverityLevel,
			exporterName:  "awss3/hub",
			expected: builder.ConfigBlock{
				"receivers":  []any{"foobaz"},
				"processors": []any{severityFilterMap[mdaiv1.InfoSeverityLevel], "batch"},
				"exporters":  []any{"awss3/hub"},
			},
		},
	}

	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("Case %d %s", idx, testCase.exporterName), func(t *testing.T) {
			assert.Equal(t, testCase.expected, getPipelineWithExporterAndSeverityFilter(testCase.receiverName, testCase.exporterName, new(testCase.severityLevel), "batch"))
		})
	}
}

func TestCreateOrUpdateMdaiCollectorRole(t *testing.T) {
	cr := &mdaiv1.MdaiCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hub",
			Namespace: "default",
		},
	}
	scheme := createTestSchemeForMdaiCollector()
	require.NoError(t, rbacv1.AddToScheme(scheme))

	cl := newFakeClientForCollectorCR(cr, scheme)
	adapter := NewMdaiCollectorAdapter(cr, logr.Discard(), cl, record.NewFakeRecorder(10), scheme)

	expectedName := adapter.getScopedMdaiCollectorResourceName("role")

	t.Run(fmt.Sprintf("creates or updates ClusterRole %q", expectedName), func(t *testing.T) {
		ctx := t.Context()
		name, err := adapter.createOrUpdateMdaiCollectorRole(ctx)
		require.NoError(t, err)
		assert.Equal(t, expectedName, name)

		var role rbacv1.ClusterRole
		require.NoError(t,
			cl.Get(ctx, client.ObjectKey{Name: name}, &role),
		)

		assert.Equal(t, expectedName, role.Name)
		assert.Equal(t, "mdai-operator", role.Labels["app.kubernetes.io/managed-by"])
		assert.Equal(t, MdaiCollectorHubComponent, role.Labels[HubComponentLabel])
		assert.Len(t, role.Rules, 5)

		for _, r := range role.Rules {
			assert.Contains(t, r.Verbs, "get")
			assert.Contains(t, r.Verbs, "list")
			assert.Contains(t, r.Verbs, "watch")
			assert.NotEmpty(t, r.Resources)
		}
	})
}

func TestCreateOrUpdateMdaiCollectorRoleBinding(t *testing.T) {
	cr := &mdaiv1.MdaiCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hub",
			Namespace: "default",
		},
	}
	scheme := createTestSchemeForMdaiCollector()
	require.NoError(t, rbacv1.AddToScheme(scheme))

	cl := newFakeClientForCollectorCR(cr, scheme)
	adapter := NewMdaiCollectorAdapter(cr, logr.Discard(), cl, record.NewFakeRecorder(10), scheme)

	expectedName := adapter.getScopedMdaiCollectorResourceName("rb")

	namespace := "mdai"
	roleName := "mdai-role"
	saName := "mdai-sa"

	t.Run(fmt.Sprintf("creates/updates RoleBinding %q", expectedName), func(t *testing.T) {
		ctx := t.Context()
		err := adapter.createOrUpdateMdaiCollectorRoleBinding(ctx, namespace, roleName, saName)
		require.NoError(t, err)

		var rb rbacv1.ClusterRoleBinding
		require.NoError(t, cl.Get(ctx, client.ObjectKey{Name: expectedName}, &rb))

		assert.Equal(t, expectedName, rb.Name)
		assert.Equal(t, map[string]string{
			"app.kubernetes.io/managed-by": "mdai-operator",
			HubComponentLabel:              MdaiCollectorHubComponent,
		}, rb.Labels)

		assert.Equal(t, "rbac.authorization.k8s.io", rb.RoleRef.APIGroup)
		assert.Equal(t, "ClusterRole", rb.RoleRef.Kind)
		assert.Equal(t, roleName, rb.RoleRef.Name)

		if assert.Len(t, rb.Subjects, 1) {
			subj := rb.Subjects[0]
			assert.Equal(t, "ServiceAccount", subj.Kind)
			assert.Equal(t, saName, subj.Name)
			assert.Equal(t, namespace, subj.Namespace)
		}
	})
}
