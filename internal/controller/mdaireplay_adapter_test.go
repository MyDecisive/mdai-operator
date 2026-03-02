package controller

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/mydecisive/mdai-operator/internal/builder"
	"k8s.io/utils/ptr"

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
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNewMdaiReplayAdapter(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	cr := &mdaiv1.MdaiReplay{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-replay",
			Namespace: "default",
		},
	}

	logger := logr.Discard()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	httpClient := &http.Client{}

	adapter := NewMdaiReplayAdapter(cr, logger, k8sClient, recorder, scheme, httpClient, nil)

	assert.NotNil(t, adapter)
	assert.Equal(t, cr, adapter.replayCR)
	assert.Equal(t, logger, adapter.logger)
	assert.Equal(t, k8sClient, adapter.client)
	assert.Equal(t, recorder, adapter.recorder)
	assert.Equal(t, scheme, adapter.scheme)
	assert.Equal(t, httpClient, adapter.httpClient)
}

func TestEnsureDeletionProcessed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name           string
		cr             *mdaiv1.MdaiReplay
		expectContinue bool
		expectStop     bool
		expectRequeue  bool
	}{
		{
			name: "not being deleted",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
			},
			expectContinue: true,
		},
		{
			name: "being deleted with finalizer",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-replay",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					Finalizers:        []string{mdaiReplayFinalizerName},
				},
				Spec: mdaiv1.MdaiReplaySpec{
					IgnoreSendingQueue: true,
				},
			},
			expectStop: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.cr).
				WithStatusSubresource(tt.cr).
				Build()

			adapter := NewMdaiReplayAdapter(
				tt.cr,
				logr.Discard(),
				k8sClient,
				record.NewFakeRecorder(10),
				scheme,
				&http.Client{},
				nil,
			)

			result, err := adapter.ensureDeletionProcessed(context.Background())

			if tt.expectContinue {
				assert.Equal(t, result, ContinueOperationResult())
			}
			if tt.expectStop {
				assert.Equal(t, result, StopOperationResult())
			}
			if tt.expectRequeue {
				expectedResult, _ := Requeue()
				assert.Equal(t, expectedResult, result)
			}
			if !tt.expectRequeue {
				require.NoError(t, err)
			}
		})
	}
}

func TestEnsureFinalizerInitialized(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name              string
		existingFinalizer bool
		expectContinue    bool
		expectStop        bool
	}{
		{
			name:              "finalizer already exists",
			existingFinalizer: true,
			expectContinue:    true,
		},
		{
			name:              "finalizer needs to be added",
			existingFinalizer: false,
			expectStop:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
			}

			if tt.existingFinalizer {
				cr.Finalizers = []string{mdaiReplayFinalizerName}
			}

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cr).
				Build()

			adapter := NewMdaiReplayAdapter(
				cr,
				logr.Discard(),
				k8sClient,
				record.NewFakeRecorder(10),
				scheme,
				&http.Client{},
				nil,
			)

			result, err := adapter.ensureFinalizerInitialized(context.Background())

			require.NoError(t, err)
			if tt.expectContinue {
				assert.Equal(t, result, ContinueOperationResult())
			}
			if tt.expectStop {
				assert.Equal(t, result, StopOperationResult())
			}
		})
	}
}

func TestEnsureStatusInitialized(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)

	tests := []struct {
		name           string
		existingStatus bool
		expectContinue bool
		expectStop     bool
	}{
		{
			name:           "status already initialized",
			existingStatus: true,
			expectContinue: true,
		},
		{
			name:           "status needs initialization",
			existingStatus: false,
			expectStop:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
			}

			if tt.existingStatus {
				cr.Status.Conditions = []metav1.Condition{
					{Type: typeAvailableHub, Status: metav1.ConditionTrue},
				}
			}

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cr).
				WithStatusSubresource(cr).
				Build()

			adapter := NewMdaiReplayAdapter(
				cr,
				logr.Discard(),
				k8sClient,
				record.NewFakeRecorder(10),
				scheme,
				&http.Client{},
				nil,
			)

			result, err := adapter.ensureStatusInitialized(context.Background())

			require.NoError(t, err)
			if tt.expectContinue {
				assert.Equal(t, result, ContinueOperationResult())
			}
			if tt.expectStop {
				assert.Equal(t, result, StopOperationResult())
			}
		})
	}
}

func TestGetReplayerResourceName(t *testing.T) {
	tests := []struct {
		name       string
		replayName string
		hubName    string
		suffix     string
		expected   string
	}{
		{
			name:       "basic resource name",
			replayName: "my-replay",
			hubName:    "my-hub",
			suffix:     "collector",
			expected:   "replay-my-replay-my-hub-collector",
		},
		{
			name:       "service suffix",
			replayName: "test",
			hubName:    "hub1",
			suffix:     "service",
			expected:   "replay-test-hub1-service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.replayName,
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName: tt.hubName,
				},
			}

			adapter := NewMdaiReplayAdapter(
				cr,
				logr.Discard(),
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			result := adapter.getReplayerResourceName(tt.suffix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAugmentCollectorConfigPerSpec(t *testing.T) {
	tests := []struct {
		name     string
		replayId string
		hubName  string
		spec     mdaiv1.MdaiReplaySpec
		validate func(t *testing.T, config builder.ConfigBlock)
	}{
		{
			name:     "basic configuration with replay id",
			replayId: "replay-123",
			hubName:  "hub-456",
			spec: mdaiv1.MdaiReplaySpec{
				StartTime:         "2024-01-01T00:00:00Z",
				EndTime:           "2024-01-02T00:00:00Z",
				StatusVariableRef: "test-var",
				TelemetryType:     mdaiv1.LogsReplayTelemetryType,
				OpAMPEndpoint:     "http://opamp:4320",
				Source: mdaiv1.MdaiReplaySourceConfiguration{
					S3: &mdaiv1.MdaiReplayS3Configuration{
						FilePrefix:  "logs/",
						S3Region:    "us-east-1",
						S3Bucket:    "my-bucket",
						S3Path:      "path/to/logs",
						S3Partition: "year=2024",
					},
				},
				Destination: mdaiv1.MdaiReplayDestinationConfiguration{
					OtlpHttp: &mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
						Endpoint: "http://opamp:4320",
					},
				},
			},
			validate: func(t *testing.T, config builder.ConfigBlock) {
				t.Helper()
				// Check attributes processor has replay_name action
				processors := config.MustMap("processors")
				attrs := processors.MustMap("attributes")
				actions := attrs.MustSlice("actions")

				found := false
				for _, action := range actions {
					actionMap, ok := action.(map[string]string)
					assert.True(t, ok)
					if actionMap["key"] == replayNameKey && actionMap["value"] == "replay-123" {
						found = true
						break
					}
				}
				assert.True(t, found, "replay_name action should be present")

				// Check S3 receiver config
				receivers := config.MustMap("receivers")
				s3Receiver := receivers.MustMap("awss3")
				assert.Equal(t, "2024-01-01T00:00:00Z", s3Receiver.MustString("starttime"))
				assert.Equal(t, "2024-01-02T00:00:00Z", s3Receiver.MustString("endtime"))

				s3Downloader := s3Receiver.MustMap("s3downloader")
				assert.Equal(t, "logs/", s3Downloader.MustString("file_prefix"))
				assert.Equal(t, "us-east-1", s3Downloader.MustString("region"))
				assert.Equal(t, "my-bucket", s3Downloader.MustString("s3_bucket"))

				// Check OpAMP extension
				extensions := config.MustMap("extensions")
				opamp := extensions.MustMap("opamp")
				agentDesc := opamp.MustMap("agent_description")
				// FIXME: Can't use MustMap here because .Set used in the impl makes it a map[string]any instead of ConfigBlock
				nonIdentAttrs, ok := agentDesc["non_identifying_attributes"].(builder.ConfigBlock)
				assert.True(t, ok)
				assert.Equal(t, "replay-123", nonIdentAttrs.MustString("replay_id"))
				assert.Equal(t, "hub-456", nonIdentAttrs.MustString("hub_name"))
			},
		},
		{
			name:     "configuration with log otlp http destination",
			replayId: "replay-456",
			hubName:  "hub-789",
			spec: mdaiv1.MdaiReplaySpec{
				StartTime:         "2024-01-01T00:00:00Z",
				EndTime:           "2024-01-02T00:00:00Z",
				TelemetryType:     mdaiv1.LogsReplayTelemetryType,
				StatusVariableRef: "test-var",
				OpAMPEndpoint:     "http://opamp:4320",
				Destination: mdaiv1.MdaiReplayDestinationConfiguration{
					OtlpHttp: &mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
						Endpoint: "http://otlp:4318",
					},
				},
				Source: mdaiv1.MdaiReplaySourceConfiguration{
					S3: &mdaiv1.MdaiReplayS3Configuration{
						FilePrefix:  "logs/",
						S3Region:    "us-west-2",
						S3Bucket:    "test-bucket",
						S3Path:      "test/path",
						S3Partition: "year=2024",
					},
				},
			},
			validate: func(t *testing.T, config builder.ConfigBlock) {
				t.Helper()
				// Check otlphttp exporter is configured
				// FIXME: Can't use MustMap here because .Set used in the impl makes it a map[string]any instead of ConfigBlock
				exporters, ok := config["exporters"].(builder.ConfigBlock)
				assert.True(t, ok)
				otlpHttp, exists := exporters.GetMap("otlphttp")
				assert.True(t, exists, "otlphttp exporter should exist")

				assert.Equal(t, "http://otlp:4318", otlpHttp.MustString("endpoint"))

				// Check pipeline includes otlphttp exporter
				service := config.MustMap("service")
				pipelines := service.MustMap("pipelines")
				logsReplay := pipelines.MustMap("logs/replay")
				exportersList := logsReplay["exporters"].([]string) // nolint:forcetypeassert

				found := slices.Contains(exportersList, "otlphttp")
				assert.True(t, found, "otlphttp should be in exporters list")
			},
		},
		{
			name:     "configuration with traces otlp http destination",
			replayId: "replay-456",
			hubName:  "hub-789",
			spec: mdaiv1.MdaiReplaySpec{
				StartTime:         "2024-01-01T00:00:00Z",
				EndTime:           "2024-01-02T00:00:00Z",
				TelemetryType:     mdaiv1.TracesReplayTelemetryType,
				StatusVariableRef: "test-var",
				OpAMPEndpoint:     "http://opamp:4320",
				Destination: mdaiv1.MdaiReplayDestinationConfiguration{
					OtlpHttp: &mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
						Endpoint: "http://otlp:4318",
					},
				},
				Source: mdaiv1.MdaiReplaySourceConfiguration{
					S3: &mdaiv1.MdaiReplayS3Configuration{
						FilePrefix:  "logs/",
						S3Region:    "us-west-2",
						S3Bucket:    "test-bucket",
						S3Path:      "test/path",
						S3Partition: "year=2024",
					},
				},
			},
			validate: func(t *testing.T, config builder.ConfigBlock) {
				t.Helper()
				// Check otlphttp exporter is configured
				// FIXME: Can't use MustMap here because .Set used in the impl makes it a map[string]any instead of ConfigBlock
				exporters, ok := config["exporters"].(builder.ConfigBlock)
				assert.True(t, ok)
				otlpHttp, exists := exporters.GetMap("otlphttp")
				assert.True(t, exists, "otlphttp exporter should exist")

				assert.Equal(t, "http://otlp:4318", otlpHttp.MustString("endpoint"))

				// Check pipeline includes otlphttp exporter
				service := config.MustMap("service")
				pipelines := service.MustMap("pipelines")
				tracesPipeline := pipelines.MustMap("traces/replay")
				exportersList := tracesPipeline["exporters"].([]string) // nolint:forcetypeassert

				found := slices.Contains(exportersList, "otlphttp")
				assert.True(t, found, "otlphttp should be in exporters list")
			},
		},
		{
			name:     "configuration with metrics otlp http destination",
			replayId: "replay-456",
			hubName:  "hub-789",
			spec: mdaiv1.MdaiReplaySpec{
				StartTime:         "2024-01-01T00:00:00Z",
				EndTime:           "2024-01-02T00:00:00Z",
				TelemetryType:     mdaiv1.MetricsReplayTelemetryType,
				StatusVariableRef: "test-var",
				OpAMPEndpoint:     "http://opamp:4320",
				Destination: mdaiv1.MdaiReplayDestinationConfiguration{
					OtlpHttp: &mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
						Endpoint: "http://otlp:4318",
					},
				},
				Source: mdaiv1.MdaiReplaySourceConfiguration{
					S3: &mdaiv1.MdaiReplayS3Configuration{
						FilePrefix:  "logs/",
						S3Region:    "us-west-2",
						S3Bucket:    "test-bucket",
						S3Path:      "test/path",
						S3Partition: "year=2024",
					},
				},
			},
			validate: func(t *testing.T, config builder.ConfigBlock) {
				t.Helper()
				// Check otlphttp exporter is configured
				// FIXME: Can't use MustMap here because .Set used in the impl makes it a map[string]any instead of ConfigBlock
				exporters, ok := config["exporters"].(builder.ConfigBlock)
				assert.True(t, ok)
				otlpHttp, exists := exporters.GetMap("otlphttp")
				assert.True(t, exists, "otlphttp exporter should exist")

				assert.Equal(t, "http://otlp:4318", otlpHttp.MustString("endpoint"))

				// Check pipeline includes otlphttp exporter
				service := config.MustMap("service")
				pipelines := service.MustMap("pipelines")
				metricsPipeline := pipelines.MustMap("metrics/replay")
				exportersList := metricsPipeline["exporters"].([]string) // nolint:forcetypeassert

				found := slices.Contains(exportersList, "otlphttp")
				assert.True(t, found, "otlphttp should be in exporters list")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a base config structure
			config := map[string]any{
				"receivers": map[string]any{
					"awss3": map[string]any{
						"s3downloader": map[string]any{},
					},
				},
				"processors": map[string]any{
					"attributes": map[string]any{
						"actions": []any{},
					},
				},
				"exporters": map[string]any{},
				"extensions": map[string]any{
					"opamp": map[string]any{
						"agent_description": map[string]any{
							"non_identifying_attributes": map[string]any{},
						},
						"server": map[string]any{
							"http": map[string]any{},
						},
					},
				},
				"service": map[string]any{
					"pipelines": map[string]any{},
				},
			}

			require.NoError(t, augmentCollectorConfigPerSpec(tt.replayId, tt.hubName, config, tt.spec))
			tt.validate(t, config)
		})
	}
}

func TestAugmentDeploymentWithValues(t *testing.T) {
	tests := []struct {
		name       string
		depName    string
		image      string
		configHash string
		awsSecret  *string
		validate   func(t *testing.T, deployment *appsv1.Deployment)
	}{
		{
			name:       "basic deployment configuration",
			depName:    "test-collector",
			image:      "otel/collector:latest",
			configHash: "abc123",
			validate: func(t *testing.T, deployment *appsv1.Deployment) {
				t.Helper()
				assert.Equal(t, int32(1), *deployment.Spec.Replicas)
				assert.Equal(t, "test-collector", deployment.Labels["app"])
				assert.Equal(t, "test-collector", deployment.Spec.Selector.MatchLabels["app"])
				assert.Equal(t, "abc123", deployment.Spec.Template.Annotations["replay-collector-config/sha256"])

				require.Len(t, deployment.Spec.Template.Spec.Containers, 1)
				container := deployment.Spec.Template.Spec.Containers[0]
				assert.Equal(t, "test-collector", container.Name)
				assert.Equal(t, "otel/collector:latest", container.Image)
				assert.Contains(t, container.Command, "/otelcol-contrib")
			},
		},
		{
			name:       "deployment with AWS secret",
			depName:    "test-collector-secret",
			image:      "custom-image:v1",
			configHash: "def456",
			awsSecret:  ptr.To("aws-creds"),
			validate: func(t *testing.T, deployment *appsv1.Deployment) {
				t.Helper()
				container := deployment.Spec.Template.Spec.Containers[0]

				found := false
				for _, envFrom := range container.EnvFrom {
					if envFrom.SecretRef != nil && envFrom.SecretRef.Name == "aws-creds" {
						found = true
						break
					}
				}
				assert.True(t, found, "AWS secret should be in EnvFrom")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName: "test-hub",
					Source: mdaiv1.MdaiReplaySourceConfiguration{
						AWSConfig: &mdaiv1.MdaiReplayAwsConfig{
							AWSAccessKeySecret: tt.awsSecret,
						},
					},
				},
			}

			adapter := NewMdaiReplayAdapter(
				cr,
				logr.Discard(),
				nil,
				nil,
				runtime.NewScheme(),
				nil,
				nil,
			)

			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.depName,
					Namespace: "default",
				},
			}

			adapter.augmentDeploymentWithValues(deployment, tt.depName, tt.image, tt.configHash)
			tt.validate(t, deployment)
		})
	}
}

func TestGetMetricValue(t *testing.T) {
	tests := []struct {
		name           string
		metricResponse string
		metricName     string
		expectedValue  float64
		expectError    bool
	}{
		{
			name: "gauge metric found",
			metricResponse: `# HELP otelcol_exporter_queue_size Queue size
# TYPE otelcol_exporter_queue_size gauge
otelcol_exporter_queue_size 42.0
`,
			metricName:    "otelcol_exporter_queue_size",
			expectedValue: 42.0,
			expectError:   false,
		},
		{
			name: "counter metric found",
			metricResponse: `# HELP otelcol_exporter_sent_log_records Number of log records sent
# TYPE otelcol_exporter_sent_log_records counter
otelcol_exporter_sent_log_records 100.0
`,
			metricName:    "otelcol_exporter_sent_log_records",
			expectedValue: 100.0,
			expectError:   false,
		},
		{
			name: "metric not found",
			metricResponse: `# HELP other_metric Other metric
# TYPE other_metric gauge
other_metric 10.0
`,
			metricName:    "otelcol_exporter_queue_size",
			expectedValue: 0,
			expectError:   true,
		},
		{
			name:           "empty response",
			metricResponse: "",
			metricName:     "otelcol_exporter_queue_size",
			expectedValue:  0,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/metrics", r.URL.Path)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(tt.metricResponse)) // nolint:errcheck
			}))
			defer server.Close()

			// For this test, we need to mock the service URL resolution
			// In a real test, you'd use the actual getMetricValue method
			// Here we're testing extractMetricValueFromResponse directly
			resp, err := server.Client().Get(server.URL + "/metrics")
			require.NoError(t, err)
			defer resp.Body.Close() // nolint:errcheck

			value, err := extractMetricValueFromResponse(resp.Body, tt.metricName)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.InDelta(t, tt.expectedValue, value, 0)
			}
		})
	}
}

func TestDeleteReplayFinalizer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)

	tests := []struct {
		name               string
		existingFinalizers []string
		finalizerToDelete  string
		expectRemoval      bool
	}{
		{
			name:               "remove existing finalizer",
			existingFinalizers: []string{mdaiReplayFinalizerName, "other-finalizer"},
			finalizerToDelete:  mdaiReplayFinalizerName,
			expectRemoval:      true,
		},
		{
			name:               "finalizer not present",
			existingFinalizers: []string{"other-finalizer"},
			finalizerToDelete:  mdaiReplayFinalizerName,
			expectRemoval:      false,
		},
		{
			name:               "no finalizers",
			existingFinalizers: []string{},
			finalizerToDelete:  mdaiReplayFinalizerName,
			expectRemoval:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-replay",
					Namespace:  "default",
					Finalizers: tt.existingFinalizers,
				},
			}

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cr).
				Build()

			adapter := NewMdaiReplayAdapter(
				cr,
				logr.Discard(),
				k8sClient,
				nil,
				nil,
				nil,
				nil,
			)

			err := adapter.deleteFinalizer(context.Background(), cr, tt.finalizerToDelete)
			require.NoError(t, err)

			// Fetch the updated object
			updated := &mdaiv1.MdaiReplay{}
			err = k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      cr.Name,
				Namespace: cr.Namespace,
			}, updated)
			require.NoError(t, err)

			if tt.expectRemoval {
				assert.NotContains(t, updated.Finalizers, tt.finalizerToDelete)
			}
		})
	}
}

func TestFinalize(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name            string
		cr              *mdaiv1.MdaiReplay
		setupMockServer func() *httptest.Server
		expectedState   ObjectState
		expectError     bool
	}{
		{
			name: "finalizer not present",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName:            "test-hub",
					IgnoreSendingQueue: true,
				},
			},
			expectedState: ObjectModified,
			expectError:   false,
			setupMockServer: func() *httptest.Server {
				return nil
			},
		},
		{
			name: "queue empty - finalize success",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-replay",
					Namespace:  "default",
					Finalizers: []string{mdaiReplayFinalizerName},
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName:            "test-hub",
					IgnoreSendingQueue: false,
				},
			},
			setupMockServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					// nolint:errcheck,revive
					w.Write([]byte(`# TYPE otelcol_exporter_queue_size gauge
otelcol_exporter_queue_size 0.0
`))
				}))
			},
			expectedState: ObjectModified,
			expectError:   false,
		},
		{
			name: "queue not empty - cannot finalize",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-replay",
					Namespace:  "default",
					Finalizers: []string{mdaiReplayFinalizerName},
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName:            "test-hub",
					IgnoreSendingQueue: false,
				},
			},
			setupMockServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					// nolint:errcheck,revive
					w.Write([]byte(`# TYPE otelcol_exporter_queue_size gauge
otelcol_exporter_queue_size 100.0
`))
				}))
			},
			expectedState: ObjectUnchanged,
			expectError:   false,
		},
		{
			name: "ignore sending queue",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-replay",
					Namespace:  "default",
					Finalizers: []string{mdaiReplayFinalizerName},
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName:            "test-hub",
					IgnoreSendingQueue: true,
				},
			},
			setupMockServer: func() *httptest.Server {
				return nil
			},
			expectedState: ObjectModified,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := tt.setupMockServer()
			defer func() {
				if server != nil {
					server.Close()
				}
			}()

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.cr).
				WithStatusSubresource(tt.cr).
				Build()

			var httpClient *http.Client
			if server != nil {
				httpClient = server.Client()
			}

			adapter := NewMdaiReplayAdapter(
				tt.cr,
				logr.Discard(),
				k8sClient,
				record.NewFakeRecorder(10),
				scheme,
				httpClient,
				func(_, _ string) string { return server.URL + "/metrics" },
			)

			state, err := adapter.finalize(context.Background())

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expectedState, state)
		})
	}
}

func TestEnsureReplayStatusSetToDone(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)

	tests := []struct {
		name           string
		cr             *mdaiv1.MdaiReplay
		expectContinue bool
	}{
		{
			name: "successfully set status to done",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName: "test-hub",
				},
			},
			expectContinue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.cr).
				WithStatusSubresource(tt.cr).
				Build()

			adapter := NewMdaiReplayAdapter(
				tt.cr,
				logr.Discard(),
				k8sClient,
				record.NewFakeRecorder(10),
				scheme,
				&http.Client{},
				nil,
			)

			result, err := adapter.ensureStatusSetToDone(context.Background())

			require.NoError(t, err)
			if tt.expectContinue {
				assert.Equal(t, result, ContinueOperationResult())
			}

			// Verify status was updated
			updated := &mdaiv1.MdaiReplay{}
			err = k8sClient.Get(context.Background(), types.NamespacedName{
				Name:      tt.cr.Name,
				Namespace: tt.cr.Namespace,
			}, updated)
			require.NoError(t, err)

			condition := meta.FindStatusCondition(updated.Status.Conditions, typeAvailableHub)
			assert.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionTrue, condition.Status)
			assert.Equal(t, "Reconciling", condition.Reason)
			assert.Equal(t, "reconciled successfully", condition.Message)
		})
	}
}

func TestCreateOrUpdateReplayerConfigMap(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name        string
		cr          *mdaiv1.MdaiReplay
		expectError bool
	}{
		{
			name: "create new configmap",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-02T00:00:00Z",
					StatusVariableRef: "test-var",
					OpAMPEndpoint:     "http://opamp:4320",
					Source: mdaiv1.MdaiReplaySourceConfiguration{
						S3: &mdaiv1.MdaiReplayS3Configuration{
							FilePrefix:  "logs/",
							S3Region:    "us-east-1",
							S3Bucket:    "test-bucket",
							S3Path:      "path/to/logs",
							S3Partition: "year=2024",
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.cr).
				Build()

			adapter := NewMdaiReplayAdapter(
				tt.cr,
				logr.Discard(),
				k8sClient,
				record.NewFakeRecorder(10),
				scheme,
				&http.Client{},
				nil,
			)

			hash, err := adapter.createOrUpdateReplayerConfigMap(context.Background())

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, hash)

				// Verify ConfigMap was created
				configMapName := adapter.getReplayerResourceName("collector-config")
				cm := &corev1.ConfigMap{}
				err = k8sClient.Get(context.Background(), types.NamespacedName{
					Name:      configMapName,
					Namespace: tt.cr.Namespace,
				}, cm)
				require.NoError(t, err)
				assert.Contains(t, cm.Data, "collector.yaml")
				assert.NotEmpty(t, cm.Data["collector.yaml"])

				// Verify labels
				assert.Equal(t, LabelManagedByMdaiValue, cm.Labels[LabelManagedByMdaiKey])
				assert.Equal(t, tt.cr.Spec.HubName, cm.Labels[hubNameLabel])
			}
		})
	}
}

func TestCreateOrUpdateReplayerDeployment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name        string
		cr          *mdaiv1.MdaiReplay
		configHash  string
		expectError bool
	}{
		{
			name: "create deployment with default image",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName: "test-hub",
					Resource: mdaiv1.MdaiReplayResourceConfiguration{
						Image: "",
					},
					Source: mdaiv1.MdaiReplaySourceConfiguration{
						AWSConfig: &mdaiv1.MdaiReplayAwsConfig{},
					},
				},
			},
			configHash:  "abc123",
			expectError: false,
		},
		{
			name: "create deployment with custom image",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName: "test-hub",
					Resource: mdaiv1.MdaiReplayResourceConfiguration{
						Image: "custom/otel-collector:v1.0.0",
					},
					Source: mdaiv1.MdaiReplaySourceConfiguration{
						AWSConfig: &mdaiv1.MdaiReplayAwsConfig{},
					},
				},
			},
			configHash:  "def456",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.cr).
				Build()

			adapter := NewMdaiReplayAdapter(
				tt.cr,
				logr.Discard(),
				k8sClient,
				record.NewFakeRecorder(10),
				scheme,
				&http.Client{},
				nil,
			)

			err := adapter.createOrUpdateReplayerDeployment(context.Background(), tt.configHash)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Verify Deployment was created
				deploymentName := adapter.getReplayerResourceName("collector")
				deployment := &appsv1.Deployment{}
				err = k8sClient.Get(context.Background(), types.NamespacedName{
					Name:      deploymentName,
					Namespace: tt.cr.Namespace,
				}, deployment)
				require.NoError(t, err)

				// Verify deployment configuration
				assert.Equal(t, int32(1), *deployment.Spec.Replicas)
				assert.Equal(t, tt.configHash, deployment.Spec.Template.Annotations["replay-collector-config/sha256"])
				assert.Equal(t, LabelManagedByMdaiValue, deployment.Labels[LabelManagedByMdaiKey])
				assert.Equal(t, replayCollectorHubComponent, deployment.Labels[HubComponentLabel])

				// Verify container
				require.Len(t, deployment.Spec.Template.Spec.Containers, 1)
				container := deployment.Spec.Template.Spec.Containers[0]

				expectedImage := replayerDefaultImage
				if tt.cr.Spec.Resource.Image != "" {
					expectedImage = tt.cr.Spec.Resource.Image
				}
				assert.Equal(t, expectedImage, container.Image)
			}
		})
	}
}

func TestCreateOrUpdateReplayerService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name        string
		cr          *mdaiv1.MdaiReplay
		expectError bool
	}{
		{
			name: "create service successfully",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName: "test-hub",
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.cr).
				Build()

			adapter := NewMdaiReplayAdapter(
				tt.cr,
				logr.Discard(),
				k8sClient,
				record.NewFakeRecorder(10),
				scheme,
				&http.Client{},
				nil,
			)

			err := adapter.createOrUpdateReplayerService(context.Background())

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Verify Service was created
				serviceName := adapter.getReplayerResourceName("service")
				service := &corev1.Service{}
				err = k8sClient.Get(context.Background(), types.NamespacedName{
					Name:      serviceName,
					Namespace: tt.cr.Namespace,
				}, service)
				require.NoError(t, err)

				// Verify service configuration
				assert.Equal(t, corev1.ServiceTypeClusterIP, service.Spec.Type)
				assert.Equal(t, LabelManagedByMdaiValue, service.Labels[LabelManagedByMdaiKey])
				assert.Equal(t, replayCollectorHubComponent, service.Labels[HubComponentLabel])
				assert.Equal(t, tt.cr.Name, service.Labels[hubNameLabel])

				// Verify ports
				require.Len(t, service.Spec.Ports, 1)
				assert.Equal(t, int32(otelMetricsPort), service.Spec.Ports[0].Port)
				assert.Equal(t, "otelcol-metrics", service.Spec.Ports[0].Name)

				// Verify selector
				appLabel := adapter.getReplayerResourceName("collector")
				assert.Equal(t, appLabel, service.Spec.Selector["app"])
			}
		})
	}
}

func TestGetReplayCollectorConfigYAML(t *testing.T) {
	tests := []struct {
		name        string
		cr          *mdaiv1.MdaiReplay
		replayId    string
		hubName     string
		expectError bool
	}{
		{
			name:     "generate valid config yaml",
			replayId: "replay-123",
			hubName:  "hub-456",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-02T00:00:00Z",
					TelemetryType:     mdaiv1.LogsReplayTelemetryType,
					StatusVariableRef: "test-var",
					OpAMPEndpoint:     "http://opamp:4320",
					Source: mdaiv1.MdaiReplaySourceConfiguration{
						S3: &mdaiv1.MdaiReplayS3Configuration{
							FilePrefix:  "logs/",
							S3Region:    "us-east-1",
							S3Bucket:    "test-bucket",
							S3Path:      "path/to/logs",
							S3Partition: "year=2024",
						},
					},
					Destination: mdaiv1.MdaiReplayDestinationConfiguration{
						OtlpHttp: &mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
							Endpoint: "http://otlp:4318",
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := NewMdaiReplayAdapter(
				tt.cr,
				logr.Discard(),
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			yaml, err := adapter.getReplayCollectorConfigYAML(tt.replayId, tt.hubName)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, yaml)

				// Verify YAML contains expected elements
				assert.Contains(t, yaml, "receivers")
				assert.Contains(t, yaml, "processors")
				assert.Contains(t, yaml, "exporters")
				assert.Contains(t, yaml, "service")
			}
		})
	}
}

func TestEnsureSynchronized(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mdaiv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name           string
		cr             *mdaiv1.MdaiReplay
		expectContinue bool
		expectError    bool
	}{
		{
			name: "successfully synchronize all resources",
			cr: &mdaiv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-02T00:00:00Z",
					StatusVariableRef: "test-var",
					OpAMPEndpoint:     "http://opamp:4320",
					Source: mdaiv1.MdaiReplaySourceConfiguration{
						AWSConfig: &mdaiv1.MdaiReplayAwsConfig{
							AWSAccessKeySecret: ptr.To("foobar-secret"),
						},
						S3: &mdaiv1.MdaiReplayS3Configuration{
							FilePrefix:  "logs/",
							S3Region:    "us-east-1",
							S3Bucket:    "test-bucket",
							S3Path:      "path/to/logs",
							S3Partition: "year=2024",
						},
					},
				},
			},
			expectContinue: true,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.cr).
				Build()

			adapter := NewMdaiReplayAdapter(
				tt.cr,
				logr.Discard(),
				k8sClient,
				record.NewFakeRecorder(10),
				scheme,
				&http.Client{},
				nil,
			)

			result, err := adapter.ensureSynchronized(context.Background())

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.expectContinue {
				assert.Equal(t, result, ContinueOperationResult())
			}

			if !tt.expectError {
				// Verify all resources were created
				configMapName := adapter.getReplayerResourceName("collector-config")
				cm := &corev1.ConfigMap{}
				err = k8sClient.Get(context.Background(), types.NamespacedName{
					Name:      configMapName,
					Namespace: tt.cr.Namespace,
				}, cm)
				require.NoError(t, err)

				deploymentName := adapter.getReplayerResourceName("collector")
				deployment := &appsv1.Deployment{}
				err = k8sClient.Get(context.Background(), types.NamespacedName{
					Name:      deploymentName,
					Namespace: tt.cr.Namespace,
				}, deployment)
				require.NoError(t, err)

				serviceName := adapter.getReplayerResourceName("service")
				service := &corev1.Service{}
				err = k8sClient.Get(context.Background(), types.NamespacedName{
					Name:      serviceName,
					Namespace: tt.cr.Namespace,
				}, service)
				require.NoError(t, err)
			}
		})
	}
}

func TestExtractMetricValueFromResponse(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		metricName    string
		expectedValue float64
		expectError   bool
		errorContains string
	}{
		{
			name: "extract gauge metric",
			body: `# HELP otelcol_exporter_queue_size Current size of the retry queue
# TYPE otelcol_exporter_queue_size gauge
otelcol_exporter_queue_size 25.0
`,
			metricName:    "otelcol_exporter_queue_size",
			expectedValue: 25.0,
			expectError:   false,
		},
		{
			name: "extract counter metric",
			body: `# HELP otelcol_exporter_sent_log_records Number of log records sent
# TYPE otelcol_exporter_sent_log_records counter
otelcol_exporter_sent_log_records 1000.0
`,
			metricName:    "otelcol_exporter_sent_log_records",
			expectedValue: 1000.0,
			expectError:   false,
		},
		{
			name: "metric not found",
			body: `# HELP other_metric Some other metric
# TYPE other_metric gauge
other_metric 10.0
`,
			metricName:    "otelcol_exporter_queue_size",
			expectError:   true,
			errorContains: "not found",
		},
		{
			name:          "empty body",
			body:          "",
			metricName:    "otelcol_exporter_queue_size",
			expectError:   true,
			errorContains: "not found",
		},
		{
			name: "metric with zero value",
			body: `# HELP otelcol_exporter_queue_size Queue size
# TYPE otelcol_exporter_queue_size gauge
otelcol_exporter_queue_size 0.0
`,
			metricName:    "otelcol_exporter_queue_size",
			expectedValue: 0.0,
			expectError:   false,
		},
		{
			name: "wrong metric type",
			body: `# HELP otelcol_exporter_queue_size Queue size
# TYPE otelcol_exporter_queue_size histogram
otelcol_exporter_queue_size 0.0
`,
			metricName:    "otelcol_exporter_queue_size",
			expectError:   true,
			errorContains: "need gauge or counter",
		},
		{
			name: "bad metric type",
			body: `# HELP otelcol_exporter_queue_size Queue size
# TYPE otelcol_exporter_queue_size asjkerhiskehraiu
otelcol_exporter_queue_size 0.0
`,
			metricName:    "otelcol_exporter_queue_size",
			expectError:   true,
			errorContains: "unknown metric type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := extractMetricValueFromResponse(
				bytes.NewBufferString(tt.body), tt.metricName)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.InDelta(t, tt.expectedValue, value, 0)
			}
		})
	}
}
