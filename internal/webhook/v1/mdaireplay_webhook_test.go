package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

func TestMdaiReplayCustomValidator_ValidateCreate(t *testing.T) {
	validator := &MdaiReplayCustomValidator{}
	ctx := t.Context()

	tests := []struct {
		name        string
		replay      *hubv1.MdaiReplay
		wantErr     bool
		errContains string
	}{
		{
			name: "valid replay with S3 source - RFC3339 time",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
					Source: hubv1.MdaiReplaySourceConfiguration{
						S3: &hubv1.MdaiReplayS3Configuration{
							S3Partition: hubv1.S3ReplayMinutePartition,
						},
						AWSConfig: &hubv1.MdaiReplayAwsConfig{
							AWSAccessKeySecret: new("aws-secret"),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid replay with S3 source - date only time format",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.MetricsReplayTelemetryType,
					StartTime:         "2024-01-01",
					EndTime:           "2024-01-02",
					Source: hubv1.MdaiReplaySourceConfiguration{
						S3: &hubv1.MdaiReplayS3Configuration{
							S3Partition: hubv1.S3ReplayHourPartition,
						},
						AWSConfig: &hubv1.MdaiReplayAwsConfig{
							AWSAccessKeySecret: new("aws-secret"),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid replay with S3 source - custom time format",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.TracesReplayTelemetryType,
					StartTime:         "2024-01-01 15:04",
					EndTime:           "2024-01-01 16:04",
					Source: hubv1.MdaiReplaySourceConfiguration{
						S3: &hubv1.MdaiReplayS3Configuration{
							S3Partition: hubv1.S3ReplayMinutePartition,
						},
						AWSConfig: &hubv1.MdaiReplayAwsConfig{
							AWSAccessKeySecret: new("aws-secret"),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing hubName",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
				},
			},
			wantErr:     true,
			errContains: "hubName cannot be empty",
		},
		{
			name: "missing statusVariableRef",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
				},
			},
			wantErr:     true,
			errContains: "status variable ref cannot be empty",
		},
		{
			name: "missing opAMPEndpoint",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
				},
			},
			wantErr:     true,
			errContains: "opampEndpoint cannot be empty",
		},
		{
			name: "invalid telemetry type",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     "invalid-type",
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
				},
			},
			wantErr:     true,
			errContains: "invalid telemetry type",
		},
		{
			name: "empty telemetry type",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     "",
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
				},
			},
			wantErr:     true,
			errContains: "invalid telemetry type",
		},
		{
			name: "invalid startTime format",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "invalid-time",
					EndTime:           "2024-01-01T01:00:00Z",
				},
			},
			wantErr:     true,
			errContains: "startTime is not in a supported format",
		},
		{
			name: "invalid endTime format",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "not-a-valid-time",
				},
			},
			wantErr:     true,
			errContains: "endTime is not in a supported format",
		},
		{
			name: "S3 source without awsConfig",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
					Source: hubv1.MdaiReplaySourceConfiguration{
						S3: &hubv1.MdaiReplayS3Configuration{
							S3Partition: hubv1.S3ReplayMinutePartition,
						},
					},
				},
			},
			wantErr:     true,
			errContains: "source.awsConfig is not set",
		},
		{
			name: "S3 source without awsAccessKeySecret",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
					Source: hubv1.MdaiReplaySourceConfiguration{
						S3: &hubv1.MdaiReplayS3Configuration{
							S3Partition: hubv1.S3ReplayMinutePartition,
						},
						AWSConfig: &hubv1.MdaiReplayAwsConfig{},
					},
				},
			},
			wantErr:     true,
			errContains: "source.awsConfig.awsAccessKeySecret is not set",
		},
		{
			name: "invalid S3 partition",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
					Source: hubv1.MdaiReplaySourceConfiguration{
						S3: &hubv1.MdaiReplayS3Configuration{
							S3Partition: "invalid-partition",
						},
						AWSConfig: &hubv1.MdaiReplayAwsConfig{
							AWSAccessKeySecret: new("aws-secret"),
						},
					},
				},
			},
			wantErr:     true,
			errContains: "invalid s3 partition",
		},
		{
			name: "empty S3 partition",
			replay: &hubv1.MdaiReplay{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
					Source: hubv1.MdaiReplaySourceConfiguration{
						S3: &hubv1.MdaiReplayS3Configuration{
							S3Partition: "",
						},
						AWSConfig: &hubv1.MdaiReplayAwsConfig{
							AWSAccessKeySecret: new("aws-secret"),
						},
					},
				},
			},
			wantErr:     true,
			errContains: "invalid s3 partition",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warnings, err := validator.ValidateCreate(ctx, tt.replay)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				assert.Empty(t, warnings)
			}
		})
	}
}

func TestMdaiReplayCustomValidator_ValidateUpdate(t *testing.T) {
	validator := &MdaiReplayCustomValidator{}
	ctx := t.Context()

	validReplay := &hubv1.MdaiReplay{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-replay",
			Namespace: "default",
		},
		Spec: hubv1.MdaiReplaySpec{
			HubName:           "test-hub",
			StatusVariableRef: "status-var",
			OpAMPEndpoint:     "http://opamp.example.com",
			TelemetryType:     hubv1.LogsReplayTelemetryType,
			StartTime:         "2024-01-01T00:00:00Z",
			EndTime:           "2024-01-01T01:00:00Z",
			Source: hubv1.MdaiReplaySourceConfiguration{
				S3: &hubv1.MdaiReplayS3Configuration{
					S3Partition: hubv1.S3ReplayMinutePartition,
				},
				AWSConfig: &hubv1.MdaiReplayAwsConfig{
					AWSAccessKeySecret: new("aws-secret"),
				},
			},
		},
	}

	tests := []struct {
		name        string
		oldReplay   *hubv1.MdaiReplay
		newReplay   *hubv1.MdaiReplay
		wantErr     bool
		errContains string
	}{
		{
			name:      "valid update",
			oldReplay: validReplay,
			newReplay: func() *hubv1.MdaiReplay {
				r := validReplay.DeepCopy()
				r.Spec.EndTime = "2024-01-01T02:00:00Z"
				return r
			}(),
			wantErr: false,
		},
		{
			name:      "update to invalid hubName",
			oldReplay: validReplay,
			newReplay: func() *hubv1.MdaiReplay {
				r := validReplay.DeepCopy()
				r.Spec.HubName = ""
				return r
			}(),
			wantErr:     true,
			errContains: "hubName cannot be empty",
		},
		{
			name:      "update to invalid telemetry type",
			oldReplay: validReplay,
			newReplay: func() *hubv1.MdaiReplay {
				r := validReplay.DeepCopy()
				r.Spec.TelemetryType = "invalid"
				return r
			}(),
			wantErr:     true,
			errContains: "invalid telemetry type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warnings, err := validator.ValidateUpdate(ctx, tt.oldReplay, tt.newReplay)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				assert.Empty(t, warnings)
			}
		})
	}
}

func TestMdaiReplayCustomValidator_ValidateDelete(t *testing.T) {
	validator := &MdaiReplayCustomValidator{}
	ctx := t.Context()

	replay := &hubv1.MdaiReplay{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-replay",
			Namespace: "default",
		},
	}

	warnings, err := validator.ValidateDelete(ctx, replay)
	require.NoError(t, err)
	assert.Empty(t, warnings)
}

func TestValidateTimeStr(t *testing.T) {
	tests := []struct {
		name    string
		timeStr string
		wantErr bool
	}{
		{
			name:    "valid RFC3339",
			timeStr: "2024-01-01T00:00:00Z",
			wantErr: false,
		},
		{
			name:    "valid RFC3339 with timezone",
			timeStr: "2024-01-01T15:04:05+07:00",
			wantErr: false,
		},
		{
			name:    "valid custom format",
			timeStr: "2024-01-01 15:04",
			wantErr: false,
		},
		{
			name:    "valid date only",
			timeStr: "2024-01-01",
			wantErr: false,
		},
		{
			name:    "invalid format",
			timeStr: "01/01/2024",
			wantErr: true,
		},
		{
			name:    "invalid string",
			timeStr: "not-a-date",
			wantErr: true,
		},
		{
			name:    "empty string",
			timeStr: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTimeStr(tt.timeStr)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateReplaySpec(t *testing.T) {
	tests := []struct {
		name        string
		replay      *hubv1.MdaiReplay
		wantErr     bool
		errContains string
	}{
		{
			name: "all valid fields",
			replay: &hubv1.MdaiReplay{
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
					Source: hubv1.MdaiReplaySourceConfiguration{
						S3: &hubv1.MdaiReplayS3Configuration{
							S3Partition: hubv1.S3ReplayMinutePartition,
						},
						AWSConfig: &hubv1.MdaiReplayAwsConfig{
							AWSAccessKeySecret: new("aws-secret"),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "no S3 source configured",
			replay: &hubv1.MdaiReplay{
				Spec: hubv1.MdaiReplaySpec{
					HubName:           "test-hub",
					StatusVariableRef: "status-var",
					OpAMPEndpoint:     "http://opamp.example.com",
					TelemetryType:     hubv1.LogsReplayTelemetryType,
					StartTime:         "2024-01-01T00:00:00Z",
					EndTime:           "2024-01-01T01:00:00Z",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warnings, err := validateReplaySpec(tt.replay.Spec, ReplayCustomResourceValidatorMode)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
				assert.Empty(t, warnings)
			}
		})
	}
}
