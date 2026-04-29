package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

func TestResolveTelemetryValidationRunID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		specRunID   string
		statusRunID string
		want        string
		wantUpdate  bool
	}{
		{
			name:        "uses specified run id",
			specRunID:   "manual-run",
			statusRunID: "",
			want:        "manual-run",
			wantUpdate:  true,
		},
		{
			name:        "specified run id replaces status",
			specRunID:   "manual-run-2",
			statusRunID: "manual-run-1",
			want:        "manual-run-2",
			wantUpdate:  true,
		},
		{
			name:        "reuses generated status run id",
			statusRunID: "existing-generated",
			want:        "existing-generated",
			wantUpdate:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			validation := &hubv1.TelemetryValidation{
				Spec: hubv1.TelemetryValidationSpec{
					RunID: tt.specRunID,
				},
				Status: hubv1.TelemetryValidationStatus{
					RunID: tt.statusRunID,
				},
			}

			got, shouldUpdate, err := resolveTelemetryValidationRunID(validation)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantUpdate, shouldUpdate)
		})
	}
}

func TestGenerateTelemetryValidationRunID(t *testing.T) {
	t.Parallel()

	first, err := generateTelemetryValidationRunID()
	require.NoError(t, err)
	second, err := generateTelemetryValidationRunID()
	require.NoError(t, err)

	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, first)
	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, second)
	assert.NotEqual(t, first, second)
}
