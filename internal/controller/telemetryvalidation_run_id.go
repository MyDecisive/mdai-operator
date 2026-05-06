package controller

import (
	"crypto/rand"
	"fmt"
	"strings"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

func resolveTelemetryValidationRunID(validation *hubv1.TelemetryValidation) (string, bool, error) {
	specified := strings.TrimSpace(validation.Spec.RunID)
	if specified != "" {
		return specified, specified != validation.Status.RunID, nil
	}
	if strings.TrimSpace(validation.Status.RunID) != "" {
		return validation.Status.RunID, false, nil
	}

	generated, err := generateTelemetryValidationRunID()
	if err != nil {
		return "", false, err
	}
	return generated, true, nil
}

func generateTelemetryValidationRunID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate telemetry validation run id: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40 //nolint:mnd
	b[8] = (b[8] & 0x3f) | 0x80 //nolint:mnd
	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	), nil
}
