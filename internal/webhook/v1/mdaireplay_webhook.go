package v1

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

var (
	timeStrLayouts      = []string{time.RFC3339, "2006-01-02 15:04", time.DateOnly}
	validTelemetryTypes = []hubv1.MdaiReplayTelemetryType{hubv1.LogsReplayTelemetryType, hubv1.MetricsReplayTelemetryType, hubv1.TracesReplayTelemetryType}
	validPartitions     = []hubv1.MdaiReplayS3Partition{hubv1.S3ReplayMinutePartition, hubv1.S3ReplayHourPartition}
)

// nolint:unused
// log is for logging in this package.
var mdaireplaylog = logf.Log.WithName("mdaireplay-resource")

// SetupMdaiReplayWebhookWithManager registers the webhook for MdaiReplay in the manager.
func SetupMdaiReplayWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &hubv1.MdaiReplay{}).
		WithCustomValidator(&MdaiReplayCustomValidator{}).
		Complete()
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-hub-mydecisive-ai-v1-mdaireplay,mutating=false,failurePolicy=fail,sideEffects=None,groups=hub.mydecisive.ai,resources=mdaireplays,verbs=create;update,versions=v1,name=vmdaireplay-v1.kb.io,admissionReviewVersions=v1

// MdaiReplayCustomValidator struct is responsible for validating the MdaiReplay resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type MdaiReplayCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &MdaiReplayCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type MdaiReplay.
func (*MdaiReplayCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	mdaireplay, ok := obj.(*hubv1.MdaiReplay)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiReplay object but got %T", obj)
	}
	mdaireplaylog.Info("Validation for MdaiReplay upon creation", "name", mdaireplay.GetName())

	var warnings admission.Warnings

	replaySpecWarnings, err := validateReplaySpec(mdaireplay.Spec, ReplayCustomResourceValidatorMode)
	warnings = append(warnings, replaySpecWarnings...)

	if err != nil {
		return warnings, err
	}

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type MdaiReplay.
func (*MdaiReplayCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	mdaireplay, ok := newObj.(*hubv1.MdaiReplay)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiReplay object for the newObj but got %T", newObj)
	}
	mdaireplaylog.Info("Validation for MdaiReplay upon update", "name", mdaireplay.GetName())

	var warnings admission.Warnings

	replaySpecWarnings, err := validateReplaySpec(mdaireplay.Spec, ReplayCustomResourceValidatorMode)
	warnings = append(warnings, replaySpecWarnings...)

	if err != nil {
		return warnings, err
	}

	return warnings, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type MdaiReplay.
func (*MdaiReplayCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	// deletion validation not used at this time, but method is retained to conform to the webhook.CustomValidator interface
	return nil, nil
}

type ValidatorMode string

const (
	ReplayCustomResourceValidatorMode ValidatorMode = "ValidateForReplayCR"
	HubAutomationValidatorMode        ValidatorMode = "ValidateForHubAutomation"
)

var validValidatorModes = []ValidatorMode{ReplayCustomResourceValidatorMode, HubAutomationValidatorMode}

func validateReplaySpec(mdaiReplaySpec hubv1.MdaiReplaySpec, validatorMode ValidatorMode) (admission.Warnings, error) {
	warnings := admission.Warnings{}

	if !slices.Contains(validValidatorModes, validatorMode) {
		return warnings, errors.New("tried to call validator with invalid validator mode (why/how are you calling this anyway?)")
	}

	if validatorMode != HubAutomationValidatorMode {
		if mdaiReplaySpec.HubName == "" {
			return warnings, errors.New("hubName cannot be empty")
		}
		if err := validateTimeStr(mdaiReplaySpec.StartTime); err != nil {
			return warnings, fmt.Errorf("startTime is not in a supported format. Error: %w", err)
		}
		if err := validateTimeStr(mdaiReplaySpec.EndTime); err != nil {
			return warnings, fmt.Errorf("endTime is not in a supported format. Error: %w", err)
		}
		if mdaiReplaySpec.TelemetryType == "" || !slices.Contains(validTelemetryTypes, mdaiReplaySpec.TelemetryType) {
			return warnings, fmt.Errorf("invalid telemetry type %s, expected one of %s", mdaiReplaySpec.TelemetryType, validTelemetryTypes)
		}
	}
	if mdaiReplaySpec.StatusVariableRef == "" {
		return warnings, errors.New("status variable ref cannot be empty")
	}
	if mdaiReplaySpec.OpAMPEndpoint == "" {
		return warnings, errors.New("opampEndpoint cannot be empty")
	}

	if mdaiReplaySpec.Source.S3 != nil {
		if mdaiReplaySpec.Source.AWSConfig == nil {
			return warnings, errors.New("source.awsConfig is not set, but is required for the s3 source")
		}
		if mdaiReplaySpec.Source.AWSConfig.AWSAccessKeySecret == nil {
			return warnings, errors.New("source.awsConfig.awsAccessKeySecret is not set, but is required for the s3 source")
		}

		s3Config := mdaiReplaySpec.Source.S3
		if s3Config.S3Partition == "" || !slices.Contains(validPartitions, s3Config.S3Partition) {
			return warnings, fmt.Errorf("invalid s3 partition %s, expected one of %s", s3Config.S3Partition, validPartitions)
		}
	}
	return warnings, nil
}

// validateTimeStr validates that time matches the formats supported by the AWS S3 receiver referenced at
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/805c95e8da7c0bdcb6b5e97985caaf7f7d553ecc/receiver/awss3receiver/config.go#L152-L161
func validateTimeStr(timeStr string) error {
	var parseErrs error
	for _, layout := range timeStrLayouts {
		_, err := time.Parse(layout, timeStr)
		if err == nil {
			// if a datetime successfully parsed, return without error
			return nil
		}
		parseErrs = errors.Join(err, parseErrs)
	}
	// if all formats returned in error, bubble up a decent error message
	return fmt.Errorf("invalid time format: %s, expected one of %s. Parser errors: %w", timeStr, timeStrLayouts, parseErrs)
}
