package v1

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	_ webhook.CustomValidator = &MdaiCollectorCustomValidator{}
	// nolint:unused
	// log is for logging in this package.
	mdaicollectorlog = logf.Log.WithName("mdaicollector-resource") // Regex explanation:
	// ^[a-z0-9] - must start with lowercase letter or digit
	// [a-z0-9-]* - can contain lowercase letters, digits, or hyphens
	// [a-z0-9]$ - must end with lowercase letter or digit
	// No underscores, dots, slashes, or other special chars that might cause S3 issues
	validNameRegex      = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	validSeverityLevels = []mdaiv1.SeverityLevel{
		mdaiv1.DebugSeverityLevel,
		mdaiv1.InfoSeverityLevel,
		mdaiv1.WarnSeverityLevel,
		mdaiv1.ErrorSeverityLevel,
	}
)

// SetupMdaiCollectorWebhookWithManager registers the webhook for MdaiCollector in the manager.
func SetupMdaiCollectorWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &mdaiv1.MdaiCollector{}).
		WithCustomValidator(&MdaiCollectorCustomValidator{}).
		Complete()
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-hub-mydecisive-ai-v1-mdaicollector,mutating=false,failurePolicy=fail,sideEffects=None,groups=hub.mydecisive.ai,resources=mdaicollectors,verbs=create;update,versions=v1,name=vmdaicollector-v1.kb.io,admissionReviewVersions=v1

// MdaiCollectorCustomValidator struct is responsible for validating the MdaiCollector resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type MdaiCollectorCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type MdaiCollector.
func (v *MdaiCollectorCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	mdaicollector, ok := obj.(*mdaiv1.MdaiCollector)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiCollector object but got %T", obj)
	}
	mdaicollectorlog.Info("Validation for MdaiCollector upon creation", "name", mdaicollector.GetName())

	return v.Validate(mdaicollector)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type MdaiCollector.
func (v *MdaiCollectorCustomValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	mdaicollector, ok := newObj.(*mdaiv1.MdaiCollector)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiCollector object for the newObj but got %T", newObj)
	}
	mdaicollectorlog.Info("Validation for MdaiCollector upon update", "name", mdaicollector.GetName())

	return v.Validate(mdaicollector)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type MdaiCollector.
func (v *MdaiCollectorCustomValidator) ValidateDelete(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	mdaicollector, ok := obj.(*mdaiv1.MdaiCollector)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiCollector object but got %T", obj)
	}
	mdaicollectorlog.Info("Validation for MdaiCollector upon deletion", "name", mdaicollector.GetName())

	return v.Validate(mdaicollector)
}

func ValidateName(name string) bool {
	if len(name) < 1 || len(name) > 32 {
		return false
	}

	return validNameRegex.MatchString(name)
}

func (v *MdaiCollectorCustomValidator) Validate(mdaiCollector *mdaiv1.MdaiCollector) (admission.Warnings, error) {
	warnings := admission.Warnings{}

	if mdaiCollector == nil {
		return warnings, errors.New("expected a MdaiCollector object but got nil")
	}

	if !ValidateName(mdaiCollector.Name) {
		return warnings, fmt.Errorf("MdaiCollector name %q is not valid. Use a lowercase alphanumeric name no longer than 32 characters with dash separators", mdaiCollector.Name)
	}

	spec := mdaiCollector.Spec

	logsConfigPtr := spec.Logs
	awsConfigPtr := spec.AWSConfig

	if logsConfigPtr == nil {
		return append(warnings, "logs configuration not present in MDAI Collector spec"), nil
	}

	s3Warnings, err := v.validateS3LogsConfig(logsConfigPtr.S3, awsConfigPtr)
	warnings = append(warnings, s3Warnings...)
	if err != nil {
		return warnings, err
	}

	otlpWarnings, err := v.validateOtlpLogsConfig(logsConfigPtr)
	warnings = append(warnings, otlpWarnings...)
	if err != nil {
		return warnings, err
	}

	return warnings, nil
}

func (*MdaiCollectorCustomValidator) validateS3LogsConfig(
	s3LogsConfig *mdaiv1.S3LogsConfig,
	awsConfig *mdaiv1.AWSConfig,
) (admission.Warnings, error) {
	var warnings admission.Warnings
	if s3LogsConfig == nil {
		return warnings, nil
	}
	if awsConfig == nil {
		return warnings, errors.New("got s3 logs configuration, but AWSConfig not specified; cannot write logs to s3 without access secret")
	}
	if s3LogsConfig.S3Bucket == "" {
		return warnings, errors.New("s3 logs configuration given but s3Bucket not specified; cannot write logs to s3")
	}
	if s3LogsConfig.S3Region == "" {
		return warnings, errors.New("s3 logs configuration given but s3Region not specified; cannot write logs to s3")
	}
	if s3LogsConfig.AuditLogs != nil && s3LogsConfig.AuditLogs.Disabled {
		warnings = append(warnings, "s3 audit logs disabled, hub audit events will not be recorded to s3!")
	}
	if c := s3LogsConfig.CollectorLogs; c != nil && c.MinSeverity != nil && !slices.Contains(validSeverityLevels, *c.MinSeverity) {
		return warnings, invalidSeverityError("collector logs", *c.MinSeverity)
	}
	if h := s3LogsConfig.HubLogs; h != nil && h.MinSeverity != nil && !slices.Contains(validSeverityLevels, *h.MinSeverity) {
		return warnings, invalidSeverityError("hub logs", *h.MinSeverity)
	}
	if o := s3LogsConfig.OtherLogs; o != nil && o.MinSeverity != nil && !slices.Contains(validSeverityLevels, *o.MinSeverity) {
		return warnings, invalidSeverityError("other logs", *o.MinSeverity)
	}
	if awsConfig.AWSAccessKeySecret == nil {
		return warnings, errors.New("got s3 logs configuration, but awsConfig.accessKeySecret not specified; cannot write logs to s3 without access secret")
	}
	return warnings, nil
}

func (*MdaiCollectorCustomValidator) validateOtlpLogsConfig(logsConfig *mdaiv1.LogsConfig) (admission.Warnings, error) {
	var warnings admission.Warnings
	if logsConfig.Otlp == nil {
		return warnings, nil
	}

	otlpConfig := *logsConfig.Otlp
	if otlpConfig.Endpoint == "" {
		return warnings, errors.New("OTLP logs configuration present but endpoint field is empty. Cannot send logs over OTLP")
	}
	if otlpConfig.AuditLogs != nil && otlpConfig.AuditLogs.Disabled {
		warnings = append(warnings, "OTLP audit logs disabled, hub audit events will not be recorded to OTLP!")
	}
	if c := otlpConfig.CollectorLogs; c != nil && c.MinSeverity != nil && !slices.Contains(validSeverityLevels, *c.MinSeverity) {
		return warnings, invalidSeverityError("collector logs", *c.MinSeverity)
	}
	if h := otlpConfig.HubLogs; h != nil && h.MinSeverity != nil && !slices.Contains(validSeverityLevels, *h.MinSeverity) {
		return warnings, invalidSeverityError("hub logs", *h.MinSeverity)
	}
	if o := otlpConfig.OtherLogs; o != nil && o.MinSeverity != nil && !slices.Contains(validSeverityLevels, *o.MinSeverity) {
		return warnings, invalidSeverityError("other logs", *o.MinSeverity)
	}

	return warnings, nil
}

func invalidSeverityError(logstream string, severity mdaiv1.SeverityLevel) error {
	return fmt.Errorf("s3 logs configuration for %s logstream has an invalid minSeverity: %s. Valid options are: %v", logstream, severity, validSeverityLevels)
}
