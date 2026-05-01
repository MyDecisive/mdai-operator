package controller

const (
	telemetryValidationLabelKey              = "hub.mydecisive.ai/telemetry-validation"
	telemetryValidationRunIDAnnotationKey    = "hub.mydecisive.ai/telemetry-validation-run-id"
	telemetryValidationRunIDMetricLabel      = "telemetry_validation_run_id"
	telemetryValidationPrometheusSourceLabel = "__meta_kubernetes_service_label_hub_mydecisive_ai_telemetry_validation"
	telemetryValidationRunIDPrometheusSource = "__meta_kubernetes_service_annotation_hub_mydecisive_ai_telemetry_validation_run_id"
	telemetryValidationRoleShadow            = "telemetry-validation-shadow-collector"
	telemetryValidationRoleValidator         = "telemetry-validation-validator"
)
