package v1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=metrics;logs;traces
type TelemetrySignal string

const (
	TelemetrySignalMetrics TelemetrySignal = "metrics"
	TelemetrySignalLogs    TelemetrySignal = "logs"
	TelemetrySignalTraces  TelemetrySignal = "traces"
)

type TelemetryValidationCollectorRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

type TelemetryValidationValidatorSpec struct {
	// +optional
	// +kubebuilder:default:="ghcr.io/mydecisive/mdai-fidelity-validator:0.1.0"
	Image string `json:"image,omitempty"`

	// +optional
	// +kubebuilder:default:=1
	Replicas *int32 `json:"replicas,omitempty"`

	// +optional
	// +kubebuilder:default:=18081
	Port int32 `json:"port,omitempty"`

	// +optional
	RulesYAML string `json:"rulesYAML,omitempty"`

	// +optional
	FieldMappingYAML string `json:"fieldMappingYAML,omitempty"`
}

type TelemetryValidationShadowCollectorSpec struct {
	// +kubebuilder:default:=true
	Enabled bool `json:"enabled,omitempty"`
}

type TelemetryValidationIngressCaptureSpec struct {
	// +kubebuilder:default:=true
	Enabled bool `json:"enabled,omitempty"`
}

type TelemetryValidationExporterRewrite struct {
	// +optional
	Name string `json:"name,omitempty"`

	// +kubebuilder:validation:MinItems=1
	MatchExporterPrefixes []string `json:"matchExporterPrefixes"`

	// +optional
	Set map[string]apiextensionsv1.JSON `json:"set,omitempty"`

	// +optional
	ReplaceStrings map[string]string `json:"replaceStrings,omitempty"`
}

type TelemetryValidationSpec struct {
	// +kubebuilder:default:=true
	Enabled bool `json:"enabled,omitempty"`

	CollectorRef TelemetryValidationCollectorRef `json:"collectorRef"`

	// +optional
	Signals []TelemetrySignal `json:"signals,omitempty"`

	Validator TelemetryValidationValidatorSpec `json:"validator"`

	// +optional
	ShadowCollector TelemetryValidationShadowCollectorSpec `json:"shadowCollector,omitempty"`

	// +optional
	IngressCapture TelemetryValidationIngressCaptureSpec `json:"ingressCapture,omitempty"`

	// +optional
	ExporterRewrites []TelemetryValidationExporterRewrite `json:"exporterRewrites,omitempty"`
}

type TelemetryValidationStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	ShadowCollectorName  string `json:"shadowCollectorName,omitempty"`
	ShadowServiceName    string `json:"shadowServiceName,omitempty"`
	ValidatorName        string `json:"validatorName,omitempty"`
	ValidatorService     string `json:"validatorService,omitempty"`
	ValidatorEndpoint    string `json:"validatorEndpoint,omitempty"`
	ValidatorIngressPort int32  `json:"validatorIngressPort,omitempty"`
	ObservedGeneration   int64  `json:"observedGeneration,omitempty"`

	// +optional
	ActiveSignals []TelemetrySignal `json:"activeSignals,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TelemetryValidation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TelemetryValidationSpec   `json:"spec,omitempty"`
	Status TelemetryValidationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type TelemetryValidationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TelemetryValidation `json:"items"`
}

func init() { //nolint:gochecknoinits
	SchemeBuilder.Register(&TelemetryValidation{}, &TelemetryValidationList{})
}
