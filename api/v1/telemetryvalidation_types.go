package v1

import (
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
	// Endpoint is the validator ingress endpoint used for raw OTLP capture and
	// as the basis for shadow Datadog exporter endpoint rewriting.
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`
}

type TelemetryValidationShadowCollectorSpec struct {
	// +kubebuilder:default:=true
	Enabled bool `json:"enabled,omitempty"`
}

type TelemetryValidationIngressCaptureSpec struct {
	// +kubebuilder:default:=true
	Enabled bool `json:"enabled,omitempty"`
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
}

type TelemetryValidationStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	ShadowCollectorName string `json:"shadowCollectorName,omitempty"`
	ShadowServiceName   string `json:"shadowServiceName,omitempty"`
	ObservedGeneration  int64  `json:"observedGeneration,omitempty"`

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
