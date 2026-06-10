package v1

import (
	"go.opentelemetry.io/collector/pdata/pmetric"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:XValidation:rule="self.telemetry_type == 'logs' && has(self.filter) ? !has(self.filter.traces) : true", message="When telemetry_type is 'logs', filter.traces must be omitted."
// +kubebuilder:validation:XValidation:rule="self.telemetry_type == 'traces' && has(self.filter) ? !has(self.filter.logs) : true", message="When telemetry_type is 'traces', filter.logs must be omitted."
// +kubebuilder:validation:XValidation:rule="self.metrics_backend == 'prometheus' ? self.aggregation_temporality == 2 : true", message="When metrics_backend is 'prometheus', aggregation_temporality must be 'cumulative'."
type Observer struct {
	// +kubebuilder:validation:Required
	Name string `json:"name" yaml:"name"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=logs;metrics;traces
	TelemetryType *string `json:"telemetry_type" yaml:"telemetry_type"` //nolint:tagliatelle
	// +kubebuilder:validation:Required
	LabelResourceAttributes []string `json:"labelResourceAttributes" yaml:"labelResourceAttributes"`
	// +optional
	CountMetricName *string `json:"countMetricName,omitempty" yaml:"countMetricName,omitempty"`
	// +optional
	BytesMetricName *string `json:"bytesMetricName,omitempty" yaml:"bytesMetricName,omitempty"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Type=integer
	// +kubebuilder:validation:Format=int32
	// +kubebuilder:validation:Enum=1;2
	AggregationTemporality pmetric.AggregationTemporality `json:"aggregation_temporality" yaml:"aggregation_temporality"` //nolint:tagliatelle
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=prometheus;greptimedb
	MetricsBackend string `json:"metrics_backend" yaml:"metrics_backend"` //nolint:tagliatelle
	// +optional
	Filter *ObserverFilter `json:"filter,omitempty" yaml:"filter,omitempty"`
}

type ObserverLogsFilter struct {
	// +kubebuilder:validation:Required
	LogRecord []string `json:"log_record" yaml:"log_record"` //nolint:tagliatelle
}

type ObserverTracesFilter struct {
	// +kubebuilder:validation:Required
	Span []string `json:"span" yaml:"span"` //nolint:tagliatelle
}

type ObserverFilter struct {
	// +optional
	ErrorMode *string `json:"error_mode" yaml:"error_mode"` //nolint:tagliatelle
	// +optional
	Logs *ObserverLogsFilter `json:"logs" yaml:"logs"`
	// +optional
	Traces *ObserverTracesFilter `json:"traces" yaml:"traces"`
}

// TODO: Add metrics and trace filters

type ObserverResource struct {
	// +kubebuilder:default="public.ecr.aws/decisiveai/observer-collector:0.1.6"
	// +optional
	Image string `json:"image,omitempty"`
	// +kubebuilder:default=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=1
	GrpcReceiverMaxMsgSize *uint64 `json:"grpcReceiverMaxMsgSize,omitempty"`
	// +optional
	OwnLogsOtlpEndpoint *string `json:"ownLogsOtlpEndpoint,omitempty"`
	// +kubebuilder:default={}
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// MdaiObserverSpec defines the desired state of MdaiObserver.
type MdaiObserverSpec struct {
	// +optional
	Observers []Observer `json:"observers,omitempty"`
	// +optional
	ObserverResource ObserverResource `json:"observerResource,omitempty"`
}

// MdaiObserverStatus defines the observed state of MdaiObserver.
type MdaiObserverStatus struct {
	// Status of the Cluster defined by its modules and dependencies statuses
	ObserverStatus string `json:"observerStatus"`

	// +optional
	LastUpdatedTime *metav1.Time `json:"lastUpdatedTime,omitempty"`

	// Conditions store the status conditions of the Cluster instances
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// MdaiObserver is the Schema for the mdaiobservers API.
type MdaiObserver struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MdaiObserverSpec   `json:"spec,omitempty"`
	Status MdaiObserverStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MdaiObserverList contains a list of MdaiObserver.
type MdaiObserverList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []MdaiObserver `json:"items"`
}

func init() { //nolint:gochecknoinits
	SchemeBuilder.Register(&MdaiObserver{}, &MdaiObserverList{})
}
