package collector

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
)

func TestPortsForComponentKinds_OnlyParsesEnabledComponents(t *testing.T) {
	const configYAML = `receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
  jaeger:
    protocols:
      grpc:
        endpoint: 0.0.0.0:14250
exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
extensions:
  health_check:
    endpoint: 0.0.0.0:13134
service:
  extensions: [health_check]
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [prometheus]
`
	var cfg v1beta1.Config
	require.NoError(t, yaml.Unmarshal([]byte(configYAML), &cfg))

	got, err := allPorts(zap.NewNop(), &cfg)
	require.NoError(t, err)

	assert.Equal(t, []corev1.ServicePort{
		{
			Name: "health-check",
			Port: 13134,
		},
		{
			Name:        "otlp-http",
			Port:        4318,
			TargetPort:  intstr.FromInt32(4318),
			AppProtocol: new("http"),
		},
		{
			Name: "prometheus",
			Port: 8889,
		},
	}, got)
}

func TestReceiverPorts_IgnoresConfiguredButDisabledReceivers(t *testing.T) {
	const configYAML = `receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
  jaeger:
    protocols:
      grpc:
        endpoint: 0.0.0.0:14250
exporters:
  debug:
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [debug]
`
	var cfg v1beta1.Config
	require.NoError(t, yaml.Unmarshal([]byte(configYAML), &cfg))

	got, err := receiverPorts(zap.NewNop(), &cfg)
	require.NoError(t, err)

	assert.Equal(t, []corev1.ServicePort{
		{
			Name:        "otlp-http",
			Port:        4318,
			TargetPort:  intstr.FromInt32(4318),
			AppProtocol: new("http"),
		},
	}, got)
}
