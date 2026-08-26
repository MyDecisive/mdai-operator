// nolint: gofumpt, goimports
package v1

import (
	"testing"

	goyaml "github.com/goccy/go-yaml"
	"github.com/mydecisive/mdai-operator/internal/components"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestGetPortsWithUrlPathsForComponentKinds(t *testing.T) {
	type args struct {
		componentKinds v1beta1.ComponentKind
	}
	one := int32(1)
	grpc := "grpc"
	http := "http"

	var configYaml = `receivers:
  jaeger:
     protocols:
        grpc:
          endpoint: 0.0.0.0:14260
  otlp:
     protocols:
        grpc:
          endpoint: 0.0.0.0:4317
        http:
          endpoint: 0.0.0.0:4318
exporters:
  debug:
service:
  pipelines:
    metrics:
      receivers: [otlp, jaeger]
      exporters: [debug]
`

	var ingressYaml = `apiVersion: hub.mydecisive.ai/v1
kind: MdaiIngress
metadata:
  name: test
  namespace: test
spec:
  cloudType: aws
  annotations:
    ingress.annotation: ingress_annotation_value
  grpcService:
    type: NodePort
    annotations:
      grpc.service.annotation: grpc_service_annotation_value
  nonGrpcService:
    type: LoadBalancer
    annotations:
      non.grpc.service.annotation: non_grpc_service_annotation_value
  collectorEndpoints:
    otlp: otlp.mdai.io
    jaeger: jaeger.mdai.io
  otelCollector:
    name: test
    namespace: test
`

	goodConfig := v1beta1.Config{}
	err := goyaml.Unmarshal([]byte(configYaml), &goodConfig)
	require.NoError(t, err)

	mdaiIngress := MdaiIngress{}
	err = goyaml.Unmarshal([]byte(ingressYaml), &mdaiIngress)
	require.NoError(t, err)

	tests := []struct {
		name     string
		instance OtelMdaiIngressComb
		args     args
		want     components.ComponentsPortsUrlPaths
	}{
		{
			name: "basic",
			instance: OtelMdaiIngressComb{
				Otelcol: v1beta1.OpenTelemetryCollector{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test",
						Namespace: "test",
					},
					Spec: v1beta1.OpenTelemetryCollectorSpec{
						OpenTelemetryCommonFields: v1beta1.OpenTelemetryCommonFields{
							Image:    "test",
							Replicas: &one,
						},
						Mode:   "deployment",
						Config: goodConfig,
					},
				},
				MdaiIngress: mdaiIngress,
			},
			args: args{
				v1beta1.KindReceiver,
			},
			want: components.ComponentsPortsUrlPaths{
				"jaeger": []components.PortUrlPaths{
					{
						Port: v1.ServicePort{
							Name: "jaeger-grpc",
							Port: 14260,
							TargetPort: intstr.IntOrString{
								Type:   intstr.Int,
								IntVal: 14260,
								StrVal: "",
							},
							Protocol:    v1.ProtocolTCP,
							AppProtocol: &grpc,
							NodePort:    0,
						},
						UrlPaths: []string{
							"/jaeger.api_v2/CollectorService",
							"/jaeger.api_v3/QueryService",
						},
					},
				},
				"otlp": []components.PortUrlPaths{
					{
						Port: v1.ServicePort{
							Name: "otlp-grpc",
							Port: 4317,
							TargetPort: intstr.IntOrString{
								Type:   intstr.Int,
								IntVal: 4317,
								StrVal: "",
							},
							Protocol:    "",
							AppProtocol: &grpc,
							NodePort:    0,
						},
						UrlPaths: []string{
							"/opentelemetry.proto.collector.logs.v1.LogsService",
							"/opentelemetry.proto.collector.traces.v1.TracesService",
							"/opentelemetry.proto.collector.metrics.v1.MetricsService",
						},
					},
					{
						Port: v1.ServicePort{
							Name: "otlp-http",
							Port: 4318,
							TargetPort: intstr.IntOrString{
								Type:   intstr.Int,
								IntVal: 4318,
								StrVal: "",
							},
							Protocol:    "",
							AppProtocol: &http,
							NodePort:    0,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			got, err := tt.instance.getPortsWithUrlPathsForComponentKinds(logger, tt.args.componentKinds)
			require.NoError(t, err)

			require.Len(t, got, 2)
			require.ElementsMatch(t, tt.want["jaeger"], got["jaeger"])
			require.ElementsMatch(t, tt.want["otlp"], got["otlp"])
		})
	}
}

func TestGetPortsWithUrlPathsForComponentKinds_IgnoresConfiguredButDisabledReceivers(t *testing.T) {
	const configYaml = `receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
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
	var config v1beta1.Config
	require.NoError(t, goyaml.Unmarshal([]byte(configYaml), &config))

	comb := OtelMdaiIngressComb{
		Otelcol: v1beta1.OpenTelemetryCollector{
			Spec: v1beta1.OpenTelemetryCollectorSpec{
				Config: config,
			},
		},
	}

	got, err := comb.GetReceiverPortsWithUrlPaths(zap.NewNop())
	require.NoError(t, err)

	require.Contains(t, got, "otlp")
	require.NotContains(t, got, "jaeger")
	require.Len(t, got["otlp"], 1)
	require.NotEmpty(t, got["otlp"][0].UrlPaths)
}
