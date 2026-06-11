package controller

import (
	"testing"

	"github.com/go-logr/logr"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/mydecisive/mdai-operator/internal/builder"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"sigs.k8s.io/yaml"
)

func TestGetObserverCollectorConfig(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		desc             string
		observers        []mdaiv1.Observer
		observerResource mdaiv1.ObserverResource
		check            func(t *testing.T, resultConfig string, err error)
	}{
		{
			desc:      "no observers provided",
			observers: []mdaiv1.Observer{},
			observerResource: mdaiv1.ObserverResource{
				GrpcReceiverMaxMsgSize: lo.ToPtr(uint64(123)),
				OwnLogsOtlpEndpoint:    lo.ToPtr("otlp://my.endpoint:4317"),
			},
			check: func(t *testing.T, resultConfig string, err error) {
				t.Helper()
				require.NoError(t, err)

				var config builder.ConfigBlock
				require.NoError(t, yaml.Unmarshal([]byte(resultConfig), &config))

				serviceBlock := config.MustMap("service")
				require.Len(t, serviceBlock.MustMap("pipelines"), 1) // only the metrics pipeline, no logs
				assert.NotNil(t, serviceBlock.MustMap("pipelines").MustMap("metrics/observeroutput"))

				grpcReceiverMaxMsgSize := config.MustMap("receivers").MustMap("otlp").MustMap("protocols").MustMap("grpc").MustFloat("max_recv_msg_size_mib")
				assert.Equal(t, 123, int(grpcReceiverMaxMsgSize)) // yaml unmarshal converts ints to floats

				telemetryProcessors := serviceBlock.MustMap("telemetry").MustMap("logs").MustSlice("processors")
				require.Len(t, telemetryProcessors, 1)
				telemetryProcessor, ok := telemetryProcessors[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "otlp://my.endpoint:4317", builder.ConfigBlock(telemetryProcessor).MustMap("batch").MustMap("exporter").MustMap("otlp").MustString("endpoint"))
			},
		},
		{
			desc: "observers present",
			observers: []mdaiv1.Observer{
				{
					Name:                    "observer-in",
					TelemetryType:           lo.ToPtr("logs"),
					LabelResourceAttributes: []string{"mdai_service"},
					CountMetricName:         lo.ToPtr("items_received_by_service_total"),
					BytesMetricName:         lo.ToPtr("bytes_received_by_service_total"),
					AggregationTemporality:  pmetric.AggregationTemporalityCumulative,
					MetricsBackend:          "prometheus",
					Filter: &mdaiv1.ObserverFilter{
						ErrorMode: lo.ToPtr("ignore"),
						Logs: &mdaiv1.ObserverLogsFilter{
							LogRecord: []string{`resource.attributes["observer_direction"] != "received"`},
						},
					},
				},
				{
					Name:                    "observer-out",
					TelemetryType:           lo.ToPtr("logs"),
					LabelResourceAttributes: []string{"mdai_service"},
					CountMetricName:         lo.ToPtr("items_sent_by_service_total"),
					BytesMetricName:         lo.ToPtr("bytes_sent_by_service_total"),
					AggregationTemporality:  pmetric.AggregationTemporalityCumulative,
					MetricsBackend:          "prometheus",
					Filter: &mdaiv1.ObserverFilter{
						ErrorMode: lo.ToPtr("ignore"),
						Logs: &mdaiv1.ObserverLogsFilter{
							LogRecord: []string{`resource.attributes["observer_direction"] != "exported"`},
						},
					},
				},
			},
			observerResource: mdaiv1.ObserverResource{
				GrpcReceiverMaxMsgSize: lo.ToPtr(uint64(123)),
				OwnLogsOtlpEndpoint:    lo.ToPtr("otlp://my.endpoint:4317"),
			},
			check: func(t *testing.T, resultConfig string, err error) {
				t.Helper()
				require.NoError(t, err)

				var config builder.ConfigBlock
				require.NoError(t, yaml.Unmarshal([]byte(resultConfig), &config))

				serviceBlock := config.MustMap("service")
				// pipelines: 1 metric, 2 logs
				pipelines := serviceBlock.MustMap("pipelines")
				require.Len(t, pipelines, 3)
				assert.NotNil(t, pipelines.MustMap("metrics/observeroutput"))
				assert.NotNil(t, pipelines.MustMap("logs/observer-in"))
				assert.NotNil(t, pipelines.MustMap("logs/observer-out"))

				grpcReceiverMaxMsgSize := config.MustMap("receivers").MustMap("otlp").MustMap("protocols").MustMap("grpc").MustFloat("max_recv_msg_size_mib")
				assert.Equal(t, 123, int(grpcReceiverMaxMsgSize)) // yaml unmarshal converts ints to floats

				telemetryProcessors := serviceBlock.MustMap("telemetry").MustMap("logs").MustSlice("processors")
				require.Len(t, telemetryProcessors, 1)
				telemetryProcessor, ok := telemetryProcessors[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "otlp://my.endpoint:4317", builder.ConfigBlock(telemetryProcessor).MustMap("batch").MustMap("exporter").MustMap("otlp").MustString("endpoint"))

				// now, validate the observer config was added
				processors := config.MustMap("processors")
				require.Len(t, processors, 6)

				require.NotNil(t, processors.MustMap("groupbyattrs/observer-in"))
				assert.ElementsMatch(t, []string{"mdai_service"}, processors.MustMap("groupbyattrs/observer-in").MustSlice("keys"))

				require.NotNil(t, processors.MustMap("filter/observer-in"))
				require.NotNil(t, processors.MustMap("filter/observer-out"))

				connectors := config.MustMap("connectors")
				require.NotNil(t, connectors.MustMap("datavolume/observer-in"))
				assert.Equal(t, []any{"mdai_service"}, connectors.MustMap("datavolume/observer-in").MustSlice("label_resource_attributes"))
				assert.Equal(t, 2, int(connectors.MustMap("datavolume/observer-in").MustFloat("aggregation_temporality")))
				assert.Equal(t, "items_received_by_service_total", connectors.MustMap("datavolume/observer-in").MustString("count_metric_name"))
				assert.Equal(t, "bytes_received_by_service_total", connectors.MustMap("datavolume/observer-in").MustString("bytes_metric_name"))

				require.NotNil(t, connectors.MustMap("datavolume/observer-out"))
				assert.Equal(t, []any{"mdai_service"}, connectors.MustMap("datavolume/observer-out").MustSlice("label_resource_attributes"))
				assert.Equal(t, 2, int(connectors.MustMap("datavolume/observer-out").MustFloat("aggregation_temporality")))
				assert.Equal(t, "items_sent_by_service_total", connectors.MustMap("datavolume/observer-out").MustString("count_metric_name"))
				assert.Equal(t, "bytes_sent_by_service_total", connectors.MustMap("datavolume/observer-out").MustString("bytes_metric_name"))
			},
		},
		{
			desc: "greptimedb observer uses otlphttp exporter and auth extension",
			observers: []mdaiv1.Observer{
				{
					Name:                    "trace-observer",
					TelemetryType:           lo.ToPtr("traces"),
					LabelResourceAttributes: []string{"service.name"},
					AggregationTemporality:  pmetric.AggregationTemporalityDelta,
					MetricsBackend:          "greptimedb",
					Filter: &mdaiv1.ObserverFilter{
						ErrorMode: lo.ToPtr("ignore"),
						Traces: &mdaiv1.ObserverTracesFilter{
							Span: []string{`attributes["http.status_code"] >= 500`},
						},
					},
				},
			},
			check: func(t *testing.T, resultConfig string, err error) {
				t.Helper()
				require.NoError(t, err)

				var config builder.ConfigBlock
				require.NoError(t, yaml.Unmarshal([]byte(resultConfig), &config))

				pipelines := config.MustMap("service").MustMap("pipelines")
				require.Len(t, pipelines, 2)
				assert.NotNil(t, pipelines.MustMap("traces/trace-observer"))

				greptimePipeline := pipelines.MustMap("metrics/observeroutput/greptimedb")
				assert.Equal(t, []any{"datavolume/trace-observer"}, greptimePipeline.MustSlice("receivers"))
				assert.Equal(t, []any{"otlphttp/greptimedb"}, greptimePipeline.MustSlice("exporters"))

				connectors := config.MustMap("connectors")
				assert.Equal(t, 1, int(connectors.MustMap("datavolume/trace-observer").MustFloat("aggregation_temporality")))

				exporter := config.MustMap("exporters").MustMap("otlphttp/greptimedb")
				assert.Equal(t, "http://${env:GREPTIME_HOST}:4000/v1/otlp", exporter.MustString("endpoint"))
				assert.Equal(t, "basicauth/client", exporter.MustMap("auth").MustString("authenticator"))
				assert.Equal(t, "${env:GREPTIME_DATABASE}", exporter.MustMap("headers").MustString("x-greptime-db-name"))
				assert.Equal(t, "service.name", exporter.MustMap("headers").MustString("x-greptime-otlp-metric-promote-resource-attrs"))
				assert.True(t, exporter.MustMap("tls")["insecure"].(bool))

				extension := config.MustMap("extensions").MustMap("basicauth/client").MustMap("client_auth")
				assert.Equal(t, "${env:GREPTIME_USER}", extension.MustString("username"))
				assert.Equal(t, "${env:GREPTIME_PASSWD}", extension.MustString("password"))
				assert.Contains(t, config.MustMap("service").MustSlice("extensions"), "basicauth/client")
			},
		},
		{
			desc: "greptimedb observers promote selected resource attributes",
			observers: []mdaiv1.Observer{
				{
					Name:                    "trace-observer",
					TelemetryType:           lo.ToPtr("traces"),
					LabelResourceAttributes: []string{"service.name", "team", "service.name"},
					AggregationTemporality:  pmetric.AggregationTemporalityDelta,
					MetricsBackend:          "greptimedb",
				},
				{
					Name:                    "log-observer",
					TelemetryType:           lo.ToPtr("logs"),
					LabelResourceAttributes: []string{"region"},
					AggregationTemporality:  pmetric.AggregationTemporalityDelta,
					MetricsBackend:          "greptimedb",
				},
			},
			check: func(t *testing.T, resultConfig string, err error) {
				t.Helper()
				require.NoError(t, err)

				var config builder.ConfigBlock
				require.NoError(t, yaml.Unmarshal([]byte(resultConfig), &config))

				headers := config.MustMap("exporters").MustMap("otlphttp/greptimedb").MustMap("headers")
				assert.Equal(t, "${env:GREPTIME_DATABASE}", headers.MustString("x-greptime-db-name"))
				assert.Equal(t, "service.name;team;region", headers.MustString("x-greptime-otlp-metric-promote-resource-attrs"))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			testObj := NewObserverAdapter(nil, logr.Discard(), nil, nil, nil)

			config, err := testObj.getObserverCollectorConfig(tc.observers, tc.observerResource)
			tc.check(t, config, err)
		})
	}
}
