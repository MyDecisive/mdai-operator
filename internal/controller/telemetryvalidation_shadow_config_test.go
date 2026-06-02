package controller

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

func TestDeriveShadowConfigInjectsFidelityProcessorAndMetadata(t *testing.T) {
	t.Parallel()

	cfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"endpoint": "0.0.0.0:8126"},
		}},
		Processors: &otelv1beta1.AnyConfig{Object: map[string]any{
			"batch": map[string]any{},
		}},
		Exporters: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"api": map[string]any{"key": "x"}},
		}},
		Service: otelv1beta1.Service{
			Pipelines: map[string]*otelv1beta1.Pipeline{
				"traces": {
					Receivers:  []string{"datadog"},
					Processors: []string{"batch"},
					Exporters:  []string{"datadog"},
				},
			},
			Telemetry: &otelv1beta1.AnyConfig{Object: map[string]any{
				"resource": map[string]any{
					"service.name": "gateway",
					"team":         "platform",
				},
			}},
		},
	}

	shadow := deriveShadowConfig(shadowConfigParams{
		Config:         cfg,
		Signals:        []hubv1.TelemetrySignal{hubv1.TelemetrySignalTraces},
		Namespace:      "mdai",
		ValidationName: "sample",
		CollectorName:  "gateway",
	})

	datadogReceiver, ok := shadow.Receivers.Object["datadog"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, datadogReceiver["include_metadata"])

	require.NotNil(t, shadow.Processors)
	correlationProcessor, ok := shadow.Processors.Object[correlationProcessorName].(map[string]any)
	require.True(t, ok)
	actions, ok := correlationProcessor["actions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)
	action, ok := actions[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "upsert", action["action"])
	assert.Equal(t, correlationAttributeKey, action["key"])
	assert.Equal(t, correlationHeaderFromCtxKey, action["from_context"])
	resourceCorrelationProcessor, ok := shadow.Processors.Object[correlationResourceProcessorName].(map[string]any)
	require.True(t, ok)
	resourceAttributes, ok := resourceCorrelationProcessor["attributes"].([]any)
	require.True(t, ok)
	require.Len(t, resourceAttributes, 1)
	resourceAction, ok := resourceAttributes[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "upsert", resourceAction["action"])
	assert.Equal(t, correlationAttributeKey, resourceAction["key"])
	assert.Equal(t, correlationHeaderFromCtxKey, resourceAction["from_context"])
	transformProcessor, ok := shadow.Processors.Object[correlationDDTagsProcessorName].(map[string]any)
	require.True(t, ok)
	traceStatements, ok := transformProcessor["trace_statements"].([]any)
	require.True(t, ok)
	require.Len(t, traceStatements, 1)
	traceStatement, ok := traceStatements[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "span", traceStatement["context"])
	statementList, ok := traceStatement["statements"].([]any)
	require.True(t, ok)
	require.Len(t, statementList, 2)
	assert.Equal(t, ddTagsSetStatement(), statementList[0])
	assert.Equal(t, ddTagsAppendStatement(), statementList[1])
	logStatements, ok := transformProcessor["log_statements"].([]any)
	require.True(t, ok)
	require.Len(t, logStatements, 1)
	logStatement, ok := logStatements[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "log", logStatement["context"])
	logStatementList, ok := logStatement["statements"].([]any)
	require.True(t, ok)
	require.Len(t, logStatementList, 2)
	assert.Equal(t, statementList[0], logStatementList[0])
	assert.Equal(t, statementList[1], logStatementList[1])
	metricsTransformProcessor, ok := shadow.Processors.Object[correlationMetricsProcessorName].(map[string]any)
	require.True(t, ok)
	metricStatements, ok := metricsTransformProcessor["metric_statements"].([]any)
	require.True(t, ok)
	require.Len(t, metricStatements, 1)
	metricStatement, ok := metricStatements[0].(map[string]any)
	require.True(t, ok)
	metricStatementList, ok := metricStatement["statements"].([]any)
	require.True(t, ok)
	require.Len(t, metricStatementList, 3)
	assert.Equal(t, metricCorrelationStatement(), metricStatementList[0])
	assert.Equal(t, deleteMetricDDTagsStatement, metricStatementList[1])
	assert.Equal(t, deleteMetricTagsStatement, metricStatementList[2])

	pipeline := shadow.Service.Pipelines["traces"]
	require.NotNil(t, pipeline)
	assert.Contains(t, pipeline.Processors, correlationProcessorName)
	assert.Contains(t, pipeline.Processors, correlationDDTagsProcessorName)

	exporterCfg, ok := shadow.Exporters.Object["datadog"].(map[string]any)
	require.True(t, ok)
	_, hasHostname := exporterCfg["hostname"]
	assert.False(t, hasHostname)
	hostMetadataCfg, ok := exporterCfg["host_metadata"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, false, hostMetadataCfg["enabled"])
	assert.Equal(t, "2s", exporterCfg["hostname_detection_timeout"])
	logsCfg, ok := exporterCfg["logs"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://mdai-fidelity-validator.mdai.svc.cluster.local:18081/observe/exporter/mdai/sample/gateway/datadog", logsCfg["endpoint"])

	require.NotNil(t, shadow.Service.Telemetry)
	resourceCfg, ok := shadow.Service.Telemetry.Object["resource"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "gateway-shadow", resourceCfg["service.name"])
	assert.Equal(t, "platform", resourceCfg["team"])
}

func TestDeriveShadowConfigDoesNotDuplicateFidelityProcessorInPipeline(t *testing.T) {
	t.Parallel()

	cfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"endpoint": "0.0.0.0:8126"},
		}},
		Processors: &otelv1beta1.AnyConfig{Object: map[string]any{}},
		Exporters: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{},
		}},
		Service: otelv1beta1.Service{Pipelines: map[string]*otelv1beta1.Pipeline{
			"logs": {
				Receivers:  []string{"datadog"},
				Processors: []string{correlationProcessorName, correlationDDTagsProcessorName},
				Exporters:  []string{"datadog"},
			},
		}},
	}

	shadow := deriveShadowConfig(shadowConfigParams{
		Config:         cfg,
		Signals:        []hubv1.TelemetrySignal{hubv1.TelemetrySignalLogs},
		Namespace:      "mdai",
		ValidationName: "sample",
		CollectorName:  "gateway",
	})

	pipeline := shadow.Service.Pipelines["logs"]
	require.NotNil(t, pipeline)
	count := 0
	ddtagsCount := 0
	for _, name := range pipeline.Processors {
		if name == correlationProcessorName {
			count++
		}
		if name == correlationDDTagsProcessorName {
			ddtagsCount++
		}
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, 1, ddtagsCount)
}

func TestDeriveShadowConfigUsesMetricsSpecificCorrelationProcessors(t *testing.T) {
	t.Parallel()

	cfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"endpoint": "0.0.0.0:8126"},
		}},
		Processors: &otelv1beta1.AnyConfig{Object: map[string]any{}},
		Exporters: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{},
		}},
		Service: otelv1beta1.Service{Pipelines: map[string]*otelv1beta1.Pipeline{
			"metrics": {
				Receivers:  []string{"datadog"},
				Processors: []string{"batch"},
				Exporters:  []string{"datadog"},
			},
		}},
	}

	shadow := deriveShadowConfig(shadowConfigParams{
		Config:         cfg,
		Signals:        []hubv1.TelemetrySignal{hubv1.TelemetrySignalMetrics},
		Namespace:      "mdai",
		ValidationName: "sample",
		CollectorName:  "gateway",
	})

	pipeline := shadow.Service.Pipelines["metrics"]
	require.NotNil(t, pipeline)
	assert.Contains(t, pipeline.Processors, correlationResourceProcessorName)
	assert.Contains(t, pipeline.Processors, correlationMetricsProcessorName)
	assert.NotContains(t, pipeline.Processors, correlationProcessorName)
	assert.NotContains(t, pipeline.Processors, correlationDDTagsProcessorName)
}

func TestDeriveShadowConfigTVRewriteOverridesDefaultByName(t *testing.T) {
	t.Parallel()

	cfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"endpoint": "0.0.0.0:8126"},
		}},
		Processors: &otelv1beta1.AnyConfig{Object: map[string]any{}},
		Exporters: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{
				"api": map[string]any{
					"site": "datadoghq.local",
				},
			},
		}},
		Service: otelv1beta1.Service{Pipelines: map[string]*otelv1beta1.Pipeline{
			"logs": {
				Receivers: []string{"datadog"},
				Exporters: []string{"datadog"},
			},
		}},
	}

	tvRules := []hubv1.TelemetryValidationExporterRewrite{
		{
			Name:                  "datadog-default",
			MatchExporterPrefixes: []string{"datadog"},
			Set: map[string]apiextensionsv1.JSON{
				"api.site": {Raw: []byte(`"dd.custom.local"`)},
			},
		},
	}

	shadow := deriveShadowConfig(shadowConfigParams{
		Config:               cfg,
		Signals:              []hubv1.TelemetrySignal{hubv1.TelemetrySignalLogs},
		Namespace:            "mdai",
		ValidationName:       "sample",
		CollectorName:        "gateway",
		ExporterRewriteRules: tvRules,
	})
	exporterCfg, ok := shadow.Exporters.Object["datadog"].(map[string]any)
	require.True(t, ok)
	_, hasHostname := exporterCfg["hostname"]
	assert.False(t, hasHostname)
	hostMetadataCfg, ok := exporterCfg["host_metadata"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, false, hostMetadataCfg["enabled"])
	assert.Equal(t, "2s", exporterCfg["hostname_detection_timeout"])
	apiCfg, ok := exporterCfg["api"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "dd.custom.local", apiCfg["site"])
	logsCfg, ok := exporterCfg["logs"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://mdai-fidelity-validator.mdai.svc.cluster.local:18081/observe/exporter/mdai/sample/gateway/datadog", logsCfg["endpoint"])
}

func TestDeriveShadowConfigAddsDebugExporterWhenEnabled(t *testing.T) {
	t.Parallel()

	cfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"endpoint": "0.0.0.0:8126"},
		}},
		Processors: &otelv1beta1.AnyConfig{Object: map[string]any{}},
		Exporters: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"api": map[string]any{"key": "x"}},
		}},
		Service: otelv1beta1.Service{Pipelines: map[string]*otelv1beta1.Pipeline{
			"logs": {
				Receivers:  []string{"datadog"},
				Processors: []string{"batch"},
				Exporters:  []string{"datadog"},
			},
		}},
	}

	shadow := deriveShadowConfig(shadowConfigParams{
		Config:                     cfg,
		Signals:                    []hubv1.TelemetrySignal{hubv1.TelemetrySignalLogs},
		Namespace:                  "mdai",
		ValidationName:             "sample",
		CollectorName:              "gateway",
		ShadowDebugExporterEnabled: true,
	})

	pipeline := shadow.Service.Pipelines["logs"]
	require.NotNil(t, pipeline)
	assert.Equal(t, []string{"datadog", "debug"}, pipeline.Exporters)

	debugCfg, ok := shadow.Exporters.Object["debug"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "detailed", debugCfg["verbosity"])
}

func TestDeriveShadowConfigDoesNotAddDebugToUnmatchedPipeline(t *testing.T) {
	t.Parallel()

	cfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"count/spans_by_facet": map[string]any{},
		}},
		Processors: &otelv1beta1.AnyConfig{Object: map[string]any{
			"deltatocumulative": map[string]any{},
		}},
		Exporters: otelv1beta1.AnyConfig{Object: map[string]any{
			"prometheus": map[string]any{"endpoint": "0.0.0.0:8899"},
		}},
		Service: otelv1beta1.Service{Pipelines: map[string]*otelv1beta1.Pipeline{
			"metrics": {
				Receivers:  []string{"count/spans_by_facet"},
				Processors: []string{"deltatocumulative"},
				Exporters:  []string{"prometheus"},
			},
		}},
	}

	shadow := deriveShadowConfig(shadowConfigParams{
		Config:                     cfg,
		Signals:                    []hubv1.TelemetrySignal{hubv1.TelemetrySignalMetrics},
		Namespace:                  "mdai",
		ValidationName:             "sample",
		CollectorName:              "gateway",
		ShadowDebugExporterEnabled: true,
	})

	assert.Nil(t, shadow.Service.Pipelines["metrics"], "debug not added to pipeline with no matching exporters")
}

func TestDeriveShadowConfigRewritesLoadBalancingExportersWithoutDatadogExporter(t *testing.T) {
	// Not parallel. Resets exporter rewrite cache.
	originalYAML := telemetryValidationExporterRewritesYAML
	t.Cleanup(func() {
		telemetryValidationExporterRewritesYAML = originalYAML
		exporterRewritesOnce = sync.Once{}
		cachedExporterRewrites = exporterRewriteConfig{}
	})
	exporterRewritesOnce = sync.Once{}
	cachedExporterRewrites = exporterRewriteConfig{}

	cfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{
				"endpoint":     "0.0.0.0:8126",
				"read_timeout": "60s",
			},
			"count/spans_by_facet": map[string]any{},
		}},
		Processors: &otelv1beta1.AnyConfig{Object: map[string]any{
			"deltatocumulative":       map[string]any{},
			"transform/add_dd_fields": map[string]any{"error_mode": "ignore"},
		}},
		Connectors: &otelv1beta1.AnyConfig{Object: map[string]any{
			"count/spans_by_facet": map[string]any{},
		}},
		Exporters: otelv1beta1.AnyConfig{Object: map[string]any{
			"loadbalancing/traces": map[string]any{
				"routing_key": "traceID",
				"resolver": map[string]any{
					"dns": map[string]any{
						"hostname": "sample-trace-sampling-collector.mdai.svc.cluster.local",
						"port":     "4317",
					},
				},
			},
			"loadbalancing/logs": map[string]any{
				"routing_key": "service",
				"resolver": map[string]any{
					"dns": map[string]any{
						"hostname": "sample-log-sampling-collector.mdai.svc.cluster.local",
						"port":     "4317",
					},
				},
			},
			"otlp_grpc/tracealyzer": map[string]any{
				"endpoint": "mdai-tracealyzer.mdai.svc.cluster.local:4317",
			},
			"prometheus": map[string]any{
				"endpoint": "0.0.0.0:8899",
			},
		}},
		Service: otelv1beta1.Service{
			Pipelines: map[string]*otelv1beta1.Pipeline{
				"logs": {
					Receivers:  []string{"datadog"},
					Processors: []string{"transform/add_dd_fields"},
					Exporters:  []string{"loadbalancing/logs"},
				},
				"traces": {
					Receivers:  []string{"datadog"},
					Processors: []string{"transform/add_dd_fields"},
					Exporters:  []string{"loadbalancing/traces", "count/spans_by_facet", "otlp_grpc/tracealyzer"},
				},
				"metrics": {
					Receivers:  []string{"count/spans_by_facet"},
					Processors: []string{"deltatocumulative"},
					Exporters:  []string{"prometheus"},
				},
			},
			Telemetry: &otelv1beta1.AnyConfig{Object: map[string]any{
				"resource": map[string]any{
					"mdai_connection": "sample",
					"service.name":    "sample-sampling-lb-collector",
				},
			}},
		},
	}

	shadow := deriveShadowConfig(shadowConfigParams{
		Config:         cfg,
		Signals:        []hubv1.TelemetrySignal{hubv1.TelemetrySignalLogs, hubv1.TelemetrySignalTraces, hubv1.TelemetrySignalMetrics},
		Namespace:      "mdai",
		ValidationName: "gateway-tv",
		CollectorName:  "sample-sampling-lb",
	})

	datadogReceiver, ok := shadow.Receivers.Object["datadog"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, datadogReceiver["include_metadata"])

	logsPipeline := shadow.Service.Pipelines["logs"]
	require.NotNil(t, logsPipeline)
	assert.Equal(t, []string{"datadog"}, logsPipeline.Exporters)
	assert.Contains(t, logsPipeline.Processors, correlationProcessorName)
	assert.Contains(t, logsPipeline.Processors, correlationDDTagsProcessorName)

	tracesPipeline := shadow.Service.Pipelines["traces"]
	require.NotNil(t, tracesPipeline)
	assert.Equal(t, []string{"datadog"}, tracesPipeline.Exporters)
	assert.NotContains(t, tracesPipeline.Exporters, "count/spans_by_facet")
	assert.NotContains(t, tracesPipeline.Exporters, "otlp_grpc/tracealyzer")
	assert.Contains(t, tracesPipeline.Processors, correlationProcessorName)
	assert.Contains(t, tracesPipeline.Processors, correlationDDTagsProcessorName)

	assert.Nil(t, shadow.Service.Pipelines["metrics"], "prometheus-only metrics pipeline should be removed by default")

	require.Len(t, shadow.Exporters.Object, 1)
	exporterCfg, ok := shadow.Exporters.Object["datadog"].(map[string]any)
	require.True(t, ok)
	apiCfg, ok := exporterCfg["api"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "validator.invalid", apiCfg["site"])
	metricsCfg, ok := exporterCfg["metrics"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://mdai-fidelity-validator.mdai.svc.cluster.local:18081/observe/exporter/mdai/gateway-tv/sample-sampling-lb/loadbalancing/traces", metricsCfg["endpoint"])
	logsCfg, ok := exporterCfg["logs"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://mdai-fidelity-validator.mdai.svc.cluster.local:18081/observe/exporter/mdai/gateway-tv/sample-sampling-lb/loadbalancing/logs", logsCfg["endpoint"])
	tracesCfg, ok := exporterCfg["traces"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://mdai-fidelity-validator.mdai.svc.cluster.local:18081/observe/exporter/mdai/gateway-tv/sample-sampling-lb/loadbalancing/traces", tracesCfg["endpoint"])

	resourceCfg, ok := shadow.Service.Telemetry.Object["resource"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "sample-sampling-lb-shadow", resourceCfg["service.name"])
	assert.Equal(t, "sample", resourceCfg["mdai_connection"])
}

func TestRewriteExporterConfig_ReplacesExporter(t *testing.T) {
	t.Parallel()

	originalCfg := map[string]any{
		"endpoint": "old:4317",
	}

	rules := []exporterRewriteRule{
		{
			MatchExporterPrefixes:  []string{"otlp/old"},
			ReplaceWithExporterKey: "otlphttp/new",
			ReplaceWithExporterValue: map[string]any{
				"endpoint": "new:4318",
			},
			Set: map[string]any{
				"headers.test": "value-{{ namespace }}",
			},
		},
	}

	vars := map[string]string{
		"namespace": "mdai-test",
	}

	newName, newCfg := rewriteExporterConfig("otlp/old", originalCfg, rules, vars)

	assert.Equal(t, "otlphttp/new", newName)

	cfgMap, ok := newCfg.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "new:4318", cfgMap["endpoint"])

	// Ensure `Set` applies against the newly replaced config object
	headers, ok := cfgMap["headers"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "value-mdai-test", headers["test"])
}

func TestDeriveShadowConfig_ReplacesExportersInPipelines(t *testing.T) {
	// Not parallel. Modifies global variables.
	originalYAML := telemetryValidationExporterRewritesYAML
	t.Cleanup(func() {
		telemetryValidationExporterRewritesYAML = originalYAML
		exporterRewritesOnce = sync.Once{}
		cachedExporterRewrites = exporterRewriteConfig{}
	})

	telemetryValidationExporterRewritesYAML = `
rules:
  - name: replace-otlp
    match_exporter_prefixes: ["otlp/original"]
    replace_with_exporter_key: "otlphttp/replaced"
    replace_with_exporter_value:
      endpoint: "http://replaced:4318"
`
	exporterRewritesOnce = sync.Once{}
	cachedExporterRewrites = exporterRewriteConfig{}

	cfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"endpoint": "0.0.0.0:8126"},
		}},
		Exporters: otelv1beta1.AnyConfig{Object: map[string]any{
			"otlp/original": map[string]any{"endpoint": "old:4317"},
			"debug":         map[string]any{},
		}},
		Service: otelv1beta1.Service{Pipelines: map[string]*otelv1beta1.Pipeline{
			"traces": {
				Receivers: []string{"datadog"},
				Exporters: []string{"otlp/original", "debug"},
			},
		}},
	}

	shadow := deriveShadowConfig(shadowConfigParams{
		Config:                     cfg,
		Signals:                    []hubv1.TelemetrySignal{hubv1.TelemetrySignalTraces},
		Namespace:                  "mdai",
		ValidationName:             "sample",
		CollectorName:              "gateway",
		ShadowDebugExporterEnabled: true,
	})

	// Assert the exporter map correctly swapped keys
	_, hasOld := shadow.Exporters.Object["otlp/original"]
	assert.False(t, hasOld, "old exporter config block should be replaced")

	replacedExporter, hasNew := shadow.Exporters.Object["otlphttp/replaced"]
	require.True(t, hasNew, "new exporter config block should be present")

	replacedMap, ok := replacedExporter.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://replaced:4318", replacedMap["endpoint"])

	// Assert the pipeline strings swapped perfectly without injecting ""
	pipeline := shadow.Service.Pipelines["traces"]
	require.NotNil(t, pipeline)

	assert.Contains(t, pipeline.Exporters, "otlphttp/replaced")
	assert.NotContains(t, pipeline.Exporters, "otlp/original")
	assert.NotContains(t, pipeline.Exporters, "")
	assert.Len(t, pipeline.Exporters, 2)
}

func TestDeriveShadowConfig_CollapsesMultipleMatchingExporters(t *testing.T) {
	// Not parallel. Modifies global variables.
	originalYAML := telemetryValidationExporterRewritesYAML
	t.Cleanup(func() {
		telemetryValidationExporterRewritesYAML = originalYAML
		exporterRewritesOnce = sync.Once{}
		cachedExporterRewrites = exporterRewriteConfig{}
	})

	telemetryValidationExporterRewritesYAML = `
rules:
  - name: collapse-otlp
    match_exporter_prefixes: ["otlp/"]
    replace_with_exporter_key: "otlphttp/collapsed"
    replace_with_exporter_value:
      endpoint: "http://collapsed:4318"
`

	baseCfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"endpoint": "0.0.0.0:8126"},
		}},
		Exporters: otelv1beta1.AnyConfig{Object: map[string]any{
			"otlp/1":  map[string]any{"endpoint": "old1:4317"},
			"otlp/2":  map[string]any{"endpoint": "old2:4317"},
			"datadog": map[string]any{"api": map[string]any{"key": "x"}},
		}},
		Service: otelv1beta1.Service{Pipelines: map[string]*otelv1beta1.Pipeline{
			"traces": {
				Receivers: []string{"datadog"},
				Exporters: []string{"otlp/1", "otlp/2", "datadog"},
			},
			"logs": {
				Receivers: []string{"datadog"},
				Exporters: []string{"otlp/2"},
			},
		}},
	}

	t.Run("KeepUnmatchedExporters = false (strips unmatched)", func(t *testing.T) {
		exporterRewritesOnce = sync.Once{}
		cachedExporterRewrites = exporterRewriteConfig{}

		shadow := deriveShadowConfig(shadowConfigParams{
			Config:                 baseCfg,
			Signals:                []hubv1.TelemetrySignal{hubv1.TelemetrySignalTraces, hubv1.TelemetrySignalLogs},
			Namespace:              "mdai",
			ValidationName:         "sample",
			CollectorName:          "gateway",
			KeepUnmatchedExporters: false,
		})

		_, hasDatadog := shadow.Exporters.Object["datadog"]
		require.False(t, hasDatadog, "unmatched exporter must be stripped")

		tracesPipeline := shadow.Service.Pipelines["traces"]
		require.NotNil(t, tracesPipeline)
		assert.Contains(t, tracesPipeline.Exporters, "otlphttp/collapsed")
		assert.NotContains(t, tracesPipeline.Exporters, "datadog")
		assert.Len(t, tracesPipeline.Exporters, 1)
	})

	t.Run("KeepUnmatchedExporters = true (retains unmatched)", func(t *testing.T) {
		exporterRewritesOnce = sync.Once{}
		cachedExporterRewrites = exporterRewriteConfig{}

		shadow := deriveShadowConfig(shadowConfigParams{
			Config:                 baseCfg,
			Signals:                []hubv1.TelemetrySignal{hubv1.TelemetrySignalTraces, hubv1.TelemetrySignalLogs},
			Namespace:              "mdai",
			ValidationName:         "sample",
			CollectorName:          "gateway",
			KeepUnmatchedExporters: true,
		})

		_, hasOld1 := shadow.Exporters.Object["otlp/1"]
		assert.False(t, hasOld1, "otlp/1 should be replaced")

		_, hasNew := shadow.Exporters.Object["otlphttp/collapsed"]
		require.True(t, hasNew, "collapsed exporter must exist")

		_, hasDatadog := shadow.Exporters.Object["datadog"]
		require.True(t, hasDatadog, "unmatched exporter must remain in the shadow config")

		tracesPipeline := shadow.Service.Pipelines["traces"]
		require.NotNil(t, tracesPipeline)
		assert.Contains(t, tracesPipeline.Exporters, "otlphttp/collapsed")
		assert.Contains(t, tracesPipeline.Exporters, "datadog")
		assert.Len(t, tracesPipeline.Exporters, 2, "Should deduplicate collapsed exporters and retain datadog")
	})
}
