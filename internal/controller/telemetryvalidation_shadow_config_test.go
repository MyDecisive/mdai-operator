package controller

import (
	"fmt"
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
	statementList, ok := traceStatement["statements"].([]any)
	require.True(t, ok)
	require.Len(t, statementList, 2)
	assert.Equal(t, fmt.Sprintf(setDDTagsOnlyStatement, correlationDDTagKey, correlationAttributeKey, correlationAttributeKey), statementList[0])
	assert.Equal(t, fmt.Sprintf(appendDDTagsStatement, correlationDDTagKey, correlationAttributeKey, correlationAttributeKey), statementList[1])
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
	assert.Equal(t, fmt.Sprintf(setMetricCorrelationStatement, correlationAttributeKey, correlationAttributeKey, correlationAttributeKey, correlationAttributeKey), metricStatementList[0])
	assert.Equal(t, deleteMetricDDTagsStatement, metricStatementList[1])
	assert.Equal(t, deleteMetricTagsStatement, metricStatementList[2])

	pipeline := shadow.Service.Pipelines["traces"]
	require.NotNil(t, pipeline)
	assert.Contains(t, pipeline.Processors, correlationProcessorName)
	assert.Contains(t, pipeline.Processors, correlationDDTagsProcessorName)

	exporterCfg, ok := shadow.Exporters.Object["datadog"].(map[string]any)
	require.True(t, ok)
	logsCfg, ok := exporterCfg["logs"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://mdai-fidelity-validator.mdai.svc.cluster.local:18081/intake/exporter/mdai/sample/gateway/datadog", logsCfg["endpoint"])

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
	apiCfg, ok := exporterCfg["api"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "dd.custom.local", apiCfg["site"])
	logsCfg, ok := exporterCfg["logs"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://mdai-fidelity-validator.mdai.svc.cluster.local:18081/intake/exporter/mdai/sample/gateway/datadog", logsCfg["endpoint"])
}

func TestDeriveShadowConfigAddsDebugExporterWhenEnabled(t *testing.T) {
	t.Parallel()

	cfg := otelv1beta1.Config{
		Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
			"datadog": map[string]any{"endpoint": "0.0.0.0:8126"},
		}},
		Processors: &otelv1beta1.AnyConfig{Object: map[string]any{}},
		Exporters:  otelv1beta1.AnyConfig{Object: map[string]any{}},
		Service: otelv1beta1.Service{Pipelines: map[string]*otelv1beta1.Pipeline{
			"logs": {
				Receivers:  []string{"datadog"},
				Processors: []string{"batch"},
				Exporters:  []string{"unused"},
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
	assert.Equal(t, []string{"debug"}, pipeline.Exporters)

	debugCfg, ok := shadow.Exporters.Object["debug"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "detailed", debugCfg["verbosity"])
}
