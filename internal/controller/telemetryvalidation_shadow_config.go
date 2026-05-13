package controller

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

const (
	correlationProcessorName         = "attributes/correlation_id"
	correlationResourceProcessorName = "resource/correlation_id"
	correlationDDTagsProcessorName   = "transform/correlation_ddtags"
	correlationMetricsProcessorName  = "transform/metrics_correlation_id"
	correlationAttributeKey          = "correlation_id"
	correlationHeaderFromCtxKey      = "metadata.x-correlation-id"
	correlationDDTagKey              = "correlation_id:"
	deleteMetricDDTagsStatement      = `delete_key(attributes, "ddtags") where attributes["ddtags"] != nil`
	deleteMetricTagsStatement        = `delete_key(attributes, "tags") where attributes["tags"] != nil`
)

func ddTagsSetStatement() string {
	return fmt.Sprintf(
		`set(attributes["ddtags"], Concat([%q, attributes[%q]], "")) where attributes[%q] != nil and attributes["ddtags"] == nil`,
		correlationDDTagKey, correlationAttributeKey, correlationAttributeKey,
	)
}

func ddTagsAppendStatement() string {
	return fmt.Sprintf(
		`set(attributes["ddtags"], Concat([attributes["ddtags"], ",", %q, attributes[%q]], "")) where attributes[%q] != nil and attributes["ddtags"] != nil`,
		correlationDDTagKey, correlationAttributeKey, correlationAttributeKey,
	)
}

func metricCorrelationStatement() string {
	return fmt.Sprintf(
		`set(attributes["%[1]s"], resource.attributes["%[1]s"]) where attributes["%[1]s"] == nil and resource.attributes["%[1]s"] != nil`,
		correlationAttributeKey,
	)
}

type shadowConfigParams struct {
	Config                     otelv1beta1.Config
	Signals                    []hubv1.TelemetrySignal
	ValidatorEndpoint          string
	Namespace                  string
	ValidationName             string
	CollectorName              string
	ExporterRewriteRules       []hubv1.TelemetryValidationExporterRewrite
	ShadowDebugExporterEnabled bool
	KeepUnmatchedExporters     bool
}

func deriveShadowConfig(params shadowConfigParams) otelv1beta1.Config {
	shadow := *params.Config.DeepCopy()
	ensureDatadogReceiversIncludeMetadata(&shadow)
	ensureCorrelationProcessors(&shadow)
	rewriteShadowTelemetryServiceName(&shadow, shadowCollectorName(params.CollectorName))
	rewriteRules := mergedExporterRewriteRules(params.ExporterRewriteRules)

	enabledSignals := make(map[hubv1.TelemetrySignal]struct{}, len(params.Signals))
	for _, signal := range params.Signals {
		enabledSignals[signal] = struct{}{}
	}

	filteredPipelines := make(map[string]*otelv1beta1.Pipeline)
	exporterSignals := make(map[string]map[hubv1.TelemetrySignal]struct{})
	for name, pipeline := range shadow.Service.Pipelines {
		signal, ok := pipelineSignal(name)
		if !ok {
			continue
		}
		if _, ok := enabledSignals[signal]; !ok {
			continue
		}

		var targetExporters []string
		if params.KeepUnmatchedExporters {
			targetExporters = append([]string(nil), pipeline.Exporters...)
		} else {
			targetExporters = exportersMatchingRewriteRules(pipeline.Exporters, rewriteRules)
		}

		if params.ShadowDebugExporterEnabled {
			targetExporters = appendExporterOnce(targetExporters, "debug")
		}
		if len(targetExporters) == 0 {
			continue
		}

		filtered := *pipeline
		filtered.Exporters = targetExporters
		switch signal {
		case hubv1.TelemetrySignalMetrics:
			filtered.Processors = appendProcessorOnce(filtered.Processors, correlationResourceProcessorName)
			filtered.Processors = appendProcessorOnce(filtered.Processors, correlationMetricsProcessorName)
		default:
			filtered.Processors = appendProcessorOnce(filtered.Processors, correlationProcessorName)
			filtered.Processors = appendProcessorOnce(filtered.Processors, correlationDDTagsProcessorName)
		}
		filteredPipelines[name] = &filtered
		for _, exporterName := range targetExporters {
			signals, ok := exporterSignals[exporterName]
			if !ok {
				signals = make(map[hubv1.TelemetrySignal]struct{})
				exporterSignals[exporterName] = signals
			}
			signals[signal] = struct{}{}
		}
	}
	shadow.Service.Pipelines = filteredPipelines

	exporters := make(map[string]any)
	validatorBase := validatorExportBaseURL(params.ValidatorEndpoint)
	templateVars := map[string]string{
		"validator_endpoint":   validatorBase,
		"namespace":            params.Namespace,
		"telemetry_validation": params.ValidationName,
		"collector":            params.CollectorName,
	}

	exporterNameMap := make(map[string]string)
	for _, exporterName := range slices.Sorted(maps.Keys(exporterSignals)) {
		if exporterName == "debug" {
			exporters[exporterName] = debugExporterConfig(shadow.Exporters.Object["debug"])
			exporterNameMap[exporterName] = exporterName
			continue
		}

		cfgExporter, ok := shadow.Exporters.Object[exporterName]
		if !ok {
			continue
		}

		perExporterVars := map[string]string{}
		maps.Copy(perExporterVars, templateVars)
		perExporterVars["exporter"] = exporterName

		newName, newExporter := rewriteExporterConfig(exporterName, cfgExporter, rewriteRules, perExporterVars)
		// Empty newName means the rule deleted this exporter.
		if newName == "" {
			continue
		}
		if rewriteLimitsToReferencedSignals(exporterName, rewriteRules) {
			newExporter = limitExporterConfigToSignals(newExporter, exporterSignals[exporterName])
		}

		if existing, ok := exporters[newName]; ok {
			exporters[newName] = mergeExporterConfig(existing, newExporter)
		} else {
			exporters[newName] = newExporter
		}
		exporterNameMap[exporterName] = newName
	}
	shadow.Exporters.Object = exporters

	for pipelineName, pipeline := range shadow.Service.Pipelines {
		newExporters := make([]string, 0, len(pipeline.Exporters))
		seen := make(map[string]bool)

		for _, oldExporter := range pipeline.Exporters {
			newExporterName, ok := exporterNameMap[oldExporter]
			if !ok {
				// If it wasn't in referencedExporters or got deleted (newName == ""), we skip it
				continue
			}

			if !seen[newExporterName] {
				seen[newExporterName] = true
				newExporters = append(newExporters, newExporterName)
			}
		}
		pipeline.Exporters = newExporters
		shadow.Service.Pipelines[pipelineName] = pipeline
	}

	return shadow
}

func rewriteLimitsToReferencedSignals(exporterName string, rules []exporterRewriteRule) bool {
	return slices.ContainsFunc(matchingRewriteRules(exporterName, rules), func(r exporterRewriteRule) bool {
		return r.LimitToReferencedSignals
	})
}

func limitExporterConfigToSignals(raw any, signals map[hubv1.TelemetrySignal]struct{}) any {
	cfg, ok := raw.(map[string]any)
	if !ok || len(signals) == 0 {
		return raw
	}

	limited := runtime.DeepCopyJSON(cfg)
	for _, signal := range []hubv1.TelemetrySignal{
		hubv1.TelemetrySignalMetrics,
		hubv1.TelemetrySignalLogs,
		hubv1.TelemetrySignalTraces,
	} {
		if _, ok := signals[signal]; !ok {
			delete(limited, string(signal))
		}
	}
	return limited
}

func mergeExporterConfig(existing, incoming any) any {
	existingMap, _ := existing.(map[string]any)
	incomingMap, _ := incoming.(map[string]any)
	if existingMap == nil || incomingMap == nil {
		return runtime.DeepCopyJSONValue(incoming)
	}

	merged := runtime.DeepCopyJSON(existingMap)
	for key, incomingValue := range incomingMap {
		existingNested, _ := merged[key].(map[string]any)
		incomingNested, _ := incomingValue.(map[string]any)
		if existingNested != nil && incomingNested != nil {
			merged[key] = mergeExporterConfig(existingNested, incomingNested)
			continue
		}
		merged[key] = runtime.DeepCopyJSONValue(incomingValue)
	}
	return merged
}

func debugExporterConfig(existing any) any {
	if cfg, ok := existing.(map[string]any); ok {
		return cfg
	}
	return map[string]any{
		"verbosity": "detailed",
	}
}

func rewriteShadowTelemetryServiceName(cfg *otelv1beta1.Config, shadowName string) {
	if cfg.Service.Telemetry == nil || cfg.Service.Telemetry.Object == nil {
		return
	}

	resourceCfg, ok := cfg.Service.Telemetry.Object["resource"].(map[string]any)
	if !ok {
		return
	}
	if _, ok := resourceCfg["service.name"].(string); !ok {
		return
	}
	resourceCfg["service.name"] = shadowName
	cfg.Service.Telemetry.Object["resource"] = resourceCfg
}

func ensureDatadogReceiversIncludeMetadata(cfg *otelv1beta1.Config) {
	if cfg.Receivers.Object == nil {
		return
	}
	for receiverName, raw := range cfg.Receivers.Object {
		if !strings.HasPrefix(receiverName, "datadog") {
			continue
		}
		receiverCfg, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		receiverCfg["include_metadata"] = true
		cfg.Receivers.Object[receiverName] = receiverCfg
	}
}

func ensureCorrelationProcessors(cfg *otelv1beta1.Config) {
	if cfg.Processors == nil {
		cfg.Processors = &otelv1beta1.AnyConfig{Object: map[string]any{}}
	}
	if cfg.Processors.Object == nil {
		cfg.Processors.Object = make(map[string]any)
	}
	cfg.Processors.Object[correlationProcessorName] = map[string]any{
		"actions": []any{
			map[string]any{
				"key":          correlationAttributeKey,
				"action":       "upsert",
				"from_context": correlationHeaderFromCtxKey,
			},
		},
	}
	cfg.Processors.Object[correlationResourceProcessorName] = map[string]any{
		"attributes": []any{
			map[string]any{
				"key":          correlationAttributeKey,
				"action":       "upsert",
				"from_context": correlationHeaderFromCtxKey,
			},
		},
	}
	cfg.Processors.Object[correlationDDTagsProcessorName] = map[string]any{
		"trace_statements": []any{
			map[string]any{
				"context":    "span",
				"statements": []any{ddTagsSetStatement(), ddTagsAppendStatement()},
			},
		},
		"log_statements": []any{
			map[string]any{
				"context":    "log",
				"statements": []any{ddTagsSetStatement(), ddTagsAppendStatement()},
			},
		},
	}
	cfg.Processors.Object[correlationMetricsProcessorName] = map[string]any{
		"metric_statements": []any{
			map[string]any{
				"context":    "datapoint",
				"statements": []any{metricCorrelationStatement(), deleteMetricDDTagsStatement, deleteMetricTagsStatement},
			},
		},
	}
}

func appendProcessorOnce(processors []string, processorName string) []string {
	if slices.Contains(processors, processorName) {
		return processors
	}
	return append(processors, processorName)
}

func appendExporterOnce(exporters []string, exporterName string) []string {
	if slices.Contains(exporters, exporterName) {
		return exporters
	}
	return append(exporters, exporterName)
}

func validatorExportBaseURL(validatorEndpoint string) string {
	if strings.TrimSpace(validatorEndpoint) != "" {
		return strings.TrimSuffix(validatorEndpoint, "/")
	}

	return "http://mdai-fidelity-validator.mdai.svc.cluster.local:18081" //nolint:revive
}

func pipelineSignal(name string) (hubv1.TelemetrySignal, bool) {
	base := strings.SplitN(name, "/", 2)[0] //nolint:mnd
	switch base {
	case string(hubv1.TelemetrySignalMetrics):
		return hubv1.TelemetrySignalMetrics, true
	case string(hubv1.TelemetrySignalLogs):
		return hubv1.TelemetrySignalLogs, true
	case string(hubv1.TelemetrySignalTraces):
		return hubv1.TelemetrySignalTraces, true
	default:
		return "", false
	}
}
