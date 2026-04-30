package controller

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"sigs.k8s.io/yaml"

	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

//go:embed config/telemetryvalidation_exporter_rewrites.yaml
var telemetryValidationExporterRewritesYAML string

//go:embed config/telemetryvalidation_validator_rules.yaml
var telemetryValidationValidatorRulesDefaultYAML string

//go:embed config/telemetryvalidation_validator_field_mapping.yaml
var telemetryValidationValidatorFieldMappingDefaultYAML string

var (
	exporterRewritesOnce   sync.Once
	cachedExporterRewrites exporterRewriteConfig
)

type exporterRewriteConfig struct {
	Rules []exporterRewriteRule `yaml:"rules"`
}

type exporterRewriteRule struct {
	Name                  string            `yaml:"name"`
	MatchExporterPrefixes []string          `yaml:"match_exporter_prefixes"`
	Set                   map[string]any    `yaml:"set"`
	ReplaceStrings        map[string]string `yaml:"replace_strings"`
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
	referencedExporters := make(map[string]struct{})
	for name, pipeline := range shadow.Service.Pipelines {
		signal, ok := pipelineSignal(name)
		if !ok {
			continue
		}
		if _, ok := enabledSignals[signal]; !ok {
			continue
		}

		targetExporters := exportersMatchingRewriteRules(pipeline.Exporters, rewriteRules)
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
			referencedExporters[exporterName] = struct{}{}
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
	for exporterName := range referencedExporters {
		if exporterName == "debug" {
			exporters[exporterName] = debugExporterConfig(shadow.Exporters.Object["debug"])
			continue
		}
		if cfgExporter, ok := shadow.Exporters.Object[exporterName]; ok {
			perExporterVars := map[string]string{}
			maps.Copy(perExporterVars, templateVars)
			perExporterVars["exporter"] = exporterName
			exporters[exporterName] = rewriteExporterConfig(exporterName, cfgExporter, rewriteRules, perExporterVars)
		}
	}
	shadow.Exporters.Object = exporters

	return shadow
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

	setDDTagsStatement := fmt.Sprintf(setDDTagsOnlyStatement, correlationDDTagKey, correlationAttributeKey, correlationAttributeKey)
	appendToDDTagsStatement := fmt.Sprintf(appendDDTagsStatement, correlationDDTagKey, correlationAttributeKey, correlationAttributeKey)
	cfg.Processors.Object[correlationDDTagsProcessorName] = map[string]any{
		"trace_statements": []any{
			map[string]any{
				"context":    "span",
				"statements": []any{setDDTagsStatement, appendToDDTagsStatement},
			},
		},
		"log_statements": []any{
			map[string]any{
				"context":    "log",
				"statements": []any{setDDTagsStatement, appendToDDTagsStatement},
			},
		},
	}
	setMetricCorrelation := fmt.Sprintf(
		setMetricCorrelationStatement,
		correlationAttributeKey,
		correlationAttributeKey,
		correlationAttributeKey,
		correlationAttributeKey,
	)
	cfg.Processors.Object[correlationMetricsProcessorName] = map[string]any{
		"metric_statements": []any{
			map[string]any{
				"context":    "datapoint",
				"statements": []any{setMetricCorrelation, deleteMetricDDTagsStatement, deleteMetricTagsStatement},
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

func rewriteExporterConfig(exporterName string, raw any, rules []exporterRewriteRule, templateVars map[string]string) any {
	cfg, ok := raw.(map[string]any)
	if !ok {
		return raw
	}

	for _, rule := range matchingRewriteRules(exporterName, rules) {
		for path, value := range rule.Set {
			setNestedValue(cfg, path, resolveTemplateValues(value, templateVars))
		}
		if len(rule.ReplaceStrings) > 0 {
			applyStringReplacementsRecursive(cfg, rule.ReplaceStrings, templateVars)
		}
	}

	return cfg
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

func exportersMatchingRewriteRules(exporters []string, rules []exporterRewriteRule) []string {
	filtered := make([]string, 0, len(exporters))
	for _, exporterName := range exporters {
		if len(matchingRewriteRules(exporterName, rules)) > 0 {
			filtered = append(filtered, exporterName)
		}
	}
	return filtered
}

func matchingRewriteRules(exporterName string, rules []exporterRewriteRule) []exporterRewriteRule {
	matched := make([]exporterRewriteRule, 0)
	for _, rule := range rules {
		for _, prefix := range rule.MatchExporterPrefixes {
			if strings.HasPrefix(exporterName, prefix) {
				matched = append(matched, rule)
				break
			}
		}
	}
	return matched
}

func setNestedValue(root map[string]any, dottedPath string, value any) {
	parts := strings.Split(dottedPath, ".")
	if len(parts) == 0 {
		return
	}

	current := root
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[part] = next
		}
		current = next
	}
	current[parts[len(parts)-1]] = value
}

func resolveTemplateValues(value any, vars map[string]string) any {
	switch v := value.(type) {
	case string:
		resolved := v
		for key, replacement := range vars {
			resolved = strings.ReplaceAll(resolved, "{{ "+key+" }}", replacement)
		}
		return resolved
	case map[string]any:
		result := make(map[string]any, len(v))
		for key, nested := range v {
			result[key] = resolveTemplateValues(nested, vars)
		}
		return result
	case []any:
		for i := range v {
			v[i] = resolveTemplateValues(v[i], vars)
		}
		return v
	default:
		return value
	}
}

func applyStringReplacementsRecursive(node any, replacements map[string]string, vars map[string]string) {
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			switch castValue := value.(type) {
			case string:
				updated := castValue
				for old, newValue := range replacements {
					updated = strings.ReplaceAll(
						updated,
						resolveTemplateString(old, vars),
						resolveTemplateString(newValue, vars),
					)
				}
				typed[key] = updated
			default:
				applyStringReplacementsRecursive(castValue, replacements, vars)
			}
		}
	case []any:
		for _, value := range typed {
			applyStringReplacementsRecursive(value, replacements, vars)
		}
	default:
		return
	}
}

func resolveTemplateString(value string, vars map[string]string) string {
	resolved := value
	for key, replacement := range vars {
		resolved = strings.ReplaceAll(resolved, "{{ "+key+" }}", replacement)
	}
	return resolved
}

func getExporterRewriteConfig() exporterRewriteConfig {
	exporterRewritesOnce.Do(func() {
		cachedExporterRewrites = defaultExporterRewriteConfig()
		loaded := exporterRewriteConfig{}
		if err := yaml.Unmarshal([]byte(telemetryValidationExporterRewritesYAML), &loaded); err == nil && len(loaded.Rules) > 0 {
			for _, rule := range loaded.Rules {
				if len(rule.MatchExporterPrefixes) == 0 {
					continue
				}
				cachedExporterRewrites.Rules = append(cachedExporterRewrites.Rules, rule)
			}
		}
	})
	return cachedExporterRewrites
}

func mergedExporterRewriteRules(tvRules []hubv1.TelemetryValidationExporterRewrite) []exporterRewriteRule {
	defaults := getExporterRewriteConfig().Rules
	if len(tvRules) == 0 {
		return defaults
	}

	defaultByName := make(map[string]exporterRewriteRule, len(defaults))
	defaultOrder := make([]string, 0, len(defaults))
	for _, rule := range defaults {
		if strings.TrimSpace(rule.Name) != "" {
			defaultByName[rule.Name] = rule
			defaultOrder = append(defaultOrder, rule.Name)
		}
	}

	namedOverrides := make(map[string]exporterRewriteRule)
	namedOrder := make([]string, 0)
	unnamed := make([]exporterRewriteRule, 0)

	for _, tvRule := range tvRules {
		converted := exporterRewriteRule{
			Name:                  tvRule.Name,
			MatchExporterPrefixes: append([]string(nil), tvRule.MatchExporterPrefixes...),
			Set:                   mapStringInterface(tvRule.Set),
			ReplaceStrings:        mapStringString(tvRule.ReplaceStrings),
		}
		if len(converted.MatchExporterPrefixes) == 0 {
			continue
		}
		if strings.TrimSpace(converted.Name) == "" {
			unnamed = append(unnamed, converted)
			continue
		}

		if existingDefault, ok := defaultByName[converted.Name]; ok {
			converted = mergeExporterRewriteRule(existingDefault, converted)
		}
		if _, exists := namedOverrides[converted.Name]; !exists {
			namedOrder = append(namedOrder, converted.Name)
		}
		namedOverrides[converted.Name] = converted
	}

	merged := make([]exporterRewriteRule, 0, len(defaults)+len(namedOverrides)+len(unnamed))
	for _, name := range defaultOrder {
		if override, ok := namedOverrides[name]; ok {
			merged = append(merged, override)
			delete(namedOverrides, name)
			continue
		}
		merged = append(merged, defaultByName[name])
	}
	for _, name := range namedOrder {
		if override, ok := namedOverrides[name]; ok {
			merged = append(merged, override)
		}
	}
	merged = append(merged, unnamed...)
	return merged
}

func mergeExporterRewriteRule(base exporterRewriteRule, override exporterRewriteRule) exporterRewriteRule {
	merged := base
	if len(override.MatchExporterPrefixes) > 0 {
		merged.MatchExporterPrefixes = append([]string(nil), override.MatchExporterPrefixes...)
	}
	if override.Name != "" {
		merged.Name = override.Name
	}
	if len(base.Set) > 0 || len(override.Set) > 0 {
		merged.Set = maps.Clone(base.Set)
		if merged.Set == nil {
			merged.Set = map[string]any{}
		}
		maps.Copy(merged.Set, override.Set)
	}
	if len(base.ReplaceStrings) > 0 || len(override.ReplaceStrings) > 0 {
		merged.ReplaceStrings = maps.Clone(base.ReplaceStrings)
		if merged.ReplaceStrings == nil {
			merged.ReplaceStrings = map[string]string{}
		}
		maps.Copy(merged.ReplaceStrings, override.ReplaceStrings)
	}
	return merged
}

func mapStringInterface(in map[string]apiextensionsv1.JSON) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		if len(value.Raw) == 0 {
			out[key] = nil
			continue
		}
		var decoded any
		if err := json.Unmarshal(value.Raw, &decoded); err != nil {
			out[key] = string(value.Raw)
			continue
		}
		out[key] = decoded
	}
	return out
}

func mapStringString(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}

func defaultExporterRewriteConfig() exporterRewriteConfig {
	return exporterRewriteConfig{
		Rules: []exporterRewriteRule{
			{
				Name:                  "datadog-default",
				MatchExporterPrefixes: []string{"datadog"},
				Set: map[string]any{
					"api.fail_on_invalid_key":    false,
					"host_metadata.enabled":      false,
					"hostname_detection_timeout": "2s",
					"metrics.endpoint":           "{{ validator_endpoint }}/observe/exporter/{{ namespace }}/{{ telemetry_validation }}/{{ collector }}/{{ exporter }}",
					"logs.endpoint":              "{{ validator_endpoint }}/observe/exporter/{{ namespace }}/{{ telemetry_validation }}/{{ collector }}/{{ exporter }}",
					"traces.endpoint":            "{{ validator_endpoint }}/observe/exporter/{{ namespace }}/{{ telemetry_validation }}/{{ collector }}/{{ exporter }}",
				},
			},
		},
	}
}
