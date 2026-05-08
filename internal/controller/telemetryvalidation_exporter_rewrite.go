package controller

import (
	_ "embed"
	"encoding/json"
	"maps"
	"strings"
	"sync"

	"sigs.k8s.io/yaml"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

//go:embed config/telemetryvalidation_exporter_rewrites.yaml
var telemetryValidationExporterRewritesYAML string

var (
	exporterRewritesOnce   sync.Once
	cachedExporterRewrites exporterRewriteConfig
)

type exporterRewriteConfig struct {
	Rules []exporterRewriteRule `json:"rules"`
}

type exporterRewriteRule struct {
	Name                     string            `json:"name"`
	MatchExporterPrefixes    []string          `json:"match_exporter_prefixes"`
	Set                      map[string]any    `json:"set"`
	ReplaceStrings           map[string]string `json:"replace_strings"`
	ReplaceWithExporterKey   string            `json:"replace_with_exporter_key"`
	ReplaceWithExporterValue map[string]any    `json:"replace_with_exporter_value,omitempty"`
}

func rewriteExporterConfig(exporterName string, raw any, rules []exporterRewriteRule, templateVars map[string]string) (string, any) {
	cfg, ok := raw.(map[string]any)
	if !ok {
		return exporterName, raw
	}

	newName := exporterName
	for _, rule := range matchingRewriteRules(exporterName, rules) {
		if rule.ReplaceWithExporterKey != "" && rule.ReplaceWithExporterValue != nil {
			newName = rule.ReplaceWithExporterKey
			cfg = rule.ReplaceWithExporterValue
		}
		for path, value := range rule.Set {
			setNestedValue(cfg, path, resolveTemplateValues(value, templateVars))
		}
		if len(rule.ReplaceStrings) > 0 {
			applyStringReplacementsRecursive(cfg, rule.ReplaceStrings, templateVars)
		}
	}

	return newName, cfg
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
		if err := yaml.Unmarshal([]byte(telemetryValidationExporterRewritesYAML), &cachedExporterRewrites); err != nil {
			cachedExporterRewrites = exporterRewriteConfig{}
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
			Name:                     tvRule.Name,
			MatchExporterPrefixes:    append([]string(nil), tvRule.MatchExporterPrefixes...),
			Set:                      mapStringInterface(tvRule.Set),
			ReplaceStrings:           mapStringString(tvRule.ReplaceStrings),
			ReplaceWithExporterKey:   tvRule.ReplaceWithExporterKey,
			ReplaceWithExporterValue: mapStringInterface(tvRule.ReplaceWithExporterValue),
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
	if override.ReplaceWithExporterKey != "" {
		merged.ReplaceWithExporterKey = override.ReplaceWithExporterKey
	}
	if len(base.ReplaceWithExporterValue) > 0 || len(override.ReplaceWithExporterValue) > 0 {
		merged.ReplaceWithExporterValue = maps.Clone(base.ReplaceWithExporterValue)
		if merged.ReplaceWithExporterValue == nil {
			merged.ReplaceWithExporterValue = map[string]any{}
		}
		maps.Copy(merged.ReplaceWithExporterValue, override.ReplaceWithExporterValue)
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
