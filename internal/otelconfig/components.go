package otelconfig

import "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"

func EnabledComponents(config *v1beta1.Config) map[v1beta1.ComponentKind]map[string]struct{} {
	enabled := map[v1beta1.ComponentKind]map[string]struct{}{
		v1beta1.KindReceiver:  {},
		v1beta1.KindProcessor: {},
		v1beta1.KindExporter:  {},
		v1beta1.KindExtension: {},
	}
	if config == nil {
		return enabled
	}

	for _, extension := range config.Service.Extensions {
		enabled[v1beta1.KindExtension][extension] = struct{}{}
	}
	for _, pipeline := range config.Service.Pipelines {
		if pipeline == nil {
			continue
		}
		for _, receiver := range pipeline.Receivers {
			enabled[v1beta1.KindReceiver][receiver] = struct{}{}
		}
		for _, exporter := range pipeline.Exporters {
			enabled[v1beta1.KindExporter][exporter] = struct{}{}
		}
		for _, processor := range pipeline.Processors {
			enabled[v1beta1.KindProcessor][processor] = struct{}{}
		}
	}
	return enabled
}
