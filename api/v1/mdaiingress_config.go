package v1

import (
	"bytes"
	"sort"

	"github.com/goccy/go-yaml"
	"github.com/mydecisive/mdai-operator/internal/components"
	"github.com/mydecisive/mdai-operator/internal/components/exporters"
	"github.com/mydecisive/mdai-operator/internal/components/extensions"
	"github.com/mydecisive/mdai-operator/internal/components/receivers"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

// Config encapsulates collector & ingress config.
type OtelMdaiIngressComb struct {
	Otelcol     v1beta1.OpenTelemetryCollector
	MdaiIngress MdaiIngress
}

func NewOtelIngressConfig(otelcolConfig v1beta1.OpenTelemetryCollector, ingressConfig MdaiIngress) *OtelMdaiIngressComb {
	return &OtelMdaiIngressComb{
		Otelcol:     otelcolConfig,
		MdaiIngress: ingressConfig,
	}
}

// mydecisive
func (c *OtelMdaiIngressComb) GetReceiverPortsWithUrlPaths(logger *zap.Logger) (components.ComponentsPortsUrlPaths, error) {
	return c.getPortsWithUrlPathsForComponentKinds(logger, v1beta1.KindReceiver)
}

// mydecisive
func (c *OtelMdaiIngressComb) getPortsWithUrlPathsForComponentKinds(logger *zap.Logger, componentKinds ...v1beta1.ComponentKind) (components.ComponentsPortsUrlPaths, error) {
	componentsPortsUrlPaths := components.ComponentsPortsUrlPaths{}
	enabledComponents := getEnabledComponents(&c.Otelcol.Spec.Config)
	for _, componentKind := range componentKinds {
		var retriever components.ParserRetriever
		var cfg v1beta1.AnyConfig
		switch componentKind {
		case v1beta1.KindReceiver:
			retriever = receivers.ReceiverFor
			cfg = c.Otelcol.Spec.Config.Receivers
		case v1beta1.KindExporter:
			retriever = exporters.ParserFor
			cfg = c.Otelcol.Spec.Config.Exporters
		case v1beta1.KindProcessor, v1beta1.KindExtension:
			continue
		default:
			logger.Error("Unsupported component kind:", zap.Int("componentKind", int(componentKind)))
			continue
		}
		for componentName := range enabledComponents[componentKind] {
			// TODO: Clean up the naming here and make it simpler to use a retriever.
			parser := retriever(componentName)
			parsedPorts, err := parser.PortsWithUrlPaths(logger, componentName, cfg.Object[componentName])
			if err != nil {
				return nil, err
			}
			componentsPortsUrlPaths[componentName] = parsedPorts
		}
	}

	return componentsPortsUrlPaths, nil
}

// ConfigYaml replicates the removed v1beta1.Config.Yaml, which was deleted upstream
// (github.com/open-telemetry/opentelemetry-operator) in v0.152.0.
func ConfigYaml(cfg *v1beta1.Config) (string, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf, yaml.IndentSequence(true), yaml.AutoInt())
	if err := enc.Encode(cfg); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// GetReceiverPorts replicates the removed v1beta1.Config.GetReceiverPorts (see getEnabledComponents).
func GetReceiverPorts(logger *zap.Logger, cfg *v1beta1.Config) ([]corev1.ServicePort, error) {
	return getPortsForComponentKinds(logger, cfg, v1beta1.KindReceiver)
}

// GetAllPorts replicates the removed v1beta1.Config.GetAllPorts (see getEnabledComponents).
func GetAllPorts(logger *zap.Logger, cfg *v1beta1.Config) ([]corev1.ServicePort, error) {
	return getPortsForComponentKinds(logger, cfg, v1beta1.KindReceiver, v1beta1.KindExporter, v1beta1.KindExtension)
}

func getPortsForComponentKinds(logger *zap.Logger, cfg *v1beta1.Config, componentKinds ...v1beta1.ComponentKind) ([]corev1.ServicePort, error) {
	var ports []corev1.ServicePort
	enabledComponents := getEnabledComponents(cfg)
	for _, componentKind := range componentKinds {
		var retriever components.ParserRetriever
		var componentCfg v1beta1.AnyConfig
		switch componentKind {
		case v1beta1.KindReceiver:
			retriever = receivers.ReceiverFor
			componentCfg = cfg.Receivers
		case v1beta1.KindExporter:
			retriever = exporters.ParserFor
			componentCfg = cfg.Exporters
		case v1beta1.KindProcessor:
			continue
		case v1beta1.KindExtension:
			retriever = extensions.ParserFor
			if cfg.Extensions != nil {
				componentCfg = *cfg.Extensions
			}
		default:
			logger.Error("Unsupported component kind:", zap.Int("componentKind", int(componentKind)))
			continue
		}
		for componentName := range enabledComponents[componentKind] {
			parser := retriever(componentName)
			parsedPorts, err := parser.Ports(logger, componentName, componentCfg.Object[componentName])
			if err != nil {
				return nil, err
			}
			ports = append(ports, parsedPorts...)
		}
	}

	sort.Slice(ports, func(i, j int) bool {
		return ports[i].Name < ports[j].Name
	})

	return ports, nil
}

// getEnabledComponents constructs a list of enabled components by component type from the
// collector's pipelines. This replicates v1beta1.Config.GetEnabledComponents, which was removed
// upstream (github.com/open-telemetry/opentelemetry-operator) in v0.152.0.
func getEnabledComponents(c *v1beta1.Config) map[v1beta1.ComponentKind]map[string]struct{} {
	enabled := map[v1beta1.ComponentKind]map[string]struct{}{
		v1beta1.KindReceiver:  {},
		v1beta1.KindProcessor: {},
		v1beta1.KindExporter:  {},
		v1beta1.KindExtension: {},
	}
	for _, extension := range c.Service.Extensions {
		enabled[v1beta1.KindExtension][extension] = struct{}{}
	}
	for _, pipeline := range c.Service.Pipelines {
		if pipeline == nil {
			continue
		}
		for _, componentID := range pipeline.Receivers {
			enabled[v1beta1.KindReceiver][componentID] = struct{}{}
		}
		for _, componentID := range pipeline.Exporters {
			enabled[v1beta1.KindExporter][componentID] = struct{}{}
		}
		for _, componentID := range pipeline.Processors {
			enabled[v1beta1.KindProcessor][componentID] = struct{}{}
		}
	}
	return enabled
}
