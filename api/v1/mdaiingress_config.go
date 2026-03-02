package v1

import (
	"github.com/mydecisive/mdai-operator/internal/components"
	"github.com/mydecisive/mdai-operator/internal/components/exporters"
	"github.com/mydecisive/mdai-operator/internal/components/receivers"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"go.uber.org/zap"
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
	enabledComponents := c.Otelcol.Spec.Config.GetEnabledComponents()
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
		case v1beta1.KindExtension, v1beta1.KindProcessor:
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
