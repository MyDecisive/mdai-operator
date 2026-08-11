package collector

import (
	"sort"

	"github.com/mydecisive/mdai-operator/internal/components"
	"github.com/mydecisive/mdai-operator/internal/components/exporters"
	"github.com/mydecisive/mdai-operator/internal/components/extensions"
	"github.com/mydecisive/mdai-operator/internal/components/receivers"
	"github.com/mydecisive/mdai-operator/internal/otelconfig"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

func receiverPorts(logger *zap.Logger, config *v1beta1.Config) ([]corev1.ServicePort, error) {
	return portsForComponentKinds(logger, config, v1beta1.KindReceiver)
}

func allPorts(logger *zap.Logger, config *v1beta1.Config) ([]corev1.ServicePort, error) {
	return portsForComponentKinds(logger, config, v1beta1.KindReceiver, v1beta1.KindExporter, v1beta1.KindExtension)
}

func portsForComponentKinds(logger *zap.Logger, config *v1beta1.Config, componentKinds ...v1beta1.ComponentKind) ([]corev1.ServicePort, error) {
	var ports []corev1.ServicePort
	enabledComponents := otelconfig.EnabledComponents(config)
	for _, componentKind := range componentKinds {
		var retriever components.ParserRetriever
		var cfg v1beta1.AnyConfig
		switch componentKind {
		case v1beta1.KindReceiver:
			retriever = receivers.ReceiverFor
			cfg = config.Receivers
		case v1beta1.KindExporter:
			retriever = exporters.ParserFor
			cfg = config.Exporters
		case v1beta1.KindProcessor:
			continue
		case v1beta1.KindExtension:
			retriever = extensions.ParserFor
			if config.Extensions != nil {
				cfg = *config.Extensions
			}
		default:
			continue
		}
		for componentName := range enabledComponents[componentKind] {
			parsedPorts, err := retriever(componentName).Ports(logger, componentName, cfg.Object[componentName])
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
