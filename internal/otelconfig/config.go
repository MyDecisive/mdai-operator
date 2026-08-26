// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelconfig

import (
	"bytes"
	"sort"

	goyaml "github.com/goccy/go-yaml"
	"github.com/mydecisive/mdai-operator/internal/components"
	"github.com/mydecisive/mdai-operator/internal/components/exporters"
	"github.com/mydecisive/mdai-operator/internal/components/extensions"
	"github.com/mydecisive/mdai-operator/internal/components/receivers"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

// GetEnabledComponents constructs a list of enabled components by component type.
//
// This preserves the public Config.GetEnabledComponents behavior that existed in
// older opentelemetry-operator releases before the helper moved under upstream
// internal/otelconfig.
func GetEnabledComponents(c *v1beta1.Config) map[v1beta1.ComponentKind]map[string]any {
	toReturn := map[v1beta1.ComponentKind]map[string]any{
		v1beta1.KindReceiver:  {},
		v1beta1.KindProcessor: {},
		v1beta1.KindExporter:  {},
		v1beta1.KindExtension: {},
	}
	for _, extension := range c.Service.Extensions {
		toReturn[v1beta1.KindExtension][extension] = struct{}{}
	}

	for _, pipeline := range c.Service.Pipelines {
		if pipeline == nil {
			continue
		}
		for _, componentID := range pipeline.Receivers {
			toReturn[v1beta1.KindReceiver][componentID] = struct{}{}
		}
		for _, componentID := range pipeline.Exporters {
			toReturn[v1beta1.KindExporter][componentID] = struct{}{}
		}
		for _, componentID := range pipeline.Processors {
			toReturn[v1beta1.KindProcessor][componentID] = struct{}{}
		}
	}
	for _, componentID := range c.Service.Extensions {
		toReturn[v1beta1.KindExtension][componentID] = struct{}{}
	}
	return toReturn
}

// GetReceiverPorts gets the ports for receivers enabled in service pipelines.
func GetReceiverPorts(c *v1beta1.Config, logger *zap.Logger) ([]corev1.ServicePort, error) {
	return getPortsForComponentKinds(c, logger, v1beta1.KindReceiver)
}

// GetAllPorts gets the ports for enabled receivers, exporters, and extensions.
func GetAllPorts(c *v1beta1.Config, logger *zap.Logger) ([]corev1.ServicePort, error) {
	return getPortsForComponentKinds(c, logger, v1beta1.KindReceiver, v1beta1.KindExporter, v1beta1.KindExtension)
}

// YAML encodes the collector config and returns it as a string.
func YAML(c *v1beta1.Config) (string, error) {
	var buf bytes.Buffer
	yamlEncoder := goyaml.NewEncoder(&buf, goyaml.IndentSequence(true), goyaml.AutoInt())
	if err := yamlEncoder.Encode(&c); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func getPortsForComponentKinds(c *v1beta1.Config, logger *zap.Logger, componentKinds ...v1beta1.ComponentKind) ([]corev1.ServicePort, error) {
	var ports []corev1.ServicePort
	enabledComponents := GetEnabledComponents(c)
	for _, componentKind := range componentKinds {
		var retriever components.ParserRetriever
		var cfg v1beta1.AnyConfig
		switch componentKind {
		case v1beta1.KindReceiver:
			retriever = receivers.ReceiverFor
			cfg = c.Receivers
		case v1beta1.KindExporter:
			retriever = exporters.ParserFor
			cfg = c.Exporters
		case v1beta1.KindProcessor:
			continue
		case v1beta1.KindExtension:
			retriever = extensions.ParserFor
			if c.Extensions != nil {
				cfg = *c.Extensions
			}
		default:
			logger.Debug("unknown component kind", zap.Int("kind", int(componentKind)))
			continue
		}
		for componentName := range enabledComponents[componentKind] {
			parser := retriever(componentName)
			parsedPorts, err := parser.Ports(logger, componentName, cfg.Object[componentName])
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
