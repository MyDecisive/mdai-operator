package collectorconfig

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"

	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultOTLPGRPCPort uint32 = 4317
	DefaultOTLPHTTPPort uint32 = 4318
)

var endpointPortEnvRef = regexp.MustCompile(`(?:^|:)\$\{(?:env:)?([A-Za-z_][A-Za-z0-9_]*)(?::-(\d+))?\}$`)

// ReceiverPort is a collector receiver port plus protocol information needed by xDS.
type ReceiverPort struct {
	Port        uint32
	EnableHTTP2 bool
}

// DefaultReceiverPorts returns the OpenTelemetry Collector's default OTLP gRPC and HTTP ports.
func DefaultReceiverPorts() []ReceiverPort {
	return []ReceiverPort{
		{Port: DefaultOTLPGRPCPort, EnableHTTP2: true},
		{Port: DefaultOTLPHTTPPort, EnableHTTP2: false},
	}
}

// ReceiverPorts extracts receiver endpoint ports from an OpenTelemetryCollector config.
func ReceiverPorts(ctx context.Context, reader client.Reader, collector otelv1beta1.OpenTelemetryCollector) ([]ReceiverPort, error) {
	portProtocols := make(map[uint32]bool)
	for receiverName, rawReceiver := range collector.Spec.Config.Receivers.Object {
		receiver, ok := rawReceiver.(map[string]any)
		if !ok {
			continue
		}

		if endpoint, ok := receiver["endpoint"].(string); ok {
			port, resolved, err := ResolveEndpointPort(ctx, reader, collector, endpoint)
			if err != nil {
				return nil, fmt.Errorf("receiver %q endpoint %q: %w", receiverName, endpoint, err)
			}
			if _, exists := portProtocols[port]; resolved && !exists {
				portProtocols[port] = false
			}
		}

		protocols, ok := receiver["protocols"].(map[string]any)
		if !ok {
			continue
		}
		for protocolName, rawProtocol := range protocols {
			protocol, ok := rawProtocol.(map[string]any)
			if !ok {
				continue
			}
			endpoint, ok := protocol["endpoint"].(string)
			if !ok {
				continue
			}
			port, resolved, err := ResolveEndpointPort(ctx, reader, collector, endpoint)
			if err != nil {
				return nil, fmt.Errorf("receiver %q protocol %q endpoint %q: %w", receiverName, protocolName, endpoint, err)
			}
			if resolved {
				portProtocols[port] = portProtocols[port] || strings.EqualFold(protocolName, "grpc")
			}
		}
	}

	ports := make([]ReceiverPort, 0, len(portProtocols))
	for port, enableHTTP2 := range portProtocols {
		ports = append(ports, ReceiverPort{
			Port:        port,
			EnableHTTP2: enableHTTP2,
		})
	}
	slices.SortFunc(ports, func(a, b ReceiverPort) int {
		return int(a.Port) - int(b.Port)
	})
	return ports, nil
}

// ReceiverPortNumbers strips protocol metadata from receiver ports.
func ReceiverPortNumbers(ports []ReceiverPort) []uint32 {
	result := make([]uint32, 0, len(ports))
	for _, p := range ports {
		result = append(result, p.Port)
	}
	return result
}

// PreferredReceiverPortNumbers returns Datadog receiver ports when present, otherwise all receiver ports.
func PreferredReceiverPortNumbers(ctx context.Context, reader client.Reader, collector otelv1beta1.OpenTelemetryCollector) ([]uint32, error) {
	datadogPorts := make([]uint32, 0)
	allPorts := make([]uint32, 0)
	for receiverName, rawReceiver := range collector.Spec.Config.Receivers.Object {
		receiver, ok := rawReceiver.(map[string]any)
		if !ok {
			continue
		}
		receiverPorts, err := receiverConfigPorts(ctx, reader, collector, receiverName, receiver)
		if err != nil {
			return nil, err
		}
		if len(receiverPorts) == 0 {
			continue
		}
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(receiverName)), "datadog") {
			datadogPorts = append(datadogPorts, receiverPorts...)
		}
		allPorts = append(allPorts, receiverPorts...)
	}
	if len(datadogPorts) > 0 {
		return sortAndDedupePorts(datadogPorts), nil
	}
	return sortAndDedupePorts(allPorts), nil
}

// ResolveEndpointPort resolves the TCP port for a static endpoint or a final ${env:...} port reference.
func ResolveEndpointPort(
	ctx context.Context,
	reader client.Reader,
	collector otelv1beta1.OpenTelemetryCollector,
	endpoint string,
) (uint32, bool, error) {
	if port, ok := StaticEndpointPort(endpoint); ok {
		return port, true, nil
	}

	matches := endpointPortEnvRef.FindStringSubmatch(strings.TrimSpace(endpoint))
	if matches == nil {
		return 0, false, nil
	}

	envName := matches[1]
	value, ok, err := ResolveEnvVar(ctx, reader, collector, envName)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		if matches[2] != "" {
			port, ok := parsePort(matches[2])
			return port, ok, nil
		}
		return 0, false, fmt.Errorf("environment variable %q is not resolvable from spec.env or spec.envFrom", envName)
	}

	port, ok := parsePort(value)
	if !ok {
		return 0, false, fmt.Errorf("environment variable %q value %q is not a valid TCP port", envName, value)
	}
	return port, true, nil
}

// StaticEndpointPort extracts a TCP port from a literal endpoint.
func StaticEndpointPort(endpoint string) (uint32, bool) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || strings.Contains(endpoint, "${") {
		return 0, false
	}

	_, portText, err := net.SplitHostPort(endpoint)
	if err == nil {
		return parsePort(portText)
	}
	return parsePort(endpoint)
}

// ResolveEnvVar resolves a collector pod environment variable from spec.env or spec.envFrom.
func ResolveEnvVar(
	ctx context.Context,
	reader client.Reader,
	collector otelv1beta1.OpenTelemetryCollector,
	name string,
) (string, bool, error) {
	for _, env := range collector.Spec.Env {
		if env.Name != name {
			continue
		}
		if env.Value != "" {
			return env.Value, true, nil
		}
		if env.ValueFrom == nil {
			return env.Value, true, nil
		}
		return resolveEnvVarSource(ctx, reader, collector.Namespace, env.ValueFrom)
	}

	resolvedValue := ""
	resolved := false
	for _, envFrom := range collector.Spec.EnvFrom {
		value, ok, err := resolveEnvFromSource(ctx, reader, collector.Namespace, name, envFrom)
		if err != nil {
			return "", false, err
		}
		if ok {
			resolvedValue = value
			resolved = true
		}
	}
	return resolvedValue, resolved, nil
}

func receiverConfigPorts(
	ctx context.Context,
	reader client.Reader,
	collector otelv1beta1.OpenTelemetryCollector,
	receiverName string,
	receiver map[string]any,
) ([]uint32, error) {
	ports := make([]uint32, 0)
	if endpoint, ok := receiver["endpoint"].(string); ok {
		port, resolved, err := ResolveEndpointPort(ctx, reader, collector, endpoint)
		if err != nil {
			return nil, fmt.Errorf("receiver %q endpoint %q: %w", receiverName, endpoint, err)
		}
		if resolved {
			ports = append(ports, port)
		}
	}
	if protocols, ok := receiver["protocols"].(map[string]any); ok {
		for protocolName, rawProtocol := range protocols {
			protocol, ok := rawProtocol.(map[string]any)
			if !ok {
				continue
			}
			endpoint, ok := protocol["endpoint"].(string)
			if !ok {
				continue
			}
			port, resolved, err := ResolveEndpointPort(ctx, reader, collector, endpoint)
			if err != nil {
				return nil, fmt.Errorf("receiver %q protocol %q endpoint %q: %w", receiverName, protocolName, endpoint, err)
			}
			if resolved {
				ports = append(ports, port)
			}
		}
	}
	return ports, nil
}

func resolveEnvVarSource(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	source *corev1.EnvVarSource,
) (string, bool, error) {
	if source == nil {
		return "", false, nil
	}
	if source.ConfigMapKeyRef != nil {
		return configMapValue(ctx, reader, namespace, source.ConfigMapKeyRef.Name, source.ConfigMapKeyRef.Key, source.ConfigMapKeyRef.Optional)
	}
	if source.SecretKeyRef != nil {
		return secretValue(ctx, reader, namespace, source.SecretKeyRef.Name, source.SecretKeyRef.Key, source.SecretKeyRef.Optional)
	}
	return "", false, nil
}

func resolveEnvFromSource(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	name string,
	source corev1.EnvFromSource,
) (string, bool, error) {
	key := name
	if source.Prefix != "" {
		if !strings.HasPrefix(name, source.Prefix) {
			return "", false, nil
		}
		key = strings.TrimPrefix(name, source.Prefix)
	}
	if source.ConfigMapRef != nil {
		return configMapValue(ctx, reader, namespace, source.ConfigMapRef.Name, key, source.ConfigMapRef.Optional)
	}
	if source.SecretRef != nil {
		return secretValue(ctx, reader, namespace, source.SecretRef.Name, key, source.SecretRef.Optional)
	}
	return "", false, nil
}

func configMapValue(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	name string,
	key string,
	optional *bool,
) (string, bool, error) {
	if reader == nil {
		return "", false, fmt.Errorf("cannot resolve ConfigMap %q without a Kubernetes reader", name)
	}
	configMap := &corev1.ConfigMap{}
	err := reader.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, configMap)
	if apierrors.IsNotFound(err) && optionalValue(optional) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	value, ok := configMap.Data[key]
	return value, ok, nil
}

func secretValue(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	name string,
	key string,
	optional *bool,
) (string, bool, error) {
	if reader == nil {
		return "", false, fmt.Errorf("cannot resolve Secret %q without a Kubernetes reader", name)
	}
	secret := &corev1.Secret{}
	err := reader.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret)
	if apierrors.IsNotFound(err) && optionalValue(optional) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	value, ok := secret.Data[key]
	if !ok {
		return "", false, nil
	}
	return string(value), true, nil
}

func optionalValue(optional *bool) bool {
	return optional != nil && *optional
}

func parsePort(raw string) (uint32, bool) {
	n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 32)
	if err != nil || n == 0 || n > 65535 {
		return 0, false
	}
	return uint32(n), true
}

func sortAndDedupePorts(ports []uint32) []uint32 {
	if len(ports) == 0 {
		return ports
	}
	sorted := slices.Clone(ports)
	slices.Sort(sorted)
	deduped := sorted[:1]
	for _, p := range sorted[1:] {
		if p != deduped[len(deduped)-1] {
			deduped = append(deduped, p)
		}
	}
	return deduped
}
