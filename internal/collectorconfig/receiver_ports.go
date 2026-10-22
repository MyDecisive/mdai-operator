package collectorconfig

import (
	"cmp"
	"context"
	"errors"
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

// ErrUnresolvedEndpoint marks a receiver endpoint whose port cannot be resolved from the collector spec,
// as opposed to a transient failure reading a referenced ConfigMap or Secret.
var ErrUnresolvedEndpoint = errors.New("unresolved receiver endpoint port")

// endpointPortEnvRef matches an env reference that is either the whole endpoint (${env:ENDPOINT}) or its port
// (host:${env:PORT}), with an optional :-default.
var endpointPortEnvRef = regexp.MustCompile(`(?:^|:)\$\{(?:env:)?([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}$`)

// ReceiverPort is a collector receiver port plus protocol information needed by xDS.
type ReceiverPort struct {
	Port        uint32
	EnableHTTP2 bool
	// Receivers lists the names of the receivers listening on Port, sorted.
	Receivers []string
}

// DefaultReceiverPorts returns the OpenTelemetry Collector's default OTLP gRPC and HTTP ports.
func DefaultReceiverPorts() []ReceiverPort {
	return []ReceiverPort{
		{Port: DefaultOTLPGRPCPort, EnableHTTP2: true},
		{Port: DefaultOTLPHTTPPort, EnableHTTP2: false},
	}
}

// ReceiverPorts extracts receiver endpoint ports from an OpenTelemetryCollector config.
//
// Endpoints whose port cannot be resolved from the collector spec are skipped; the ports that did resolve are
// returned together with an error wrapping ErrUnresolvedEndpoint for each skipped endpoint. Any other error
// (e.g. failing to read a referenced ConfigMap or Secret) aborts resolution and returns no ports.
func ReceiverPorts(ctx context.Context, reader client.Reader, collector otelv1beta1.OpenTelemetryCollector) ([]ReceiverPort, error) {
	byPort := make(map[uint32]*ReceiverPort)
	var unresolved []error
	for receiverName, rawReceiver := range collector.Spec.Config.Receivers.Object {
		receiver, ok := rawReceiver.(map[string]any)
		if !ok {
			continue
		}
		for _, ep := range receiverEndpoints(receiverName, receiver) {
			port, resolved, err := ResolveEndpointPort(ctx, reader, collector, ep.endpoint)
			if errors.Is(err, ErrUnresolvedEndpoint) {
				unresolved = append(unresolved, fmt.Errorf("%s endpoint %q: %w", ep.location, ep.endpoint, err))
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("%s endpoint %q: %w", ep.location, ep.endpoint, err)
			}
			if !resolved {
				continue
			}
			rp, exists := byPort[port]
			if !exists {
				rp = &ReceiverPort{Port: port}
				byPort[port] = rp
			}
			rp.EnableHTTP2 = rp.EnableHTTP2 || ep.grpc
			if !slices.Contains(rp.Receivers, receiverName) {
				rp.Receivers = append(rp.Receivers, receiverName)
			}
		}
	}

	ports := make([]ReceiverPort, 0, len(byPort))
	for _, rp := range byPort {
		slices.Sort(rp.Receivers)
		ports = append(ports, *rp)
	}
	slices.SortFunc(ports, func(a, b ReceiverPort) int {
		return cmp.Compare(a.Port, b.Port)
	})
	return ports, errors.Join(unresolved...)
}

// ReceiverPortNumbers strips protocol metadata from receiver ports.
func ReceiverPortNumbers(ports []ReceiverPort) []uint32 {
	result := make([]uint32, 0, len(ports))
	for _, p := range ports {
		result = append(result, p.Port)
	}
	return result
}

// PreferredReceiverPortNumbers returns the ports of Datadog receivers when present, otherwise all receiver ports.
func PreferredReceiverPortNumbers(ports []ReceiverPort) []uint32 {
	datadogPorts := make([]uint32, 0)
	for _, p := range ports {
		if slices.ContainsFunc(p.Receivers, isDatadogReceiver) {
			datadogPorts = append(datadogPorts, p.Port)
		}
	}
	if len(datadogPorts) > 0 {
		return datadogPorts
	}
	return ReceiverPortNumbers(ports)
}

// ResolveEndpointPort resolves the TCP port for a static endpoint, an endpoint that is entirely an ${env:...}
// reference (whose value is host:port or a bare port), or a final ${env:...} port reference.
// Errors caused by the collector spec itself (missing or invalid variables, missing non-optional sources) wrap
// ErrUnresolvedEndpoint.
func ResolveEndpointPort(
	ctx context.Context,
	reader client.Reader,
	collector otelv1beta1.OpenTelemetryCollector,
	endpoint string,
) (uint32, bool, error) {
	if port, ok := StaticEndpointPort(endpoint); ok {
		return port, true, nil
	}

	endpoint = strings.TrimSpace(endpoint)
	matches := endpointPortEnvRef.FindStringSubmatch(endpoint)
	if matches == nil {
		return 0, false, nil
	}
	// A reference covering the whole endpoint expands to host:port; one after "host:" expands to the port alone.
	parseValue := parsePort
	if matches[0] == endpoint {
		parseValue = StaticEndpointPort
	}

	envName := matches[1]
	value, ok, err := ResolveEnvVar(ctx, reader, collector, envName)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		if matches[2] != "" {
			port, ok := parseValue(matches[2])
			return port, ok, nil
		}
		return 0, false, fmt.Errorf("%w: environment variable %q is not resolvable from spec.env or spec.envFrom", ErrUnresolvedEndpoint, envName)
	}

	port, ok := parseValue(value)
	if !ok {
		// The value is deliberately omitted: it may come from a Secret.
		return 0, false, fmt.Errorf("%w: environment variable %q is not a valid TCP port", ErrUnresolvedEndpoint, envName)
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

// ResolveEnvVar resolves a collector pod environment variable from spec.env or spec.envFrom, following kubelet
// precedence: envFrom sources are applied first (later sources win), then spec.env entries override them (later
// entries win). A spec.env valueFrom that references a missing optional ConfigMap/Secret or key is skipped, leaving
// any envFrom value in place.
// Kubernetes $(VAR) dependent-variable expansion in spec.env values is not performed.
func ResolveEnvVar(
	ctx context.Context,
	reader client.Reader,
	collector otelv1beta1.OpenTelemetryCollector,
	name string,
) (string, bool, error) {
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

	for _, env := range collector.Spec.Env {
		if env.Name != name {
			continue
		}
		if env.ValueFrom == nil {
			resolvedValue = env.Value
			resolved = true
			continue
		}
		value, ok, err := resolveEnvVarSource(ctx, reader, collector.Namespace, name, env.ValueFrom)
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

// resolveEnvVarSource resolves a spec.env valueFrom. It reports ok=false only when an optional ConfigMap/Secret or
// key is missing, which kubelet skips; a missing required key fails, as it prevents the container from starting.
func resolveEnvVarSource(
	ctx context.Context,
	reader client.Reader,
	namespace string,
	envName string,
	source *corev1.EnvVarSource,
) (string, bool, error) {
	var (
		value string
		ok    bool
		err   error
	)
	switch {
	case source.ConfigMapKeyRef != nil:
		ref := source.ConfigMapKeyRef
		value, ok, err = configMapValue(ctx, reader, namespace, ref.Name, ref.Key, ref.Optional)
		if err == nil && !ok && !optionalValue(ref.Optional) {
			err = fmt.Errorf("%w: key %q not found in ConfigMap %q", ErrUnresolvedEndpoint, ref.Key, ref.Name)
		}
	case source.SecretKeyRef != nil:
		ref := source.SecretKeyRef
		value, ok, err = secretValue(ctx, reader, namespace, ref.Name, ref.Key, ref.Optional)
		if err == nil && !ok && !optionalValue(ref.Optional) {
			err = fmt.Errorf("%w: key %q not found in Secret %q", ErrUnresolvedEndpoint, ref.Key, ref.Name)
		}
	default:
		err = fmt.Errorf("%w: environment variable %q uses an unsupported valueFrom source", ErrUnresolvedEndpoint, envName)
	}
	return value, ok, err
}

type receiverEndpoint struct {
	location string
	endpoint string
	grpc     bool
}

// receiverEndpoints lists the receiver's top-level endpoint and each protocol's endpoint.
func receiverEndpoints(receiverName string, receiver map[string]any) []receiverEndpoint {
	var endpoints []receiverEndpoint
	if endpoint, ok := receiver["endpoint"].(string); ok {
		endpoints = append(endpoints, receiverEndpoint{
			location: fmt.Sprintf("receiver %q", receiverName),
			endpoint: endpoint,
		})
	}
	protocols, ok := receiver["protocols"].(map[string]any)
	if !ok {
		return endpoints
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
		endpoints = append(endpoints, receiverEndpoint{
			location: fmt.Sprintf("receiver %q protocol %q", receiverName, protocolName),
			endpoint: endpoint,
			grpc:     strings.EqualFold(protocolName, "grpc"),
		})
	}
	return endpoints
}

func isDatadogReceiver(receiverName string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(receiverName)), "datadog")
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
	if apierrors.IsNotFound(err) {
		if optionalValue(optional) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("%w: ConfigMap %q not found", ErrUnresolvedEndpoint, name)
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
	if apierrors.IsNotFound(err) {
		if optionalValue(optional) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("%w: Secret %q not found", ErrUnresolvedEndpoint, name)
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
