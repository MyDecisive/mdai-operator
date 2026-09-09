package collectorconfig

import (
	"testing"

	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestStaticEndpointPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		wantPort uint32
		wantOK   bool
	}{
		{name: "bare port", endpoint: "4317", wantPort: 4317, wantOK: true},
		{name: "wildcard host", endpoint: "0.0.0.0:4317", wantPort: 4317, wantOK: true},
		{name: "empty host", endpoint: ":4317", wantPort: 4317, wantOK: true},
		{name: "localhost", endpoint: "localhost:4317", wantPort: 4317, wantOK: true},
		{name: "dns name", endpoint: "dns:4317", wantPort: 4317, wantOK: true},
		{name: "ipv6", endpoint: "[::]:4317", wantPort: 4317, wantOK: true},
		{name: "too large", endpoint: "localhost:99999", wantOK: false},
		{name: "missing port", endpoint: "localhost", wantOK: false},
		{name: "env ref", endpoint: "${env:PORT}", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotPort, gotOK := StaticEndpointPort(tt.endpoint)
			assert.Equal(t, tt.wantOK, gotOK)
			assert.Equal(t, tt.wantPort, gotPort)
		})
	}
}

func TestResolveEndpointPortFromCollectorEnvironment(t *testing.T) {
	t.Parallel()

	optional := true
	tests := []struct {
		name      string
		endpoint  string
		env       []corev1.EnvVar
		envFrom   []corev1.EnvFromSource
		objects   []client.Object
		wantPort  uint32
		wantOK    bool
		wantError string
	}{
		{
			name:     "direct env value",
			endpoint: "localhost:${env:PORT}",
			env:      []corev1.EnvVar{{Name: "PORT", Value: "8126"}},
			wantPort: 8126,
			wantOK:   true,
		},
		{
			name:     "direct env shorthand",
			endpoint: ":${PORT}",
			env:      []corev1.EnvVar{{Name: "PORT", Value: "4318"}},
			wantPort: 4318,
			wantOK:   true,
		},
		{
			name:     "env default",
			endpoint: "0.0.0.0:${env:PORT:-14268}",
			wantPort: 14268,
			wantOK:   true,
		},
		{
			name:     "configmap key ref",
			endpoint: "0.0.0.0:${env:PORT}",
			env: []corev1.EnvVar{{
				Name: "PORT",
				ValueFrom: &corev1.EnvVarSource{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "ports"},
						Key:                  "grpc",
					},
				},
			}},
			objects:  []client.Object{configMap("ports", map[string]string{"grpc": "4317"})},
			wantPort: 4317,
			wantOK:   true,
		},
		{
			name:     "secret key ref",
			endpoint: "0.0.0.0:${env:PORT}",
			env: []corev1.EnvVar{{
				Name: "PORT",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "ports"},
						Key:                  "grpc",
					},
				},
			}},
			objects:  []client.Object{secret("ports", map[string][]byte{"grpc": []byte("4317")})},
			wantPort: 4317,
			wantOK:   true,
		},
		{
			name:     "configmap envFrom",
			endpoint: "0.0.0.0:${env:PORT}",
			envFrom: []corev1.EnvFromSource{{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "ports"},
				},
			}},
			objects:  []client.Object{configMap("ports", map[string]string{"PORT": "8126"})},
			wantPort: 8126,
			wantOK:   true,
		},
		{
			name:     "prefixed envFrom",
			endpoint: "0.0.0.0:${env:OTEL_PORT}",
			envFrom: []corev1.EnvFromSource{{
				Prefix: "OTEL_",
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "ports"},
				},
			}},
			objects:  []client.Object{configMap("ports", map[string]string{"PORT": "4318"})},
			wantPort: 4318,
			wantOK:   true,
		},
		{
			name:     "envFrom later source wins",
			endpoint: "0.0.0.0:${env:PORT}",
			envFrom: []corev1.EnvFromSource{
				{ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "first"},
				}},
				{ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "second"},
				}},
			},
			objects: []client.Object{
				configMap("first", map[string]string{"PORT": "4317"}),
				configMap("second", map[string]string{"PORT": "4318"}),
			},
			wantPort: 4318,
			wantOK:   true,
		},
		{
			name:     "explicit env wins over envFrom",
			endpoint: "0.0.0.0:${env:PORT}",
			env:      []corev1.EnvVar{{Name: "PORT", Value: "8126"}},
			envFrom: []corev1.EnvFromSource{{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "ports"},
				},
			}},
			objects:  []client.Object{configMap("ports", map[string]string{"PORT": "4317"})},
			wantPort: 8126,
			wantOK:   true,
		},
		{
			name:     "optional missing source is unresolved",
			endpoint: "0.0.0.0:${env:PORT}",
			envFrom: []corev1.EnvFromSource{{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "missing"},
					Optional:             &optional,
				},
			}},
			wantError: `environment variable "PORT" is not resolvable`,
		},
		{
			name:      "missing env",
			endpoint:  "0.0.0.0:${env:PORT}",
			wantError: `environment variable "PORT" is not resolvable`,
		},
		{
			name:      "invalid env port",
			endpoint:  "0.0.0.0:${env:PORT}",
			env:       []corev1.EnvVar{{Name: "PORT", Value: "nope"}},
			wantError: `environment variable "PORT" value "nope" is not a valid TCP port`,
		},
		{
			name:      "empty direct env value",
			endpoint:  "0.0.0.0:${env:PORT}",
			env:       []corev1.EnvVar{{Name: "PORT"}},
			wantError: `environment variable "PORT" value "" is not a valid TCP port`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			collector := collector(tt.env, tt.envFrom, otelv1beta1.Config{})
			port, ok, err := ResolveEndpointPort(t.Context(), fakeReader(tt.objects...), collector, tt.endpoint)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantPort, port)
		})
	}
}

func TestReceiverPorts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   otelv1beta1.Config
		env      []corev1.EnvVar
		want     []ReceiverPort
		wantPort []uint32
	}{
		{
			name: "dedupes and marks grpc ports for HTTP2",
			config: otelv1beta1.Config{Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
				"datadog": map[string]any{"endpoint": "localhost:8126"},
				"otlp": map[string]any{"protocols": map[string]any{
					"grpc": map[string]any{"endpoint": "localhost:4317"},
					"http": map[string]any{"endpoint": "0.0.0.0:4318"},
				}},
				"zipkin": map[string]any{"endpoint": "0.0.0.0:${env:ZIPKIN_PORT}"},
			}}},
			env: []corev1.EnvVar{{Name: "ZIPKIN_PORT", Value: "9411"}},
			want: []ReceiverPort{
				{Port: 4317, EnableHTTP2: true},
				{Port: 4318, EnableHTTP2: false},
				{Port: 8126, EnableHTTP2: false},
				{Port: 9411, EnableHTTP2: false},
			},
			wantPort: []uint32{4317, 4318, 8126, 9411},
		},
		{
			name: "ignores unsupported non-env endpoint forms",
			config: otelv1beta1.Config{Receivers: otelv1beta1.AnyConfig{Object: map[string]any{
				"otlp": map[string]any{"endpoint": "http://localhost:4317"},
			}}},
			want:     []ReceiverPort{},
			wantPort: []uint32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ports, err := ReceiverPorts(t.Context(), nil, collector(tt.env, nil, tt.config))
			require.NoError(t, err)
			assert.Equal(t, tt.want, ports)
			assert.Equal(t, tt.wantPort, ReceiverPortNumbers(ports))
		})
	}
}

func fakeReader(objects ...client.Object) client.Reader {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
}

func collector(env []corev1.EnvVar, envFrom []corev1.EnvFromSource, config otelv1beta1.Config) otelv1beta1.OpenTelemetryCollector {
	return otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{Name: "gateway", Namespace: "mdai"},
		Spec: otelv1beta1.OpenTelemetryCollectorSpec{
			OpenTelemetryCommonFields: otelv1beta1.OpenTelemetryCommonFields{
				Env:     env,
				EnvFrom: envFrom,
			},
			Config: config,
		},
	}
}

func configMap(name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "mdai"},
		Data:       data,
	}
}

func secret(name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "mdai"},
		Data:       data,
	}
}
