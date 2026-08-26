package otelconfig

import (
	"testing"

	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"github.com/stretchr/testify/assert"
)

func TestEnabledComponents(t *testing.T) {
	tests := []struct {
		name string
		cfg  *v1beta1.Config
		want map[v1beta1.ComponentKind][]string
	}{
		{
			name: "nil config returns initialized empty component sets",
			cfg:  nil,
			want: map[v1beta1.ComponentKind][]string{
				v1beta1.KindReceiver:  {},
				v1beta1.KindProcessor: {},
				v1beta1.KindExporter:  {},
				v1beta1.KindExtension: {},
			},
		},
		{
			name: "collects only service graph references",
			cfg: &v1beta1.Config{
				Service: v1beta1.Service{
					Extensions: []string{"health_check"},
					Pipelines: map[string]*v1beta1.Pipeline{
						"metrics": {
							Receivers:  []string{"otlp", "prometheus"},
							Processors: []string{"batch"},
							Exporters:  []string{"debug", "prometheus"},
						},
						"traces": {
							Receivers: []string{"otlp"},
							Exporters: []string{"debug"},
						},
						"logs": nil,
					},
				},
			},
			want: map[v1beta1.ComponentKind][]string{
				v1beta1.KindReceiver:  {"otlp", "prometheus"},
				v1beta1.KindProcessor: {"batch"},
				v1beta1.KindExporter:  {"debug", "prometheus"},
				v1beta1.KindExtension: {"health_check"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EnabledComponents(tt.cfg)

			for kind, wantNames := range tt.want {
				gotNames := make([]string, 0, len(got[kind]))
				for name := range got[kind] {
					gotNames = append(gotNames, name)
				}
				assert.ElementsMatch(t, wantNames, gotNames)
			}
		})
	}
}
