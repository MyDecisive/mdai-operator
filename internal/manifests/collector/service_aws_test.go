// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// nolint:gofumpt,goconst
package collector

import (
	"slices"
	"strings"
	"testing"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const testFileServiceAws = "testdata/service_aws_testdata.yaml"

func TestDesiredServiceAws(t *testing.T) {
	grpc := "grpc"
	http := "http"

	t.Run("create gRPC and non-gRPC Services", func(t *testing.T) {
		params, err := newParams(testFileServiceAws)
		if err != nil {
			t.Fatal(err)
		}
		params.OtelMdaiIngressComb.MdaiIngress.Spec.CloudType = mdaiv1.CloudProviderAws
		params.OtelMdaiIngressComb.Otelcol.Spec.Ports = []v1beta1.PortsSpec{}
		params.OtelMdaiIngressComb.MdaiIngress.Spec.GrpcService = &mdaiv1.IngressService{Type: corev1.ServiceTypeNodePort}
		params.OtelMdaiIngressComb.MdaiIngress.Spec.NonGrpcService = &mdaiv1.IngressService{Type: corev1.ServiceTypeLoadBalancer}

		desiredGrpcSpec := corev1.ServiceSpec{
			Type: corev1.ServiceTypeNodePort,
			Ports: []corev1.ServicePort{
				{
					Name:        "jaeger-grpc",
					Port:        14260,
					TargetPort:  intstr.FromInt32(14260),
					Protocol:    corev1.ProtocolTCP,
					AppProtocol: &grpc,
				},
				{
					Name:        "otlp-1-grpc",
					Port:        12345,
					TargetPort:  intstr.FromInt32(12345),
					Protocol:    "",
					AppProtocol: &grpc,
				},
				{
					Name:        "otlp-2-grpc",
					Port:        98765,
					TargetPort:  intstr.FromInt32(98765),
					Protocol:    "",
					AppProtocol: &grpc,
				},
			},
		}
		desiredNonGrpcSpec := corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{
				{
					Name:        "otlp-1-http",
					Port:        12121,
					TargetPort:  intstr.FromInt32(12121),
					Protocol:    "",
					AppProtocol: &http,
				},
				{
					Name:        "otlp-2-http",
					Port:        4318,
					TargetPort:  intstr.FromInt32(4318),
					Protocol:    "",
					AppProtocol: &http,
				},
				{
					Name:        "port-14268",
					Port:        14268,
					TargetPort:  intstr.FromInt32(14268),
					Protocol:    "TCP",
					AppProtocol: &http,
				},
			},
		}

		actualGrpc, err := GrpcService(params)
		require.NoError(t, err)
		assert.Equal(t, desiredGrpcSpec.Type, actualGrpc.Spec.Type)

		desiredPorts := desiredGrpcSpec.Ports
		actualPorts := actualGrpc.Spec.Ports
		slices.SortFunc(desiredPorts, func(a, b corev1.ServicePort) int { return strings.Compare(a.Name, b.Name) })
		slices.SortFunc(actualPorts, func(a, b corev1.ServicePort) int { return strings.Compare(a.Name, b.Name) })
		assert.Equal(t, desiredPorts, actualPorts)

		actualNonGrpc, err := NonGrpcService(params)
		require.NoError(t, err)
		assert.Equal(t, desiredNonGrpcSpec.Type, actualNonGrpc.Spec.Type)

		desiredPorts = desiredNonGrpcSpec.Ports
		actualPorts = actualNonGrpc.Spec.Ports
		slices.SortFunc(desiredPorts, func(a, b corev1.ServicePort) int { return strings.Compare(a.Name, b.Name) })
		slices.SortFunc(actualPorts, func(a, b corev1.ServicePort) int { return strings.Compare(a.Name, b.Name) })
		assert.Equal(t, desiredPorts, actualPorts)
	})
}
func TestAnnotationsForNonGrpcService(t *testing.T) {
	http := "http"

	t.Run("create non-gRPC Service", func(t *testing.T) {
		params, err := newParams(testFileServiceAws)
		if err != nil {
			t.Fatal(err)
		}
		params.OtelMdaiIngressComb.MdaiIngress.Spec.CloudType = mdaiv1.CloudProviderAws
		params.OtelMdaiIngressComb.Otelcol.Spec.Ports = []v1beta1.PortsSpec{}
		params.OtelMdaiIngressComb.Otelcol.Annotations = map[string]string{
			"annotation_common": "value_from_meta",
			"meta.annotation":   "meta_value_2",
		}
		params.OtelMdaiIngressComb.MdaiIngress.Spec.NonGrpcService = &mdaiv1.IngressService{
			Type: corev1.ServiceTypeLoadBalancer,
			Annotations: map[string]string{
				"annotation_common":  "value_from_service",
				"service.annotation": "value_from_service_2",
			},
		}
		trafficPolicy := corev1.ServiceInternalTrafficPolicyCluster

		desiredNonGrpcService := corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					"meta.annotation":    "meta_value_2",
					"annotation_common":  "value_from_service",
					"service.annotation": "value_from_service_2",
				},
			},
			Spec: corev1.ServiceSpec{
				Type:                  corev1.ServiceTypeLoadBalancer,
				InternalTrafficPolicy: &trafficPolicy,
				Ports: []corev1.ServicePort{
					{
						Name:        "otlp-1-http",
						Port:        4318,
						TargetPort:  intstr.FromInt32(4318),
						Protocol:    "",
						AppProtocol: &http,
					},
				},
			},
		}

		actualNonGrpcService, err := NonGrpcService(params)
		require.NoError(t, err)

		desiredAnnotations := desiredNonGrpcService.Annotations
		actualAnnotations := actualNonGrpcService.Annotations
		assert.Equal(t, desiredAnnotations, actualAnnotations)
	})
}

func TestDesiredServiceAwsEmptyServiceTypes(t *testing.T) {
	t.Run("create gRPC and non-gRPC Services", func(t *testing.T) {
		params, err := newParams(testFileServiceAws)
		if err != nil {
			t.Fatal(err)
		}
		params.OtelMdaiIngressComb.MdaiIngress.Spec.CloudType = mdaiv1.CloudProviderAws
		params.OtelMdaiIngressComb.Otelcol.Spec.Ports = []v1beta1.PortsSpec{}

		actualGrpc, err := GrpcService(params)
		require.NoError(t, err)
		assert.Equal(t, corev1.ServiceTypeClusterIP, actualGrpc.Spec.Type)

		actualNonGrpc, err := NonGrpcService(params)
		require.NoError(t, err)
		assert.Equal(t, corev1.ServiceTypeClusterIP, actualNonGrpc.Spec.Type)
	})
}
