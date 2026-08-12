package builder

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	appName                     = "hub-monitor"
	mdaiCollectorDeploymentName = "mdai-collector"
	collectorEnvConfigMapName   = "collector-env"
	otlpGRPCPort                = 4317
	otlpHTTPPort                = 4318
	promHTTPPort                = 8899
	collectorConfigMapName      = "collector"
	hubNameLabel                = "mydecisive.ai/hub-name"
	hubName                     = "mdai-hub"
	serviceAccountName          = "service-account"
	hash                        = "0123456789abcdefghijklmnopqrstuvwxyz"
)

var awsAccessKeySecret = new("aws-access-key")

var cmpContainerOpts = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.SortSlices(func(x, y corev1.EnvFromSource) bool {
		return envFromKey(x) < envFromKey(y)
	}),
}

func envFromKey(e corev1.EnvFromSource) string {
	if e.ConfigMapRef != nil {
		return "cm/" + e.ConfigMapRef.Name + "/" + e.Prefix
	}
	if e.SecretRef != nil {
		return "secret/" + e.SecretRef.Name + "/" + e.Prefix
	}
	return "other/" + e.Prefix
}

func wantContainerBase() corev1.Container {
	return corev1.Container{
		Name:    mdaiCollectorDeploymentName,
		Image:   "public.ecr.aws/decisiveai/mdai-collector:0.1.6",
		Command: []string{"/mdai-collector", "--config=/conf/collector.yaml"},
		Ports: []corev1.ContainerPort{
			{ContainerPort: 4317, Name: "otlp-grpc"},
			{ContainerPort: 4318, Name: "otlp-http"},
			{ContainerPort: 8899, Name: "prom-http"},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "config-volume", MountPath: "/conf/collector.yaml", SubPath: "collector.yaml"},
		},
		EnvFrom: []corev1.EnvFromSource{
			{ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: collectorEnvConfigMapName},
			}},
		},
		SecurityContext: &corev1.SecurityContext{
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			AllowPrivilegeEscalation: new(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			RunAsNonRoot:             new(true),
		},
	}
}

func wantContainerWithSecret(a corev1.Container, name string) corev1.Container {
	a.EnvFrom = append(a.EnvFrom, corev1.EnvFromSource{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
		},
	})
	return a
}

func buildContainerWithBuilder(awsSecret *string) corev1.Container {
	c := Container(mdaiCollectorDeploymentName, "public.ecr.aws/decisiveai/mdai-collector:0.1.6").
		WithCommand("/mdai-collector", "--config=/conf/collector.yaml").
		WithPorts(
			corev1.ContainerPort{ContainerPort: otlpGRPCPort, Name: "otlp-grpc"},
			corev1.ContainerPort{ContainerPort: otlpHTTPPort, Name: "otlp-http"},
			corev1.ContainerPort{ContainerPort: promHTTPPort, Name: "prom-http"},
		).
		WithVolumeMounts(corev1.VolumeMount{
			Name: "config-volume", MountPath: "/conf/collector.yaml", SubPath: "collector.yaml",
		}).
		WithSecurityContext(&corev1.SecurityContext{
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			AllowPrivilegeEscalation: new(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			RunAsNonRoot:             new(true),
		}).
		WithAWSSecret(awsSecret).
		WithEnvFrom(corev1.EnvFromSource{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: collectorEnvConfigMapName},
			},
		})
	return c.Build()
}

func wantServiceBase() *corev1.Service {
	a := &corev1.Service{}
	if a.Labels == nil {
		a.Labels = make(map[string]string)
	}
	a.Labels["app"] = appName
	a.Labels[hubNameLabel] = hubName

	a.Spec = corev1.ServiceSpec{
		Selector: map[string]string{
			"app": appName,
		},
		Ports: []corev1.ServicePort{
			{
				Name:       "otlp-grpc",
				Protocol:   corev1.ProtocolTCP,
				Port:       4317,
				TargetPort: intstr.FromString("otlp-grpc"),
			},
			{
				Name:       "otlp-http",
				Protocol:   corev1.ProtocolTCP,
				Port:       4318,
				TargetPort: intstr.FromString("otlp-http"),
			},
		},
		Type: corev1.ServiceTypeClusterIP,
	}
	return a
}

func buildServiceWithBuilder() *corev1.Service {
	a := &corev1.Service{}
	Service(a).
		WithLabel("app", appName).
		WithSelectorLabel("app", appName).
		WithPorts(
			corev1.ServicePort{Port: 4317, Name: "otlp-grpc", TargetPort: intstr.FromString("otlp-grpc"), Protocol: corev1.ProtocolTCP},
			corev1.ServicePort{Port: 4318, Name: "otlp-http", TargetPort: intstr.FromString("otlp-http"), Protocol: corev1.ProtocolTCP},
		).
		WithType(corev1.ServiceTypeClusterIP)
	return a
}

func wantDeploymentBase(container corev1.Container) *appsv1.Deployment {
	a := &appsv1.Deployment{}
	if a.Labels == nil {
		a.Labels = make(map[string]string)
	}
	a.Labels["app"] = mdaiCollectorDeploymentName
	a.Labels[hubNameLabel] = hubName

	a.Spec.Replicas = new(int32(1))
	if a.Spec.Selector == nil {
		a.Spec.Selector = &metav1.LabelSelector{}
	}
	if a.Spec.Selector.MatchLabels == nil {
		a.Spec.Selector.MatchLabels = make(map[string]string)
	}
	a.Spec.Selector.MatchLabels["app"] = mdaiCollectorDeploymentName

	if a.Spec.Template.Labels == nil {
		a.Spec.Template.Labels = make(map[string]string)
	}
	a.Spec.Template.Labels["app"] = mdaiCollectorDeploymentName
	a.Spec.Template.Labels["app.kubernetes.io/component"] = mdaiCollectorDeploymentName

	if a.Spec.Template.Annotations == nil {
		a.Spec.Template.Annotations = make(map[string]string)
	}
	a.Spec.Template.Annotations["mdai-collector-config/sha256"] = hash

	a.Spec.Template.Spec.ServiceAccountName = serviceAccountName

	a.Spec.Template.Spec.Containers = []corev1.Container{container}
	a.Spec.Template.Spec.Volumes = []corev1.Volume{
		{
			Name: "config-volume",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: collectorConfigMapName,
					},
				},
			},
		},
	}
	a.Spec.Template.Spec.Tolerations = nil

	return a
}

func buildDeploymentWithBuilder(container corev1.Container) *appsv1.Deployment {
	a := &appsv1.Deployment{}
	Deployment(a).
		WithLabel("app", mdaiCollectorDeploymentName).
		WithLabel(hubNameLabel, hubName).
		WithSelectorLabel("app", mdaiCollectorDeploymentName).
		WithTemplateLabel("app", mdaiCollectorDeploymentName).
		WithTemplateLabel("app.kubernetes.io/component", mdaiCollectorDeploymentName).
		WithTemplateAnnotation("mdai-collector-config/sha256", hash).
		WithTolerations(corev1.Toleration{}).
		WithReplicas(1).
		WithServiceAccount(serviceAccountName).
		WithContainers(container).
		WithVolumes(corev1.Volume{
			Name: "config-volume",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: collectorConfigMapName,
					},
				},
			},
		})
	return a
}

func sampleConfig() ConfigBlock {
	return map[string]any{
		"receivers": map[string]any{
			"otlp": map[string]any{
				"protocols": map[string]any{
					"grpc": map[string]any{
						"max_recv_msg_size_mib": 32,
					},
					"http": map[string]any{},
				},
			},
		},
		"processors": map[string]any{},
		"service": map[string]any{
			"pipelines": map[string]any{},
		},
	}
}
