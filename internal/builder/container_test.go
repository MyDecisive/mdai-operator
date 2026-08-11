package builder

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestContainerBuilderEquivalence(t *testing.T) {
	t.Parallel()

	want := wantContainerBase()
	got := buildContainerWithBuilder(nil)

	diff := cmp.Diff(want, got, cmpContainerOpts)
	require.Empty(t, diff, "container differs (-want +got):\n%s", diff)
}

func TestContainerBuilderHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func() corev1.Container
		want  corev1.Container
	}{
		{
			name: "env and envFrom helpers append values",
			build: func() corev1.Container {
				return Container("app", "image").
					WithEnv(
						corev1.EnvVar{Name: "FIRST", Value: "one"},
						corev1.EnvVar{Name: "SECOND", Value: "two"},
					).
					WithEnvFromConfigMap("config").
					WithEnvFromSecret("secret").
					Build()
			},
			want: corev1.Container{
				Name:  "app",
				Image: "image",
				Env: []corev1.EnvVar{
					{Name: "FIRST", Value: "one"},
					{Name: "SECOND", Value: "two"},
				},
				EnvFrom: []corev1.EnvFromSource{
					{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "config"}}},
					{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "secret"}}},
				},
			},
		},
		{
			name: "command ports volumeMounts securityContext and envFrom",
			build: func() corev1.Container {
				return Container("app", "image").
					WithCommand("/bin/app", "--flag").
					WithPorts(
						corev1.ContainerPort{ContainerPort: 80, Name: "http"},
						corev1.ContainerPort{ContainerPort: 443, Name: "https"},
					).
					WithVolumeMounts(corev1.VolumeMount{Name: "cfg", MountPath: "/etc/cfg"}).
					WithSecurityContext(&corev1.SecurityContext{RunAsNonRoot: new(true)}).
					WithEnvFrom(corev1.EnvFromSource{
						ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}},
					}).
					Build()
			},
			want: corev1.Container{
				Name:    "app",
				Image:   "image",
				Command: []string{"/bin/app", "--flag"},
				Ports: []corev1.ContainerPort{
					{ContainerPort: 80, Name: "http"},
					{ContainerPort: 443, Name: "https"},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "cfg", MountPath: "/etc/cfg"},
				},
				SecurityContext: &corev1.SecurityContext{RunAsNonRoot: new(true)},
				EnvFrom: []corev1.EnvFromSource{
					{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}}},
				},
			},
		},
		{
			name: "env from secret key adds SecretKeyRef env var",
			build: func() corev1.Container {
				return Container("app", "image").
					WithEnvFromSecretKey("ENV_NAME", "secret", "key").
					Build()
			},
			want: corev1.Container{
				Name:  "app",
				Image: "image",
				Env: []corev1.EnvVar{
					{
						Name: "ENV_NAME",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "secret"},
								Key:                  "key",
							},
						},
					},
				},
			},
		},
		{
			name: "probes and pull policy are set",
			build: func() corev1.Container {
				liveness := &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8080)},
					},
				}
				readiness := &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(8081)},
					},
				}

				return Container("app", "image").
					WithLivenessProbe(liveness).
					WithReadinessProbe(readiness).
					WithImagePullPolicy(corev1.PullAlways).
					Build()
			},
			want: corev1.Container{
				Name:            "app",
				Image:           "image",
				LivenessProbe:   &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8080)}}},
				ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(8081)}}},
				ImagePullPolicy: corev1.PullAlways,
			},
		},
		{
			name: "env from secret key is no-op when inputs missing",
			build: func() corev1.Container {
				return Container("app", "image").
					WithEnvFromSecretKey("", "secret", "key").
					WithEnvFromSecretKey("ENV_NAME", "", "key").
					WithEnvFromSecretKey("ENV_NAME", "secret", "").
					Build()
			},
			want: corev1.Container{
				Name:  "app",
				Image: "image",
			},
		},
		{
			name: "aws secret helper adds secret when provided",
			build: func() corev1.Container {
				secret := "aws-secret"
				return Container("app", "image").
					WithAWSSecret(&secret).
					Build()
			},
			want: corev1.Container{
				Name:  "app",
				Image: "image",
				EnvFrom: []corev1.EnvFromSource{
					{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "aws-secret"}}},
				},
			},
		},
		{
			name: "aws secret helper is no-op when nil",
			build: func() corev1.Container {
				return Container("app", "image").
					WithAWSSecret(nil).
					Build()
			},
			want: corev1.Container{
				Name:  "app",
				Image: "image",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.build()

			diff := cmp.Diff(tt.want, got, cmpContainerOpts)
			require.Empty(t, diff, "container differs (-want +got):\n%s", diff)
		})
	}
}

func TestContainerBuilderEquivalenceWithAWSSecret(t *testing.T) {
	t.Parallel()

	want := wantContainerWithSecret(wantContainerBase(), *awsAccessKeySecret)
	got := buildContainerWithBuilder(awsAccessKeySecret)

	diff := cmp.Diff(want, got, cmpContainerOpts)
	require.Empty(t, diff, "container differs (-want +got):\n%s", diff)
}
