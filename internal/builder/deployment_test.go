package builder

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func TestDeploymentBuilderEquivalence(t *testing.T) {
	t.Parallel()

	container := wantContainerBase()
	want := wantDeploymentBase(container)
	got := buildDeploymentWithBuilder(container)

	diff := cmp.Diff(want, got)
	require.Empty(t, diff, "Spec differs (-want +got):\n%s", diff)
}

func TestDeploymentBuilderHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func() *appsv1.Deployment
		want  *appsv1.Deployment
	}{
		{
			name: "sets labels selectors template fields containers volumes and tolerations",
			build: func() *appsv1.Deployment {
				dep := &appsv1.Deployment{}
				Deployment(dep).
					WithLabel("app", "demo").
					WithSelectorLabel("sel", "match").
					WithTemplateLabel("pod", "demo").
					WithTemplateAnnotation("anno", "yes").
					WithReplicas(2).
					WithServiceAccount("svcacct").
					WithContainers(corev1.Container{Name: "c"}).
					WithVolumes(corev1.Volume{Name: "vol"}).
					WithTolerations(corev1.Toleration{Key: "taint", Operator: corev1.TolerationOpEqual, Value: "val"})
				return dep
			},
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "demo"},
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"sel": "match"},
					},
					Replicas: new(int32(2)),
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"pod": "demo"},
							Annotations: map[string]string{"anno": "yes"},
						},
						Spec: corev1.PodSpec{
							ServiceAccountName: "svcacct",
							Containers:         []corev1.Container{{Name: "c"}},
							Volumes:            []corev1.Volume{{Name: "vol"}},
							Tolerations: []corev1.Toleration{
								{Key: "taint", Operator: corev1.TolerationOpEqual, Value: "val"},
							},
						},
					},
				},
			},
		},
		{
			name: "tolerations filtered and normalized",
			build: func() *appsv1.Deployment {
				dep := &appsv1.Deployment{}
				Deployment(dep).
					WithTolerations(
						corev1.Toleration{}, // removed
						corev1.Toleration{Operator: corev1.TolerationOpEqual}, // key empty, operator normalized to Exists
						corev1.Toleration{Key: "keep", Operator: corev1.TolerationOpExists},
					)
				return dep
			},
			want: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Tolerations: []corev1.Toleration{
								{Operator: corev1.TolerationOpExists},
								{Key: "keep", Operator: corev1.TolerationOpExists},
							},
						},
					},
				},
			},
		},
		{
			name: "tolerations cleared when all empty",
			build: func() *appsv1.Deployment {
				dep := &appsv1.Deployment{}
				Deployment(dep).
					WithTolerations(corev1.Toleration{})
				return dep
			},
			want: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Tolerations: nil,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.build()
			diff := cmp.Diff(tt.want, got)
			require.Empty(t, diff, "deployment differs (-want +got):\n%s", diff)
		})
	}
}

func TestDeploymentBuilderEquivalenceWithAWSSecret(t *testing.T) {
	t.Parallel()

	container := wantContainerWithSecret(wantContainerBase(), *awsAccessKeySecret)
	want := wantDeploymentBase(container)
	got := buildDeploymentWithBuilder(container)

	diff := cmp.Diff(want, got)
	require.Empty(t, diff, "Spec differs (-want +got):\n%s", diff)
}
