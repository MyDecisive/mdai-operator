package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("MdaiObserver Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		mdaiobserver := &hubv1.MdaiObserver{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind MdaiObserver")
			err := k8sClient.Get(ctx, typeNamespacedName, mdaiobserver)
			if err != nil && errors.IsNotFound(err) {
				resource := &hubv1.MdaiObserver{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &hubv1.MdaiObserver{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance MdaiObserver")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &MdaiObserverReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})

func TestBuildCollectorConfig(t *testing.T) {
	cr := &hubv1.MdaiObserver{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-observer",
			Namespace: "default",
		},
		Spec:   hubv1.MdaiObserverSpec{},
		Status: hubv1.MdaiObserverStatus{},
	}
	observers := []hubv1.Observer{
		{
			Name:                    "obs1",
			LabelResourceAttributes: []string{"label1", "label2"},
		},
	}
	cr.Spec.Observers = observers
	observerResource := hubv1.ObserverResource{}

	scheme := createTestScheme()
	fakeClient := observerFakeClient(scheme, cr)
	recorder := record.NewFakeRecorder(10)

	adapter := NewObserverAdapter(cr, logr.Discard(), fakeClient, recorder, scheme)
	config, err := adapter.getObserverCollectorConfig(observers, observerResource)
	if err != nil {
		t.Fatalf("getObserverCollectorConfig returned error: %v", err)
	}
	if !strings.Contains(config, "obs1") {
		t.Errorf("Expected collector config to contain observer name %q, got: %s", "obs1", config)
	}
}

func observerFakeClient(scheme *runtime.Scheme, cr *hubv1.MdaiObserver) client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cr).
		WithStatusSubresource(cr).
		Build()
}

func TestEnsureObserversSynchronized_WithObservers(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()

	observer := hubv1.Observer{
		Name:                    "observer4",
		LabelResourceAttributes: []string{"service.name", "team", "region"},
		CountMetricName:         new("mdai_observer_four_count_total"),
		BytesMetricName:         new("mdai_observer_four_bytes_total"),
		Filter: &hubv1.ObserverFilter{
			ErrorMode: new("ignore"),
			Logs: &hubv1.ObserverLogsFilter{
				LogRecord: []string{`attributes["log_level"] == "INFO"`},
			},
		},
	}
	observers := []hubv1.Observer{observer}

	mdaiCR := &hubv1.MdaiObserver{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-observer",
			Namespace: "default",
		},
		Spec: hubv1.MdaiObserverSpec{
			Observers: observers,
			ObserverResource: hubv1.ObserverResource{
				Image: "public.ecr.aws/p3k6k6h3/observer-observer:latest",
			},
		},
		Status: hubv1.MdaiObserverStatus{},
	}

	fakeClient := observerFakeClient(scheme, mdaiCR)
	recorder := record.NewFakeRecorder(10)
	adapter := NewObserverAdapter(mdaiCR, logr.Discard(), fakeClient, recorder, scheme)

	// Call ensureSynchronized.
	opResult, err := adapter.ensureSynchronized(ctx)
	if err != nil {
		t.Fatalf("ensureSynchronized returned error: %v", err)
	}
	if opResult != ContinueOperationResult() {
		t.Errorf("expected ContinueOperationResult, got: %v", opResult)
	}

	configMapName := adapter.getScopedObserverResourceName("config")
	cm := &v1core.ConfigMap{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: mdaiCR.Namespace}, cm); err != nil {
		t.Fatalf("failed to get ConfigMap %q: %v", configMapName, err)
	}
	if _, ok := cm.Data["collector.yaml"]; !ok {
		t.Errorf("expected collector.yaml key in ConfigMap data, got: %v", cm.Data)
	}

	deploymentName := mdaiCR.Name + "-" + mdaiObserverResourceBaseName
	deploy := &appsv1.Deployment{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: mdaiCR.Namespace}, deploy); err != nil {
		t.Fatalf("failed to get Deployment %q: %v", deploymentName, err)
	}
	hash, ok := deploy.Spec.Template.Annotations["mdai-collector-config/sha256"]
	if !ok || hash == "" {
		t.Errorf("expected mdai-collector-config/sha256 annotation to be set in Deployment, got: %v", deploy.Spec.Template.Annotations)
	}

	serviceName := mdaiCR.Name + "-" + mdaiObserverResourceBaseName + "-service"
	svc := &v1core.Service{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: mdaiCR.Namespace}, svc); err != nil {
		t.Fatalf("failed to get Service %q: %v", serviceName, err)
	}
	expectedAppLabel := mdaiCR.Name + "-" + mdaiObserverResourceBaseName
	if svc.Spec.Selector["app"] != expectedAppLabel {
		t.Errorf("expected service selector app to be %q, got: %q", expectedAppLabel, svc.Spec.Selector["app"])
	}
	if len(svc.Spec.Ports) != 2 {
		t.Errorf("expected service to have two ports, got %d", len(svc.Spec.Ports))
	} else {
		port := svc.Spec.Ports[0]
		if port.Name != "otlp-grpc" || port.Port != 4317 || port.TargetPort.String() != "otlp-grpc" {
			t.Errorf("unexpected service port configuration: %+v", port)
		}
	}
}

func TestEnsureObserversSynchronized_WithGreptimeDBObserverCopiesSecret(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	t.Setenv(PodNamespaceEnv, "operator-system")

	observer := hubv1.Observer{
		Name:                    "observer-greptimedb",
		TelemetryType:           "traces",
		LabelResourceAttributes: []string{"service.name"},
		AggregationTemporality:  hubv1.AggregationTemporalityDelta,
		MetricsBackend:          "greptimedb",
	}
	observers := []hubv1.Observer{observer}

	mdaiCR := &hubv1.MdaiObserver{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-observer",
			Namespace: "default",
		},
		Spec: hubv1.MdaiObserverSpec{
			Observers: observers,
			ObserverResource: hubv1.ObserverResource{
				Image: "public.ecr.aws/p3k6k6h3/observer-observer:latest",
			},
		},
		Status: hubv1.MdaiObserverStatus{},
	}
	sourceSecret := &v1core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      greptimeDBUsersAuthSecretName,
			Namespace: "operator-system",
		},
		Type: v1core.SecretTypeOpaque,
		Data: map[string][]byte{
			"GREPTIME_HOST":     []byte("greptime.example.com"),
			"GREPTIME_DATABASE": []byte("metrics"),
			"GREPTIME_USER":     []byte("user"),
			"GREPTIME_PASSWD":   []byte("password"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(mdaiCR, sourceSecret).
		WithStatusSubresource(mdaiCR).
		Build()
	recorder := record.NewFakeRecorder(10)
	adapter := NewObserverAdapter(mdaiCR, logr.Discard(), fakeClient, recorder, scheme)

	opResult, err := adapter.ensureSynchronized(ctx)
	if err != nil {
		t.Fatalf("ensureSynchronized returned error: %v", err)
	}
	if opResult != ContinueOperationResult() {
		t.Errorf("expected ContinueOperationResult, got: %v", opResult)
	}

	targetSecret := &v1core.Secret{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: greptimeDBUsersAuthSecretName, Namespace: mdaiCR.Namespace}, targetSecret); err != nil {
		t.Fatalf("failed to get copied GreptimeDB Secret: %v", err)
	}
	if string(targetSecret.Data["GREPTIME_HOST"]) != "greptime.example.com" {
		t.Errorf("expected copied GREPTIME_HOST, got %q", string(targetSecret.Data["GREPTIME_HOST"]))
	}
	if len(targetSecret.OwnerReferences) != 1 || targetSecret.OwnerReferences[0].Name != mdaiCR.Name {
		t.Errorf("expected copied Secret to be owned by MdaiObserver %q, got: %+v", mdaiCR.Name, targetSecret.OwnerReferences)
	}

	deploymentName := mdaiCR.Name + "-" + mdaiObserverResourceBaseName
	deploy := &appsv1.Deployment{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: mdaiCR.Namespace}, deploy); err != nil {
		t.Fatalf("failed to get Deployment %q: %v", deploymentName, err)
	}
	if len(deploy.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected one container, got %d", len(deploy.Spec.Template.Spec.Containers))
	}
	envFrom := deploy.Spec.Template.Spec.Containers[0].EnvFrom
	if len(envFrom) != 1 || envFrom[0].SecretRef == nil || envFrom[0].SecretRef.Name != greptimeDBUsersAuthSecretName {
		t.Errorf("expected collector Deployment to import GreptimeDB Secret via envFrom, got: %+v", envFrom)
	}
}

func TestEnsureObserversSynchronized_WithGreptimeDBObserverSkipsSecretCopyInOperatorNamespace(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	t.Setenv(PodNamespaceEnv, "default")

	mdaiCR := &hubv1.MdaiObserver{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-observer",
			Namespace: "default",
		},
		Spec: hubv1.MdaiObserverSpec{
			Observers: []hubv1.Observer{
				{
					Name:                    "observer-greptimedb",
					TelemetryType:           "traces",
					LabelResourceAttributes: []string{"service.name"},
					AggregationTemporality:  hubv1.AggregationTemporalityDelta,
					MetricsBackend:          "greptimedb",
				},
			},
			ObserverResource: hubv1.ObserverResource{
				Image: "public.ecr.aws/p3k6k6h3/observer-observer:latest",
			},
		},
		Status: hubv1.MdaiObserverStatus{},
	}

	fakeClient := observerFakeClient(scheme, mdaiCR)
	recorder := record.NewFakeRecorder(10)
	adapter := NewObserverAdapter(mdaiCR, logr.Discard(), fakeClient, recorder, scheme)

	opResult, err := adapter.ensureSynchronized(ctx)
	if err != nil {
		t.Fatalf("ensureSynchronized returned error: %v", err)
	}
	if opResult != ContinueOperationResult() {
		t.Errorf("expected ContinueOperationResult, got: %v", opResult)
	}

	secret := &v1core.Secret{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: greptimeDBUsersAuthSecretName, Namespace: mdaiCR.Namespace}, secret)
	if !errors.IsNotFound(err) {
		t.Fatalf("expected GreptimeDB Secret not to be created in operator namespace, got error: %v", err)
	}
}
