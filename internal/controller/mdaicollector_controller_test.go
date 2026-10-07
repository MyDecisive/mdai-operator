package controller

import (
	"context"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("MdaiCollector Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		mdaicollector := &hubv1.MdaiCollector{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind MdaiCollector")
			err := k8sClient.Get(ctx, typeNamespacedName, mdaicollector)
			if err != nil && errors.IsNotFound(err) {
				resource := &hubv1.MdaiCollector{
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
			resource := &hubv1.MdaiCollector{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance MdaiCollector")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &MdaiCollectorReconciler{
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

var _ = Describe("MdaiCollector Controller", func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		mgrStopped chan struct{}
		namespace  = "default"
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext

		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:  scheme.Scheme,
			Metrics: server.Options{BindAddress: "0"},
		})
		Expect(err).NotTo(HaveOccurred())

		reconciler := &MdaiCollectorReconciler{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		}
		Expect(reconciler.SetupWithManager(mgr)).To(Succeed())

		mgrStopped = make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(mgrStopped)
			Expect(mgr.Start(ctx)).To(Succeed())
		}()
	})

	AfterEach(func() {
		cancel()
		// Wait for the manager to shut down so it doesn't overlap with the next spec.
		Eventually(mgrStopped).Should(BeClosed())
	})

	It("should create a Deployment with expected tolerations", func() {
		By("creating an MdaiCollector CR with tolerations")
		cr := &hubv1.MdaiCollector{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-collector",
				Namespace: namespace,
			},
			Spec: hubv1.MdaiCollectorSpec{
				Tolerations: []corev1.Toleration{
					{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "gpu", Effect: corev1.TaintEffectNoSchedule},
				},
			},
		}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		By("verifying that a Deployment was created")
		deploy := &appsv1.Deployment{}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "test-collector-mdai-collector",
				Namespace: namespace,
			}, deploy)).To(Succeed())
		}).Should(Succeed())

		Expect(deploy.Spec.Template.Spec.Tolerations).To(ContainElement(corev1.Toleration{
			Key:      "dedicated",
			Operator: corev1.TolerationOpEqual,
			Value:    "gpu",
			Effect:   corev1.TaintEffectNoSchedule,
		}))

		By("creating an MdaiCollector CR with no tolerations")
		cr = &hubv1.MdaiCollector{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-collector-no-tolerations",
				Namespace: namespace,
			},
			Spec: hubv1.MdaiCollectorSpec{},
		}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())

		By("verifying that a Deployment was created")
		deploy = &appsv1.Deployment{}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "test-collector-no-tolerations-mdai-collector",
				Namespace: namespace,
			}, deploy)).To(Succeed())
		}).Should(Succeed())

		Expect(deploy.Spec.Template.Spec.Tolerations).To(BeEmpty())
	})
})
