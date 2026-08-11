package v1

import (
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var _ = Describe("MdaiObserver Webhook", func() {
	var (
		obj       *mdaiv1.MdaiObserver
		oldObj    *mdaiv1.MdaiObserver
		validator MdaiObserverCustomValidator
	)

	BeforeEach(func() {
		obj = &mdaiv1.MdaiObserver{}
		oldObj = &mdaiv1.MdaiObserver{}
		validator = MdaiObserverCustomValidator{}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
		// TODO (user): Add any setup logic common to all tests
	})

	AfterEach(func() {
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("When creating or updating MdaiObserver under Validating Webhook", func() {
		It("Should deny creation if a required field is missing", func() {
			By("simulating an invalid creation scenario")
			obj = createObserver()
			obj.Spec.ObserverResource.Resources = nil
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(warnings).To(Equal(admission.Warnings{
				"ObserverResource test-observer does not define resource requests/limits",
			}))
			Expect(err).Error().To(Not(HaveOccurred()))
		})

		It("Should admit creation if all required fields are present", func() {
			By("simulating a valid creation scenario")
			obj = createObserver()
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(warnings).To(Equal(admission.Warnings{}))
			Expect(err).ToNot(HaveOccurred())
		})

		It("Should validate updates correctly", func() {
			By("simulating a valid update scenario")
			obj = createObserver()
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(warnings).To(Equal(admission.Warnings{}))
			Expect(err).ToNot(HaveOccurred())
		})
	})
})

func createObserver() *mdaiv1.MdaiObserver {
	return &mdaiv1.MdaiObserver{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-observer",
			Namespace: "default",
		},
		Spec: mdaiv1.MdaiObserverSpec{
			Observers: []mdaiv1.Observer{
				{
					Name:                    "watcher1",
					LabelResourceAttributes: []string{"service.name"},
					CountMetricName:         new("mdai_watcher_one_count_total"),
					BytesMetricName:         new("mdai_watcher_one_bytes_total"),
				},
				{
					Name:                    "watcher2",
					LabelResourceAttributes: []string{"team", "log_level"},
					CountMetricName:         new("mdai_watcher_two_count_total"),
				},
				{
					Name:                    "watcher3",
					LabelResourceAttributes: []string{"region", "log_level"},
					BytesMetricName:         new("mdai_watcher_three_count_total"),
				},
				{
					Name:                    "watcher4",
					LabelResourceAttributes: []string{"service.name", "team", "region"},
					CountMetricName:         new("mdai_watcher_four_count_total"),
					BytesMetricName:         new("mdai_watcher_four_bytes_total"),
					Filter: &mdaiv1.ObserverFilter{
						ErrorMode: new("ignore"),
						Logs: &mdaiv1.ObserverLogsFilter{
							LogRecord: []string{`attributes["log_level"] == "INFO"`},
						},
					},
				},
			},
			ObserverResource: mdaiv1.ObserverResource{
				Image:    "watcher-image:9.9.9",
				Replicas: int32(3),
				Resources: &corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						"Cpu":    resource.MustParse("500m"),
						"Memory": resource.MustParse("1Gi"),
					},
					Requests: corev1.ResourceList{
						"Cpu":    resource.MustParse("200m"),
						"Memory": resource.MustParse("256Mi"),
					},
				},
			},
		},
	}
}
