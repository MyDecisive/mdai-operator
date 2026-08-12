package v1

import (
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func createTestMdaiCollector() *mdaiv1.MdaiCollector {
	return &mdaiv1.MdaiCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-collector",
			Namespace: "mdai",
			Labels: map[string]string{
				"app.kubernetes.io/name":       "test-collector",
				"app.kubernetes.io/managed-by": "kustomize",
			},
		},
		Spec: mdaiv1.MdaiCollectorSpec{
			AWSConfig: &mdaiv1.AWSConfig{
				AWSAccessKeySecret: new("foobar"),
			},
			Logs: &mdaiv1.LogsConfig{
				S3: &mdaiv1.S3LogsConfig{
					S3Region: "asdf",
					S3Bucket: "qwer",
				},
			},
		},
	}
}

var _ = Describe("MdaiCollector Webhook", func() {
	var (
		obj       *mdaiv1.MdaiCollector
		oldObj    *mdaiv1.MdaiCollector
		validator MdaiCollectorCustomValidator
	)

	BeforeEach(func() {
		obj = &mdaiv1.MdaiCollector{}
		oldObj = &mdaiv1.MdaiCollector{}
		validator = MdaiCollectorCustomValidator{}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
		// TODO (user): Add any setup logic common to all tests
	})

	AfterEach(func() {
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("When creating or updating MdaiCollector under Validating Webhook", func() {
		It("Should deny creation if name is funky", func() {
			By("simulating an invalid creation scenario")
			obj := createTestMdaiCollector()
			obj.Name = "what/do"
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
		})

		It("Should deny creation if name is something else funky", func() {
			By("simulating an invalid creation scenario")
			obj := createTestMdaiCollector()
			obj.Name = "whoa?what?"
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
		})

		It("Should deny creation if a required AWSConfig field is missing but s3 config is present", func() {
			By("simulating an invalid creation scenario")
			obj := createTestMdaiCollector()
			obj.Spec.AWSConfig = nil
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
		})

		It("Should deny update if a required AWSConfig field is missing but s3 config is present", func() {
			By("simulating an invalid creation scenario")
			oldObj := createTestMdaiCollector()
			newObj := createTestMdaiCollector()
			newObj.Spec.AWSConfig = nil
			_, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).To(HaveOccurred())
		})

		It("Should admit creation if all required fields are present when AWS config is not relevant", func() {
			By("simulating a valid creation scenario")
			obj := createTestMdaiCollector()
			obj.Spec.AWSConfig = nil
			obj.Spec.Logs.S3 = nil
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should validate updates correctly when AWS config is not relevant", func() {
			By("simulating a valid update scenario")
			oldObj := createTestMdaiCollector()
			newObj := createTestMdaiCollector()
			newObj.Spec.AWSConfig = nil
			newObj.Spec.Logs.S3 = nil
			warnings, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should admit creation but warn if logs config is absent", func() {
			By("simulating a valid creation scenario")
			obj := createTestMdaiCollector()
			obj.Spec.Logs = nil
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{
				"logs configuration not present in MDAI Collector spec",
			}))
		})

		It("Should validate updates but warn if logs config is not present", func() {
			By("simulating a valid update scenario")
			oldObj := createTestMdaiCollector()
			newObj := createTestMdaiCollector()
			newObj.Spec.Logs = nil
			warnings, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{
				"logs configuration not present in MDAI Collector spec",
			}))
		})

		It("Should admit creation if all required fields are present", func() {
			By("simulating a valid creation scenario")
			obj := createTestMdaiCollector()
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should validate updates correctly", func() {
			By("simulating a valid update scenario")
			oldObj := createTestMdaiCollector()
			newObj := createTestMdaiCollector()
			newObj.Spec.AWSConfig.AWSAccessKeySecret = new("barbaz")
			warnings, err := validator.ValidateUpdate(ctx, oldObj, newObj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})
	})
})
