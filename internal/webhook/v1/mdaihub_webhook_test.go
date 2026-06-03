package v1

import (
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	prometheusv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func createSampleMdaiHub() *mdaiv1.MdaiHub {
	storageType := "mdai-valkey"
	var duration1 prometheusv1.Duration = "5m"

	return &mdaiv1.MdaiHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-sample",
			Namespace: "mdai",
			Labels: map[string]string{
				"app.kubernetes.io/name":       "mdai-operator",
				"app.kubernetes.io/managed-by": "kustomize",
			},
		},
		Spec: mdaiv1.MdaiHubSpec{
			Variables: []mdaiv1.Variable{
				{
					Key: "service_list_1",
					SerializeAs: &[]mdaiv1.Serializer{
						{
							Name: "SERVICE_LIST_REGEX",
							Transformers: []mdaiv1.VariableTransformer{
								{
									Type: mdaiv1.TransformerTypeJoin,
									Join: &mdaiv1.JoinTransformer{
										Delimiter: "|",
									},
								},
							},
						},
						{
							Name: "SERVICE_LIST_CSV",
							Transformers: []mdaiv1.VariableTransformer{
								{
									Type: mdaiv1.TransformerTypeJoin,
									Join: &mdaiv1.JoinTransformer{
										Delimiter: ",",
									},
								},
							},
						},
					},
					DataType:    mdaiv1.VariableDataTypeSet,
					StorageType: mdaiv1.VariableStorageType(storageType),
				},
				{
					Key: "service_list_2",
					SerializeAs: &[]mdaiv1.Serializer{
						{
							Name: "SERVICE_LIST_2_REGEX",
							Transformers: []mdaiv1.VariableTransformer{
								{
									Type: mdaiv1.TransformerTypeJoin,
									Join: &mdaiv1.JoinTransformer{
										Delimiter: "|",
									},
								},
							},
						},
						{
							Name: "SERVICE_LIST_2_CSV",
							Transformers: []mdaiv1.VariableTransformer{
								{
									Type: mdaiv1.TransformerTypeJoin,
									Join: &mdaiv1.JoinTransformer{
										Delimiter: ",",
									},
								},
							},
						},
					},
					DataType:    mdaiv1.VariableDataTypeSet,
					StorageType: mdaiv1.VariableStorageType(storageType),
				},
				{
					Key:         "string",
					DataType:    mdaiv1.VariableDataTypeString,
					StorageType: mdaiv1.VariableStorageType(storageType),
					SerializeAs: &[]mdaiv1.Serializer{{Name: "STR"}},
				},
				{
					Key:         "bool",
					DataType:    mdaiv1.VariableDataTypeBoolean,
					StorageType: mdaiv1.VariableStorageType(storageType),
					SerializeAs: &[]mdaiv1.Serializer{{Name: "BOOL"}},
				},
				{
					Key:         "int",
					DataType:    mdaiv1.VariableDataTypeInt,
					StorageType: mdaiv1.VariableStorageType(storageType),
					SerializeAs: &[]mdaiv1.Serializer{{Name: "INT"}},
				},
				{
					Key:         "map",
					DataType:    mdaiv1.VariableDataTypeMap,
					StorageType: mdaiv1.VariableStorageType(storageType),
					SerializeAs: &[]mdaiv1.Serializer{{Name: "MAP"}},
				},
				{
					Key:          "priority_list",
					Type:         mdaiv1.VariableTypeMeta,
					DataType:     mdaiv1.MetaVariableDataTypePriorityList,
					VariableRefs: []string{"ref1", "ref2", "ref3"},
					StorageType:  mdaiv1.VariableStorageType(storageType),
					SerializeAs: &[]mdaiv1.Serializer{
						{
							Name: "PRIORITY_LIST",
							Transformers: []mdaiv1.VariableTransformer{
								{
									Type: mdaiv1.TransformerTypeJoin,
									Join: &mdaiv1.JoinTransformer{
										Delimiter: ",",
									},
								},
							},
						},
					},
				},
				{
					Key:          "hashset",
					Type:         mdaiv1.VariableTypeMeta,
					DataType:     mdaiv1.MetaVariableDataTypeHashSet,
					VariableRefs: []string{"ref1", "ref2"},
					StorageType:  mdaiv1.VariableStorageType(storageType),
					SerializeAs: &[]mdaiv1.Serializer{
						{
							Name: "HASH_SET",
						},
					},
				},
			},
			PrometheusAlerts: []mdaiv1.PrometheusAlert{
				{
					Name:     "logBytesOutTooHighBySvc",
					Expr:     intstr.FromString("increase(mdai_log_bytes_sent_total[1h]) > 100*1024*1024"),
					Severity: "warning",
					For:      &duration1,
				},
			},
			Rules: []mdaiv1.AutomationRule{
				{
					Name: "automation-1",
					When: mdaiv1.When{
						AlertName: ptr.To("logBytesOutTooHighBySvc"),
						Status:    ptr.To("firing"),
					},
					Then: []mdaiv1.Action{
						{
							AddToSet: &mdaiv1.SetAction{
								Set:   "service_list_1",
								Value: "service_name",
							},
						},
					},
				},
			},
		},
	}
}

var _ = Describe("MdaiHub Webhook", func() {
	var (
		obj          *mdaiv1.MdaiHub
		oldObj       *mdaiv1.MdaiHub
		validator    MdaiHubCustomValidator
		defaulter    MdaiHubCustomDefaulter
		actionPath   *field.Path
		knownVarKeys map[string]struct{}
	)

	BeforeEach(func() {
		obj = &mdaiv1.MdaiHub{}
		oldObj = &mdaiv1.MdaiHub{}
		validator = MdaiHubCustomValidator{}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		defaulter = MdaiHubCustomDefaulter{}
		Expect(defaulter).NotTo(BeNil(), "Expected defaulter to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
		actionPath = field.NewPath("spec", "rules").Index(0).Child("then").Index(0)
		knownVarKeys = map[string]struct{}{
			"service_list_1": {},
			"map_1":          {},
			"string_1":       {},
		}
	})

	AfterEach(func() {
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("When creating MdaiHub under Defaulting Webhook", func() {
		// TODO (user): Add logic for defaulting webhooks
		// Example:
		// It("Should apply defaults when a required field is empty", func() {
		//     By("simulating a scenario where defaults should be applied")
		//     obj.SomeFieldWithDefault = ""
		//     By("calling the Default method to apply defaults")
		//     defaulter.Default(ctx, obj)
		//     By("checking that the default values are set")
		//     Expect(obj.SomeFieldWithDefault).To(Equal("default_value"))
		// })
	})

	Context("When creating or updating MdaiHub under Validating Webhook", func() {
		It("Should deny creation if variable keys are duplicated", func() {
			By("simulating an invalid creation scenario")
			obj := createSampleMdaiHub()
			(obj.Spec.Variables)[0].Key = "service_list_2"
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).Error().To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring(`"mdaihub-sample" is invalid: [spec.variables[1].key: Duplicate value: "service_list_2"`)))
		})

		It("Should admit creation if all required fields are present", func() {
			By("simulating a valid creation scenario")
			obj := createSampleMdaiHub()
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail creation if expr does not validate", func() {
			By("simulating an invalid creation scenario")
			obj := createSampleMdaiHub()
			(obj.Spec.PrometheusAlerts)[0].Expr = intstr.FromString("increaser(mdai_log_bytes_sent_total[1h]) > 100*1024*1024")
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring(`parse error: unknown function with name "increaser"`)))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should validate updates correctly", func() {
			By("simulating a valid update scenario")
			oldObj = createSampleMdaiHub()
			obj := createSampleMdaiHub()
			(obj.Spec.Variables)[1].Key = "service_list_3"
			warnings, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail validation if references changed", func() {
			oldObj = createSampleMdaiHub()
			obj := createSampleMdaiHub()
			(obj.Spec.Variables)[6].VariableRefs[0] = "service_list_3"
			warnings, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(MatchError(ContainSubstring(`meta variable references must not change; delete and recreate the variable to update references`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if no references provided for meta variable", func() {
			obj := createSampleMdaiHub()
			(obj.Spec.Variables)[6].VariableRefs = nil
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(MatchError(ContainSubstring(`spec.variables[6].variableRefs: Required value: required for meta variable`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if references provided for non meta variable", func() {
			obj := createSampleMdaiHub()
			(obj.Spec.Variables)[1].VariableRefs = []string{"ref1"}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(MatchError(ContainSubstring(`MdaiHub.hub.mydecisive.ai "mdaihub-sample" is invalid: spec.variables[1].variableRefs: Forbidden: not supported for non-meta variables`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if more than two ref provided for hashmap", func() {
			obj := createSampleMdaiHub()
			(obj.Spec.Variables)[7].VariableRefs = []string{"ref1", "ref2", "ref3"}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(MatchError(ContainSubstring(`MdaiHub.hub.mydecisive.ai "mdaihub-sample" is invalid: spec.variables[7].variableRefs: Invalid value: ["ref1","ref2","ref3"]: Meta HashSet must have exactly 2 elements`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should admit creation when manual variables declare valid defaults per dataType", func() {
			obj := createSampleMdaiHub()
			obj.Spec.Variables[2].Type = mdaiv1.VariableTypeManual
			obj.Spec.Variables[2].Default = &apiextensionsv1.JSON{Raw: []byte(`"hello"`)}
			obj.Spec.Variables[3].Type = mdaiv1.VariableTypeManual
			obj.Spec.Variables[3].Default = &apiextensionsv1.JSON{Raw: []byte(`true`)}
			obj.Spec.Variables[4].Type = mdaiv1.VariableTypeManual
			obj.Spec.Variables[4].Default = &apiextensionsv1.JSON{Raw: []byte(`100`)}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should admit when default has nil Raw (collapses to no-default; apiserver prunes null leaves before admission)", func() {
			obj := createSampleMdaiHub()
			obj.Spec.Variables[2].Type = mdaiv1.VariableTypeManual
			obj.Spec.Variables[2].Default = &apiextensionsv1.JSON{Raw: nil}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if default is declared for a meta variable", func() {
			obj := createSampleMdaiHub()
			obj.Spec.Variables[6].Default = &apiextensionsv1.JSON{Raw: []byte(`["a","b"]`)}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(MatchError(ContainSubstring(`spec.variables[6].default: Forbidden: defaults are only allowed for manual variables`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if default is declared for a computed variable", func() {
			obj := createSampleMdaiHub()
			obj.Spec.Variables[2].Type = mdaiv1.VariableTypeComputed
			obj.Spec.Variables[2].Default = &apiextensionsv1.JSON{Raw: []byte(`"hello"`)}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(MatchError(ContainSubstring(`spec.variables[2].default: Forbidden: defaults are only allowed for manual variables`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if default shape mismatches dataType", func() {
			obj := createSampleMdaiHub()
			obj.Spec.Variables[4].Type = mdaiv1.VariableTypeManual
			obj.Spec.Variables[4].Default = &apiextensionsv1.JSON{Raw: []byte(`1.5`)}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(MatchError(ContainSubstring(`spec.variables[4].default: Invalid value: "1.5"`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if float default is NaN/Inf", func() {
			obj := createSampleMdaiHub()
			obj.Spec.Variables[2].Type = mdaiv1.VariableTypeManual
			obj.Spec.Variables[2].DataType = mdaiv1.VariableDataTypeFloat
			obj.Spec.Variables[2].Default = &apiextensionsv1.JSON{Raw: []byte(`1e400`)}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(MatchError(ContainSubstring(`spec.variables[2].default`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if exported variable name is duplicated", func() {
			obj := createSampleMdaiHub()
			(*(obj.Spec.Variables)[7].SerializeAs)[0].Name = "SERVICE_LIST_CSV"
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(MatchError(ContainSubstring(`MdaiHub.hub.mydecisive.ai "mdaihub-sample" is invalid: spec.variables[7].serializeAs[0].name: Duplicate value: "SERVICE_LIST_CSV"`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if transformers specified for boolean", func() {
			obj := createSampleMdaiHub()
			(*(obj.Spec.Variables)[3].SerializeAs)[0].Transformers = []mdaiv1.VariableTransformer{
				{
					Type: mdaiv1.TransformerTypeJoin,
					Join: &mdaiv1.JoinTransformer{
						Delimiter: "|",
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(MatchError(ContainSubstring(`MdaiHub.hub.mydecisive.ai "mdaihub-sample" is invalid: spec.variables[3].serializeAs[0].transformers: Forbidden: transformers are not supported for variable type boolean`)))
			Expect(warnings).To(BeEmpty())
		})

		It("Should fail if status is set without alertName", func() {
			By("setting status but clearing alertName in a rule")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].When.AlertName = nil

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("alertName and status must be set together"))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail if updateType is set without variableUpdated", func() {
			By("setting updateType but leaving variableUpdated empty")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].When.UpdateType = ptr.To("added")

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.rules[0].when.updateType"))
			Expect(err.Error()).To(ContainSubstring("can only be set when variableUpdated is set"))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail if variableUpdated is not defined in spec.variables", func() {
			By("referencing an unknown variable in when.variableUpdated")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].When.VariableUpdated = ptr.To("does_not_exist")
			obj.Spec.Rules[0].When.UpdateType = ptr.To("set")

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.rules[0].when.variableUpdated"))
			Expect(err.Error()).To(ContainSubstring("not defined in spec.variables"))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail if alertName is not defined in spec.alerts", func() {
			By("referencing an unknown alert in when.alertName")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].When.AlertName = ptr.To("UnknownAlert")
			// keep status to trigger the existence check
			obj.Spec.Rules[0].When.Status = ptr.To("firing")

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.rules[0].when.alertName"))
			Expect(err.Error()).To(ContainSubstring("not defined in spec.prometheusAlerts"))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail if an action item has no fields set", func() {
			By("providing an empty action (no addToSet/removeFromSet/callWebhook)")
			obj := createSampleMdaiHub()
			// Replace the default valid action with an empty one
			obj.Spec.Rules[0].Then = []mdaiv1.Action{{}}

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.rules[0].then[0]"))
			Expect(err.Error()).To(ContainSubstring("at least one action must be specified"))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail if addToSet targets an unknown variable", func() {
			By("pointing addToSet.set to a variable that does not exist")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then[0].AddToSet.Set = "does_not_exist"

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.rules[0].then[0].addToSet.set"))
			Expect(err.Error()).To(ContainSubstring("not defined in spec.variables"))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should admit creation when removeFromSet references an existing set variable", func() {
			By("switching to removeFromSet with a valid set variable")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then = []mdaiv1.Action{
				{
					RemoveFromSet: &mdaiv1.SetAction{
						Set:   "service_list_1",
						Value: "service_name",
					},
				},
			}

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail if removeFromSet targets an unknown variable", func() {
			By("using removeFromSet on a non-existent variable")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then = []mdaiv1.Action{
				{
					RemoveFromSet: &mdaiv1.SetAction{
						Set:   "ghost_variable",
						Value: "service_name",
					},
				},
			}

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.rules[0].then[0].removeFromSet.set"))
			Expect(err.Error()).To(ContainSubstring("not defined in spec.variables"))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail if callWebhook.url is an explicit empty string", func() {
			By("setting url to an empty string")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then = []mdaiv1.Action{{CallWebhook: &mdaiv1.CallWebhookAction{}}}
			obj.Spec.Rules[0].Then[0].CallWebhook.URL.Value = ptr.To("")

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`spec.rules[0].then[0].callWebhook.url`))
			Expect(err.Error()).To(ContainSubstring(`Required value: required`))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail if callWebhook.url is only whitespace", func() {
			By("setting url to whitespace")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then = []mdaiv1.Action{{CallWebhook: &mdaiv1.CallWebhookAction{}}}
			obj.Spec.Rules[0].Then[0].CallWebhook.URL.Value = ptr.To("   \t  ")

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`spec.rules[0].then[0].callWebhook.url`))
			Expect(err.Error()).To(ContainSubstring(`Required value`))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should fail if callWebhook.url is not an absolute http(s) URL", func() {
			By("using a non-absolute/non-http URL")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then = []mdaiv1.Action{{CallWebhook: &mdaiv1.CallWebhookAction{}}}
			obj.Spec.Rules[0].Then[0].CallWebhook.URL.Value = ptr.To("example.com/hook") // missing scheme

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`spec.rules[0].then[0].callWebhook.url`))
			Expect(err.Error()).To(ContainSubstring(`must be an absolute http(s) URL`))
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should admit creation when callWebhook has a valid absolute URL and allowed method", func() {
			By("providing a proper https URL and a supported method")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then = []mdaiv1.Action{{CallWebhook: &mdaiv1.CallWebhookAction{}}}
			obj.Spec.Rules[0].Then[0].CallWebhook.URL.Value = ptr.To("https://hooks.example.com/x")
			obj.Spec.Rules[0].Then[0].CallWebhook.Method = "PUT" // allowed

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should admit creation when callWebhook has a valid absolute URL and empty method (defaults to POST)", func() {
			By("supplying a valid URL without method")
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then = []mdaiv1.Action{{CallWebhook: &mdaiv1.CallWebhookAction{}}}
			obj.Spec.Rules[0].Then[0].CallWebhook.URL.Value = ptr.To("http://example.com/hook")

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should admit creation when callWebhook.url comes from a Secret", func() {
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then = []mdaiv1.Action{
				{
					CallWebhook: &mdaiv1.CallWebhookAction{
						URL: mdaiv1.StringOrFrom{
							ValueFrom: &mdaiv1.ValueFromSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: "webhook-secret"},
									Key:                  "url",
								},
							},
						},
					},
				},
			}

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("Should admit creation when callWebhook.url comes from a ConfigMap", func() {
			obj := createSampleMdaiHub()
			obj.Spec.Rules[0].Then = []mdaiv1.Action{
				{
					CallWebhook: &mdaiv1.CallWebhookAction{
						URL: mdaiv1.StringOrFrom{
							ValueFrom: &mdaiv1.ValueFromSource{
								ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: "webhook-config"},
									Key:                  "url",
								},
							},
						},
						Method: "PATCH",
					},
				},
			}

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(Equal(admission.Warnings{}))
		})

		It("passes with exactly one action specified (addToSet)", func() {
			action := mdaiv1.Action{
				AddToSet: &mdaiv1.SetAction{
					Set:   "service_list_1",
					Value: "val1",
				},
			}

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(BeEmpty(),
				"single valid action should not trigger multi/zero-action errors")
		})

		It("passes with exactly one action specified (removeFromSet)", func() {
			action := mdaiv1.Action{
				RemoveFromSet: &mdaiv1.SetAction{
					Set:   "service_list_1",
					Value: "valX",
				},
			}

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(BeEmpty())
		})

		It("fails validation if no action is specified", func() {
			action := mdaiv1.Action{} // all nil

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Type).To(Equal(field.ErrorTypeInvalid))
			Expect(errs[0].Field).To(Equal(actionPath.String()))
			Expect(errs[0].BadValue).To(Equal("<action>"))
			Expect(errs[0].Detail).To(Equal("at least one action must be specified"))
		})

		It("should fail validation if an action specifies both addToSet and removeFromSet", func() {
			action := mdaiv1.Action{
				AddToSet: &mdaiv1.SetAction{
					Set:   "service_list_1",
					Value: "val1",
				},
				RemoveFromSet: &mdaiv1.SetAction{
					Set:   "service_list_1",
					Value: "val2",
				},
			}

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Type).To(Equal(field.ErrorTypeInvalid))
			Expect(errs[0].Field).To(Equal(actionPath.String()))
			Expect(errs[0].BadValue).To(Equal("<action>"))
			Expect(errs[0].Detail).To(Equal("only one action may be specified"))
		})

		It("should pass validation when a single 'setVariable' action is specified", func() {
			knownVarKeys := map[string]struct{}{"int": {}}
			action := mdaiv1.Action{
				SetVariable: &mdaiv1.ScalarAction{
					Scalar: "int",
					Value:  "123",
				},
			}
			actionPath := field.NewPath("spec", "rules").Index(0).Child("then").Index(0)

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(BeEmpty())
		})

		It("should pass validation when a single 'addToMap' action is specified", func() {
			action := mdaiv1.Action{
				AddToMap: &mdaiv1.MapAction{
					Map:   "map",
					Key:   "new-key",
					Value: ptr.To("new-value"),
				},
			}
			knownVars := map[string]struct{}{
				"map": {},
			}
			actionPath := field.NewPath("spec").Child("rules").Index(0).Child("then").Index(0)

			errs := validateAction(actionPath, action, knownVars)
			Expect(errs).To(BeEmpty())
		})

		It("should pass validation when a single 'removeFromMap' action is specified", func() {
			knownVarKeys := map[string]struct{}{"map": {}}
			action := mdaiv1.Action{
				RemoveFromMap: &mdaiv1.MapAction{
					Map: "map",
					Key: "some-key",
				},
			}
			actionPath := field.NewPath("spec", "rules").Index(0).Child("then").Index(0)

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(BeEmpty())
		})

		It("should pass validation when a single 'callWebhook' action is specified", func() {
			action := mdaiv1.Action{
				CallWebhook: &mdaiv1.CallWebhookAction{
					URL:    mdaiv1.StringOrFrom{Value: ptr.To("http://example.com/webhook")},
					Method: "POST",
				},
			}
			knownVarKeys := map[string]struct{}{}
			actionPath := field.NewPath("spec", "rules").Index(0).Child("then").Index(0)

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(BeEmpty())
		})

		It("Should return an error when the variable key is an empty string and not in the known keys map", func() {
			variableKey := ""
			childPath := "set"
			expectedErrs := field.ErrorList{
				field.Invalid(actionPath.Child(childPath), variableKey, "not defined in spec.variables"),
			}

			errs := validateVariableAction(actionPath, variableKey, knownVarKeys, childPath)

			Expect(errs).To(ConsistOf(expectedErrs))
		})

		It("Should return nil when the variable key exists in a map containing multiple known keys", func() {
			variableKey := "service_list_1"
			childPath := "set"

			errs := validateVariableAction(actionPath, variableKey, knownVarKeys, childPath)

			Expect(errs).To(BeNil())
		})

		It("Should fail validation for an 'addToMap' action if the specified map variable is not defined", func() {
			action := &mdaiv1.MapAction{
				Map:   "unknown_map",
				Key:   "some_key",
				Value: ptr.To("some_value"),
			}
			path := actionPath.Child("addToMap")
			errs := validateMapAction(path, action, knownVarKeys)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Type).To(Equal(field.ErrorTypeInvalid))
			Expect(errs[0].Field).To(Equal("spec.rules[0].then[0].addToMap.map"))
			Expect(errs[0].Detail).To(ContainSubstring("not defined in spec.variables"))
		})

		It("Should pass validation for a 'removeFromMap' action when a value is provided", func() {
			action := mdaiv1.Action{
				RemoveFromMap: &mdaiv1.MapAction{
					Map:   "map_1",
					Key:   "some-key",
					Value: ptr.To("some-value"),
				},
			}
			path := actionPath.Child("removeFromMap")
			errs := validateAction(path, action, knownVarKeys)
			Expect(errs).To(BeEmpty())
		})

		It("should pass validation when a single valid 'deployReplay' action is specified", func() {
			action := mdaiv1.Action{
				DeployReplay: &mdaiv1.DeployReplayAction{
					ReplaySpec: mdaiv1.MdaiReplaySpec{
						StatusVariableRef: "string_1",
						OpAMPEndpoint:     "http://opamp.example.com",
						Source: mdaiv1.MdaiReplaySourceConfiguration{
							AWSConfig: &mdaiv1.MdaiReplayAwsConfig{
								AWSAccessKeySecret: ptr.To("secret"),
							},
							S3: &mdaiv1.MdaiReplayS3Configuration{
								S3Region:    "region",
								S3Bucket:    "bucket",
								FilePrefix:  "prefix",
								S3Path:      "path",
								S3Partition: mdaiv1.S3ReplayMinutePartition,
							},
						},
						Destination: mdaiv1.MdaiReplayDestinationConfiguration{
							OtlpHttp: &mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
								Endpoint: "http://otlp.example.com",
							},
						},
					},
				},
			}
			actionPath := field.NewPath("spec", "rules").Index(0).Child("then").Index(0)

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(BeEmpty())
		})

		It("should fail validation when a single invalid 'deployReplay' action is specified", func() {
			action := mdaiv1.Action{
				DeployReplay: &mdaiv1.DeployReplayAction{
					ReplaySpec: mdaiv1.MdaiReplaySpec{
						StatusVariableRef: "string_1",
						Source: mdaiv1.MdaiReplaySourceConfiguration{
							AWSConfig: &mdaiv1.MdaiReplayAwsConfig{
								AWSAccessKeySecret: ptr.To("secret"),
							},
							S3: &mdaiv1.MdaiReplayS3Configuration{
								S3Region:    "region",
								S3Bucket:    "bucket",
								FilePrefix:  "prefix",
								S3Path:      "path",
								S3Partition: mdaiv1.S3ReplayMinutePartition,
							},
						},
						Destination: mdaiv1.MdaiReplayDestinationConfiguration{
							OtlpHttp: &mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
								Endpoint: "http://otlp.example.com",
							},
						},
					},
				},
			}
			actionPath := field.NewPath("spec", "rules").Index(0).Child("then").Index(0)

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Type).To(Equal(field.ErrorTypeInvalid))
			Expect(errs[0].Field).To(Equal("spec.rules[0].then[0].deployReplay.replaySpec"))
			Expect(errs[0].Detail).To(ContainSubstring("invalid replay spec"))
		})

		It("should fail validation when a single invalid 'deployReplay' action with unknown var ref is specified", func() {
			action := mdaiv1.Action{
				DeployReplay: &mdaiv1.DeployReplayAction{
					ReplaySpec: mdaiv1.MdaiReplaySpec{
						StatusVariableRef: "adlsfjlskadjflkadsasfjalskdjf",
						OpAMPEndpoint:     "http://opamp.example.com",
						Source: mdaiv1.MdaiReplaySourceConfiguration{
							AWSConfig: &mdaiv1.MdaiReplayAwsConfig{
								AWSAccessKeySecret: ptr.To("secret"),
							},
							S3: &mdaiv1.MdaiReplayS3Configuration{
								S3Region:    "region",
								S3Bucket:    "bucket",
								FilePrefix:  "prefix",
								S3Path:      "path",
								S3Partition: mdaiv1.S3ReplayMinutePartition,
							},
						},
						Destination: mdaiv1.MdaiReplayDestinationConfiguration{
							OtlpHttp: &mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
								Endpoint: "http://otlp.example.com",
							},
						},
					},
				},
			}
			actionPath := field.NewPath("spec", "rules").Index(0).Child("then").Index(0)

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Type).To(Equal(field.ErrorTypeInvalid))
			Expect(errs[0].Field).To(Equal("spec.rules[0].then[0].deployReplay.replaySpec.statusVariableRef"))
			Expect(errs[0].Detail).To(ContainSubstring("does not reference"))
		})

		It("should pass validation when a single 'cleanUpReplay' action is specified", func() {
			action := mdaiv1.Action{
				CleanUpReplay: &mdaiv1.CleanUpReplayAction{},
			}
			actionPath := field.NewPath("spec", "rules").Index(0).Child("then").Index(0)

			errs := validateAction(actionPath, action, knownVarKeys)
			Expect(errs).To(BeEmpty())
		})

		Context("validateMapAction", func() {
			It("Should fail validation for an 'addToMap' action when the value is nil", func() {
				action := &mdaiv1.MapAction{
					Map: "map_1",
				}
				path := actionPath.Child("addToMap")
				errs := validateMapAction(path, action, knownVarKeys)
				Expect(errs).To(HaveLen(1))
				Expect(errs[0].Type).To(Equal(field.ErrorTypeRequired))
				Expect(errs[0].Field).To(Equal("spec.rules[0].then[0].addToMap.value"))
				Expect(errs[0].Detail).To(Equal("required for addToMap"))
			})
		})

		It("Should return an error when the object passed to the defaulter is not an MdaiHub type", func() {
			By("creating an object of a different type")
			notAnMdaiHub := &corev1.Pod{}

			By("calling the Default method with the wrong object type")
			err := defaulter.Default(admission.NewContextWithRequest(ctx, admission.Request{}), notAnMdaiHub)

			By("checking that an error is returned")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("expected an MdaiHub object but got *v1.Pod"))
		})

		It("Should not return an error when a valid MdaiHub object is passed", func() {
			By("creating a valid MdaiHub object")
			obj = createSampleMdaiHub()

			By("calling the Default method")
			err := defaulter.Default(ctx, obj)

			By("checking that no error is returned")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Should not return an error when a valid MdaiHub object is passed", func() {
			By("creating a valid MdaiHub object")
			obj = createSampleMdaiHub()

			By("calling the Default method")
			err := defaulter.Default(ctx, obj)

			By("checking that no error is returned")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Should return an error when the object passed to ValidateDelete is not an MdaiHub type", func() {
			By("creating a non-MdaiHub object")
			invalidObj := &corev1.Pod{}

			By("calling ValidateDelete with the invalid object")
			warnings, err := validator.ValidateDelete(ctx, invalidObj)

			By("checking that an error is returned and warnings are nil")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("expected a MdaiHub object but got *v1.Pod"))
			Expect(warnings).To(BeNil())
		})

		It("Should not return an error when a valid MdaiHub object is passed to ValidateDelete", func() {
			By("creating a valid MdaiHub object for deletion")
			obj = createSampleMdaiHub()

			By("calling ValidateDelete")
			warnings, err := validator.ValidateDelete(ctx, obj)

			By("checking that no error or warnings are returned")
			Expect(err).ToNot(HaveOccurred())
			Expect(warnings).To(BeNil())
		})

		It("accepts same case variable names against knownVarKeys", func() {
			known := map[string]struct{}{"highRiskStates": {}}
			errs := validateVariableAction(field.NewPath("then").Index(0).Child("addToSet"), "highRiskStates", known, "set")
			Expect(errs).To(BeEmpty())
		})

		It("disallows unsupported HTTP methods for callWebhook", func() {
			action := mdaiv1.Action{
				CallWebhook: &mdaiv1.CallWebhookAction{
					URL: mdaiv1.StringOrFrom{
						ValueFrom: &mdaiv1.ValueFromSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "webhook-config"},
								Key:                  "url",
							},
						},
					},
					Method: "not_allowed_methods",
				},
			}
			path := actionPath.Child("callWebhook")

			errs := validateWebhookCall(path, action.CallWebhook)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Type).To(Equal(field.ErrorTypeNotSupported))
			Expect(errs[0].Field).To(Equal("spec.rules[0].then[0].callWebhook.method"))
			Expect(errs[0].BadValue).To(Equal("not_allowed_methods"))
		})

		It("requires POST when payloadTemplate is provided and method is explicitly set", func() {
			action := mdaiv1.Action{
				CallWebhook: &mdaiv1.CallWebhookAction{
					URL: mdaiv1.StringOrFrom{
						ValueFrom: &mdaiv1.ValueFromSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "webhook-config"},
								Key:                  "url",
							},
						},
					},
					Method: "PUT", // anything non-POST should fail when payloadTemplate is present
					PayloadTemplate: &mdaiv1.StringOrFrom{
						Value: ptr.To("{{ .metadata.name }}"),
					},
				},
			}
			path := actionPath.Child("callWebhook")

			errs := validateWebhookCall(path, action.CallWebhook)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Type).To(Equal(field.ErrorTypeInvalid))
			Expect(errs[0].Field).To(Equal("spec.rules[0].then[0].callWebhook.method"))
			Expect(errs[0].BadValue).To(Equal("PUT"))
			Expect(errs[0].Detail).To(Equal("payloadTemplate requires POST"))
		})

		It("allows payloadTemplate with POST or with default (empty) method", func() {
			// Explicit POST
			actionPost := mdaiv1.Action{
				CallWebhook: &mdaiv1.CallWebhookAction{
					URL: mdaiv1.StringOrFrom{
						ValueFrom: &mdaiv1.ValueFromSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "webhook-config"},
								Key:                  "url",
							},
						},
					},
					Method: "POST",
					PayloadTemplate: &mdaiv1.StringOrFrom{
						Value: ptr.To(`{"ok":true}`),
					},
				},
			}
			path := actionPath.Child("callWebhook")
			Expect(validateWebhookCall(path, actionPost.CallWebhook)).To(BeEmpty())

			// Empty method (validator only enforces POST when method is non-empty)
			actionEmpty := mdaiv1.Action{
				CallWebhook: &mdaiv1.CallWebhookAction{
					URL: mdaiv1.StringOrFrom{
						ValueFrom: &mdaiv1.ValueFromSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "webhook-config"},
								Key:                  "url",
							},
						},
					},
					// Method empty on purpose
					PayloadTemplate: &mdaiv1.StringOrFrom{
						Value: ptr.To(`{"ok":true}`),
					},
				},
			}
			Expect(validateWebhookCall(path, actionEmpty.CallWebhook)).To(BeEmpty())
		})

		It("forbids payloadTemplate when templateRef=slackAlertTemplate", func() {
			action := mdaiv1.Action{
				CallWebhook: &mdaiv1.CallWebhookAction{
					URL: mdaiv1.StringOrFrom{
						ValueFrom: &mdaiv1.ValueFromSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "webhook-config"},
								Key:                  "url",
							},
						},
					},
					Method:      "POST",
					TemplateRef: mdaiv1.TemplateRefSlack,
					PayloadTemplate: &mdaiv1.StringOrFrom{
						Value: ptr.To("{{ .metadata.name }}"),
					},
				},
			}
			path := actionPath.Child("callWebhook")

			errs := validateWebhookCall(path, action.CallWebhook)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Type).To(Equal(field.ErrorTypeForbidden))
			Expect(errs[0].Field).To(Equal("spec.rules[0].then[0].callWebhook.payloadTemplate"))
			Expect(errs[0].Detail).To(Equal("must be empty when templateRef=slackAlertTemplate"))
		})

		It("requires payloadTemplate when templateRef=jsonTemplate", func() {
			action := mdaiv1.Action{
				CallWebhook: &mdaiv1.CallWebhookAction{
					URL: mdaiv1.StringOrFrom{
						ValueFrom: &mdaiv1.ValueFromSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "webhook-config"},
								Key:                  "url",
							},
						},
					},
					Method:      "POST",
					TemplateRef: "jsonTemplate",
					// PayloadTemplate nil on purpose
				},
			}
			path := actionPath.Child("callWebhook")

			errs := validateWebhookCall(path, action.CallWebhook)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Type).To(Equal(field.ErrorTypeRequired))
			Expect(errs[0].Field).To(Equal("spec.rules[0].then[0].callWebhook.payloadTemplate"))
			Expect(errs[0].Detail).To(Equal("required when templateRef=jsonTemplate"))
		})

		It("forbids managed headers regardless of input case (canonicalizes detail)", func() {
			action := mdaiv1.Action{
				CallWebhook: &mdaiv1.CallWebhookAction{
					URL: mdaiv1.StringOrFrom{
						ValueFrom: &mdaiv1.ValueFromSource{
							ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "webhook-config"},
								Key:                  "url",
							},
						},
					},
					Method: "POST",
					Headers: map[string]string{
						"content-length": "123", // lower-case on purpose
					},
				},
			}
			path := actionPath.Child("callWebhook")

			errs := validateWebhookCall(path, action.CallWebhook)
			Expect(errs).NotTo(BeEmpty())

			// Find the forbidden header error for the exact map key we set
			expectedField := path.Child("headers").Key("content-length").String()
			found := false
			for _, e := range errs {
				if e.Type == field.ErrorTypeForbidden && e.Field == expectedField {
					Expect(e.Detail).To(ContainSubstring(`header "Content-Length" is managed by the client and cannot be set`))
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected forbidden error for Content-Length header")
		})
	})
})
