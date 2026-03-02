package e2e

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mydecisive/mdai-operator/internal/controller"
	"github.com/mydecisive/mdai-operator/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

//go:embed testdata/*.yaml
var testdata embed.FS

// namespace where the project is deployed in
const (
	namespace     = "mdai"
	otelNamespace = "otel"
)

// serviceAccountName created for the project
const serviceAccountName = "mdai-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "mdai-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "mdai-operator-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("creating otel namespace")
		cmd = exec.Command("kubectl", "create", "ns", otelNamespace)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("creating the release-info configmap for the controller-manager")
		cmd = exec.Command("kubectl", "create", "configmap", "mdai-operator-release-info",
			"--namespace", namespace,
			"--from-literal=RELEASE_NAME=mdai")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create configmap")

		By("creating a valkey secret for the controller-manager")
		cmd = exec.Command("kubectl", "create", "secret", "generic", "valkey-secret",
			"--namespace", namespace,
			"--from-literal=VALKEY_ENDPOINT=valkey-primary.default.svc.cluster.local:6379",
			"--from-literal=VALKEY_PASSWORD=abc")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create secret")

		By("creating an AWS secret for the mdai-collector")
		cmd = exec.Command("kubectl", "create", "secret", "generic", "awsy-awsface",
			"--namespace", namespace,
			"--from-literal=AWS_ACCESS_KEY_ID=asdfasdfasdfasdfasd",
			"--from-literal=AWS_SECRET_ACCESS_KEY=qwerqwerqwerqwerqwerqwerqw")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create secret")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", "IMG="+projectImage)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)

		By("removing otel namespace")
		cmd = exec.Command("kubectl", "delete", "ns", otelNamespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				GinkgoWriter.Printf("Controller logs:\n %s", controllerLogs)
			} else {
				GinkgoWriter.Printf("Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				GinkgoWriter.Printf("Kubernetes events:\n%s", eventsOutput)
			} else {
				GinkgoWriter.Printf("Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				GinkgoWriter.Printf("Metrics logs:\n %s", metricsOutput)
			} else {
				GinkgoWriter.Printf("Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				GinkgoWriter.Println("Pod description:\n", podDescription)
			} else {
				GinkgoWriter.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=mdai-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("validating that the ServiceMonitor for Prometheus is applied in the namespace")
			cmd = exec.Command("kubectl", "get", "ServiceMonitor", "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "ServiceMonitor should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("waiting for the metrics endpoint to be ready")
			verifyMetricsEndpointReady := func(g Gomega) {
				cmd = exec.Command("kubectl", "get", "endpoints", metricsServiceName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("8443"), "Metrics endpoint is not ready")
			}
			Eventually(verifyMetricsEndpointReady).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring(`"msg":"Serving metrics server"`),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted).Should(Succeed())

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
							"securityContext": {
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccount": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			metricsOutput := getMetricsOutput()
			Expect(metricsOutput).To(ContainSubstring(
				"controller_runtime_reconcile_total",
			))
		})

		It("should provisioned cert-manager", func() {
			By("validating that cert-manager has the certificate Secret")
			verifyCertManager := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "secrets", "webhook-server-cert", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyCertManager).Should(Succeed())
		})

		It("should have CA injection for mutating webhooks", func() {
			By("checking CA injection for mutating webhooks")
			verifyCAInjection := func(g Gomega) {
				cmd := exec.Command("kubectl", "get",
					"mutatingwebhookconfigurations.admissionregistration.k8s.io",
					"mdai-operator-mutating-webhook-configuration",
					"-o", "go-template={{ range .webhooks }}{{ .clientConfig.caBundle }}{{ end }}")
				mwhOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(len(mwhOutput)).To(BeNumerically(">", 10))
			}
			Eventually(verifyCAInjection).Should(Succeed())
		})

		It("should have CA injection for validating webhooks", func() {
			By("checking CA injection for validating webhooks")
			verifyCAInjection := func(g Gomega) {
				cmd := exec.Command("kubectl", "get",
					"validatingwebhookconfigurations.admissionregistration.k8s.io",
					"mdai-operator-validating-webhook-configuration",
					"-o", "go-template={{ range .webhooks }}{{ .clientConfig.caBundle }}{{ end }}")
				vwhOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(len(vwhOutput)).To(BeNumerically(">", 10))
			}
			Eventually(verifyCAInjection).Should(Succeed())
		})

		It("shouldn't cache events for non-managed resources", func() {
			By("Creating a non-managed ConfigMap")
			verifyCmCreate := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/configmap-unmanaged.yaml", "-n", "default")
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyCmCreate).Should(Succeed())

			By("Updating a non-managed ConfigMap")
			verifyCmUpdate := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/configmap-unmanaged-upd.yaml", "-n", "default")
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyCmUpdate).Should(Succeed())

			By("checking manager's logs")
			verifyLogs := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				controllerLogs, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(controllerLogs).NotTo(ContainSubstring("<UpdateFunc> random-configmap shouldReconcile: false"))
			}
			Eventually(verifyLogs, "30s", "10s").Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		It("should reconcile successfully", func() {
			By("applying a MdaiHub CR")
			verifyMdaiHub := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/mdai_v1_mdaihub.yaml", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiHub).Should(Succeed())

			By("checking manager's metrics")
			verifyMetrics := func(g Gomega) {
				metricsOutput := getMetricsOutputFull()
				Expect(metricsOutput).To(SatisfyAny(
					// there is some variability in the numbers of reconciles, so we check for a range
					ContainSubstring(`controller_runtime_reconcile_total{controller="mdaihub",result="success"} 4`),
					ContainSubstring(`controller_runtime_reconcile_total{controller="mdaihub",result="success"} 5`),
				))
			}
			Eventually(verifyMetrics, "30s", "10s").Should(Succeed())
		})

		It("can trigger another reconcile on managed OTEL CR created", func() {
			By("applying a managed OTEL CR")
			verifyMdaiHub := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/collector.yaml", "-n", otelNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiHub).Should(Succeed())
		})

		It("does not trigger another reconcile on unmanaged OTEL CR created", func() {
			By("applying an umanaged OTEL CR")
			verifyMdaiHub := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/collector-unmanaged.yaml", "-n", "default")
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiHub).Should(Succeed())
		})

		It("has the MdaiHub CR in Ready state", func() {
			verifyStatus := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "mdaihub", "mdaihub-sample", "-n", namespace,
					"-o", "jsonpath='{.status.conditions[?(@.type=='Available')].status}'")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("True"))
			}
			Eventually(verifyStatus, "10m", "5s").Should(Succeed())
			metricsOutput := getMetricsOutputFull()
			Expect(metricsOutput).To(SatisfyAny(
				// there is some variability in the numbers of reconciles, so we check for a range
				ContainSubstring(`controller_runtime_reconcile_total{controller="mdaihub",result="success"} 5`),
				ContainSubstring(`controller_runtime_reconcile_total{controller="mdaihub",result="success"} 6`)))
			Expect(metricsOutput).To(ContainSubstring(`controller_runtime_reconcile_errors_total{controller="mdaihub"} 0`))
			Expect(metricsOutput).To(ContainSubstring(`controller_runtime_reconcile_panics_total{controller="mdaihub"} 0`))
		})

		It("should create the config map for variables", func() {
			By("verifying the config map for variables exists")
			configMapExists("mdaihub-sample-variables", otelNamespace)
			configMapExists("mdaihub-sample-manual-variables", namespace)

			By("verifying the config map content for variables defaults")
			verifyConfigMap := func(g Gomega) {
				data := getDataFromMap(g, "mdaihub-sample-variables", otelNamespace)
				g.Expect(data).To(HaveLen(8))
				g.Expect(data["ATTRIBUTES"]).To(Equal("{}\n"))
				g.Expect(data["SEVERITY_FILTERS_BY_LEVEL"]).To(Equal("{}\n"))
				g.Expect(data["SERVICE_LIST_2_CSV"]).To(Equal(""))
				g.Expect(data["SERVICE_LIST_2_REGEX"]).To(Equal(""))
				g.Expect(data["SERVICE_LIST_CSV"]).To(Equal(""))
				g.Expect(data["SERVICE_LIST_REGEX"]).To(Equal(""))
				g.Expect(data["SERVICE_LIST_CSV_MANUAL"]).To(Equal(""))
				g.Expect(data["SERVICE_LIST_REGEX_MANUAL"]).To(Equal(""))
			}
			Eventually(verifyConfigMap).Should(Succeed())

			By("verifying the config map content for manual variables")
			verifyConfigMapManual := func(g Gomega) {
				data := getDataFromMap(g, "mdaihub-sample-manual-variables", namespace)
				g.Expect(data).To(HaveLen(2))
				g.Expect(data["manual_filter"]).To(Equal("string"))
				g.Expect(data["service_list_manual"]).To(Equal("set"))
			}
			Eventually(verifyConfigMapManual).Should(Succeed())
		})

		It("can create the config map for automation", func() {
			verifyConfigMapManual := func(g Gomega) {
				data := getDataFromMap(g, "mdaihub-sample-automation", "mdai")
				g.Expect(data).To(HaveLen(2))
				g.Expect(data["HandleAddNoisyServiceToSet"]).
					To(Equal(`{"name":"HandleAddNoisyServiceToSet","trigger":{"kind":"alert","spec":{"name":"logBytesOutTooHighBySvc"}},"commands":[{"type":"variable.set.add","inputs":{"set":"service_list_1","value":"${trigger:payload.labels.service_name}"}},{"type":"variable.scalar.update","inputs":{"scalar":"severity_number","value":"3"}}]}`))
				g.Expect(data["anomalous_error_rate"]).
					To(Equal(`{"name":"anomalous_error_rate","trigger":{"kind":"alert","spec":{"name":"anomalous_error_rate","status":"firing"}},"commands":[{"type":"webhook.call","inputs":{"url":{"valueFrom":{"secretKeyRef":{"name":"slack-webhook-secret","key":"url"}}},"method":"POST","templateRef":"slackAlertTemplate","templateValues":{"labels_val_ref_primary":"mdai_service","link_text":"See alert in Prometheus","link_url":"http://localhost:9090/alerts","message":"Service was \u003e2x expected error rate for five minutes compared to the last hour!"}}}]}`))
			}
			Eventually(verifyConfigMapManual).Should(Succeed())
		})

		It("can create the config map for manual variables", func() {
			verifyConfigMapManual := func(g Gomega) {
				data := getDataFromMap(g, "mdaihub-sample-manual-variables", namespace)
				g.Expect(data).To(HaveLen(2))
				g.Expect(data).
					To(Equal(map[string]any{"manual_filter": "string", "service_list_manual": "set"}))
			}
			Eventually(verifyConfigMapManual).Should(Succeed())
		})

		It("can reconcile a MDAI Observer CR", func() {
			By("applying a MDAI Observer CR")
			verifyMdaiObserver := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/mdai-observer.yaml", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiObserver).Should(Succeed())
		})

		It("can create the config map for observer", func() {
			configMapExists("mdaihub-sample-mdai-observer-config", namespace)
			yamlStr, err := configMapYAML("mdaihub-sample-mdai-observer-config", namespace)
			Expect(err).NotTo(HaveOccurred())

			var (
				actual   k8sObject
				expected k8sObject
			)

			err = yaml.Unmarshal([]byte(yamlStr), &actual)
			Expect(err).NotTo(HaveOccurred())
			actual.stripVolatile()

			bytes, err := testdata.ReadFile("testdata/observer-configmap.yaml")
			Expect(err).NotTo(HaveOccurred())
			err = yaml.Unmarshal(bytes, &expected)
			Expect(err).NotTo(HaveOccurred())

			Expect(actual).To(Equal(expected))
		})

		It("can deploy the observer", func() {
			verifyObserver := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment", "mdaihub-sample-mdai-observer",
					"-n", namespace,
					"-o", "jsonpath={.status.readyReplicas}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "failed to get observer deployment status")
				g.Expect(out).To(Equal("2"), "observer deployment should have 2 ready replicas")
			}
			Eventually(verifyObserver, "1m", "5s").Should(Succeed())
			verifyObserverPods := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods",
					"-n", namespace,
					"-l", "app=mdaihub-sample-mdai-observer",
					"-o", "jsonpath={range .items[*]}{.metadata.name}:{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)

				g.Expect(err).NotTo(HaveOccurred(), "failed to get observer pods")
				lines := strings.Split(strings.TrimSpace(out), "\n")

				g.Expect(lines).To(HaveLen(2), "expected 2 observer pods")
				for _, line := range lines {
					parts := strings.Split(line, ":")
					g.Expect(parts).To(HaveLen(2), "unexpected pod output format")
					g.Expect(parts[1]).To(Equal("Running"), "expected pod to be in Running state")
				}
			}
			Eventually(verifyObserverPods, "1m", "5s").Should(Succeed())

			By("verifying tolerations are populated properly", func() {
				verifyTolerations := func(g Gomega) {
					cmd := exec.Command(
						"kubectl", "get", "deployment", "mdaihub-sample-mdai-observer",
						"-n", namespace,
						"-o", "jsonpath={.spec.template.spec.tolerations}",
					)

					out, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred(), "failed to get tolerations from observer")

					var tolerations []corev1.Toleration
					g.Expect(json.Unmarshal([]byte(out), &tolerations)).To(Succeed(), "failed to parse tolerations JSON")

					g.Expect(tolerations).To(ContainElement(corev1.Toleration{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "gpu",
						Effect:   corev1.TaintEffectNoSchedule,
					}))
				}

				Eventually(verifyTolerations, "1m", "5s").Should(Succeed())
			})

			verifyObserverLogs := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-l",
					"app=mdaihub-sample-observer-collector", "-o", "jsonpath={.items[*].metadata.name}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())

				podNames := strings.FieldsSeq(out)
				for pod := range podNames {
					if pod == "" {
						continue
					}
					logCmd := exec.Command("kubectl", "logs", pod, "-n", namespace)
					logOut, err := utils.Run(logCmd)
					g.Expect(err).NotTo(HaveOccurred())

					g.Expect(strings.Contains(strings.ToLower(logOut), "error")).To(BeFalse(), "Log for pod %s contains error", pod)
				}
			}
			Eventually(verifyObserverLogs).Should(Succeed())
		})

		It("can reconcile a MDAI Collector CR", func() {
			By("applying a managed OTEL CR")
			verifyMdaiCollector := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/internal-mdai-collector.yaml", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiCollector).Should(Succeed())
		})

		It("can deploy the mdai-collector", func() {
			verifyMdaiCollectorRoleBinding := func(g Gomega) {
				cmd := exec.Command(
					"kubectl",
					"get",
					"clusterrolebinding",
					"-l",
					fmt.Sprintf("%s=%s", controller.HubComponentLabel, controller.MdaiCollectorHubComponent),
				)
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.Contains(out, "internal-mdai-collector-rb")).To(BeTrue())
			}
			Eventually(verifyMdaiCollectorRoleBinding, "1m", "5s").Should(Succeed())

			verifyMdaiCollector := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment", "internal-mdai-collector", "-n", namespace)
				response, err := utils.Run(cmd)
				g.Expect(response).To(ContainSubstring("mdai-collector   1/1"))
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiCollector, "1m", "5s").Should(Succeed())

			verifyMdaiCollectorPods := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-l", "app=internal-mdai-collector")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.Contains(out, "Running")).To(BeTrue())
			}
			Eventually(verifyMdaiCollectorPods, "1m", "5s").Should(Succeed())

			By("verifying tolerations are populated properly", func() {
				verifyTolerations := func(g Gomega) {
					cmd := exec.Command(
						"kubectl", "get", "deployment", "internal-mdai-collector",
						"-n", namespace,
						"-o", "jsonpath={.spec.template.spec.tolerations}",
					)

					out, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred(), "failed to get tolerations from deployment")

					var tolerations []corev1.Toleration
					g.Expect(json.Unmarshal([]byte(out), &tolerations)).To(Succeed(), "failed to parse tolerations JSON")

					g.Expect(tolerations).To(ContainElement(corev1.Toleration{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "gpu",
						Effect:   corev1.TaintEffectNoSchedule,
					}))
				}

				Eventually(verifyTolerations, "1m", "5s").Should(Succeed())
			})

			verifyMdaiCollectorLogs := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-l",
					"app=mdai-collector", "-o", "jsonpath={.items[*].metadata.name}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())

				podNames := strings.FieldsSeq(out)
				for pod := range podNames {
					if pod == "" {
						continue
					}
					logCmd := exec.Command("kubectl", "logs", pod, "-n", namespace)
					logOut, err := utils.Run(logCmd)
					g.Expect(err).NotTo(HaveOccurred())

					g.Expect(strings.Contains(strings.ToLower(logOut), "error")).To(BeFalse(), "Log for pod %s contains error", pod)
				}
			}
			Eventually(verifyMdaiCollectorLogs).Should(Succeed())
		})

		It("can reconcile a MDAI DAL CR", func() {
			By("creating an aws-credentials secret for the MDAI DAL")
			cmd := exec.Command("kubectl", "create", "secret", "generic", "aws-credentials",
				"--namespace", namespace,
				"--from-literal=AWS_ACCESS_KEY_ID=foo",
				"--from-literal=AWS_SECRET_ACCESS_KEY=bar")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create secret")

			By("applying a MDAI DAL CR")
			verifyMdaiDal := func(g Gomega) {
				cmd := exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/mdai-dal.yaml", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiDal).Should(Succeed())
		})

		It("can create the config map for MDAI DAL", func() {
			By("verifying the config map exists")
			configMapExists("mdaidal-sample-mdai-dal-config", namespace)

			By("reading the config map in as YAML")
			yamlStr, err := configMapYAML("mdaidal-sample-mdai-dal-config", namespace)
			Expect(err).NotTo(HaveOccurred())

			By("validating the config map values are correct")
			var (
				actual   k8sObject
				expected k8sObject
			)

			err = yaml.Unmarshal([]byte(yamlStr), &actual)
			Expect(err).NotTo(HaveOccurred())
			actual.stripVolatile()

			bytes, err := testdata.ReadFile("testdata/mdai-dal-configmap.yaml")
			Expect(err).NotTo(HaveOccurred())
			err = yaml.Unmarshal(bytes, &expected)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		})

		It("can deploy the MDAI DAL", func() {
			By("verifying the MDAI DAL deployment is ready")
			verifyMdaiDal := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment", "mdaidal-sample",
					"-n", namespace,
					"-o", "jsonpath={.status.readyReplicas}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "failed to get MDAI DAL deployment status")
				g.Expect(out).To(Equal("1"), "MDAI DAL deployment should have 1 ready replicas")
			}
			Eventually(verifyMdaiDal, "1m", "5s").Should(Succeed())

			By("verifying the service and deployment carry per-instance labels/selectors")
			cmd := exec.Command("kubectl", "get", "service", "mdaidal-sample",
				"-n", namespace,
				"-o", "jsonpath={.spec.selector.app\\.kubernetes\\.io\\/instance}")
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "failed to get MDAI DAL service selector")
			Expect(out).To(Equal("mdaidal-sample"), "service selector should target instance label")

			cmd = exec.Command("kubectl", "get", "deployment", "mdaidal-sample",
				"-n", namespace,
				"-o", "jsonpath={.spec.template.metadata.labels.app\\.kubernetes\\.io\\/instance}")
			out, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "failed to get MDAI DAL deployment labels")
			Expect(out).To(Equal("mdaidal-sample"), "deployment template should include instance label")

			By("verifying the MDAI DAL pods are ready")
			verifyMdaiDalPods := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods",
					"-n", namespace,
					"-l", "app.kubernetes.io/name=mdai-dal",
					"-o", "jsonpath={range .items[*]}{.metadata.name}:{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)

				g.Expect(err).NotTo(HaveOccurred(), "failed to get MDAI DAL pods")
				lines := strings.Split(strings.TrimSpace(out), "\n")

				g.Expect(lines).To(HaveLen(1), "expected 1 MDAI DAL pods")
				for _, line := range lines {
					parts := strings.Split(line, ":")
					g.Expect(parts).To(HaveLen(2), "unexpected pod output format")
					g.Expect(parts[1]).To(Equal("Running"), "expected pod to be in Running state")
				}
			}
			Eventually(verifyMdaiDalPods, "1m", "5s").Should(Succeed())

			By("verifying the MDAL DAL logs do not contain errors")
			verifyMdaiDalLogs := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "-l",
					"app.kubernetes.io/name=mdai-dal", "-o", "jsonpath={.items[*].metadata.name}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())

				podNames := strings.FieldsSeq(out)
				for pod := range podNames {
					if pod == "" {
						continue
					}
					logCmd := exec.Command("kubectl", "logs", pod, "-n", namespace)
					logOut, err := utils.Run(logCmd)
					g.Expect(err).NotTo(HaveOccurred())

					g.Expect(strings.Contains(strings.ToLower(logOut), "error")).To(BeFalse(), "Log for pod %s contains error", pod)
				}
			}
			Eventually(verifyMdaiDalLogs).Should(Succeed())
		})

		It("can create Prometheus rules", func() {
			verifyPrometheusRules := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "prometheusrule", "mdai-mdaihub-sample-alert-rules", "-n", namespace, "-o", "yaml")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())

				var (
					actual   k8sObject
					expected k8sObject
				)

				_ = yaml.Unmarshal([]byte(out), &actual)
				actual.stripVolatile()

				bytes, err := testdata.ReadFile("testdata/prometheus_rule.yaml")
				Expect(err).NotTo(HaveOccurred())
				_ = yaml.Unmarshal(bytes, &expected)
				Expect(actual).To(Equal(expected))
			}
			Eventually(verifyPrometheusRules).Should(Succeed())
		})

		It("can update variable in config map", func() {
			By("executing the valkey-cli command to update the service-list variable")

			initialRevision := getOtelDeploymentRevision("gateway-collector", otelNamespace)

			portForwardCmd := exec.Command("kubectl", "port-forward", "--namespace", "default",
				"svc/valkey-primary", "6379:6379")
			err := portForwardCmd.Start()
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				if portForwardCmd.Process != nil {
					_ = portForwardCmd.Process.Kill()
				}
			}()

			var valkeyClient valkey.Client
			connectToValkey := func(g Gomega) {
				valkeyClient, err = valkey.NewClient(valkey.ClientOption{
					InitAddress: []string{"127.0.0.1:6379"},
					Password:    "abc",
				})
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(connectToValkey, 30*time.Second, 2*time.Second).Should(Succeed())

			ctx := context.Background()

			err = valkeyClient.Do(ctx, valkeyClient.B().Sadd().Key("variable/mdaihub-sample/service_list_1").
				Member("noisy-service").Build()).Error()
			Expect(err).NotTo(HaveOccurred())

			err = valkeyClient.Do(
				ctx,
				valkeyClient.B().Set().
					Key("variable/mdaihub-sample/any_service_alerted").
					Value("true").
					Build(),
			).Error()
			Expect(err).NotTo(HaveOccurred())

			err = valkeyClient.Do(
				ctx,
				valkeyClient.B().Sadd().
					Key("variable/mdaihub-sample/service_list").
					Member("serviceA").
					Build(),
			).Error()
			Expect(err).NotTo(HaveOccurred())

			err = valkeyClient.Do(
				ctx,
				valkeyClient.B().Hset().
					Key("variable/mdaihub-sample/attribute_map").FieldValue().
					FieldValue("send_batch_size", "100").
					FieldValue("timeout", "15s").
					Build(),
			).Error()
			Expect(err).NotTo(HaveOccurred())

			err = valkeyClient.Do(
				ctx,
				valkeyClient.B().Set().
					Key("variable/mdaihub-sample/default").
					Value("default").
					Build(),
			).Error()
			Expect(err).NotTo(HaveOccurred())

			err = valkeyClient.Do(
				ctx,
				valkeyClient.B().Set().
					Key("variable/mdaihub-sample/filter").
					Value(`- severity_number < SEVERITY_NUMBER_WARN
- IsMatch(resource.attributes["service.name"], "${env:SERVICE_LIST}")`).
					Build(),
			).Error()
			Expect(err).NotTo(HaveOccurred())

			err = valkeyClient.Do(
				ctx,
				valkeyClient.B().Set().
					Key("variable/mdaihub-sample/severity_number").
					Value("1").
					Build(),
			).Error()
			Expect(err).NotTo(HaveOccurred())

			err = valkeyClient.Do(
				ctx,
				valkeyClient.B().Hset().
					Key("variable/mdaihub-sample/severity_filters_by_level").FieldValue().
					FieldValue("1", "INFO|WARNING").
					FieldValue("2", "INFO").
					Build(),
			).Error()
			Expect(err).NotTo(HaveOccurred())

			By("validating that the config map has the updated variable value")
			verifyConfigMap := func(g Gomega) {
				data := getDataFromMap(g, "mdaihub-sample-variables", otelNamespace)
				g.Expect(data).To(HaveLen(14))
				g.Expect(data["ATTRIBUTES"]).To(Equal("send_batch_size: 100\ntimeout: 15s\n"))
				g.Expect(data["SERVICE_ALERTED"]).To(Equal("true"))
				g.Expect(data["SERVICE_HASH_SET"]).To(Equal("INFO|WARNING"))
				g.Expect(data["SERVICE_LIST_2_CSV"]).To(Equal(""))
				g.Expect(data["SERVICE_LIST_2_REGEX"]).To(Equal(""))
				g.Expect(data["DEFAULT"]).To(Equal("default"))
				g.Expect(data["FILTER"]).To(Equal("- severity_number < SEVERITY_NUMBER_WARN\n- " +
					"IsMatch(resource.attributes[\"service.name\"], \"${env:SERVICE_LIST}\")"))
				g.Expect(data["SERVICE_LIST_CSV"]).To(Equal("noisy-service"))
				g.Expect(data["SERVICE_LIST_REGEX"]).To(Equal("noisy-service"))
				g.Expect(data["SERVICE_PRIORITY"]).To(Equal("default"))
				g.Expect(data["SEVERITY_FILTERS_BY_LEVEL"]).To(Equal("\"1\": INFO|WARNING\n\"2\": INFO\n"))
				g.Expect(data["SEVERITY_NUMBER"]).To(Equal("1"))
				g.Expect(data["SERVICE_LIST_REGEX_MANUAL"]).To(Equal(""))
				g.Expect(data["SERVICE_LIST_CSV_MANUAL"]).To(Equal(""))
			}
			Eventually(verifyConfigMap).Should(Succeed())

			By("validating that the managed OTel collector was restarted, but unmanaged collector was not")
			verifyOtelDeployment := func(g Gomega) {
				updatedRevision := getOtelDeploymentRevision("gateway-collector", otelNamespace)
				g.Expect(updatedRevision).To(BeNumerically(">", initialRevision))
			}
			Eventually(verifyOtelDeployment).Should(Succeed())

			verifyUnmanagedOtelDeployment := func(g Gomega) {
				updatedRevision := getOtelDeploymentRevision("gateway-unmanaged-collector", "default")
				g.Expect(updatedRevision).To(Equal(1))
			}
			Eventually(verifyUnmanagedOtelDeployment).Should(Succeed())
		})

		It("can delete MdaiHub CRs and clean up resources", func() {
			By("deleting a MdaiHub CR")
			verifyMdaiHub := func(g Gomega) {
				cmd := exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/mdai_v1_mdaihub.yaml", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiHub).Should(Succeed())

			By("validating the config map for observer is still present")
			configMapExists("mdaihub-sample-variables", otelNamespace)

			By("validating the config map for manual variables is deleted")
			configMapDoesNotExist("mdaihub-sample-manual-variables", namespace)

			By("validating the config map for automations is deleted")
			configMapDoesNotExist("mdaihub-sample-automations", namespace)

			By("validating the prometheus rules deleted")
			verifyPromRulesDeleted := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "prometheusrules", "-n", namespace, "-o", "json")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				var result struct {
					Items []any `json:"items"`
				}
				err = json.Unmarshal([]byte(out), &result)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(result.Items).To(BeEmpty())
			}
			Eventually(verifyPromRulesDeleted).Should(Succeed())

			By("validating valkey keys deleted")
			// TODO check valkey store

			By("validating all related observers are not deleted")
			verifyObserver := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment", "mdaihub-sample-mdai-observer",
					"-n", namespace,
					"-o", "jsonpath={.status.readyReplicas}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "failed to get observer deployment status")
				g.Expect(out).To(Equal("2"), "observer deployment should have 2 ready replicas")
			}
			Eventually(verifyObserver, "1m", "5s").Should(Succeed())
			verifyObserverPods := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods",
					"-n", namespace,
					"-l", "app=mdaihub-sample-mdai-observer",
					"-o", "jsonpath={range .items[*]}{.metadata.name}:{.status.phase}{\"\\n\"}{end}")
				out, err := utils.Run(cmd)

				g.Expect(err).NotTo(HaveOccurred(), "failed to get observer pods")
				lines := strings.Split(strings.TrimSpace(out), "\n")

				g.Expect(lines).To(HaveLen(2), "expected 2 observer pods")
				for _, line := range lines {
					parts := strings.Split(line, ":")
					g.Expect(parts).To(HaveLen(2), "unexpected pod output format")
					g.Expect(parts[1]).To(Equal("Running"), "expected pod to be in Running state")
				}
			}
			Eventually(verifyObserverPods, "1m", "5s").Should(Succeed())
		})

		It("can delete MdaiCollector CRs and clean up resources", func() {
			By("deleting a MdaiCollector CR")
			verifyMdaiCollector := func(g Gomega) {
				cmd := exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/internal-mdai-collector.yaml", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiCollector).Should(Succeed())

			By("validating collector deleted")
			verifyCollectorDeleted := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods",
					"-n", namespace,
					"-l", "app=internal-mdai-collector",
					"-o", "json")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				expectedYAML := `
apiVersion: v1
items: []
kind: List
metadata:
  resourceVersion: ""
`
				g.Expect(out).To(MatchYAML(expectedYAML))
			}

			Eventually(verifyCollectorDeleted, "30s", "3s").Should(Succeed(), "expected all mdai collector pods to be deleted")
		})

		It("can delete MdaiObserver CRs and clean up resources", func() {
			By("deleting a MdaiObserver CR")
			verifyMdaiCollector := func(g Gomega) {
				cmd := exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/mdai-observer.yaml", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiCollector).Should(Succeed())

			By("validating observer deleted")

			verifyObserverDeleted := func() error {
				cmd := exec.Command("kubectl", "get", "pods",
					"-n", namespace,
					"-l", "app=mdaihub-sample-mdai-observer",
					"-o", "json")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if strings.Contains(out, `"items": [`) && !strings.Contains(out, `"items": []`) {
					return errors.New("observer pods still present")
				}
				return nil
			}

			Eventually(verifyObserverDeleted, "30s", "3s").Should(Succeed(), "expected all observer pods to be deleted")
		})

		It("can delete MdaiDal CRs and clean up resources", func() {
			By("deleting a MdaiDal CR")
			verifyMdaiCollector := func(g Gomega) {
				cmd := exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/mdai-dal.yaml", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiCollector).Should(Succeed())

			By("validating observer deleted")

			verifyObserverDeleted := func() error {
				cmd := exec.Command("kubectl", "get", "pods",
					"-n", namespace,
					"-l", "app=mdai-dal",
					"-o", "json")
				out, err := utils.Run(cmd)
				if err != nil {
					return err
				}
				if strings.Contains(out, `"items": [`) && !strings.Contains(out, `"items": []`) {
					return errors.New("mdai dal pods still present")
				}
				return nil
			}

			Eventually(verifyObserverDeleted, "30s", "3s").Should(Succeed(), "expected all mdai dal pods to be deleted")
		})

		It("can delete OTEL CRs", func() {
			verifyMdaiHub := func(g Gomega) {
				cmd := exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/collector.yaml", "-n", otelNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyMdaiHub).Should(Succeed())
		})
	})
})

func getDataFromMap(g Gomega, cmName string, namespace string) map[string]any {
	cmd := exec.Command("kubectl", "get", "configmap", cmName,
		"-n", namespace, "-o", "json")
	out, err := utils.Run(cmd)
	g.Expect(err).NotTo(HaveOccurred())

	var cm map[string]any
	err = json.Unmarshal([]byte(out), &cm)
	g.Expect(err).NotTo(HaveOccurred())
	data, ok := cm["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "Expected 'data' field to be a map")
	return data
}

func getOtelDeploymentRevision(deploymentName string, namespace string) int {
	cmd := exec.Command("kubectl", "get", "deployment", deploymentName,
		"-n", namespace,
		"-o", "jsonpath={.metadata.annotations.deployment\\.kubernetes\\.io/revision}")
	out, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	newRevision, err := strconv.Atoi(strings.TrimSpace(out))
	Expect(err).NotTo(HaveOccurred())
	return newRevision
}

func configMapExists(name string, namespace string) {
	verifyConfigMap := func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "configmap", name, "-n", namespace)
		_, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
	}
	Eventually(verifyConfigMap, "1m", "5s").Should(Succeed())
}

func configMapDoesNotExist(name string, namespace string) {
	verifyConfigMap := func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "configmap", name, "-n", namespace)
		_, err := utils.Run(cmd)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err.Error()).To(ContainSubstring("not found"))
	}
	Eventually(verifyConfigMap, "1m", "5s").Should(Succeed())
}

func configMapYAML(name, namespace string) (string, error) {
	cmd := exec.Command("kubectl", "get", "configmap", name,
		"-n", namespace,
		"-o", "yaml")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := serviceAccountName + "-token-request"
	tokenRequestFile := filepath.Join(os.TempDir(), secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() string {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	metricsOutput, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
	Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
	return metricsOutput
}

func getMetricsOutputFull() string {
	By("running curl to get fresh metrics")
	token, err := serviceAccountToken()
	Expect(err).NotTo(HaveOccurred(), "Failed to get service account token")
	cmd := exec.Command("kubectl", "run", "--rm", "-it", "curl-metrics-temp",
		"--restart=Never",
		"--namespace", namespace,
		"--image=curlimages/curl:latest",
		"--overrides="+
			fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
							"securityContext": {
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccount": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName),
	)

	metricsOutput, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to run curl command for metrics")
	return metricsOutput
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
// nolint:revive
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}

type k8sObject map[string]any

func (obj k8sObject) stripVolatile() {
	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		return
	}
	delete(meta, "uid")
	delete(meta, "resourceVersion")
	delete(meta, "creationTimestamp")
	delete(meta, "generation")
	delete(meta, "managedFields")

	if ors, ok := meta["ownerReferences"].([]any); ok {
		for _, r := range ors {
			if m, ok := r.(map[string]any); ok {
				delete(m, "uid")
			}
		}
	}
}
