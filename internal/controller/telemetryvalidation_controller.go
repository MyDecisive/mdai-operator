package controller

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logger "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"
)

type TelemetryValidationReconciler struct {
	client.Client

	Scheme *runtime.Scheme
}

const (
	correlationProcessorName             = "attributes/correlation_id"
	correlationDDTagsProcessorName       = "transform/correlation_ddtags"
	correlationAttributeKey              = "correlation_id"
	correlationHeaderFromCtxKey          = "metadata.x-correlation-id"
	correlationDDTagKey                  = "correlation_id:"
	setDDTagsOnlyStatement               = `set(attributes["ddtags"], Concat(["%s", attributes["%s"]], "")) where attributes["%s"] != nil and attributes["ddtags"] == nil`
	appendDDTagsStatement                = `set(attributes["ddtags"], Concat([attributes["ddtags"], ",", "%s", attributes["%s"]], "")) where attributes["%s"] != nil and attributes["ddtags"] != nil`
	defaultValidatorImage                = "ghcr.io/mydecisive/mdai-fidelity-validator:0.1.0"
	defaultValidatorPort           int32 = 18081
	defaultValidatorReceiverPort   int32 = 8126
	defaultValidatorReplicas       int32 = 1
)

//go:embed config/telemetryvalidation_exporter_rewrites.yaml
var telemetryValidationExporterRewritesYAML string

var (
	exporterRewritesOnce   sync.Once
	cachedExporterRewrites exporterRewriteConfig
)

type exporterRewriteConfig struct {
	Rules []exporterRewriteRule `yaml:"rules"`
}

type exporterRewriteRule struct {
	Name                  string            `yaml:"name"`
	MatchExporterPrefixes []string          `yaml:"match_exporter_prefixes"`
	Set                   map[string]any    `yaml:"set"`
	ReplaceStrings        map[string]string `yaml:"replace_strings"`
}

// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations/finalizers,verbs=update
// +kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

func (r *TelemetryValidationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)

	var validation hubv1.TelemetryValidation
	if err := r.Get(ctx, req.NamespacedName, &validation); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	validatorName, validatorServiceName, resolvedValidatorEndpoint, err := r.reconcileValidator(ctx, &validation)
	if err != nil {
		return ctrl.Result{}, err
	}
	validatorIngressPort, _, err := r.resolveValidatorIngressPorts(ctx, &validation)
	if err != nil {
		return ctrl.Result{}, err
	}
	validatorIngressPortStatus := int32(0)
	if validation.Spec.Enabled {
		validatorIngressPortStatus = validatorIngressPort
	}

	sourceName := validation.Spec.CollectorRef.Name
	sourceKey := types.NamespacedName{Name: sourceName, Namespace: validation.Namespace}
	var source otelv1beta1.OpenTelemetryCollector
	if err := r.Get(ctx, sourceKey, &source); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "unable to fetch referenced OpenTelemetryCollector", "collector", sourceName)
			return ctrl.Result{}, err
		}

		metaCopy := validation.DeepCopy()
		metaCopy.Status.ObservedGeneration = validation.Generation
		setValidationCondition(&metaCopy.Status.Conditions, validation.Generation, metav1.ConditionFalse, "CollectorNotFound", fmt.Sprintf("Referenced collector %q not found", sourceName))
		if statusErr := r.Status().Update(ctx, metaCopy); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{}, nil
	}

	shadowName := shadowCollectorName(source.Name)
	shadowKey := types.NamespacedName{Name: shadowName, Namespace: validation.Namespace}

	if !validation.Spec.Enabled || !validation.Spec.ShadowCollector.Enabled {
		shadow := &otelv1beta1.OpenTelemetryCollector{}
		if err := r.Get(ctx, shadowKey, shadow); err == nil {
			if err := r.Delete(ctx, shadow); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}

		validation.Status.ShadowCollectorName = ""
		validation.Status.ShadowServiceName = ""
		validation.Status.ValidatorName = validatorName
		validation.Status.ValidatorService = validatorServiceName
		validation.Status.ValidatorEndpoint = resolvedValidatorEndpoint
		validation.Status.ValidatorIngressPort = validatorIngressPortStatus
		validation.Status.ObservedGeneration = validation.Generation
		validation.Status.ActiveSignals = activeSignals(validation.Spec.Signals)
		setValidationCondition(&validation.Status.Conditions, validation.Generation, metav1.ConditionFalse, "Disabled", "Telemetry validation shadow collector is disabled")
		return ctrl.Result{}, r.Status().Update(ctx, &validation)
	}

	shadow := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shadowName,
			Namespace: validation.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, shadow, func() error {
		if err := controllerutil.SetControllerReference(&validation, shadow, r.Scheme); err != nil {
			return err
		}

		shadow.Labels = mergeMaps(source.Labels, map[string]string{
			LabelManagedByMdaiKey:      LabelManagedByMdaiValue,
			"hub.mydecisive.ai/source": source.Name,
			"hub.mydecisive.ai/role":   "telemetry-validation-shadow",
			"hub.mydecisive.ai/shadow": "true",
		})
		shadow.Annotations = mergeMaps(source.Annotations, map[string]string{
			"hub.mydecisive.ai/telemetry-validation": validation.Name,
			"hub.mydecisive.ai/shadow":               "true",
		})

		spec := *source.Spec.DeepCopy()
		spec.Config = deriveShadowConfig(
			source.Spec.Config,
			activeSignals(validation.Spec.Signals),
			resolvedValidatorEndpoint,
			validation.Namespace,
			validation.Name,
			source.Name,
			validation.Spec.ExporterRewrites,
		)
		shadow.Spec = spec
		return nil
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	validation.Status.ShadowCollectorName = shadow.Name
	validation.Status.ShadowServiceName = shadow.Name + "-collector"
	validation.Status.ValidatorName = validatorName
	validation.Status.ValidatorService = validatorServiceName
	validation.Status.ValidatorEndpoint = resolvedValidatorEndpoint
	validation.Status.ValidatorIngressPort = validatorIngressPortStatus
	validation.Status.ObservedGeneration = validation.Generation
	validation.Status.ActiveSignals = activeSignals(validation.Spec.Signals)
	setValidationCondition(&validation.Status.Conditions, validation.Generation, metav1.ConditionTrue, "Ready", "Telemetry validation shadow collector is configured")
	if err := r.Status().Update(ctx, &validation); err != nil {
		return ctrl.Result{}, err
	}

	ready, err := r.ensureShadowDeploymentHostAliases(ctx, validation.Namespace, shadow.Name, validatorServiceName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil //nolint:mnd
	}

	return ctrl.Result{}, nil
}

func (r *TelemetryValidationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hubv1.TelemetryValidation{}).
		Owns(&otelv1beta1.OpenTelemetryCollector{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}

func (r *TelemetryValidationReconciler) reconcileValidator(ctx context.Context, validation *hubv1.TelemetryValidation) (string, string, string, error) {
	if !validation.Spec.Enabled {
		if err := r.deleteManagedValidatorResources(ctx, validation); err != nil {
			return "", "", "", err
		}
		return "", "", "", nil
	}

	validatorName := validatorNameForTV(validation.Name)
	validatorServiceName := validatorName
	validatorConfigName := validatorConfigMapNameForTV(validation.Name)
	validatorPort := validatorPort(validation.Spec.Validator.Port)
	validatorIngressPort, validatorIngressPorts, err := r.resolveValidatorIngressPorts(ctx, validation)
	if err != nil {
		return "", "", "", err
	}
	validatorReplicas := validatorReplicas(validation.Spec.Validator.Replicas)
	validatorImage := validatorImage(validation.Spec.Validator.Image)

	cfgMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      validatorConfigName,
			Namespace: validation.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cfgMap, func() error {
		if err := controllerutil.SetControllerReference(validation, cfgMap, r.Scheme); err != nil {
			return err
		}
		cfgMap.Labels = map[string]string{
			LabelManagedByMdaiKey:    LabelManagedByMdaiValue,
			"hub.mydecisive.ai/tv":   validation.Name,
			"hub.mydecisive.ai/role": "telemetry-validation-validator",
		}
		cfgMap.Data = map[string]string{
			"rules.yaml":         validation.Spec.Validator.RulesYAML,
			"field-mapping.yaml": validation.Spec.Validator.FieldMappingYAML,
		}
		return nil
	})
	if err != nil {
		return "", "", "", err
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      validatorServiceName,
			Namespace: validation.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := controllerutil.SetControllerReference(validation, service, r.Scheme); err != nil {
			return err
		}
		service.Labels = map[string]string{
			LabelManagedByMdaiKey:    LabelManagedByMdaiValue,
			"hub.mydecisive.ai/tv":   validation.Name,
			"hub.mydecisive.ai/role": "telemetry-validation-validator",
		}
		service.Spec = corev1.ServiceSpec{
			Selector: map[string]string{
				"app.kubernetes.io/name":     validatorName,
				"app.kubernetes.io/instance": validation.Name,
			},
			Ports: append(
				buildValidatorReceiverServicePorts(validatorIngressPorts, validatorIngressPort),
				corev1.ServicePort{
					Name:       "exporter-intake",
					Port:       validatorPort,
					TargetPort: intstr.FromInt32(validatorPort),
					Protocol:   corev1.ProtocolTCP,
				},
			),
			Type: corev1.ServiceTypeClusterIP,
		}
		return nil
	})
	if err != nil {
		return "", "", "", err
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      validatorName,
			Namespace: validation.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		if err := controllerutil.SetControllerReference(validation, deployment, r.Scheme); err != nil {
			return err
		}
		labels := map[string]string{
			LabelManagedByMdaiKey:        LabelManagedByMdaiValue,
			"hub.mydecisive.ai/tv":       validation.Name,
			"hub.mydecisive.ai/role":     "telemetry-validation-validator",
			"app.kubernetes.io/name":     validatorName,
			"app.kubernetes.io/instance": validation.Name,
		}
		deployment.Labels = labels
		deployment.Spec = appsv1.DeploymentSpec{
			Replicas: &validatorReplicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":     validatorName,
					"app.kubernetes.io/instance": validation.Name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "validator",
							Image:           validatorImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Ports: []corev1.ContainerPort{
								{
									Name:          "receiver-intake",
									ContainerPort: validatorIngressPort,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "exporter-intake",
									ContainerPort: validatorPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							Env: []corev1.EnvVar{
								{Name: "MDAI_DATADOG_AGENT_INGEST_ADDR", Value: fmt.Sprintf(":%d", validatorIngressPort)},
								{Name: "MDAI_EXPORTER_API_ADDR", Value: fmt.Sprintf(":%d", validatorPort)},
								{Name: "MDAI_FIDELITY_RULES_PATH", Value: "/etc/mdai-fidelity-validator/rules.yaml"},
								{Name: "MDAI_FIDELITY_FIELD_MAPPING_PATH", Value: "/etc/mdai-fidelity-validator/field-mapping.yaml"},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "validator-config",
									MountPath: "/etc/mdai-fidelity-validator",
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "validator-config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: validatorConfigName},
								},
							},
						},
					},
				},
			},
		}
		return nil
	})
	if err != nil {
		return "", "", "", err
	}

	return validatorName, validatorServiceName, fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", validatorServiceName, validation.Namespace, validatorPort), nil
}

func (r *TelemetryValidationReconciler) deleteManagedValidatorResources(ctx context.Context, validation *hubv1.TelemetryValidation) error {
	name := validatorNameForTV(validation.Name)
	configName := validatorConfigMapNameForTV(validation.Name)

	for _, obj := range []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: validation.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: validation.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configName, Namespace: validation.Namespace}},
	} {
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func validatorNameForTV(tvName string) string {
	return fmt.Sprintf("%s-fidelity-validator", tvName)
}

func validatorConfigMapNameForTV(tvName string) string {
	return fmt.Sprintf("%s-fidelity-validator-config", tvName)
}

func validatorPort(port int32) int32 {
	if port > 0 {
		return port
	}
	return defaultValidatorPort
}

func validatorReplicas(replicas *int32) int32 {
	if replicas != nil {
		return *replicas
	}
	return defaultValidatorReplicas
}

func validatorImage(image string) string {
	if strings.TrimSpace(image) != "" {
		return image
	}
	return defaultValidatorImage
}

func (r *TelemetryValidationReconciler) resolveValidatorIngressPorts(ctx context.Context, validation *hubv1.TelemetryValidation) (int32, []int32, error) {
	sourceName := strings.TrimSpace(validation.Spec.CollectorRef.Name)
	if sourceName == "" {
		return defaultValidatorReceiverPort, []int32{defaultValidatorReceiverPort}, nil
	}

	var source otelv1beta1.OpenTelemetryCollector
	if err := r.Get(ctx, types.NamespacedName{Name: sourceName, Namespace: validation.Namespace}, &source); err != nil {
		if apierrors.IsNotFound(err) {
			return defaultValidatorReceiverPort, []int32{defaultValidatorReceiverPort}, nil
		}
		return 0, nil, err
	}

	allPorts := extractAllReceiverPorts(source.Spec.Config)
	if len(allPorts) == 0 {
		allPorts = []uint32{uint32(defaultValidatorReceiverPort)}
	}
	preferredPorts := extractPreferredReceiverPorts(source.Spec.Config)
	if len(preferredPorts) == 0 {
		preferredPorts = allPorts
	}

	resolvedPorts := make([]int32, 0, len(allPorts))
	for _, p := range allPorts {
		resolvedPorts = append(resolvedPorts, int32(p))
	}

	return int32(preferredPorts[0]), resolvedPorts, nil
}

func buildValidatorReceiverServicePorts(exposedPorts []int32, targetPort int32) []corev1.ServicePort {
	if len(exposedPorts) == 0 {
		exposedPorts = []int32{targetPort}
	}
	ports := make([]corev1.ServicePort, 0, len(exposedPorts))
	for _, p := range exposedPorts {
		ports = append(ports, corev1.ServicePort{
			Name:       fmt.Sprintf("receiver-%d", p),
			Port:       p,
			TargetPort: intstr.FromInt32(targetPort),
			Protocol:   corev1.ProtocolTCP,
		})
	}
	return ports
}

func extractPreferredReceiverPorts(config otelv1beta1.Config) []uint32 {
	datadogPorts := make([]uint32, 0)
	allPorts := make([]uint32, 0)

	for receiverName, rawReceiver := range config.Receivers.Object {
		receiver, ok := rawReceiver.(map[string]any)
		if !ok {
			continue
		}
		isDatadog := strings.HasPrefix(strings.ToLower(strings.TrimSpace(receiverName)), "datadog")

		receiverPorts := make([]uint32, 0)
		if endpoint, ok := receiver["endpoint"].(string); ok {
			if port := extractPort(endpoint); port != 0 {
				receiverPorts = append(receiverPorts, port)
			}
		}
		if protocols, ok := receiver["protocols"].(map[string]any); ok {
			for _, rawProtocol := range protocols {
				protocol, ok := rawProtocol.(map[string]any)
				if !ok {
					continue
				}
				if endpoint, ok := protocol["endpoint"].(string); ok {
					if port := extractPort(endpoint); port != 0 {
						receiverPorts = append(receiverPorts, port)
					}
				}
			}
		}

		if len(receiverPorts) == 0 {
			continue
		}
		if isDatadog {
			datadogPorts = append(datadogPorts, receiverPorts...)
		}
		allPorts = append(allPorts, receiverPorts...)
	}

	if len(datadogPorts) > 0 {
		return sortAndDedupePorts(datadogPorts)
	}
	return sortAndDedupePorts(allPorts)
}

func extractAllReceiverPorts(config otelv1beta1.Config) []uint32 {
	allPorts := make([]uint32, 0)
	for _, rawReceiver := range config.Receivers.Object {
		receiver, ok := rawReceiver.(map[string]any)
		if !ok {
			continue
		}
		if endpoint, ok := receiver["endpoint"].(string); ok {
			if port := extractPort(endpoint); port != 0 {
				allPorts = append(allPorts, port)
			}
		}
		if protocols, ok := receiver["protocols"].(map[string]any); ok {
			for _, rawProtocol := range protocols {
				protocol, ok := rawProtocol.(map[string]any)
				if !ok {
					continue
				}
				if endpoint, ok := protocol["endpoint"].(string); ok {
					if port := extractPort(endpoint); port != 0 {
						allPorts = append(allPorts, port)
					}
				}
			}
		}
	}
	return sortAndDedupePorts(allPorts)
}

func sortAndDedupePorts(ports []uint32) []uint32 {
	if len(ports) == 0 {
		return ports
	}
	sorted := slices.Clone(ports)
	slices.Sort(sorted)
	deduped := sorted[:1]
	for _, p := range sorted[1:] {
		if p != deduped[len(deduped)-1] {
			deduped = append(deduped, p)
		}
	}
	return deduped
}

func extractPort(addr string) uint32 {
	var port uint32
	_, _ = fmt.Sscanf(addr, "0.0.0.0:%d", &port)
	if port == 0 {
		_, _ = fmt.Sscanf(addr, ":%d", &port)
	}
	if port == 0 {
		_, _ = fmt.Sscanf(addr, "%d", &port)
	}
	return port
}

func deriveShadowConfig(
	cfg otelv1beta1.Config,
	signals []hubv1.TelemetrySignal,
	validatorEndpoint string,
	namespace string,
	validationName string,
	collectorName string,
	tvRules []hubv1.TelemetryValidationExporterRewrite,
) otelv1beta1.Config {
	shadow := *cfg.DeepCopy()
	ensureDatadogReceiversIncludeMetadata(&shadow)
	ensureCorrelationProcessors(&shadow)
	rewriteRules := mergedExporterRewriteRules(tvRules)

	enabledSignals := make(map[hubv1.TelemetrySignal]struct{}, len(signals))
	for _, signal := range signals {
		enabledSignals[signal] = struct{}{}
	}

	filteredPipelines := make(map[string]*otelv1beta1.Pipeline)
	referencedExporters := make(map[string]struct{})
	for name, pipeline := range shadow.Service.Pipelines {
		signal, ok := pipelineSignal(name)
		if !ok {
			continue
		}
		if _, ok := enabledSignals[signal]; !ok {
			continue
		}

		targetExporters := exportersMatchingRewriteRules(pipeline.Exporters, rewriteRules)
		if len(targetExporters) == 0 {
			continue
		}

		filtered := *pipeline
		filtered.Exporters = targetExporters
		filtered.Processors = appendProcessorOnce(filtered.Processors, correlationProcessorName)
		filtered.Processors = appendProcessorOnce(filtered.Processors, correlationDDTagsProcessorName)
		filteredPipelines[name] = &filtered
		for _, exporterName := range targetExporters {
			referencedExporters[exporterName] = struct{}{}
		}
	}
	shadow.Service.Pipelines = filteredPipelines

	exporters := make(map[string]any)
	validatorBase := validatorExportBaseURL(validatorEndpoint)
	templateVars := map[string]string{
		"validator_endpoint":   validatorBase,
		"namespace":            namespace,
		"telemetry_validation": validationName,
		"collector":            collectorName,
	}
	for exporterName := range referencedExporters {
		if cfgExporter, ok := shadow.Exporters.Object[exporterName]; ok {
			perExporterVars := map[string]string{}
			maps.Copy(perExporterVars, templateVars)
			perExporterVars["exporter"] = exporterName
			exporters[exporterName] = rewriteExporterConfig(exporterName, cfgExporter, rewriteRules, perExporterVars)
		}
	}
	shadow.Exporters.Object = exporters

	return shadow
}

func ensureDatadogReceiversIncludeMetadata(cfg *otelv1beta1.Config) {
	if cfg.Receivers.Object == nil {
		return
	}
	for receiverName, raw := range cfg.Receivers.Object {
		if !strings.HasPrefix(receiverName, "datadog") {
			continue
		}
		receiverCfg, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		receiverCfg["include_metadata"] = true
		cfg.Receivers.Object[receiverName] = receiverCfg
	}
}

func ensureCorrelationProcessors(cfg *otelv1beta1.Config) {
	if cfg.Processors == nil {
		cfg.Processors = &otelv1beta1.AnyConfig{Object: map[string]any{}}
	}
	if cfg.Processors.Object == nil {
		cfg.Processors.Object = make(map[string]any)
	}
	cfg.Processors.Object[correlationProcessorName] = map[string]any{
		"actions": []any{
			map[string]any{
				"key":          correlationAttributeKey,
				"action":       "upsert",
				"from_context": correlationHeaderFromCtxKey,
			},
		},
	}

	setDDTagsStatement := fmt.Sprintf(setDDTagsOnlyStatement, correlationDDTagKey, correlationAttributeKey, correlationAttributeKey)
	appendToDDTagsStatement := fmt.Sprintf(appendDDTagsStatement, correlationDDTagKey, correlationAttributeKey, correlationAttributeKey)
	cfg.Processors.Object[correlationDDTagsProcessorName] = map[string]any{
		"trace_statements": []any{
			map[string]any{
				"context":    "span",
				"statements": []any{setDDTagsStatement, appendToDDTagsStatement},
			},
		},
		"log_statements": []any{
			map[string]any{
				"context":    "log",
				"statements": []any{setDDTagsStatement, appendToDDTagsStatement},
			},
		},
		"metric_statements": []any{
			map[string]any{
				"context":    "datapoint",
				"statements": []any{setDDTagsStatement, appendToDDTagsStatement},
			},
		},
	}
}

func appendProcessorOnce(processors []string, processorName string) []string {
	if slices.Contains(processors, processorName) {
		return processors
	}
	return append(processors, processorName)
}

func rewriteExporterConfig(exporterName string, raw any, rules []exporterRewriteRule, templateVars map[string]string) any {
	cfg, ok := raw.(map[string]any)
	if !ok {
		return raw
	}

	for _, rule := range matchingRewriteRules(exporterName, rules) {
		for path, value := range rule.Set {
			setNestedValue(cfg, path, resolveTemplateValues(value, templateVars))
		}
		if len(rule.ReplaceStrings) > 0 {
			applyStringReplacementsRecursive(cfg, rule.ReplaceStrings, templateVars)
		}
	}

	return cfg
}

func validatorExportBaseURL(validatorEndpoint string) string {
	if strings.TrimSpace(validatorEndpoint) != "" {
		return strings.TrimSuffix(validatorEndpoint, "/")
	}
	return "http://mdai-fidelity-validator.mdai.svc.cluster.local:18081"
}

func pipelineSignal(name string) (hubv1.TelemetrySignal, bool) {
	base := strings.SplitN(name, "/", 2)[0] //nolint:mnd
	switch base {
	case string(hubv1.TelemetrySignalMetrics):
		return hubv1.TelemetrySignalMetrics, true
	case string(hubv1.TelemetrySignalLogs):
		return hubv1.TelemetrySignalLogs, true
	case string(hubv1.TelemetrySignalTraces):
		return hubv1.TelemetrySignalTraces, true
	default:
		return "", false
	}
}

func exportersMatchingRewriteRules(exporters []string, rules []exporterRewriteRule) []string {
	filtered := make([]string, 0, len(exporters))
	for _, exporterName := range exporters {
		if len(matchingRewriteRules(exporterName, rules)) > 0 {
			filtered = append(filtered, exporterName)
		}
	}
	return filtered
}

func matchingRewriteRules(exporterName string, rules []exporterRewriteRule) []exporterRewriteRule {
	matched := make([]exporterRewriteRule, 0)
	for _, rule := range rules {
		for _, prefix := range rule.MatchExporterPrefixes {
			if strings.HasPrefix(exporterName, prefix) {
				matched = append(matched, rule)
				break
			}
		}
	}
	return matched
}

func setNestedValue(root map[string]any, dottedPath string, value any) {
	parts := strings.Split(dottedPath, ".")
	if len(parts) == 0 {
		return
	}

	current := root
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[part] = next
		}
		current = next
	}
	current[parts[len(parts)-1]] = value
}

func resolveTemplateValues(value any, vars map[string]string) any {
	switch v := value.(type) {
	case string:
		resolved := v
		for key, replacement := range vars {
			resolved = strings.ReplaceAll(resolved, "{{ "+key+" }}", replacement)
		}
		return resolved
	case map[string]any:
		for key, nested := range v {
			v[key] = resolveTemplateValues(nested, vars)
		}
		return v
	case []any:
		for i := range v {
			v[i] = resolveTemplateValues(v[i], vars)
		}
		return v
	default:
		return value
	}
}

func applyStringReplacementsRecursive(node any, replacements map[string]string, vars map[string]string) {
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			switch castValue := value.(type) {
			case string:
				updated := castValue
				for old, newValue := range replacements {
					updated = strings.ReplaceAll(updated, resolveTemplateValues(old, vars).(string), resolveTemplateValues(newValue, vars).(string))
				}
				typed[key] = updated
			default:
				applyStringReplacementsRecursive(castValue, replacements, vars)
			}
		}
	case []any:
		for _, value := range typed {
			applyStringReplacementsRecursive(value, replacements, vars)
		}
	}
}

func getExporterRewriteConfig() exporterRewriteConfig {
	exporterRewritesOnce.Do(func() {
		cachedExporterRewrites = defaultExporterRewriteConfig()
		loaded := exporterRewriteConfig{}
		if err := yaml.Unmarshal([]byte(telemetryValidationExporterRewritesYAML), &loaded); err == nil && len(loaded.Rules) > 0 {
			for _, rule := range loaded.Rules {
				if len(rule.MatchExporterPrefixes) == 0 {
					continue
				}
				cachedExporterRewrites.Rules = append(cachedExporterRewrites.Rules, rule)
			}
		}
	})
	return cachedExporterRewrites
}

func mergedExporterRewriteRules(tvRules []hubv1.TelemetryValidationExporterRewrite) []exporterRewriteRule {
	defaults := getExporterRewriteConfig().Rules
	if len(tvRules) == 0 {
		return defaults
	}

	defaultByName := make(map[string]exporterRewriteRule, len(defaults))
	defaultOrder := make([]string, 0, len(defaults))
	for _, rule := range defaults {
		if strings.TrimSpace(rule.Name) != "" {
			defaultByName[rule.Name] = rule
			defaultOrder = append(defaultOrder, rule.Name)
		}
	}

	namedOverrides := make(map[string]exporterRewriteRule)
	namedOrder := make([]string, 0)
	unnamed := make([]exporterRewriteRule, 0)

	for _, tvRule := range tvRules {
		converted := exporterRewriteRule{
			Name:                  tvRule.Name,
			MatchExporterPrefixes: append([]string(nil), tvRule.MatchExporterPrefixes...),
			Set:                   mapStringInterface(tvRule.Set),
			ReplaceStrings:        mapStringString(tvRule.ReplaceStrings),
		}
		if len(converted.MatchExporterPrefixes) == 0 {
			continue
		}
		if strings.TrimSpace(converted.Name) == "" {
			unnamed = append(unnamed, converted)
			continue
		}

		if existingDefault, ok := defaultByName[converted.Name]; ok {
			converted = mergeExporterRewriteRule(existingDefault, converted)
		}
		if _, exists := namedOverrides[converted.Name]; !exists {
			namedOrder = append(namedOrder, converted.Name)
		}
		namedOverrides[converted.Name] = converted
	}

	merged := make([]exporterRewriteRule, 0, len(defaults)+len(namedOverrides)+len(unnamed))
	for _, name := range defaultOrder {
		if override, ok := namedOverrides[name]; ok {
			merged = append(merged, override)
			delete(namedOverrides, name)
			continue
		}
		merged = append(merged, defaultByName[name])
	}
	for _, name := range namedOrder {
		if override, ok := namedOverrides[name]; ok {
			merged = append(merged, override)
		}
	}
	merged = append(merged, unnamed...)
	return merged
}

func mergeExporterRewriteRule(base exporterRewriteRule, override exporterRewriteRule) exporterRewriteRule {
	merged := base
	if len(override.MatchExporterPrefixes) > 0 {
		merged.MatchExporterPrefixes = append([]string(nil), override.MatchExporterPrefixes...)
	}
	if override.Name != "" {
		merged.Name = override.Name
	}
	if len(base.Set) > 0 || len(override.Set) > 0 {
		merged.Set = maps.Clone(base.Set)
		if merged.Set == nil {
			merged.Set = map[string]any{}
		}
		for key, value := range override.Set {
			merged.Set[key] = value
		}
	}
	if len(base.ReplaceStrings) > 0 || len(override.ReplaceStrings) > 0 {
		merged.ReplaceStrings = maps.Clone(base.ReplaceStrings)
		if merged.ReplaceStrings == nil {
			merged.ReplaceStrings = map[string]string{}
		}
		for key, value := range override.ReplaceStrings {
			merged.ReplaceStrings[key] = value
		}
	}
	return merged
}

func mapStringInterface(in map[string]apiextensionsv1.JSON) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		if len(value.Raw) == 0 {
			out[key] = nil
			continue
		}
		var decoded any
		if err := json.Unmarshal(value.Raw, &decoded); err != nil {
			out[key] = string(value.Raw)
			continue
		}
		out[key] = decoded
	}
	return out
}

func mapStringString(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func defaultExporterRewriteConfig() exporterRewriteConfig {
	return exporterRewriteConfig{
		Rules: []exporterRewriteRule{
			{
				Name:                  "datadog-default",
				MatchExporterPrefixes: []string{"datadog"},
				Set: map[string]any{
					"api.site":                 "datadoghq.local",
					"tls.insecure_skip_verify": true,
					"metrics.endpoint":         "{{ validator_endpoint }}/intake/exporter/{{ namespace }}/{{ telemetry_validation }}/{{ collector }}/{{ exporter }}",
					"logs.endpoint":            "{{ validator_endpoint }}/intake/exporter/{{ namespace }}/{{ telemetry_validation }}/{{ collector }}/{{ exporter }}",
					"traces.endpoint":          "{{ validator_endpoint }}/intake/exporter/{{ namespace }}/{{ telemetry_validation }}/{{ collector }}/{{ exporter }}",
				},
			},
		},
	}
}

func activeSignals(signals []hubv1.TelemetrySignal) []hubv1.TelemetrySignal {
	if len(signals) == 0 {
		return []hubv1.TelemetrySignal{
			hubv1.TelemetrySignalMetrics,
			hubv1.TelemetrySignalLogs,
			hubv1.TelemetrySignalTraces,
		}
	}

	unique := make([]hubv1.TelemetrySignal, 0, len(signals))
	for _, signal := range signals {
		if !slices.Contains(unique, signal) {
			unique = append(unique, signal)
		}
	}

	return unique
}

func setValidationCondition(conditions *[]metav1.Condition, generation int64, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               typeAvailableHub,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

func mergeMaps(a, b map[string]string) map[string]string {
	if a == nil && b == nil {
		return nil
	}

	out := make(map[string]string, len(a)+len(b))
	maps.Copy(out, a)
	maps.Copy(out, b)
	return out
}

func shadowCollectorName(collectorName string) string {
	return collectorName + "-shadow"
}

func (r *TelemetryValidationReconciler) ensureShadowDeploymentHostAliases(ctx context.Context, namespace, shadowCollector, validatorServiceName string) (bool, error) {
	if strings.TrimSpace(validatorServiceName) == "" {
		return true, nil
	}
	validatorService := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: validatorServiceName, Namespace: namespace}, validatorService); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if validatorService.Spec.ClusterIP == "" || validatorService.Spec.ClusterIP == corev1.ClusterIPNone {
		return false, nil
	}

	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Name: shadowCollector + "-collector", Namespace: namespace}, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	desiredAlias := corev1.HostAlias{
		IP: validatorService.Spec.ClusterIP,
		Hostnames: []string{
			"api.datadoghq.local",
		},
	}

	updated := false
	found := false
	for i := range deployment.Spec.Template.Spec.HostAliases {
		alias := &deployment.Spec.Template.Spec.HostAliases[i]
		if slices.Contains(alias.Hostnames, "api.datadoghq.local") {
			found = true
			if alias.IP != desiredAlias.IP || !sameStrings(alias.Hostnames, desiredAlias.Hostnames) {
				*alias = desiredAlias
				updated = true
			}
		}
	}
	if !found {
		deployment.Spec.Template.Spec.HostAliases = append(deployment.Spec.Template.Spec.HostAliases, desiredAlias)
		updated = true
	}

	if updated {
		if deployment.Spec.Template.Annotations == nil {
			deployment.Spec.Template.Annotations = map[string]string{}
		}
		deployment.Spec.Template.Annotations["hub.mydecisive.ai/validator-hostalias-ip"] = desiredAlias.IP
		if err := r.Update(ctx, deployment); err != nil {
			return false, err
		}
	}

	return true, nil
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, item := range a {
		if !slices.Contains(b, item) {
			return false
		}
	}
	return true
}
