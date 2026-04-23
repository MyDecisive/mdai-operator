package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logger "sigs.k8s.io/controller-runtime/pkg/log"
)

type TelemetryValidationReconciler struct {
	client.Client

	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hub.mydecisive.ai,resources=telemetryvalidations/finalizers,verbs=update
// +kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors,verbs=get;list;watch;create;update;patch;delete

func (r *TelemetryValidationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)

	var validation hubv1.TelemetryValidation
	if err := r.Get(ctx, req.NamespacedName, &validation); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
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

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, shadow, func() error {
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
		spec.Config = deriveShadowConfig(source.Spec.Config, activeSignals(validation.Spec.Signals), validation.Spec.Validator.Endpoint)
		shadow.Spec = spec
		return nil
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	validation.Status.ShadowCollectorName = shadow.Name
	validation.Status.ShadowServiceName = shadow.Name + "-collector"
	validation.Status.ObservedGeneration = validation.Generation
	validation.Status.ActiveSignals = activeSignals(validation.Spec.Signals)
	setValidationCondition(&validation.Status.Conditions, validation.Generation, metav1.ConditionTrue, "Ready", "Telemetry validation shadow collector is configured")
	if err := r.Status().Update(ctx, &validation); err != nil {
		return ctrl.Result{}, err
	}

	ready, err := r.ensureShadowDeploymentHostAliases(ctx, validation.Namespace, shadow.Name)
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
		Complete(r)
}

func deriveShadowConfig(cfg otelv1beta1.Config, signals []hubv1.TelemetrySignal, validatorEndpoint string) otelv1beta1.Config {
	shadow := *cfg.DeepCopy()
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

		ddExporters := datadogExporters(pipeline.Exporters)
		if len(ddExporters) == 0 {
			continue
		}

		filtered := *pipeline
		filtered.Exporters = ddExporters
		filteredPipelines[name] = &filtered
		for _, exporterName := range ddExporters {
			referencedExporters[exporterName] = struct{}{}
		}
	}
	shadow.Service.Pipelines = filteredPipelines

	exporters := make(map[string]any)
	for exporterName := range referencedExporters {
		if cfgExporter, ok := shadow.Exporters.Object[exporterName]; ok {
			exporters[exporterName] = rewriteDatadogExporter(cfgExporter, validatorEndpoint)
		}
	}
	shadow.Exporters.Object = exporters

	return shadow
}

func rewriteDatadogExporter(raw any, validatorEndpoint string) any {
	cfg, ok := raw.(map[string]any)
	if !ok {
		return raw
	}

	host := datadogSiteForValidator(validatorEndpoint)
	exportURL := datadogExportEndpointForValidator(validatorEndpoint)

	apiCfg, ok := cfg["api"].(map[string]any)
	if !ok {
		apiCfg = map[string]any{}
	}
	apiCfg["site"] = host
	cfg["api"] = apiCfg

	tlsCfg, ok := cfg["tls"].(map[string]any)
	if !ok {
		tlsCfg = map[string]any{}
	}
	tlsCfg["insecure_skip_verify"] = true
	cfg["tls"] = tlsCfg

	for _, key := range []string{"metrics", "logs", "traces"} {
		sub, ok := cfg[key].(map[string]any)
		if !ok {
			sub = map[string]any{}
		}
		sub["endpoint"] = exportURL
		cfg[key] = sub
	}

	return cfg
}

func datadogSiteForValidator(_ string) string {
	return "datadoghq.local"
}

func datadogExportEndpointForValidator(validatorEndpoint string) string {
	if strings.TrimSpace(validatorEndpoint) != "" {
		return validatorEndpoint
	}
	return "http://mdai-fidelity-validator.mdai.svc.cluster.local:8081"
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

func datadogExporters(exporters []string) []string {
	filtered := make([]string, 0, len(exporters))
	for _, exporter := range exporters {
		if strings.HasPrefix(exporter, "datadog") {
			filtered = append(filtered, exporter)
		}
	}
	return filtered
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

func (r *TelemetryValidationReconciler) ensureShadowDeploymentHostAliases(ctx context.Context, namespace, shadowCollector string) (bool, error) {
	validatorService := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: "mdai-fidelity-validator", Namespace: namespace}, validatorService); err != nil {
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
