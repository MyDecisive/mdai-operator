package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logger "sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *TelemetryValidationReconciler) reconcileShadowCollector(
	ctx context.Context,
	validation *hubv1.TelemetryValidation,
	validatorName string,
	validatorServiceName string,
	resolvedValidatorEndpoint string,
	validatorIngressPortStatus int32,
) (ctrl.Result, error) {
	log := logger.FromContext(ctx)

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

	if !validation.Spec.Enabled {
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
		return ctrl.Result{}, r.Status().Update(ctx, validation)
	}

	shadow := &otelv1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shadowName,
			Namespace: validation.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, shadow, func() error {
		if err := controllerutil.SetControllerReference(validation, shadow, r.Scheme); err != nil {
			return err
		}

		shadow.Labels = mergeMaps(source.Labels, map[string]string{
			LabelManagedByMdaiKey:       LabelManagedByMdaiValue,
			"hub.mydecisive.ai/source":  source.Name,
			telemetryValidationLabelKey: validation.Name,
			"hub.mydecisive.ai/role":    telemetryValidationRoleShadow,
			"hub.mydecisive.ai/shadow":  "true",
		})
		shadow.Annotations = mergeMaps(source.Annotations, map[string]string{
			"hub.mydecisive.ai/shadow": "true",
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
			validation.Spec.ShadowDebugExporterEnabled,
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
	if err := r.Status().Update(ctx, validation); err != nil {
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
