package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logger "sigs.k8s.io/controller-runtime/pkg/log"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
)

func (r *TelemetryValidationReconciler) reconcileShadowCollector(
	ctx context.Context,
	validation *hubv1.TelemetryValidation,
	validatorName string,
	validatorServiceName string,
	resolvedValidatorEndpoint string,
	validatorIngressPortStatus int32,
	runID string,
) (OperationResult, error) {
	log := logger.FromContext(ctx)

	sourceName := validation.Spec.CollectorRef.Name
	sourceKey := types.NamespacedName{Name: sourceName, Namespace: validation.Namespace}
	var source otelv1beta1.OpenTelemetryCollector
	if err := r.Get(ctx, sourceKey, &source); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "unable to fetch referenced OpenTelemetryCollector", "collector", sourceName)
			return ContinueWithError(err)
		}

		metaCopy := validation.DeepCopy()
		metaCopy.Status.ObservedGeneration = validation.Generation
		metaCopy.Status.RunID = runID
		setValidationCondition(
			&metaCopy.Status.Conditions,
			validation.Generation,
			metav1.ConditionFalse,
			"CollectorNotFound",
			fmt.Sprintf("Referenced collector %q not found", sourceName),
		)
		if statusErr := r.Status().Update(ctx, metaCopy); statusErr != nil {
			return ContinueWithError(statusErr)
		}
		return ContinueProcessing()
	}

	shadowName := shadowCollectorName(source.Name)
	shadowKey := types.NamespacedName{Name: shadowName, Namespace: validation.Namespace}

	if !validation.Spec.Enabled {
		shadow := &otelv1beta1.OpenTelemetryCollector{}
		if err := r.Get(ctx, shadowKey, shadow); err == nil {
			if err := r.Delete(ctx, shadow); err != nil && !apierrors.IsNotFound(err) {
				return ContinueWithError(err)
			}
		}

		metaCopy := validation.DeepCopy()
		metaCopy.Status.ShadowCollectorName = ""
		metaCopy.Status.ShadowServiceName = ""
		metaCopy.Status.ValidatorName = validatorName
		metaCopy.Status.ValidatorService = validatorServiceName
		metaCopy.Status.ValidatorEndpoint = resolvedValidatorEndpoint
		metaCopy.Status.ValidatorIngressPort = validatorIngressPortStatus
		metaCopy.Status.ObservedGeneration = validation.Generation
		metaCopy.Status.RunID = runID
		metaCopy.Status.ActiveSignals = activeSignals(validation.Spec.Signals)
		setValidationCondition(
			&metaCopy.Status.Conditions,
			validation.Generation,
			metav1.ConditionFalse,
			"Disabled",
			"Telemetry validation shadow collector is disabled",
		)
		if err := r.Status().Update(ctx, metaCopy); err != nil {
			return ContinueWithError(err)
		}
		return ContinueProcessing()
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

		if shadow.Labels == nil {
			shadow.Labels = map[string]string{}
		}
		maps.Copy(shadow.Labels, source.Labels)
		maps.Copy(shadow.Labels, map[string]string{
			LabelManagedByMdaiKey:       LabelManagedByMdaiValue,
			"hub.mydecisive.ai/source":  source.Name,
			telemetryValidationLabelKey: validation.Name,
			"hub.mydecisive.ai/role":    telemetryValidationRoleShadow,
			"hub.mydecisive.ai/shadow":  "true",
		})

		if shadow.Annotations == nil {
			shadow.Annotations = map[string]string{}
		}
		maps.Copy(shadow.Annotations, source.Annotations)
		maps.Copy(shadow.Annotations, map[string]string{
			"hub.mydecisive.ai/shadow":            "true",
			telemetryValidationRunIDAnnotationKey: runID,
		})

		spec := *source.Spec.DeepCopy()
		spec.Config = deriveShadowConfig(shadowConfigParams{
			Config:                     source.Spec.Config,
			Signals:                    activeSignals(validation.Spec.Signals),
			ValidatorEndpoint:          resolvedValidatorEndpoint,
			Namespace:                  validation.Namespace,
			ValidationName:             validation.Name,
			CollectorName:              source.Name,
			ExporterRewriteRules:       validation.Spec.ExporterRewrites,
			ShadowDebugExporterEnabled: validation.Spec.ShadowDebugExporterEnabled,
		})
		shadow.Spec = spec
		return nil
	})
	if err != nil {
		return ContinueWithError(err)
	}

	metaCopy := validation.DeepCopy()
	metaCopy.Status.ShadowCollectorName = shadow.Name
	metaCopy.Status.ShadowServiceName = shadow.Name + "-collector"
	metaCopy.Status.ValidatorName = validatorName
	metaCopy.Status.ValidatorService = validatorServiceName
	metaCopy.Status.ValidatorEndpoint = resolvedValidatorEndpoint
	metaCopy.Status.ValidatorIngressPort = validatorIngressPortStatus
	metaCopy.Status.ObservedGeneration = validation.Generation
	metaCopy.Status.RunID = runID
	metaCopy.Status.ActiveSignals = activeSignals(validation.Spec.Signals)
	setValidationCondition(
		&metaCopy.Status.Conditions,
		validation.Generation,
		metav1.ConditionTrue,
		"Ready",
		"Telemetry validation shadow collector is configured",
	)
	if err := r.Status().Update(ctx, metaCopy); err != nil {
		return ContinueWithError(err)
	}

	return ContinueProcessing()
}

func shadowCollectorName(collectorName string) string {
	return collectorName + "-shadow"
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
	apimeta.SetStatusCondition(conditions, metav1.Condition{
		Type:               typeAvailableHub,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}
