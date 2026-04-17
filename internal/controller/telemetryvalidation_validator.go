package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	otelv1beta1 "github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	prometheusv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

//nolint:revive // function-result-limit: returning these values avoids an extra struct in hot reconcile flow.
func (r *TelemetryValidationReconciler) reconcileValidator(
	ctx context.Context,
	validation *hubv1.TelemetryValidation,
) (string, string, string, error) {
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
	validatorRulesYAML, validatorFieldMappingYAML := resolveValidatorConfigYAMLs(validation)
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
			LabelManagedByMdaiKey:       LabelManagedByMdaiValue,
			telemetryValidationLabelKey: validation.Name,
			"hub.mydecisive.ai/role":    telemetryValidationRoleValidator,
		}
		cfgMap.Data = map[string]string{
			"rules.yaml":         validatorRulesYAML,
			"field-mapping.yaml": validatorFieldMappingYAML,
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
			LabelManagedByMdaiKey:       LabelManagedByMdaiValue,
			telemetryValidationLabelKey: validation.Name,
			"hub.mydecisive.ai/role":    telemetryValidationRoleValidator,
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
				corev1.ServicePort{
					Name:       otelMetricsName,
					Port:       otelMetricsPort,
					TargetPort: intstr.FromString(otelMetricsName),
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

	monitor := &prometheusv1.ServiceMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      validatorMetricsMonitorNameForTV(validation.Name),
			Namespace: validation.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, monitor, func() error {
		if err := controllerutil.SetControllerReference(validation, monitor, r.Scheme); err != nil {
			return err
		}

		mdaiConnectionSourceLabel := prometheusv1.LabelName("__meta_kubernetes_service_label_hub_mydecisive_ai_telemetry_validation")
		monitor.Labels = map[string]string{
			LabelManagedByMdaiKey:       LabelManagedByMdaiValue,
			telemetryValidationLabelKey: validation.Name,
			"hub.mydecisive.ai/role":    telemetryValidationRoleValidator,
		}
		monitor.Spec = prometheusv1.ServiceMonitorSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					telemetryValidationLabelKey: validation.Name,
					"hub.mydecisive.ai/role":    telemetryValidationRoleValidator,
				},
			},
			NamespaceSelector: prometheusv1.NamespaceSelector{
				MatchNames: []string{validation.Namespace},
			},
			Endpoints: []prometheusv1.Endpoint{
				{
					Port:        otelMetricsName,
					Path:        "/metrics",
					HonorLabels: true,
					RelabelConfigs: []prometheusv1.RelabelConfig{
						{
							SourceLabels: []prometheusv1.LabelName{mdaiConnectionSourceLabel},
							TargetLabel:  "mdai_connection",
							Action:       "replace",
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
			telemetryValidationLabelKey:  validation.Name,
			"hub.mydecisive.ai/role":     telemetryValidationRoleValidator,
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
								{
									Name:          otelMetricsName,
									ContainerPort: otelMetricsPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							Env: []corev1.EnvVar{
								{Name: "MDAI_DATADOG_AGENT_INGEST_ADDR", Value: fmt.Sprintf(":%d", validatorIngressPort)},
								{Name: "MDAI_EXPORTER_API_ADDR", Value: fmt.Sprintf(":%d", validatorPort)},
								{Name: "MDAI_FIDELITY_RULES_PATH", Value: "/etc/mdai-fidelity-validator/rules.yaml"},
								{Name: "MDAI_FIDELITY_FIELD_MAPPING_PATH", Value: "/etc/mdai-fidelity-validator/field-mapping.yaml"},
								{Name: "MDAI_CONNECTION_NAME", Value: validation.Name},
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

	//nolint:revive // in-cluster validator endpoint is HTTP by design.
	return validatorName, validatorServiceName, fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", validatorServiceName, validation.Namespace, validatorPort), nil
}

func (r *TelemetryValidationReconciler) deleteManagedValidatorResources(ctx context.Context, validation *hubv1.TelemetryValidation) error {
	name := validatorNameForTV(validation.Name)
	configName := validatorConfigMapNameForTV(validation.Name)
	monitorName := validatorMetricsMonitorNameForTV(validation.Name)

	for _, obj := range []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: validation.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: validation.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configName, Namespace: validation.Namespace}},
		&prometheusv1.ServiceMonitor{ObjectMeta: metav1.ObjectMeta{Name: monitorName, Namespace: validation.Namespace}},
	} {
		if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func validatorNameForTV(tvName string) string {
	return tvName + "-fidelity-validator"
}

func validatorConfigMapNameForTV(tvName string) string {
	return tvName + "-fidelity-validator-config"
}

func validatorMetricsMonitorNameForTV(tvName string) string {
	return tvName + "-fidelity-validator-metrics"
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

//nolint:revive // confusing-results: paired YAML string return is intentional and localized.
func resolveValidatorConfigYAMLs(validation *hubv1.TelemetryValidation) (string, string) {
	rulesYAML := strings.TrimSpace(validation.Spec.Validator.RulesYAML)
	fieldMappingYAML := strings.TrimSpace(validation.Spec.Validator.FieldMappingYAML)

	if rulesYAML == "" {
		rulesYAML = strings.TrimSpace(telemetryValidationValidatorRulesDefaultYAML)
	}
	if fieldMappingYAML == "" {
		fieldMappingYAML = strings.TrimSpace(telemetryValidationValidatorFieldMappingDefaultYAML)
	}

	return rulesYAML, fieldMappingYAML
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
		converted, err := uint32ToInt32(p)
		if err != nil {
			return 0, nil, err
		}
		resolvedPorts = append(resolvedPorts, converted)
	}

	preferredPort, err := uint32ToInt32(preferredPorts[0])
	if err != nil {
		return 0, nil, err
	}

	return preferredPort, resolvedPorts, nil
}

func uint32ToInt32(value uint32) (int32, error) {
	if value > uint32(1<<31-1) {
		return 0, fmt.Errorf("value %d exceeds int32 max", value)
	}
	return int32(value), nil
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
