package controller

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/mydecisive/mdai-operator/internal/builder"
	"go.opentelemetry.io/collector/pdata/pmetric"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"
)

const (
	observerDefaultImage             = "public.ecr.aws/decisiveai/observer-collector:0.1"
	mdaiObserverHubComponent         = "mdai-observer"
	mdaiObserverResourceBaseName     = "mdai-observer"
	observerTelemetryTypeLogs        = "logs"
	observerTelemetryTypeTraces      = "traces"
	observerMetricsBackendGreptimeDB = "greptimedb"
	observerMetricsBackendPrometheus = "prometheus"
	greptimeDBUsersAuthSecretName    = "greptimedb-users-auth"
)

//go:embed config/observer_base_collector_config.yaml
var baseObserverCollectorYAML string

func (c ObserverAdapter) getScopedObserverResourceName(postfix string) string {
	if postfix != "" {
		return fmt.Sprintf("%s-%s-%s", c.observerCR.Name, mdaiObserverResourceBaseName, postfix)
	}
	return fmt.Sprintf("%s-%s", c.observerCR.Name, mdaiObserverResourceBaseName)
}

func (c ObserverAdapter) createOrUpdateObserverResourceService(ctx context.Context, namespace string) error {
	name := c.getScopedObserverResourceName("service")
	appLabel := c.getScopedObserverResourceName("")

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	if err := controllerutil.SetControllerReference(c.observerCR, service, c.scheme); err != nil {
		c.logger.Error(err, "Failed to set owner reference on Service", "service", name)
		return err
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, service, func() error {
		if service.Labels == nil {
			service.Labels = map[string]string{
				"app":                 appLabel,
				hubNameLabel:          c.observerCR.Name,
				HubComponentLabel:     mdaiObserverHubComponent,
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
			}
		}

		service.Spec = corev1.ServiceSpec{
			Selector: map[string]string{
				"app": appLabel,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "otlp-grpc",
					Protocol:   corev1.ProtocolTCP,
					Port:       otlpGRPCPort,
					TargetPort: intstr.FromString("otlp-grpc"),
				},
				{
					Name:       "otlp-http",
					Protocol:   corev1.ProtocolTCP,
					Port:       otlpHTTPPort,
					TargetPort: intstr.FromString("otlp-http"),
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update observer-collector-service: %w", err)
	}

	c.logger.Info("Successfully created or updated observer-collector-service", "service", name, "namespace", namespace, "operation", operationResult)
	return nil
}

func (c ObserverAdapter) createOrUpdateObserverResourceGreptimeDBSecret(ctx context.Context, namespace string) error {
	operatorNamespace := os.Getenv(PodNamespaceEnv)
	if operatorNamespace == "" {
		return fmt.Errorf("%s is not set", PodNamespaceEnv)
	}
	if namespace == operatorNamespace {
		c.logger.Info("Skipping GreptimeDB secret copy because observer namespace matches operator namespace", "namespace", namespace, "secret", greptimeDBUsersAuthSecretName)
		return nil
	}

	sourceSecret := &corev1.Secret{}
	if err := c.client.Get(ctx, types.NamespacedName{Name: greptimeDBUsersAuthSecretName, Namespace: operatorNamespace}, sourceSecret); err != nil {
		return fmt.Errorf("failed to get GreptimeDB auth secret: %w", err)
	}

	desiredSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      greptimeDBUsersAuthSecretName,
			Namespace: namespace,
		},
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, desiredSecret, func() error {
		if err := controllerutil.SetControllerReference(c.observerCR, desiredSecret, c.scheme); err != nil {
			c.logger.Error(err, "Failed to set owner reference on Secret", "secret", desiredSecret.Name)
			return err
		}

		if desiredSecret.Labels == nil {
			desiredSecret.Labels = map[string]string{
				"app":                 c.getScopedObserverResourceName(""),
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				HubComponentLabel:     mdaiObserverHubComponent,
				hubNameLabel:          c.observerCR.Name,
			}
		}
		desiredSecret.Type = sourceSecret.Type
		desiredSecret.Data = copySecretData(sourceSecret.Data)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update GreptimeDB auth secret: %w", err)
	}

	c.logger.Info("GreptimeDB auth Secret created or updated successfully", "secret", desiredSecret.Name, "namespace", namespace, "operation", operationResult)
	return nil
}

func (c ObserverAdapter) createOrUpdateObserverResourceConfigMap(ctx context.Context, observerResource mdaiv1.ObserverResource, observers []mdaiv1.Observer) (string, error) {
	namespace := c.observerCR.Namespace
	configMapName := c.getScopedObserverResourceName("config")

	collectorYAML, err := c.getObserverCollectorConfig(observers, observerResource)
	if err != nil {
		return "", fmt.Errorf("failed to build observer configuration: %w", err)
	}

	desiredConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                 c.getScopedObserverResourceName(""),
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				HubComponentLabel:     mdaiObserverHubComponent,
				hubNameLabel:          c.observerCR.Name,
			},
		},
		Data: map[string]string{
			"collector.yaml": collectorYAML,
		},
	}
	if err := controllerutil.SetControllerReference(c.observerCR, desiredConfigMap, c.scheme); err != nil {
		c.logger.Error(err, "Failed to set owner reference on ConfigMap", "configmap", configMapName)
		return "", err
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, desiredConfigMap, func() error {
		desiredConfigMap.Data["collector.yaml"] = collectorYAML
		return nil
	})
	if err != nil {
		c.logger.Error(err, "Failed to create or update ConfigMap", "configmap", configMapName)
		return "", err
	}

	c.logger.Info("ConfigMap created or updated successfully", "configmap", configMapName, "operation", operationResult)
	return getConfigMapSHA(*desiredConfigMap)
}

func (c ObserverAdapter) createOrUpdateObserverResourceDeployment(ctx context.Context, namespace string, hash string, observerResource mdaiv1.ObserverResource, greptimeDBEnabled bool) error {
	name := c.getScopedObserverResourceName("")

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, deployment, func() error {
		if err := controllerutil.SetControllerReference(c.observerCR, deployment, c.scheme); err != nil {
			c.logger.Error(err, "Failed to set owner reference on Deployment", "deployment", deployment.Name)
			return err
		}

		if deployment.Labels == nil {
			deployment.Labels = map[string]string{
				"app":                 name,
				HubComponentLabel:     mdaiObserverHubComponent,
				hubNameLabel:          c.observerCR.Name,
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
			}
		}

		deployment.Spec.Replicas = &observerResource.Replicas
		if deployment.Spec.Selector == nil {
			deployment.Spec.Selector = &metav1.LabelSelector{}
		}
		if deployment.Spec.Selector.MatchLabels == nil {
			deployment.Spec.Selector.MatchLabels = make(map[string]string)
		}
		deployment.Spec.Selector.MatchLabels["app"] = name

		if deployment.Spec.Template.Labels == nil {
			deployment.Spec.Template.Labels = make(map[string]string)
		}
		deployment.Spec.Template.Labels["app"] = name
		deployment.Spec.Template.Labels["app.kubernetes.io/component"] = name

		if deployment.Spec.Template.Annotations == nil {
			deployment.Spec.Template.Annotations = make(map[string]string)
		}
		deployment.Spec.Template.Annotations["prometheus.io/path"] = "/metrics"
		deployment.Spec.Template.Annotations["prometheus.io/port"] = "8899"
		deployment.Spec.Template.Annotations["prometheus.io/scrape"] = "true"
		// FIXME: replace this annotation with mdai_observer_resource in other hub components (prometheus scraping config)
		deployment.Spec.Template.Annotations["mdai_component_type"] = "mdai-observer"
		deployment.Spec.Template.Annotations["mdai-collector-config/sha256"] = hash

		containerSpec := corev1.Container{
			Name:  name,
			Image: observerDefaultImage,
			Ports: []corev1.ContainerPort{
				{ContainerPort: otelMetricsPort, Name: otelMetricsName},
				{ContainerPort: observerMetricsPort, Name: observerMetricsName},
				{ContainerPort: otlpGRPCPort, Name: otlpGRPCName},
				{ContainerPort: otlpHTTPPort, Name: otlpHTTPName},
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "config-volume",
					MountPath: "/conf/collector.yaml",
					SubPath:   "collector.yaml",
				},
			},
			Command: []string{
				// FIXME: update name away from observer
				"/mdai-observer-collector",
				"--config=/conf/collector.yaml",
			},
			SecurityContext: DefaultSecurityContext,
		}

		containerSpec.Image = observerResource.Image
		if observerResource.Resources != nil {
			containerSpec.Resources = *observerResource.Resources
		}
		if greptimeDBEnabled {
			containerSpec.EnvFrom = append(containerSpec.EnvFrom, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: greptimeDBUsersAuthSecretName,
					},
				},
			})
		}

		deployment.Spec.Template.Spec.Containers = []corev1.Container{
			containerSpec,
		}

		deployment.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "config-volume",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: c.getScopedObserverResourceName("config"),
						},
					},
				},
			},
		}

		deployment.Spec.Template.Spec.Tolerations = observerResource.Tolerations

		return nil
	})
	if err != nil {
		return err
	}
	c.logger.Info("Deployment created or updated successfully", "deployment", deployment.Name, "operationResult", operationResult)

	return nil
}

func (c ObserverAdapter) getObserverCollectorConfig(observers []mdaiv1.Observer, observerResource mdaiv1.ObserverResource) (string, error) {
	var config builder.ConfigBlock
	if err := yaml.Unmarshal([]byte(baseObserverCollectorYAML), &config); err != nil {
		c.logger.Error(err, "Failed to unmarshal base collector config")
		return "", fmt.Errorf(`unmarshal base collector config: %w`, err)
	}
	grpcReceiverMaxMsgSize := observerResource.GrpcReceiverMaxMsgSize
	if grpcReceiverMaxMsgSize != nil {
		config.
			MustMap("receivers").
			MustMap("otlp").
			MustMap("protocols").
			MustMap("grpc").
			Set("max_recv_msg_size_mib", *grpcReceiverMaxMsgSize)
	}

	prometheusDataVolumeReceivers := make([]string, 0)
	prometheusObservers := make([]mdaiv1.Observer, 0)
	greptimeDataVolumeReceivers := make([]string, 0)
	greptimeObservers := make([]mdaiv1.Observer, 0)

	processors := config.MustMap("processors")
	connectors := config.MustMap("connectors")
	pipelines := config.MustMap("service").MustMap("pipelines")
	telemetry := config.MustMap("service").MustMap("telemetry")

	for _, obs := range observers {
		observerName := obs.Name

		groupByKey := "groupbyattrs/" + observerName
		processors.Set(groupByKey, map[string]any{
			"keys": obs.LabelResourceAttributes,
		})

		dvKey := "datavolume/" + observerName
		dvSpec := map[string]any{
			"label_resource_attributes": obs.LabelResourceAttributes,
		}
		if obs.CountMetricName != nil {
			dvSpec["count_metric_name"] = *obs.CountMetricName
		}
		if obs.BytesMetricName != nil {
			dvSpec["bytes_metric_name"] = *obs.BytesMetricName
		}
		connectors.Set(dvKey, dvSpec)

		filterName := ""
		if obs.Filter != nil {
			filterName = "filter/" + observerName
			config.MustMap("processors").Set(filterName, getObserverFilterProcessorConfig(obs.Filter))
		}

		var pipelineProcessors []string
		if filterName != "" {
			pipelineProcessors = append(pipelineProcessors, filterName)
		}
		pipelineProcessors = append(pipelineProcessors, "batch", groupByKey)

		pipeline := map[string]any{
			"receivers":  []string{"otlp"},
			"processors": pipelineProcessors,
			"exporters":  []string{dvKey},
		}

		switch obs.TelemetryType {
		case observerTelemetryTypeLogs:
			pipelines.Set("logs/"+observerName, pipeline)
		case observerTelemetryTypeTraces:
			pipelines.Set("traces/"+observerName, pipeline)
		}

		metricsBackend := obs.MetricsBackend
		if metricsBackend == "" {
			metricsBackend = observerMetricsBackendPrometheus
		}
		switch metricsBackend {
		case observerMetricsBackendGreptimeDB:
			greptimeDataVolumeReceivers = append(greptimeDataVolumeReceivers, dvKey)
			greptimeObservers = append(greptimeObservers, obs)
		case observerMetricsBackendPrometheus:
			prometheusDataVolumeReceivers = append(prometheusDataVolumeReceivers, dvKey)
			prometheusObservers = append(prometheusObservers, obs)
		}
	}

	if len(prometheusDataVolumeReceivers) > 0 || len(greptimeDataVolumeReceivers) == 0 {
		pipeline := map[string]any{
			"receivers": prometheusDataVolumeReceivers,
			"exporters": []string{"prometheus"},
		}
		if processors := getMetricsOutputPipelineProcessors(prometheusObservers); len(processors) > 0 {
			pipeline["processors"] = processors
		}
		pipelines.
			Set("metrics/observeroutput", pipeline)
	}

	if len(greptimeDataVolumeReceivers) > 0 {
		configureGreptimeDBMetricsExporter(config, greptimeObservers)
		pipeline := map[string]any{
			"receivers": greptimeDataVolumeReceivers,
			"exporters": []string{"otlphttp/greptimedb"},
		}
		if processors := getMetricsOutputPipelineProcessors(greptimeObservers); len(processors) > 0 {
			pipeline["processors"] = processors
		}
		pipelines.
			Set("metrics/observeroutput/greptimedb", pipeline)
	}

	if ownLogsOtlpEndpoint := observerResource.OwnLogsOtlpEndpoint; ownLogsOtlpEndpoint != nil && *ownLogsOtlpEndpoint != "" {
		telemetry.Set("logs", map[string]any{
			"processors": []any{
				map[string]any{
					"batch": map[string]any{
						"exporter": map[string]any{
							"otlp": map[string]any{
								"protocol": "http/protobuf",
								"endpoint": *ownLogsOtlpEndpoint,
							},
						},
					},
				},
			},
		})
	}

	return config.YAML()
}

func hasGreptimeDBObservers(observers []mdaiv1.Observer) bool {
	for _, obs := range observers {
		if obs.MetricsBackend == observerMetricsBackendGreptimeDB {
			return true
		}
	}
	return false
}

func copySecretData(data map[string][]byte) map[string][]byte {
	if data == nil {
		return nil
	}
	result := make(map[string][]byte, len(data))
	for key, value := range data {
		result[key] = append([]byte(nil), value...)
	}
	return result
}

func getMetricsOutputPipelineProcessors(observers []mdaiv1.Observer) []string {
	for _, obs := range observers {
		if obs.AggregationTemporality == pmetric.AggregationTemporalityCumulative {
			return []string{"deltatocumulative"}
		}
	}
	return nil
}

func configureGreptimeDBMetricsExporter(config builder.ConfigBlock, observers []mdaiv1.Observer) {
	config.MustMap("extensions").Set("basicauth/client", map[string]any{
		"client_auth": map[string]any{
			"username": "${env:GREPTIME_USER}",
			"password": "${env:GREPTIME_PASSWD}",
		},
	})
	config.MustMap("exporters").Set("otlphttp/greptimedb", getGreptimeDBOTLPHTTPExporterConfig(observers))

	service := config.MustMap("service")
	serviceExtensions := service.MustSlice("extensions")
	service.Set("extensions", append(serviceExtensions, "basicauth/client"))
}

func getGreptimeDBOTLPHTTPExporterConfig(observers []mdaiv1.Observer) map[string]any {
	return map[string]any{
		"endpoint": "http://${env:GREPTIME_HOST}:4000/v1/otlp",
		"auth": map[string]any{
			"authenticator": "basicauth/client",
		},
		"headers": getGreptimeDBOTLPHTTPExporterHeaders(observers),
		"tls": map[string]any{
			"insecure": true,
		},
	}
}

func getGreptimeDBOTLPHTTPExporterHeaders(observers []mdaiv1.Observer) map[string]any {
	return map[string]any{
		"x-greptime-db-name":                            "${env:GREPTIME_DATABASE}",
		"x-greptime-otlp-metric-promote-resource-attrs": strings.Join(greptimeDBPromotedResourceAttributes(observers), ";"),
	}
}

func greptimeDBPromotedResourceAttributes(observers []mdaiv1.Observer) []string {
	seenAttributes := make(map[string]struct{})
	attributes := make([]string, 0)

	for _, obs := range observers {
		for _, attribute := range obs.LabelResourceAttributes {
			if _, ok := seenAttributes[attribute]; ok {
				continue
			}
			seenAttributes[attribute] = struct{}{}
			attributes = append(attributes, attribute)
		}
	}

	return attributes
}

func getObserverFilterProcessorConfig(filter *mdaiv1.ObserverFilter) map[string]any {
	filterMap := map[string]any{}

	if filter.ErrorMode != nil {
		filterMap["error_mode"] = filter.ErrorMode
	}

	if filter.Logs != nil && len(filter.Logs.LogRecord) > 0 {
		filterMap["logs"] = map[string]any{
			"log_record": filter.Logs.LogRecord,
		}
	}

	if filter.Traces != nil && len(filter.Traces.Span) > 0 {
		filterMap["traces"] = map[string]any{
			"span": filter.Traces.Span,
		}
	}
	// TODO: Add metrics and trace filters

	return filterMap
}
