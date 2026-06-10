package controller

import (
	"context"
	_ "embed"
	"fmt"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/mydecisive/mdai-operator/internal/builder"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func (c ObserverAdapter) createOrUpdateObserverResourceDeployment(ctx context.Context, namespace string, hash string, observerResource mdaiv1.ObserverResource) error {
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
	greptimeDataVolumeReceivers := make([]string, 0)

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
		if obs.AggregationTemporality != "" {
			dvSpec["aggregation_temporality"] = obs.AggregationTemporality
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

		if obs.TelemetryType != nil {
			switch *obs.TelemetryType {
			case observerTelemetryTypeLogs:
				pipelines.Set("logs/"+observerName, pipeline)
			case observerTelemetryTypeTraces:
				pipelines.Set("traces/"+observerName, pipeline)
			}
		}

		metricsBackend := obs.MetricsBackend
		if metricsBackend == "" {
			metricsBackend = observerMetricsBackendPrometheus
		}
		switch metricsBackend {
		case observerMetricsBackendGreptimeDB:
			greptimeDataVolumeReceivers = append(greptimeDataVolumeReceivers, dvKey)
		case observerMetricsBackendPrometheus:
			prometheusDataVolumeReceivers = append(prometheusDataVolumeReceivers, dvKey)
		}
	}

	if len(prometheusDataVolumeReceivers) > 0 || len(greptimeDataVolumeReceivers) == 0 {
		pipelines.
			Set("metrics/observeroutput",
				map[string]any{
					"receivers":  prometheusDataVolumeReceivers,
					"processors": []string{"deltatocumulative"},
					"exporters":  []string{"prometheus"},
				},
			)
	}

	if len(greptimeDataVolumeReceivers) > 0 {
		configureGreptimeDBMetricsExporter(config)
		pipelines.
			Set("metrics/observeroutput/greptimedb",
				map[string]any{
					"receivers": greptimeDataVolumeReceivers,
					"exporters": []string{"otlphttp/greptimedb"},
				},
			)
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

func configureGreptimeDBMetricsExporter(config builder.ConfigBlock) {
	username, password := getGreptimeDBCredentials()

	config.MustMap("extensions").Set("basicauth/client", map[string]any{
		"client_auth": map[string]any{
			"username": username,
			"password": password,
		},
	})
	config.MustMap("exporters").Set("otlphttp/greptimedb", getGreptimeDBOTLPHTTPExporterConfig())

	service := config.MustMap("service")
	serviceExtensions := service.MustSlice("extensions")
	service.Set("extensions", append(serviceExtensions, "basicauth/client"))
}

func getGreptimeDBOTLPHTTPExporterConfig() map[string]any {
	return map[string]any{
		"endpoint": "https://greptimedb.example.com/v1/otlp",
		"auth": map[string]any{
			"authenticator": "basicauth/client",
		},
	}
}

func getGreptimeDBCredentials() (string, string) {
	return "<your_username>", "<your_password>"
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
