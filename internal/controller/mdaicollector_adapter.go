package controller

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/mydecisive/mdai-operator/internal/builder"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"
)

type otlpTlsConfig struct {
	Insecure bool `yaml:"insecure" json:"insecure"`
}

type otlpExporterConfig struct {
	Endpoint  string        `yaml:"endpoint" json:"endpoint"`
	TlsConfig otlpTlsConfig `yaml:"tls" json:"tls"`
}

type s3ExporterConfig struct {
	S3Uploader s3UploaderConfig `yaml:"s3uploader" json:"s3uploader"`
}

type s3UploaderConfig struct {
	Region            string `yaml:"region" json:"region"`
	S3Bucket          string `yaml:"s3_bucket" json:"s3_bucket"`
	S3Prefix          string `yaml:"s3_prefix" json:"s3_prefix"`
	S3PartitionFormat string `yaml:"s3_partition_format" json:"s3_partition_format"`
	FilePrefix        string `yaml:"file_prefix" json:"file_prefix"`
	DisableSSL        bool   `yaml:"disable_ssl" json:"disable_ssl"`
}

type routingConnectorTableEntry struct {
	Context   string   `yaml:"context" json:"context"`
	Condition string   `yaml:"condition" json:"condition"`
	Pipelines []string `yaml:"pipelines" json:"pipelines"`
}

const (
	MdaiCollectorHubComponent     = "mdai-collector"
	mdaiCollectorResourceNameBase = "mdai-collector"

	S3PartitionFormat = "%Y/%m/%d/%H"
)

var (
	//go:embed config/mdai_collector_base_config.yaml
	baseMdaiCollectorYAML string
	severityFilterMap     = map[mdaiv1.SeverityLevel]string{
		mdaiv1.DebugSeverityLevel: "filter/severity_min_debug",
		mdaiv1.InfoSeverityLevel:  "filter/severity_min_info",
		mdaiv1.WarnSeverityLevel:  "filter/severity_min_warn",
		mdaiv1.ErrorSeverityLevel: "filter/severity_min_error",
	}
	routingTableContextMap = map[mdaiv1.MDAILogStream]string{
		mdaiv1.CollectorLogstream: "resource",
		mdaiv1.HubLogstream:       "resource",
		mdaiv1.AuditLogstream:     "log",
	}
	routingTableConditionMap = map[mdaiv1.MDAILogStream]string{
		mdaiv1.CollectorLogstream: `attributes["mdai-logstream"] == "collector"`,
		mdaiv1.HubLogstream:       `attributes["mdai-logstream"] == "hub"`,
		mdaiv1.AuditLogstream:     `attributes["mdai-logstream"] == "audit"`,
	}
)

var _ Adapter = (*MdaiCollectorAdapter)(nil)

type MdaiCollectorAdapter struct {
	collectorCR *mdaiv1.MdaiCollector
	logger      logr.Logger
	client      client.Client
	scheme      *runtime.Scheme
}

func NewMdaiCollectorAdapter(
	cr *mdaiv1.MdaiCollector,
	log logr.Logger,
	k8sClient client.Client,
	scheme *runtime.Scheme,
) *MdaiCollectorAdapter {
	return &MdaiCollectorAdapter{
		collectorCR: cr,
		logger:      log,
		client:      k8sClient,
		scheme:      scheme,
	}
}

func (c MdaiCollectorAdapter) ensureFinalizerInitialized(ctx context.Context) (OperationResult, error) {
	if controllerutil.ContainsFinalizer(c.collectorCR, hubFinalizer) {
		return ContinueProcessing()
	}
	c.logger.Info("Adding Finalizer for MdaiCollector")
	if ok := controllerutil.AddFinalizer(c.collectorCR, hubFinalizer); !ok {
		c.logger.Error(nil, "Failed to add finalizer into the custom resource")
		return RequeueWithError(errors.New("failed to add finalizer " + hubFinalizer))
	}

	if err := c.client.Update(ctx, c.collectorCR); err != nil {
		c.logger.Error(err, "Failed to update custom resource to add finalizer")
		return RequeueWithError(err)
	}
	return StopProcessing() // when finalizer is added it will trigger reconciliation
}

func (c MdaiCollectorAdapter) ensureStatusInitialized(ctx context.Context) (OperationResult, error) {
	if len(c.collectorCR.Status.Conditions) != 0 {
		return ContinueProcessing()
	}
	meta.SetStatusCondition(&c.collectorCR.Status.Conditions, metav1.Condition{Type: typeAvailableHub, Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Starting reconciliation"})
	if err := c.client.Status().Update(ctx, c.collectorCR); err != nil {
		c.logger.Error(err, "Failed to update MDAI Collector status")
		return RequeueWithError(err)
	}
	c.logger.Info("Re-queued to reconcile with updated status")
	return StopProcessing()
}

// finalize handles the deletion of a hub
func (c MdaiCollectorAdapter) finalize(ctx context.Context) (ObjectState, error) {
	if !controllerutil.ContainsFinalizer(c.collectorCR, hubFinalizer) {
		c.logger.Info("No finalizer found")
		return ObjectModified, nil
	}

	c.logger.Info("Performing Finalizer Operations for MDAI Collector before delete CR")

	if err := c.client.Get(ctx, types.NamespacedName{Name: c.collectorCR.Name, Namespace: c.collectorCR.Namespace}, c.collectorCR); err != nil {
		if apierrors.IsNotFound(err) {
			c.logger.Info("Cluster has been deleted, no need to finalize")
			return ObjectModified, nil
		}
		c.logger.Error(err, "Failed to re-fetch MdaiHub")
		return ObjectUnchanged, err
	}

	if meta.SetStatusCondition(&c.collectorCR.Status.Conditions, metav1.Condition{
		Type:    typeDegradedHub,
		Status:  metav1.ConditionTrue,
		Reason:  "Finalizing",
		Message: fmt.Sprintf("Finalizer operations for custom resource %s name were successfully accomplished", c.collectorCR.Name),
	}) {
		if err := c.client.Status().Update(ctx, c.collectorCR); err != nil {
			if apierrors.IsNotFound(err) {
				c.logger.Info("Cluster has been deleted, no need to finalize")
				return ObjectModified, nil
			}
			c.logger.Error(err, "Failed to update MDAI Collector status")

			return ObjectUnchanged, err
		}
	}

	c.logger.Info("Removing Finalizer for MDAI Collector after successfully perform the operations")
	if err := c.ensureFinalizerDeleted(ctx); err != nil {
		return ObjectUnchanged, err
	}

	return ObjectModified, nil
}

// ensureFinalizerDeleted removes finalizer of a Hub
func (c MdaiCollectorAdapter) ensureFinalizerDeleted(ctx context.Context) error {
	c.logger.Info("Deleting MDAI Collector Finalizer")
	return c.deleteFinalizer(ctx, c.collectorCR, hubFinalizer)
}

// deleteFinalizer deletes finalizer of a generic CR
func (c MdaiCollectorAdapter) deleteFinalizer(ctx context.Context, object client.Object, finalizer string) error {
	metadata, err := meta.Accessor(object)
	if err != nil {
		c.logger.Error(err, "Failed to delete finalizer", "finalizer", finalizer)
		return err
	}
	finalizers := metadata.GetFinalizers()
	if slices.Contains(finalizers, finalizer) {
		metadata.SetFinalizers(slices.DeleteFunc(finalizers, func(f string) bool { return f == finalizer }))
		return c.client.Update(ctx, object)
	}
	return nil
}

func (c MdaiCollectorAdapter) ensureSynchronized(ctx context.Context) (OperationResult, error) {
	namespace := c.collectorCR.Namespace
	var (
		awsConfig          *mdaiv1.AWSConfig
		logsConfig         *mdaiv1.LogsConfig
		awsAccessKeySecret *string
	)
	awsConfig = c.collectorCR.Spec.AWSConfig
	if awsConfig != nil {
		awsAccessKeySecret = awsConfig.AWSAccessKeySecret
	}
	logsConfig = c.collectorCR.Spec.Logs

	collectorConfigMapName, hash, err := c.createOrUpdateMdaiCollectorConfigMap(ctx, namespace, logsConfig, awsAccessKeySecret)
	if err != nil {
		return RequeueOnErrorOrContinue(err)
	}

	collectorEnvConfigMapName, err := c.createOrUpdateMdaiCollectorEnvVarConfigMap(ctx, namespace)
	if err != nil {
		return RequeueOnErrorOrContinue(err)
	}
	serviceAccountName, err := c.createOrUpdateMdaiCollectorServiceAccount(ctx, namespace)
	if err != nil {
		return RequeueOnErrorOrContinue(err)
	}
	roleName, err := c.createOrUpdateMdaiCollectorRole(ctx)
	if err != nil {
		return RequeueOnErrorOrContinue(err)
	}
	if err = c.createOrUpdateMdaiCollectorRoleBinding(ctx, namespace, roleName, serviceAccountName); err != nil {
		return RequeueOnErrorOrContinue(err)
	}

	params := CollectorDeploymentParams{
		Namespace:              namespace,
		CollectorConfigMapName: collectorConfigMapName,
		CollectorEnvConfigName: collectorEnvConfigMapName,
		ServiceAccountName:     serviceAccountName,
		AwsAccessKeySecret:     awsAccessKeySecret,
		Hash:                   hash,
		Image:                  c.collectorCR.Spec.Image,
		Tolerations:            c.collectorCR.Spec.Tolerations,
		Replicas:               c.collectorCR.Spec.Replicas,
	}

	deploymentName, err := c.createOrUpdateMdaiCollectorDeployment(ctx, params)
	if err != nil {
		if apierrors.ReasonForError(err) == metav1.StatusReasonConflict {
			c.logger.Info("re-queuing due to resource conflict")
			return Requeue()
		}
		return RequeueOnErrorOrContinue(err)
	}

	if _, err := c.createOrUpdateMdaiCollectorService(ctx, namespace, deploymentName); err != nil {
		return RequeueOnErrorOrContinue(err)
	}

	return ContinueProcessing()
}

func (c MdaiCollectorAdapter) getScopedMdaiCollectorResourceName(postfix string) string {
	if postfix != "" {
		return fmt.Sprintf("%s-%s-%s", c.collectorCR.Name, mdaiCollectorResourceNameBase, postfix)
	}
	return fmt.Sprintf("%s-%s", c.collectorCR.Name, mdaiCollectorResourceNameBase)
}

func (c MdaiCollectorAdapter) getMdaiCollectorConfig(logsConfig *mdaiv1.LogsConfig, awsAccessKeySecret *string) (string, error) {
	var mdaiCollectorConfig builder.ConfigBlock
	if err := yaml.Unmarshal([]byte(baseMdaiCollectorYAML), &mdaiCollectorConfig); err != nil {
		c.logger.Error(err, "Failed to unmarshal base mdai collector config")
		return "", err
	}

	if logsConfig != nil {
		exporters := mdaiCollectorConfig.MustMap("exporters")
		serviceBlock := mdaiCollectorConfig.MustMap("service")
		pipelines := serviceBlock.MustMap("pipelines")
		connectors := mdaiCollectorConfig.MustMap("connectors")
		routingTableMap := make(routingTable)
		otherLogstreamPipelines := make([]string, 0)

		otlpOtherLogstreamPipelines := c.augmentConfigForOtlpConfigAndGetOtherLogstreamPipelines(logsConfig, exporters, pipelines, routingTableMap)
		otherLogstreamPipelines = append(otherLogstreamPipelines, otlpOtherLogstreamPipelines...)

		s3OtherLogstreamPipelines := c.augmentPipelinesForS3ConfigAndGetOtherLogstreamPipelines(logsConfig, awsAccessKeySecret, exporters, pipelines, routingTableMap)
		otherLogstreamPipelines = append(otherLogstreamPipelines, s3OtherLogstreamPipelines...)

		logstreamRouter := connectors.MustMap("routing/logstream")
		newRoutingTable := make([]routingConnectorTableEntry, 0, len(routingTableMap))
		for _, entry := range routingTableMap {
			newRoutingTable = append(newRoutingTable, entry)
		}
		logstreamRouter.Set("table", newRoutingTable)
		if len(otherLogstreamPipelines) > 0 {
			logstreamRouter["default_pipelines"] = otherLogstreamPipelines
		}
	}

	collectorConfigBytes, err := yaml.Marshal(mdaiCollectorConfig)
	if err != nil {
		c.logger.Error(err, "Failed to marshal mdai-collector config", "mdaiCollectorConfig", mdaiCollectorConfig)
		return "", err
	}
	collectorConfig := string(collectorConfigBytes)

	return collectorConfig, nil
}

func (c MdaiCollectorAdapter) augmentPipelinesForS3ConfigAndGetOtherLogstreamPipelines(
	logsConfig *mdaiv1.LogsConfig,
	awsAccessKeySecret *string,
	exporters builder.ConfigBlock,
	pipelines builder.ConfigBlock,
	routingTableMap routingTable,
) []string {
	defaultRoutingPipelines := make([]string, 0)
	if awsAccessKeySecret == nil {
		c.logger.Info("Skipped adding s3 components to mdai-collector due to missing s3 configuration", "logsConfig", logsConfig, "awsAccessKeySecret", awsAccessKeySecret)
		return defaultRoutingPipelines
	}
	s3Config := logsConfig.S3
	if s3Config == nil {
		return defaultRoutingPipelines
	}
	c.logger.Info("Adding S3 components to mdai-collector config for " + c.collectorCR.Name)
	auditLogstreamConfig := s3Config.AuditLogs
	if auditLogstreamConfig == nil || !auditLogstreamConfig.Disabled {
		logstream := mdaiv1.AuditLogstream
		c.addS3ComponentsAndPipeline(logstream, s3Config, exporters, nil, pipelines, routingTableMap)
	}

	collectorLogstreamConfig := s3Config.CollectorLogs
	if collectorLogstreamConfig == nil || !collectorLogstreamConfig.Disabled {
		logstream := mdaiv1.CollectorLogstream
		s3ExporterName, s3Exporter := getS3ExporterForLogstream(c.collectorCR.Name, logstream, *s3Config)
		exporters[s3ExporterName] = s3Exporter
		var minSeverity *mdaiv1.SeverityLevel
		if collectorLogstreamConfig != nil && collectorLogstreamConfig.MinSeverity != nil {
			minSeverity = collectorLogstreamConfig.MinSeverity
		}
		c.addS3ComponentsAndPipeline(logstream, s3Config, exporters, minSeverity, pipelines, routingTableMap)
	}

	hubLogstreamConfig := s3Config.HubLogs
	if hubLogstreamConfig == nil || !hubLogstreamConfig.Disabled {
		logstream := mdaiv1.HubLogstream
		var minSeverity *mdaiv1.SeverityLevel
		if hubLogstreamConfig != nil && hubLogstreamConfig.MinSeverity != nil {
			minSeverity = hubLogstreamConfig.MinSeverity
		}
		c.addS3ComponentsAndPipeline(logstream, s3Config, exporters, minSeverity, pipelines, routingTableMap)
	}

	otherLogstreamConfig := s3Config.OtherLogs
	if otherLogstreamConfig == nil || !otherLogstreamConfig.Disabled {
		logstream := mdaiv1.OtherLogstream
		var minSeverity *mdaiv1.SeverityLevel
		if otherLogstreamConfig != nil && otherLogstreamConfig.MinSeverity != nil {
			minSeverity = otherLogstreamConfig.MinSeverity
		}
		pipelineName := c.addS3ComponentsAndPipeline(logstream, s3Config, exporters, minSeverity, pipelines, routingTableMap)
		defaultRoutingPipelines = append(defaultRoutingPipelines, pipelineName)
	}

	return defaultRoutingPipelines
}

func (c MdaiCollectorAdapter) augmentConfigForOtlpConfigAndGetOtherLogstreamPipelines(
	logsConfig *mdaiv1.LogsConfig,
	exporters builder.ConfigBlock,
	pipelines builder.ConfigBlock,
	routingTableMap routingTable,
) []string {
	defaultRoutingPipelines := make([]string, 0)
	otlpConfig := logsConfig.Otlp
	if otlpConfig == nil || otlpConfig.Endpoint == "" {
		return defaultRoutingPipelines
	}
	c.logger.Info("Adding OTLP components to mdai-collector config for " + c.collectorCR.Name)
	otlpExporterName, otlpExporterCfg := getOtlpExporterForLogstream(*otlpConfig)
	exporters[otlpExporterName] = otlpExporterCfg

	auditLogstreamConfig := otlpConfig.AuditLogs
	if auditLogstreamConfig == nil || !auditLogstreamConfig.Disabled {
		logstream := mdaiv1.AuditLogstream
		pipelineName := fmt.Sprintf("logs/otlp_%s", logstream)
		newPipeline := getPipelineWithExporterAndSeverityFilter("routing/logstream", otlpExporterName, nil, "batch/audit")
		pipelines[pipelineName] = newPipeline
		routingTableMap.addOrCreateRoutingTableEntryWithPipeline(logstream, pipelineName)
	}

	collectorLogstreamConfig := otlpConfig.CollectorLogs
	if collectorLogstreamConfig == nil || !collectorLogstreamConfig.Disabled {
		logstream := mdaiv1.CollectorLogstream
		addPipelineForLogstream(collectorLogstreamConfig, logstream, otlpExporterName, pipelines, routingTableMap)
	}

	hubLogstreamConfig := otlpConfig.HubLogs
	if hubLogstreamConfig == nil || !hubLogstreamConfig.Disabled {
		logstream := mdaiv1.HubLogstream
		addPipelineForLogstream(hubLogstreamConfig, logstream, otlpExporterName, pipelines, routingTableMap)
	}

	otherLogstreamConfig := otlpConfig.OtherLogs
	if otherLogstreamConfig == nil || !otherLogstreamConfig.Disabled {
		logstream := mdaiv1.OtherLogstream
		pipelineName := addPipelineForLogstream(otherLogstreamConfig, logstream, otlpExporterName, pipelines, routingTableMap)
		defaultRoutingPipelines = append(defaultRoutingPipelines, pipelineName)
	}
	return defaultRoutingPipelines
}

func (c MdaiCollectorAdapter) addS3ComponentsAndPipeline(
	logstream mdaiv1.MDAILogStream,
	s3Config *mdaiv1.S3LogsConfig,
	exporters builder.ConfigBlock,
	minSeverity *mdaiv1.SeverityLevel,
	pipelines builder.ConfigBlock,
	routingTableMap routingTable,
) string {
	s3ExporterName, s3Exporter := getS3ExporterForLogstream(c.collectorCR.Name, logstream, *s3Config)
	exporters[s3ExporterName] = s3Exporter
	pipelineName := fmt.Sprintf("logs/s3_%s", logstream)
	newPipeline := getPipelineWithExporterAndSeverityFilter("routing/logstream", s3ExporterName, minSeverity, "batch")
	pipelines[pipelineName] = newPipeline
	routingTableMap.addOrCreateRoutingTableEntryWithPipeline(logstream, pipelineName)
	return pipelineName
}

func addPipelineForLogstream(
	logstreamConfig *mdaiv1.LogstreamConfig,
	logstream mdaiv1.MDAILogStream,
	otlpExporterName string,
	pipelines builder.ConfigBlock,
	routingTableMap routingTable,
) string {
	var minSeverity *mdaiv1.SeverityLevel
	if logstreamConfig != nil && logstreamConfig.MinSeverity != nil {
		minSeverity = logstreamConfig.MinSeverity
	}
	pipelineName := fmt.Sprintf("logs/otlp_%s", logstream)
	newPipeline := getPipelineWithExporterAndSeverityFilter("routing/logstream", otlpExporterName, minSeverity, "")
	pipelines[pipelineName] = newPipeline
	routingTableMap.addOrCreateRoutingTableEntryWithPipeline(logstream, pipelineName)
	return pipelineName
}

type routingTable map[mdaiv1.MDAILogStream]routingConnectorTableEntry

func (rt routingTable) addOrCreateRoutingTableEntryWithPipeline(
	logstream mdaiv1.MDAILogStream,
	pipelineName string,
) {
	if logstream == mdaiv1.OtherLogstream {
		return
	}
	entry, ok := rt[logstream]
	if !ok {
		rt[logstream] = routingConnectorTableEntry{
			Context:   routingTableContextMap[logstream],
			Condition: routingTableConditionMap[logstream],
			Pipelines: []string{
				pipelineName,
			},
		}
		return
	}
	entry.Pipelines = append(entry.Pipelines, pipelineName)
	rt[logstream] = entry
}

func getOtlpExporterForLogstream(otlpLogsConfig mdaiv1.OtlpLogsConfig) (string, otlpExporterConfig) {
	exporter := otlpExporterConfig{
		Endpoint: otlpLogsConfig.Endpoint,
		TlsConfig: otlpTlsConfig{
			Insecure: true,
		},
	}
	return "otlp", exporter
}

func getS3ExporterForLogstream(hubName string, logstream mdaiv1.MDAILogStream, s3LogsConfig mdaiv1.S3LogsConfig) (string, s3ExporterConfig) {
	s3Prefix := fmt.Sprintf("%s-%s-logs", hubName, logstream)
	exporterKey := fmt.Sprintf("awss3/%s", logstream)
	filePrefix := fmt.Sprintf("%s-", logstream)
	exporter := s3ExporterConfig{
		S3Uploader: s3UploaderConfig{
			Region:            s3LogsConfig.S3Region,
			S3Bucket:          s3LogsConfig.S3Bucket,
			S3Prefix:          s3Prefix,
			FilePrefix:        filePrefix,
			S3PartitionFormat: S3PartitionFormat,
			DisableSSL:        true,
		},
	}
	return exporterKey, exporter
}

func getPipelineWithExporterAndSeverityFilter(
	receiverName string,
	exporterName string,
	minSeverity *mdaiv1.SeverityLevel,
	batchProcessor string,
) builder.ConfigBlock {
	processors := make([]any, 0)
	if minSeverity != nil {
		if filter := severityFilterMap[*minSeverity]; filter != "" {
			processors = append(processors, filter)
		}
	}
	if batchProcessor != "" {
		processors = append(processors, batchProcessor)
	}
	return builder.ConfigBlock{
		"receivers":  []any{receiverName},
		"processors": processors,
		"exporters":  []any{exporterName},
	}
}

// ensureDeletionProcessed deletes MDAI Collector in cases a deletion was triggered
func (c MdaiCollectorAdapter) ensureDeletionProcessed(ctx context.Context) (OperationResult, error) {
	if c.collectorCR.DeletionTimestamp.IsZero() {
		return ContinueProcessing()
	}
	c.logger.Info("Deleting Cluster:" + c.collectorCR.Name)
	crState, err := c.finalize(ctx)
	if crState == ObjectUnchanged || err != nil {
		c.logger.Info("Has to requeue mdai")
		return RequeueAfter(requeueTime, err)
	}
	return StopProcessing()
}

func (c MdaiCollectorAdapter) ensureStatusSetToDone(ctx context.Context) (OperationResult, error) {
	// Re-fetch the Custom Resource after update or create
	if err := c.client.Get(ctx, types.NamespacedName{Name: c.collectorCR.Name, Namespace: c.collectorCR.Namespace}, c.collectorCR); err != nil {
		c.logger.Error(err, "Failed to re-fetch MDAI Collector")
		return Requeue()
	}
	meta.SetStatusCondition(&c.collectorCR.Status.Conditions, metav1.Condition{
		Type:   typeAvailableHub,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: "reconciled successfully",
	})
	if err := c.client.Status().Update(ctx, c.collectorCR); err != nil {
		if apierrors.ReasonForError(err) == metav1.StatusReasonConflict {
			c.logger.Info("re-queuing due to resource conflict")
			return Requeue()
		}
		c.logger.Error(err, "Failed to update MDAI Collector status")
		return Requeue()
	}
	c.logger.Info("Status set to done for MDAI Collector", "mdaiHub", c.collectorCR.Name)
	return ContinueProcessing()
}

//nolint:revive
func (c MdaiCollectorAdapter) createOrUpdateMdaiCollectorConfigMap(ctx context.Context, namespace string, logsConfig *mdaiv1.LogsConfig, awsAccessKeySecret *string) (string, string, error) {
	mdaiCollectorConfigConfigMapName := c.getScopedMdaiCollectorResourceName("config")
	collectorYAML, err := c.getMdaiCollectorConfig(logsConfig, awsAccessKeySecret)
	if err != nil {
		return "", "", fmt.Errorf("failed to build mdai-collector configuration: %w", err)
	}

	desiredConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mdaiCollectorConfigConfigMapName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                          "mdai-collector",
				hubNameLabel:                   c.collectorCR.Name,
				"app.kubernetes.io/managed-by": "mdai-operator",
				HubComponentLabel:              MdaiCollectorHubComponent,
			},
		},
		Data: map[string]string{
			"collector.yaml": collectorYAML,
		},
	}

	if err := controllerutil.SetControllerReference(c.collectorCR, desiredConfigMap, c.scheme); err != nil {
		c.logger.Error(err, "Failed to set owner reference on "+mdaiCollectorConfigConfigMapName+" ConfigMap", "configmap", mdaiCollectorConfigConfigMapName)
		return "", "", err
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, desiredConfigMap, func() error {
		desiredConfigMap.Data["collector.yaml"] = collectorYAML
		return nil
	})
	if err != nil {
		c.logger.Error(err, "Failed to create or update "+mdaiCollectorConfigConfigMapName+" ConfigMap", "configmap", mdaiCollectorConfigConfigMapName)
		return "", "", err
	}

	c.logger.Info(mdaiCollectorConfigConfigMapName+" ConfigMap created or updated successfully", "configmap", mdaiCollectorConfigConfigMapName, "operation", operationResult)
	sha, err := getConfigMapSHA(*desiredConfigMap)
	return mdaiCollectorConfigConfigMapName, sha, err
}

func (c MdaiCollectorAdapter) createOrUpdateMdaiCollectorEnvVarConfigMap(ctx context.Context, namespace string) (string, error) {
	mdaiCollectorEnvVarConfigMapName := c.getScopedMdaiCollectorResourceName("env")
	data := map[string]string{
		"LOG_SEVERITY":  "SEVERITY_NUMBER_WARN",
		"K8S_NAMESPACE": namespace,
	}

	desiredConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mdaiCollectorEnvVarConfigMapName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                 "mdai-collector",
				hubNameLabel:          c.collectorCR.Name,
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				HubComponentLabel:     MdaiCollectorHubComponent,
			},
		},
		Data: data,
	}
	if err := controllerutil.SetControllerReference(c.collectorCR, desiredConfigMap, c.scheme); err != nil {
		c.logger.Error(err, "Failed to set owner reference on "+mdaiCollectorEnvVarConfigMapName+" ConfigMap", "configmap", mdaiCollectorEnvVarConfigMapName)
		return "", err
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, desiredConfigMap, func() error {
		desiredConfigMap.Data = data
		return nil
	})
	if err != nil {
		c.logger.Error(err, "Failed to create or update "+mdaiCollectorEnvVarConfigMapName+" ConfigMap", "configmap", mdaiCollectorEnvVarConfigMapName)
		return "", err
	}

	c.logger.Info(mdaiCollectorEnvVarConfigMapName+" ConfigMap created or updated successfully", "configmap", mdaiCollectorEnvVarConfigMapName, "operation", operationResult)
	return mdaiCollectorEnvVarConfigMapName, nil
}

type CollectorDeploymentParams struct {
	Namespace              string
	CollectorConfigMapName string
	CollectorEnvConfigName string
	ServiceAccountName     string
	AwsAccessKeySecret     *string
	Hash                   string
	Image                  string
	Tolerations            []corev1.Toleration
	Replicas               int32
}

var DefaultSecurityContext = &corev1.SecurityContext{
	SeccompProfile: &corev1.SeccompProfile{
		Type: corev1.SeccompProfileTypeRuntimeDefault,
	},
	AllowPrivilegeEscalation: new(false),
	Capabilities: &corev1.Capabilities{
		Drop: []corev1.Capability{"ALL"},
	},
	RunAsNonRoot: new(true),
}

func (c MdaiCollectorAdapter) createOrUpdateMdaiCollectorDeployment(ctx context.Context, params CollectorDeploymentParams) (string, error) {
	mdaiCollectorDeploymentName := c.getScopedMdaiCollectorResourceName("")
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mdaiCollectorDeploymentName,
			Namespace: params.Namespace,
			Labels: map[string]string{
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				HubComponentLabel:     MdaiCollectorHubComponent,
			},
		},
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, deployment, func() error {
		if err := controllerutil.SetControllerReference(c.collectorCR, deployment, c.scheme); err != nil {
			return err
		}

		container := builder.Container(mdaiCollectorDeploymentName, params.Image).
			WithCommand("/mdai-collector", "--config=/conf/collector.yaml").
			WithPorts(
				corev1.ContainerPort{ContainerPort: otlpGRPCPort, Name: otlpGRPCName},
				corev1.ContainerPort{ContainerPort: otlpHTTPPort, Name: otlpHTTPName},
				corev1.ContainerPort{ContainerPort: promHTTPPort, Name: promHTTPName},
			).
			WithVolumeMounts(corev1.VolumeMount{
				Name:      "config-volume",
				MountPath: "/conf/collector.yaml",
				SubPath:   "collector.yaml",
			}).
			WithEnvFrom(corev1.EnvFromSource{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: params.CollectorConfigMapName,
					},
				},
			}).
			WithAWSSecret(params.AwsAccessKeySecret).
			WithSecurityContext(DefaultSecurityContext).
			Build()

		builder.Deployment(deployment).
			WithLabel("app", mdaiCollectorDeploymentName).
			WithLabel(hubNameLabel, c.collectorCR.Name).
			WithSelectorLabel("app", mdaiCollectorDeploymentName).
			WithTemplateLabel("app", mdaiCollectorDeploymentName).
			WithTemplateLabel("app.kubernetes.io/component", mdaiCollectorDeploymentName).
			WithTemplateAnnotation("mdai-collector-config/sha256", params.Hash).
			WithTolerations(params.Tolerations...).
			WithReplicas(params.Replicas).
			WithServiceAccount(params.ServiceAccountName).
			WithContainers(container).
			WithVolumes(corev1.Volume{
				Name: "config-volume",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: params.CollectorConfigMapName,
						},
					},
				},
			})

		return nil
	})
	if err != nil {
		return "", err
	}
	c.logger.Info(mdaiCollectorDeploymentName+" Deployment created or updated successfully",
		"deployment", deployment.Name,
		"operationResult", operationResult,
	)

	return mdaiCollectorDeploymentName, nil
}

func (c MdaiCollectorAdapter) createOrUpdateMdaiCollectorService(ctx context.Context, namespace string, appName string) (string, error) {
	mdaiCollectorServiceName := c.getScopedMdaiCollectorResourceName("service")
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mdaiCollectorServiceName,
			Namespace: namespace,
			Labels: map[string]string{
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				HubComponentLabel:     MdaiCollectorHubComponent,
			},
		},
	}

	if err := controllerutil.SetControllerReference(c.collectorCR, service, c.scheme); err != nil {
		c.logger.Error(err, "Failed to set owner reference on "+mdaiCollectorServiceName+" Service", "service", mdaiCollectorServiceName)
		return "", err
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, service, func() error {
		if err := controllerutil.SetControllerReference(c.collectorCR, service, c.scheme); err != nil {
			return err
		}

		builder.Service(service).
			WithLabel("app", appName).
			WithLabel(hubNameLabel, c.collectorCR.Name).
			WithSelectorLabel("app", appName).
			WithPorts(
				corev1.ServicePort{
					Port:       otlpGRPCPort,
					Name:       "otlp-grpc",
					TargetPort: intstr.FromString("otlp-grpc"),
					Protocol:   corev1.ProtocolTCP,
				},
				corev1.ServicePort{
					Port:       otlpHTTPPort,
					Name:       "otlp-http",
					TargetPort: intstr.FromString("otlp-http"),
					Protocol:   corev1.ProtocolTCP,
				},
			).
			WithType(corev1.ServiceTypeClusterIP)

		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to create or update mdai-collector-service: %w", err)
	}

	c.logger.Info("Successfully created or updated"+mdaiCollectorServiceName+" Service", "service", mdaiCollectorServiceName, "namespace", namespace, "operation", operationResult)
	return mdaiCollectorServiceName, nil
}

func (c MdaiCollectorAdapter) createOrUpdateMdaiCollectorServiceAccount(ctx context.Context, namespace string) (string, error) {
	mdaiCollectorServiceAccountName := c.getScopedMdaiCollectorResourceName("sa")
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mdaiCollectorServiceAccountName,
			Namespace: namespace,
			Labels: map[string]string{
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				HubComponentLabel:     MdaiCollectorHubComponent,
			},
		},
	}

	if err := controllerutil.SetControllerReference(c.collectorCR, serviceAccount, c.scheme); err != nil {
		c.logger.Error(err, "Failed to set owner reference on "+mdaiCollectorServiceAccountName+" ServiceAccount", "serviceAccount", mdaiCollectorServiceAccountName)
		return "", err
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, serviceAccount, func() error {
		return nil
	})
	if err != nil {
		return "", err
	}
	c.logger.Info(mdaiCollectorServiceAccountName+" ServiceAccount created or updated successfully", "serviceAccount", serviceAccount.Name, "operationResult", operationResult)

	return mdaiCollectorServiceAccountName, nil
}

func (c MdaiCollectorAdapter) createOrUpdateMdaiCollectorRole(ctx context.Context) (string, error) {
	mdaiCollectorClusterRoleName := c.getScopedMdaiCollectorResourceName("role")
	// BEWARE: If you're thinking about making a yaml and unmarshaling it, to extract all this out... there's a weird problem where apiGroups won't unmarshal from yaml.
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: mdaiCollectorClusterRoleName,
			Labels: map[string]string{
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				HubComponentLabel:     MdaiCollectorHubComponent,
			},
		},
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, role, func() error {
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{
					"events",
					"namespaces",
					"namespaces/status",
					"nodes",
					"nodes/spec",
					"pods",
					"pods/status",
					"replicationcontrollers",
					"replicationcontrollers/status",
					"resourcequotas",
					"services",
				},
				Verbs: []string{
					"get",
					"list",
					"watch",
				},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{
					"daemonsets",
					"deployments",
					"replicasets",
					"statefulsets",
				},
				Verbs: []string{
					"get",
					"list",
					"watch",
				},
			},
			{
				APIGroups: []string{"extensions"},
				Resources: []string{
					"daemonsets",
					"deployments",
					"replicasets",
				},
				Verbs: []string{
					"get",
					"list",
					"watch",
				},
			},
			{
				APIGroups: []string{"batch"},
				Resources: []string{
					"jobs",
					"cronjobs",
				},
				Verbs: []string{
					"get",
					"list",
					"watch",
				},
			},
			{
				APIGroups: []string{"autoscaling"},
				Resources: []string{
					"horizontalpodautoscalers",
				},
				Verbs: []string{
					"get",
					"list",
					"watch",
				},
			},
		}

		return nil
	})
	if err != nil {
		return "", err
	}
	c.logger.Info(mdaiCollectorClusterRoleName+" ClusterRole created or updated successfully", "role", role.Name, "operationResult", operationResult)

	return mdaiCollectorClusterRoleName, nil
}

func (c MdaiCollectorAdapter) createOrUpdateMdaiCollectorRoleBinding(ctx context.Context, namespace string, roleName string, serviceAccountName string) error {
	mdaiCollectorClusterRoleBindingName := c.getScopedMdaiCollectorResourceName("rb")
	roleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: mdaiCollectorClusterRoleBindingName,
			Labels: map[string]string{
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				HubComponentLabel:     MdaiCollectorHubComponent,
			},
		},
	}

	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, roleBinding, func() error {
		roleBinding.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     roleName,
		}
		roleBinding.Subjects = []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      serviceAccountName,
				Namespace: namespace,
			},
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.logger.Info(mdaiCollectorClusterRoleBindingName+"RoleBinding created or updated successfully", "roleBinding", roleBinding.Name, "operationResult", operationResult)

	return nil
}
