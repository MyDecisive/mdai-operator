package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/mydecisive/mdai-data-core/opamp"

	events "github.com/mydecisive/mdai-data-core/eventing/rule"
	"go.uber.org/zap"

	"github.com/go-logr/logr"
	"github.com/mydecisive/mdai-data-core/audit"
	vars "github.com/mydecisive/mdai-data-core/variables"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	prometheusv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/valkey-io/valkey-go"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	envConfigMapNamePostfix        = "-variables"
	manualEnvConfigMapNamePostfix  = "-manual-variables"
	automationConfigMapNamePostfix = "-automation"

	requeueTime = time.Second * 5

	hubNameLabel       = "mydecisive.ai/hub-name"
	HubComponentLabel  = "mydecisive.ai/hub-component"
	configMapTypeLabel = "mydecisive.ai/configmap-type"
)

var (
	errUnsupportedVariableDataType   = errors.New("unsupported variable data type")
	errUnsupportedVariableType       = errors.New("unsupported variable type")
	errUnsupportedVariableSourceType = errors.New("unsupported variable source type")
	errUnsupportedTransformerType    = errors.New("unsupported transformer type")
)

var _ Adapter = (*HubAdapter)(nil)

type HubAdapter struct {
	mdaiCR                  *mdaiv1.MdaiHub
	logger                  logr.Logger
	client                  client.Client
	recorder                record.EventRecorder
	scheme                  *runtime.Scheme
	valKeyClient            valkey.Client
	valkeyAuditStreamExpiry time.Duration
	releaseName             string
	zapLogger               *zap.Logger
	opampConnectionManager  opamp.ConnectionManager
}

func NewHubAdapter(
	cr *mdaiv1.MdaiHub,
	log logr.Logger,
	zapLogger *zap.Logger,
	k8sClient client.Client,
	recorder record.EventRecorder,
	scheme *runtime.Scheme,
	valkeyClient valkey.Client,
	valkeyAuditStreamExpiry time.Duration,
	opampConnectionManager opamp.ConnectionManager,
) *HubAdapter {
	return &HubAdapter{
		mdaiCR:                  cr,
		logger:                  log,
		client:                  k8sClient,
		recorder:                recorder,
		scheme:                  scheme,
		valKeyClient:            valkeyClient,
		valkeyAuditStreamExpiry: valkeyAuditStreamExpiry,
		releaseName:             os.Getenv("RELEASE_NAME"),
		zapLogger:               zapLogger,
		opampConnectionManager:  opampConnectionManager,
	}
}

func (c HubAdapter) ensureFinalizerInitialized(ctx context.Context) (OperationResult, error) {
	if controllerutil.ContainsFinalizer(c.mdaiCR, hubFinalizer) {
		return ContinueProcessing()
	}
	c.logger.Info("Adding Finalizer for MdaiHub")
	if ok := controllerutil.AddFinalizer(c.mdaiCR, hubFinalizer); !ok {
		c.logger.Error(nil, "Failed to add finalizer into the custom resource")
		return RequeueWithError(errors.New("failed to add finalizer " + hubFinalizer))
	}

	if err := c.client.Update(ctx, c.mdaiCR); err != nil {
		c.logger.Error(err, "Failed to update custom resource to add finalizer")
		return RequeueWithError(err)
	}
	return StopProcessing() // when finalizer is added it will trigger reconciliation
}

func (c HubAdapter) ensureStatusInitialized(ctx context.Context) (OperationResult, error) {
	if len(c.mdaiCR.Status.Conditions) != 0 {
		return ContinueProcessing()
	}
	meta.SetStatusCondition(&c.mdaiCR.Status.Conditions, metav1.Condition{Type: typeAvailableHub, Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Starting reconciliation"})
	if err := c.client.Status().Update(ctx, c.mdaiCR); err != nil {
		c.logger.Error(err, "Failed to update Cluster status")
		return RequeueWithError(err)
	}
	c.logger.Info("Re-queued to reconcile with updated status")
	return StopProcessing()
}

// finalize handles the deletion of a hub
func (c HubAdapter) finalize(ctx context.Context) (ObjectState, error) {
	if !controllerutil.ContainsFinalizer(c.mdaiCR, hubFinalizer) {
		c.logger.Info("No finalizer found")
		return ObjectModified, nil
	}

	c.logger.Info("Performing Finalizer Operations for Cluster before delete CR")

	err := c.deletePrometheusRule(ctx)
	if err != nil {
		c.logger.Info("Failed to delete prometheus rules, will re-try later")
		return ObjectUnchanged, err
	}

	if err := c.client.Get(ctx, types.NamespacedName{Name: c.mdaiCR.Name, Namespace: c.mdaiCR.Namespace}, c.mdaiCR); err != nil {
		if apierrors.IsNotFound(err) {
			c.logger.Info("Cluster has been deleted, no need to finalize")
			return ObjectModified, nil
		}
		c.logger.Error(err, "Failed to re-fetch MdaiHub")
		return ObjectUnchanged, err
	}

	if meta.SetStatusCondition(&c.mdaiCR.Status.Conditions, metav1.Condition{
		Type:    typeDegradedHub,
		Status:  metav1.ConditionTrue,
		Reason:  "Finalizing",
		Message: fmt.Sprintf("Finalizer operations for custom resource %s name were successfully accomplished", c.mdaiCR.Name),
	}) {
		if err := c.client.Status().Update(ctx, c.mdaiCR); err != nil {
			if apierrors.IsNotFound(err) {
				c.logger.Info("Cluster has been deleted, no need to finalize")
				return ObjectModified, nil
			}
			c.logger.Error(err, "Failed to update Cluster status")

			return ObjectUnchanged, err
		}
	}

	c.logger.Info("Removing Finalizer for Cluster after successfully perform the operations")
	if err := c.ensureFinalizerDeleted(ctx); err != nil {
		return ObjectUnchanged, err
	}

	prefix := VariableKeyPrefix + c.mdaiCR.Name + "/"
	c.logger.Info("Cleaning up old variables from Valkey with prefix", "prefix", prefix)
	if err := vars.NewValkeyAdapter(c.valKeyClient, c.zapLogger).DeleteKeysWithPrefixUsingScan(ctx, map[string]struct{}{}, c.mdaiCR.Name); err != nil {
		return ObjectUnchanged, err
	}

	return ObjectModified, nil
}

// ensureFinalizerDeleted removes finalizer of a Hub
func (c HubAdapter) ensureFinalizerDeleted(ctx context.Context) error {
	c.logger.Info("Deleting Cluster Finalizer")
	return c.deleteFinalizer(ctx, c.mdaiCR, hubFinalizer)
}

// deleteFinalizer deletes finalizer of a generic CR
func (c HubAdapter) deleteFinalizer(ctx context.Context, object client.Object, finalizer string) error {
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

// ensurePrometheusRuleSynchronized creates or updates PrometheusFilter CR
func (c HubAdapter) ensurePrometheusAlertsSynchronized(ctx context.Context) (OperationResult, error) {
	defaultPrometheusRuleName := "mdai-" + c.mdaiCR.Name + "-alert-rules"
	c.logger.Info("EnsurePrometheusRuleSynchronized")

	evals := c.mdaiCR.Spec.PrometheusAlerts

	prometheusRule := &prometheusv1.PrometheusRule{}
	err := c.client.Get(
		ctx,
		client.ObjectKey{Namespace: c.mdaiCR.Namespace, Name: defaultPrometheusRuleName},
		prometheusRule,
	)

	// rules exist, but no evaluations
	if evals == nil {
		c.logger.Info("No evaluations found, skipping PrometheusRule creation")
		if err == nil {
			c.logger.Info("Removing existing rules")
			if deleteErr := c.deletePrometheusRule(ctx); deleteErr != nil {
				c.logger.Error(deleteErr, "Failed to remove existing rules")
			}
		}
		return ContinueProcessing()
	}

	// create new prometheus rule
	if apierrors.IsNotFound(err) {
		prometheusRule = &prometheusv1.PrometheusRule{
			ObjectMeta: metav1.ObjectMeta{
				Name:      defaultPrometheusRuleName,
				Namespace: c.mdaiCR.Namespace,
				Labels: map[string]string{
					LabelManagedByMdaiKey:        LabelManagedByMdaiValue,
					"app.kubernetes.io/part-of":  "kube-prometheus-stack",
					"app.kubernetes.io/instance": c.releaseName,
				},
			},
			Spec: prometheusv1.PrometheusRuleSpec{
				Groups: []prometheusv1.RuleGroup{
					{
						Name:  "mdai",
						Rules: []prometheusv1.Rule{},
					},
				},
			},
		}
		if createErr := c.client.Create(ctx, prometheusRule); createErr != nil {
			c.logger.Error(createErr, "Failed to create PrometheusRule", "prometheus_rule_name", defaultPrometheusRuleName)
			return RequeueAfter(requeueTime, err)
		}
		c.logger.Info("Created new PrometheusRule", "prometheus_rule_name", defaultPrometheusRuleName)
	} else if err != nil {
		c.logger.Error(err, "Failed to get PrometheusRule", "prometheus_rule_name", defaultPrometheusRuleName)
		return RequeueAfter(requeueTime, err)
	}

	if c.mdaiCR.Spec.Config != nil && c.mdaiCR.Spec.Config.EvaluationInterval != nil {
		prometheusRule.Spec.Groups[0].Interval = c.mdaiCR.Spec.Config.EvaluationInterval
	}

	rules := make([]prometheusv1.Rule, 0, len(evals))
	for _, eval := range evals {
		rules = append(rules, c.composePrometheusRule(eval))
	}

	prometheusRule.Spec.Groups[0].Rules = rules
	if err = c.client.Update(ctx, prometheusRule); err != nil {
		c.logger.Error(err, "Failed to update PrometheusRule")
	}

	return ContinueProcessing()
}

func (c HubAdapter) composePrometheusRule(alertingRule mdaiv1.PrometheusAlert) prometheusv1.Rule {
	prometheusRule := prometheusv1.Rule{
		Expr:  alertingRule.Expr,
		Alert: alertingRule.Name,
		For:   alertingRule.For,
		Annotations: map[string]string{
			"alert_name":    alertingRule.Name,
			"hub_name":      c.mdaiCR.Name,
			"current_value": "{{ $value | printf \"%.2f\" }}",
			"expression":    alertingRule.Expr.StrVal,
		},
		Labels: map[string]string{
			"severity": alertingRule.Severity,
		},
	}

	return prometheusRule
}

func (c HubAdapter) deletePrometheusRule(ctx context.Context) error {
	prometheusRuleName := "mdai-" + c.mdaiCR.Name + "-alert-rules"
	prometheusRule := &prometheusv1.PrometheusRule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prometheusRuleName,
			Namespace: c.mdaiCR.Namespace,
		},
	}

	err := c.client.Delete(ctx, prometheusRule)
	if err != nil {
		if apierrors.IsNotFound(err) {
			c.logger.Info("PrometheusRule not found, nothing to delete", "prometheus_rule_name", prometheusRuleName)
			return nil
		}
		c.logger.Error(err, "Failed to delete prometheusRule", "prometheus_rule_name", prometheusRuleName)
		return err
	}

	c.logger.Info("Deleted PrometheusRule", "prometheus_rule_name", prometheusRuleName)
	return nil
}

func (c HubAdapter) handleComputedVariable(ctx context.Context, dataAdapter *vars.ValkeyAdapter, variable mdaiv1.Variable, envMap map[string]string) error {
	//nolint: exhaustive
	switch variable.DataType {
	case mdaiv1.VariableDataTypeSet:
		valueAsSlice, err := dataAdapter.GetSetAsStringSlice(ctx, variable.Key, c.mdaiCR.Name)
		if err != nil {
			return err
		}
		c.applySetTransformation(variable, envMap, valueAsSlice)
	case mdaiv1.VariableDataTypeString, mdaiv1.VariableDataTypeInt, mdaiv1.VariableDataTypeBoolean, mdaiv1.VariableDataTypeFloat:
		// int is represented as string in Valkey, we assume writer guarantee the variable type is correct
		// boolean is represented as string in Valkey: false or true, we assume writer guarantee the variable type is correct
		value, found, err := dataAdapter.GetString(ctx, variable.Key, c.mdaiCR.Name)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		c.applySerializerToString(variable, envMap, value)
	case mdaiv1.VariableDataTypeMap:
		value, err := dataAdapter.GetMapAsString(ctx, variable.Key, c.mdaiCR.Name)
		if err != nil {
			return err
		}
		c.applySerializerToString(variable, envMap, value)
	default:
		c.logger.Error(fmt.Errorf("%w: %s", errUnsupportedVariableDataType, variable.DataType), errUnsupportedVariableDataType.Error(), "key", variable.Key)
	}

	return nil
}

func (c HubAdapter) handleMetaVariable(ctx context.Context, dataAdapter *vars.ValkeyAdapter, variable mdaiv1.Variable, envMap map[string]string) error {
	//nolint: exhaustive
	switch variable.DataType {
	case mdaiv1.MetaVariableDataTypePriorityList:
		valueAsSlice, found, err := dataAdapter.GetOrCreateMetaPriorityList(ctx, variable.Key, c.mdaiCR.Name, variable.VariableRefs)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		c.applySetTransformation(variable, envMap, valueAsSlice)
	case mdaiv1.MetaVariableDataTypeHashSet:
		value, found, err := dataAdapter.GetOrCreateMetaHashSet(ctx, variable.Key, c.mdaiCR.Name, variable.VariableRefs[0], variable.VariableRefs[1])
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		c.applySerializerToString(variable, envMap, value)
	default:
		c.logger.Error(fmt.Errorf("%w: %s", errUnsupportedVariableDataType, variable.DataType), errUnsupportedVariableDataType.Error(), "key", variable.Key)
	}
	return nil
}

func (c HubAdapter) syncValkeyVariables(ctx context.Context, envMap, manualEnvMap map[string]string, valkeyKeysToKeep map[string]struct{}) error {
	dataAdapter := vars.NewValkeyAdapter(c.valKeyClient, c.zapLogger)

	for _, variable := range c.mdaiCR.Spec.Variables {
		c.logger.Info("Processing variable", "key", variable.Key)

		if variable.StorageType != mdaiv1.VariableSourceTypeBuiltInValkey {
			c.logger.Error(fmt.Errorf("%w: %s", errUnsupportedVariableSourceType, variable.StorageType), errUnsupportedVariableSourceType.Error(), "key", variable.Key)
			continue
		}

		key := variable.Key
		valkeyKeysToKeep[key] = struct{}{}

		switch variable.Type {
		case mdaiv1.VariableTypeManual:
			manualEnvMap[key] = string(variable.DataType)
			fallthrough
		case mdaiv1.VariableTypeComputed:
			if err := c.handleComputedVariable(ctx, dataAdapter, variable, envMap); err != nil {
				return err
			}
		case mdaiv1.VariableTypeMeta:
			if err := c.handleMetaVariable(ctx, dataAdapter, variable, envMap); err != nil {
				return err
			}
		default:
			c.logger.Error(fmt.Errorf("%w: %s", errUnsupportedVariableType, variable.Type), errUnsupportedVariableType.Error(), "key", variable.Key)
			continue
		}
	}
	return nil
}

func (c HubAdapter) restartCollectorAndAudit(ctx context.Context, collector v1beta1.OpenTelemetryCollector, envMap map[string]string) error {
	c.logger.Info("Triggering restart of OpenTelemetry Collector(s)")

	if c.opampConnectionManager.IsAgentManaged(collector.Name) {
		// this collector manages restarts with opamp.
		if err := c.opampConnectionManager.DispatchRestartCommand(ctx); err != nil {
			return err
		}
	} else {
		// this collector gets manually restarted.
		collectorCopy := collector.DeepCopy()
		if collectorCopy.Annotations == nil {
			collectorCopy.Annotations = make(map[string]string)
		}
		collectorCopy.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
		if err := c.client.Update(ctx, collectorCopy); err != nil {
			if apierrors.IsConflict(err) {
				c.logger.Info("Conflict while updating OpenTelemetry Collector, will retry", "name", collectorCopy.Name)
				return nil // let controller requeue naturally
			}
			return err
		}
	}

	auditAdapter := audit.NewAuditAdapter(c.zapLogger, c.valKeyClient)
	restartEvent := auditAdapter.CreateRestartEvent(c.mdaiCR.Name, envMap)
	logFields := []any{"mdai-logstream", "audit"}
	for k, v := range restartEvent {
		logFields = append(logFields, k, v)
	}
	c.logger.Info("AUDIT: Triggering restart of OpenTelemetry Collector", logFields...)
	return auditAdapter.InsertAuditLogEventFromMap(ctx, restartEvent)
}

func (c HubAdapter) syncComputedConfigMapsAndRestart(ctx context.Context, envMap map[string]string) (OperationResult, error) {
	collectors, err := c.listOtelCollectorsWithLabel(ctx, fmt.Sprintf("%s=%s", LabelMdaiHubName, c.mdaiCR.Name))
	if err != nil {
		return RequeueWithError(err)
	}

	// determine which namespaces to restart
	namespaceToRestart := map[string]struct{}{}
	for _, collector := range collectors {
		result, _, err := c.createOrUpdateEnvConfigMap(ctx, envMap, envConfigMapNamePostfix, collector.Namespace)
		if err != nil {
			return RequeueWithError(err)
		}
		if result == controllerutil.OperationResultCreated || result == controllerutil.OperationResultUpdated {
			namespaceToRestart[collector.Namespace] = struct{}{}
		}
	}

	// trigger restarts
	for _, collector := range collectors {
		if _, shouldRestart := namespaceToRestart[collector.Namespace]; shouldRestart {
			if err := c.restartCollectorAndAudit(ctx, collector, envMap); err != nil {
				return RequeueWithError(err)
			}
		}
	}
	return ContinueProcessing()
}

//nolint:gocyclo // TODO refactor later
func (c HubAdapter) ensureVariableSynchronized(ctx context.Context) (OperationResult, error) {
	// current assumption is we have only built-in Valkey storage
	variables := c.mdaiCR.Spec.Variables
	if variables == nil {
		c.logger.Info("No variables found in the CR, skipping variable synchronization")
		return ContinueProcessing()
	}

	envMap := make(map[string]string)
	manualEnvMap := make(map[string]string)
	dataAdapter := vars.NewValkeyAdapter(c.valKeyClient, c.zapLogger)
	valkeyKeysToKeep := map[string]struct{}{}

	if err := c.syncValkeyVariables(ctx, envMap, manualEnvMap, valkeyKeysToKeep); err != nil {
		return RequeueWithError(err)
	}

	c.logger.Info("Deleting old valkey keys", "valkeyKeysToKeep", valkeyKeysToKeep)
	if err := dataAdapter.DeleteKeysWithPrefixUsingScan(ctx, valkeyKeysToKeep, c.mdaiCR.Name); err != nil {
		return RequeueOnErrorOrContinue(err)
	}

	// manual variables: we need one ConfigMap for hub in hub's namespace
	if len(manualEnvMap) == 0 {
		c.logger.Info("No manual variables defined in the MDAI CR", "name", c.mdaiCR.Name)
		if err := c.deleteEnvConfigMap(ctx, manualEnvConfigMapNamePostfix, c.mdaiCR.Namespace); err != nil {
			c.logger.Error(err, "Failed to delete manual variables ConfigMap", "name", c.mdaiCR.Name, "namespace", c.mdaiCR.Namespace)
			return ContinueWithError(err)
		}
	} else {
		_, _, err := c.createOrUpdateEnvConfigMap(ctx,
			manualEnvMap,
			manualEnvConfigMapNamePostfix,
			c.mdaiCR.Namespace,
			WithOwnerRef(c.mdaiCR, c.scheme))
		if err != nil {
			return ContinueWithError(err)
		}
	}

	return c.syncComputedConfigMapsAndRestart(ctx, envMap)
}

func (c HubAdapter) applySerializerToString(variable mdaiv1.Variable, envMap map[string]string, value string) {
	if variable.SerializeAs == nil {
		return
	}

	for _, serializer := range *variable.SerializeAs {
		exportedVariableName := serializer.Name
		if _, exists := envMap[exportedVariableName]; exists {
			c.logger.Info("Serializer configuration overrides existing configuration", "exportedVariableName", exportedVariableName)
		}
		envMap[exportedVariableName] = value
	}
}

func (c HubAdapter) applySetTransformation(variable mdaiv1.Variable, envMap map[string]string, valueAsSlice []string) {
	if variable.SerializeAs == nil {
		return
	}

	for _, serializer := range *variable.SerializeAs {
		exportedVariableName := serializer.Name
		if _, exists := envMap[exportedVariableName]; exists {
			c.logger.Info("Serializer configuration overrides existing configuration", "exportedVariableName", exportedVariableName)
			continue
		}

		transformers := serializer.Transformers
		if len(transformers) == 0 {
			c.logger.Info("No Transformers configured", "exportedVariableName", exportedVariableName)
			continue
		}
		for _, transformer := range transformers {
			switch transformer.Type {
			case mdaiv1.TransformerTypeJoin:
				delimiter := transformer.Join.Delimiter
				variableWithDelimiter := strings.Join(valueAsSlice, delimiter)
				envMap[exportedVariableName] = variableWithDelimiter
			default:
				c.logger.Error(fmt.Errorf("%w: %s", errUnsupportedTransformerType, transformer.Type), errUnsupportedTransformerType.Error(), "exportedVariableName", exportedVariableName)
			}
		}
	}
}

func (c HubAdapter) ensureAutomationsSynchronized(ctx context.Context) (OperationResult, error) {
	if c.mdaiCR.Spec.Rules == nil {
		c.logger.Info("No automations defined in the MDAI CR", "name", c.mdaiCR.Name)
		if err := c.deleteEnvConfigMap(ctx, automationConfigMapNamePostfix, c.mdaiCR.Namespace); err != nil {
			c.logger.Error(err, "Failed to delete automations ConfigMap", "name", c.mdaiCR.Name)
			return RequeueOnErrorOrContinue(err)
		}
		return ContinueProcessing()
	}

	c.logger.Info("Creating or updating ConfigMap for automations", "name", c.mdaiCR.Name)
	automationMap := make(map[string]string, len(c.mdaiCR.Spec.Rules))
	for _, automationRule := range c.mdaiCR.Spec.Rules {
		key := automationRule.Name
		trig, err := transformWhenToTrigger(&automationRule.When)
		if err != nil {
			return ContinueWithError(fmt.Errorf("failed to transform when to trigger: %w", err))
		}
		cmds, err := transformThenToCommands(automationRule.Then)
		if err != nil {
			return ContinueWithError(fmt.Errorf("failed to transform then to command: %w", err))
		}
		rule := events.Rule{
			Name:     automationRule.Name,
			Trigger:  trig,
			Commands: cmds,
		}
		ruleJSON, err := json.Marshal(rule)
		if err != nil {
			return OperationResult{}, fmt.Errorf("failed to marshal automationRule workflow: %w", err)
		}
		automationMap[key] = string(ruleJSON)
	}

	operationResult, _, err := c.createOrUpdateEnvConfigMap(ctx,
		automationMap,
		automationConfigMapNamePostfix,
		c.mdaiCR.Namespace,
		WithOwnerRef(c.mdaiCR, c.scheme)) // TODO double check this line
	c.logger.Info(fmt.Sprintf("Successfully %s ConfigMap for automations", operationResult), "name", c.mdaiCR.Name)
	if err != nil {
		return RequeueOnErrorOrContinue(err)
	}

	return ContinueProcessing()
}

func (c HubAdapter) ensureSynchronized(ctx context.Context) (OperationResult, error) {
	if operationResult, err := c.ensurePrometheusAlertsSynchronized(ctx); err != nil {
		return operationResult, err
	}
	if operationResult, err := c.ensureAutomationsSynchronized(ctx); err != nil {
		return operationResult, err
	}
	return c.ensureVariableSynchronized(ctx)
}

func (c HubAdapter) deleteEnvConfigMap(ctx context.Context, postfix string, namespace string) error {
	configMapName := c.mdaiCR.Name + postfix
	c.logger.Info("Deleting ConfigMap", "name", configMapName, "namespace", namespace)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: namespace,
		},
	}

	if err := c.client.Delete(ctx, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			c.logger.Info("ConfigMap not found, skipping deletion", "name", configMapName, "namespace", namespace)
			return nil
		}
		c.logger.Error(err, "Failed to delete ConfigMap", "name", configMapName, "namespace", namespace)
		return fmt.Errorf("failed to delete ConfigMap: %w", err)
	}
	return nil
}

type CMOption func(cm *corev1.ConfigMap) error

func WithOwnerRef(owner metav1.Object, scheme *runtime.Scheme) CMOption {
	return func(cm *corev1.ConfigMap) error {
		return controllerutil.SetControllerReference(owner, cm, scheme)
	}
}

func (c HubAdapter) createOrUpdateEnvConfigMap(
	ctx context.Context,
	envMap map[string]string,
	configMapPostfix string,
	namespace string,
	opts ...CMOption,
) (controllerutil.OperationResult, *corev1.ConfigMap, error) {
	envConfigMapName := c.mdaiCR.Name + configMapPostfix
	desiredConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      envConfigMapName,
			Namespace: namespace,
			Labels: map[string]string{
				LabelManagedByMdaiKey: LabelManagedByMdaiValue,
				LabelMdaiHubName:      c.mdaiCR.Name,
				configMapTypeLabel:    fmt.Sprintf("hub%v", configMapPostfix),
			},
		},
	}

	// we are not setting an owner reference here as we want to allow config maps being deployed across namespaces
	operationResult, err := controllerutil.CreateOrUpdate(ctx, c.client, desiredConfigMap, func() error {
		desiredConfigMap.Data = envMap
		for _, opt := range opts {
			if err := opt(desiredConfigMap); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.logger.Error(err, "Failed to create or update ConfigMap", "name", envConfigMapName, "namespace", namespace)
		return controllerutil.OperationResultNone, nil, fmt.Errorf("failed to create or update ConfigMap: %w", err)
	}

	c.logger.Info("Successfully created or updated ConfigMap", "name", envConfigMapName, "namespace", namespace, "operation", operationResult)
	return operationResult, desiredConfigMap, nil
}

func (c HubAdapter) listOtelCollectorsWithLabel(ctx context.Context, labelSelector string) ([]v1beta1.OpenTelemetryCollector, error) {
	var collectorList v1beta1.OpenTelemetryCollectorList

	selector, err := labels.Parse(labelSelector)
	if err != nil {
		return nil, fmt.Errorf("failed to parse label selector: %w", err)
	}

	listOptions := &client.ListOptions{
		LabelSelector: selector,
	}

	if err := c.client.List(ctx, &collectorList, listOptions); err != nil {
		return nil, fmt.Errorf("failed to list OpenTelemetryCollectors: %w", err)
	}

	return collectorList.Items, nil
}

// ensureDeletionProcessed deletes Hub in cases a deletion was triggered
func (c HubAdapter) ensureDeletionProcessed(ctx context.Context) (OperationResult, error) {
	if c.mdaiCR.DeletionTimestamp.IsZero() {
		return ContinueProcessing()
	}
	c.logger.Info("Deleting Hub:" + c.mdaiCR.Name)
	crState, err := c.finalize(ctx)
	if crState == ObjectUnchanged || err != nil {
		c.logger.Info("Has to requeue mdai")
		return RequeueAfter(requeueTime, err)
	}
	return StopProcessing()
}

func getConfigMapSHA(config corev1.ConfigMap) (string, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (c HubAdapter) ensureStatusSetToDone(ctx context.Context) (OperationResult, error) {
	// Re-fetch the Custom Resource after update or create
	if err := c.client.Get(ctx, types.NamespacedName{Name: c.mdaiCR.Name, Namespace: c.mdaiCR.Namespace}, c.mdaiCR); err != nil {
		c.logger.Error(err, "Failed to re-fetch MdaiHub")
		return Requeue()
	}
	meta.SetStatusCondition(&c.mdaiCR.Status.Conditions, metav1.Condition{
		Type:   typeAvailableHub,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: "reconciled successfully",
	})
	if err := c.client.Status().Update(ctx, c.mdaiCR); err != nil {
		if apierrors.ReasonForError(err) == metav1.StatusReasonConflict {
			c.logger.Info("re-queuing due to resource conflict")
			return Requeue()
		}
		c.logger.Error(err, "Failed to update mdai hub status")
		return Requeue()
	}
	c.logger.Info("Status set to done for mdai hub", "mdaiHub", c.mdaiCR.Name)
	return ContinueProcessing()
}
