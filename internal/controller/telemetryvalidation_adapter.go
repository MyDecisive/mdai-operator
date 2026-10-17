package controller

import (
	"context"
	"errors"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/mydecisive/mdai-operator/internal/collectorconfig"
)

var _ Adapter = (*TelemetryValidationAdapter)(nil)

type TelemetryValidationAdapter struct {
	validation *hubv1.TelemetryValidation
	logger     logr.Logger
	client     ctrlclient.Client
	scheme     *runtime.Scheme

	reconciler *TelemetryValidationReconciler

	validatorName              string
	validatorServiceName       string
	resolvedValidatorEndpoint  string
	validatorIngressPortStatus int32
	resolvedRunID              string
}

func NewTelemetryValidationAdapter(
	validation *hubv1.TelemetryValidation,
	log logr.Logger,
	k8sClient ctrlclient.Client,
	scheme *runtime.Scheme,
	reconciler *TelemetryValidationReconciler,
) *TelemetryValidationAdapter {
	return &TelemetryValidationAdapter{
		validation: validation,
		logger:     log,
		client:     k8sClient,
		scheme:     scheme,
		reconciler: reconciler,
	}
}

func (*TelemetryValidationAdapter) ensureDeletionProcessed(context.Context) (OperationResult, error) {
	return ContinueProcessing()
}

func (*TelemetryValidationAdapter) ensureFinalizerInitialized(context.Context) (OperationResult, error) {
	return ContinueProcessing()
}

func (*TelemetryValidationAdapter) ensureFinalizerDeleted(context.Context) error {
	return nil
}

func (*TelemetryValidationAdapter) ensureStatusInitialized(context.Context) (OperationResult, error) {
	return ContinueProcessing()
}

func (*TelemetryValidationAdapter) ensureStatusSetToDone(context.Context) (OperationResult, error) {
	return ContinueProcessing()
}

func (c *TelemetryValidationAdapter) ensureRunIDResolved(ctx context.Context) (OperationResult, error) {
	runID, shouldUpdateStatus, err := resolveTelemetryValidationRunID(c.validation)
	if err != nil {
		return ContinueWithError(err)
	}
	c.resolvedRunID = runID
	c.validation.Status.RunID = runID

	if !shouldUpdateStatus {
		return ContinueProcessing()
	}

	metaCopy := c.validation.DeepCopy()
	metaCopy.Status.RunID = runID
	if err := c.reconciler.Status().Update(ctx, metaCopy); err != nil {
		return ContinueWithError(err)
	}
	c.validation = metaCopy
	return ContinueProcessing()
}

func (c *TelemetryValidationAdapter) ensureSynchronized(ctx context.Context) (OperationResult, error) {
	if !c.validation.DeletionTimestamp.IsZero() {
		c.logger.Info("TelemetryValidation is being deleted, skipping synchronization")
		return ContinueProcessing()
	}

	validatorName, validatorServiceName, resolvedValidatorEndpoint, validatorIngressPort, unresolvedErr := c.reconciler.reconcileValidator(
		ctx,
		c.validation,
		c.resolvedRunID,
	)
	if unresolvedErr != nil && !errors.Is(unresolvedErr, collectorconfig.ErrUnresolvedEndpoint) {
		return ContinueWithError(unresolvedErr)
	}

	c.validatorName = validatorName
	c.validatorServiceName = validatorServiceName
	c.resolvedValidatorEndpoint = resolvedValidatorEndpoint
	c.validatorIngressPortStatus = validatorIngressPort

	result, err := c.reconciler.reconcileShadowCollector(
		ctx,
		c.validation,
		c.validatorName,
		c.validatorServiceName,
		c.resolvedValidatorEndpoint,
		c.validatorIngressPortStatus,
		c.resolvedRunID,
	)
	if err != nil || result.RequeueOrCancel() || unresolvedErr == nil {
		return result, err
	}

	c.logger.Error(unresolvedErr, "using fallback validator ingress ports for unresolvable source collector endpoints; retrying",
		"retryAfter", unresolvedEndpointRetryInterval)
	return RequeueAfter(unresolvedEndpointRetryInterval, nil)
}

func (*TelemetryValidationAdapter) finalize(context.Context) (ObjectState, error) {
	return ObjectUnchanged, nil
}

func (*TelemetryValidationAdapter) deleteFinalizer(context.Context, ctrlclient.Object, string) error {
	return nil
}
