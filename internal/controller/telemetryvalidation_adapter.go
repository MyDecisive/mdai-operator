package controller

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	hubv1 "github.com/mydecisive/mdai-operator/api/v1"
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

func (c *TelemetryValidationAdapter) ensureSynchronized(ctx context.Context) (OperationResult, error) {
	validatorName, validatorServiceName, resolvedValidatorEndpoint, validatorIngressPort, err := c.reconciler.reconcileValidator(ctx, c.validation)
	if err != nil {
		return ContinueWithError(err)
	}

	c.validatorName = validatorName
	c.validatorServiceName = validatorServiceName
	c.resolvedValidatorEndpoint = resolvedValidatorEndpoint
	c.validatorIngressPortStatus = validatorIngressPort

	return c.reconciler.reconcileShadowCollector(
		ctx,
		c.validation,
		c.validatorName,
		c.validatorServiceName,
		c.resolvedValidatorEndpoint,
		c.validatorIngressPortStatus,
	)
}

func (*TelemetryValidationAdapter) finalize(context.Context) (ObjectState, error) {
	return ObjectUnchanged, nil
}

func (*TelemetryValidationAdapter) deleteFinalizer(context.Context, ctrlclient.Object, string) error {
	return nil
}
