package controller

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

type ReconcileOperation func(context.Context) (OperationResult, error)

type OperationResult struct {
	RequeueDelay   time.Duration
	RequeueRequest bool
	CancelRequest  bool
}

func (r OperationResult) RequeueOrCancel() bool {
	return r.RequeueRequest || r.CancelRequest
}

func ContinueOperationResult() OperationResult {
	return OperationResult{
		RequeueDelay:   0,
		RequeueRequest: false,
		CancelRequest:  false,
	}
}

func StopOperationResult() OperationResult {
	return OperationResult{
		RequeueDelay:   0,
		RequeueRequest: false,
		CancelRequest:  true,
	}
}

func StopProcessing() (OperationResult, error) {
	return StopOperationResult(), nil
}

func Requeue() (OperationResult, error) {
	return OperationResult{
		RequeueDelay:   0,
		RequeueRequest: true,
		CancelRequest:  false,
	}, nil
}

func RequeueWithError(errIn error) (OperationResult, error) {
	return OperationResult{
		RequeueDelay:   0,
		RequeueRequest: true,
		CancelRequest:  false,
	}, errIn
}

func ContinueWithError(errIn error) (OperationResult, error) {
	return OperationResult{
		RequeueDelay:   0,
		RequeueRequest: false,
		CancelRequest:  false,
	}, errIn
}

func RequeueOnErrorOrStop(errIn error) (OperationResult, error) {
	return OperationResult{
		RequeueDelay:   0,
		RequeueRequest: false,
		CancelRequest:  true,
	}, errIn
}

func RequeueOnErrorOrContinue(errIn error) (OperationResult, error) {
	return OperationResult{
		RequeueDelay:   0,
		RequeueRequest: false,
		CancelRequest:  false,
	}, errIn
}

func RequeueAfter(delay time.Duration, errIn error) (OperationResult, error) {
	return OperationResult{
		RequeueDelay:   delay,
		RequeueRequest: true,
		CancelRequest:  false,
	}, errIn
}

func ContinueProcessing() (OperationResult, error) {
	return ContinueOperationResult(), nil
}

func RunReconcileOperations(ctx context.Context, operations []ReconcileOperation) (ctrl.Result, error) {
	for _, operation := range operations {
		result, err := operation(ctx)
		if err != nil {
			if result.RequeueRequest && result.RequeueDelay > 0 {
				return ctrl.Result{RequeueAfter: result.RequeueDelay}, nil
			}
			return ctrl.Result{}, err
		}
		if result.RequeueRequest {
			return ctrl.Result{RequeueAfter: result.RequeueDelay}, nil
		}
		if result.CancelRequest {
			return ctrl.Result{}, nil
		}
	}

	return ctrl.Result{}, nil
}
