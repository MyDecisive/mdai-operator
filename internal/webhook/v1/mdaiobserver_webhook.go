package v1

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// nolint:unused
// log is for logging in this package.
var mdaiobserverlog = logf.Log.WithName("mdaiobserver-resource")

// SetupMdaiObserverWebhookWithManager registers the webhook for MdaiObserver in the manager.
func SetupMdaiObserverWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &mdaiv1.MdaiObserver{}).
		WithCustomValidator(&MdaiObserverCustomValidator{}).
		Complete()
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-hub-mydecisive-ai-v1-mdaiobserver,mutating=false,failurePolicy=fail,sideEffects=None,groups=hub.mydecisive.ai,resources=mdaiobservers,verbs=create;update,versions=v1,name=vmdaiobserver-v1.kb.io,admissionReviewVersions=v1

// MdaiObserverCustomValidator struct is responsible for validating the MdaiObserver resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type MdaiObserverCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &MdaiObserverCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type MdaiObserver.
func (v *MdaiObserverCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	mdaiobserver, ok := obj.(*mdaiv1.MdaiObserver)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiObserver object but got %T", obj)
	}
	mdaiobserverlog.Info("Validation for MdaiObserver upon creation", "name", mdaiobserver.GetName())

	return v.validateObserversAndObserverResources(mdaiobserver)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type MdaiObserver.
func (v *MdaiObserverCustomValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	mdaiobserver, ok := newObj.(*mdaiv1.MdaiObserver)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiObserver object for the newObj but got %T", newObj)
	}
	mdaiobserverlog.Info("Validation for MdaiObserver upon update", "name", mdaiobserver.GetName())

	return v.validateObserversAndObserverResources(mdaiobserver)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type MdaiObserver.
func (*MdaiObserverCustomValidator) ValidateDelete(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	mdaiobserver, ok := obj.(*mdaiv1.MdaiObserver)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiObserver object but got %T", obj)
	}
	mdaiobserverlog.Info("Validation for MdaiObserver upon deletion", "name", mdaiobserver.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}

func (*MdaiObserverCustomValidator) validateObserversAndObserverResources(mdaiobserver *mdaiv1.MdaiObserver) (admission.Warnings, error) {
	newWarnings := admission.Warnings{}
	observers := mdaiobserver.Spec.Observers
	observerSpec := mdaiobserver.Spec

	if observerSpec.ObserverResource.Resources == nil {
		newWarnings = append(newWarnings, "ObserverResource "+mdaiobserver.Name+" does not define resource requests/limits")
	}

	if len(observers) == 0 {
		return append(newWarnings, "Observers are not specified"), nil
	}
	for _, observer := range observers {
		if observer.BytesMetricName == nil && observer.CountMetricName == nil {
			return newWarnings, fmt.Errorf("observer %s must have either a bytesMetricName or countMetricName", observer.Name)
		}
		if len(observer.LabelResourceAttributes) == 0 {
			newWarnings = append(newWarnings, "observer "+observer.Name+" does not define any labels to apply to counts")
		}
	}
	return newWarnings, nil
}
