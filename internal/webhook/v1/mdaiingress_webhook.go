package v1

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/mydecisive/mdai-operator/internal/controller"
)

// nolint:unused
// log is for logging in this package.
var mdaiIngresslog = logf.Log.WithName("mdaiingress-resource")

// SetupMdaiIngressWebhookWithManager registers the webhook for MdaiIngress in the manager.
func SetupMdaiIngressWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &mdaiv1.MdaiIngress{}).
		WithCustomValidator(&MdaiIngressCustomValidator{client: mgr.GetClient()}).
		WithCustomDefaulter(&MdaiIngressCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-hub-mydecisive-ai-v1-mdaiingress,mutating=true,failurePolicy=fail,sideEffects=None,groups=hub.mydecisive.ai,resources=mdaiingresses,verbs=create;update,versions=v1,name=mmdaiingress-v1.kb.io,admissionReviewVersions=v1

// MdaiIngressCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind MdaiIngress when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type MdaiIngressCustomDefaulter struct{}

var _ webhook.CustomDefaulter = &MdaiIngressCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind MdaiIngress.
//
//revive:disable:unused-receiver
func (d *MdaiIngressCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	mdaiIngress, ok := obj.(*mdaiv1.MdaiIngress)

	if !ok {
		return fmt.Errorf("expected an MdaiIngress object but got %T", obj)
	}
	mdaiIngresslog.Info("Defaulting for MdaiIngress", "name", mdaiIngress.GetName())

	if mdaiIngress.Spec.CloudType == "" {
		mdaiIngress.Spec.CloudType = mdaiv1.CloudProviderAws
	}

	if mdaiIngress.Spec.GrpcService == nil {
		mdaiIngress.Spec.GrpcService = &mdaiv1.IngressService{Type: corev1.ServiceTypeNodePort}
	} else if mdaiIngress.Spec.GrpcService.Type == "" {
		mdaiIngress.Spec.GrpcService.Type = corev1.ServiceTypeNodePort
	}

	if mdaiIngress.Spec.NonGrpcService == nil {
		mdaiIngress.Spec.NonGrpcService = &mdaiv1.IngressService{Type: corev1.ServiceTypeLoadBalancer}
	} else if mdaiIngress.Spec.NonGrpcService.Type == "" {
		mdaiIngress.Spec.NonGrpcService.Type = corev1.ServiceTypeLoadBalancer
	}

	return nil
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-hub-mydecisive-ai-v1-mdaiingress,mutating=false,failurePolicy=fail,sideEffects=None,groups=hub.mydecisive.ai,resources=mdaiingresses,verbs=create;update,versions=v1,name=vmdaiingress-v1.kb.io,admissionReviewVersions=v1

// MdaiIngressCustomValidator struct is responsible for validating the MdaiIngress resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type MdaiIngressCustomValidator struct {
	client client.Client
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type MdaiIngress.
func (v *MdaiIngressCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	mdaiIngress, ok := obj.(*mdaiv1.MdaiIngress)
	if !ok {
		return nil, fmt.Errorf("expected an MdaiIngress, received %T", obj)
	}
	warnings, err := v.Validate(ctx, mdaiIngress)
	if err != nil {
		return warnings, err
	}
	return warnings, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type MdaiIngress.
func (v *MdaiIngressCustomValidator) ValidateUpdate(ctx context.Context, _ runtime.Object, newObj runtime.Object) (admission.Warnings, error) {
	mdaiIngress, ok := newObj.(*mdaiv1.MdaiIngress)
	if !ok {
		return nil, fmt.Errorf("expected an MdaiIngress, received %T", newObj)
	}
	warnings, err := v.Validate(ctx, mdaiIngress)
	if err != nil {
		return warnings, err
	}
	return warnings, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type MdaiINgress.
func (v *MdaiIngressCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return admission.Warnings{}, nil
}

func (v *MdaiIngressCustomValidator) Validate(ctx context.Context, mdaiIngress *mdaiv1.MdaiIngress) (admission.Warnings, error) {
	warnings := admission.Warnings{}
	if v.client == nil {
		return warnings, errors.New("webhook client not initialized")
	}

	var list mdaiv1.MdaiIngressList
	if err := v.client.List(ctx, &list,
		client.InNamespace(mdaiIngress.Namespace),
		client.MatchingFields{controller.MdaiIngressOtelColLookupKey: mdaiIngress.Spec.OtelCollector.Name},
	); err != nil {
		return warnings, apierrors.NewInternalError(err)
	}

	for _, other := range list.Items {
		if other.Name == mdaiIngress.Name && other.Namespace == mdaiIngress.Namespace {
			continue
		}
		return warnings, fmt.Errorf("OtelCol reference is already used by MdaiIngress %s/%s",
			other.Namespace, other.Name)
	}

	return warnings, nil
}
