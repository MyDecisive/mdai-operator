package v1

import (
	"context"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"strings"

	"github.com/prometheus/prometheus/promql/parser"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// nolint:unused
// log is for logging in this package.
var mdaihublog = logf.Log.WithName("mdaihub-resource")

var allowedHTTP = sets.NewString("GET", "POST", "PUT", "PATCH", "DELETE")

// hop-by-hop / managed headers we don't allow users to override for safety reason
var forbiddenHeaders = sets.NewString("Host", "Content-Length", "Transfer-Encoding")

// SetupMdaiHubWebhookWithManager registers the webhook for MdaiHub in the manager.
func SetupMdaiHubWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&mdaiv1.MdaiHub{}).
		WithValidator(&MdaiHubCustomValidator{}).
		WithDefaulter(&MdaiHubCustomDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-hub-mydecisive-ai-v1-mdaihub,mutating=true,failurePolicy=fail,sideEffects=None,groups=hub.mydecisive.ai,resources=mdaihubs,verbs=create;update,versions=v1,name=mmdaihub-v1.kb.io,admissionReviewVersions=v1

// MdaiHubCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind MdaiHub when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type MdaiHubCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &MdaiHubCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind MdaiHub.
func (*MdaiHubCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	mdaihub, ok := obj.(*mdaiv1.MdaiHub)

	if !ok {
		return fmt.Errorf("expected an MdaiHub object but got %T", obj)
	}
	mdaihublog.Info("Defaulting for MdaiHub", "name", mdaihub.GetName())

	// a placeholder for defaulting logic.

	return nil
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-hub-mydecisive-ai-v1-mdaihub,mutating=false,failurePolicy=fail,sideEffects=None,groups=hub.mydecisive.ai,resources=mdaihubs,verbs=create;update,versions=v1,name=vmdaihub-v1.kb.io,admissionReviewVersions=v1

// MdaiHubCustomValidator struct is responsible for validating the MdaiHub resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type MdaiHubCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &MdaiHubCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type MdaiHub.
func (v *MdaiHubCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	mdaihub, ok := obj.(*mdaiv1.MdaiHub)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiHub object but got %T", obj)
	}
	mdaihublog.Info("Validation for MdaiHub upon creation", "name", mdaihub.GetName())

	return v.Validate(mdaihub)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type MdaiHub.
func (v *MdaiHubCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	newHub, ok := newObj.(*mdaiv1.MdaiHub)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiHub object for the newObj but got %T", newObj)
	}
	oldHub, ok := oldObj.(*mdaiv1.MdaiHub)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiHub object for the oldObj but got %T", oldHub)
	}

	if errorList := validateMetaVarRefs(oldHub.Spec.Variables, newHub.Spec.Variables); len(errorList) > 0 {
		return nil, apierrors.NewInvalid(
			schema.GroupKind{Group: mdaiv1.GroupVersion.Group, Kind: "MdaiHub"},
			newHub.GetName(),
			errorList,
		)
	}

	mdaihublog.Info("Validation for MdaiHub upon update", "name", newHub.GetName())
	return v.Validate(newHub)
}

// validateMetaVarRefs ensures meta variable references are immutable between old and new.
func validateMetaVarRefs(oldVars, newVars []mdaiv1.Variable) field.ErrorList {
	if len(oldVars) == 0 || len(newVars) == 0 {
		return nil
	}

	oldVariablesMap := make(map[string]mdaiv1.Variable, len(oldVars))
	for _, oldVariables := range oldVars {
		if oldVariables.Type == mdaiv1.VariableTypeMeta {
			oldVariablesMap[oldVariables.Key] = oldVariables
		}
	}

	errorList := field.ErrorList{}
	for index, newVariable := range newVars {
		if newVariable.Type != mdaiv1.VariableTypeMeta {
			continue
		}
		if oldVariable, found := oldVariablesMap[newVariable.Key]; found {
			if !reflect.DeepEqual(oldVariable.VariableRefs, newVariable.VariableRefs) {
				variableRefsPath := field.NewPath("spec", "variables").Index(index).Child("variableRefs")
				errorList = append(errorList, field.Forbidden(
					variableRefsPath,
					"meta variable references must not change; delete and recreate the variable to update references",
				))
			}
		}
	}
	return errorList
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type MdaiHub.
func (*MdaiHubCustomValidator) ValidateDelete(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	mdaihub, ok := obj.(*mdaiv1.MdaiHub)
	if !ok {
		return nil, fmt.Errorf("expected a MdaiHub object but got %T", obj)
	}
	mdaihublog.Info("Validation for MdaiHub upon deletion", "name", mdaihub.GetName())

	// TODO(user): fill in your validation logic upon object deletion.
	return nil, nil
}

func (v *MdaiHubCustomValidator) Validate(mdaihub *mdaiv1.MdaiHub) (admission.Warnings, error) {
	allWarnings, allErrs := v.validateVariables(mdaihub)

	evaluations := mdaihub.Spec.PrometheusAlerts
	if len(evaluations) == 0 {
		allWarnings = append(allWarnings, "no `PrometheusAlerts` provided; MdaiHub will not setup alerts")
	} else {
		for _, evaluation := range evaluations {
			if _, err := parser.ParseExpr(evaluation.Expr.StrVal); err != nil {
				return allWarnings, err
			}
		}
	}

	warnings, errs := v.validateAutomations(mdaihub)
	allWarnings = append(allWarnings, warnings...)
	allErrs = append(allErrs, errs...)

	if len(allErrs) == 0 {
		return allWarnings, nil
	}

	return allWarnings, apierrors.NewInvalid(schema.GroupKind{Group: mdaiv1.GroupVersion.Group, Kind: "MdaiHub"}, mdaihub.GetName(), allErrs)
}

func (*MdaiHubCustomValidator) validateAutomations(mdaihub *mdaiv1.MdaiHub) (admission.Warnings, field.ErrorList) {
	warnings := admission.Warnings{}
	errs := field.ErrorList{}

	rules := mdaihub.Spec.Rules
	if len(rules) == 0 {
		warnings = append(warnings, "no `Rules` provided; MdaiHub will perform no actions")
		return warnings, nil
	}

	vars := mdaihub.Spec.Variables
	varKeys := make(map[string]struct{}, len(vars))
	for i := range vars {
		key := vars[i].Key
		if key == "" {
			continue
		}
		varKeys[key] = struct{}{}
	}

	alerts := mdaihub.Spec.PrometheusAlerts
	alertNames := make(map[string]struct{}, len(alerts))
	for i := range alerts {
		key := alerts[i].Name
		if key == "" {
			continue
		}
		alertNames[key] = struct{}{}
	}

	specPath := field.NewPath("spec")
	for i, rule := range mdaihub.Spec.Rules {
		rulePath := specPath.Child("rules").Index(i)
		errs = append(errs, validateWhen(rulePath.Child("when"), rule.When, varKeys, alertNames)...)

		if len(rule.Then) == 0 {
			errs = append(errs, field.Required(rulePath.Child("then"), "at least 1 action is required"))
			continue
		}
		for j, act := range rule.Then {
			errs = append(errs, validateAction(rulePath.Child("then").Index(j), act, varKeys)...)
		}
	}

	return warnings, errs
}

func validateWhen(path *field.Path, when mdaiv1.When, knownVarKeys map[string]struct{}, knownAlertNames map[string]struct{}) field.ErrorList {
	var errs field.ErrorList

	hasAlertName := strings.TrimSpace(ptr.Deref(when.AlertName, "")) != ""
	hasStatus := strings.TrimSpace(ptr.Deref(when.Status, "")) != ""
	hasVariableUpdated := strings.TrimSpace(ptr.Deref(when.VariableUpdated, "")) != ""
	hasUpdateType := strings.TrimSpace(ptr.Deref(when.UpdateType, "")) != ""

	if hasStatus && !hasAlertName {
		errs = append(errs, field.Invalid(path, "<alert>", "alertName and status must be set together"))
	}

	if hasUpdateType && !hasVariableUpdated {
		errs = append(errs, field.Invalid(path.Child("updateType"), *when.UpdateType, "can only be set when variableUpdated is set"))
	}

	// Existence checks
	if hasVariableUpdated && !hasKey(knownVarKeys, *when.VariableUpdated) {
		errs = append(errs, field.Invalid(path.Child("variableUpdated"), *when.VariableUpdated, "not defined in spec.variables"))
	}
	if hasAlertName && !hasKey(knownAlertNames, *when.AlertName) {
		errs = append(errs, field.Invalid(path.Child("alertName"), *when.AlertName, "not defined in spec.prometheusAlerts"))
	}

	return errs
}

func hasKey(knownVarKeys map[string]struct{}, key string) bool {
	_, ok := knownVarKeys[key]
	return ok
}

func validateAction(actionPath *field.Path, action mdaiv1.Action, knownVarKeys map[string]struct{}) field.ErrorList {
	const (
		errAtLeastOneAction = "at least one action must be specified"
		errOnlyOneAction    = "only one action may be specified"
	)

	actions := []struct {
		key      string
		present  bool
		validate func() field.ErrorList
	}{
		{
			key:     "addToSet",
			present: action.AddToSet != nil,
			validate: func() field.ErrorList {
				return validateVariableAction(actionPath.Child("addToSet"), action.AddToSet.Set, knownVarKeys, "set")
			},
		},
		{
			key:     "removeFromSet",
			present: action.RemoveFromSet != nil,
			validate: func() field.ErrorList {
				return validateVariableAction(actionPath.Child("removeFromSet"), action.RemoveFromSet.Set, knownVarKeys, "set")
			},
		},
		{
			key:     "setVariable",
			present: action.SetVariable != nil,
			validate: func() field.ErrorList {
				return validateVariableAction(actionPath.Child("setVariable"), action.SetVariable.Scalar, knownVarKeys, "scalar")
			},
		},
		{
			key:     "addToMap",
			present: action.AddToMap != nil,
			validate: func() field.ErrorList {
				return validateMapAction(actionPath.Child("addToMap"), action.AddToMap, knownVarKeys)
			},
		},
		{
			key:     "removeFromMap",
			present: action.RemoveFromMap != nil,
			validate: func() field.ErrorList {
				return validateMapAction(actionPath.Child("removeFromMap"), action.RemoveFromMap, knownVarKeys)
			},
		},
		{
			key:     "callWebhook",
			present: action.CallWebhook != nil,
			validate: func() field.ErrorList {
				return validateWebhookCall(actionPath.Child("callWebhook"), action.CallWebhook)
			},
		},
		{
			key:     "deployReplay",
			present: action.DeployReplay != nil,
			validate: func() field.ErrorList {
				return validateDeployReplayAction(actionPath.Child("deployReplay"), action.DeployReplay, knownVarKeys)
			},
		},
		{
			key:     "cleanUpReplay",
			present: action.CleanUpReplay != nil,
			validate: func() field.ErrorList {
				return validateCleanUpReplayAction(actionPath.Child("cleanUpReplay"), action.CleanUpReplay, knownVarKeys)
			},
		},
	}

	// find exactly one present; short-circuit on multi
	presentIdx := -1
	for i, a := range actions {
		if a.present {
			if presentIdx != -1 {
				return field.ErrorList{field.Invalid(actionPath, "<action>", errOnlyOneAction)}
			}
			presentIdx = i
		}
	}

	if presentIdx == -1 {
		return field.ErrorList{field.Invalid(actionPath, "<action>", errAtLeastOneAction)}
	}

	return actions[presentIdx].validate() // nolint: gosec
}

func validateVariableAction(path *field.Path, variableKey string, knownVarKeys map[string]struct{}, childPath string) field.ErrorList {
	// validation for non-empty value is done at the CRD level
	if !hasKey(knownVarKeys, variableKey) {
		return field.ErrorList{
			field.Invalid(path.Child(childPath), variableKey, "not defined in spec.variables"),
		}
	}
	return nil
}

func validateMapAction(p *field.Path, a *mdaiv1.MapAction, knownVarKeys map[string]struct{}) field.ErrorList {
	var errs field.ErrorList

	mapName := a.Map
	if !hasKey(knownVarKeys, mapName) {
		errs = append(errs, field.Invalid(p.Child("map"), mapName, "not defined in spec.variables"))
	}

	// Require value for addToMap; allow it to be omitted for removeFromMap.
	isAdd := strings.HasSuffix(p.String(), ".addToMap")
	if isAdd {
		if a.Value == nil || strings.TrimSpace(ptr.Deref(a.Value, "")) == "" {
			errs = append(errs, field.Required(p.Child("value"), "required for addToMap"))
		}
	}

	return errs
}

func validateDeployReplayAction(p *field.Path, a *mdaiv1.DeployReplayAction, knownVarKeys map[string]struct{}) field.ErrorList {
	var errs field.ErrorList

	if _, ok := knownVarKeys[a.ReplaySpec.StatusVariableRef]; !ok {
		errs = append(errs, field.Invalid(p.Child("replaySpec.statusVariableRef"), a.ReplaySpec.StatusVariableRef, "does not reference a known variable"))
	}

	if _, replaySpecErrs := validateReplaySpec(a.ReplaySpec, HubAutomationValidatorMode); replaySpecErrs != nil {
		errs = append(errs, field.Invalid(p.Child("replaySpec"), a.ReplaySpec, fmt.Sprintf("invalid replay spec with errors: %v", replaySpecErrs)))
	}

	return errs
}

func validateCleanUpReplayAction(p *field.Path, a *mdaiv1.CleanUpReplayAction, knownVarKeys map[string]struct{}) field.ErrorList {
	var errs field.ErrorList

	// TODO: No fields to validate on CleanUpReplayAction yet. Implement this when we have fields

	return errs
}

func validateWebhookCall(callPath *field.Path, call *mdaiv1.CallWebhookAction) field.ErrorList {
	var errs field.ErrorList

	// Validate URL only when provided as a literal
	if call.URL.Value != nil {
		endpoint := strings.TrimSpace(*call.URL.Value)
		if endpoint == "" {
			errs = append(errs, field.Required(callPath.Child("url"), "required"))
		} else if !isValidURL(endpoint) {
			errs = append(errs, field.Invalid(callPath.Child("url"), endpoint, "must be an absolute http(s) URL"))
		}
	}

	if m := call.Method; m != "" && !allowedHTTP.Has(m) {
		errs = append(errs, field.NotSupported(callPath.Child("method"), m, allowedHTTP.UnsortedList()))
	}

	// If payloadTemplate is present, it must not be used with slackAlertTemplate and must use POST
	ref := call.TemplateRef
	if call.PayloadTemplate != nil {
		if ref == mdaiv1.TemplateRefSlack {
			errs = append(errs, field.Forbidden(callPath.Child("payloadTemplate"), "must be empty when templateRef=slackAlertTemplate"))
		}
		if call.Method != "" && call.Method != http.MethodPost {
			errs = append(errs, field.Invalid(callPath.Child("method"), call.Method, "payloadTemplate requires POST"))
		}
	}

	// If templateRef=jsonTemplate, require a payloadTemplate
	if ref == "jsonTemplate" && call.PayloadTemplate == nil {
		errs = append(errs, field.Required(callPath.Child("payloadTemplate"), "required when templateRef=jsonTemplate"))
	}

	// forbid managed headers in headers (case-insensitive, canonicalize)
	for hk := range call.Headers {
		key := textproto.CanonicalMIMEHeaderKey(hk)
		if forbiddenHeaders.Has(key) {
			errs = append(errs, field.Forbidden(callPath.Child("headers").Key(hk), fmt.Sprintf("header %q is managed by the client and cannot be set", key)))
		}
	}

	return errs
}

func isValidURL(s string) bool {
	u, err := url.ParseRequestURI(s)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	return u.Host != ""
}

func (*MdaiHubCustomValidator) validateVariables(mdaihub *mdaiv1.MdaiHub) (admission.Warnings, field.ErrorList) {
	warnings := admission.Warnings{}
	errs := field.ErrorList{}
	keys := map[string]struct{}{}
	exportedVariableNames := map[string]struct{}{}

	variables := mdaihub.Spec.Variables
	if len(variables) == 0 {
		return append(warnings, "variables are not specified"), nil
	}

	vPath := field.NewPath("spec", "variables")

	for i, variable := range variables {
		varIndex := vPath.Index(i)

		if variable.StorageType != mdaiv1.VariableSourceTypeBuiltInValkey {
			errs = append(errs, field.NotSupported(
				varIndex.Child("storageType"),
				string(variable.StorageType),
				[]string{string(mdaiv1.VariableSourceTypeBuiltInValkey)},
			))
		}

		if _, exists := keys[variable.Key]; exists {
			errs = append(errs, field.Duplicate(varIndex.Child("key"), variable.Key))
		} else {
			keys[variable.Key] = struct{}{}
		}

		refs := variable.VariableRefs
		if variable.Type == mdaiv1.VariableTypeMeta {
			if len(refs) == 0 {
				errs = append(errs, field.Required(varIndex.Child("variableRefs"), "required for meta variable"))
			}
			if variable.DataType == mdaiv1.MetaVariableDataTypeHashSet && len(refs) != 2 {
				errs = append(errs, field.Invalid(varIndex.Child("variableRefs"), refs, "Meta HashSet must have exactly 2 elements"))
			}
		} else if len(refs) > 0 {
			errs = append(errs, field.Forbidden(varIndex.Child("variableRefs"), "not supported for non-meta variables"))
		}

		if variable.SerializeAs != nil {
			for j, with := range *variable.SerializeAs {
				serializeIndex := varIndex.Child("serializeAs").Index(j)

				if _, exists := exportedVariableNames[with.Name]; exists {
					errs = append(errs, field.Duplicate(serializeIndex.Child("name"), with.Name))
				} else {
					exportedVariableNames[with.Name] = struct{}{}
				}

				switch variable.DataType {
				case mdaiv1.VariableDataTypeSet, mdaiv1.MetaVariableDataTypePriorityList:
					continue
				case mdaiv1.VariableDataTypeString,
					mdaiv1.VariableDataTypeFloat,
					mdaiv1.VariableDataTypeInt,
					mdaiv1.VariableDataTypeBoolean,
					mdaiv1.VariableDataTypeMap,
					mdaiv1.MetaVariableDataTypeHashSet:
					if len(with.Transformers) > 0 {
						errs = append(errs, field.Forbidden(
							serializeIndex.Child("transformers"),
							fmt.Sprintf("transformers are not supported for variable type %s", variable.DataType),
						))
					}
				default:
					errs = append(errs, field.Invalid(varIndex.Child("dataType"), variable.DataType, "unsupported variable type"))
				}
			}
		}
	}

	if len(errs) > 0 {
		return warnings, errs
	}

	return warnings, nil
}
