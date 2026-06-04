package controller

import (
	"encoding/json"
	"errors"
	"fmt"

	events "github.com/mydecisive/mdai-data-core/eventing/rule"
	"github.com/mydecisive/mdai-data-core/eventing/triggers"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"k8s.io/utils/ptr"
)

func transformWhenToTrigger(when *mdaiv1.When) (triggers.Trigger, error) {
	if when == nil {
		return nil, errors.New("when is required")
	}

	alertName := ptr.Deref(when.AlertName, "")
	status := ptr.Deref(when.Status, "")
	varUpdated := ptr.Deref(when.VariableUpdated, "")
	updateType := ptr.Deref(when.UpdateType, "")

	hasAlert := alertName != ""
	hasVar := varUpdated != ""

	switch {
	case hasAlert && hasVar:
		return nil, errors.New("when: specify exactly one of alertName or variableUpdated")
	case hasAlert:
		return &triggers.AlertTrigger{
			Name:   alertName,
			Status: status,
		}, nil
	case hasVar:
		return &triggers.VariableTrigger{
			Name:       varUpdated,
			UpdateType: updateType,
		}, nil
	default:
		return nil, errors.New("when: specify either alertName or variableUpdated")
	}
}

func transformThenToCommands(actions []mdaiv1.Action) ([]events.Command, error) {
	cmds := make([]events.Command, 0, len(actions))

	appendCmd := func(present bool, payload any, typ events.CommandType) error {
		if !present {
			return nil
		}
		b, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode %s: %w", typ, err)
		}
		cmds = append(cmds, events.Command{Type: typ, Inputs: b})
		return nil
	}

	for i, action := range actions {
		start := len(cmds)

		if err := appendCmd(action.AddToSet != nil, action.AddToSet, events.CmdVarSetAdd); err != nil {
			return nil, err
		}
		if err := appendCmd(action.RemoveFromSet != nil, action.RemoveFromSet, events.CmdVarSetRemove); err != nil {
			return nil, err
		}
		if err := appendCmd(action.SetVariable != nil, action.SetVariable, events.CmdVarScalarUpdate); err != nil {
			return nil, err
		}
		if err := appendCmd(action.UnsetVariable != nil, action.UnsetVariable, events.CmdVarScalarRemove); err != nil {
			return nil, err
		}
		if err := appendCmd(action.AddToMap != nil, action.AddToMap, events.CmdVarMapAdd); err != nil {
			return nil, err
		}
		if err := appendCmd(action.RemoveFromMap != nil, action.RemoveFromMap, events.CmdVarMapRemove); err != nil {
			return nil, err
		}
		if err := appendCmd(action.CallWebhook != nil, action.CallWebhook, events.CmdWebhookCall); err != nil {
			return nil, err
		}
		if err := appendCmd(action.DeployReplay != nil, action.DeployReplay, events.CmdDeployReplay); err != nil {
			return nil, err
		}
		if err := appendCmd(action.CleanUpReplay != nil, action.CleanUpReplay, events.CmdCleanUpReplay); err != nil {
			return nil, err
		}

		if len(cmds) == start {
			return nil, fmt.Errorf("then[%d]: at least one action must be specified", i)
		}
	}
	return cmds, nil
}
