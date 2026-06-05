package controller

import (
	"encoding/json"
	"testing"

	"github.com/mydecisive/mdai-data-core/eventing/rule"
	"github.com/mydecisive/mdai-data-core/eventing/triggers"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestTransformWhenToTrigger_AlertTrigger(t *testing.T) {
	when := &mdaiv1.When{
		AlertName: ptr.To("some-alert-name"),
		Status:    ptr.To("firing"),
	}

	trigger, err := transformWhenToTrigger(when)
	require.NoError(t, err)
	require.NotNil(t, trigger)

	alertTrigger, ok := trigger.(*triggers.AlertTrigger)
	require.True(t, ok)

	assert.Equal(t, "some-alert-name", alertTrigger.Name)
	assert.Equal(t, "firing", alertTrigger.Status)
}

func TestTransformWhenToTrigger_AlertTrigger_EmptyStatus(t *testing.T) {
	when := &mdaiv1.When{
		AlertName: ptr.To("some-alert-name"),
		Status:    ptr.To(""), // Empty string status
	}

	trigger, err := transformWhenToTrigger(when)
	require.NoError(t, err)
	require.NotNil(t, trigger)

	alertTrigger, ok := trigger.(*triggers.AlertTrigger)
	require.True(t, ok)

	assert.Equal(t, "some-alert-name", alertTrigger.Name)
	assert.Empty(t, alertTrigger.Status)
}

func TestTransformWhenToTrigger_VariableTrigger(t *testing.T) {
	when := &mdaiv1.When{
		VariableUpdated: ptr.To("some-variable-name"),
		UpdateType:      ptr.To("any"),
	}

	trigger, err := transformWhenToTrigger(when)
	require.NoError(t, err)
	require.NotNil(t, trigger)

	variableTrigger, ok := trigger.(*triggers.VariableTrigger)
	require.True(t, ok)

	assert.Equal(t, "some-variable-name", variableTrigger.Name)
	assert.Equal(t, "any", variableTrigger.UpdateType)
}

func TestTransformWhenToTrigger_VariableTrigger_EmptyUpdateType(t *testing.T) {
	testCases := []struct {
		name       string
		updateType *string
	}{
		{
			name: "nil update type",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			when := &mdaiv1.When{
				VariableUpdated: ptr.To("some-variable-name"),
				UpdateType:      tc.updateType,
			}

			trigger, err := transformWhenToTrigger(when)
			require.NoError(t, err)
			require.NotNil(t, trigger)

			variableTrigger, ok := trigger.(*triggers.VariableTrigger)
			require.True(t, ok)

			assert.Equal(t, "some-variable-name", variableTrigger.Name)
			assert.Empty(t, variableTrigger.UpdateType)
		})
	}
}

func TestTransformWhenToTrigger_NilInput(t *testing.T) {
	trigger, err := transformWhenToTrigger(nil)
	require.Error(t, err)
	assert.Nil(t, trigger)
	assert.EqualError(t, err, "when is required")
}

func TestTransformWhenToTrigger_BothAlertAndVariableSpecified(t *testing.T) {
	when := &mdaiv1.When{
		AlertName:       ptr.To("some-alert"),
		VariableUpdated: ptr.To("some-variable"),
	}

	trigger, err := transformWhenToTrigger(when)
	require.Error(t, err)
	assert.Nil(t, trigger)
	assert.EqualError(t, err, "when: specify exactly one of alertName or variableUpdated")
}

func TestTransformWhenToTrigger_NeitherAlertNorVariableSpecified(t *testing.T) {
	when := &mdaiv1.When{}

	trigger, err := transformWhenToTrigger(when)
	require.Error(t, err)
	assert.Nil(t, trigger)
	assert.EqualError(t, err, "when: specify either alertName or variableUpdated")
}

func TestTransformThenToCommands_AddToSet(t *testing.T) {
	actions := []mdaiv1.Action{
		{
			AddToSet: &mdaiv1.SetAction{
				Set:   "test-set",
				Value: "new-item",
			},
		},
	}

	cmds, err := transformThenToCommands(actions)
	require.NoError(t, err)
	require.Len(t, cmds, 1)

	assert.Equal(t, rule.CmdVarSetAdd, cmds[0].Type)
	expectedJSON := `{"set":"test-set","value":"new-item"}`
	assert.JSONEq(t, expectedJSON, string(cmds[0].Inputs))
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func assertCmd(t *testing.T, got rule.Command, wantType rule.CommandType, wantPayload any) {
	t.Helper()
	require.Equal(t, wantType, got.Type)
	require.JSONEq(t, string(mustJSON(t, wantPayload)), string(got.Inputs))
}

func sampleAdd() *mdaiv1.SetAction                     { return &mdaiv1.SetAction{} }
func sampleRemove() *mdaiv1.SetAction                  { return &mdaiv1.SetAction{} }
func sampleScalar() *mdaiv1.ScalarAction               { return &mdaiv1.ScalarAction{} }
func sampleWebhook() *mdaiv1.CallWebhookAction         { return &mdaiv1.CallWebhookAction{} }
func sampleMap() *mdaiv1.MapAction                     { return &mdaiv1.MapAction{} }
func sampleDeployReplay() *mdaiv1.DeployReplayAction   { return &mdaiv1.DeployReplayAction{} }
func sampleCleanUpReplay() *mdaiv1.CleanUpReplayAction { return &mdaiv1.CleanUpReplayAction{} }
func sampleUnset() *mdaiv1.ScalarRemoveAction          { return &mdaiv1.ScalarRemoveAction{} }

func TestTransformThenToCommands_SinglePerAction_Succeeds(t *testing.T) {
	actions := []mdaiv1.Action{
		{AddToSet: sampleAdd()},
		{RemoveFromSet: sampleRemove()},
		{SetVariable: sampleScalar()},
		{AddToMap: sampleMap()},
		{RemoveFromMap: sampleMap()},
		{CallWebhook: sampleWebhook()},
		{DeployReplay: sampleDeployReplay()},
		{CleanUpReplay: sampleCleanUpReplay()},
		{UnsetVariable: sampleUnset()},
	}

	cmds, err := transformThenToCommands(actions)
	require.NoError(t, err)
	require.Len(t, cmds, len(rule.AllCommandTypes))

	assertCmd(t, cmds[0], rule.CmdVarSetAdd, actions[0].AddToSet)
	assertCmd(t, cmds[1], rule.CmdVarSetRemove, actions[1].RemoveFromSet)
	assertCmd(t, cmds[2], rule.CmdVarScalarUpdate, actions[2].SetVariable)
	assertCmd(t, cmds[3], rule.CmdVarMapAdd, actions[3].AddToMap)
	assertCmd(t, cmds[4], rule.CmdVarMapRemove, actions[4].RemoveFromMap)
	assertCmd(t, cmds[5], rule.CmdWebhookCall, actions[5].CallWebhook)
	assertCmd(t, cmds[6], rule.CmdDeployReplay, actions[6].DeployReplay)
	assertCmd(t, cmds[7], rule.CmdCleanUpReplay, actions[7].CleanUpReplay)
	assertCmd(t, cmds[8], rule.CmdVarScalarRemove, actions[8].UnsetVariable)
}

func TestTransformThenToCommands_EmptySlice_OK(t *testing.T) {
	cmds, err := transformThenToCommands(nil)
	require.NoError(t, err)
	require.Empty(t, cmds)
}
