package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	opampmock "github.com/mydecisive/mdai-data-core/mock/opamp"

	"github.com/go-logr/logr"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	prometheusv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	v1core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newTestMdaiCR() *mdaiv1.MdaiHub {
	return &mdaiv1.MdaiHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hub",
			Namespace: "default",
		},
		Spec:   mdaiv1.MdaiHubSpec{},
		Status: mdaiv1.MdaiHubStatus{},
	}
}

func newFakeClientForCR(cr *mdaiv1.MdaiHub, scheme *runtime.Scheme) client.Client {
	collector := &v1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "collector1",
			Namespace: "default",
			Labels: map[string]string{
				LabelMdaiHubName: "test-hub",
			},
		},
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cr, collector).
		WithStatusSubresource(cr).
		Build()
}

func createTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = v1core.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = prometheusv1.AddToScheme(scheme)
	_ = mdaiv1.AddToScheme(scheme)
	_ = v1beta1.AddToScheme(scheme)
	return scheme
}

func TestFinalizeHub_Success(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()

	mdaiCR := &mdaiv1.MdaiHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-hub",
			Namespace:  "default",
			Finalizers: []string{hubFinalizer},
		},
		Spec:   mdaiv1.MdaiHubSpec{},
		Status: mdaiv1.MdaiHubStatus{Conditions: []metav1.Condition{}},
	}
	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	fakeValkey := mock.NewClient(ctrl)
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Scan().Cursor(0).Match(VariableKeyPrefix+mdaiCR.Name+"/"+"*").Count(100).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyInt64(0), mock.ValkeyArray(mock.ValkeyString(VariableKeyPrefix+mdaiCR.Name+"/"+"key")))))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Del().Key(VariableKeyPrefix+mdaiCR.Name+"/"+"key").Build()).
		Return(mock.Result(mock.ValkeyInt64(1)))

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		fakeValkey,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)

	state, err := adapter.finalize(ctx)
	if err != nil {
		t.Fatalf("finalize returned error: %v", err)
	}
	if state != ObjectModified {
		t.Errorf("expected state ObjectModified, got %v", state)
	}

	updatedCR := &mdaiv1.MdaiHub{}
	if err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-hub", Namespace: "default"}, updatedCR); err != nil {
		t.Fatalf("failed to get updated CR: %v", err)
	}
	for _, f := range updatedCR.Finalizers {
		if f == hubFinalizer {
			t.Errorf("expected finalizer %q to be removed, got: %v", hubFinalizer, updatedCR.Finalizers)
		}
	}

	cond := meta.FindStatusCondition(updatedCR.Status.Conditions, typeDegradedHub)
	if cond == nil {
		t.Errorf("expected condition %q to be set", typeDegradedHub)
	} else if cond.Status != metav1.ConditionTrue {
		t.Errorf("expected condition %q to be True, got %v", typeDegradedHub, cond.Status)
	}
}

func TestEnsureFinalizerInitialized_AddsFinalizer(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()

	mdaiCR := &mdaiv1.MdaiHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-hub",
			Namespace:  "default",
			Finalizers: []string{},
		},
		Spec:   mdaiv1.MdaiHubSpec{},
		Status: mdaiv1.MdaiHubStatus{},
	}

	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		nil,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)
	_, err := adapter.ensureFinalizerInitialized(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	updatedCR := &mdaiv1.MdaiHub{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-hub", Namespace: "default"}, updatedCR)
	if err != nil {
		t.Fatalf("Failed to get updated CR: %v", err)
	}
	if !slices.Contains(updatedCR.Finalizers, hubFinalizer) {
		t.Errorf("Expected finalizer %q to be added", hubFinalizer)
	}
}

func TestEnsureFinalizerInitialized_AlreadyPresent(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	mdaiCR := &mdaiv1.MdaiHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-hub",
			Namespace:  "default",
			Finalizers: []string{hubFinalizer},
		},
		Spec:   mdaiv1.MdaiHubSpec{},
		Status: mdaiv1.MdaiHubStatus{},
	}

	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		nil,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)
	_, err := adapter.ensureFinalizerInitialized(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	updatedCR := &mdaiv1.MdaiHub{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-hub", Namespace: "default"}, updatedCR)
	if err != nil {
		t.Fatalf("Failed to get updated CR: %v", err)
	}
	if len(updatedCR.Finalizers) != 1 || !slices.Contains(updatedCR.Finalizers, hubFinalizer) {
		t.Errorf("Expected finalizers to contain only %q, got %v", hubFinalizer, updatedCR.Finalizers)
	}
}

func TestEnsureStatusInitialized_SetsInitialStatus(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	mdaiCR := newTestMdaiCR()
	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		nil,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)
	_, err := adapter.ensureStatusInitialized(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	updatedCR := &mdaiv1.MdaiHub{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-hub", Namespace: "default"}, updatedCR)
	if err != nil {
		t.Fatalf("Failed to get updated CR: %v", err)
	}
	if len(updatedCR.Status.Conditions) == 0 {
		t.Error("Expected at least one status condition to be set")
	} else {
		cond := meta.FindStatusCondition(updatedCR.Status.Conditions, typeAvailableHub)
		if cond == nil || cond.Status != metav1.ConditionUnknown {
			t.Errorf("Expected %q condition with status %q, got: %+v", typeAvailableHub, metav1.ConditionUnknown, cond)
		}
	}
}

func TestGetConfigMapSHA(t *testing.T) {
	cm := v1core.ConfigMap{
		Data: map[string]string{"key": "value"},
	}
	sha, err := getConfigMapSHA(cm)
	if err != nil {
		t.Fatalf("getConfigMapSHA returned error: %v", err)
	}

	data, err := json.Marshal(cm)
	if err != nil {
		t.Fatalf("json.Marshal error: %v", err)
	}
	sum := sha256.Sum256(data)
	expected := hex.EncodeToString(sum[:])
	if sha != expected {
		t.Errorf("Expected SHA %q, got %q", expected, sha)
	}
}

func TestDeleteFinalizer(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()

	mdaiCR := &mdaiv1.MdaiHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-hub",
			Namespace:  "default",
			Finalizers: []string{hubFinalizer, "other"},
		},
	}

	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		nil,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)
	if err := adapter.deleteFinalizer(ctx, mdaiCR, hubFinalizer); err != nil {
		t.Fatalf("deleteFinalizer returned error: %v", err)
	}

	if slices.Contains(mdaiCR.Finalizers, hubFinalizer) {
		t.Errorf("Expected finalizer %q to be removed", hubFinalizer)
	}
	if !slices.Contains(mdaiCR.Finalizers, "other") {
		t.Errorf("Expected finalizer %q to remain", "other")
	}
}

func TestCreateOrUpdateEnvConfigMap(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	mdaiCR := newTestMdaiCR()
	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		nil,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)
	envMap := map[string]string{"VAR": "value"}
	if _, _, err := adapter.createOrUpdateEnvConfigMap(ctx, envMap, envConfigMapNamePostfix, "default"); err != nil {
		t.Fatalf("createOrUpdateEnvConfigMap returned error: %v", err)
	}

	cm := &v1core.ConfigMap{}
	cmName := mdaiCR.Name + envConfigMapNamePostfix
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: cmName, Namespace: "default"}, cm); err != nil {
		t.Fatalf("Failed to get ConfigMap %q: %v", cmName, err)
	}
	if cm.Data["VAR"] != "value" {
		t.Errorf("Expected env var value %q, got %q", "value", cm.Data["VAR"])
	}
}

func TestCreateOrUpdateManualEnvConfigMap(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	mdaiCR := newTestMdaiCR()
	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		nil,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)
	envMap := map[string]string{"VAR": "string"}
	_, cm, err := adapter.createOrUpdateEnvConfigMap(
		ctx,
		envMap,
		manualEnvConfigMapNamePostfix,
		"default",
		WithOwnerRef(mdaiCR, scheme))
	require.NoError(t, err)
	assert.Equal(t, "string", cm.Data["VAR"])
}

func TestEnsureVariableSynced(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	storageType := mdaiv1.VariableSourceTypeBuiltInValkey

	variableSet := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeComputed,
		DataType:    mdaiv1.VariableDataTypeSet,
		Key:         "mykey_set",
		SerializeAs: &[]mdaiv1.Serializer{
			{
				Name: "MY_ENV_SET",
				Transformers: []mdaiv1.VariableTransformer{
					{
						Type: mdaiv1.TransformerTypeJoin,
						Join: &mdaiv1.JoinTransformer{
							Delimiter: ",",
						},
					},
				},
			},
		},
	}
	variableString := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeComputed,
		DataType:    mdaiv1.VariableDataTypeString,
		Key:         "mykey_string",
		SerializeAs: &[]mdaiv1.Serializer{
			{
				Name: "MY_ENV_STR",
			},
		},
	}
	variableBoolean := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeComputed,
		DataType:    mdaiv1.VariableDataTypeString,
		Key:         "mykey_bool",
		SerializeAs: &[]mdaiv1.Serializer{
			{
				Name: "MY_ENV_BOOL",
			},
		},
	}

	variableInt := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeComputed,
		DataType:    mdaiv1.VariableDataTypeInt,
		Key:         "mykey_int",
		SerializeAs: &[]mdaiv1.Serializer{
			{
				Name: "MY_ENV_INT",
			},
		},
	}

	variableMap := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeComputed,
		DataType:    mdaiv1.VariableDataTypeMap,
		Key:         "mykey_map",
		SerializeAs: &[]mdaiv1.Serializer{
			{
				Name: "MY_ENV_MAP",
			},
		},
	}

	variablePl := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeMeta,
		DataType:    mdaiv1.MetaVariableDataTypePriorityList,
		Key:         "mykey_pl",
		VariableRefs: []string{
			"some_key",
			"mykey_set",
		},
		SerializeAs: &[]mdaiv1.Serializer{
			{
				Name: "MY_ENV_PL",
				Transformers: []mdaiv1.VariableTransformer{
					{
						Type: mdaiv1.TransformerTypeJoin,
						Join: &mdaiv1.JoinTransformer{
							Delimiter: ",",
						},
					},
				},
			},
		},
	}

	variableHs := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeMeta,
		DataType:    mdaiv1.MetaVariableDataTypeHashSet,
		Key:         "mykey_hs",
		VariableRefs: []string{
			"some_key",
			"mykey_set",
		},
		SerializeAs: &[]mdaiv1.Serializer{
			{
				Name: "MY_ENV_HS",
			},
		},
	}

	mdaiCR := newTestMdaiCR()
	mdaiCR.Spec.Variables = []mdaiv1.Variable{
		variableSet,
		variableString,
		variableBoolean,
		variableInt,
		variableMap,
		variablePl,
		variableHs,
	}

	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	fakeValkey := mock.NewClient(ctrl)

	// audit
	fakeValkey.EXPECT().Do(ctx, XaddMatcher{Type: "collector_restart"}).Return(mock.Result(mock.ValkeyString(""))).Times(1)

	// getting variables
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Smembers().Key(VariableKeyPrefix+mdaiCR.Name+"/"+variableSet.Key).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyString("service1"), mock.ValkeyString("service2"))))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Get().Key(VariableKeyPrefix+mdaiCR.Name+"/"+variableString.Key).Build()).
		Return(mock.Result(mock.ValkeyString("serviceA")))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Get().Key(VariableKeyPrefix+mdaiCR.Name+"/"+variableBoolean.Key).Build()).
		Return(mock.Result(mock.ValkeyString("true")))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Get().Key(VariableKeyPrefix+mdaiCR.Name+"/"+variableInt.Key).Build()).
		Return(mock.Result(mock.ValkeyString("10")))

	expectedMap := map[string]valkey.ValkeyMessage{
		"field1": mock.ValkeyString("value1"),
		"field2": mock.ValkeyString("value1"),
	}
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Hgetall().Key(VariableKeyPrefix+mdaiCR.Name+"/"+variableMap.Key).Build()).
		Return(mock.Result(mock.ValkeyMap(expectedMap)))

	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Arbitrary("PRIORITYLIST.GETORCREATE").
		Keys(VariableKeyPrefix+mdaiCR.Name+"/"+variablePl.Key).
		Args(VariableKeyPrefix+mdaiCR.Name+"/"+"some_key", VariableKeyPrefix+mdaiCR.Name+"/"+"mykey_set").Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyString("service1"), mock.ValkeyString("service2"))))

	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Arbitrary("HASHSET.GETORCREATE").
		Keys(VariableKeyPrefix+mdaiCR.Name+"/"+variableHs.Key).
		Args(VariableKeyPrefix+mdaiCR.Name+"/"+"some_key", VariableKeyPrefix+mdaiCR.Name+"/"+"mykey_set").Build()).
		Return(mock.Result(mock.ValkeyString("INFO|WARNING")))

	// scan for delete & actual delete of non defined variable
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Scan().Cursor(0).Match(VariableKeyPrefix+mdaiCR.Name+"/"+"*").Count(100).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyInt64(0), mock.ValkeyArray(mock.ValkeyString(VariableKeyPrefix+mdaiCR.Name+"/"+"key")))))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Del().Key(VariableKeyPrefix+mdaiCR.Name+"/"+"key").Build()).
		Return(mock.Result(mock.ValkeyInt64(1)))

	mockConnectionManager := opampmock.NewMockConnectionManager(t)
	mockConnectionManager.EXPECT().DispatchRestartCommand(testifymock.Anything).Return(nil).Times(1)

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		fakeValkey,
		time.Duration(30),
		mockConnectionManager,
	)

	opResult, err := adapter.ensureVariableSynchronized(ctx)
	if err != nil {
		t.Fatalf("ensureVariableSynchronized returned error: %v", err)
	}
	if opResult != ContinueOperationResult() {
		t.Errorf("expected ContinueProcessing, got: %v", opResult)
	}

	envCMName := mdaiCR.Name + envConfigMapNamePostfix
	envCM := &v1core.ConfigMap{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: envCMName, Namespace: "default"}, envCM); err != nil {
		t.Fatalf("failed to get env ConfigMap %q: %v", envCMName, err)
	}

	assert.Len(t, envCM.Data, 7)
	assert.Equal(t, "service1,service2", envCM.Data["MY_ENV_SET"])
	assert.Equal(t, "serviceA", envCM.Data["MY_ENV_STR"])
	assert.Equal(t, "true", envCM.Data["MY_ENV_BOOL"])
	assert.Equal(t, "10", envCM.Data["MY_ENV_INT"])
	assert.Equal(t, "field1: value1\nfield2: value1\n", envCM.Data["MY_ENV_MAP"])
	assert.Equal(t, "service1,service2", envCM.Data["MY_ENV_PL"])
	assert.Equal(t, "INFO|WARNING", envCM.Data["MY_ENV_HS"])
}

func TestEnsureManualAndComputedVariableSynced(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	storageType := mdaiv1.VariableSourceTypeBuiltInValkey
	variableType := mdaiv1.VariableDataTypeSet
	varWith := mdaiv1.Serializer{
		Name: "MY_ENV",
		Transformers: []mdaiv1.VariableTransformer{
			{
				Type: mdaiv1.TransformerTypeJoin,
				Join: &mdaiv1.JoinTransformer{
					Delimiter: ",",
				},
			},
		},
	}
	computedVariable := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeComputed,
		DataType:    variableType,
		Key:         "mykey",
		SerializeAs: &[]mdaiv1.Serializer{varWith},
	}
	manualVariable := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeManual,
		DataType:    variableType,
		Key:         "mymanualkey",
		SerializeAs: &[]mdaiv1.Serializer{varWith},
	}
	mdaiCR := newTestMdaiCR()
	mdaiCR.Spec.Variables = []mdaiv1.Variable{computedVariable, manualVariable}

	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	fakeValkey := mock.NewClient(ctrl)
	expectedComputedKey := VariableKeyPrefix + mdaiCR.Name + "/" + computedVariable.Key
	expectedManualKey := VariableKeyPrefix + mdaiCR.Name + "/" + manualVariable.Key

	fakeValkey.EXPECT().Do(ctx, XaddMatcher{Type: "collector_restart"}).Return(mock.Result(mock.ValkeyString(""))).Times(1)

	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Smembers().Key(expectedComputedKey).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyString("default"))))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Smembers().Key(expectedManualKey).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyString("default"))))

	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Scan().Cursor(0).Match(VariableKeyPrefix+mdaiCR.Name+"/"+"*").Count(100).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyInt64(0), mock.ValkeyArray(mock.ValkeyString(VariableKeyPrefix+mdaiCR.Name+"/"+"key")))))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Del().Key(VariableKeyPrefix+mdaiCR.Name+"/"+"key").Build()).
		Return(mock.Result(mock.ValkeyInt64(1)))

	mockConnectionManager := opampmock.NewMockConnectionManager(t)
	mockConnectionManager.EXPECT().DispatchRestartCommand(testifymock.Anything).Return(nil).Times(1)
	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		fakeValkey,
		time.Duration(30),
		mockConnectionManager,
	)

	opResult, err := adapter.ensureVariableSynchronized(ctx)
	if err != nil {
		t.Fatalf("ensureVariableSynchronized returned error: %v", err)
	}
	if opResult != ContinueOperationResult() {
		t.Errorf("expected ContinueProcessing, got: %v", opResult)
	}
	envCMName := mdaiCR.Name + envConfigMapNamePostfix
	envCM := &v1core.ConfigMap{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: envCMName, Namespace: "default"}, envCM); err != nil {
		t.Fatalf("failed to get env ConfigMap %q: %v", envCMName, err)
	}
	if v, ok := envCM.Data["MY_ENV"]; !ok || v != "default" {
		t.Errorf("expected env var MY_ENV to be 'set', got %q", v)
	}

	envManualCMName := mdaiCR.Name + manualEnvConfigMapNamePostfix
	envManualCM := &v1core.ConfigMap{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: envManualCMName, Namespace: "default"}, envManualCM); err != nil {
		t.Fatalf("failed to get env ConfigMap %q: %v", envManualCMName, err)
	}
	if v, ok := envManualCM.Data["mymanualkey"]; !ok || v != "set" {
		t.Errorf("expected env var MY_ENV to be 'default', got %q", v)
	}
}

func TestEnsureVariableSynced_errorDispatchingRestart(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	storageType := mdaiv1.VariableSourceTypeBuiltInValkey
	variableType := mdaiv1.VariableDataTypeSet
	varWith := mdaiv1.Serializer{
		Name: "MY_ENV",
		Transformers: []mdaiv1.VariableTransformer{
			{
				Type: mdaiv1.TransformerTypeJoin,
				Join: &mdaiv1.JoinTransformer{
					Delimiter: ",",
				},
			},
		},
	}
	computedVariable := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeComputed,
		DataType:    variableType,
		Key:         "mykey",
		SerializeAs: &[]mdaiv1.Serializer{varWith},
	}
	manualVariable := mdaiv1.Variable{
		StorageType: storageType,
		Type:        mdaiv1.VariableTypeManual,
		DataType:    variableType,
		Key:         "mymanualkey",
		SerializeAs: &[]mdaiv1.Serializer{varWith},
	}
	mdaiCR := newTestMdaiCR()
	mdaiCR.Spec.Variables = []mdaiv1.Variable{computedVariable, manualVariable}

	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	fakeValkey := mock.NewClient(ctrl)
	expectedComputedKey := VariableKeyPrefix + mdaiCR.Name + "/" + computedVariable.Key
	expectedManualKey := VariableKeyPrefix + mdaiCR.Name + "/" + manualVariable.Key

	// audit restart NOT sent since we errored on dispatching the restart event.
	fakeValkey.EXPECT().Do(ctx, XaddMatcher{Type: "collector_restart"}).Return(mock.Result(mock.ValkeyString(""))).Times(0)

	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Smembers().Key(expectedComputedKey).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyString("default"))))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Smembers().Key(expectedManualKey).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyString("default"))))

	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Scan().Cursor(0).Match(VariableKeyPrefix+mdaiCR.Name+"/"+"*").Count(100).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyInt64(0), mock.ValkeyArray(mock.ValkeyString(VariableKeyPrefix+mdaiCR.Name+"/"+"key")))))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Del().Key(VariableKeyPrefix+mdaiCR.Name+"/"+"key").Build()).
		Return(mock.Result(mock.ValkeyInt64(1)))

	mockConnectionManager := opampmock.NewMockConnectionManager(t)
	mockConnectionManager.EXPECT().DispatchRestartCommand(testifymock.Anything).Return(errors.New("whoopsie")).Times(1)
	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		fakeValkey,
		time.Duration(30),
		mockConnectionManager,
	)

	opResult, err := adapter.ensureVariableSynchronized(ctx)
	require.ErrorContains(t, err, "whoopsie")

	expectedOpResult, _ := Requeue()
	require.Equal(t, expectedOpResult, opResult)
}

type XaddMatcher struct {
	Type string
}

func (xadd XaddMatcher) Matches(x any) bool {
	if cmd, ok := x.(valkey.Completed); ok {
		commands := cmd.Commands()
		return slices.Contains(commands, "XADD") && slices.Contains(commands, "mdai_hub_event_history") && slices.Contains(commands, xadd.Type)
	}
	return false
}

func (xadd XaddMatcher) String() string {
	return "Wanted XADD to mdai_hub_event_history command with " + xadd.Type
}

func TestEnsureEvaluationsSynchronized_WithEvaluations(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()

	alertName := "alert1"
	expr := intstr.FromString("up == 0")
	var duration1 prometheusv1.Duration = "5m"
	eval := mdaiv1.PrometheusAlert{
		Name:     alertName,
		Expr:     expr,
		For:      &duration1,
		Severity: "critical",
	}

	evals := []mdaiv1.PrometheusAlert{eval}
	var interval prometheusv1.Duration = "10m"
	mdaiCR := &mdaiv1.MdaiHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hub",
			Namespace: "default",
		},
		Spec: mdaiv1.MdaiHubSpec{
			PrometheusAlerts: evals,
			Config: &mdaiv1.Config{
				EvaluationInterval: &interval,
			},
		},
		Status: mdaiv1.MdaiHubStatus{},
	}

	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)
	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		nil,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)

	opResult, err := adapter.ensurePrometheusAlertsSynchronized(ctx)
	if err != nil {
		t.Fatalf("ensurePrometheusAlertsSynchronized returned error: %v", err)
	}
	if opResult != ContinueOperationResult() {
		t.Errorf("expected ContinueOperationResult, got: %v", opResult)
	}

	ruleName := "mdai-" + mdaiCR.Name + "-alert-rules"
	promRule := &prometheusv1.PrometheusRule{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: ruleName, Namespace: mdaiCR.Namespace}, promRule)
	if err != nil {
		t.Fatalf("failed to get PrometheusRule: %v", err)
	}

	group := promRule.Spec.Groups[0]
	if group.Interval == nil || *group.Interval != interval {
		t.Errorf("expected EvaluationInterval %q, got %v", interval, group.Interval)
	}

	if len(group.Rules) != 1 {
		t.Errorf("expected 1 rule, got %d", len(group.Rules))
	}
	rule := group.Rules[0]
	if rule.Alert != "alert1" {
		t.Errorf("expected alert name 'alert1', got %q", rule.Alert)
	}
	if rule.Expr != intstr.FromString("up == 0") {
		t.Errorf("expected expr 'up == 0', got %q", rule.Expr)
	}
	if *rule.For != duration1 {
		t.Errorf("expected For '5m', got %q", *rule.For)
	}
}

func TestEnsureEvaluationsSynchronized_NoEvaluations(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	mdaiCR := newTestMdaiCR()
	ruleName := "mdai-" + mdaiCR.Name + "-alert-rules"
	promRule := &prometheusv1.PrometheusRule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ruleName,
			Namespace: mdaiCR.Namespace,
		},
		Spec: prometheusv1.PrometheusRuleSpec{
			Groups: []prometheusv1.RuleGroup{
				{
					Name:  "mdai",
					Rules: []prometheusv1.Rule{{Alert: "old-alert", Expr: intstr.FromString("1==1")}},
				},
			},
		},
	}

	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)
	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		nil,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)

	opResult, err := adapter.ensurePrometheusAlertsSynchronized(ctx)
	if err != nil {
		t.Fatalf("ensurePrometheusAlertsSynchronized returned error: %v", err)
	}
	if opResult != ContinueOperationResult() {
		t.Errorf("expected ContinueOperationResult, got: %v", opResult)
	}

	err = fakeClient.Get(ctx, types.NamespacedName{Name: ruleName, Namespace: mdaiCR.Namespace}, promRule)
	if err == nil {
		t.Errorf("expected PrometheusRule %q to be deleted, but it still exists", ruleName)
	} else if !apierrors.IsNotFound(err) {
		t.Errorf("unexpected error getting PrometheusRule: %v", err)
	}
}

func TestEnsureHubDeletionProcessed_WithDeletion(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()

	mdaiCR := &mdaiv1.MdaiHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-hub",
			Namespace:         "default",
			Finalizers:        []string{hubFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now().Add(-1 * time.Hour)},
		},
		Spec:   mdaiv1.MdaiHubSpec{},
		Status: mdaiv1.MdaiHubStatus{Conditions: []metav1.Condition{}},
	}

	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	fakeValkey := mock.NewClient(ctrl)
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Scan().Cursor(0).Match(VariableKeyPrefix+mdaiCR.Name+"/"+"*").Count(100).Build()).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyInt64(0), mock.ValkeyArray(mock.ValkeyString(VariableKeyPrefix+mdaiCR.Name+"/"+"key")))))
	fakeValkey.EXPECT().Do(ctx, fakeValkey.B().Del().Key(VariableKeyPrefix+mdaiCR.Name+"/"+"key").Build()).
		Return(mock.Result(mock.ValkeyInt64(1)))

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		fakeValkey,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)

	opResult, err := adapter.ensureDeletionProcessed(ctx)
	if err != nil {
		t.Fatalf("ensureDeletionProcessed returned error: %v", err)
	}

	if opResult != StopOperationResult() {
		t.Errorf("expected StopOperationResult, got: %v", opResult)
	}

	updatedCR := &mdaiv1.MdaiHub{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: mdaiCR.Name, Namespace: mdaiCR.Namespace}, updatedCR)
	if err != nil {
		if apierrors.IsNotFound(err) {
			t.Log("CR not found after finalization, which is acceptable")
		} else {
			t.Fatalf("failed to get updated CR: %v", err)
		}
	}
}

func TestEnsureStatusSetToDone(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	mdaiCR := newTestMdaiCR()
	fakeClient := newFakeClientForCR(mdaiCR, scheme)
	recorder := record.NewFakeRecorder(10)

	adapter := NewHubAdapter(
		mdaiCR,
		logr.Discard(),
		zap.NewNop(),
		fakeClient,
		recorder,
		scheme,
		nil,
		time.Duration(30),
		opampmock.NewMockConnectionManager(t),
	)

	opResult, err := adapter.ensureStatusSetToDone(ctx)
	if err != nil {
		t.Fatalf("ensureStatusSetToDone returned error: %v", err)
	}
	if opResult != ContinueOperationResult() {
		t.Errorf("expected ContinueOperationResult, got: %v", opResult)
	}

	updatedCR := &mdaiv1.MdaiHub{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-hub", Namespace: "default"}, updatedCR)
	if err != nil {
		t.Fatalf("failed to re-fetch mdaiCR: %v", err)
	}

	cond := meta.FindStatusCondition(updatedCR.Status.Conditions, typeAvailableHub)
	if cond == nil {
		t.Fatalf("expected condition %q to exist, but it was not found", typeAvailableHub)
	}
	if cond.Status != metav1.ConditionTrue { //nolint:staticcheck
		t.Errorf("expected condition %q to be True, got: %v", typeAvailableHub, cond.Status)
	}
	if cond.Reason != "Reconciling" {
		t.Errorf("expected reason 'Reconciling', got: %q", cond.Reason)
	}
	if cond.Message != "reconciled successfully" {
		t.Errorf("expected message 'reconciled successfully', got: %q", cond.Message)
	}
}

func TestEnsureAutomationsSynchronized(t *testing.T) {
	ctx := t.Context()

	mdaiCR := newTestMdaiCR()
	mdaiCR.Spec.Rules = []mdaiv1.AutomationRule{
		{
			Name: "automation-1",
			When: mdaiv1.When{
				AlertName: ptr.To("my-alert"),
				Status:    ptr.To("firing"),
			},
			Then: []mdaiv1.Action{
				{
					AddToSet: &mdaiv1.SetAction{
						Set:   "my-set",
						Value: "my-value",
					},
				},
			},
		},
	}

	scheme := createTestScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mdaiCR).Build()
	adapter := HubAdapter{
		mdaiCR: mdaiCR,
		client: fakeClient,
		logger: logr.Discard(),
		scheme: scheme,
	}

	opResult, err := adapter.ensureAutomationsSynchronized(ctx)
	require.NoError(t, err)
	assert.Equal(t, ContinueOperationResult(), opResult)

	configMapName := mdaiCR.Name + automationConfigMapNamePostfix
	cm := &v1core.ConfigMap{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: "default"}, cm)
	require.NoError(t, err)

	expectedData := `{"name":"automation-1","trigger":{"kind":"alert","spec":{"name":"my-alert","status":"firing"}},"commands":[{"type":"variable.set.add","inputs":{"set":"my-set","value":"my-value"}}]}`
	actualData, exists := cm.Data["automation-1"]
	assert.True(t, exists)
	assert.JSONEq(t, expectedData, actualData)

	mdaiCR.Spec.Rules = nil
	adapter.mdaiCR = mdaiCR

	opResult, err = adapter.ensureAutomationsSynchronized(ctx)
	require.NoError(t, err)
	assert.Equal(t, ContinueOperationResult(), opResult)

	err = fakeClient.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: "default"}, cm)
	assert.True(t, apierrors.IsNotFound(err))
}
