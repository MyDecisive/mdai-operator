package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	mdaiv1 "github.com/mydecisive/mdai-operator/api/v1"
	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	prometheusv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
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
	// Endpoint uses an indirection pattern so extractCollectorEnvRefs returns wildcard;
	// shared-fixture tests expect the collector to restart on any variable change.
	collector := &v1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "collector1",
			Namespace: "default",
			Labels: map[string]string{
				LabelMdaiHubName: "test-hub",
			},
		},
		Spec: v1beta1.OpenTelemetryCollectorSpec{
			Config: v1beta1.Config{
				Receivers: v1beta1.AnyConfig{
					Object: map[string]any{
						"otlp": map[string]any{"endpoint": "${env:${PREFIX}_ENDPOINT}"},
					},
				},
				Exporters: v1beta1.AnyConfig{
					Object: map[string]any{"debug": map[string]any{}},
				},
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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, fakeValkey, time.Duration(30))

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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))
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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))
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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))
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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))
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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))
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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))
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

func TestBuildVariableSchemaEnvMap(t *testing.T) {
	serializer := []mdaiv1.Serializer{
		{Name: "MY_ENV"},
	}
	variables := []mdaiv1.Variable{
		{
			Key:         "manual_var",
			Type:        mdaiv1.VariableTypeManual,
			DataType:    mdaiv1.VariableDataTypeString,
			StorageType: mdaiv1.VariableSourceTypeBuiltInValkey,
		},
		{
			Key:         "computed_var",
			Type:        mdaiv1.VariableTypeComputed,
			DataType:    mdaiv1.VariableDataTypeSet,
			StorageType: mdaiv1.VariableSourceTypeBuiltInValkey,
			SerializeAs: &serializer,
		},
		{
			Key:         "meta_var",
			Type:        mdaiv1.VariableTypeMeta,
			DataType:    mdaiv1.MetaVariableDataTypeHashSet,
			StorageType: mdaiv1.VariableSourceTypeBuiltInValkey,
			VariableRefs: []string{
				"first",
				"second",
			},
		},
	}

	schemaMap, err := buildVariableSchemaEnvMap(variables)
	require.NoError(t, err)
	require.Len(t, schemaMap, 3)

	manualPayload := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(schemaMap["manual_var"]), &manualPayload))
	assert.Equal(t, "manual", manualPayload["type"])
	assert.Equal(t, "string", manualPayload["dataType"])
	assert.Equal(t, "mdai-valkey", manualPayload["storageType"])
	_, hasManualRefs := manualPayload["variableRefs"]
	assert.False(t, hasManualRefs)
	_, hasManualSerializeAs := manualPayload["serializeAs"]
	assert.False(t, hasManualSerializeAs)

	computedPayload := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(schemaMap["computed_var"]), &computedPayload))
	_, hasComputedSerializeAs := computedPayload["serializeAs"]
	assert.True(t, hasComputedSerializeAs)

	metaPayload := variableSchemaConfigMapEntry{}
	require.NoError(t, json.Unmarshal([]byte(schemaMap["meta_var"]), &metaPayload))
	assert.Equal(t, []string{"first", "second"}, metaPayload.VariableRefs)
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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, fakeValkey, time.Duration(30))

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

	schemaCMName := mdaiCR.Name + variableSchemaConfigMapPostfix
	schemaCM := &v1core.ConfigMap{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: schemaCMName, Namespace: "default"}, schemaCM); err != nil {
		t.Fatalf("failed to get schema ConfigMap %q: %v", schemaCMName, err)
	}
	assert.Len(t, schemaCM.Data, 7)

	setSchema := variableSchemaConfigMapEntry{}
	require.NoError(t, json.Unmarshal([]byte(schemaCM.Data[variableSet.Key]), &setSchema))
	assert.Equal(t, variableSet.Type, setSchema.Type)
	assert.Equal(t, variableSet.DataType, setSchema.DataType)
	assert.Equal(t, variableSet.StorageType, setSchema.StorageType)
	require.NotNil(t, setSchema.SerializeAs)

	hashSetSchema := variableSchemaConfigMapEntry{}
	require.NoError(t, json.Unmarshal([]byte(schemaCM.Data[variableHs.Key]), &hashSetSchema))
	assert.Equal(t, variableHs.Type, hashSetSchema.Type)
	assert.Equal(t, variableHs.VariableRefs, hashSetSchema.VariableRefs)

	updatedCollector := &v1beta1.OpenTelemetryCollector{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: "collector1", Namespace: "default"}, updatedCollector); err != nil {
		t.Fatalf("failed to get updated collector: %v", err)
	}
	restartAnnotation := "kubectl.kubernetes.io/restartedAt"
	if ann, ok := updatedCollector.Annotations[restartAnnotation]; !ok || strings.TrimSpace(ann) == "" {
		t.Errorf("expected collector to have restart annotation %q set, got: %v", restartAnnotation, updatedCollector.Annotations)
	}
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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, fakeValkey, time.Duration(30))

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

	schemaCMName := mdaiCR.Name + variableSchemaConfigMapPostfix
	schemaCM := &v1core.ConfigMap{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: schemaCMName, Namespace: "default"}, schemaCM); err != nil {
		t.Fatalf("failed to get schema ConfigMap %q: %v", schemaCMName, err)
	}
	assert.Len(t, schemaCM.Data, 2)

	manualSchema := variableSchemaConfigMapEntry{}
	require.NoError(t, json.Unmarshal([]byte(schemaCM.Data[manualVariable.Key]), &manualSchema))
	assert.Equal(t, mdaiv1.VariableTypeManual, manualSchema.Type)
	assert.Equal(t, manualVariable.DataType, manualSchema.DataType)

	computedSchema := variableSchemaConfigMapEntry{}
	require.NoError(t, json.Unmarshal([]byte(schemaCM.Data[computedVariable.Key]), &computedSchema))
	assert.Equal(t, mdaiv1.VariableTypeComputed, computedSchema.Type)
	require.NotNil(t, computedSchema.SerializeAs)

	updatedCollector := &v1beta1.OpenTelemetryCollector{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: "collector1", Namespace: "default"}, updatedCollector); err != nil {
		t.Fatalf("failed to get updated collector: %v", err)
	}
	restartAnnotation := "kubectl.kubernetes.io/restartedAt"
	if ann, ok := updatedCollector.Annotations[restartAnnotation]; !ok || strings.TrimSpace(ann) == "" {
		t.Errorf("expected collector to have restart annotation %q set, got: %v", restartAnnotation, updatedCollector.Annotations)
	}
}

func TestEnsureVariableSynced_DeletesSchemaConfigMapForEmptyVariables(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	mdaiCR := newTestMdaiCR()
	mdaiCR.Spec.Variables = []mdaiv1.Variable{}

	collector := &v1beta1.OpenTelemetryCollector{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "collector1",
			Namespace: "default",
			Labels: map[string]string{
				LabelMdaiHubName: "test-hub",
			},
		},
	}
	schemaCMName := mdaiCR.Name + variableSchemaConfigMapPostfix
	existingSchemaCM := &v1core.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      schemaCMName,
			Namespace: mdaiCR.Namespace,
		},
		Data: map[string]string{
			"legacy": "schema",
		},
	}
	manualCMName := mdaiCR.Name + manualEnvConfigMapNamePostfix
	existingManualCM := &v1core.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      manualCMName,
			Namespace: mdaiCR.Namespace,
		},
		Data: map[string]string{
			"legacy": "manual",
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(mdaiCR, collector, existingSchemaCM, existingManualCM).
		WithStatusSubresource(mdaiCR).
		Build()
	recorder := record.NewFakeRecorder(10)

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))

	opResult, err := adapter.ensureVariableSynchronized(ctx)
	require.NoError(t, err)
	assert.Equal(t, ContinueOperationResult(), opResult)

	deletedSchemaCM := &v1core.ConfigMap{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: schemaCMName, Namespace: mdaiCR.Namespace}, deletedSchemaCM)
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err), "expected schema ConfigMap %q to be deleted", schemaCMName)

	deletedManualCM := &v1core.ConfigMap{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: manualCMName, Namespace: mdaiCR.Namespace}, deletedManualCM)
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err), "expected manual ConfigMap %q to be deleted", manualCMName)
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
	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))

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
	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))

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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, fakeValkey, time.Duration(30))

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

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, nil, time.Duration(30))

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

func TestDiffEnvMapKeys(t *testing.T) {
	tests := []struct {
		name    string
		old     map[string]string
		current map[string]string
		want    []string
	}{
		{
			name:    "identical",
			old:     map[string]string{"A": "1", "B": "2"},
			current: map[string]string{"A": "1", "B": "2"},
			want:    nil,
		},
		{
			name:    "added",
			old:     map[string]string{"A": "1"},
			current: map[string]string{"A": "1", "B": "2"},
			want:    []string{"B"},
		},
		{
			name:    "removed",
			old:     map[string]string{"A": "1", "B": "2"},
			current: map[string]string{"A": "1"},
			want:    []string{"B"},
		},
		{
			name:    "value changed",
			old:     map[string]string{"A": "1"},
			current: map[string]string{"A": "2"},
			want:    []string{"A"},
		},
		{
			name:    "old nil treats every current key as new",
			old:     nil,
			current: map[string]string{"A": "1", "B": "2"},
			want:    []string{"A", "B"},
		},
		{
			name:    "current nil treats every old key as removed",
			old:     map[string]string{"A": "1"},
			current: nil,
			want:    []string{"A"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := diffEnvMapKeys(tt.old, tt.current)
			gotKeys := make([]string, 0, len(got))
			for k := range got {
				gotKeys = append(gotKeys, k)
			}
			assert.ElementsMatch(t, tt.want, gotKeys)
		})
	}
}

func TestExtractCollectorEnvRefs(t *testing.T) {
	collectorWithReceiverValue := func(value string) v1beta1.OpenTelemetryCollector {
		return v1beta1.OpenTelemetryCollector{
			Spec: v1beta1.OpenTelemetryCollectorSpec{
				Config: v1beta1.Config{
					Receivers: v1beta1.AnyConfig{
						Object: map[string]any{
							"otlp": map[string]any{"endpoint": value},
						},
					},
					Exporters: v1beta1.AnyConfig{
						Object: map[string]any{"debug": map[string]any{}},
					},
				},
			},
		}
	}

	tests := []struct {
		name         string
		collector    v1beta1.OpenTelemetryCollector
		wantRefs     []string
		wantWildcard bool
	}{
		{
			name:         "no env refs",
			collector:    collectorWithReceiverValue("0.0.0.0:4317"),
			wantRefs:     nil,
			wantWildcard: false,
		},
		{
			name:         "simple ref",
			collector:    collectorWithReceiverValue("${env:OTEL_ENDPOINT}"),
			wantRefs:     []string{"OTEL_ENDPOINT"},
			wantWildcard: false,
		},
		{
			name:         "ref with default",
			collector:    collectorWithReceiverValue("${env:OTEL_ENDPOINT:-0.0.0.0:4317}"),
			wantRefs:     []string{"OTEL_ENDPOINT"},
			wantWildcard: false,
		},
		{
			name:         "indirection falls back to wildcard",
			collector:    collectorWithReceiverValue("${env:${PREFIX}_ENDPOINT}"),
			wantRefs:     nil,
			wantWildcard: true,
		},
		{
			name: "multiple refs across components",
			collector: v1beta1.OpenTelemetryCollector{
				Spec: v1beta1.OpenTelemetryCollectorSpec{
					Config: v1beta1.Config{
						Receivers: v1beta1.AnyConfig{
							Object: map[string]any{
								"otlp": map[string]any{"endpoint": "${env:OTEL_RECEIVER_ENDPOINT}"},
							},
						},
						Exporters: v1beta1.AnyConfig{
							Object: map[string]any{
								"datadog": map[string]any{"api_key": "${env:DD_API_KEY:-x}"},
							},
						},
					},
				},
			},
			wantRefs:     []string{"OTEL_RECEIVER_ENDPOINT", "DD_API_KEY"},
			wantWildcard: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRefs, gotWildcard := extractCollectorEnvRefs(tt.collector)
			assert.Equal(t, tt.wantWildcard, gotWildcard)

			gotKeys := make([]string, 0, len(gotRefs))
			for k := range gotRefs {
				gotKeys = append(gotKeys, k)
			}
			assert.ElementsMatch(t, tt.wantRefs, gotKeys)
		})
	}
}

func TestSyncComputedConfigMapsAndRestart_RestartsOnlyReferencingCollectors(t *testing.T) {
	ctx := t.Context()
	scheme := createTestScheme()
	mdaiCR := newTestMdaiCR()

	collectorWithRef := func(name, ref string) *v1beta1.OpenTelemetryCollector {
		return &v1beta1.OpenTelemetryCollector{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels:    map[string]string{LabelMdaiHubName: mdaiCR.Name},
			},
			Spec: v1beta1.OpenTelemetryCollectorSpec{
				Config: v1beta1.Config{
					Receivers: v1beta1.AnyConfig{
						Object: map[string]any{"otlp": map[string]any{"endpoint": ref}},
					},
					Exporters: v1beta1.AnyConfig{
						Object: map[string]any{"debug": map[string]any{}},
					},
				},
			},
		}
	}
	overlap := collectorWithRef("c-overlap", "${env:OVERLAP}")
	other := collectorWithRef("c-other", "${env:OTHER}")

	existingEnvCM := &v1core.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mdaiCR.Name + envConfigMapNamePostfix,
			Namespace: "default",
		},
		Data: map[string]string{"OVERLAP": "old", "OTHER": "x"},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(mdaiCR, overlap, other, existingEnvCM).
		Build()
	recorder := record.NewFakeRecorder(10)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	fakeValkey := mock.NewClient(ctrl)
	// Exactly one collector should restart, so exactly one audit event should be emitted.
	fakeValkey.EXPECT().Do(ctx, XaddMatcher{Type: "collector_restart"}).
		Return(mock.Result(mock.ValkeyString(""))).Times(1)

	adapter := NewHubAdapter(mdaiCR, logr.Discard(), zap.NewNop(), fakeClient, recorder, scheme, fakeValkey, time.Duration(30))

	envMap := map[string]string{"OVERLAP": "new", "OTHER": "x"}
	opResult, err := adapter.syncComputedConfigMapsAndRestart(ctx, envMap)
	require.NoError(t, err)
	assert.Equal(t, ContinueOperationResult(), opResult)

	restartAnnotation := "kubectl.kubernetes.io/restartedAt"

	got := &v1beta1.OpenTelemetryCollector{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "c-overlap", Namespace: "default"}, got))
	if ann, ok := got.Annotations[restartAnnotation]; !ok || strings.TrimSpace(ann) == "" {
		t.Errorf("expected c-overlap to be restarted, annotations: %v", got.Annotations)
	}

	got = &v1beta1.OpenTelemetryCollector{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "c-other", Namespace: "default"}, got))
	if _, ok := got.Annotations[restartAnnotation]; ok {
		t.Errorf("expected c-other NOT to be restarted, annotations: %v", got.Annotations)
	}
}
