// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package capacitybuffers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	ccapiv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	cbv1beta1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/client-go/rest"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestGetOrCreateCCCCacheIsolation(t *testing.T) {
	r := &Reconciler{}
	cacheA := r.getOrCreateCache("ccc-a")
	cacheB := r.getOrCreateCache("ccc-b")

	if cacheA == cacheB {
		t.Fatalf("Expected strictly isolated caches for different ComputeClasses, but pointers matched")
	}

	cacheAAgain := r.getOrCreateCache("ccc-a")
	if cacheA != cacheAAgain {
		t.Fatalf("Expected same pointer for previously stored cache, but got a new one")
	}

	r.deleteCache("ccc-a")
	cacheARecreated := r.getOrCreateCache("ccc-a")
	if cacheA == cacheARecreated {
		t.Fatalf("Expected brand new cache pointer after delete, but got the old one")
	}
}

func TestBuildCandidateRepresentations(t *testing.T) {
	nowTime := time.Now()

	// Slice 1: Actives
	activeCBs := []*cbv1beta1.CapacityBuffer{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "live-cb",
				UID:               "ui-1",
				CreationTimestamp: metav1.Time{Time: nowTime},
			},
		},
	}

	// Slice 2: InFlight buffers simulating both exact name collisions and unmapped ghost creates
	inFlights := []internalCapacityBuffer{
		{
			Name:            "live-cb", // Simulates collision with an identically named live buffer
			UID:             "ui-1",
			PendingCreation: true,
		},
		{
			Name:              "ghost-cb",
			UID:               "ui-2",
			PendingCreation:   true,
			CreationTimestamp: nowTime.Add(-1 * time.Minute),
		},
	}

	finalArray := buildCandidateRepresentations(activeCBs, inFlights)
	if len(finalArray) != 3 {
		t.Fatalf("Expected precisely 3 unified nodes simulating exact pass-through, got %d", len(finalArray))
	}
	if finalArray[0].Name != "live-cb" || finalArray[1].Name != "live-cb" || finalArray[2].Name != "ghost-cb" {
		t.Fatalf("Candidate array flattening mismatch ordering: %v", finalArray)
	}
}

func TestBuildObservedBuffer(t *testing.T) {
	// 1. Nil buffer returns old observed buffer without panicking
	oldObs := ccapiv1.ObservedBufferStatus{
		Name:      "old-buf",
		Namespace: NamespaceGkeManagedCCC,
	}
	if got := buildObservedBuffer(nil, oldObs); got.Name != oldObs.Name {
		t.Errorf("Expected nil cb to return old observed buffer, got %v", got)
	}

	// 2. Custom provisioning strategy
	customStrategy := "custom.x-k8s.io/special"
	customCB := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-buf",
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &customStrategy,
		},
	}
	gotCustom := buildObservedBuffer(customCB, ccapiv1.ObservedBufferStatus{})
	if gotCustom.Name != "custom-buf" || len(gotCustom.Conditions) != 1 {
		t.Fatalf("Unexpected custom observed buffer: %+v", gotCustom)
	}
	if gotCustom.Conditions[0].Type != "CustomProvisioningStrategy" || gotCustom.Conditions[0].Status != metav1.ConditionTrue {
		t.Errorf("Expected CustomProvisioningStrategy=True, got %+v", gotCustom.Conditions[0])
	}

	// 3. Built-in strategy with existing ReadyForProvisioning condition
	activeStrat := activeCapacityStrategy
	readyCB := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ready-buf",
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStrat,
		},
		Status: cbv1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{
					Type:   capacitybuffer.ReadyForProvisioningCondition,
					Status: metav1.ConditionTrue,
					Reason: "Ready",
				},
			},
		},
	}
	gotReady := buildObservedBuffer(readyCB, ccapiv1.ObservedBufferStatus{})
	if gotReady.Name != "ready-buf" || len(gotReady.Conditions) != 1 {
		t.Fatalf("Unexpected ready observed buffer: %+v", gotReady)
	}
	if gotReady.Conditions[0].Status != metav1.ConditionTrue {
		t.Errorf("Expected ReadyForProvisioning=True, got %+v", gotReady.Conditions[0])
	}

	// 4. Built-in strategy with both ReadyForProvisioning and Provisioning conditions
	readyAndProvCB := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ready-prov-buf",
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStrat,
		},
		Status: cbv1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{
					Type:   capacitybuffer.ReadyForProvisioningCondition,
					Status: metav1.ConditionTrue,
					Reason: "AttributesSetSuccessfully",
				},
				{
					Type:    capacitybuffer.ProvisioningCondition,
					Status:  metav1.ConditionFalse,
					Reason:  "BufferIsEmpty",
					Message: "CapacityBuffer has zero replicas",
				},
			},
		},
	}
	gotReadyAndProv := buildObservedBuffer(readyAndProvCB, ccapiv1.ObservedBufferStatus{})
	if gotReadyAndProv.Name != "ready-prov-buf" || len(gotReadyAndProv.Conditions) != 2 {
		t.Fatalf("Expected 2 conditions on observed buffer, got: %+v", gotReadyAndProv)
	}
	if provCond := meta.FindStatusCondition(gotReadyAndProv.Conditions, capacitybuffer.ProvisioningCondition); provCond == nil || provCond.Status != metav1.ConditionFalse || provCond.Reason != "BufferIsEmpty" {
		t.Errorf("Expected Provisioning=False (BufferIsEmpty), got %+v", provCond)
	}

	// 5. Built-in strategy initializing (missing ReadyForProvisioning condition)
	initCB := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "init-buf",
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStrat,
		},
	}
	gotInit := buildObservedBuffer(initCB, ccapiv1.ObservedBufferStatus{})
	if gotInit.Name != "init-buf" || len(gotInit.Conditions) != 1 {
		t.Fatalf("Unexpected init observed buffer: %+v", gotInit)
	}
	if gotInit.Conditions[0].Status != metav1.ConditionFalse || gotInit.Conditions[0].Reason != "Pending" {
		t.Errorf("Expected ReadyForProvisioning=False Reason=Pending, got %+v", gotInit.Conditions[0])
	}
}

func TestReconcileStatus_ProvisioningCondition(t *testing.T) {
	activeStrat := activeCapacityStrategy

	t.Run("ReadyForProvisioning=True and Provisioning=True sets BuffersConfigured=True", func(t *testing.T) {
		cc := &ccapiv1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cc", Generation: 1},
			Spec: ccapiv1.ComputeClassSpec{
				Buffers: []ccapiv1.ComputeClassBuffer{{ProvisioningStrategy: activeStrat}},
			},
		}
		cb := &cbv1beta1.CapacityBuffer{
			ObjectMeta: metav1.ObjectMeta{Name: "cb-1", Namespace: NamespaceGkeManagedCCC},
			Spec:       cbv1beta1.CapacityBufferSpec{ProvisioningStrategy: &activeStrat},
			Status: cbv1beta1.CapacityBufferStatus{
				Conditions: []metav1.Condition{
					{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue, Reason: "AttributesSetSuccessfully", Message: "ready"},
					{Type: capacitybuffer.ProvisioningCondition, Status: metav1.ConditionTrue, Reason: "FakePodsInjected"},
				},
			},
		}

		statusWriter := &mockStatusWriter{}
		r := &Reconciler{Client: &mockReconcileClient{cc: cc, statusWriter: statusWriter}}
		if err := r.reconcileStatus(context.Background(), cc, []*cbv1beta1.CapacityBuffer{cb}, nil); err != nil {
			t.Fatalf("reconcileStatus returned unexpected error: %v", err)
		}

		var patchedCC ccapiv1.ComputeClass
		if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
			t.Fatalf("Failed to unmarshal status patch: %v", err)
		}
		cond := meta.FindStatusCondition(patchedCC.Status.Conditions, conditionTypeBuffersConfigured)
		if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Ready" || cond.Message != "All capacity buffers configured and ready for provisioning" {
			t.Errorf("Unexpected BuffersConfigured condition: %+v", cond)
		}
		if len(patchedCC.Status.ObservedBuffers) != 1 || len(patchedCC.Status.ObservedBuffers[0].Conditions) != 2 {
			t.Errorf("Expected ObservedBuffers[0] to have 2 mirrored conditions, got %+v", patchedCC.Status.ObservedBuffers)
		}
	})

	t.Run("ReadyForProvisioning=True and Provisioning=False sets BuffersConfigured=False with Provisioning reason", func(t *testing.T) {
		cc := &ccapiv1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cc", Generation: 1},
			Spec: ccapiv1.ComputeClassSpec{
				Buffers: []ccapiv1.ComputeClassBuffer{{ProvisioningStrategy: activeStrat}},
			},
		}
		cb := &cbv1beta1.CapacityBuffer{
			ObjectMeta: metav1.ObjectMeta{Name: "cb-1", Namespace: NamespaceGkeManagedCCC},
			Spec:       cbv1beta1.CapacityBufferSpec{ProvisioningStrategy: &activeStrat},
			Status: cbv1beta1.CapacityBufferStatus{
				Conditions: []metav1.Condition{
					{Type: capacitybuffer.ReadyForProvisioningCondition, Status: metav1.ConditionTrue, Reason: "AttributesSetSuccessfully", Message: "ready"},
					{Type: capacitybuffer.ProvisioningCondition, Status: metav1.ConditionFalse, Reason: "BufferIsEmpty", Message: "CapacityBuffer has zero replicas"},
				},
			},
		}

		statusWriter := &mockStatusWriter{}
		r := &Reconciler{Client: &mockReconcileClient{cc: cc, statusWriter: statusWriter}}
		if err := r.reconcileStatus(context.Background(), cc, []*cbv1beta1.CapacityBuffer{cb}, nil); err != nil {
			t.Fatalf("reconcileStatus returned unexpected error: %v", err)
		}

		var patchedCC ccapiv1.ComputeClass
		if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
			t.Fatalf("Failed to unmarshal status patch: %v", err)
		}
		cond := meta.FindStatusCondition(patchedCC.Status.Conditions, conditionTypeBuffersConfigured)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "BufferIsEmpty" || cond.Message != `Buffer "cb-1": CapacityBuffer has zero replicas` {
			t.Errorf("Unexpected BuffersConfigured condition: %+v", cond)
		}
	})
}

func TestIsObservedBuffersEqual(t *testing.T) {
	bufA := ccapiv1.ObservedBufferStatus{
		Name:      "buf-a",
		Namespace: NamespaceGkeManagedCCC,
		Conditions: []metav1.Condition{
			{Type: "Ready", Status: metav1.ConditionTrue},
		},
	}
	bufB := ccapiv1.ObservedBufferStatus{
		Name:      "buf-b",
		Namespace: NamespaceGkeManagedCCC,
		Conditions: []metav1.Condition{
			{Type: "Ready", Status: metav1.ConditionFalse},
		},
	}

	// 1. Identical slices
	if !isObservedBuffersEqual([]ccapiv1.ObservedBufferStatus{bufA, bufB}, []ccapiv1.ObservedBufferStatus{bufA, bufB}) {
		t.Errorf("Expected identical slices to be equal")
	}

	// 2. Order independent
	if !isObservedBuffersEqual([]ccapiv1.ObservedBufferStatus{bufA, bufB}, []ccapiv1.ObservedBufferStatus{bufB, bufA}) {
		t.Errorf("Expected reordered slices with identical elements to be equal")
	}

	// 3. Different lengths
	if isObservedBuffersEqual([]ccapiv1.ObservedBufferStatus{bufA}, []ccapiv1.ObservedBufferStatus{bufA, bufB}) {
		t.Errorf("Expected different length slices to not be equal")
	}

	// 4. Different condition status
	bufAModified := bufA
	bufAModified.Conditions = []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionFalse},
	}
	if isObservedBuffersEqual([]ccapiv1.ObservedBufferStatus{bufA}, []ccapiv1.ObservedBufferStatus{bufAModified}) {
		t.Errorf("Expected modified condition slice to not be equal")
	}
}

type mockStatusWriter struct {
	client.SubResourceWriter
	patchedBytes []byte
	// onPatch, if set, runs after each status patch is recorded.
	onPatch func()
}

func (m *mockStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	m.patchedBytes = data
	if m.onPatch != nil {
		m.onPatch()
	}
	return nil
}

type mockReconcileClient struct {
	client.Client
	cc             *ccapiv1.ComputeClass
	cbItems        []cbv1beta1.CapacityBuffer
	ptItems        []corev1.PodTemplate
	statusWriter   *mockStatusWriter
	createCBError  func(cb *cbv1beta1.CapacityBuffer) error
	patchCBError   func(cb *cbv1beta1.CapacityBuffer) error
	deleteCBError  func(cb *cbv1beta1.CapacityBuffer) error
	patchPTError   func(pt *corev1.PodTemplate) error
	createdCBs     []*cbv1beta1.CapacityBuffer
	patchedCBs     []string
	patchedCBBytes map[string][]byte
	deletedCBs     []string
	patchedPTs     []string
	deletedPTs     []string
}

func (m *mockReconcileClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if out, ok := obj.(*ccapiv1.ComputeClass); ok && m.cc != nil && m.cc.Name == key.Name {
		m.cc.DeepCopyInto(out)
		return nil
	}
	return fmt.Errorf("unexpected Get(%v, %T)", key, obj)
}

func (m *mockReconcileClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := (&client.ListOptions{}).ApplyOptions(opts)
	matches := func(obj metav1.Object) bool {
		if listOpts.Namespace != "" && obj.GetNamespace() != listOpts.Namespace {
			return false
		}
		return listOpts.LabelSelector == nil || listOpts.LabelSelector.Matches(labels.Set(obj.GetLabels()))
	}
	switch out := list.(type) {
	case *cbv1beta1.CapacityBufferList:
		out.Items = nil
		for _, cb := range m.cbItems {
			if matches(&cb) {
				out.Items = append(out.Items, cb)
			}
		}
		return nil
	case *corev1.PodTemplateList:
		out.Items = nil
		for _, pt := range m.ptItems {
			if matches(&pt) {
				out.Items = append(out.Items, pt)
			}
		}
		return nil
	default:
		return fmt.Errorf("unexpected List(%T)", list)
	}
}

func (m *mockReconcileClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if cb, ok := obj.(*cbv1beta1.CapacityBuffer); ok {
		if m.createCBError != nil {
			if err := m.createCBError(cb); err != nil {
				return err
			}
		}
		if cb.Name == "" {
			cb.Name = fmt.Sprintf("%s%d", cb.GenerateName, len(m.createdCBs)+1)
		}
		cb.UID = types.UID("uid-" + cb.Name)
		m.createdCBs = append(m.createdCBs, cb.DeepCopy())
		return nil
	}
	return fmt.Errorf("unexpected Create(%T)", obj)
}

func (m *mockReconcileClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	switch o := obj.(type) {
	case *corev1.PodTemplate:
		if m.patchPTError != nil {
			return m.patchPTError(o)
		}
		m.patchedPTs = append(m.patchedPTs, o.Name)
		return nil
	case *cbv1beta1.CapacityBuffer:
		if m.patchCBError != nil {
			return m.patchCBError(o)
		}
		m.patchedCBs = append(m.patchedCBs, o.Name)
		if data, err := patch.Data(o); err == nil {
			if m.patchedCBBytes == nil {
				m.patchedCBBytes = make(map[string][]byte)
			}
			m.patchedCBBytes[o.Name] = data
		}
		return nil
	default:
		return fmt.Errorf("unexpected Patch(%T)", obj)
	}
}

func (m *mockReconcileClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	switch o := obj.(type) {
	case *cbv1beta1.CapacityBuffer:
		if m.deleteCBError != nil {
			return m.deleteCBError(o)
		}
		m.deletedCBs = append(m.deletedCBs, o.Name)
		return nil
	case *corev1.PodTemplate:
		m.deletedPTs = append(m.deletedPTs, o.Name)
		return nil
	default:
		return fmt.Errorf("unexpected Delete(%T)", obj)
	}
}

func (m *mockReconcileClient) Status() client.SubResourceWriter {
	return m.statusWriter
}

func TestReconcile_DuplicateProvisioningStrategy(t *testing.T) {
	tests := []struct {
		name    string
		buffers []ccapiv1.ComputeClassBuffer
	}{
		{
			name: "duplicate explicit standby strategy",
			buffers: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: standbyCapacityStrategy},
				{ProvisioningStrategy: standbyCapacityStrategy},
			},
		},
		{
			name: "duplicate explicit active strategy",
			buffers: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: activeCapacityStrategy},
				{ProvisioningStrategy: activeCapacityStrategy},
			},
		},
		{
			name: "duplicate empty default strategy",
			buffers: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: ""},
				{ProvisioningStrategy: ""},
			},
		},
		{
			name: "empty default strategy combined with explicit active strategy",
			buffers: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: ""},
				{ProvisioningStrategy: activeCapacityStrategy},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cc := &ccapiv1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-cc",
					Generation: 1,
				},
				Spec: ccapiv1.ComputeClassSpec{
					Buffers: tc.buffers,
				},
			}

			statusWriter := &mockStatusWriter{}
			mockClient := &mockReconcileClient{
				cc:           cc,
				statusWriter: statusWriter,
			}

			r := &Reconciler{Client: mockClient}
			_, err := r.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "test-cc"},
			})
			if err != nil {
				t.Fatalf("Reconcile returned unexpected error: %v", err)
			}

			if len(statusWriter.patchedBytes) == 0 {
				t.Fatalf("Expected status Patch to be called, got empty patch")
			}

			var patchedCC ccapiv1.ComputeClass
			if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
				t.Fatalf("Failed to unmarshal status patch payload: %v", err)
			}

			cond := meta.FindStatusCondition(patchedCC.Status.Conditions, conditionTypeBuffersConfigured)
			if cond == nil {
				t.Fatalf("Expected %s condition to be present, got nil", conditionTypeBuffersConfigured)
			}
			if cond.Status != metav1.ConditionFalse || cond.Reason != "DuplicateProvisioningStrategy" {
				t.Errorf("Expected status=False reason=DuplicateProvisioningStrategy, got status=%s reason=%s message=%q",
					cond.Status, cond.Reason, cond.Message)
			}
		})
	}
}

type mockBufferPatchClient struct {
	client.Client
}

func (m *mockBufferPatchClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return nil
}

func TestHandleBufferUpdate_PreservesLiveStatus(t *testing.T) {
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cc",
			UID:  "cc-uid-1",
		},
	}
	activeStrat := activeCapacityStrategy
	liveCB := &cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cb-1",
			Namespace: NamespaceGkeManagedCCC,
			UID:       "cb-uid-1",
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStrat,
		},
		Status: cbv1beta1.CapacityBufferStatus{
			Conditions: []metav1.Condition{
				{
					Type:    capacitybuffer.ReadyForProvisioningCondition,
					Status:  metav1.ConditionTrue,
					Reason:  "Ready",
					Message: "Buffer provisioned",
				},
			},
		},
	}

	r := &Reconciler{Client: &mockBufferPatchClient{}}
	desired := ccapiv1.ComputeClassBuffer{
		ProvisioningStrategy: activeCapacityStrategy,
	}
	pair := bufferMatchPair{
		Desired: &desired,
		Actual: &internalCapacityBuffer{
			Name: liveCB.Name,
			UID:  liveCB.UID,
		},
	}

	updatedCB, err := r.handleBufferUpdate(context.Background(), cc, map[string]*cbv1beta1.CapacityBuffer{liveCB.Name: liveCB}, pair, "pt-basic-test-cc")
	if err != nil {
		t.Fatalf("handleBufferUpdate returned unexpected error: %v", err)
	}
	cond := meta.FindStatusCondition(updatedCB.Status.Conditions, capacitybuffer.ReadyForProvisioningCondition)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Ready" {
		t.Errorf("Expected ReadyForProvisioning=True (Ready) to be preserved on updated CB, got %+v", cond)
	}
}

func TestRecordReconcileFailure_ZeroBuffers(t *testing.T) {
	ccUID := types.UID("cc-uid-1")
	ownerRef := metav1.OwnerReference{
		APIVersion: ccapiv1.SchemeGroupVersion.String(),
		Kind:       kindComputeClass,
		Name:       "test-cc",
		UID:        ccUID,
	}

	tests := []struct {
		name        string
		cc          *ccapiv1.ComputeClass
		cbItems     []cbv1beta1.CapacityBuffer
		ptItems     []corev1.PodTemplate
		err         error
		wantPatch   bool
		wantMessage string
	}{
		{
			name: "zero buffers with existing BuffersConfigured condition records failure",
			cc: &ccapiv1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-cc",
					UID:        ccUID,
					Generation: 2,
				},
				Spec: ccapiv1.ComputeClassSpec{
					Buffers: nil,
				},
				Status: ccapiv1.ComputeClassStatus{
					Conditions: []metav1.Condition{
						{
							Type:   conditionTypeBuffersConfigured,
							Status: metav1.ConditionFalse,
							Reason: "Pending",
						},
					},
				},
			},
			err:         fmt.Errorf("apiserver error"),
			wantPatch:   true,
			wantMessage: "Failed to reconcile capacity buffers: internal error",
		},
		{
			name: "zero buffers with owned CapacityBuffer records failure",
			cc: &ccapiv1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-cc",
					UID:        ccUID,
					Generation: 2,
				},
			},
			cbItems: []cbv1beta1.CapacityBuffer{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "cb-1",
						Namespace:       NamespaceGkeManagedCCC,
						Labels:          commonBufferLabels("test-cc"),
						OwnerReferences: []metav1.OwnerReference{ownerRef},
					},
				},
			},
			err:         apierrors.NewForbidden(schema.GroupResource{Group: "autoscaling.x-k8s.io", Resource: "capacitybuffers"}, "cb-1", fmt.Errorf("quota exceeded")),
			wantPatch:   true,
			wantMessage: "Failed to reconcile capacity buffers: insufficient permissions or quota",
		},
		{
			name: "zero buffers with owned PodTemplate records failure",
			cc: &ccapiv1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-cc",
					UID:        ccUID,
					Generation: 2,
				},
			},
			ptItems: []corev1.PodTemplate{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "pt-basic-test-cc",
						Namespace:       NamespaceGkeManagedCCC,
						Labels:          commonBufferLabels("test-cc"),
						OwnerReferences: []metav1.OwnerReference{ownerRef},
					},
				},
			},
			err:         apierrors.NewTimeoutError("request timed out", 5),
			wantPatch:   true,
			wantMessage: "Failed to reconcile capacity buffers: API server temporarily unavailable",
		},
		{
			name: "zero buffers on ComputeClass that never used buffers skips status update",
			cc: &ccapiv1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-cc",
					UID:        ccUID,
					Generation: 1,
				},
			},
			err:       fmt.Errorf("transient list error"),
			wantPatch: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			statusWriter := &mockStatusWriter{}
			mockClient := &mockReconcileClient{
				cc:           tc.cc,
				cbItems:      tc.cbItems,
				ptItems:      tc.ptItems,
				statusWriter: statusWriter,
			}
			r := &Reconciler{Client: mockClient}
			r.recordReconcileFailure(context.Background(), tc.cc, tc.err)

			if !tc.wantPatch {
				if len(statusWriter.patchedBytes) != 0 {
					t.Fatalf("Expected no status patch for unused ComputeClass, got %s", string(statusWriter.patchedBytes))
				}
				return
			}

			if len(statusWriter.patchedBytes) == 0 {
				t.Fatalf("Expected status Patch to be called on reconcile failure")
			}

			var patchedCC ccapiv1.ComputeClass
			if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
				t.Fatalf("Failed to unmarshal status patch payload: %v", err)
			}

			cond := meta.FindStatusCondition(patchedCC.Status.Conditions, conditionTypeBuffersConfigured)
			if cond == nil {
				t.Fatalf("Expected %s condition to be present, got nil", conditionTypeBuffersConfigured)
			}
			if cond.Status != metav1.ConditionFalse || cond.Reason != "ReconcileFailed" || cond.Message != tc.wantMessage {
				t.Errorf("Expected status=False reason=ReconcileFailed message=%q, got status=%s reason=%s message=%q",
					tc.wantMessage, cond.Status, cond.Reason, cond.Message)
			}
		})
	}
}

func TestSanitizeReconcileError(t *testing.T) {
	gr := schema.GroupResource{Group: "autoscaling.x-k8s.io", Resource: "capacitybuffers"}
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "invalid error",
			err:  apierrors.NewInvalid(schema.GroupKind{Group: gr.Group, Kind: "CapacityBuffer"}, "cb-1", nil),
			want: "Failed to reconcile capacity buffers: invalid resource configuration",
		},
		{
			name: "bad request error",
			err:  apierrors.NewBadRequest("bad request"),
			want: "Failed to reconcile capacity buffers: invalid resource configuration",
		},
		{
			name: "forbidden error",
			err:  apierrors.NewForbidden(gr, "cb-1", fmt.Errorf("denied")),
			want: "Failed to reconcile capacity buffers: insufficient permissions or quota",
		},
		{
			name: "unauthorized error",
			err:  apierrors.NewUnauthorized("unauthorized"),
			want: "Failed to reconcile capacity buffers: insufficient permissions or quota",
		},
		{
			name: "timeout error",
			err:  apierrors.NewTimeoutError("timeout", 1),
			want: "Failed to reconcile capacity buffers: API server temporarily unavailable",
		},
		{
			name: "conflict error",
			err:  apierrors.NewConflict(gr, "cb-1", fmt.Errorf("conflict")),
			want: "Failed to reconcile capacity buffers: resource update conflict",
		},
		{
			name: "joined conflict and forbidden prioritizes forbidden",
			err: errors.Join(
				apierrors.NewConflict(gr, "cb-1", fmt.Errorf("conflict")),
				apierrors.NewForbidden(gr, "cb-2", fmt.Errorf("quota exceeded")),
			),
			want: "Failed to reconcile capacity buffers: insufficient permissions or quota",
		},
		{
			name: "joined forbidden and invalid prioritizes invalid",
			err: errors.Join(
				apierrors.NewForbidden(gr, "cb-1", fmt.Errorf("quota exceeded")),
				apierrors.NewInvalid(schema.GroupKind{Group: gr.Group, Kind: "CapacityBuffer"}, "cb-2", nil),
			),
			want: "Failed to reconcile capacity buffers: invalid resource configuration",
		},
		{
			name: "generic error",
			err:  fmt.Errorf("unexpected internal failure"),
			want: "Failed to reconcile capacity buffers: internal error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitizeReconcileError(tc.err); got != tc.want {
				t.Errorf("sanitizeReconcileError(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestReconcile_PartialBufferFailure(t *testing.T) {
	ccUID := types.UID("cc-uid-1")
	ownerRef := metav1.OwnerReference{
		APIVersion: ccapiv1.SchemeGroupVersion.String(),
		Kind:       kindComputeClass,
		Name:       "test-cc",
		UID:        ccUID,
	}
	activeStrat := activeCapacityStrategy
	standbyStrat := standbyCapacityStrategy
	gr := schema.GroupResource{Group: "autoscaling.x-k8s.io", Resource: "capacitybuffers"}

	t.Run("one buffer fails update while sibling buffer converges and templates are pruned", func(t *testing.T) {
		cc := &ccapiv1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-cc",
				UID:        ccUID,
				Generation: 2,
			},
			Spec: ccapiv1.ComputeClassSpec{
				Buffers: []ccapiv1.ComputeClassBuffer{
					{
						ProvisioningStrategy: activeStrat,
						Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
					},
					{
						ProvisioningStrategy: standbyStrat,
						Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")},
					},
				},
			},
		}

		// cb-standby exists with outdated limits (4 CPU instead of 8 CPU) so it triggers handleBufferUpdate,
		// while cb-active does not exist yet so it triggers handleBufferCreate.
		cbStandby := cbv1beta1.CapacityBuffer{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "cb-standby",
				Namespace:       NamespaceGkeManagedCCC,
				UID:             "uid-cb-standby",
				Labels:          map[string]string{gkelabels.ComputeClassLabel: "test-cc"},
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
			Spec: cbv1beta1.CapacityBufferSpec{
				ProvisioningStrategy: &standbyStrat,
				Limits:               &cbv1beta1.ResourceList{cbv1beta1.ResourceName(corev1.ResourceCPU): resource.MustParse("4")},
			},
			Status: cbv1beta1.CapacityBufferStatus{
				Conditions: []metav1.Condition{
					{
						Type:    capacitybuffer.ReadyForProvisioningCondition,
						Status:  metav1.ConditionTrue,
						Reason:  "Ready",
						Message: "Buffer ready",
					},
				},
			},
		}

		orphanPT := corev1.PodTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "pt-orphan",
				Namespace:       NamespaceGkeManagedCCC,
				Labels:          map[string]string{gkelabels.ComputeClassLabel: "test-cc"},
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
		}

		statusWriter := &mockStatusWriter{}
		mockClient := &mockReconcileClient{
			cc:           cc,
			cbItems:      []cbv1beta1.CapacityBuffer{cbStandby},
			ptItems:      []corev1.PodTemplate{orphanPT},
			statusWriter: statusWriter,
			patchCBError: func(cb *cbv1beta1.CapacityBuffer) error {
				if cb.Name == "cb-standby" {
					return apierrors.NewForbidden(gr, cb.Name, fmt.Errorf("quota exceeded"))
				}
				return nil
			},
		}

		r := &Reconciler{Client: mockClient}
		_, err := r.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{Name: "test-cc"},
		})
		if err == nil {
			t.Fatalf("Expected Reconcile to return error when cb-standby update fails")
		}

		// Sibling buffer (active) should still have been created!
		if len(mockClient.createdCBs) != 1 {
			t.Fatalf("Expected sibling active buffer to be created despite cb-standby failure, got %d created", len(mockClient.createdCBs))
		}
		// Unneeded PodTemplate should still be pruned since no buffer deletion failed.
		if len(mockClient.deletedPTs) != 1 || mockClient.deletedPTs[0] != "pt-orphan" {
			t.Errorf("Expected orphan PodTemplate to be pruned, got deletedPTs=%v", mockClient.deletedPTs)
		}

		var patchedCC ccapiv1.ComputeClass
		if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
			t.Fatalf("Failed to unmarshal status patch: %v", err)
		}
		if len(patchedCC.Status.ObservedBuffers) != 2 {
			t.Fatalf("Expected 2 ObservedBuffers (both created active buffer and failed standby buffer), got %+v", patchedCC.Status.ObservedBuffers)
		}
		var foundStandby bool
		for _, obs := range patchedCC.Status.ObservedBuffers {
			if obs.Name == "cb-standby" {
				foundStandby = true
				cond := meta.FindStatusCondition(obs.Conditions, capacitybuffer.ReadyForProvisioningCondition)
				if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ReconcileFailed" {
					t.Errorf("Expected cb-standby to have ReadyForProvisioning=False (ReconcileFailed), got %+v", cond)
				}
			}
		}
		if !foundStandby {
			t.Errorf("Expected cb-standby in ObservedBuffers, got %+v", patchedCC.Status.ObservedBuffers)
		}
		cond := meta.FindStatusCondition(patchedCC.Status.Conditions, conditionTypeBuffersConfigured)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ReconcileFailed" {
			t.Errorf("Expected BuffersConfigured=False (ReconcileFailed), got %+v", cond)
		}
	})

	t.Run("buffer deletion failure skips template pruning and retains buffer in ObservedBuffers", func(t *testing.T) {
		cc := &ccapiv1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-cc",
				UID:        ccUID,
				Generation: 3,
			},
			Spec: ccapiv1.ComputeClassSpec{
				Buffers: nil,
			},
		}

		cbStandby := cbv1beta1.CapacityBuffer{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "cb-standby",
				Namespace:       NamespaceGkeManagedCCC,
				UID:             "uid-cb-standby",
				Labels:          map[string]string{gkelabels.ComputeClassLabel: "test-cc"},
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
			Spec: cbv1beta1.CapacityBufferSpec{
				ProvisioningStrategy: &standbyStrat,
			},
		}
		standbyPT := corev1.PodTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "pt-standby",
				Namespace:       NamespaceGkeManagedCCC,
				Labels:          map[string]string{gkelabels.ComputeClassLabel: "test-cc"},
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
		}

		statusWriter := &mockStatusWriter{}
		mockClient := &mockReconcileClient{
			cc:           cc,
			cbItems:      []cbv1beta1.CapacityBuffer{cbStandby},
			ptItems:      []corev1.PodTemplate{standbyPT},
			statusWriter: statusWriter,
			deleteCBError: func(cb *cbv1beta1.CapacityBuffer) error {
				return apierrors.NewForbidden(gr, cb.Name, fmt.Errorf("cannot delete"))
			},
		}

		r := &Reconciler{Client: mockClient}
		_, err := r.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{Name: "test-cc"},
		})
		if err == nil {
			t.Fatalf("Expected Reconcile to return error when buffer deletion fails")
		}

		if len(mockClient.deletedPTs) != 0 {
			t.Errorf("Expected pruneUnneededTemplates to be skipped when buffer deletion fails, got deletedPTs=%v", mockClient.deletedPTs)
		}

		var patchedCC ccapiv1.ComputeClass
		if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
			t.Fatalf("Failed to unmarshal status patch: %v", err)
		}
		if len(patchedCC.Status.ObservedBuffers) != 1 || patchedCC.Status.ObservedBuffers[0].Name != "cb-standby" {
			t.Fatalf("Expected cb-standby to remain in ObservedBuffers when delete fails, got %+v", patchedCC.Status.ObservedBuffers)
		}
		cond := meta.FindStatusCondition(patchedCC.Status.Conditions, conditionTypeBuffersConfigured)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ReconcileFailed" {
			t.Errorf("Expected BuffersConfigured=False (ReconcileFailed), got %+v", cond)
		}
	})

	t.Run("applyRequiredTemplates failure gates reconcileBuffers", func(t *testing.T) {
		cc := &ccapiv1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-cc",
				UID:        ccUID,
				Generation: 2,
			},
			Spec: ccapiv1.ComputeClassSpec{
				Buffers: []ccapiv1.ComputeClassBuffer{
					{ProvisioningStrategy: activeStrat},
					{ProvisioningStrategy: standbyStrat},
				},
			},
		}

		statusWriter := &mockStatusWriter{}
		var patchedPTCount int
		mockClient := &mockReconcileClient{
			cc:           cc,
			statusWriter: statusWriter,
			patchPTError: func(pt *corev1.PodTemplate) error {
				patchedPTCount++
				return apierrors.NewForbidden(schema.GroupResource{Resource: "podtemplates"}, pt.Name, fmt.Errorf("forbidden"))
			},
		}

		r := &Reconciler{Client: mockClient}
		_, err := r.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{Name: "test-cc"},
		})
		if err == nil {
			t.Fatalf("Expected Reconcile to return error when applyRequiredTemplates fails")
		}
		if patchedPTCount != 2 {
			t.Errorf("Expected both strategy PodTemplates to be attempted, got %d", patchedPTCount)
		}
		if len(mockClient.createdCBs) != 0 {
			t.Errorf("Expected reconcileBuffers not to run when applyRequiredTemplates fails, got %d created CBs", len(mockClient.createdCBs))
		}

		var patchedCC ccapiv1.ComputeClass
		if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
			t.Fatalf("Failed to unmarshal status patch: %v", err)
		}
		cond := meta.FindStatusCondition(patchedCC.Status.Conditions, conditionTypeBuffersConfigured)
		wantMessage := "Failed to reconcile capacity buffers: insufficient permissions or quota"
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ReconcileFailed" || cond.Message != wantMessage {
			t.Errorf("Expected BuffersConfigured=False (ReconcileFailed) with message %q, got %+v", wantMessage, cond)
		}
	})

	t.Run("buffer create failure is reported on the ComputeClass", func(t *testing.T) {
		cc := &ccapiv1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "test-cc",
				UID:        ccUID,
				Generation: 2,
			},
			Spec: ccapiv1.ComputeClassSpec{
				Buffers: []ccapiv1.ComputeClassBuffer{
					{
						ProvisioningStrategy: activeStrat,
						Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
					},
				},
			},
		}

		statusWriter := &mockStatusWriter{}
		mockClient := &mockReconcileClient{
			cc:           cc,
			statusWriter: statusWriter,
			createCBError: func(cb *cbv1beta1.CapacityBuffer) error {
				return apierrors.NewForbidden(gr, cb.GenerateName, fmt.Errorf("quota exceeded"))
			},
		}

		r := &Reconciler{Client: mockClient}
		_, err := r.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: types.NamespacedName{Name: "test-cc"},
		})
		if err == nil {
			t.Fatalf("Expected Reconcile to return error when buffer creation fails")
		}
		if len(mockClient.createdCBs) != 0 {
			t.Fatalf("Expected no CapacityBuffer to be created, got %d", len(mockClient.createdCBs))
		}
		if pending := r.getOrCreateCache("test-cc").GetPendingBuffers(); len(pending) != 0 {
			t.Errorf("Expected no in-flight create after a failed Create, got %+v", pending)
		}

		// No CapacityBuffer exists to carry the failure, so only the ComputeClass condition reports it.
		var patchedCC ccapiv1.ComputeClass
		if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
			t.Fatalf("Failed to unmarshal status patch: %v", err)
		}
		if len(patchedCC.Status.ObservedBuffers) != 0 {
			t.Errorf("Expected no ObservedBuffers, got %+v", patchedCC.Status.ObservedBuffers)
		}
		cond := meta.FindStatusCondition(patchedCC.Status.Conditions, conditionTypeBuffersConfigured)
		wantMessage := "Failed to reconcile capacity buffers: insufficient permissions or quota"
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ReconcileFailed" || cond.Message != wantMessage {
			t.Errorf("Expected BuffersConfigured=False (ReconcileFailed) with message %q, got %+v", wantMessage, cond)
		}
	})
}

func TestReconcile_DeletionTimestampEvictsCache(t *testing.T) {
	now := metav1.Now()
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleting-cc",
			DeletionTimestamp: &now,
		},
	}

	r := &Reconciler{Client: &mockReconcileClient{cc: cc}}
	initialCache := r.getOrCreateCache("deleting-cc")

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "deleting-cc"},
	})
	if err != nil {
		t.Fatalf("Reconcile returned unexpected error: %v", err)
	}

	if _, exists := r.caches.Load("deleting-cc"); exists {
		t.Errorf("Expected cache for deleting-cc to be evicted when DeletionTimestamp is set")
	}
	if r.getOrCreateCache("deleting-cc") == initialCache {
		t.Errorf("Expected a new cache instance after eviction")
	}
}

func TestReconcile_LifecycleFlow(t *testing.T) {
	ccUID := types.UID("cc-uid-lifecycle")
	ownerRef := metav1.OwnerReference{
		APIVersion: ccapiv1.SchemeGroupVersion.String(),
		Kind:       kindComputeClass,
		Name:       "test-cc",
		UID:        ccUID,
	}
	activeStrat := activeCapacityStrategy
	standbyStrat := standbyCapacityStrategy

	// Step 1: Initial create (active + standby)
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cc",
			UID:        ccUID,
			Generation: 1,
		},
		Spec: ccapiv1.ComputeClassSpec{
			Buffers: []ccapiv1.ComputeClassBuffer{
				{
					ProvisioningStrategy: activeStrat,
					Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
				},
				{
					ProvisioningStrategy: standbyStrat,
					Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")},
				},
			},
		},
	}

	statusWriter := &mockStatusWriter{}
	mockClient := &mockReconcileClient{
		cc:           cc,
		statusWriter: statusWriter,
	}
	r := &Reconciler{Client: mockClient}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-cc"},
	}); err != nil {
		t.Fatalf("Step 1 (initial create): Reconcile returned unexpected error: %v", err)
	}

	if len(mockClient.patchedPTs) != 2 || mockClient.patchedPTs[0] != "pt-basic-test-cc" || mockClient.patchedPTs[1] != "pt-standby-test-cc" {
		t.Fatalf("Step 1: expected PodTemplates [pt-basic-test-cc, pt-standby-test-cc] to be applied via SSA, got %v", mockClient.patchedPTs)
	}
	if len(mockClient.createdCBs) != 2 {
		t.Fatalf("Step 1: expected 2 CapacityBuffers to be created, got %d", len(mockClient.createdCBs))
	}

	var patchedCCStep1 ccapiv1.ComputeClass
	if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCCStep1); err != nil {
		t.Fatalf("Step 1: failed to unmarshal status patch: %v", err)
	}
	condStep1 := meta.FindStatusCondition(patchedCCStep1.Status.Conditions, conditionTypeBuffersConfigured)
	if condStep1 == nil || condStep1.Status != metav1.ConditionFalse || condStep1.Reason != "Pending" {
		t.Errorf("Step 1: expected BuffersConfigured=False (Pending) during initial creation, got %+v", condStep1)
	}
	if len(patchedCCStep1.Status.ObservedBuffers) != 2 {
		t.Fatalf("Step 1: expected 2 ObservedBuffers, got %+v", patchedCCStep1.Status.ObservedBuffers)
	}

	// Step 2: Limits update via SSA (active 4 CPU -> 16 CPU) & Ready status aggregation
	activeCB := *mockClient.createdCBs[0].DeepCopy()
	activeCB.Status.Conditions = []metav1.Condition{
		{
			Type:    capacitybuffer.ReadyForProvisioningCondition,
			Status:  metav1.ConditionTrue,
			Reason:  "Ready",
			Message: "Active buffer ready",
		},
	}
	standbyCB := *mockClient.createdCBs[1].DeepCopy()
	standbyCB.Status.Conditions = []metav1.Condition{
		{
			Type:    capacitybuffer.ReadyForProvisioningCondition,
			Status:  metav1.ConditionTrue,
			Reason:  "Ready",
			Message: "Standby buffer ready",
		},
	}

	ptBasic := corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pt-basic-test-cc",
			Namespace:       NamespaceGkeManagedCCC,
			Labels:          commonBufferLabels("test-cc"),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
	}
	ptStandby := corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pt-standby-test-cc",
			Namespace:       NamespaceGkeManagedCCC,
			Labels:          commonBufferLabels("test-cc"),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
	}

	cc.Generation = 2
	cc.Spec.Buffers[0].Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16")}
	cc.Status = patchedCCStep1.Status

	mockClient.cbItems = []cbv1beta1.CapacityBuffer{activeCB, standbyCB}
	mockClient.ptItems = []corev1.PodTemplate{ptBasic, ptStandby}
	mockClient.createdCBs = nil
	mockClient.patchedCBs = nil
	mockClient.patchedCBBytes = nil
	mockClient.deletedCBs = nil
	mockClient.patchedPTs = nil
	mockClient.deletedPTs = nil
	statusWriter.patchedBytes = nil

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-cc"},
	}); err != nil {
		t.Fatalf("Step 2 (limits update): Reconcile returned unexpected error: %v", err)
	}

	if len(mockClient.createdCBs) != 0 || len(mockClient.deletedCBs) != 0 {
		t.Fatalf("Step 2: expected no CB creates or deletes during in-place update, got created=%d deleted=%d", len(mockClient.createdCBs), len(mockClient.deletedCBs))
	}
	if len(mockClient.patchedCBs) != 1 || mockClient.patchedCBs[0] != activeCB.Name {
		t.Fatalf("Step 2: expected a merge patch only on %s, got %v", activeCB.Name, mockClient.patchedCBs)
	}
	var patchedActiveCB cbv1beta1.CapacityBuffer
	if err := json.Unmarshal(mockClient.patchedCBBytes[activeCB.Name], &patchedActiveCB); err != nil {
		t.Fatalf("Step 2: failed to unmarshal CB merge patch: %v", err)
	}
	if patchedActiveCB.Spec.Limits == nil {
		t.Fatalf("Step 2: expected patched CB limits to be set, got nil")
	}
	gotCPU := (*patchedActiveCB.Spec.Limits)[cbv1beta1.ResourceName(corev1.ResourceCPU)]
	if !gotCPU.Equal(resource.MustParse("16")) {
		t.Errorf("Step 2: expected patched CB CPU limit to be 16, got %v", gotCPU)
	}

	var patchedCCStep2 ccapiv1.ComputeClass
	if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCCStep2); err != nil {
		t.Fatalf("Step 2: failed to unmarshal status patch: %v", err)
	}
	condStep2 := meta.FindStatusCondition(patchedCCStep2.Status.Conditions, conditionTypeBuffersConfigured)
	if condStep2 == nil || condStep2.Status != metav1.ConditionTrue || condStep2.Reason != "Ready" {
		t.Errorf("Step 2: expected BuffersConfigured=True (Ready), got %+v", condStep2)
	}

	// Step 3: Strategy removal (remove standby -> standby CB deleted + pt-standby-test-cc pruned)
	activeCB.Spec.Limits = &cbv1beta1.ResourceList{cbv1beta1.ResourceName(corev1.ResourceCPU): resource.MustParse("16")}
	cc.Generation = 3
	cc.Spec.Buffers = cc.Spec.Buffers[:1] // Keep only active strategy
	cc.Status = patchedCCStep2.Status

	mockClient.cbItems = []cbv1beta1.CapacityBuffer{activeCB, standbyCB}
	mockClient.ptItems = []corev1.PodTemplate{ptBasic, ptStandby}
	mockClient.createdCBs = nil
	mockClient.patchedCBs = nil
	mockClient.deletedCBs = nil
	mockClient.patchedPTs = nil
	mockClient.deletedPTs = nil
	statusWriter.patchedBytes = nil

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-cc"},
	}); err != nil {
		t.Fatalf("Step 3 (strategy removal): Reconcile returned unexpected error: %v", err)
	}

	if len(mockClient.deletedCBs) != 1 || mockClient.deletedCBs[0] != standbyCB.Name {
		t.Errorf("Step 3: expected standby CapacityBuffer %s to be deleted, got %v", standbyCB.Name, mockClient.deletedCBs)
	}
	if len(mockClient.deletedPTs) != 1 || mockClient.deletedPTs[0] != "pt-standby-test-cc" {
		t.Errorf("Step 3: expected pt-standby-test-cc to be pruned, got %v", mockClient.deletedPTs)
	}

	var patchedCCStep3 ccapiv1.ComputeClass
	if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCCStep3); err != nil {
		t.Fatalf("Step 3: failed to unmarshal status patch: %v", err)
	}
	if len(patchedCCStep3.Status.ObservedBuffers) != 1 || patchedCCStep3.Status.ObservedBuffers[0].Name != activeCB.Name {
		t.Errorf("Step 3: expected ObservedBuffers to contain only %s, got %+v", activeCB.Name, patchedCCStep3.Status.ObservedBuffers)
	}

	// Step 4: Last buffer removal (active CB deleted + pt-basic-test-cc pruned + our status fields cleared)
	cc.Generation = 4
	cc.Spec.Buffers = nil
	cc.Status = patchedCCStep3.Status

	mockClient.cbItems = []cbv1beta1.CapacityBuffer{activeCB}
	mockClient.ptItems = []corev1.PodTemplate{ptBasic}
	mockClient.createdCBs = nil
	mockClient.patchedCBs = nil
	mockClient.deletedCBs = nil
	mockClient.patchedPTs = nil
	mockClient.deletedPTs = nil
	statusWriter.patchedBytes = nil

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-cc"},
	}); err != nil {
		t.Fatalf("Step 4 (last buffer removal): Reconcile returned unexpected error: %v", err)
	}

	if len(mockClient.deletedCBs) != 1 || mockClient.deletedCBs[0] != activeCB.Name {
		t.Errorf("Step 4: expected active CapacityBuffer %s to be deleted, got %v", activeCB.Name, mockClient.deletedCBs)
	}
	if len(mockClient.deletedPTs) != 1 || mockClient.deletedPTs[0] != "pt-basic-test-cc" {
		t.Errorf("Step 4: expected pt-basic-test-cc to be pruned, got %v", mockClient.deletedPTs)
	}
	if len(statusWriter.patchedBytes) == 0 {
		t.Fatalf("Step 4: expected a status patch that clears BuffersConfigured and ObservedBuffers")
	}
	var patchedCCStep4 ccapiv1.ComputeClass
	if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCCStep4); err != nil {
		t.Fatalf("Step 4: failed to unmarshal status patch: %v", err)
	}
	// The apply omits the fields we own, so SSA removes them.
	if len(patchedCCStep4.Status.Conditions) != 0 || len(patchedCCStep4.Status.ObservedBuffers) != 0 {
		t.Errorf("Step 4: expected the status apply to omit conditions and ObservedBuffers, got %+v", patchedCCStep4.Status)
	}

	// Step 5: Steady state without buffers (no writes, no status patch, no requeue)
	cc.Status = patchedCCStep4.Status

	mockClient.cbItems = nil
	mockClient.ptItems = nil
	mockClient.deletedCBs = nil
	mockClient.deletedPTs = nil
	statusWriter.patchedBytes = nil

	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-cc"},
	})
	if err != nil {
		t.Fatalf("Step 5 (steady state): Reconcile returned unexpected error: %v", err)
	}
	if len(mockClient.createdCBs) != 0 || len(mockClient.patchedCBs) != 0 || len(mockClient.deletedCBs) != 0 ||
		len(mockClient.patchedPTs) != 0 || len(mockClient.deletedPTs) != 0 {
		t.Errorf("Step 5: expected no CB or PT writes, got created=%d patchedCBs=%v deletedCBs=%v patchedPTs=%v deletedPTs=%v",
			len(mockClient.createdCBs), mockClient.patchedCBs, mockClient.deletedCBs, mockClient.patchedPTs, mockClient.deletedPTs)
	}
	if len(statusWriter.patchedBytes) != 0 {
		t.Errorf("Step 5: expected no status patch in steady state, got %s", statusWriter.patchedBytes)
	}
	if res != (reconcile.Result{}) {
		t.Errorf("Step 5: expected no requeue, got %+v", res)
	}
}

func TestReconcile_InFlightCachePreventsDuplicateCreateOnStaleList(t *testing.T) {
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cc",
			UID:        "cc-uid-inflight",
			Generation: 1,
		},
		Spec: ccapiv1.ComputeClassSpec{
			Buffers: []ccapiv1.ComputeClassBuffer{
				{
					ProvisioningStrategy: activeCapacityStrategy,
					Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
				},
			},
		},
	}

	statusWriter := &mockStatusWriter{}
	mockClient := &mockReconcileClient{
		cc:           cc,
		cbItems:      nil, // List remains empty across both passes to simulate informer lag
		statusWriter: statusWriter,
	}
	r := &Reconciler{Client: mockClient}

	// Pass 1: Creates the buffer and records it in cccInFlightCache
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-cc"},
	}); err != nil {
		t.Fatalf("Pass 1: Reconcile returned unexpected error: %v", err)
	}
	if len(mockClient.createdCBs) != 1 {
		t.Fatalf("Pass 1: expected 1 CapacityBuffer to be created, got %d", len(mockClient.createdCBs))
	}
	createdName := mockClient.createdCBs[0].Name

	// Pass 2: List is still empty (stale informer cache); in-flight cache must prevent a duplicate Create
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-cc"},
	}); err != nil {
		t.Fatalf("Pass 2: Reconcile returned unexpected error: %v", err)
	}
	if len(mockClient.createdCBs) != 1 {
		t.Fatalf("Pass 2: expected in-flight cache to prevent duplicate Create on stale List, got %d total creates", len(mockClient.createdCBs))
	}
	if len(mockClient.patchedCBs) != 0 || len(mockClient.deletedCBs) != 0 {
		t.Errorf("Pass 2: expected no CB patch or delete for pending in-flight buffer, got patched=%v deleted=%v", mockClient.patchedCBs, mockClient.deletedCBs)
	}

	var patchedCC ccapiv1.ComputeClass
	if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
		t.Fatalf("Pass 2: failed to unmarshal status patch: %v", err)
	}
	if len(patchedCC.Status.ObservedBuffers) != 1 || patchedCC.Status.ObservedBuffers[0].Name != createdName {
		t.Errorf("Pass 2: expected ObservedBuffers to retain pending buffer %s, got %+v", createdName, patchedCC.Status.ObservedBuffers)
	}
}

func TestReconcile_IgnoresForeignOwnerUID(t *testing.T) {
	ccUID := types.UID("current-cc-uid")
	foreignOwnerRef := metav1.OwnerReference{
		APIVersion: ccapiv1.SchemeGroupVersion.String(),
		Kind:       kindComputeClass,
		Name:       "test-cc",
		UID:        types.UID("foreign-old-cc-uid"),
	}
	activeStrat := activeCapacityStrategy

	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cc",
			UID:        ccUID,
			Generation: 1,
		},
		Spec: ccapiv1.ComputeClassSpec{
			Buffers: []ccapiv1.ComputeClassBuffer{
				{
					ProvisioningStrategy: activeStrat,
					Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
				},
			},
		},
	}

	// Foreign CapacityBuffer and PodTemplate have matching labels in gke-managed-ccc, but a different OwnerReference UID.
	foreignCB := cbv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cb-foreign",
			Namespace:       NamespaceGkeManagedCCC,
			UID:             "uid-cb-foreign",
			Labels:          commonBufferLabels("test-cc"),
			OwnerReferences: []metav1.OwnerReference{foreignOwnerRef},
		},
		Spec: cbv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &activeStrat,
			Limits:               &cbv1beta1.ResourceList{cbv1beta1.ResourceName(corev1.ResourceCPU): resource.MustParse("4")},
		},
	}
	foreignPT := corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pt-foreign",
			Namespace:       NamespaceGkeManagedCCC,
			Labels:          commonBufferLabels("test-cc"),
			OwnerReferences: []metav1.OwnerReference{foreignOwnerRef},
		},
	}

	statusWriter := &mockStatusWriter{}
	mockClient := &mockReconcileClient{
		cc:           cc,
		cbItems:      []cbv1beta1.CapacityBuffer{foreignCB},
		ptItems:      []corev1.PodTemplate{foreignPT},
		statusWriter: statusWriter,
	}
	r := &Reconciler{Client: mockClient}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "test-cc"},
	}); err != nil {
		t.Fatalf("Reconcile returned unexpected error: %v", err)
	}

	// Foreign CB must be ignored: neither matched/updated nor deleted, and a new CB owned by ccUID must be created.
	if len(mockClient.createdCBs) != 1 {
		t.Fatalf("Expected 1 new CapacityBuffer to be created for current UID, got %d", len(mockClient.createdCBs))
	}
	if len(mockClient.patchedCBs) != 0 || len(mockClient.deletedCBs) != 0 {
		t.Errorf("Expected foreign CapacityBuffer to be untouched (neither patched nor deleted), got patched=%v deleted=%v", mockClient.patchedCBs, mockClient.deletedCBs)
	}
	if len(mockClient.deletedPTs) != 0 {
		t.Errorf("Expected foreign PodTemplate to be untouched by pruneUnneededTemplates, got deletedPTs=%v", mockClient.deletedPTs)
	}

	var patchedCC ccapiv1.ComputeClass
	if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
		t.Fatalf("Failed to unmarshal status patch: %v", err)
	}
	if len(patchedCC.Status.ObservedBuffers) != 1 || patchedCC.Status.ObservedBuffers[0].Name != mockClient.createdCBs[0].Name {
		t.Errorf("Expected ObservedBuffers to contain only newly created CB %s, got %+v", mockClient.createdCBs[0].Name, patchedCC.Status.ObservedBuffers)
	}
}

func TestNewJSONClient_CreatesCapacityBuffer(t *testing.T) {
	// Like the apiserver for CRDs: accept only JSON bodies and echo the object back.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if ct := req.Header.Get("Content-Type"); ct != runtime.ContentTypeJSON {
			http.Error(w, "unsupported content type "+ct, http.StatusUnsupportedMediaType)
			return
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", runtime.ContentTypeJSON)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	scheme := runtime.NewScheme()
	if err := cbv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add CapacityBuffer to scheme: %v", err)
	}
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(cbv1beta1.SchemeGroupVersion.WithKind("CapacityBuffer"), meta.RESTScopeNamespace)
	opts := client.Options{Scheme: scheme, Mapper: mapper}
	// Same ContentType as the rest config CA builds with the default --kube-api-content-type.
	cfg := &rest.Config{Host: srv.URL, ContentConfig: rest.ContentConfig{ContentType: runtime.ContentTypeProtobuf}}
	newCB := func() *cbv1beta1.CapacityBuffer {
		return &cbv1beta1.CapacityBuffer{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: NamespaceGkeManagedCCC}}
	}

	protobufClient, err := client.New(cfg, opts)
	if err != nil {
		t.Fatalf("Failed to create protobuf client: %v", err)
	}
	if err := protobufClient.Create(context.Background(), newCB()); err == nil {
		t.Fatal("Expected Create with the protobuf config to fail encoding the CapacityBuffer")
	}

	jsonClient, err := newJSONClient(cfg, opts)
	if err != nil {
		t.Fatalf("Failed to create JSON client: %v", err)
	}
	if err := jsonClient.Create(context.Background(), newCB()); err != nil {
		t.Errorf("Expected Create with the JSON client to succeed, got %v", err)
	}
	if cfg.ContentType != runtime.ContentTypeProtobuf {
		t.Errorf("Expected newJSONClient to leave the input config unchanged, got ContentType %q", cfg.ContentType)
	}
}

// mockCBPatchRecorder records CapacityBuffer patches.
type mockCBPatchRecorder struct {
	client.Client
	patchErr      error
	patchTypes    []types.PatchType
	patchData     []string
	fieldManagers []string
}

func (m *mockCBPatchRecorder) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	m.patchTypes = append(m.patchTypes, patch.Type())
	m.patchData = append(m.patchData, string(data))
	m.fieldManagers = append(m.fieldManagers, (&client.PatchOptions{}).ApplyOptions(opts).FieldManager)
	return m.patchErr
}

func TestPatchCapacityBuffer(t *testing.T) {
	cc := &ccapiv1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "test-cc", UID: "cc-uid-1"}}
	patchErr := errors.New("patch failed")
	tests := []struct {
		name          string
		liveLimits    cbv1beta1.ResourceList
		editLive      func(cb *cbv1beta1.CapacityBuffer)
		desiredLimits corev1.ResourceList
		patchErr      error
		wantPatch     string
	}{
		{
			name:          "removed key is nulled",
			liveLimits:    cbv1beta1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
			desiredLimits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			wantPatch:     `{"spec":{"limits":{"memory":null}}}`,
		},
		{
			name:          "extended resource key needs no escaping",
			liveLimits:    cbv1beta1.ResourceList{"cpu": resource.MustParse("1"), "nvidia.com/gpu": resource.MustParse("1")},
			desiredLimits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			wantPatch:     `{"spec":{"limits":{"nvidia.com/gpu":null}}}`,
		},
		{
			name:          "value change and key removal share one patch",
			liveLimits:    cbv1beta1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
			desiredLimits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			wantPatch:     `{"spec":{"limits":{"cpu":"2","memory":null}}}`,
		},
		{
			name:       "drifted PodTemplateRef and label are restored",
			liveLimits: cbv1beta1.ResourceList{"cpu": resource.MustParse("1")},
			editLive: func(cb *cbv1beta1.CapacityBuffer) {
				cb.Spec.PodTemplateRef.Name = "pt-standby-test-cc"
				delete(cb.Labels, labelManagedBy)
			},
			desiredLimits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			wantPatch:     `{"metadata":{"labels":{"autoscaling.gke.io/managed-by":"ccc-cb-controller"}},"spec":{"podTemplateRef":{"name":"pt-basic-test-cc"}}}`,
		},
		{
			name:          "patch error is returned",
			liveLimits:    cbv1beta1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
			desiredLimits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			patchErr:      patchErr,
			wantPatch:     `{"spec":{"limits":{"memory":null}}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := ccapiv1.ComputeClassBuffer{ProvisioningStrategy: activeCapacityStrategy, Limits: tc.desiredLimits}
			live := buildCapacityBuffer(cc, buf, "pt-basic-test-cc", "cb-1", "cb-uid-1")
			live.Spec.Limits = &tc.liveLimits
			if tc.editLive != nil {
				tc.editLive(live)
			}
			mockClient := &mockCBPatchRecorder{patchErr: tc.patchErr}
			r := &Reconciler{Client: mockClient}

			err := r.patchCapacityBuffer(context.Background(), cc, live, buf, "pt-basic-test-cc")
			if !errors.Is(err, tc.patchErr) {
				t.Fatalf("Expected error %v, got %v", tc.patchErr, err)
			}
			if !slices.Equal(mockClient.patchTypes, []types.PatchType{types.MergePatchType}) {
				t.Fatalf("Expected one merge patch, got %v", mockClient.patchTypes)
			}
			if got := mockClient.patchData[0]; got != tc.wantPatch {
				t.Errorf("Expected patch %s, got %s", tc.wantPatch, got)
			}
			if got := mockClient.fieldManagers[0]; got != fieldOwnerCCC {
				t.Errorf("Expected field manager %q, got %q", fieldOwnerCCC, got)
			}
		})
	}
}

func TestApplyRequiredTemplates_AppliesSharedTemplateOnce(t *testing.T) {
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cc", UID: "cc-uid-1"},
		Spec: ccapiv1.ComputeClassSpec{
			Buffers: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: activeCapacityStrategy},
				{ProvisioningStrategy: "custom.x-k8s.io/special"},
				{ProvisioningStrategy: standbyCapacityStrategy},
			},
		},
	}
	mockClient := &mockReconcileClient{cc: cc}
	r := &Reconciler{Client: mockClient}

	got, err := r.applyRequiredTemplates(context.Background(), cc)
	if err != nil {
		t.Fatalf("applyRequiredTemplates returned unexpected error: %v", err)
	}
	// Active and custom strategies share pt-basic-, so it is applied once.
	wantPTs := []string{"pt-basic-test-cc", "pt-standby-test-cc"}
	if !slices.Equal(mockClient.patchedPTs, wantPTs) {
		t.Errorf("Expected PodTemplate applies %v, got %v", wantPTs, mockClient.patchedPTs)
	}
	if !got.Equal(sets.New(wantPTs...)) {
		t.Errorf("Expected required PodTemplate names %v, got %v", wantPTs, sets.List(got))
	}
}

func TestReconcile_ListedBufferEndsInFlightCreate(t *testing.T) {
	tests := []struct {
		name string
		// ccUID is the ComputeClass UID in pass 2; pass 1 runs with "cc-uid-1".
		ccUID  types.UID
		listAs func(cb *cbv1beta1.CapacityBuffer)
	}{
		{
			name:  "buffer is terminating",
			ccUID: "cc-uid-1",
			listAs: func(cb *cbv1beta1.CapacityBuffer) {
				now := metav1.Now()
				cb.DeletionTimestamp = &now
				cb.Finalizers = []string{"example.com/hold"}
			},
		},
		{
			name:   "ComputeClass was recreated with a new UID",
			ccUID:  "cc-uid-2",
			listAs: func(cb *cbv1beta1.CapacityBuffer) {},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cc := &ccapiv1.ComputeClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cc", UID: "cc-uid-1", Generation: 1},
				Spec: ccapiv1.ComputeClassSpec{
					Buffers: []ccapiv1.ComputeClassBuffer{{
						ProvisioningStrategy: activeCapacityStrategy,
						Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
					}},
				},
			}
			mockClient := &mockReconcileClient{cc: cc, statusWriter: &mockStatusWriter{}}
			r := &Reconciler{Client: mockClient}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-cc"}}

			// Pass 1: creates a buffer that no List has shown yet.
			if _, err := r.Reconcile(context.Background(), req); err != nil {
				t.Fatalf("Pass 1: Reconcile returned unexpected error: %v", err)
			}
			if len(mockClient.createdCBs) != 1 {
				t.Fatalf("Pass 1: expected 1 CapacityBuffer to be created, got %d", len(mockClient.createdCBs))
			}

			// Pass 2: the buffer is listed but can't serve the ComputeClass, so it must be replaced.
			listed := mockClient.createdCBs[0].DeepCopy()
			tc.listAs(listed)
			mockClient.cbItems = []cbv1beta1.CapacityBuffer{*listed}
			mockClient.cc = cc.DeepCopy()
			mockClient.cc.UID = tc.ccUID
			if _, err := r.Reconcile(context.Background(), req); err != nil {
				t.Fatalf("Pass 2: Reconcile returned unexpected error: %v", err)
			}
			if len(mockClient.createdCBs) != 2 {
				t.Fatalf("Pass 2: expected a replacement CapacityBuffer, got %d total creates", len(mockClient.createdCBs))
			}
			if refs := mockClient.createdCBs[1].OwnerReferences; len(refs) != 1 || refs[0].UID != tc.ccUID {
				t.Errorf("Pass 2: expected the replacement to be owned by %s, got %+v", tc.ccUID, refs)
			}
			if len(mockClient.patchedCBs) != 0 || len(mockClient.deletedCBs) != 0 {
				t.Errorf("Pass 2: expected the listed buffer to be untouched, got patched=%v deleted=%v", mockClient.patchedCBs, mockClient.deletedCBs)
			}
		})
	}
}

func TestReconcile_RequeuesWhileCreatesInFlight(t *testing.T) {
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cc", UID: "cc-uid-1", Generation: 1},
		Spec: ccapiv1.ComputeClassSpec{
			Buffers: []ccapiv1.ComputeClassBuffer{{
				ProvisioningStrategy: activeCapacityStrategy,
				Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
			}},
		},
	}
	mockClient := &mockReconcileClient{cc: cc, statusWriter: &mockStatusWriter{}}
	r := &Reconciler{Client: mockClient}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-cc"}}
	wantRequeue := reconcile.Result{RequeueAfter: defaultInFlightTTL}

	// Pass 1: creates a buffer and requeues while its create is in flight.
	res, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Pass 1: Reconcile returned unexpected error: %v", err)
	}
	if len(mockClient.createdCBs) != 1 || res != wantRequeue {
		t.Fatalf("Pass 1: expected 1 create and %+v, got %d creates and %+v", wantRequeue, len(mockClient.createdCBs), res)
	}

	// Pass 2: the buffer vanished before any List showed it. Its in-flight record still
	// stands in for it, so nothing is created and the pass requeues again.
	res, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Pass 2: Reconcile returned unexpected error: %v", err)
	}
	if len(mockClient.createdCBs) != 1 || res != wantRequeue {
		t.Fatalf("Pass 2: expected no create and %+v, got %d creates and %+v", wantRequeue, len(mockClient.createdCBs), res)
	}

	// Pass 3: the record has expired, so the buffer is created again.
	cache := r.getOrCreateCache("test-cc")
	cache.createTTL = -1 * time.Second
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Pass 3: Reconcile returned unexpected error: %v", err)
	}
	if len(mockClient.createdCBs) != 2 {
		t.Fatalf("Pass 3: expected the expired create to be replaced, got %d total creates", len(mockClient.createdCBs))
	}

	// Pass 4: the new buffer is listed and nothing is in flight, so there's no requeue.
	cache.createTTL = defaultInFlightTTL
	mockClient.cbItems = []cbv1beta1.CapacityBuffer{*mockClient.createdCBs[1].DeepCopy()}
	res, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Pass 4: Reconcile returned unexpected error: %v", err)
	}
	if len(mockClient.createdCBs) != 2 || res != (reconcile.Result{}) {
		t.Errorf("Pass 4: expected no create and no requeue, got %d creates and %+v", len(mockClient.createdCBs), res)
	}
}

func TestEnqueueRequestsForOwner(t *testing.T) {
	ccOwner := metav1.OwnerReference{APIVersion: ccapiv1.SchemeGroupVersion.String(), Kind: kindComputeClass, Name: "test-cc", UID: "cc-uid-1"}
	otherOwner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "other", UID: "deployment-uid-1"}
	tests := []struct {
		name string
		obj  client.Object
		want []reconcile.Request
	}{
		{
			name: "CapacityBuffer owned by a ComputeClass maps to it",
			obj: &cbv1beta1.CapacityBuffer{ObjectMeta: metav1.ObjectMeta{
				Name: "cb-1", Namespace: NamespaceGkeManagedCCC, OwnerReferences: []metav1.OwnerReference{ccOwner},
			}},
			want: []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "test-cc"}}},
		},
		{
			name: "PodTemplate maps to its ComputeClass owner listed after another owner",
			obj: &corev1.PodTemplate{ObjectMeta: metav1.ObjectMeta{
				Name: "pt-basic-test-cc", Namespace: NamespaceGkeManagedCCC, OwnerReferences: []metav1.OwnerReference{otherOwner, ccOwner},
			}},
			want: []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "test-cc"}}},
		},
		{
			name: "object outside the managed namespace is ignored",
			obj: &cbv1beta1.CapacityBuffer{ObjectMeta: metav1.ObjectMeta{
				Name: "cb-1", Namespace: "default", OwnerReferences: []metav1.OwnerReference{ccOwner},
			}},
		},
		{
			name: "object owned by another kind is ignored",
			obj: &corev1.PodTemplate{ObjectMeta: metav1.ObjectMeta{
				Name: "pt-1", Namespace: NamespaceGkeManagedCCC, OwnerReferences: []metav1.OwnerReference{otherOwner},
			}},
		},
		{
			name: "object without owners is ignored",
			obj:  &cbv1beta1.CapacityBuffer{ObjectMeta: metav1.ObjectMeta{Name: "cb-1", Namespace: NamespaceGkeManagedCCC}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := enqueueRequestsForOwner(context.Background(), tc.obj); !slices.Equal(got, tc.want) {
				t.Errorf("enqueueRequestsForOwner() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconcile_RecentlyDeletedBufferIsNotACandidate(t *testing.T) {
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cc", UID: "cc-uid-1", Generation: 1},
	}
	buf := ccapiv1.ComputeClassBuffer{
		ProvisioningStrategy: activeCapacityStrategy,
		Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
	}
	cb1 := buildCapacityBuffer(cc, buf, "pt-basic-test-cc", "cb-1", "uid-cb-1")

	statusWriter := &mockStatusWriter{}
	mockClient := &mockReconcileClient{
		cc:           cc,
		cbItems:      []cbv1beta1.CapacityBuffer{*cb1},
		statusWriter: statusWriter,
	}
	r := &Reconciler{Client: mockClient}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-cc"}}

	// Pass 1: the ComputeClass has no buffers, so cb-1 is deleted.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Pass 1: Reconcile returned unexpected error: %v", err)
	}
	if !slices.Equal(mockClient.deletedCBs, []string{"cb-1"}) {
		t.Fatalf("Pass 1: expected cb-1 to be deleted, got %v", mockClient.deletedCBs)
	}

	// Pass 2: the same buffer is added back while a stale List still shows cb-1. cb-1 must not
	// be reused or deleted again; a new buffer is created instead.
	cc.Generation = 2
	cc.Spec.Buffers = []ccapiv1.ComputeClassBuffer{buf}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Pass 2: Reconcile returned unexpected error: %v", err)
	}
	if len(mockClient.createdCBs) != 1 {
		t.Fatalf("Pass 2: expected a new CapacityBuffer to be created, got %d", len(mockClient.createdCBs))
	}
	if len(mockClient.patchedCBs) != 0 || !slices.Equal(mockClient.deletedCBs, []string{"cb-1"}) {
		t.Errorf("Pass 2: expected cb-1 to be neither patched nor deleted again, got patched=%v deleted=%v", mockClient.patchedCBs, mockClient.deletedCBs)
	}

	var patchedCC ccapiv1.ComputeClass
	if err := json.Unmarshal(statusWriter.patchedBytes, &patchedCC); err != nil {
		t.Fatalf("Pass 2: failed to unmarshal status patch: %v", err)
	}
	if len(patchedCC.Status.ObservedBuffers) != 1 || patchedCC.Status.ObservedBuffers[0].Name != mockClient.createdCBs[0].Name {
		t.Errorf("Pass 2: expected ObservedBuffers to contain only the new buffer %s, got %+v", mockClient.createdCBs[0].Name, patchedCC.Status.ObservedBuffers)
	}
}

func TestReconcile_RequeuesWhenCreateRecordExpiresDuringPass(t *testing.T) {
	cc := &ccapiv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cc", UID: "cc-uid-1", Generation: 1},
		Spec: ccapiv1.ComputeClassSpec{
			Buffers: []ccapiv1.ComputeClassBuffer{{
				ProvisioningStrategy: activeCapacityStrategy,
				Limits:               corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
			}},
		},
	}
	statusWriter := &mockStatusWriter{}
	mockClient := &mockReconcileClient{cc: cc, statusWriter: statusWriter}
	r := &Reconciler{Client: mockClient}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "test-cc"}}

	// Pass 1: creates a buffer that no List shows afterwards.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Pass 1: Reconcile returned unexpected error: %v", err)
	}
	if len(mockClient.createdCBs) != 1 {
		t.Fatalf("Pass 1: expected 1 CapacityBuffer to be created, got %d", len(mockClient.createdCBs))
	}

	// Pass 2: the record is live when buffers are matched, so it blocks a replacement. The
	// status write then expires it, standing in for the clock passing the TTL mid-pass. The
	// pass must still requeue, or nothing replaces the buffer.
	cache := r.getOrCreateCache("test-cc")
	expired := false
	statusWriter.onPatch = func() {
		cache.createTTL = -1 * time.Second
		expired = true
	}
	res, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Pass 2: Reconcile returned unexpected error: %v", err)
	}
	if !expired || len(mockClient.createdCBs) != 1 {
		t.Fatalf("Pass 2: expected the record to expire after matching, with no replacement; got expired=%v and %d total creates", expired, len(mockClient.createdCBs))
	}
	if want := (reconcile.Result{RequeueAfter: defaultInFlightTTL}); res != want {
		t.Errorf("Pass 2: expected %+v, got %+v", want, res)
	}
}
