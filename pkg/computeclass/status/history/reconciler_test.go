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

package history

import (
	"fmt"
	"testing"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

type mockInformer struct {
	cache.SharedIndexInformer
	handler cache.ResourceEventHandlerDetailedFuncs
}

func (m *mockInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	detailedHandler, ok := handler.(cache.ResourceEventHandlerDetailedFuncs)
	if !ok {
		return nil, fmt.Errorf("expected ResourceEventHandlerDetailedFuncs")
	}
	m.handler = detailedHandler
	return nil, nil
}

func TestSetupHistoryResetObserver_AddFunc(t *testing.T) {
	e2 := "e2"
	cc := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ccc"},
		Spec: cccv1.ComputeClassSpec{
			Priorities: []cccv1.Priority{{MachineFamily: &e2}},
		},
	}

	testCases := []struct {
		name            string
		obj             interface{}
		isInInitialList bool
		expectUpdate    bool
	}{
		{
			name:            "not in initial list",
			obj:             cc,
			isInInitialList: false,
			expectUpdate:    false,
		},
		{
			name:            "in initial list",
			obj:             cc,
			isInInitialList: true,
			expectUpdate:    true,
		},
		{
			name:            "invalid object",
			obj:             "not-a-cc",
			isInInitialList: true,
			expectUpdate:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updatesCh := make(chan npc_status.UpdateMessage, 1)
			mInformer := &mockInformer{}
			mockManager := experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true},
				map[string]string{},
			)

			SetupHistoryResetObserver(mInformer, updatesCh, mockManager)
			assert.NotNil(t, mInformer.handler.AddFunc, "AddFunc should be registered")

			mInformer.handler.OnAdd(tc.obj, tc.isInInitialList)

			if tc.expectUpdate {
				assert.Len(t, updatesCh, 1, "should send update")
				msg := <-updatesCh
				assert.Equal(t, "test-ccc", msg.Id.CRDName)
				assert.Equal(t, gkelabels.ComputeClassLabel, msg.Id.CRDLabel)

				mockStatus := new(crd.MockCRDStatus)
				mockStatus.On("ResetAllScalingHistories").Return()

				msg.Mutate(mockStatus)
				mockStatus.AssertExpectations(t)
			} else {
				assert.Len(t, updatesCh, 0, "should not send update")
			}
		})
	}
}

func TestSetupHistoryResetObserver_UpdateFunc(t *testing.T) {
	e2 := "e2"
	n2 := "n2"

	oldCC := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ccc"},
		Spec: cccv1.ComputeClassSpec{
			Priorities: []cccv1.Priority{{MachineFamily: &e2}},
		},
	}

	newCC := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ccc"},
		Spec: cccv1.ComputeClassSpec{
			Priorities: []cccv1.Priority{{MachineFamily: &n2}},
		},
	}

	sameCC := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ccc"},
		Spec: cccv1.ComputeClassSpec{
			Priorities: []cccv1.Priority{{MachineFamily: &e2}},
		},
	}

	testCases := []struct {
		name         string
		oldObj       interface{}
		newObj       interface{}
		expectUpdate bool
	}{
		{
			name:         "same priorities",
			oldObj:       oldCC,
			newObj:       sameCC,
			expectUpdate: false,
		},
		{
			name:         "different priorities",
			oldObj:       oldCC,
			newObj:       newCC,
			expectUpdate: true,
		},
		{
			name:         "invalid old object",
			oldObj:       "not-a-cc",
			newObj:       newCC,
			expectUpdate: false,
		},
		{
			name:         "invalid new object",
			oldObj:       oldCC,
			newObj:       "not-a-cc",
			expectUpdate: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updatesCh := make(chan npc_status.UpdateMessage, 1)
			mInformer := &mockInformer{}
			mockManager := experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true},
				map[string]string{},
			)

			SetupHistoryResetObserver(mInformer, updatesCh, mockManager)
			assert.NotNil(t, mInformer.handler.UpdateFunc, "UpdateFunc should be registered")

			mInformer.handler.OnUpdate(tc.oldObj, tc.newObj)

			if tc.expectUpdate {
				assert.Len(t, updatesCh, 1, "should send update")
				msg := <-updatesCh
				assert.Equal(t, "test-ccc", msg.Id.CRDName)
				assert.Equal(t, gkelabels.ComputeClassLabel, msg.Id.CRDLabel)

				mockStatus := new(crd.MockCRDStatus)
				mockStatus.On("ResetAllScalingHistories").Return()

				msg.Mutate(mockStatus)
				mockStatus.AssertExpectations(t)
			} else {
				assert.Len(t, updatesCh, 0, "should not send update")
			}
		})
	}
}

func TestHistoryResetObserver_HashPriorities(t *testing.T) {
	nodePools1 := []string{"np1", "np2"}
	nodePools2 := []string{"np2", "np1"} // different order

	e2 := "e2"
	n2 := "n2"

	priorities1 := []cccv1.Priority{
		{MachineFamily: &e2},
		{Nodepools: nodePools1},
	}

	priorities2 := []cccv1.Priority{
		{MachineFamily: &e2},
		{Nodepools: nodePools1},
	}

	priorities3 := []cccv1.Priority{
		{MachineFamily: &n2}, // different family
		{Nodepools: nodePools1},
	}

	priorities4 := []cccv1.Priority{
		{MachineFamily: &e2},
		{Nodepools: nodePools2}, // different nodepool order
	}

	testCases := []struct {
		name        string
		prioritiesA []cccv1.Priority
		prioritiesB []cccv1.Priority
		expectEqual bool
	}{
		{
			name:        "identical priorities",
			prioritiesA: priorities1,
			prioritiesB: priorities2,
			expectEqual: true,
		},
		{
			name:        "different priorities",
			prioritiesA: priorities1,
			prioritiesB: priorities3,
			expectEqual: false,
		},
		{
			name:        "different nodepool order",
			prioritiesA: priorities1,
			prioritiesB: priorities4,
			expectEqual: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hashA, errA := hashPriorities(tc.prioritiesA)
			assert.NoError(t, errA)

			hashB, errB := hashPriorities(tc.prioritiesB)
			assert.NoError(t, errB)

			if tc.expectEqual {
				assert.Equal(t, hashA, hashB, "should have same hash")
			} else {
				assert.NotEqual(t, hashA, hashB, "should have different hash")
			}
		})
	}
}

func TestSetupHistoryResetObserver_UnownedSkipped(t *testing.T) {
	mInformer := &mockInformer{}
	updatesCh := make(chan npc_status.UpdateMessage, 10)
	mockManager := experiments.NewMockManagerWithOptions(
		version.Version{},
		map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true},
		map[string]string{},
	)

	SetupHistoryResetObserver(mInformer, updatesCh, mockManager, "karpenter-ca")

	// Unannotated CC belongs to default CA, so karpenter-ca should ignore it.
	unownedCC := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-cc",
		},
	}
	mInformer.handler.AddFunc(unownedCC, true)
	assert.Empty(t, updatesCh, "unowned ComputeClass should not trigger history reset message")

	// Owned CC annotated with karpenter-ca should trigger update.
	ownedCC := &cccv1.ComputeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "karpenter-cc",
			Annotations: map[string]string{
				"autoscaling.gke.io/autoscaler-name": "karpenter-ca",
			},
		},
	}
	mInformer.handler.AddFunc(ownedCC, true)
	assert.Len(t, updatesCh, 1, "owned ComputeClass should trigger history reset message")
}

func TestSetupHistoryResetObserver_Ownership(t *testing.T) {
	standardNames := []string{options.UnannotatedAutoscalerName, options.StandardAutoscalerName}
	computeClass := func(owner, machineFamily string) *cccv1.ComputeClass {
		cc := &cccv1.ComputeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "cc"},
			Spec:       cccv1.ComputeClassSpec{Priorities: []cccv1.Priority{{MachineFamily: &machineFamily}}},
		}
		if owner != "" {
			cc.Annotations = map[string]string{options.AutoscalerNameAnnotation: owner}
		}
		return cc
	}
	mockManager := experiments.NewMockManagerWithOptions(
		version.Version{},
		map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true},
		map[string]string{},
	)

	// Each case observes the ComputeClass during the initial informer sync and
	// then a change of its priorities, each of which resets the scaling history
	// of an owned ComputeClass.
	testCases := []struct {
		name            string
		autoscalerNames []string
		owner           string
		wantMessages    int
	}{
		{
			name:            "unowned ComputeClass doesn't reset history",
			autoscalerNames: standardNames,
			owner:           options.SolverAutoscalerName,
			wantMessages:    0,
		},
		{
			name:            "owned ComputeClass resets history",
			autoscalerNames: standardNames,
			wantMessages:    2,
		},
		{
			name:         "all ComputeClasses reset history when autoscaler names are not set",
			owner:        options.SolverAutoscalerName,
			wantMessages: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mInformer := &mockInformer{}
			updatesCh := make(chan npc_status.UpdateMessage, 10)
			SetupHistoryResetObserver(mInformer, updatesCh, mockManager, tc.autoscalerNames...)

			mInformer.handler.AddFunc(computeClass(tc.owner, "e2"), true)
			mInformer.handler.UpdateFunc(computeClass(tc.owner, "e2"), computeClass(tc.owner, "n2"))

			assert.Len(t, updatesCh, tc.wantMessages)
		})
	}
}
