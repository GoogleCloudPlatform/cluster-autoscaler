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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/scaleupfailures"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
)

func TestAutoscalingStatusHistoryProcessor(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		name                     string
		unfinishedScaleUps       map[string][]ScaleUpDelta
		scaleUpFailures          map[string][]scaleupfailures.Record
		nodeGroupsRegistered     map[string]bool
		nodeGroupsAtTarget       map[string]bool
		cloudProviderTargetSizes map[string]int
		existingHistory          map[npc_status.CRDId]map[string]crd.ScalingEventsHistory
		wantUpdates              map[npc_status.CRDId]map[string]crd.ScalingEventsHistory
		expectedMsgCount         int
		expectNodeGroupRemoved   map[string]bool
	}{
		{
			name:             "no unfinished scaleups",
			expectedMsgCount: 0,
		},
		{
			name: "scale up not at target size yet",
			unfinishedScaleUps: map[string][]ScaleUpDelta{
				"nodepool-1": {
					{
						crdId:       npc_status.CRDId{CRDName: "default-crd-test", CRDLabel: "test-crd-label"},
						ruleIndex:   "0",
						addedNodes:  4,
						initialSize: 0,
						targetSize:  4,
					},
				},
			},
			nodeGroupsRegistered: map[string]bool{"nodepool-1": true},
			nodeGroupsAtTarget:   map[string]bool{"nodepool-1": false},
			wantUpdates:          nil,
			expectedMsgCount:     0,
		},
		{
			name: "single rule scale up finishing at target size without existing history",
			unfinishedScaleUps: map[string][]ScaleUpDelta{
				"nodepool-1": {
					{
						crdId:       npc_status.CRDId{CRDName: "default-crd-test", CRDLabel: "test-crd-label"},
						ruleIndex:   "0",
						addedNodes:  4,
						initialSize: 0,
						targetSize:  4,
					},
				},
			},
			nodeGroupsRegistered: map[string]bool{"nodepool-1": true},
			nodeGroupsAtTarget:   map[string]bool{"nodepool-1": true},
			existingHistory:      nil,
			wantUpdates: map[npc_status.CRDId]map[string]crd.ScalingEventsHistory{
				{CRDName: "default-crd-test", CRDLabel: "test-crd-label"}: {
					"0": { // ruleIndex 0
						ProvisionedNodesCount: 4,
						MeasuredAt:            metav1.NewTime(now),
						MeasuredSince:         metav1.NewTime(now),
					},
				},
			},
			expectedMsgCount:       1,
			expectNodeGroupRemoved: map[string]bool{"nodepool-1": true},
		},
		{
			name: "single rule scale up finishing with existing history",
			unfinishedScaleUps: map[string][]ScaleUpDelta{
				"nodepool-1": {
					{
						crdId:       npc_status.CRDId{CRDName: "default-crd-test", CRDLabel: "test-crd-label"},
						ruleIndex:   "0",
						addedNodes:  2,
						initialSize: 0,
						targetSize:  2,
					},
				},
			},
			nodeGroupsRegistered: map[string]bool{"nodepool-1": true},
			nodeGroupsAtTarget:   map[string]bool{"nodepool-1": true},
			existingHistory: map[npc_status.CRDId]map[string]crd.ScalingEventsHistory{
				{CRDName: "default-crd-test", CRDLabel: "test-crd-label"}: {
					"0": {
						ProvisionedNodesCount: 5,
						MeasuredAt:            metav1.NewTime(now.Add(-1 * time.Hour)),
						MeasuredSince:         metav1.NewTime(now.Add(-2 * time.Hour)),
					},
				},
			},
			wantUpdates: map[npc_status.CRDId]map[string]crd.ScalingEventsHistory{
				{CRDName: "default-crd-test", CRDLabel: "test-crd-label"}: {
					"0": {
						ProvisionedNodesCount: 7, // 5 + 2
						MeasuredAt:            metav1.NewTime(now),
						MeasuredSince:         metav1.NewTime(now.Add(-2 * time.Hour)),
					},
				},
			},
			expectedMsgCount:       1,
			expectNodeGroupRemoved: map[string]bool{"nodepool-1": true},
		},
		{
			name: "failed scale up",
			unfinishedScaleUps: map[string][]ScaleUpDelta{
				"nodepool-1": {
					{
						crdId:       npc_status.CRDId{CRDName: "default-crd-test", CRDLabel: "test-crd-label"},
						ruleIndex:   "0",
						addedNodes:  2,
						initialSize: 0,
						targetSize:  2,
					},
				},
			},
			nodeGroupsRegistered: map[string]bool{"nodepool-1": true},
			nodeGroupsAtTarget:   map[string]bool{"nodepool-1": true},
			scaleUpFailures: map[string][]scaleupfailures.Record{
				"nodepool-1": {{}},
			},
			cloudProviderTargetSizes: map[string]int{"nodepool-1": 1},
			wantUpdates: map[npc_status.CRDId]map[string]crd.ScalingEventsHistory{
				{CRDName: "default-crd-test", CRDLabel: "test-crd-label"}: {
					"0": {
						ProvisionedNodesCount: 1,
						MeasuredAt:            metav1.NewTime(now),
						MeasuredSince:         metav1.NewTime(now),
					},
				},
			},
			expectedMsgCount:       1,
			expectNodeGroupRemoved: map[string]bool{"nodepool-1": true},
		},
		{
			name: "multiple nodegroups in same rule",
			unfinishedScaleUps: map[string][]ScaleUpDelta{
				"nodepool-1": {
					{
						crdId:       npc_status.CRDId{CRDName: "multi-ng-crd", CRDLabel: "test-label"},
						ruleIndex:   "0",
						addedNodes:  2,
						initialSize: 0,
						targetSize:  2,
					},
				},
				"nodepool-2": {
					{
						crdId:       npc_status.CRDId{CRDName: "multi-ng-crd", CRDLabel: "test-label"},
						ruleIndex:   "0",
						addedNodes:  3,
						initialSize: 0,
						targetSize:  3,
					},
				},
			},
			nodeGroupsRegistered: map[string]bool{"nodepool-1": true, "nodepool-2": true},
			nodeGroupsAtTarget:   map[string]bool{"nodepool-1": true, "nodepool-2": true},
			wantUpdates: map[npc_status.CRDId]map[string]crd.ScalingEventsHistory{
				{CRDName: "multi-ng-crd", CRDLabel: "test-label"}: {
					"0": {
						ProvisionedNodesCount: 5, // 2 + 3
						MeasuredAt:            metav1.NewTime(now),
						MeasuredSince:         metav1.NewTime(now),
					},
				},
			},
			expectedMsgCount:       1, // Aggregated by rule
			expectNodeGroupRemoved: map[string]bool{"nodepool-1": true, "nodepool-2": true},
		},
		{
			name: "multiple rules in same CRD",
			unfinishedScaleUps: map[string][]ScaleUpDelta{
				"nodepool-1": {
					{
						crdId:       npc_status.CRDId{CRDName: "multi-rule-crd", CRDLabel: "test-label"},
						ruleIndex:   "0",
						addedNodes:  2,
						initialSize: 0,
						targetSize:  2,
					},
				},
				"nodepool-2": {
					{
						crdId:       npc_status.CRDId{CRDName: "multi-rule-crd", CRDLabel: "test-label"},
						ruleIndex:   "1",
						addedNodes:  3,
						initialSize: 0,
						targetSize:  3,
					},
				},
			},
			nodeGroupsRegistered: map[string]bool{"nodepool-1": true, "nodepool-2": true},
			nodeGroupsAtTarget:   map[string]bool{"nodepool-1": true, "nodepool-2": true},
			wantUpdates: map[npc_status.CRDId]map[string]crd.ScalingEventsHistory{
				{CRDName: "multi-rule-crd", CRDLabel: "test-label"}: {
					"0": {
						ProvisionedNodesCount: 2,
						MeasuredAt:            metav1.NewTime(now),
						MeasuredSince:         metav1.NewTime(now),
					},
					"1": {
						ProvisionedNodesCount: 3,
						MeasuredAt:            metav1.NewTime(now),
						MeasuredSince:         metav1.NewTime(now),
					},
				},
			},
			expectedMsgCount:       2, // Two separate rules
			expectNodeGroupRemoved: map[string]bool{"nodepool-1": true, "nodepool-2": true},
		},
		{
			name: "multiple CRDs",
			unfinishedScaleUps: map[string][]ScaleUpDelta{
				"nodepool-crd1": {
					{
						crdId:       npc_status.CRDId{CRDName: "crd-1", CRDLabel: "test-label"},
						ruleIndex:   "0",
						addedNodes:  2,
						initialSize: 0,
						targetSize:  2,
					},
				},
				"nodepool-crd2": {
					{
						crdId:       npc_status.CRDId{CRDName: "crd-2", CRDLabel: "test-label"},
						ruleIndex:   "0",
						addedNodes:  3,
						initialSize: 0,
						targetSize:  3,
					},
				},
			},
			nodeGroupsRegistered: map[string]bool{"nodepool-crd1": true, "nodepool-crd2": true},
			nodeGroupsAtTarget:   map[string]bool{"nodepool-crd1": true, "nodepool-crd2": true},
			wantUpdates: map[npc_status.CRDId]map[string]crd.ScalingEventsHistory{
				{CRDName: "crd-1", CRDLabel: "test-label"}: {
					"0": {
						ProvisionedNodesCount: 2,
						MeasuredAt:            metav1.NewTime(now),
						MeasuredSince:         metav1.NewTime(now),
					},
				},
				{CRDName: "crd-2", CRDLabel: "test-label"}: {
					"0": {
						ProvisionedNodesCount: 3,
						MeasuredAt:            metav1.NewTime(now),
						MeasuredSince:         metav1.NewTime(now),
					},
				},
			},
			expectedMsgCount:       2, // Two separate CRDs
			expectNodeGroupRemoved: map[string]bool{"nodepool-crd1": true, "nodepool-crd2": true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sharedData := NewScaleUpData()
			if tc.unfinishedScaleUps != nil {
				for nodeGroupId, deltas := range tc.unfinishedScaleUps {
					for _, delta := range deltas {
						sharedData.registerScaleUp(nodeGroupId, delta)
					}
				}
			}

			mockCsr := &MockClusterStateRegistry{
				scaleUpFailures:      tc.scaleUpFailures,
				nodeGroupsRegistered: tc.nodeGroupsRegistered,
				nodeGroupsAtTarget:   tc.nodeGroupsAtTarget,
			}

			updatesCh := make(chan npc_status.UpdateMessage, 10)

			processor := NewAutoscalingStatusHistoryProcessor(sharedData, updatesCh, nil)

			var nodeGroups []cloudprovider.NodeGroup
			for nodeGroupId, deltas := range tc.unfinishedScaleUps {
				targetSize := 1
				if len(deltas) > 0 {
					targetSize = deltas[0].targetSize
				}
				// Use specific mock target size if provided (e.g., for failure simulation)
				if val, ok := tc.cloudProviderTargetSizes[nodeGroupId]; ok {
					targetSize = val
				}
				nodeGroups = append(nodeGroups, &mockNodeGroupForTargetSize{id: nodeGroupId, targetSize: targetSize})
			}

			mockProvider := &mockCloudProviderForTargetSize{nodeGroups: nodeGroups}
			ctx := &context.AutoscalingContext{CloudProvider: mockProvider}

			processor.process(ctx, mockCsr, now)

			// Track current state of each CRD's rules
			currentStatusState := make(map[npc_status.CRDId]map[string]crd.ScalingEventsHistory)
			for crdId, rules := range tc.existingHistory {
				currentStatusState[crdId] = make(map[string]crd.ScalingEventsHistory)
				for ridx, h := range rules {
					currentStatusState[crdId][ridx] = h
				}
			}

			msgCount := 0
			timeout := time.After(1 * time.Second)
			for i := 0; i < tc.expectedMsgCount; i++ {
				select {
				case msg := <-updatesCh:
					msgCount++
					mockStatus := new(crd.MockCRDStatus)

					crdId := msg.Id
					if _, ok := currentStatusState[crdId]; !ok {
						currentStatusState[crdId] = make(map[string]crd.ScalingEventsHistory)
					}

					// Set up mock to return current state for all rules
					for ridx, h := range currentStatusState[crdId] {
						hCopy := h // Create a copy for the closure
						mockStatus.On("GetRuleScalingHistory", ridx).Return(&hCopy).Maybe()
					}
					mockStatus.On("GetRuleScalingHistory", mock.Anything).Return((*crd.ScalingEventsHistory)(nil)).Maybe()

					mockStatus.On("GetRuleConditions", mock.Anything).Return(nil).Maybe()
					mockStatus.On("UpdateRuleConditions", mock.Anything, mock.Anything).Return().Maybe()

					// Set up mock to capture updates
					mockStatus.On("UpdateRuleScalingHistory", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
						ridx := args.String(0)
						h := args.Get(1).(crd.ScalingEventsHistory)
						currentStatusState[crdId][ridx] = h
					}).Return(nil)

					msg.Mutate(mockStatus)
				case <-timeout:
					t.Errorf("Timed out waiting for update message")
				}
			}
			assert.Equal(t, tc.expectedMsgCount, msgCount)

			// After all messages, verify final state matches wantUpdates
			for crdId, expectedRules := range tc.wantUpdates {
				for ridx, expectedHistory := range expectedRules {
					actual, ok := currentStatusState[crdId][ridx]
					assert.True(t, ok, "Expected update for CRD %v rule %s missing", crdId, ridx)
					assert.Equal(t, expectedHistory.ProvisionedNodesCount, actual.ProvisionedNodesCount)
					assert.Equal(t, expectedHistory.MeasuredAt, actual.MeasuredAt)
					assert.Equal(t, expectedHistory.MeasuredSince, actual.MeasuredSince)
				}
			}

			unfinished := sharedData.getUnfinishedNodeGroups()
			for ngId, expectRemoved := range tc.expectNodeGroupRemoved {
				if expectRemoved {
					assert.Empty(t, unfinished[ngId])
				} else {
					assert.NotEmpty(t, unfinished[ngId])
				}
			}
		})
	}
}

// MockClusterStateRegistry implements historyClusterStateRegistry for testing.
type MockClusterStateRegistry struct {
	scaleUpFailures      map[string][]scaleupfailures.Record
	nodeGroupsRegistered map[string]bool
	nodeGroupsAtTarget   map[string]bool
}

func (m *MockClusterStateRegistry) IsNodeGroupRegistered(nodeGroupName string) bool {
	return m.nodeGroupsRegistered[nodeGroupName]
}

func (m *MockClusterStateRegistry) IsNodeGroupAtTargetSize(nodeGroupName string) bool {
	return m.nodeGroupsAtTarget[nodeGroupName]
}

func (m *MockClusterStateRegistry) GetScaleUpFailures() map[string][]scaleupfailures.Record {
	return m.scaleUpFailures
}

type mockCloudProviderForTargetSize struct {
	cloudprovider.CloudProvider
	nodeGroups []cloudprovider.NodeGroup
}

func (m *mockCloudProviderForTargetSize) NodeGroups() []cloudprovider.NodeGroup {
	return m.nodeGroups
}

type mockNodeGroupForTargetSize struct {
	cloudprovider.NodeGroup
	id         string
	targetSize int
}

func (m *mockNodeGroupForTargetSize) Id() string {
	return m.id
}

func (m *mockNodeGroupForTargetSize) TargetSize() (int, error) {
	return m.targetSize, nil
}

func TestAutoscalingStatusHistoryProcessor_Conditions(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultTestCrd := "default-crd-test"
	now := time.Now()

	crdId := npc_status.CRDId{CRDName: defaultTestCrd, CRDLabel: testCrdLabel}

	testCases := []struct {
		name               string
		existingConditions []metav1.Condition
		expectedConditions []metav1.Condition
	}{
		{
			name: "clears NodeProvisioningInProgress condition",
			existingConditions: []metav1.Condition{
				{
					Type:    ConditionTypeNodeProvisioningInProgress,
					Status:  metav1.ConditionTrue,
					Reason:  "PodPending",
					Message: "Scale up triggered",
				},
				{
					Type:    "OtherCondition",
					Status:  metav1.ConditionTrue,
					Reason:  "OtherReason",
					Message: "Other message",
				},
			},
			expectedConditions: []metav1.Condition{
				{
					Type:    "OtherCondition",
					Status:  metav1.ConditionTrue,
					Reason:  "OtherReason",
					Message: "Other message",
				},
			},
		},
		{
			name:               "no existing conditions",
			existingConditions: nil,
			expectedConditions: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sharedData := NewScaleUpData()
			// Register an unfinished scale-up so it tries to finish it
			sharedData.registerScaleUp("nodepool-1", ScaleUpDelta{
				crdId:       crdId,
				ruleIndex:   "0",
				addedNodes:  2,
				initialSize: 0,
				targetSize:  2,
			})

			updatesCh := make(chan npc_status.UpdateMessage, 10)
			processor := NewAutoscalingStatusHistoryProcessor(sharedData, updatesCh, nil)

			mockCsr := &MockClusterStateRegistry{
				nodeGroupsAtTarget: map[string]bool{"nodepool-1": true}, // Trigger finish
			}

			// Mock CloudProvider to return target size
			nodeGroups := []cloudprovider.NodeGroup{&mockNodeGroupForTargetSize{id: "nodepool-1", targetSize: 2}}
			mockProvider := &mockCloudProviderForTargetSize{nodeGroups: nodeGroups}
			ctx := &context.AutoscalingContext{CloudProvider: mockProvider}

			processor.process(ctx, mockCsr, now)

			select {
			case msg := <-updatesCh:
				mockStatus := new(crd.MockCRDStatus)
				mockStatus.On("GetRuleScalingHistory", "0").Return((*crd.ScalingEventsHistory)(nil)).Maybe()
				mockStatus.On("UpdateRuleScalingHistory", "0", mock.Anything).Return(nil).Maybe()

				// Provide existing conditions
				mockStatus.On("GetRuleConditions", "0").Return(tc.existingConditions)

				mockStatus.On("UpdateRuleConditions", "0", mock.Anything).Run(func(args mock.Arguments) {
					updatedConditions := args.Get(1).([]metav1.Condition)
					assert.Equal(t, tc.expectedConditions, updatedConditions)
				})

				msg.Mutate(mockStatus)
				mockStatus.AssertExpectations(t)
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Timed out waiting for update message")
			}
		})
	}
}
