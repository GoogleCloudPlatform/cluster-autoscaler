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
	"strings"
	"testing"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	cc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	npc_status "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
)

func TestScaleUpStatusHistoryProcessor(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultTestCrd := "default-crd-test"
	testCrdType := "TEST"

	testCases := []struct {
		name            string
		crds            []crd.CRD
		scaleUpStatus   *status.ScaleUpStatus
		initialScaleUps map[string][]ScaleUpDelta
		expectDeltas    map[string]map[string]int // Map of nodegroup -> CrdName -> AddedNodes
	}{
		{
			name:         "no scaleup info",
			expectDeltas: nil,
		},
		{
			name: "single rule scale up registers to shared data",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}),
					crd.WithScaleUpAnyway()),
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetGceRefName("nodepool-1-mig").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels: map[string]string{testCrdLabel: defaultCrdName()},
							}).Build(),
						CurrentSize: 3,
						NewSize:     7,
						MaxSize:     10,
					},
				},
			},
			expectDeltas: map[string]map[string]int{
				"nodepool-1": {
					defaultTestCrd: 4,
				},
			},
		},
		{
			name: "skips async nodegroups",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2-async"})),
					}),
					crd.WithScaleUpAnyway()),
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					func() nodegroupset.ScaleUpInfo {
						mockManager := &gke.GkeManagerMock{MockIsUpcoming: true}
						mockManager.On("IsUpcoming", mock.Anything).Return(true)
						mockManager.On("GetMigTemplateNodeInfo", mock.Anything).Return(nil, nil)
						return nodegroupset.ScaleUpInfo{
							Group: gke.NewTestGkeMigBuilder().
								SetNodePoolName("nodepool-2-async").
								SetGceRefName("nodepool-2-async-mig-1").
								SetGkeManager(mockManager).
								SetExist(false).
								SetSpec(&gkeclient.NodePoolSpec{
									Labels: map[string]string{testCrdLabel: defaultCrdName()},
								}).Build(),
							CurrentSize: 10,
							NewSize:     15,
							MaxSize:     20,
						}
					}(),
				},
			},
			expectDeltas: nil,
		},
		{
			name: "accumulates deltas for same nodegroup",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}),
					crd.WithScaleUpAnyway()),
			},
			initialScaleUps: map[string][]ScaleUpDelta{
				"nodepool-1": {
					{
						crdId:       npc_status.CRDId{CRDName: defaultTestCrd, CRDLabel: testCrdLabel},
						ruleIndex:   "0",
						addedNodes:  2,
						initialSize: 0,
						targetSize:  2,
					},
				},
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetGceRefName("nodepool-1-mig").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels: map[string]string{testCrdLabel: defaultCrdName()},
							}).Build(),
						CurrentSize: 2,
						NewSize:     5,
						MaxSize:     10,
					},
				},
			},
			expectDeltas: map[string]map[string]int{
				"nodepool-1": {
					defaultTestCrd: 3, // New delta is 3 (5-2)
				},
			},
		},
		{
			name: "skips scale up that doesn't increase size",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}),
					crd.WithScaleUpAnyway()),
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetGceRefName("nodepool-1-mig").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels: map[string]string{testCrdLabel: defaultCrdName()},
							}).Build(),
						CurrentSize: 5,
						NewSize:     5, // Same size
						MaxSize:     10,
					},
				},
			},
			expectDeltas: nil,
		},
		{
			name: "handles initial data for different nodegroup",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}),
					crd.WithScaleUpAnyway()),
			},
			initialScaleUps: map[string][]ScaleUpDelta{
				"nodepool-1": {
					{
						crdId:       npc_status.CRDId{CRDName: defaultTestCrd, CRDLabel: testCrdLabel},
						ruleIndex:   "0",
						addedNodes:  2,
						initialSize: 0,
						targetSize:  2,
					},
				},
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetGceRefName("nodepool-2-mig").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels: map[string]string{testCrdLabel: defaultCrdName()},
							}).Build(),
						CurrentSize: 0,
						NewSize:     3,
						MaxSize:     10,
					},
				},
			},
			expectDeltas: map[string]map[string]int{
				"nodepool-1": {
					defaultTestCrd: 2, // Preserved
				},
				"nodepool-2": {
					defaultTestCrd: 3, // New
				},
			},
		},
		{
			name: "accumulates multiple deltas for same nodegroup",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}),
					crd.WithScaleUpAnyway()),
			},
			initialScaleUps: map[string][]ScaleUpDelta{
				"nodepool-1": {
					{
						crdId:       npc_status.CRDId{CRDName: defaultTestCrd, CRDLabel: testCrdLabel},
						ruleIndex:   "0",
						addedNodes:  2,
						initialSize: 0,
						targetSize:  2,
					},
					{
						crdId:       npc_status.CRDId{CRDName: defaultTestCrd, CRDLabel: testCrdLabel},
						ruleIndex:   "0",
						addedNodes:  3,
						initialSize: 2,
						targetSize:  5,
					},
				},
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetGceRefName("nodepool-1-mig").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels: map[string]string{testCrdLabel: defaultCrdName()},
							}).Build(),
						CurrentSize: 5,
						NewSize:     9,
						MaxSize:     10,
					},
				},
			},
			expectDeltas: map[string]map[string]int{
				"nodepool-1": {
					defaultTestCrd: 4, // New delta is 4 (9-5)
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister(tc.crds)
			mockLister.SetCrdLabel(testCrdLabel)
			mockLister.SetDefaultCrdName(defaultTestCrd)

			mockProvider := NewMockCloudProvider()
			mockProvider.On("IsAutopilotEnabled").Return(false)

			sharedData := NewScaleUpData()
			if tc.initialScaleUps != nil {
				for key, deltas := range tc.initialScaleUps {
					actualKey := key
					for _, info := range tc.scaleUpStatus.ScaleUpInfos {
						if strings.Contains(info.Group.Id(), key) {
							actualKey = info.Group.Id()
							break
						}
					}
					for _, delta := range deltas {
						sharedData.registerScaleUp(actualKey, delta)
					}
				}
			}
			processor := NewScaleUpStatusHistoryProcessor(mockLister, mockProvider, sharedData, nil, nil)

			processor.Process(nil, tc.scaleUpStatus)

			unfinished := sharedData.getUnfinishedNodeGroups()

			if tc.expectDeltas == nil {
				assert.Empty(t, unfinished)
			} else {
				for expectedNg, expectedCrdNodes := range tc.expectDeltas {
					// We need to find the actual ID generated by the mock builder for expectedNg
					var actualGroupId string
					for id := range unfinished {
						if strings.Contains(id, expectedNg) {
							actualGroupId = id
							break
						}
					}

					info, found := unfinished[actualGroupId]
					assert.True(t, found, "Expected to find deltas for group %s (expected %s)", actualGroupId, expectedNg)

					for expectedCrdName, expectedNodes := range expectedCrdNodes {
						foundCrd := false
						foundDelta := false
						if info != nil {
							for _, d := range info.deltas {
								if d.crdId.CRDName == expectedCrdName {
									foundCrd = true
									if d.addedNodes == expectedNodes {
										foundDelta = true
									}
								}
							}
						}
						assert.True(t, foundCrd, "Expected crd name %s in deltas", expectedCrdName)
						assert.True(t, foundDelta, "Expected delta %d for crd %s in deltas", expectedNodes, expectedCrdName)
					}
				}
			}
		})
	}
}

func defaultCrdName() string {
	return "default-crd-test"
}

// Mock CloudProvider
type mockCloudProvider struct {
	mock.Mock
}

func (m *mockCloudProvider) IsAutopilotEnabled() bool {
	args := m.Called()
	return args.Bool(0)
}
func NewMockCloudProvider() *mockCloudProvider {
	return &mockCloudProvider{}
}

func TestScaleUpStatusHistoryProcessor_Conditions(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultTestCrd := "default-crd-test"
	testCrdType := "TEST"
	now := time.Now()

	crd1 := crd.NewTestCrd(crd.WithLabel(testCrdLabel),
		crd.WithName(defaultTestCrd),
		crd.WithCrdType(testCrdType),
		crd.WithRules([]rules.Rule{
			rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1", "nodepool-2", "nodepool-3"})),
		}),
		crd.WithScaleUpAnyway())

	mig1 := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-1").
		SetGceRefZone("us-central1-a").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "n1-standard-4",
			Labels:      map[string]string{testCrdLabel: defaultTestCrd},
		}).Build()

	mig2 := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-2").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "a2-highgpu-1g",
			Labels:      map[string]string{testCrdLabel: defaultTestCrd},
			Accelerators: []*gke_api_beta.AcceleratorConfig{
				{AcceleratorType: "nvidia-tesla-a100", AcceleratorCount: 1},
			},
		}).Build()

	mig3 := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-3").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "ct5lp-hightpu-4t",
			Labels:      map[string]string{testCrdLabel: defaultTestCrd},
			TpuType:     "tpu-v5-lite-podslice",
			TpuTopology: "2x2x1",
		}).Build()

	mig4 := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-1").
		SetGceRefZone("us-central1-b").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "n1-standard-4",
			Labels:      map[string]string{testCrdLabel: defaultTestCrd},
		}).Build()

	mig5 := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-1").
		SetGceRefZone("us-central1-c").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "n1-standard-4",
			Labels:      map[string]string{testCrdLabel: defaultTestCrd},
		}).Build()

	scaleUpStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpSuccessful,
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group:       mig1,
				CurrentSize: 1,
				NewSize:     3,
				MaxSize:     10,
			},
			{
				Group:       mig2,
				CurrentSize: 1,
				NewSize:     2,
				MaxSize:     10,
			},
			{
				Group:       mig3,
				CurrentSize: 0,
				NewSize:     4,
				MaxSize:     10,
			},
			{
				Group:       mig4,
				CurrentSize: 0,
				NewSize:     1,
				MaxSize:     10,
			},
			{
				Group:       mig5,
				CurrentSize: 0,
				NewSize:     1,
				MaxSize:     10,
			},
		},
	}

	expectedMessage := "NodeProvisioning associated with this priority triggered due to pending pods. 9 new nodes will be added with config: {NodePool: nodepool-1, MachineType: n1-standard-4, Zones: us-central1-a, us-central1-b, us-central1-c}, {NodePool: nodepool-2, MachineType: a2-highgpu-1g, GPU: type: nvidia-tesla-a100, count: 1, Zones: }, {NodePool: nodepool-3, MachineType: ct5lp-hightpu-4t, TPU: type: tpu-v5-lite-podslice, topology: 2x2x1, Zones: }"

	testCases := []struct {
		name               string
		existingConditions []metav1.Condition
		expectedConditions []metav1.Condition
	}{
		{
			name:               "no existing conditions",
			existingConditions: nil,
			expectedConditions: []metav1.Condition{
				{
					Type:               ConditionTypeNodeProvisioningInProgress,
					Status:             metav1.ConditionTrue,
					Reason:             ConditionReasonPodPending,
					Message:            expectedMessage,
					LastTransitionTime: metav1.NewTime(now),
				},
			},
		},
		{
			name: "existing condition to deduplicate",
			existingConditions: []metav1.Condition{
				{
					Type:               ConditionTypeNodeProvisioningInProgress,
					Status:             metav1.ConditionTrue,
					Reason:             ConditionReasonPodPending,
					Message:            "Old message",
					LastTransitionTime: metav1.NewTime(now.Add(-1 * time.Hour)),
				},
			},
			expectedConditions: []metav1.Condition{
				{
					Type:               ConditionTypeNodeProvisioningInProgress,
					Status:             metav1.ConditionTrue,
					Reason:             ConditionReasonPodPending,
					Message:            expectedMessage,
					LastTransitionTime: metav1.NewTime(now),
				},
			},
		},
		{
			name: "other existing conditions are preserved",
			existingConditions: []metav1.Condition{
				{
					Type:    "OtherCondition",
					Status:  metav1.ConditionTrue,
					Reason:  "OtherReason",
					Message: "Other message",
				},
				{
					Type:               ConditionTypeNodeProvisioningInProgress,
					Status:             metav1.ConditionTrue,
					Reason:             ConditionReasonPodPending,
					Message:            "Old message",
					LastTransitionTime: metav1.NewTime(now.Add(-1 * time.Hour)),
				},
			},
			expectedConditions: []metav1.Condition{
				{
					Type:    "OtherCondition",
					Status:  metav1.ConditionTrue,
					Reason:  "OtherReason",
					Message: "Other message",
				},
				{
					Type:               ConditionTypeNodeProvisioningInProgress,
					Status:             metav1.ConditionTrue,
					Reason:             ConditionReasonPodPending,
					Message:            expectedMessage,
					LastTransitionTime: metav1.NewTime(now),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister([]crd.CRD{crd1})
			mockLister.SetCrdLabel(testCrdLabel)
			mockLister.SetDefaultCrdName(defaultTestCrd)

			mockProvider := NewMockCloudProvider()
			mockProvider.On("IsAutopilotEnabled").Return(false)

			updatesCh := make(chan npc_status.UpdateMessage, 10)
			sharedData := NewScaleUpData()
			processor := NewScaleUpStatusHistoryProcessor(mockLister, mockProvider, sharedData, updatesCh, nil)
			processor.now = func() time.Time { return now }

			processor.Process(nil, scaleUpStatus)

			select {
			case msg := <-updatesCh:
				mockStatus := new(crd.MockCRDStatus)

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

type mockMinCapacityObserver struct {
	mock.Mock
}

func (m *mockMinCapacityObserver) Refresh(now time.Time) {
	m.Called(now)
}

func (m *mockMinCapacityObserver) OnScaleUpDecision(ccName string, ruleIdx int, now time.Time) {
	m.Called(ccName, ruleIdx, now)
}

func (m *mockMinCapacityObserver) OnProvisioningComplete(cccName string, ruleIdx int, now time.Time) {
	m.Called(cccName, ruleIdx, now)
}

func (m *mockMinCapacityObserver) OnProvisioningError(cccName string, errType string, unhelpable bool, now time.Time) {
	m.Called(cccName, errType, unhelpable, now)
}

func (m *mockMinCapacityObserver) CheckLongUnprovisioned(now time.Time) {
	m.Called(now)
}

func (m *mockMinCapacityObserver) OnShortfallDetected(ccName string, ruleIdx int, now time.Time) {
	m.Called(ccName, ruleIdx, now)
}

func (m *mockMinCapacityObserver) OnComputeClassAdded(cc *cccv1.ComputeClass, now time.Time) {
	m.Called(cc, now)
}

func (m *mockMinCapacityObserver) OnComputeClassUpdated(oldCC, newCC *cccv1.ComputeClass, now time.Time) {
	m.Called(oldCC, newCC, now)
}

func (m *mockMinCapacityObserver) OnComputeClassDeleted(name string) {
	m.Called(name)
}

type scaleUpDecisionCall struct {
	ccName  string
	ruleIdx int
}

func intPtr(i int) *int {
	return &i
}

func TestScaleUpStatusHistoryProcessor_ProcessMinCapacity(t *testing.T) {
	tests := []struct {
		name          string
		scaleUpStatus *status.ScaleUpStatus
		wantDecisions []scaleUpDecisionCall
	}{
		{
			name: "successful scale up with priority fake pod",
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetGceRefName("nodepool-1-mig").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels: map[string]string{labels.ComputeClassLabel: "test-cc"},
							}).
							Build(),
						CurrentSize: 0,
						NewSize:     2,
					},
				},
				PodsTriggeredScaleUp: []*apiv1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								cc_processors.MinCapacityFakePodAnnotation: "true",
							},
						},
						Spec: apiv1.PodSpec{
							NodeSelector: map[string]string{
								labels.ComputeClassLabel:            "test-cc",
								labels.ComputeClassPriorityIdxLabel: "0",
							},
						},
					},
				},
			},
			wantDecisions: []scaleUpDecisionCall{
				{ccName: "test-cc", ruleIdx: 0},
			},
		},
		{
			name: "successful scale up with non-priority fake pod",
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetGceRefName("nodepool-2-mig").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels: map[string]string{labels.ComputeClassLabel: "test-cc"},
							}).
							Build(),
						CurrentSize: 0,
						NewSize:     2,
					},
				},
				PodsTriggeredScaleUp: []*apiv1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								cc_processors.MinCapacityFakePodAnnotation: "true",
							},
						},
						Spec: apiv1.PodSpec{
							NodeSelector: map[string]string{
								labels.ComputeClassLabel: "test-cc",
							},
						},
					},
				},
			},
			wantDecisions: []scaleUpDecisionCall{
				{ccName: "test-cc", ruleIdx: -1},
			},
		},
		{
			name: "successful scale up with ScaleUpAnyway fallback fake pod (-1)",
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetGceRefName("nodepool-2-mig").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels: map[string]string{labels.ComputeClassLabel: "test-cc"},
							}).
							Build(),
						CurrentSize: 0,
						NewSize:     2,
					},
				},
				PodsTriggeredScaleUp: []*apiv1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								cc_processors.MinCapacityFakePodAnnotation: "true",
							},
						},
						Spec: apiv1.PodSpec{
							NodeSelector: map[string]string{
								labels.ComputeClassLabel:            "test-cc",
								labels.ComputeClassPriorityIdxLabel: "-1",
							},
						},
					},
				},
			},
			wantDecisions: []scaleUpDecisionCall{
				{ccName: "test-cc", ruleIdx: -1},
			},
		},
		{
			name: "successful scale up without fake pod",
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetGceRefName("nodepool-1-mig").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels: map[string]string{labels.ComputeClassLabel: "test-cc"},
							}).
							Build(),
						CurrentSize: 0,
						NewSize:     2,
					},
				},
				PodsTriggeredScaleUp: []*apiv1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{},
						Spec: apiv1.PodSpec{
							NodeSelector: map[string]string{
								labels.ComputeClassLabel: "test-cc",
							},
						},
					},
				},
			},
			wantDecisions: []scaleUpDecisionCall{
				{ccName: "test-cc", ruleIdx: 0},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockObserver := &mockMinCapacityObserver{}
			sharedData := NewScaleUpData()

			// Create valid CRD and Lister to satisfy getRuleIndex
			crd1 := crd.NewTestCrd(
				crd.WithLabel(labels.ComputeClassLabel),
				crd.WithName("test-cc"),
				crd.WithCrdType("CCC"),
				crd.WithRules([]rules.Rule{rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"}), rules.WithTargetNodeCountRule(intPtr(5)))}),
				crd.WithTargetNodeCount(intPtr(5)),
				crd.WithScaleUpAnyway(),
			)
			mockLister := lister.NewMockCrdLister([]crd.CRD{crd1})
			mockLister.SetCrdLabel(labels.ComputeClassLabel)

			mockProvider := NewMockCloudProvider()
			mockProvider.On("IsAutopilotEnabled").Return(false)

			processor := NewScaleUpStatusHistoryProcessor(mockLister, mockProvider, sharedData, nil, mockObserver)

			for _, want := range tc.wantDecisions {
				mockObserver.On("OnScaleUpDecision", want.ccName, want.ruleIdx, mock.AnythingOfType("time.Time")).Return()
			}

			processor.Process(nil, tc.scaleUpStatus)

			mockObserver.AssertExpectations(t)
			if len(tc.wantDecisions) == 0 {
				mockObserver.AssertNotCalled(t, "OnScaleUpDecision", mock.Anything, mock.Anything, mock.Anything)
			}

			// Verify isMinCapacity on Deltas
			unfinished := sharedData.getUnfinishedNodeGroups()
			assert.NotEmpty(t, unfinished, "ScaleUpData should have recorded unfinished node groups")
			for _, info := range unfinished {
				for _, delta := range info.deltas {
					if len(tc.wantDecisions) > 0 {
						assert.True(t, delta.isMinCapacity, "expected isMinCapacity to be true")
					} else {
						assert.False(t, delta.isMinCapacity, "expected isMinCapacity to be false")
					}
				}
			}
		})
	}
}
