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

package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestMinCapacityController_Reconcile(t *testing.T) {
	tests := []struct {
		name           string
		cc             crd.CRD
		nodes          []*apiv1.Node
		matchedRuleIdx *int
		wantCompletes  []provisioningCompleteCall
		wantShortfalls []shortfallDetectedCall
	}{
		{
			name: "spec level fulfilled",
			cc: crd.NewTestCrd(
				crd.WithName("test-cc"),
				crd.WithTargetNodeCount(intPtr(2)),
			),
			nodes: []*apiv1.Node{
				buildNodeWithLabels("n1", "test-cc", ""),
				buildNodeWithLabels("n2", "test-cc", ""),
			},
			matchedRuleIdx: nil,
			wantCompletes: []provisioningCompleteCall{
				{ccName: "test-cc", ruleIdx: -1},
			},
		},
		{
			name: "priority level fulfilled",
			cc: crd.NewTestCrd(
				crd.WithName("test-cc"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithTargetNodeCountRule(intPtr(2))),
				}),
			),
			nodes: []*apiv1.Node{
				buildNodeWithLabels("n1", "test-cc", ""),
				buildNodeWithLabels("n2", "test-cc", ""),
			},
			matchedRuleIdx: intPtr(0),
			wantCompletes: []provisioningCompleteCall{
				{ccName: "test-cc", ruleIdx: 0},
			},
		},
		{
			name: "skip upcoming nodes",
			cc: crd.NewTestCrd(
				crd.WithName("test-cc"),
				crd.WithTargetNodeCount(intPtr(2)),
			),
			nodes: []*apiv1.Node{
				buildNodeWithLabels("n1", "test-cc", ""),
				func() *apiv1.Node {
					n := buildNodeWithLabels("n2", "test-cc", "")
					n.Annotations = map[string]string{annotations.NodeUpcomingAnnotation: "true"}
					// Upcoming nodes are not ready.
					n.Status.Conditions = []apiv1.NodeCondition{
						{
							Type:   apiv1.NodeReady,
							Status: apiv1.ConditionFalse,
						},
					}
					return n
				}(),
			},
			matchedRuleIdx: nil,
			wantCompletes:  nil,
			wantShortfalls: []shortfallDetectedCall{
				{ccName: "test-cc", ruleIdx: -1},
			},
		},
		{
			name: "skip notready nodes",
			cc: crd.NewTestCrd(
				crd.WithName("test-cc"),
				crd.WithTargetNodeCount(intPtr(2)),
			),
			nodes: []*apiv1.Node{
				buildNodeWithLabels("n1", "test-cc", ""),
				func() *apiv1.Node {
					n := buildNodeWithLabels("n2", "test-cc", "")
					// Make it NotReady
					n.Status.Conditions = []apiv1.NodeCondition{
						{
							Type:   apiv1.NodeReady,
							Status: apiv1.ConditionFalse,
						},
					}
					return n
				}(),
			},
			matchedRuleIdx: nil,
			wantCompletes:  nil,
			wantShortfalls: []shortfallDetectedCall{
				{ccName: "test-cc", ruleIdx: -1},
			},
		},
		{
			name: "spec level shortfall",
			cc: crd.NewTestCrd(
				crd.WithName("test-cc"),
				crd.WithTargetNodeCount(intPtr(2)),
			),
			nodes: []*apiv1.Node{
				buildNodeWithLabels("n1", "test-cc", ""),
			},
			matchedRuleIdx: nil,
			wantCompletes:  nil,
			wantShortfalls: []shortfallDetectedCall{
				{ccName: "test-cc", ruleIdx: -1},
			},
		},
		{
			name: "priority level shortfall",
			cc: crd.NewTestCrd(
				crd.WithName("test-cc"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithTargetNodeCountRule(intPtr(2))),
				}),
			),
			nodes: []*apiv1.Node{
				buildNodeWithLabels("n1", "test-cc", ""),
			},
			matchedRuleIdx: intPtr(0),
			wantCompletes:  nil,
			wantShortfalls: []shortfallDetectedCall{
				{ccName: "test-cc", ruleIdx: 0},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := &minCapacityMockCloudProvider{}
			mockMatcher := &mockMatcher{}
			mockObserver := &mockMinCapacityObserver{}
			mockNodeLister := &mockNodeLister{}

			ccLister := lister.NewMockCrdLister([]crd.CRD{tc.cc})
			mockNodeLister.On("List").Return(tc.nodes, nil)

			mockProvider.nodeGroupForNodeFunc = func(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
				return &dummyNodeGroup{}, nil
			}

			if tc.matchedRuleIdx != nil {
				var matchedRule rules.Rule
				if *tc.matchedRuleIdx >= 0 && *tc.matchedRuleIdx < len(tc.cc.Rules()) {
					matchedRule = tc.cc.Rules()[*tc.matchedRuleIdx]
				}
				mockMatcher.On("FirstMatchedRule", mock.Anything, mock.Anything).Return(true, *tc.matchedRuleIdx, matchedRule)
			} else {
				mockMatcher.On("FirstMatchedRule", mock.Anything, mock.Anything).Return(false, 0, nil)
			}

			controller := NewMinCapacityController(time.Minute, ccLister, mockNodeLister, mockProvider, mockMatcher, mockObserver)

			mockObserver.On("CheckLongUnprovisioned", mock.AnythingOfType("time.Time")).Return()

			for _, want := range tc.wantCompletes {
				mockObserver.On("OnProvisioningComplete", want.ccName, want.ruleIdx, mock.AnythingOfType("time.Time")).Return().Once()
			}

			for _, want := range tc.wantShortfalls {
				mockObserver.On("OnShortfallDetected", want.ccName, want.ruleIdx, mock.AnythingOfType("time.Time")).Return().Once()
			}

			err := controller.(*minCapacityController).reconcile(time.Now())
			assert.NoError(t, err)

			mockObserver.AssertExpectations(t)
		})
	}
}

type provisioningCompleteCall struct {
	ccName  string
	ruleIdx int
}

type shortfallDetectedCall struct {
	ccName  string
	ruleIdx int
}

func buildNodeWithLabels(name, ccName, priorityStr string) *apiv1.Node {
	n := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				labels.ComputeClassLabel: ccName,
			},
		},
		Status: apiv1.NodeStatus{
			Conditions: []apiv1.NodeCondition{
				{
					Type:   apiv1.NodeReady,
					Status: apiv1.ConditionTrue,
				},
			},
		},
	}
	if priorityStr != "" {
		n.Labels[labels.ComputeClassPriorityIdxLabel] = priorityStr
	}
	return n
}

func intPtr(v int) *int { return &v }
