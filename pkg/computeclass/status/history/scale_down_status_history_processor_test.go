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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	scaledownstatus "sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/status"
	ctrl_client "sigs.k8s.io/controller-runtime/pkg/client"
)

type fakeCRDStatus struct {
	histories map[string]crd.ScalingEventsHistory
}

func (f *fakeCRDStatus) UpdateConditions(conditions []metav1.Condition)                     {}
func (f *fakeCRDStatus) UpdateResourceInfo(info crd.ResourceInfo)                           {}
func (f *fakeCRDStatus) UpdateRuleConditions(ruleIdx string, conditions []metav1.Condition) {}
func (f *fakeCRDStatus) UpdateRuleResourceInfo(ruleIdx string, info crd.ResourceInfo)       {}
func (f *fakeCRDStatus) UpdateRuleScalingHistory(ruleIdx string, history crd.ScalingEventsHistory) {
	if f.histories == nil {
		f.histories = make(map[string]crd.ScalingEventsHistory)
	}
	f.histories[ruleIdx] = history
}
func (f *fakeCRDStatus) UpdateRuleConfigHash(ruleIdx string, hash string) {}
func (f *fakeCRDStatus) UpdateRuleConsolidationStatus(ruleIdx string, status crd.ConsolidationStatus) {
}
func (f *fakeCRDStatus) GetConditions() []metav1.Condition                   { return nil }
func (f *fakeCRDStatus) GetRuleConditions(ruleIdx string) []metav1.Condition { return nil }
func (f *fakeCRDStatus) GetRuleScalingHistory(ruleIdx string) *crd.ScalingEventsHistory {
	if f.histories == nil {
		return nil
	}
	if h, ok := f.histories[ruleIdx]; ok {
		return &h
	}
	return nil
}
func (f *fakeCRDStatus) ResetAllScalingHistories()                      {}
func (f *fakeCRDStatus) ResetAllResourceInfo()                          {}
func (f *fakeCRDStatus) UpdateConfigDriftInfo(info crd.ConfigDriftInfo) {}
func (f *fakeCRDStatus) ResetConfigDriftInfo()                          {}
func (f *fakeCRDStatus) GetCRDStatusPatch() ctrl_client.Object          { return nil }

func TestScaleDownStatusHistoryProcessor(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultTestCrd := "default-crd-test"
	testCrdType := "TEST"

	crd1 := crd.NewTestCrd(crd.WithLabel(testCrdLabel),
		crd.WithName(defaultTestCrd),
		crd.WithCrdType(testCrdType),
		crd.WithRules([]rules.Rule{
			rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
		}))

	mig1 := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-1").
		SetGceRefName("nodepool-1-mig").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels: map[string]string{testCrdLabel: defaultTestCrd},
		}).Build()

	testCases := []struct {
		name          string
		crds          []crd.CRD
		steps         []*scaledownstatus.ScaleDownStatus
		initialCounts map[status.CRDId]map[string]int
		expectCounts  map[status.CRDId]map[string]int
	}{
		{
			name: "no scale down info",
			steps: []*scaledownstatus.ScaleDownStatus{
				{},
			},
			expectCounts: map[status.CRDId]map[string]int{},
		},
		{
			name: "single node scale down updates count (async)",
			crds: []crd.CRD{crd1},
			steps: []*scaledownstatus.ScaleDownStatus{
				// Cycle 1: Node is scaling down, no delete results yet
				{
					ScaledDownNodes: []*scaledownstatus.ScaleDownNode{
						{
							NodeGroup: mig1,
							Node:      &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
						},
					},
				},
				// Cycle 2: Deletion finishes successfully
				{
					NodeDeleteResults: map[string]scaledownstatus.NodeDeleteResult{
						"node-1": {ResultType: scaledownstatus.NodeDeleteOk},
					},
				},
			},
			expectCounts: map[status.CRDId]map[string]int{
				{CRDName: defaultTestCrd, CRDLabel: testCrdLabel}: {
					"0": 1,
				},
			},
		},
		{
			name: "scale down with existing count accumulates (async)",
			crds: []crd.CRD{crd1},
			steps: []*scaledownstatus.ScaleDownStatus{
				// Cycle 1: Node is scaling down
				{
					ScaledDownNodes: []*scaledownstatus.ScaleDownNode{
						{
							NodeGroup: mig1,
							Node:      &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
						},
					},
				},
				// Cycle 2: Deletion completes
				{
					NodeDeleteResults: map[string]scaledownstatus.NodeDeleteResult{
						"node-1": {ResultType: scaledownstatus.NodeDeleteOk},
					},
				},
			},
			initialCounts: map[status.CRDId]map[string]int{
				{CRDName: defaultTestCrd, CRDLabel: testCrdLabel}: {
					"0": 5,
				},
			},
			expectCounts: map[status.CRDId]map[string]int{
				{CRDName: defaultTestCrd, CRDLabel: testCrdLabel}: {
					"0": 6,
				},
			},
		},
		{
			name: "multiple nodes scaled down in same cycle (async)",
			crds: []crd.CRD{crd1},
			steps: []*scaledownstatus.ScaleDownStatus{
				// Cycle 1: Two nodes are scaling down
				{
					ScaledDownNodes: []*scaledownstatus.ScaleDownNode{
						{
							NodeGroup: mig1,
							Node:      &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
						},
						{
							NodeGroup: mig1,
							Node:      &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-2"}},
						},
					},
				},
				// Cycle 2: Both deletions complete successfully
				{
					NodeDeleteResults: map[string]scaledownstatus.NodeDeleteResult{
						"node-1": {ResultType: scaledownstatus.NodeDeleteOk},
						"node-2": {ResultType: scaledownstatus.NodeDeleteOk},
					},
				},
			},
			initialCounts: map[status.CRDId]map[string]int{
				{CRDName: defaultTestCrd, CRDLabel: testCrdLabel}: {
					"0": 5,
				},
			},
			expectCounts: map[status.CRDId]map[string]int{
				{CRDName: defaultTestCrd, CRDLabel: testCrdLabel}: {
					"0": 7,
				},
			},
		},
		{
			name: "scale down node deletion error (async)",
			crds: []crd.CRD{crd1},
			steps: []*scaledownstatus.ScaleDownStatus{
				// Cycle 1: Node is scaling down
				{
					ScaledDownNodes: []*scaledownstatus.ScaleDownNode{
						{
							NodeGroup: mig1,
							Node:      &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
						},
					},
				},
				// Cycle 2: Deletion finishes with error
				{
					NodeDeleteResults: map[string]scaledownstatus.NodeDeleteResult{
						"node-1": {ResultType: scaledownstatus.NodeDeleteErrorFailedToDelete},
					},
				},
			},
			expectCounts: map[status.CRDId]map[string]int{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister(tc.crds)
			mockLister.SetCrdLabel(testCrdLabel)
			mockLister.SetDefaultCrdName(defaultTestCrd)

			mockProvider := NewMockCloudProvider()
			mockProvider.On("IsAutopilotEnabled").Return(false)

			updatesCh := make(chan status.UpdateMessage, 100)
			mockManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true}, map[string]string{})
			processor := NewScaleDownStatusHistoryProcessor(mockLister, mockProvider, updatesCh, mockManager)
			processor.now = func() time.Time { return time.Unix(0, 0) }

			// Initialize the persistent fake statuses
			statuses := make(map[status.CRDId]*fakeCRDStatus)
			if tc.initialCounts != nil {
				for crdId, ruleCounts := range tc.initialCounts {
					fakeStatus := &fakeCRDStatus{histories: make(map[string]crd.ScalingEventsHistory)}
					for rIdx, count := range ruleCounts {
						fakeStatus.histories[rIdx] = crd.ScalingEventsHistory{
							ConsolidatedNodesCount: count,
						}
					}
					statuses[crdId] = fakeStatus
				}
			}

			// Run all steps sequentially
			for _, step := range tc.steps {
				processor.Process(context.TODO(), nil, step)
			}

			// Consume all messages generated across all steps and apply to the persistent fake status
			for len(updatesCh) > 0 {
				msg := <-updatesCh
				fakeStatus, exists := statuses[msg.Id]
				if !exists {
					fakeStatus = &fakeCRDStatus{histories: make(map[string]crd.ScalingEventsHistory)}
					statuses[msg.Id] = fakeStatus
				}
				msg.Mutate(fakeStatus)
			}

			// Assert the final state matches expectations
			if len(tc.expectCounts) > 0 {
				for crdId, expectedRuleCounts := range tc.expectCounts {
					fakeStatus, exists := statuses[crdId]
					if !assert.True(t, exists, "Status should be created for %v", crdId) {
						continue
					}
					for rIdx, count := range expectedRuleCounts {
						h := fakeStatus.GetRuleScalingHistory(rIdx)
						if assert.NotNil(t, h, "History should exist for rule %s", rIdx) {
							assert.Equal(t, count, h.ConsolidatedNodesCount)
							assert.Equal(t, metav1.NewTime(time.Unix(0, 0)), h.MeasuredAt)
							// A zero MeasuredSince serialises to null, which the CRD
							// schema rejects, and the API server then throws away the
							// entire status patch rather than just this field.
							assert.Equal(t, metav1.NewTime(time.Unix(0, 0)), h.MeasuredSince,
								"MeasuredSince must be stamped when the collection window opens")
						}

					}
				}
			} else {
				assert.Len(t, statuses, 0)
			}

			// Assert pending deletes state at the end of the test case
			assert.Empty(t, processor.pendingDeletes)
		})
	}
}

func TestScaleDownStatusHistoryProcessor_CleanupStalePendingDeletes(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultTestCrd := "default-crd-test"
	testCrdType := "TEST"

	crd1 := crd.NewTestCrd(crd.WithLabel(testCrdLabel),
		crd.WithName(defaultTestCrd),
		crd.WithCrdType(testCrdType),
		crd.WithRules([]rules.Rule{
			rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
		}))

	mig1 := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-1").
		SetGceRefName("nodepool-1-mig").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels: map[string]string{testCrdLabel: defaultTestCrd},
		}).Build()

	mockLister := lister.NewMockCrdLister([]crd.CRD{crd1})
	mockLister.SetCrdLabel(testCrdLabel)
	mockLister.SetDefaultCrdName(defaultTestCrd)

	mockProvider := NewMockCloudProvider()
	mockProvider.On("IsAutopilotEnabled").Return(false)

	updatesCh := make(chan status.UpdateMessage, 10)
	mockManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true}, map[string]string{})
	processor := NewScaleDownStatusHistoryProcessor(mockLister, mockProvider, updatesCh, mockManager)

	nowTime := time.Unix(0, 0)
	processor.now = func() time.Time { return nowTime }

	// 1. Register a pending delete
	processor.Process(context.TODO(), nil, &scaledownstatus.ScaleDownStatus{
		ScaledDownNodes: []*scaledownstatus.ScaleDownNode{
			{
				NodeGroup: mig1,
				Node:      &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
			},
		},
	})

	assert.Len(t, processor.pendingDeletes, 1)
	assert.Contains(t, processor.pendingDeletes, "node-1")

	// 2. Run a loop after some time, but less than 15 minutes (e.g. 10 minutes) - should not be cleaned up
	nowTime = nowTime.Add(10 * time.Minute)
	processor.Process(context.TODO(), nil, &scaledownstatus.ScaleDownStatus{})
	assert.Len(t, processor.pendingDeletes, 1)
	assert.Contains(t, processor.pendingDeletes, "node-1")

	// 3. Run a loop after 16 minutes (total 26 minutes) - should be cleaned up as stale
	nowTime = nowTime.Add(16 * time.Minute)
	processor.Process(context.TODO(), nil, &scaledownstatus.ScaleDownStatus{})
	assert.Len(t, processor.pendingDeletes, 0)
}

// TestScaleDownStatusHistoryProcessorMeasuredSince covers the collection window
// stamped on ScalingEventsHistory: it must be set the first time a priority
// records a consolidation, and must not move afterwards.
//
// The first part guards a real outage rather than a cosmetic field. A zero
// metav1.Time marshals to null, the CRD types measuredSince as a non-nullable
// string, and the API server rejects the whole ComputeClass status patch on it -
// so a single unstamped field silently stops resourceInfo, conditions and the
// consolidation breakdown from being reported at all.
func TestScaleDownStatusHistoryProcessorMeasuredSince(t *testing.T) {
	const (
		testCrdLabel   = "test-crd-label"
		defaultTestCrd = "default-crd-test"
	)

	testCrd := crd.NewTestCrd(crd.WithLabel(testCrdLabel),
		crd.WithName(defaultTestCrd),
		crd.WithCrdType("TEST"),
		crd.WithRules([]rules.Rule{
			rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
		}))

	mig := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-1").
		SetGceRefName("nodepool-1-mig").
		SetSpec(&gkeclient.NodePoolSpec{
			Labels: map[string]string{testCrdLabel: defaultTestCrd},
		}).Build()

	mockLister := lister.NewMockCrdLister([]crd.CRD{testCrd})
	mockLister.SetCrdLabel(testCrdLabel)
	mockLister.SetDefaultCrdName(defaultTestCrd)

	mockProvider := NewMockCloudProvider()
	mockProvider.On("IsAutopilotEnabled").Return(false)

	updatesCh := make(chan status.UpdateMessage, 100)
	mockManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true}, map[string]string{})
	processor := NewScaleDownStatusHistoryProcessor(mockLister, mockProvider, updatesCh, mockManager)

	now := time.Unix(1000, 0)
	processor.now = func() time.Time { return now }

	fakeStatus := &fakeCRDStatus{histories: make(map[string]crd.ScalingEventsHistory)}
	// consolidateOneNode runs the two cycles a node deletion takes (announced, then
	// reported complete) and applies the resulting updates to fakeStatus.
	consolidateOneNode := func(nodeName string) {
		processor.Process(context.TODO(), nil, &scaledownstatus.ScaleDownStatus{
			ScaledDownNodes: []*scaledownstatus.ScaleDownNode{
				{NodeGroup: mig, Node: &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}},
			},
		})
		processor.Process(context.TODO(), nil, &scaledownstatus.ScaleDownStatus{
			NodeDeleteResults: map[string]scaledownstatus.NodeDeleteResult{
				nodeName: {ResultType: scaledownstatus.NodeDeleteOk},
			},
		})
		for len(updatesCh) > 0 {
			(<-updatesCh).Mutate(fakeStatus)
		}
	}

	consolidateOneNode("node-1")

	history := fakeStatus.GetRuleScalingHistory("0")
	if !assert.NotNil(t, history, "expected a scaling history for the priority") {
		return
	}
	assert.Equal(t, 1, history.ConsolidatedNodesCount)
	assert.Equal(t, metav1.NewTime(now), history.MeasuredSince,
		"the first consolidation must open the collection window; a zero value serialises to null and the API server rejects the entire status patch")
	assert.Equal(t, metav1.NewTime(now), history.MeasuredAt)

	// A later consolidation advances MeasuredAt but must leave the window open
	// where it started, otherwise the two timestamps carry the same information
	// and the counts cannot be interpreted as a rate.
	opened := now
	now = now.Add(time.Hour)

	consolidateOneNode("node-2")

	history = fakeStatus.GetRuleScalingHistory("0")
	if !assert.NotNil(t, history, "expected the scaling history to survive the second consolidation") {
		return
	}
	assert.Equal(t, 2, history.ConsolidatedNodesCount)
	assert.Equal(t, metav1.NewTime(opened), history.MeasuredSince, "MeasuredSince must not be reset by later consolidations")
	assert.Equal(t, metav1.NewTime(now), history.MeasuredAt, "MeasuredAt must advance with each consolidation")
}
