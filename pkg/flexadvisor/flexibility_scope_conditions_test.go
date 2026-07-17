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

package flexadvisor

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	optstracking "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options/tracking"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/utils/ptr"
)

type priorityDef struct {
	machineType   *string
	nodepools     []string
	zones         []string
	priorityScore *int
}

type wantCondition struct {
	cType    string
	cReason  string
	cMessage string
}

func TestFlexibilityScope_EmitRuleFilteringConditions(t *testing.T) {
	testCases := []struct {
		name           string
		priorities     []priorityDef
		availability   map[string]map[string]int
		wantConditions map[string]wantCondition
	}{
		{
			name: "emit ProvisioningSuspended when all zones are filtered out",
			priorities: []priorityDef{
				{machineType: ptr.To("e2-standard-2"), zones: []string{"us-west1-a"}},
			},
			availability: map[string]map[string]int{
				"us-west1-a": {
					"e2-standard-2": 0,
				},
			},
			wantConditions: map[string]wantCondition{
				"0": {
					cType:    ConditionTypeRuleFilteredOut,
					cReason:  ConditionReasonFilteredOut,
					cMessage: "No matching configuration is available (0/2 available) due to active capacity cooldowns across requested zones.",
				},
			},
		},
		{
			name: "emit ProvisioningConstrained when some but not all zones are filtered out",
			priorities: []priorityDef{
				{machineType: ptr.To("e2-standard-2"), zones: []string{"us-west1-a", "us-west1-b"}},
			},
			availability: map[string]map[string]int{
				"us-west1-a": {
					"e2-standard-2": 0,
				},
				"us-west1-b": {
					"e2-standard-2": 10,
				},
			},
			wantConditions: map[string]wantCondition{
				"0": {
					cType:    ConditionTypeRulePartiallyFiltered,
					cReason:  ConditionReasonFilteredOut,
					cMessage: "A subset (2/4) of configurations are excluded due to active capacity cooldowns across requested zones.",
				},
			},
		},
		{
			name: "no condition emitted when all zones are available",
			priorities: []priorityDef{
				{machineType: ptr.To("e2-standard-2"), zones: []string{"us-west1-a", "us-west1-b"}},
			},
			availability: map[string]map[string]int{
				"us-west1-a": {
					"e2-standard-2": 5,
				},
				"us-west1-b": {
					"e2-standard-2": 10,
				},
			},
			wantConditions: map[string]wantCondition{},
		},
		{
			name: "emit conditions independently across multiple priorities when multiple are filtered out",
			priorities: []priorityDef{
				{machineType: ptr.To("e2-standard-2"), zones: []string{"us-west1-a"}},
				{machineType: ptr.To("n2-standard-2"), zones: []string{"us-west1-a"}},
			},
			availability: map[string]map[string]int{
				"us-west1-a": {
					"e2-standard-2": 0,
					"n2-standard-2": 0,
				},
			},
			wantConditions: map[string]wantCondition{
				"0": {
					cType:    ConditionTypeRuleFilteredOut,
					cReason:  ConditionReasonFilteredOut,
					cMessage: "No matching configuration is available (0/2 available) due to active capacity cooldowns across requested zones.",
				},
				"1": {
					cType:    ConditionTypeRuleFilteredOut,
					cReason:  ConditionReasonFilteredOut,
					cMessage: "No matching configuration is available (0/2 available) due to active capacity cooldowns across requested zones.",
				},
			},
		},
		{
			name: "emit conditions only for priorities with filtered or constrained availability",
			priorities: []priorityDef{
				{machineType: ptr.To("e2-standard-2"), zones: []string{"us-west1-a", "us-west1-b"}},
				{machineType: ptr.To("n2-standard-2"), zones: []string{"us-west1-a", "us-west1-b"}},
				{machineType: ptr.To("c2-standard-4"), zones: []string{"us-west1-a", "us-west1-b"}},
			},
			availability: map[string]map[string]int{
				"us-west1-a": {
					"e2-standard-2": 0,
					"n2-standard-2": 0,
					"c2-standard-4": 10,
				},
				"us-west1-b": {
					"e2-standard-2": 0,
					"n2-standard-2": 5,
					"c2-standard-4": 10,
				},
			},
			wantConditions: map[string]wantCondition{
				"0": {
					cType:    ConditionTypeRuleFilteredOut,
					cReason:  ConditionReasonFilteredOut,
					cMessage: "No matching configuration is available (0/4 available) due to active capacity cooldowns across requested zones.",
				},
				"1": {
					cType:    ConditionTypeRulePartiallyFiltered,
					cReason:  ConditionReasonFilteredOut,
					cMessage: "A subset (2/4) of configurations are excluded due to active capacity cooldowns across requested zones.",
				},
			},
		},
		{
			name: "emit conditions for priorities with the same priority score (same weight/rank in GroupedRules)",
			priorities: []priorityDef{
				{machineType: ptr.To("e2-standard-2"), zones: []string{"us-west1-a", "us-west1-b"}, priorityScore: ptr.To(100)},
				{machineType: ptr.To("n2-standard-2"), zones: []string{"us-west1-a", "us-west1-b"}, priorityScore: ptr.To(100)},
				{machineType: ptr.To("c2-standard-4"), zones: []string{"us-west1-a", "us-west1-b"}, priorityScore: ptr.To(50)},
			},
			availability: map[string]map[string]int{
				"us-west1-a": {
					"e2-standard-2": 0,
					"n2-standard-2": 0,
					"c2-standard-4": 10,
				},
				"us-west1-b": {
					"e2-standard-2": 0,
					"n2-standard-2": 10,
					"c2-standard-4": 10,
				},
			},
			wantConditions: map[string]wantCondition{
				"0": {
					cType:    ConditionTypeRuleFilteredOut,
					cReason:  ConditionReasonFilteredOut,
					cMessage: "No matching configuration is available (0/4 available) due to active capacity cooldowns across requested zones.",
				},
				"1": {
					cType:    ConditionTypeRulePartiallyFiltered,
					cReason:  ConditionReasonFilteredOut,
					cMessage: "A subset (2/4) of configurations are excluded due to active capacity cooldowns across requested zones.",
				},
			},
		},
		{
			name: "emit ProvisioningSuspended for a node pool rule when filtered out",
			priorities: []priorityDef{
				{nodepools: []string{"node-pool-1"}},
			},
			availability: map[string]map[string]int{
				"us-west1-a": {
					"e2-standard-2": 0,
				},
			},
			wantConditions: map[string]wantCondition{
				"0": {
					cType:    ConditionTypeRuleFilteredOut,
					cReason:  ConditionReasonFilteredOut,
					cMessage: "No matching configuration is available (0/1 available) due to active capacity cooldowns across requested zones.",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {

				// Setup tested objects and structures
				mockProvider := &mockAdviceProvider{}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				var priorities []v1.Priority
				var allZones []string
				for _, p := range tc.priorities {
					var loc *v1.Location
					if p.zones != nil {
						loc = &v1.Location{Zones: p.zones}
						allZones = append(allZones, p.zones...)
					}
					priorities = append(priorities, v1.Priority{
						MachineType:   p.machineType,
						Nodepools:     p.nodepools,
						Location:      loc,
						PriorityScore: p.priorityScore,
					})
				}

				crd1 := ccc.NewCccCrd(&v1.ComputeClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "ccc-1",
					},
					Spec: v1.ComputeClassSpec{
						Priorities: priorities,
					},
				}, "", false, crd.TestDefaultDataProvider(), nil)

				mig1 := gke.NewTestGkeMigBuilder().SetNodePoolName("node-pool-1").SetSpec(&gkeclient.NodePoolSpec{
					Locations:   []string{"us-west1-a"},
					MachineType: "e2-standard-2",
					Spot:        true,
				}).Build()
				if len(allZones) == 0 {
					allZones = []string{"us-west1-a"}
				}

				instanceConfigCloudProvider := newMockInstanceConfigCloudProvider(allZones, []*gke.GkeMig{mig1}, machinetypes.E2, true, nil)
				optionsTracker := optstracking.FakeOptionsTracker(options.AutoscalingOptions{}, gkeclient.Cluster{}, experiments.NewMockManager())
				mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crd1}, labels.ComputeClassLabel)
				updatesCh := make(chan status.UpdateMessage, 10)

				fa, err := NewFlexAdvisor(ctx, mockProvider, mockLister, instanceConfigCloudProvider, optionsTracker, updatesCh)
				assert.NoError(t, err)

				setupScopeAvailability(t, fa, mockProvider, "ccc-1", crd1, tc.availability)

				fa.RegisterFlexibilityScope("ccc-1")
				// we want the cache to be populated, it's ok to call this for empty instanceConfigKey.
				fa.AwaitInstanceAvailability("ccc-1", "")

				mockStatus := &mockCrdStatus{}

				// We expect as many update messages as the number of rules
				expectedMessagesCount := len(crd1.Rules())
				assert.Equal(t, expectedMessagesCount, len(updatesCh), "number of messages in updatesCh should match expectedMessagesCount")
				for i := 0; i < expectedMessagesCount; i++ {
					select {
					case msg := <-updatesCh:
						assert.Equal(t, "ccc-1", msg.Id.CRDName)
						msg.Mutate(mockStatus)
					default:
						t.Fatalf("expected update message %d in updatesCh, but got none", i)
					}
				}
				assert.Empty(t, updatesCh, "expected no extra messages in updatesCh")

				// Verify rule conditions match expectations
				for idxStr, wantCond := range tc.wantConditions {
					conds := mockStatus.ruleConditions[idxStr]
					assert.Len(t, conds, 1)
					assert.Equal(t, wantCond.cType, conds[0].Type)
					assert.Equal(t, metav1.ConditionTrue, conds[0].Status)
					assert.Equal(t, wantCond.cReason, conds[0].Reason)
					assert.Equal(t, wantCond.cMessage, conds[0].Message)
				}

				// Verify rules not in wantConditions have NO conditions populated
				for idx := 0; idx < len(crd1.Rules()); idx++ {
					idxStr := fmt.Sprintf("%d", idx)
					if _, expected := tc.wantConditions[idxStr]; !expected {
						assert.Len(t, mockStatus.ruleConditions[idxStr], 0)
					}
				}
			})
		})
	}
}

type mockCrdStatus struct {
	crd.CRDStatus
	ruleConditions map[string][]metav1.Condition
	updated        bool
}

func (m *mockCrdStatus) GetRuleConditions(ruleIdx string) []metav1.Condition {
	return m.ruleConditions[ruleIdx]
}

func (m *mockCrdStatus) UpdateRuleConditions(ruleIdx string, conditions []metav1.Condition) {
	if m.ruleConditions == nil {
		m.ruleConditions = make(map[string][]metav1.Condition)
	}
	m.ruleConditions[ruleIdx] = conditions
	m.updated = true
}

func generateConfigsForCrd(t *testing.T, fa *flexAdvisor, c crd.CRD) []*api.InstanceConfig {
	var allConfigs []*api.InstanceConfig
	for groupIdx, ruleGroup := range c.GroupedRules() {
		for _, rule := range ruleGroup {
			configs, errs := fa.instanceConfigGenerator.generateInstanceConfigsForRule(rule, groupIdx+1)
			assert.Empty(t, errs)
			assert.NotEmpty(t, configs)
			allConfigs = append(allConfigs, configs...)
		}
	}
	return allConfigs
}

func setupScopeAvailability(t *testing.T, fa *flexAdvisor, mockProvider *mockAdviceProvider, crdName string, c crd.CRD, availability map[string]map[string]int) {
	configs := generateConfigsForCrd(t, fa, c)
	mockApiResponse := make(map[string]*api.InstanceAvailability)
	for _, config := range configs {
		sig := config.Signature()
		zonalCounts := make(map[string]int)
		for zone, machineMap := range availability {
			if count, ok := machineMap[config.MachineType()]; ok {
				zonalCounts[zone] = count
			}
		}
		mockApiResponse[sig] = api.NewTestInstanceAvailabilityBuilder(crdName, sig).
			WithZonalInstanceCount(zonalCounts).Build()
	}
	mockProvider.On("FetchCapacityGuidance").Return(mockApiResponse, nil).Once()
}
