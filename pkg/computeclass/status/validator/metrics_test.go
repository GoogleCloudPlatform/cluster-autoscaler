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

package validator

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/validator/conditions"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

func TestLoopObservesCrdMetrics(t *testing.T) {
	testCrdLabel := "test-crd-label"
	testCrdType := "TEST"
	testCrdType2 := "TEST2"
	cccCrdType := "CCC"
	npcCrdType := "NPC"
	defaultGkeManager := &gke.GkeManagerMock{}
	migs := []*gke.GkeMig{
		gke.NewTestGkeMigBuilder().
			SetNodePoolName("nodepool-1").
			SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{testCrdLabel: "crd-object-1"},
				Taints: []v1.Taint{
					{Key: testCrdLabel, Value: "crd-object-1"},
				},
				MachineType: "machine-type",
				Spot:        true}).
			SetGkeManager(defaultGkeManager).
			Build(),
		gke.NewTestGkeMigBuilder().
			SetNodePoolName("nodepool-2").
			SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{testCrdLabel: "crd-object-2"},
				Taints: []v1.Taint{
					{Key: testCrdLabel, Value: "crd-object-2"},
				},
				MachineType: "machine-type",
				Spot:        true}).
			SetGkeManager(defaultGkeManager).
			Build(),
		gke.NewTestGkeMigBuilder().
			SetNodePoolName("nodepool-3").
			SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{testCrdLabel: "crd-object-3"},
				Taints: []v1.Taint{
					{Key: testCrdLabel, Value: "crd-object-3"},
				},
				MachineType: "machine-type",
				Spot:        true}).
			SetGkeManager(defaultGkeManager).
			Build(),
		gke.NewTestGkeMigBuilder().
			SetNodePoolName("nodepool-4").
			SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{testCrdLabel: "crd-object-4"},
				Taints: []v1.Taint{
					{Key: testCrdLabel, Value: "crd-object-4"},
				},
				MachineType: "machine-type",
				Spot:        true}).
			SetGkeManager(defaultGkeManager).
			Build(),
		gke.NewTestGkeMigBuilder().
			SetNodePoolName("nodepool-5").
			SetSpec(&gkeclient.NodePoolSpec{
				Labels: map[string]string{testCrdLabel: "crd-object-5"},
				Taints: []v1.Taint{
					{Key: testCrdLabel, Value: "crd-object-5"},
				},
				MachineType: "machine-type",
				Spot:        true}).
			SetGkeManager(defaultGkeManager).
			Build(),
	}

	testCases := []struct {
		name                              string
		crds                              []crd.CRD
		validatorNapEnabled               bool
		wantHealthMap                     map[string]healthiness
		wantUnhealthinessConditionSamples []metrics.CrdUnhealthinessConditionSample
		wantCrdCountSamples               []metrics.NpcCountSample
	}{
		{
			name:                "no crds",
			crds:                []crd.CRD{},
			wantCrdCountSamples: nil,
		},
		{
			name: "healthy crd with defrag",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway(), crd.WithOptimizeRulePriority()),
			},
			wantHealthMap: map[string]healthiness{
				testCrdType: {
					healthy:   1,
					unhealthy: 0,
				},
			},
			wantCrdCountSamples: []metrics.NpcCountSample{{
				NapEnabled:        false,
				DefragEnabled:     true,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           testCrdType,
			}},
		},
		{
			name: "unhealthy crd with NAP",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
			wantHealthMap: map[string]healthiness{
				testCrdType: {
					healthy:   0,
					unhealthy: 1, // NAP is not enabled in Conditions
				},
			},
			wantUnhealthinessConditionSamples: []metrics.CrdUnhealthinessConditionSample{
				{
					Condition: conditions.CrdMisconfiguredCondition,
					Reason:    conditions.NapCannotBeEnabledReason,
					CrdType:   testCrdType,
				},
			},
			wantCrdCountSamples: []metrics.NpcCountSample{{
				NapEnabled:        true,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           testCrdType,
			}},
		},
		{
			name: "healthy crd with DoNotScaleUp",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}),
				),
			},
			wantHealthMap: map[string]healthiness{
				testCrdType: {
					healthy:   1,
					unhealthy: 0,
				},
			},
			wantCrdCountSamples: []metrics.NpcCountSample{{
				NapEnabled:        false,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.DoNotScaleUp,
				CrdType:           testCrdType,
			}},
		},
		{
			name: "healthy crd with nap and defrag explicitly disabled",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			wantHealthMap: map[string]healthiness{
				testCrdType: {
					healthy:   1,
					unhealthy: 0,
				},
			},
			wantCrdCountSamples: []metrics.NpcCountSample{{
				NapEnabled:        false,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           testCrdType,
			}},
		},
		{
			name: "healthy crd with no priorities",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"), crd.WithCrdType(testCrdType), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
			validatorNapEnabled: true, // when no rules then NAP is required for healthy Crd
			wantHealthMap: map[string]healthiness{
				testCrdType: {
					healthy:   1,
					unhealthy: 0,
				},
			},
			wantCrdCountSamples: []metrics.NpcCountSample{{
				NapEnabled:        true,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           testCrdType,
			}},
		},
		{
			name: "multiple healthy and unhealthy crds of different types",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-2"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					})),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-3"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-3"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-4"),
					crd.WithCrdType(testCrdType2),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-4"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-5"),
					crd.WithCrdType(testCrdType2),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-5"})),
					}), crd.WithScaleUpAnyway()),
			},
			wantHealthMap: map[string]healthiness{
				testCrdType2: {
					healthy:   1,
					unhealthy: 1,
				},
				testCrdType: {
					healthy:   2,
					unhealthy: 1,
				},
			},
			wantUnhealthinessConditionSamples: []metrics.CrdUnhealthinessConditionSample{
				{
					Condition: conditions.CrdMisconfiguredCondition,
					Reason:    conditions.NapCannotBeEnabledReason,
					CrdType:   testCrdType,
				},
				{
					Condition: conditions.CrdMisconfiguredCondition,
					Reason:    conditions.NapCannotBeEnabledReason,
					CrdType:   testCrdType2,
				},
			},
			wantCrdCountSamples: []metrics.NpcCountSample{{
				NapEnabled:        false,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           testCrdType,
			}, {
				NapEnabled:        false,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.DoNotScaleUp,
				CrdType:           testCrdType,
			}, {
				NapEnabled:        true,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           testCrdType,
			}, {
				NapEnabled:        true,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           testCrdType2,
			}, {
				NapEnabled:        false,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           testCrdType2,
			}},
		},
		{
			name: "ccc crd check unhealthy",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(cccCrdType),
					crd.WithName("crd-object-1"), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
			wantHealthMap: map[string]healthiness{
				cccCrdType: {
					healthy:   0,
					unhealthy: 1,
				},
			},
			wantUnhealthinessConditionSamples: []metrics.CrdUnhealthinessConditionSample{
				{
					Condition: conditions.CrdMisconfiguredCondition,
					Reason:    conditions.NapCannotBeEnabledReason,
					CrdType:   cccCrdType,
				},
			},
			wantCrdCountSamples: []metrics.NpcCountSample{{
				NapEnabled:        true,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           cccCrdType,
			}},
		},
		{
			name: "npc crd check unhealthy",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(npcCrdType),
					crd.WithName("crd-object-2"), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
			wantHealthMap: map[string]healthiness{
				npcCrdType: {
					healthy:   0,
					unhealthy: 1,
				},
			},
			wantUnhealthinessConditionSamples: []metrics.CrdUnhealthinessConditionSample{
				{
					Condition: conditions.CrdMisconfiguredCondition,
					Reason:    conditions.NapCannotBeEnabledReason,
					CrdType:   npcCrdType,
				},
			},
			wantCrdCountSamples: []metrics.NpcCountSample{{
				NapEnabled:        true,
				DefragEnabled:     false,
				WhenUnsatisfiable: computeclass.ScaleUpAnyway,
				CrdType:           npcCrdType,
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutoprovisioningDefaultFamily(machinetypes.E2).
				WithGkeMigs(migs).
				WithAutoprovisioningEnabled(tc.validatorNapEnabled).
				Build()
			mockCrdLister := lister.NewMockCrdLister(tc.crds)
			mockCrdLister.SetCrdLabel(testCrdLabel)

			client := &fakeClient{}
			mockMetricsObserver := computeclass.NewMockMetrics()
			validator, _ := NewValidator(client, mockCrdLister, provider, mockMetricsObserver, nil, nil, nil, emptyConfig, nil, false)

			validator.loop()

			mockMetricsObserver.AssertNumberOfCalls(t, "ObserveNpcHealth", len(tc.wantHealthMap))
			for crdType, healthData := range tc.wantHealthMap {
				mockMetricsObserver.AssertCalled(t, "ObserveNpcHealth", crdType, healthData.healthy, healthData.unhealthy)
			}

			mockMetricsObserver.AssertNumberOfCalls(t, "ObserveCrdUnhealthinessConditions", 1)
			mockMetricsObserver.AssertCalled(t, "ObserveCrdUnhealthinessConditions", tc.wantUnhealthinessConditionSamples)

			mockMetricsObserver.AssertNumberOfCalls(t, "ObserveNpcCount", 1)
			mockMetricsObserver.AssertCalled(t, "ObserveNpcCount", tc.wantCrdCountSamples)
		})
	}
}

func TestObserveUnhealthinessConditionsMetrics(t *testing.T) {
	message := "Test message"
	testCrdType1 := "TEST_CRD_TYPE_1"
	testCrdType2 := "TEST_CRD_TYPE_2"

	testCrdName1 := "TEST_NAME_1"
	testCrdName2 := "TEST_NAME_2"
	testCrdName3 := "TEST_NAME_3"

	testConditionType1 := "TEST_CONDITION_TYPE_1"
	testConditionReason1 := "TEST_CONDITION_REASON_1"
	genericTestUnhealthinessCondition1 := metav1.Condition{
		Type:               testConditionType1,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             testConditionReason1,
		Message:            message,
	}

	testConditionType2 := "TEST_CONDITION_TYPE_2"
	testConditionReason2 := "TEST_CONDITION_REASON_2"
	genericTestUnhealthinessCondition2 := metav1.Condition{
		Type:               testConditionType2,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             testConditionReason2,
		Message:            message,
	}

	testConditionType3 := "TEST_CONDITION_TYPE_3"
	testConditionReason3 := "TEST_CONDITION_REASON_3"
	genericTestUnhealthinessCondition3 := metav1.Condition{
		Type:               testConditionType3,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             testConditionReason3,
		Message:            message,
	}

	testCases := []struct {
		name                              string
		unhealthinessConditions           []crdConditions
		wantUnhealthinessConditionSamples []metrics.CrdUnhealthinessConditionSample
	}{
		{
			name: "1 unhealthy crd, nap cannot be enabled",
			unhealthinessConditions: []crdConditions{
				{
					crd: crd.NewTestCrd(
						crd.WithCrdType(testCrdType1),
						crd.WithName(testCrdName1),
						crd.WithScaleUpAnyway(),
					),
					conditions: []metav1.Condition{
						*conditions.NapCannotBeEnabledCondition(),
					},
				},
			},
			wantUnhealthinessConditionSamples: []metrics.CrdUnhealthinessConditionSample{
				{
					Condition: conditions.CrdMisconfiguredCondition,
					Reason:    conditions.NapCannotBeEnabledReason,
					CrdType:   testCrdType1,
				},
			},
		},
		{
			name: "1 unhealthy crd, generic test case",
			unhealthinessConditions: []crdConditions{
				{
					crd: crd.NewTestCrd(
						crd.WithCrdType(testCrdType1),
						crd.WithName(testCrdName1),
						crd.WithScaleUpAnyway(),
					),
					conditions: []metav1.Condition{
						genericTestUnhealthinessCondition1,
					},
				},
			},
			wantUnhealthinessConditionSamples: []metrics.CrdUnhealthinessConditionSample{
				{
					Condition: testConditionType1,
					Reason:    testConditionReason1,
					CrdType:   testCrdType1,
				},
			},
		},
		{
			name: "1 crd with 2 generic unhealthy conditions",
			unhealthinessConditions: []crdConditions{
				{
					crd: crd.NewTestCrd(
						crd.WithCrdType(testCrdType1),
						crd.WithName(testCrdName1),
						crd.WithScaleUpAnyway(),
					),
					conditions: []metav1.Condition{
						genericTestUnhealthinessCondition1,
						genericTestUnhealthinessCondition2,
					},
				},
			},
			wantUnhealthinessConditionSamples: []metrics.CrdUnhealthinessConditionSample{
				{
					Condition: testConditionType1,
					Reason:    testConditionReason1,
					CrdType:   testCrdType1,
				},
				{
					Condition: testConditionType2,
					Reason:    testConditionReason2,
					CrdType:   testCrdType1,
				},
			},
		},
		{
			name: "multiple crds with multiple generic unhealthy conditions",
			unhealthinessConditions: []crdConditions{
				{
					crd: crd.NewTestCrd(
						crd.WithCrdType(testCrdType1),
						crd.WithName(testCrdName1),
						crd.WithScaleUpAnyway(),
					),
					conditions: []metav1.Condition{
						genericTestUnhealthinessCondition1,
						genericTestUnhealthinessCondition2,
					},
				},
				{
					crd: crd.NewTestCrd(
						crd.WithCrdType(testCrdType2),
						crd.WithName(testCrdName2),
						crd.WithScaleUpAnyway(),
					),
					conditions: []metav1.Condition{
						genericTestUnhealthinessCondition3,
						genericTestUnhealthinessCondition2,
						genericTestUnhealthinessCondition1,
					},
				},
				{
					crd: crd.NewTestCrd(
						crd.WithCrdType(testCrdType1),
						crd.WithName(testCrdName3),
						crd.WithScaleUpAnyway(),
					),
					conditions: []metav1.Condition{
						genericTestUnhealthinessCondition1,
						genericTestUnhealthinessCondition3,
					},
				},
			},
			wantUnhealthinessConditionSamples: []metrics.CrdUnhealthinessConditionSample{
				{
					Condition: testConditionType1,
					Reason:    testConditionReason1,
					CrdType:   testCrdType1,
				},
				{
					Condition: testConditionType2,
					Reason:    testConditionReason2,
					CrdType:   testCrdType1,
				},

				{
					Condition: testConditionType3,
					Reason:    testConditionReason3,
					CrdType:   testCrdType2,
				},
				{
					Condition: testConditionType2,
					Reason:    testConditionReason2,
					CrdType:   testCrdType2,
				},
				{
					Condition: testConditionType1,
					Reason:    testConditionReason1,
					CrdType:   testCrdType2,
				},

				{
					Condition: testConditionType1,
					Reason:    testConditionReason1,
					CrdType:   testCrdType1,
				},
				{
					Condition: testConditionType3,
					Reason:    testConditionReason3,
					CrdType:   testCrdType1,
				},
			},
		},
		{
			name: "healthy crd, no unhealthiness conditions, should be skipped",
			unhealthinessConditions: []crdConditions{
				{
					crd: crd.NewTestCrd(
						crd.WithCrdType(testCrdType1),
						crd.WithName(testCrdName1),
						crd.WithScaleUpAnyway(),
					),
					conditions: []metav1.Condition{},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetricsObserver := computeclass.NewMockMetrics()
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			validator, _ := NewValidator(nil, nil, provider, mockMetricsObserver, nil, nil, nil, emptyConfig, nil, false)
			validator.observeUnhealthinessConditionsMetrics(tc.unhealthinessConditions)

			mockMetricsObserver.AssertNumberOfCalls(t, "ObserveCrdUnhealthinessConditions", 1)
			mockMetricsObserver.AssertCalled(t, "ObserveCrdUnhealthinessConditions", tc.wantUnhealthinessConditionSamples)
		})
	}
}

func TestLoopObservesNpcRuleMetrics(t *testing.T) {
	testCrdLabel := "test-crd-label"
	testCrdType := "TEST"
	testCrdType2 := "TEST2"
	cccCrdType := "CCC"
	npcCrdType := "NPC"

	testCases := []struct {
		name                    string
		crds                    []crd.CRD
		wantNpcRuleCountSamples []metrics.NpcRuleCountSample
	}{
		{
			name:                    "no npcs",
			crds:                    []crd.CRD{},
			wantNpcRuleCountSamples: nil,
		},
		{
			name: "test crd without any rule",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			},
			wantNpcRuleCountSamples: nil,
		},
		{
			name: "npc crd type check with Nodepool rules only",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(npcCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			wantNpcRuleCountSamples: []metrics.NpcRuleCountSample{{
				RuleIndex: 0,
				RuleType:  "Nodepools",
				CrdType:   npcCrdType,
			}},
		},
		{
			name: "ccc crd type check with Nodepool rules only",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(cccCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			wantNpcRuleCountSamples: []metrics.NpcRuleCountSample{{
				RuleIndex: 0,
				RuleType:  "Nodepools",
				CrdType:   cccCrdType,
			}},
		},
		{
			name: "test crd with Nodepool rules only",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			wantNpcRuleCountSamples: []metrics.NpcRuleCountSample{{
				RuleIndex: 0,
				RuleType:  "Nodepools",
				CrdType:   testCrdType,
			}},
		},
		{
			name: "test crd with InstanceCharacterstics rule only",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewMachineSpecRule(nil, nil, nil, nil),
					}), crd.WithScaleUpAnyway()),
			},
			wantNpcRuleCountSamples: []metrics.NpcRuleCountSample{{
				RuleIndex: 0,
				RuleType:  "NodeConfig",
				CrdType:   testCrdType,
			}},
		},
		{
			name: "test crd with multiple rules",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
						rules.NewMachineSpecRule(nil, nil, nil, nil),
					}), crd.WithScaleUpAnyway()),
			},
			wantNpcRuleCountSamples: []metrics.NpcRuleCountSample{
				{
					RuleIndex: 0,
					RuleType:  "Nodepools",
					CrdType:   testCrdType,
				},
				{
					RuleIndex: 1,
					RuleType:  "Nodepools",
					CrdType:   testCrdType,
				},
				{
					RuleIndex: 2,
					RuleType:  "NodeConfig",
					CrdType:   testCrdType,
				},
			},
		},
		{
			name: "multiple npcs with multiple rules",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
						rules.NewMachineSpecRule(nil, nil, nil, nil),
					}), crd.WithScaleUpAnyway()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-2"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewMachineSpecRule(nil, nil, nil, nil),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-3"})),
					}), crd.WithScaleUpAnyway()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-3"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewMachineSpecRule(nil, nil, nil, nil),
					}), crd.WithScaleUpAnyway()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-4"),
					crd.WithCrdType(testCrdType2),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-4"})),
					}), crd.WithScaleUpAnyway()),
			},
			wantNpcRuleCountSamples: []metrics.NpcRuleCountSample{
				{
					RuleIndex: 0,
					RuleType:  "Nodepools",
					CrdType:   testCrdType,
				},
				{
					RuleIndex: 1,
					RuleType:  "Nodepools",
					CrdType:   testCrdType,
				},
				{
					RuleIndex: 2,
					RuleType:  "NodeConfig",
					CrdType:   testCrdType,
				},
				{
					RuleIndex: 0,
					RuleType:  "NodeConfig",
					CrdType:   testCrdType,
				},
				{
					RuleIndex: 1,
					RuleType:  "Nodepools",
					CrdType:   testCrdType,
				},
				{
					RuleIndex: 0,
					RuleType:  "NodeConfig",
					CrdType:   testCrdType,
				},
				{
					RuleIndex: 0,
					RuleType:  "Nodepools",
					CrdType:   testCrdType2,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			mockCrdLister := lister.NewMockCrdLister(tc.crds)
			mockCrdLister.SetCrdLabel(testCrdLabel)

			client := &fakeClient{}
			mockMetricsObserver := computeclass.NewMockMetrics()
			validator, _ := NewValidator(client, mockCrdLister, provider, mockMetricsObserver, nil, nil, nil, emptyConfig, nil, false)

			validator.observeRuleCountMetrics(tc.crds)

			mockMetricsObserver.AssertNumberOfCalls(t, "ObserveNpcRuleCount", 1)
			mockMetricsObserver.AssertCalled(t, "ObserveNpcRuleCount", tc.wantNpcRuleCountSamples)
		})
	}
}

func TestObserveCrdHealthHandlesCorruptInput(t *testing.T) {
	testCrdType := "TEST"
	cccCrdType := "CCC"
	npcCrdType := "NPC"
	testCases := []struct {
		name          string
		crds          []crd.CRD
		conditions    [][]metav1.Condition
		wantHealthMap map[string]healthiness
	}{
		{
			name: "simple npc crd type check",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel("crd-1"),
					crd.WithCrdType(npcCrdType),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			conditions: [][]metav1.Condition{
				{*conditions.CrdHealthyCondition()},
			},
			wantHealthMap: map[string]healthiness{
				npcCrdType: {
					healthy:   1,
					unhealthy: 0,
				},
			},
		},
		{
			name: "simple ccc crd type check",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel("crd-1"),
					crd.WithCrdType(cccCrdType),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			conditions: [][]metav1.Condition{
				{*conditions.CrdHealthyCondition()},
			},
			wantHealthMap: map[string]healthiness{
				cccCrdType: {
					healthy:   1,
					unhealthy: 0,
				},
			},
		},
		{
			name: "skip crd with no health condition",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel("crd-1"),
					crd.WithName("skipped"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
				crd.NewTestCrd(crd.WithLabel("crd-1"),
					crd.WithCrdType(testCrdType),
					crd.WithName("crd-object-2"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway()),
			},
			conditions: [][]metav1.Condition{
				{},
				{*conditions.CrdHealthyCondition()},
			},
			wantHealthMap: map[string]healthiness{
				testCrdType: {
					healthy:   1,
					unhealthy: 0,
				},
			},
		},
		{
			name: "skip crd with corrupt health condition status",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel("crd-1"),
					crd.WithCrdType(testCrdType),
					crd.WithName("skipped"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
				crd.NewTestCrd(crd.WithLabel("crd-1"),
					crd.WithCrdType(testCrdType),
					crd.WithName("crd-object-2"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway()),
			},
			conditions: [][]metav1.Condition{
				{
					metav1.Condition{
						Type:               conditions.HealthCondition,
						Status:             "corrupt",
						LastTransitionTime: metav1.Now(),
						Reason:             "",
						Message:            "",
					},
				},
				{*conditions.CrdHealthyCondition()},
			},
			wantHealthMap: map[string]healthiness{
				testCrdType: {
					healthy:   1,
					unhealthy: 0,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetricsObserver := computeclass.NewMockMetrics()
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			validator, _ := NewValidator(nil, nil, provider, mockMetricsObserver, nil, nil, nil, emptyConfig, nil, false)

			healthConditionMap := make(map[crd.CRD]metav1.Condition)
			for i, c := range tc.crds {
				if found := k8sapimeta.FindStatusCondition(tc.conditions[i], conditions.HealthCondition); found != nil {
					healthConditionMap[c] = *found
				}
			}
			validator.observeMetrics(tc.crds, healthConditionMap)
			mockMetricsObserver.AssertNumberOfCalls(t, "ObserveNpcHealth", len(tc.wantHealthMap))
			for crdType, healthData := range tc.wantHealthMap {
				mockMetricsObserver.AssertCalled(t, "ObserveNpcHealth", crdType, healthData.healthy, healthData.unhealthy)
			}
		})
	}
}
