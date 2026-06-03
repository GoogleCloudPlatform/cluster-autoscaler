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

package processors

import (
	"testing"

	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestCrdScaleUpStatusProcessor(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultTestCrd := "default-crd-test"
	testCrdType := "TEST"
	testCrdType2 := "TEST2"
	cccCrdType := "CCC"
	npcCrdType := "NPC"

	type wantResultData struct {
		wantRuleIndex int
		wantCount     int
		wantCrdType   string
	}

	testCases := []struct {
		name             string
		crds             []crd.CRD
		scaleUpStatus    *status.ScaleUpStatus
		wantResult       []wantResultData
		noDefaultCrdName bool
	}{
		{
			name: "no scaleup info",
		},
		{
			name: "crd not present",
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
						CurrentSize: 1,
						NewSize:     8,
						MaxSize:     10,
					},
				},
			},
		},
		{
			name: "ScaleUpInfos not present",
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
			},
		},
		{
			name: "scale up with no default crd name defined for lister",
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group:       buildMigWithDefaultCrd(),
						CurrentSize: 3,
						NewSize:     9,
						MaxSize:     10,
					},
				},
			},
			noDefaultCrdName: true,
		},
		{
			name: "empty ScaleUpInfos",
			scaleUpStatus: &status.ScaleUpStatus{
				Result:       status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{},
			},
		},
		{
			name: "scaleUpStatus not successful",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpError,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
						CurrentSize: 3,
						NewSize:     7,
						MaxSize:     10,
					},
				},
			},
		},
		{
			name: "scaleUpAnyway case, test crd",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group:       buildMigWithDefaultCrd(),
						CurrentSize: 3,
						NewSize:     7,
						MaxSize:     10,
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 1,
					wantCount:     4,
					wantCrdType:   testCrdType,
				},
			},
		},
		{
			name: "scaleUpAnyway case, npc crd",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(npcCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group:       buildMigWithDefaultCrd(),
						CurrentSize: 3,
						NewSize:     7,
						MaxSize:     10,
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 1,
					wantCount:     4,
					wantCrdType:   npcCrdType,
				},
			},
		},
		{
			name: "scaleUpAnyway case, ccc crd",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(cccCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group:       buildMigWithDefaultCrd(),
						CurrentSize: 3,
						NewSize:     7,
						MaxSize:     10,
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 1,
					wantCount:     4,
					wantCrdType:   cccCrdType,
				},
			},
		},
		{
			name: "default",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType2),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{}), crd.WithScaleUpAnyway()),
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetSpec(&gkeclient.NodePoolSpec{
									Labels:      map[string]string{testCrdLabel: "crd-object-1"},
									MachineType: "machine-type",
									Spot:        true}).
							Build(), // matches rule[1] crd-1:crd-object-1.
						CurrentSize: 3,
						NewSize:     7,
						MaxSize:     10,
					},
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetSpec(&gkeclient.NodePoolSpec{
									Labels:      map[string]string{testCrdLabel: "crd-object-2"},
									MachineType: "machine-type",
									Spot:        true}).
							Build(), // crd crd-1:crd-object-2 not present
						CurrentSize: 1,
						NewSize:     8,
						MaxSize:     10,
					},
					{
						Group:       buildMigWithDefaultCrd(), // matches crd-1:default with no rule, increase 0th rule metrics.
						CurrentSize: 1,
						NewSize:     8,
						MaxSize:     10,
					},
					{
						Group: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-3").
							SetSpec(&gkeclient.NodePoolSpec{
									Labels:      map[string]string{testCrdLabel: "crd-object-1"},
									MachineType: "machine-type",
									Spot:        true}).
							Build(), // matches no rule, incrase 2nd rule metrics.
						CurrentSize: 5,
						NewSize:     8,
						MaxSize:     10,
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 0,
					wantCount:     7,
					wantCrdType:   testCrdType,
				},
				{
					wantRuleIndex: 1,
					wantCount:     4,
					wantCrdType:   testCrdType2,
				},
				{
					wantRuleIndex: 2,
					wantCount:     3,
					wantCrdType:   testCrdType2,
				},
			},
		},
		{
			name: "scaleUp with test crd",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName(defaultTestCrd),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			scaleUpStatus: &status.ScaleUpStatus{
				Result: status.ScaleUpSuccessful,
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group:       buildMigWithDefaultCrd(),
						CurrentSize: 3,
						NewSize:     9,
						MaxSize:     10,
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 1,
					wantCount:     6,
					wantCrdType:   testCrdType,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister(tc.crds)
			mockLister.SetCrdLabel(testCrdLabel)
			if !tc.noDefaultCrdName {
				mockLister.SetDefaultCrdName(defaultTestCrd)
			}
			mockMetricsObserver := computeclass.NewMockMetrics()
			mockProvider := NewMockCloudProvider()
			mockProvider.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)
			mockProvider.On("IsAutopilotEnabled").Return(false)
			processor := NewCrdScaleUpStatusProcessor(mockLister, mockProvider, mockMetricsObserver)
			processor.Process(nil, tc.scaleUpStatus)
			mockMetricsObserver.AssertNumberOfCalls(t, "IncreaseScaledUpNodesPerRule", len(tc.wantResult))
			for _, wantResult := range tc.wantResult {
				mockMetricsObserver.AssertCalled(t, "IncreaseScaledUpNodesPerRule", wantResult.wantRuleIndex, wantResult.wantCount, wantResult.wantCrdType)
			}
		})
	}
}
