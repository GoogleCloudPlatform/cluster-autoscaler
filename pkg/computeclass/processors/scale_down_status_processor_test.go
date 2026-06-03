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

	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestCrdScaleDownStatusProcessor(t *testing.T) {
	testCrdLabel := "test-crd-label"
	testCrdType := "TEST"
	testCrdType2 := "TEST2"
	cccCrdType := "CCC"
	npcCrdType := "NPC"

	type wantResultData struct {
		wantRuleIndex int
		wantCrdType   string
	}

	testCases := []struct {
		name       string
		crds       []crd.CRD
		status     *status.ScaleDownStatus
		wantResult []wantResultData
	}{
		{
			name: "nil scale down status",
		},
		{
			name: "ScaledDownNodes nil",
			status: &status.ScaleDownStatus{
				Result:          status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: nil,
			},
		},
		{
			name: "ScaledDownNodes empty",
			status: &status.ScaleDownStatus{
				Result:          status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{},
			},
		},
		{
			name: "crd not present",
			status: &status.ScaleDownStatus{
				Result: status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{
					{
						NodeGroup: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
					},
				},
			},
		},
		{
			name: "scale down node with default crd",
			status: &status.ScaleDownStatus{
				Result: status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{
					{
						NodeGroup: buildMigWithDefaultCrd(),
					},
				},
			},
		},
		{
			name: "both nodepools matches to same index in different crds",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-2"),
					crd.WithCrdType(testCrdType2),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway()),
			},
			status: &status.ScaleDownStatus{
				Result: status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{
					{
						NodeGroup: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
					},
					{
						NodeGroup: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-2"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 0,
					wantCrdType:   testCrdType,
				},
				{
					wantRuleIndex: 0,
					wantCrdType:   testCrdType2,
				},
			},
		},
		{
			name: "default",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway()),
			},
			status: &status.ScaleDownStatus{
				Result: status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{
					{
						NodeGroup: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
					},
					{
						NodeGroup: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
					},
					{
						NodeGroup: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-3").
							SetSpec(&gkeclient.NodePoolSpec{
									Labels:      map[string]string{testCrdLabel: "crd-object-2"},
									MachineType: "machine-type",
									Spot:        true}).
							Build(), // crd not present - not counted
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 0,
					wantCrdType:   testCrdType,
				},
				{
					wantRuleIndex: 1,
					wantCrdType:   testCrdType,
				},
			},
		},
		{
			name: "nodepool with test crd",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(testCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			status: &status.ScaleDownStatus{
				Result: status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{
					{
						NodeGroup: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 0,
					wantCrdType:   testCrdType,
				},
			},
		},
		{
			name: "nodepool with npc crd",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(npcCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			status: &status.ScaleDownStatus{
				Result: status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{
					{
						NodeGroup: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 0,
					wantCrdType:   npcCrdType,
				},
			},
		},
		{
			name: "nodepool with ccc crd",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithCrdType(cccCrdType),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
					}), crd.WithScaleUpAnyway()),
			},
			status: &status.ScaleDownStatus{
				Result: status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{
					{
						NodeGroup: gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: "machine-type",
								Spot:        true}).
							Build(),
					},
				},
			},
			wantResult: []wantResultData{
				{
					wantRuleIndex: 0,
					wantCrdType:   cccCrdType,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister(tc.crds)
			mockLister.SetCrdLabel(testCrdLabel)
			mockMetricsObserver := computeclass.NewMockMetrics()
			mockProvider := NewMockCloudProvider()
			mockProvider.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)
			mockProvider.On("IsAutopilotEnabled").Return(false)
			processor := NewCrdScaleDownStatusProcessor(mockLister, mockProvider, mockMetricsObserver)
			processor.Process(nil, tc.status)
			mockMetricsObserver.AssertNumberOfCalls(t, "IncreaseScaledDownNodesPerRule", len(tc.wantResult))
			for _, wantResult := range tc.wantResult {
				mockMetricsObserver.AssertCalled(t, "IncreaseScaledDownNodesPerRule", wantResult.wantRuleIndex, wantResult.wantCrdType)
			}
		})
	}
}
