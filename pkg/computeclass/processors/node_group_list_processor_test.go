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

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestCrdNodeGroupListProcessor(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultMachineType := "n1-standard-1"
	defaultSpot := true

	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsDataplaneV2Enabled").Return(true)

	testCases := []struct {
		name           string
		nodegroups     []cloudprovider.NodeGroup
		crds           []crd.CRD
		wantNodegroups []cloudprovider.NodeGroup
	}{
		{
			name: "No crds",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetGceRefName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetGceRefName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
			},
		},
		{
			name: "No Nodegroups",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
		},
		{
			name: "default case",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetGceRefName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from testCrdLabel:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetGceRefName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 2 from testCrdLabel:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetGceRefName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from testCrdLabel:crd-object-2
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-7").
					SetGceRefName("nodepool-7").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "default"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 3 from testCrdLabel:default
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-5").
					SetGceRefName("nodepool-5").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-2": ""},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't match to any crd
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-2"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-3"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-4"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("default"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-5"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-6"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-7", "nodepool-8", "nodepool-9"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetGceRefName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from testCrdLabel:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetGceRefName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 2 from testCrdLabel:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetGceRefName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from testCrdLabel:crd-object-2
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-7").
					SetGceRefName("nodepool-7").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "default"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 3 from testCrdLabel:default
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-5").
					SetGceRefName("nodepool-5").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-2": ""},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't match to any crd
			},
		},
		{
			name: "Grouped rules",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetGceRefName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetGceRefName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetGceRefName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithGroupedRules([][]rules.Rule{
						{
							rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
							rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-2"})),
						},
						{rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-3"}))},
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetGceRefName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetGceRefName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetGceRefName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					SetGkeManager(gkeManager).
					Build(),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister(tc.crds)
			nodegroupListProcessor := newMockNodeGroupListProcessor()
			mockMetricsObserver := computeclass.NewMockMetrics()
			mockProvider := computeclass.NewMockGKEProvider(nil, machinetypes.N1)
			processor := NewNodeGroupListProcessor(mockLister, nodegroupListProcessor, mockMetricsObserver, mockProvider)
			node := apiv1.Node{}
			node.Labels = map[string]string{}
			for _, ng := range tc.nodegroups {
				gkeManager.On("GetMigTemplateNodeInfo", ng).Return(framework.NewTestNodeInfo(&node), nil)
			}
			actual, _, _ := processor.Process(nil, tc.nodegroups, nil, nil)
			assert.ElementsMatch(t, actual, tc.wantNodegroups)

			mockMetricsObserver.AssertNotCalled(t, "ObserveInvalidNpcScaleUpOrder")
		})
	}
}

func TestCRDBipackingLimiter(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultMachineType := "n1-standard-1"
	defaultSpot := true

	testCases := []struct {
		name                string
		nodegroupsPool      [][]cloudprovider.NodeGroup
		wantNodegroups      []cloudprovider.NodeGroup
		expansionOptions    []expander.Option
		processedNodeGroups []string
		stopBinpacking      bool
	}{
		{
			name: "stop binpacking - false",
			nodegroupsPool: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-1").
						SetGceRefName("nodepool-1").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "default"},
							MachineType: defaultMachineType,
							Spot:        defaultSpot}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-2").
						SetGceRefName("nodepool-2").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "default"},
							MachineType: defaultMachineType,
							Spot:        defaultSpot}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-3").
						SetGceRefName("nodepool-3").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{"crd-2": "default"},
							MachineType: defaultMachineType,
							Spot:        defaultSpot}).
						Build(),
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetGceRefName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "default"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetGceRefName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "default"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetGceRefName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-2": "default"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
			},
			expansionOptions: createMockExpansionOptions(1),
			processedNodeGroups: []string{
				"https://www.googleapis.com/compute/v1/projects//zones//instanceGroups/nodepool-1",
				"https://www.googleapis.com/compute/v1/projects//zones//instanceGroups/nodepool-2",
			},
			stopBinpacking: false,
		},
		{
			name: "stop binpacking - true",
			nodegroupsPool: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-1").
						SetGceRefName("nodepool-1").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "default"},
							MachineType: defaultMachineType,
							Spot:        defaultSpot}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-2").
						SetGceRefName("nodepool-2").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "default"},
							MachineType: defaultMachineType,
							Spot:        defaultSpot}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-3").
						SetGceRefName("nodepool-3").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{"crd-2": "default"},
							MachineType: defaultMachineType,
							Spot:        defaultSpot}).
						Build(),
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetGceRefName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "default"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetGceRefName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "default"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetGceRefName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-2": "default"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
			},
			expansionOptions: createMockExpansionOptions(1),
			processedNodeGroups: []string{
				"https://www.googleapis.com/compute/v1/projects//zones//instanceGroups/nodepool-1",
				"https://www.googleapis.com/compute/v1/projects//zones//instanceGroups/nodepool-2",
				"https://www.googleapis.com/compute/v1/projects//zones//instanceGroups/nodepool-3",
			},
			stopBinpacking: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister([]crd.CRD{})
			nodegroupListProcessor := newMockNodeGroupListProcessor()
			mockMetricsObserver := computeclass.NewMockMetrics()
			mockProvider := computeclass.NewMockGKEProvider(nil, machinetypes.N1)
			processor := NewNodeGroupListProcessor(mockLister, nodegroupListProcessor, mockMetricsObserver, mockProvider)

			nodegroups := processor.flattenNodeGroupsByBucket(tc.nodegroupsPool)
			assert.ElementsMatch(t, nodegroups, tc.wantNodegroups)

			for _, id := range tc.processedNodeGroups {
				processor.MarkProcessed(nil, id)
			}
			res := processor.StopBinpacking(nil, tc.expansionOptions)
			assert.Equal(t, res, tc.stopBinpacking)

			mockMetricsObserver.AssertNotCalled(t, "ObserveInvalidNpcScaleUpOrder")
		})
	}
}

func TestFlattenAndMarkLastNodeGroupInPriority(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultMachineType := "n1-standard-1"
	defaultSpot := true

	testCases := []struct {
		name           string
		nodegroupsPool [][]cloudprovider.NodeGroup
		wantNodegroups []cloudprovider.NodeGroup
	}{
		{
			name: "Empty nodegroups",
		},
		{
			name: "default case",
			nodegroupsPool: [][]cloudprovider.NodeGroup{
				{
					// Empty priority
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-1").
						SetGceRefName("nodepool-1").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: defaultMachineType,
							Spot:        defaultSpot}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-2").
						SetGceRefName("nodepool-2").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{"crd-2": "crd-object-1"},
							MachineType: defaultMachineType,
							Spot:        defaultSpot}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-3").
						SetGceRefName("nodepool-3").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "default"},
							MachineType: defaultMachineType,
							Spot:        defaultSpot}).
						Build(),
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetGceRefName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetGceRefName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{"crd-2": "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetGceRefName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "default"},
						MachineType: defaultMachineType,
						Spot:        defaultSpot}).
					Build(),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister([]crd.CRD{})
			nodegroupListProcessor := newMockNodeGroupListProcessor()
			mockMetricsObserver := computeclass.NewMockMetrics()
			mockProvider := computeclass.NewMockGKEProvider(nil, machinetypes.N1)
			processor := NewNodeGroupListProcessor(mockLister, nodegroupListProcessor, mockMetricsObserver, mockProvider)
			nodegroups := processor.flattenNodeGroupsByBucket(tc.nodegroupsPool)
			for i, ng := range nodegroups {
				assert.Equal(t, ng, tc.wantNodegroups[i])
			}

			mockMetricsObserver.AssertNotCalled(t, "ObserveInvalidNpcScaleUpOrder")
		})
	}
}

func TestProcessorE2E(t *testing.T) {
	testCrdLabel := "test-crd-label"
	allNodepools := []string{"np1", "np2", "np3", "np4", "np5", "np6"}
	matchingNodepools := []string{"np1", "np2", "np3", "np4", "np5"}

	crds := []crd.CRD{
		crd.NewTestCrd(crd.WithLabel(testCrdLabel),
			crd.WithName("crd-object-1"),
			crd.WithRules([]rules.Rule{
				rules.NewRule(rules.WithNodePoolsRule([]string{"np1", "np2"})),
				rules.NewRule(rules.WithNodePoolsRule([]string{"np3", "np4"})),
				rules.NewRule(rules.WithNodePoolsRule([]string{"np5"})),
			}), crd.WithAutoprovisioningEnabled()),
	}

	var nodeGroups []cloudprovider.NodeGroup
	for _, np := range allNodepools {
		nodeGroups = append(nodeGroups, gke.NewTestGkeMigBuilder().
			SetNodePoolName(np).
			SetGceRefName(np).
			SetSpec(&gkeclient.NodePoolSpec{
				Labels:      map[string]string{testCrdLabel: "crd-object-1"},
				MachineType: "n1-standard-1"}).
			Build())
	}

	var wantNodeGroups []cloudprovider.NodeGroup
	for _, np := range matchingNodepools {
		wantNodeGroups = append(wantNodeGroups, gke.NewTestGkeMigBuilder().
			SetNodePoolName(np).
			SetGceRefName(np).
			SetSpec(&gkeclient.NodePoolSpec{
				Labels:      map[string]string{testCrdLabel: "crd-object-1"},
				MachineType: "n1-standard-1"}).
			Build())
	}

	mockLister := lister.NewMockCrdLister(crds)
	mockLister.SetCrdLabel(testCrdLabel)
	nodegroupListProcessor := newMockNodeGroupListProcessor()
	mockMetricsObserver := computeclass.NewMockMetrics()
	mockProvider := computeclass.NewMockGKEProvider(nil, machinetypes.N1)
	processor := NewNodeGroupListProcessor(mockLister, nodegroupListProcessor, mockMetricsObserver, mockProvider)

	actual, _, _ := processor.Process(nil, nodeGroups, nil, nil)
	assert.ElementsMatch(t, actual, wantNodeGroups)

	expansionOptionCount := []int{0, 0, 1, 1, 2}
	wantStopBinpacking := []bool{false, false, false, true, true}
	for i, ng := range wantNodeGroups {
		processor.MarkProcessed(nil, ng.Id())
		res := processor.StopBinpacking(nil, createMockExpansionOptions(expansionOptionCount[i]))
		assert.Equal(t, wantStopBinpacking[i], res)
	}

	mockMetricsObserver.AssertNotCalled(t, "ObserveInvalidNpcScaleUpOrder")
}

func TestInvalidNpcScaleUpOrder(t *testing.T) {
	testCrdLabel := "test-crd-label"
	testCrd := crd.NewTestCrd(crd.WithLabel(testCrdLabel),
		crd.WithName("crd-object-1"),
		crd.WithRules([]rules.Rule{
			rules.NewRule(rules.WithNodePoolsRule([]string{"np-1-1", "np-1-2"})),
			rules.NewRule(rules.WithNodePoolsRule([]string{"np-2-1", "np-2-2"})),
		}), crd.WithAutoprovisioningEnabled(), crd.WithScaleUpAnyway())

	testCases := []struct {
		name             string
		skippedNodepools []string
		nodepoolOrder    []string
		wantErrors       int
	}{
		{
			name:          "Single matching node group, no error",
			nodepoolOrder: []string{"np-1-1"},
		},
		{
			name:          "Single non-matching node group, no error",
			nodepoolOrder: []string{"np-other"},
		},
		{
			name:          "Matching node groups, correct order, no error",
			nodepoolOrder: []string{"np-1-1", "np-1-2", "np-2-1", "np-2-2"},
		},
		{
			name:          "Non-matching node groups, correct order, no error",
			nodepoolOrder: []string{"np-another", "np-one", "np-bites", "np-the"},
		},
		{
			name:          "Mixed node groups, correct order, no error",
			nodepoolOrder: []string{"np-1-1", "np-1-2", "np-2-1", "np-2-2", "np-dust"},
		},
		{
			name:          "Matching node groups, incorrect order, single error",
			nodepoolOrder: []string{"np-1-1", "np-2-1", "np-1-2", "np-2-2"},
			wantErrors:    1,
		},
		{
			name:          "Mixed node groups, incorrect order, two error",
			nodepoolOrder: []string{"np-freddy", "np-1-1", "np-1-2", "np-mercury", "np-2-1", "np-2-2"},
			wantErrors:    2,
		},
		{
			name:             "Skipping lower priority node groups don't cause errors",
			skippedNodepools: []string{"np-2-1"},
			nodepoolOrder:    []string{"np-1-1", "np-1-2", "np-2-2"},
		},
		{
			name:             "Skipping higher priority node groups don't cause errors",
			skippedNodepools: []string{"np-1-2"},
			nodepoolOrder:    []string{"np-1-1", "np-2-1", "np-2-2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister([]crd.CRD{testCrd})
			mockLister.SetCrdLabel(testCrdLabel)
			nodegroupListProcessor := newMockNodeGroupListProcessor()
			mockMetricsObserver := computeclass.NewMockMetrics()
			mockProvider := computeclass.NewMockGKEProvider(nil, machinetypes.N1)
			processor := NewNodeGroupListProcessor(mockLister, nodegroupListProcessor, mockMetricsObserver, mockProvider)

			var skippedNodegroups []cloudprovider.NodeGroup
			for _, np := range tc.skippedNodepools {
				skippedNodegroups = append(skippedNodegroups,
					gke.NewTestGkeMigBuilder().
						SetNodePoolName(np).
						SetGceRefName(np).
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "n1-standard-1"}).
						Build(),
				)
			}
			var processedNodegroups []cloudprovider.NodeGroup
			for _, np := range tc.nodepoolOrder {
				processedNodegroups = append(processedNodegroups,
					gke.NewTestGkeMigBuilder().
						SetNodePoolName(np).
						SetGceRefName(np).
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "n1-standard-1"}).
						Build(),
				)
			}

			// Process and order nodegroups
			_, _, _ = processor.Process(nil, append(skippedNodegroups, processedNodegroups...), nil, nil)

			// Skip some nodepools
			for _, ng := range skippedNodegroups {
				processor.MarkProcessed(nil, ng.Id())
			}
			// Mark nodegroups in provided order
			for _, ng := range processedNodegroups {
				processor.MarkProcessed(nil, ng.Id())
				processor.StopBinpacking(nil, nil)
			}

			// Check the errors were handled as expected
			mockMetricsObserver.AssertNumberOfCalls(t, "ObserveInvalidNpcScaleUpOrder", tc.wantErrors)
		})
	}
}

func TestInvalidNpcScaleUpOrderGroupedRules(t *testing.T) {
	testCrdLabel := "test-crd-label"
	testCrd := crd.NewTestCrd(crd.WithLabel(testCrdLabel),
		crd.WithName("crd-object-1"),
		crd.WithGroupedRules([][]rules.Rule{
			{
				rules.NewRule(rules.WithNodePoolsRule([]string{"np-1-1"})),
				rules.NewRule(rules.WithNodePoolsRule([]string{"np-1-2"})),
			},
			{
				rules.NewRule(rules.WithNodePoolsRule([]string{"np-2-1"})),
				rules.NewRule(rules.WithNodePoolsRule([]string{"np-2-2"})),
			},
		}), crd.WithAutoprovisioningEnabled(), crd.WithScaleUpAnyway())

	testCases := []struct {
		name             string
		skippedNodepools []string
		nodepoolOrder    []string
		wantErrors       int
	}{
		{
			name:          "Matching node groups, correct order, no error",
			nodepoolOrder: []string{"np-1-1", "np-1-2", "np-2-1", "np-2-2"},
		},
		{
			name:          "Matching node groups, incorrect order, single error",
			nodepoolOrder: []string{"np-1-1", "np-2-1", "np-1-2", "np-2-2"},
			wantErrors:    1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister([]crd.CRD{testCrd})
			mockLister.SetCrdLabel(testCrdLabel)
			nodegroupListProcessor := newMockNodeGroupListProcessor()
			mockMetricsObserver := computeclass.NewMockMetrics()
			mockProvider := computeclass.NewMockGKEProvider(nil, machinetypes.N1)
			processor := NewNodeGroupListProcessor(mockLister, nodegroupListProcessor, mockMetricsObserver, mockProvider)

			var skippedNodegroups []cloudprovider.NodeGroup
			for _, np := range tc.skippedNodepools {
				skippedNodegroups = append(skippedNodegroups,
					gke.NewTestGkeMigBuilder().
						SetNodePoolName(np).
						SetGceRefName(np).
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "n1-standard-1"}).
						Build(),
				)
			}
			var processedNodegroups []cloudprovider.NodeGroup
			for _, np := range tc.nodepoolOrder {
				processedNodegroups = append(processedNodegroups,
					gke.NewTestGkeMigBuilder().
						SetNodePoolName(np).
						SetGceRefName(np).
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "n1-standard-1"}).
						Build(),
				)
			}

			// Process and order nodegroups
			_, _, _ = processor.Process(nil, append(skippedNodegroups, processedNodegroups...), nil, nil)

			// Skip some nodepools
			for _, ng := range skippedNodegroups {
				processor.MarkProcessed(nil, ng.Id())
			}
			// Mark nodegroups in provided order
			for _, ng := range processedNodegroups {
				processor.MarkProcessed(nil, ng.Id())
				processor.StopBinpacking(nil, nil)
			}

			// Check the errors were handled as expected
			mockMetricsObserver.AssertNumberOfCalls(t, "ObserveInvalidNpcScaleUpOrder", tc.wantErrors)
		})
	}
}

func TestProcessorE2EGroupedRules(t *testing.T) {
	testCrdLabel := "test-crd-label"
	allNodepools := []string{"np1", "np2", "np3", "np4", "np5", "np6"}
	matchingNodepools := []string{"np1", "np2", "np3", "np4", "np5"}

	crds := []crd.CRD{
		crd.NewTestCrd(crd.WithLabel(testCrdLabel),
			crd.WithName("crd-object-1"),
			crd.WithGroupedRules([][]rules.Rule{
				{
					rules.NewRule(rules.WithNodePoolsRule([]string{"np1"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"np2"})),
				},
				{
					rules.NewRule(rules.WithNodePoolsRule([]string{"np3"})),
					rules.NewRule(rules.WithNodePoolsRule([]string{"np4"})),
				},
				{
					rules.NewRule(rules.WithNodePoolsRule([]string{"np5"})),
				},
			}), crd.WithAutoprovisioningEnabled()),
	}

	var nodeGroups []cloudprovider.NodeGroup
	for _, np := range allNodepools {
		nodeGroups = append(nodeGroups, gke.NewTestGkeMigBuilder().
			SetNodePoolName(np).
			SetGceRefName(np).
			SetSpec(&gkeclient.NodePoolSpec{
				Labels:      map[string]string{testCrdLabel: "crd-object-1"},
				MachineType: "n1-standard-1"}).
			Build())
	}

	var wantNodeGroups []cloudprovider.NodeGroup
	for _, np := range matchingNodepools {
		wantNodeGroups = append(wantNodeGroups, gke.NewTestGkeMigBuilder().
			SetNodePoolName(np).
			SetGceRefName(np).
			SetSpec(&gkeclient.NodePoolSpec{
				Labels:      map[string]string{testCrdLabel: "crd-object-1"},
				MachineType: "n1-standard-1"}).
			Build())
	}

	mockLister := lister.NewMockCrdLister(crds)
	mockLister.SetCrdLabel(testCrdLabel)
	nodegroupListProcessor := newMockNodeGroupListProcessor()
	mockMetricsObserver := computeclass.NewMockMetrics()
	mockProvider := computeclass.NewMockGKEProvider(nil, machinetypes.N1)
	processor := NewNodeGroupListProcessor(mockLister, nodegroupListProcessor, mockMetricsObserver, mockProvider)

	actual, _, _ := processor.Process(nil, nodeGroups, nil, nil)
	assert.ElementsMatch(t, actual, wantNodeGroups)

	expansionOptionCount := []int{0, 0, 1, 1, 2}
	wantStopBinpacking := []bool{false, false, false, true, true}
	for i, ng := range wantNodeGroups {
		processor.MarkProcessed(nil, ng.Id())
		res := processor.StopBinpacking(nil, createMockExpansionOptions(expansionOptionCount[i]))
		assert.Equal(t, wantStopBinpacking[i], res)
	}

	mockMetricsObserver.AssertNotCalled(t, "ObserveInvalidNpcScaleUpOrder")
}

func createMockExpansionOptions(total int) []expander.Option {
	options := []expander.Option{}
	for i := 0; i < total; i++ {
		options = append(options, expander.Option{
			Debug: "debug",
		})
	}
	return options
}

type mockNodeGroupListProcessor struct {
}

func newMockNodeGroupListProcessor() *mockNodeGroupListProcessor {
	return &mockNodeGroupListProcessor{}
}

func (p *mockNodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	return nodeGroups, nodeInfos, nil
}

func (m *mockNodeGroupListProcessor) CleanUp() {
}
