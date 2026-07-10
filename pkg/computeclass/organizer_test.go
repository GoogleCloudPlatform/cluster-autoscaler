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

package computeclass

import (
	"testing"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestOrganizeByRules(t *testing.T) {
	testCrdLabel := "test-crd"
	defaultMachineType := "n1-standard-1"
	defaultNodepoolName := "nodepool-name"
	defaultFamily := "n1"
	a2family := "a2"
	trueSpot := true
	falseSpot := false
	podFamily := "general-purpose"
	defaultMinCores := 1
	defaultMinMemoryGB := 3
	acceleratorType := "nvidia-tesla-t4"
	accelerators := []*gke_api_beta.AcceleratorConfig{{
		AcceleratorCount: 1,
		AcceleratorType:  acceleratorType,
	}}

	testCases := []struct {
		name                       string
		nodeGroups                 []cloudprovider.NodeGroup
		rules                      []rules.Rule
		crd                        crd.CRD
		isEkWithinPodFamilyEnabled bool
		wantGroups                 [][]cloudprovider.NodeGroup
	}{
		{
			name: "no node groups",
			rules: []rules.Rule{
				rules.NewMachineSpecRule(&defaultFamily, &trueSpot, &defaultMinCores, &defaultMinMemoryGB),
				rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
			},
			crd: crd.NewTestCrd(crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&defaultFamily, &trueSpot, &defaultMinCores, &defaultMinMemoryGB),
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}),
				crd.WithScaleUpAnyway(),
			),
			wantGroups: [][]cloudprovider.NodeGroup{},
		},
		{
			name: "no rules",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName(defaultNodepoolName).
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
					}).
					Build(),
			},
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules(nil),
				crd.WithScaleUpAnyway(),
			),
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName(defaultNodepoolName).
						SetSpec(&gkeclient.NodePoolSpec{
							Labels: map[string]string{testCrdLabel: "crd-object-1"},
						}).
						Build(),
				},
			},
		},
		{
			name: "unknown rules",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName(defaultNodepoolName).
					SetSpec(&gkeclient.NodePoolSpec{
						Labels: map[string]string{testCrdLabel: "crd-object-1"},
					}).
					Build(),
			},
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{nil, nil}),
				crd.WithScaleUpAnyway(),
			),
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName(defaultNodepoolName).
						SetSpec(&gkeclient.NodePoolSpec{
							Labels: map[string]string{testCrdLabel: "crd-object-1"},
						}).
						Build(),
				},
			},
		},
		{
			name: "node groups are organized by rules - default case with scale up anyway",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName(defaultNodepoolName).
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					Build(), // matches 3rd and 4th PR.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "n1-standard-2",
					}).
					Build(), // matches 2nd PR only.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "a2-ultragpu-1g",
						Spot:        trueSpot,
					}).
					Build(), // matches 1st and 4th PR.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-4").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					Build(), // matches 3rd and 6th PR.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-5").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "a2-ultragpu-1g",
					}).
					Build(), // matches 4th PR only.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-6").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "totally-diff-machine",
					}).
					Build(), // matches 5th PR only.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-7").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "totally-diff-machine",
					}).
					Build(), // matches no PR.
			},
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-3"})),
					rules.NewMachineSpecRule(&defaultFamily, &falseSpot, nil, nil),
					rules.NewMachineSpecRule(&defaultFamily, &trueSpot, &defaultMinCores, &defaultMinMemoryGB),
					rules.NewRule(rules.WithNodePoolsRule([]string{defaultNodepoolName, "nodepool-8", "nodepool-8"})),
					rules.NewMachineSpecRule(&a2family, nil, nil, nil),
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-4", "nodepool-6"})),
					nil,
				}),
				crd.WithScaleUpAnyway(),
			),
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-3").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "a2-ultragpu-1g",
							Spot:        trueSpot,
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-2").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "n1-standard-2",
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName(defaultNodepoolName).
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: defaultMachineType,
							Spot:        trueSpot,
						}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-4").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: defaultMachineType,
							Spot:        trueSpot,
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-5").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "a2-ultragpu-1g",
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-6").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "totally-diff-machine",
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-7").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "totally-diff-machine",
						}).
						Build(),
				},
			},
		},
		{
			name: "node groups are organized by rules - default case with scale up anyway disabled",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName(defaultNodepoolName).
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					Build(), // matches 3rd and 4th PR.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "n1-standard-2",
					}).
					Build(), // matches 2nd PR only.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "a2-ultragpu-1g",
						Spot:        trueSpot,
					}).
					Build(), // matches 1st and 4th PR.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-4").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					Build(), // matches 3rd and 6th PR.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-5").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "a2-ultragpu-1g",
					}).
					Build(), // matches 4th PR only.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-6").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "totally-diff-machine",
					}).
					Build(), // matches 5th PR only.
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-7").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "totally-diff-machine",
					}).
					Build(), // matches no PR.
			},
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-3"})),
					rules.NewMachineSpecRule(&defaultFamily, &falseSpot, nil, nil),
					rules.NewMachineSpecRule(&defaultFamily, &trueSpot, &defaultMinCores, &defaultMinMemoryGB),
					rules.NewRule(rules.WithNodePoolsRule([]string{defaultNodepoolName, "nodepool-8", "nodepool-8"})),
					rules.NewMachineSpecRule(&a2family, nil, nil, nil),
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-4", "nodepool-6"})),
					nil,
				}),
			),
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-3").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "a2-ultragpu-1g",
							Spot:        trueSpot,
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-2").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "n1-standard-2",
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName(defaultNodepoolName).
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: defaultMachineType,
							Spot:        trueSpot,
						}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-4").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: defaultMachineType,
							Spot:        trueSpot,
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-5").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "a2-ultragpu-1g",
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-6").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "totally-diff-machine",
						}).
						Build(),
				},
			},
		},
		{
			name: "flex start over on-demand rules",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName(defaultNodepoolName).
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: defaultMachineType,
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("flex-gpu-np").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:       map[string]string{testCrdLabel: "crd-object-1"},
						MachineType:  defaultMachineType,
						FlexStart:    true,
						Accelerators: accelerators,
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("std-gpu-np").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:       map[string]string{testCrdLabel: "crd-object-1"},
						MachineType:  defaultMachineType,
						Accelerators: accelerators,
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("std-gpu-np-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:       map[string]string{testCrdLabel: "crd-object-1"},
						MachineType:  defaultMachineType,
						Accelerators: accelerators,
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("std-gpu-np-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:       map[string]string{testCrdLabel: "crd-object-1"},
						MachineType:  defaultMachineType,
						Accelerators: accelerators,
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("flex-gpu-np-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:       map[string]string{testCrdLabel: "crd-object-1"},
						MachineType:  defaultMachineType,
						FlexStart:    true,
						Accelerators: accelerators,
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("flex-gpu-np-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:       map[string]string{testCrdLabel: "crd-object-1"},
						MachineType:  defaultMachineType,
						FlexStart:    true,
						Accelerators: accelerators,
					}).
					Build(),
			},
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithFlexStartRule(true, nil),
						rules.WithMachineTypeRule(&defaultMachineType),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							Config: machinetypes.GpuConfig{
								GpuType: acceleratorType,
							},
							Count:            1,
							PhysicalGPUCount: 1,
						})),
					rules.NewRule(
						rules.WithMachineTypeRule(&defaultMachineType),
						rules.WithGpuRule(&machinetypes.GpuRequest{
							Config: machinetypes.GpuConfig{
								GpuType: acceleratorType,
							},
							Count:            1,
							PhysicalGPUCount: 1,
						})),
				}),
			),
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("flex-gpu-np").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:       map[string]string{testCrdLabel: "crd-object-1"},
							MachineType:  defaultMachineType,
							FlexStart:    true,
							Accelerators: accelerators,
						}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("flex-gpu-np-2").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:       map[string]string{testCrdLabel: "crd-object-1"},
							MachineType:  defaultMachineType,
							FlexStart:    true,
							Accelerators: accelerators,
						}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("flex-gpu-np-3").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:       map[string]string{testCrdLabel: "crd-object-1"},
							MachineType:  defaultMachineType,
							FlexStart:    true,
							Accelerators: accelerators,
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("std-gpu-np").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:       map[string]string{testCrdLabel: "crd-object-1"},
							MachineType:  defaultMachineType,
							Accelerators: accelerators,
						}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("std-gpu-np-2").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:       map[string]string{testCrdLabel: "crd-object-1"},
							MachineType:  defaultMachineType,
							Accelerators: accelerators,
						}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("std-gpu-np-3").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:       map[string]string{testCrdLabel: "crd-object-1"},
							MachineType:  defaultMachineType,
							Accelerators: accelerators,
						}).
						Build(),
				},
			},
		},
		{
			name: "node groups are organized by rules - prefer EKs over E2s in CCC buckets",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "ek-standard-32",
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "e2-standard-32",
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "e2-standard-32",
						Spot:        trueSpot,
					}).
					Build(),
			},
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&podFamily),
						rules.WithSpotRule(&trueSpot),
					),
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(&podFamily),
					),
				}),
				crd.WithAutopilotManaged(),
			),
			isEkWithinPodFamilyEnabled: true,
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-3").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "e2-standard-32",
							Spot:        trueSpot,
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-1").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "ek-standard-32",
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-2").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "e2-standard-32",
						}).
						Build(),
				},
			},
		},
		{
			name: "node groups are organized by rules - prioritize E4A over N4A over C4A in ARM buckets",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-c4a").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "c4a-standard-32",
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-n4a").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "n4a-standard-32",
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-e4a").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: "e4a-standard-32",
					}).
					Build(),
			},
			crd: crd.NewTestCrd(
				crd.WithLabel(testCrdLabel),
				crd.WithName("crd-object-1"),
				crd.WithRules([]rules.Rule{
					rules.NewRule(
						rules.WithAutopilotModeRule(),
						rules.WithPodFamilyRule(stringPtr("general-purpose-arm")),
					),
				}),
				crd.WithAutopilotManaged(),
			),
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-e4a").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "e4a-standard-32",
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-n4a").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "n4a-standard-32",
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-c4a").
						SetSpec(&gkeclient.NodePoolSpec{
							Labels:      map[string]string{testCrdLabel: "crd-object-1"},
							MachineType: "c4a-standard-32",
						}).
						Build(),
				},
			},
		},
		{
			name: "crd with priorityScore",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-1").SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-standard-4"}).Build(),
				gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-2").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build(),
				gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-3").SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-custom-4-2048"}).Build(),
				gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-4").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-custom-4-2048"}).Build(),
				gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-5").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-4"}).Build(),
			},
			crd: crd.NewTestCrd(
				crd.WithGroupedRules([][]rules.Rule{
					{
						// priorityScore 10
						rules.NewRule(rules.WithMachineTypeRule(stringPtr("e2-standard-4"))), // matches 1st nodegroup.
					},
					{
						// priorityScore 5
						rules.NewRule(rules.WithMachineFamilyRule(stringPtr("n2"))), // matches 2nd and 4th nodegroup.
						rules.NewRule(rules.WithMachineFamilyRule(stringPtr("n1"))), // matches 5th nodegroup.
					},
				}),
			),
			isEkWithinPodFamilyEnabled: true,
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					// priorityScore 10
					gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-1").SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-standard-4"}).Build(),
				},
				{
					// priorityScore 5
					gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-2").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-standard-4"}).Build(),
					gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-4").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n2-custom-4-2048"}).Build(),
					gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-5").SetSpec(&gkeclient.NodePoolSpec{MachineType: "n1-standard-4"}).Build(),
				},
			},
		},
		{
			name: "crd with overlapping rules but different priorityScore",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-1").SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-standard-4"}).Build(),
				gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-1").SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-standard-8"}).Build(),
			},
			crd: crd.NewTestCrd(
				crd.WithGroupedRules([][]rules.Rule{
					{rules.NewRule(rules.WithMachineFamilyRule(stringPtr("e2")))},            // matches both nodegroups, higher priorityScore.
					{rules.NewRule(rules.WithMachineFamilyRule(stringPtr("e2-standard-4")))}, // matches e2-standard-4, lower priorityScore.
				}),
			),
			isEkWithinPodFamilyEnabled: true,
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					// priorityScore 10
					gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-1").SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-standard-4"}).Build(),
					gke.NewTestGkeMigBuilder().SetNodePoolName("nodepool-1").SetSpec(&gkeclient.NodePoolSpec{MachineType: "e2-standard-8"}).Build(),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var crds []crd.CRD
			if tc.crd != nil {
				crds = append(crds, tc.crd)
			}
			mockCrdLister := lister.NewMockCrdLister(crds)
			mockCrdLister.SetCrdLabel(testCrdLabel)
			mockProvider := NewMockGKEProvider(nil, machinetypes.N1)
			if tc.isEkWithinPodFamilyEnabled {
				mockProvider.SetResizableVmWithinPodFamilyEnabled(machinetypes.EK.Name(), true)
			}
			organizerObj := NewOrganizer(mockCrdLister, mockProvider)
			actual := organizerObj.(*organizer).organizeByRules(tc.nodeGroups, tc.crd)
			assert.Equal(t, len(tc.wantGroups), len(actual))
			for i := range actual {
				assert.ElementsMatch(t, tc.wantGroups[i], actual[i])
			}
		})
	}
}

func TestOrganizeByCrds(t *testing.T) {
	testCrdLabel := "test-crd"
	defaultMachineType := "n1-standard-1"
	defaultNodepoolName := "nodepool-name"
	defaultFamily := "n1"
	defaultMinCores := 1
	defaultMinMemoryGB := 3
	trueSpot := true

	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsDataplaneV2Enabled").Return(true)

	testCases := []struct {
		name                      string
		nodeGroups                []cloudprovider.NodeGroup
		crds                      []crd.CRD
		ekAutoprovisioningEnabled bool
		wantGroups                [][][]cloudprovider.NodeGroup
	}{
		{
			name: "no crds and no node groups",
		},
		{
			name: "no node groups",
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithRules([]rules.Rule{
					rules.NewMachineSpecRule(&defaultFamily, &trueSpot, &defaultMinCores, &defaultMinMemoryGB),
					rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-1"})),
				}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
		},
		{
			name: "no crds",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName(defaultNodepoolName).
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					Build(),
			},
			wantGroups: [][][]cloudprovider.NodeGroup{
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName(defaultNodepoolName).
							SetSpec(&gkeclient.NodePoolSpec{
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							Build(),
					},
				},
			},
		},
		{
			name: "node groups are organized by crds - default case",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from crd-1:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 2 from crd-1:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-x").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't match any rule from crd-1:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from crd-1:crd-object-2
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-4").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 2 from crd-1:crd-object-2
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-y").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					Build(), // Doesn't match any rule from crd-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-5").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-3"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from crd-2:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-6").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-3"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 2 from crd-2:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-7").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-3"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 3 from crd-2:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-z").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-3"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't match any rule from crd-2:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-8").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-4"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't match to any crd
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-9").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-5"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
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
					}), crd.WithAutoprovisioningEnabled()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-3"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-5"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-6"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-7", "nodepool-8", "nodepool-9"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
			wantGroups: [][][]cloudprovider.NodeGroup{
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-x").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-3").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-2"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-4").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-2"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-5").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-3"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-6").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-3"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-7").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-3"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-z").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-3"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-8").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-4"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-9").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-5"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
			},
		},
		{
			name:                      "no crds - EK autoprovisioning enabled",
			ekAutoprovisioningEnabled: true,
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "ek-standard-32",
					}).
					Build(),
			},
			wantGroups: [][][]cloudprovider.NodeGroup{
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetSpec(&gkeclient.NodePoolSpec{
								MachineType: "ek-standard-32",
							}).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							Build(),
					},
				},
			},
		},
		{
			name:                      "crds and EKs specified",
			ekAutoprovisioningEnabled: true,
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from crd-1:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 2 from crd-1:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-x").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't match any rule from crd-1:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from crd-1:crd-object-2
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-4").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 2 from crd-1:crd-object-2
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-y").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					Build(), // Doesn't match any rule from crd-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-5").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-3"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 1 from crd-2:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-6").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-3"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 2 from crd-2:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-7").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-3"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Matches Priority 3 from crd-2:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-z").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-3"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't match any rule from crd-2:crd-object-1
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-8").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-4"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't match to any crd
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-9").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-5"},
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't match to any crd
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-10").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: defaultMachineType,
						Spot:        trueSpot,
					}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't specify any crd
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-11").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "ek-standard-32",
					}).
					SetGkeManager(gkeManager).
					Build(), // Doesn't specify any crd
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
					}), crd.WithAutoprovisioningEnabled()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-3"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-5"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-6"})),
						rules.NewRule(rules.WithNodePoolsRule([]string{"nodepool-7", "nodepool-8", "nodepool-9"})),
					}), crd.WithScaleUpAnyway(), crd.WithAutoprovisioningEnabled()),
			},
			wantGroups: [][][]cloudprovider.NodeGroup{
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-x").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-3").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-2"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-4").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-2"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-5").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-3"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-6").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-3"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-7").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-3"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-z").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-3"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-11").
							SetSpec(&gkeclient.NodePoolSpec{
								MachineType: "ek-standard-32",
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-8").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-4"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-9").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-5"},
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-10").
							SetSpec(&gkeclient.NodePoolSpec{
								MachineType: defaultMachineType,
								Spot:        trueSpot,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
			},
		},
		{
			name:                      "CRDS by Service Account",
			ekAutoprovisioningEnabled: true,
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
					}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:         map[string]string{testCrdLabel: "crd-object-2"},
						MachineType:    defaultMachineType,
						ServiceAccount: "test@1234.iam.gserviceaccount.com",
					}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:         map[string]string{testCrdLabel: "crd-object-2"},
						MachineType:    "n2-standard-2",
						ServiceAccount: "test@1234.iam.gserviceaccount.com",
					}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-4").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
					}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithMachineTypeRule(&defaultMachineType)),
					}),
					crd.WithScaleUpAnyway(),
					crd.WithAutoprovisioningEnabled()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-2"),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithMachineTypeRule(&defaultMachineType)),
					}),
					crd.WithScaleUpAnyway(),
					crd.WithAutoprovisioningEnabled(),
					crd.WithServiceAccount("test@1234.iam.gserviceaccount.com")),
			},
			wantGroups: [][][]cloudprovider.NodeGroup{
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: defaultMachineType,
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-2").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:         map[string]string{testCrdLabel: "crd-object-2"},
								MachineType:    defaultMachineType,
								ServiceAccount: "test@1234.iam.gserviceaccount.com",
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-3").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:         map[string]string{testCrdLabel: "crd-object-2"},
								MachineType:    "n2-standard-2",
								ServiceAccount: "test@1234.iam.gserviceaccount.com",
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
			},
		},
		{
			name: "crds with or without Image Type",
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						ImageType:   "cos_containerd",
					}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-1"},
						MachineType: defaultMachineType,
						ImageType:   "ubuntu_containerd",
					}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						ImageType:   "cos_containerd",
					}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-4").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "crd-object-2"},
						MachineType: defaultMachineType,
						ImageType:   "ubuntu_containerd",
					}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-1"),
					crd.WithImageType("cos_containerd"),
					crd.WithScaleUpAnyway(),
					crd.WithAutoprovisioningEnabled()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithName("crd-object-2"),
					crd.WithScaleUpAnyway(),
					crd.WithAutoprovisioningEnabled()),
			},
			wantGroups: [][][]cloudprovider.NodeGroup{
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-1").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-1"},
								MachineType: defaultMachineType,
								ImageType:   "cos_containerd",
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
				{
					{
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-3").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-2"},
								MachineType: defaultMachineType,
								ImageType:   "cos_containerd",
							}).
							SetGkeManager(gkeManager).
							Build(),
						gke.NewTestGkeMigBuilder().
							SetNodePoolName("nodepool-4").
							SetSpec(&gkeclient.NodePoolSpec{
								Labels:      map[string]string{testCrdLabel: "crd-object-2"},
								MachineType: defaultMachineType,
								ImageType:   "ubuntu_containerd",
							}).
							SetGkeManager(gkeManager).
							Build(),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCrdLister := lister.NewMockCrdLister(tc.crds)
			mockCrdLister.SetCrdLabel(testCrdLabel)
			mockProvider := NewMockGKEProvider(nil, machinetypes.N1)
			if tc.ekAutoprovisioningEnabled {
				mockProvider.SetResizableVmInAutopilotEnabled(machinetypes.EK.Name(), true)
			}
			organizerObj := NewOrganizer(mockCrdLister, mockProvider)
			node := apiv1.Node{}
			node.Labels = map[string]string{}
			for _, ng := range tc.nodeGroups {
				gkeManager.On("GetMigTemplateNodeInfo", ng).Return(framework.NewTestNodeInfo(&node), nil)
			}
			actual := organizerObj.OrganizeByCrds(tc.nodeGroups, tc.crds)
			assert.Equal(t, len(tc.wantGroups), len(actual))
			for c := range actual {
				assert.Equal(t, tc.wantGroups[c], actual[c])
			}
		})
	}
}

func TestOrganizeByMachineFamily(t *testing.T) {
	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsDataplaneV2Enabled").Return(true)

	testCases := []struct {
		name              string
		nodeGroups        []cloudprovider.NodeGroup
		prioritizedFamily machinetypes.MachineFamily
		wantGroups        [][]cloudprovider.NodeGroup
	}{
		{
			name: "no node groups",
		},
		{
			name:              "high priority node groups only",
			prioritizedFamily: machinetypes.EK,
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "ek-standard-32",
					}).
					Build(),
			},
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-1").
						SetSpec(&gkeclient.NodePoolSpec{
							MachineType: "ek-standard-32",
						}).
						Build(),
				},
			},
		},
		{
			name:              "normal priority node groups only",
			prioritizedFamily: machinetypes.EK,
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "e2-standard-1",
					}).
					Build(),
			},
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-1").
						SetSpec(&gkeclient.NodePoolSpec{
							MachineType: "e2-standard-1",
						}).
						Build(),
				},
			},
		},
		{
			name:              "mixed nodegroups",
			prioritizedFamily: machinetypes.EK,
			nodeGroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-1").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "e2-standard-1",
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "n2-standard-1",
					}).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("nodepool-3").
					SetSpec(&gkeclient.NodePoolSpec{
						MachineType: "ek-standard-32",
					}).
					Build(),
			},
			wantGroups: [][]cloudprovider.NodeGroup{
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-3").
						SetSpec(&gkeclient.NodePoolSpec{
							MachineType: "ek-standard-32",
						}).
						Build(),
				},
				{
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-1").
						SetSpec(&gkeclient.NodePoolSpec{
							MachineType: "e2-standard-1",
						}).
						Build(),
					gke.NewTestGkeMigBuilder().
						SetNodePoolName("nodepool-2").
						SetSpec(&gkeclient.NodePoolSpec{
							MachineType: "n2-standard-1",
						}).
						Build(),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := NewMockGKEProvider(nil, machinetypes.N1)
			o := organizer{provider: mockProvider}
			actual := o.organizeByMachineFamily(tc.nodeGroups, tc.prioritizedFamily)
			assert.Equal(t, len(tc.wantGroups), len(actual))
			for i := range actual {
				assert.ElementsMatch(t, tc.wantGroups[i], actual[i])
			}
		})
	}
}

func intPtr(i int) *int {
	return &i
}

func stringPtr(s string) *string {
	return &s
}
