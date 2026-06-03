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

package status

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	computeclass "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	npc_rules "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

var GB int64 = 1024 * 1024 * 1024

// TestCrdResourceReportingProcessor_Process tests CrdResourceReportingProcessor resources reporting.
func TestCrdResourceReportingProcessor_Process(t *testing.T) {
	machineFamilyN2 := "n2"
	machineFamilyCt5p := "ct5p"
	machineFamilyE2 := "e2"
	testCrdLabel := "ComputeClass"

	tests := map[string]struct {
		isAutopilot     bool
		crds            []crd.CRD
		nodeGroups      []nodeGroupDef
		expectedUpdates []expectedUpdateDef
		initialStatus   []expectedUpdateDef
	}{
		"comprehensive cluster state processing": {
			isAutopilot: false,
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithCrdType("CCC"),
					crd.WithLabel(testCrdLabel),
					crd.WithName("test-ccc"),
					crd.WithRules([]npc_rules.Rule{
						npc_rules.NewMachineSpecRule(&machineFamilyN2, nil, nil, nil),
						npc_rules.NewMachineSpecRule(&machineFamilyCt5p, nil, nil, nil),
					}), crd.WithScaleUpAnyway(),
				),
				crd.NewTestCrd(
					crd.WithCrdType("CCC"),
					crd.WithLabel(testCrdLabel),
					crd.WithName("test-ccc-2"),
					crd.WithRules([]npc_rules.Rule{
						npc_rules.NewMachineSpecRule(&machineFamilyE2, nil, nil, nil),
					}), crd.WithScaleUpAnyway(),
				),
			},
			nodeGroups: []nodeGroupDef{
				{
					machineType: "n2-standard-4",
					labels:      map[string]string{testCrdLabel: "test-ccc"},
					nodes: []nodeDef{
						{
							name: "node-1", cpu: 10000, mem: 64 * GB,
							pods: []podDef{
								{name: "pod-1", cpu: 2000, mem: 4 * GB},
								{name: "pod-2", cpu: 3000, mem: 12 * GB},
							},
						},
						{
							name: "node-2", cpu: 3000, mem: 8 * GB, gpu: 5,
							gpuConfig: &cloudprovider.GpuConfig{
								Label:                gkelabels.GPULabel,
								Type:                 machinetypes.NvidiaTeslaA100.Name(),
								ExtendedResourceName: gpu.ResourceNvidiaGPU,
							},
							pods: []podDef{
								{name: "pod-3", cpu: 1500, mem: 8 * GB, gpu: 3},
							},
						},
					},
				},
				{
					machineType: "ct5p-hightpu-4t",
					labels:      map[string]string{testCrdLabel: "test-ccc"},
					nodes: []nodeDef{
						{
							name: "node-3", cpu: 4000, mem: 16 * GB, tpu: 4,
							pods: []podDef{
								{name: "pod-4", cpu: 2000, mem: 8 * GB, tpu: 2},
							},
						},
					},
				},
				{
					machineType: "e2-standard-4",
					labels:      map[string]string{testCrdLabel: "test-ccc-2"},
					nodes: []nodeDef{
						{
							name: "node-4", cpu: 4000, mem: 16 * GB,
							pods: []podDef{
								{name: "pod-5", cpu: 1000, mem: 2 * GB},
							},
						},
						{
							name: "node-5", cpu: 4000, mem: 16 * GB, isTemplate: true,
							pods: []podDef{
								{name: "pod-6", cpu: 1000, mem: 2 * GB},
							},
						},
					},
				},
				{
					machineType: "n1-standard-8",
					labels:      map[string]string{testCrdLabel: "test-ccc-2"},
					nodes: []nodeDef{
						{
							name: "node-6", cpu: 8000, mem: 32 * GB,
							pods: []podDef{
								{name: "pod-7", cpu: 2000, mem: 16 * GB},
							},
						},
					},
				},
			},
			expectedUpdates: []expectedUpdateDef{
				{crdName: "test-ccc", ruleIdx: "0", resource: "cpu", unit: "Cores", currentCount: 13, targetCount: 13, utilizationPercentage: 50},
				{crdName: "test-ccc", ruleIdx: "0", resource: "memory", unit: "GiB", currentCount: 72, targetCount: 72, utilizationPercentage: 33},
				{crdName: "test-ccc", ruleIdx: "0", resource: crd.ResourceName(gpu.ResourceNvidiaGPU), unit: "Devices", currentCount: 5, targetCount: 5, utilizationPercentage: 60},

				{crdName: "test-ccc", ruleIdx: "1", resource: "cpu", unit: "Cores", currentCount: 4, targetCount: 4, utilizationPercentage: 50},
				{crdName: "test-ccc", ruleIdx: "1", resource: "memory", unit: "GiB", currentCount: 16, targetCount: 16, utilizationPercentage: 50},
				{crdName: "test-ccc", ruleIdx: "1", resource: crd.ResourceName(tpu.ResourceGoogleTPU), unit: "Chips", currentCount: 4, targetCount: 4, utilizationPercentage: 50},

				{crdName: "test-ccc-2", ruleIdx: "0", resource: "cpu", unit: "Cores", currentCount: 4, targetCount: 8, utilizationPercentage: 50},
				{crdName: "test-ccc-2", ruleIdx: "0", resource: "memory", unit: "GiB", currentCount: 16, targetCount: 32, utilizationPercentage: 25},

				{crdName: "test-ccc-2", ruleIdx: "ScaleUpAnyway", resource: "cpu", unit: "Cores", currentCount: 8, targetCount: 8, utilizationPercentage: 25},
				{crdName: "test-ccc-2", ruleIdx: "ScaleUpAnyway", resource: "memory", unit: "GiB", currentCount: 32, targetCount: 32, utilizationPercentage: 50},
			},
		},
		"clearing old values for inactive priority": {
			isAutopilot: false,
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithCrdType("CCC"),
					crd.WithLabel(testCrdLabel),
					crd.WithName("test-ccc"),
					crd.WithRules([]npc_rules.Rule{
						npc_rules.NewMachineSpecRule(&machineFamilyN2, nil, nil, nil),
					}),
				),
			},
			nodeGroups: []nodeGroupDef{
				{
					machineType: "n2-standard-4",
					labels:      map[string]string{testCrdLabel: "test-ccc"},
					nodes: []nodeDef{
						{
							name: "node-1", cpu: 4000, mem: 16 * GB,
						},
					},
				},
			},
			initialStatus: []expectedUpdateDef{
				{crdName: "test-ccc", ruleIdx: "1", resource: "cpu", unit: "Cores", currentCount: 10, targetCount: 10},
			},
			expectedUpdates: []expectedUpdateDef{
				{crdName: "test-ccc", ruleIdx: "0", resource: "cpu", unit: "Cores", currentCount: 4, targetCount: 4, utilizationPercentage: 0},
				{crdName: "test-ccc", ruleIdx: "0", resource: "memory", unit: "GiB", currentCount: 16, targetCount: 16, utilizationPercentage: 0},
			},
		},
		"empty cluster processing": {
			isAutopilot: false,
		},
		"clearing old values when no nodes match the CRD": {
			isAutopilot: false,
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithCrdType("CCC"),
					crd.WithLabel(testCrdLabel),
					crd.WithName("test-ccc"),
					crd.WithRules([]npc_rules.Rule{
						npc_rules.NewMachineSpecRule(&machineFamilyN2, nil, nil, nil),
					}),
				),
			},
			nodeGroups: []nodeGroupDef{},
			initialStatus: []expectedUpdateDef{
				{crdName: "test-ccc", ruleIdx: "0", resource: "cpu", unit: "Cores", currentCount: 10, targetCount: 10},
			},
		},
		"one CRD priority matching multiple node groups": {
			isAutopilot: false,
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithCrdType("CCC"),
					crd.WithLabel(testCrdLabel),
					crd.WithName("multi-mig-ccc"),
					crd.WithRules([]npc_rules.Rule{
						npc_rules.NewMachineSpecRule(&machineFamilyN2, nil, nil, nil),
					}), crd.WithScaleUpAnyway(),
				),
			},
			nodeGroups: []nodeGroupDef{
				{
					machineType: "n2-standard-4",
					labels:      map[string]string{testCrdLabel: "multi-mig-ccc"},
					nodes: []nodeDef{
						{
							name: "node-1", cpu: 4000, mem: 16 * GB,
							pods: []podDef{
								{name: "pod-1", cpu: 1000, mem: 2 * GB},
							},
						},
					},
				},
				{
					machineType: "n2-standard-8",
					labels:      map[string]string{testCrdLabel: "multi-mig-ccc"},
					nodes: []nodeDef{
						{
							name: "node-2", cpu: 8000, mem: 32 * GB,
							pods: []podDef{
								{name: "pod-2", cpu: 2000, mem: 10 * GB},
							},
						},
					},
				},
			},
			expectedUpdates: []expectedUpdateDef{
				{crdName: "multi-mig-ccc", ruleIdx: "0", resource: "cpu", unit: "Cores", currentCount: 12, targetCount: 12, utilizationPercentage: 25},
				{crdName: "multi-mig-ccc", ruleIdx: "0", resource: "memory", unit: "GiB", currentCount: 48, targetCount: 48, utilizationPercentage: 25},
			},
		},
		"Memory is rounded up to the nearest GiB": {
			isAutopilot: false,
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithCrdType("CCC"),
					crd.WithLabel(testCrdLabel),
					crd.WithName("multi-mig-ccc"),
					crd.WithRules([]npc_rules.Rule{
						npc_rules.NewMachineSpecRule(&machineFamilyN2, nil, nil, nil),
					}), crd.WithScaleUpAnyway(),
				),
			},
			nodeGroups: []nodeGroupDef{
				{
					machineType: "n2-standard-4",
					labels:      map[string]string{testCrdLabel: "multi-mig-ccc"},
					nodes: []nodeDef{
						{
							name: "node-1", cpu: 1000, mem: 10*GB + GB/1000,
							pods: []podDef{
								{name: "pod-1", cpu: 1000, mem: 1 * GB},
							},
						},
					},
				},
			},
			expectedUpdates: []expectedUpdateDef{
				{crdName: "multi-mig-ccc", ruleIdx: "0", resource: "cpu", unit: "Cores", currentCount: 1, targetCount: 1, utilizationPercentage: 100},
				{crdName: "multi-mig-ccc", ruleIdx: "0", resource: "memory", unit: "GiB", currentCount: 11, targetCount: 11, utilizationPercentage: 9},
			},
		},
		"only template (upcoming) nodes in node group": {
			isAutopilot: false,
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithCrdType("CCC"),
					crd.WithLabel(testCrdLabel),
					crd.WithName("template-ccc"),
					crd.WithRules([]npc_rules.Rule{
						npc_rules.NewMachineSpecRule(&machineFamilyN2, nil, nil, nil),
					}),
				),
			},
			nodeGroups: []nodeGroupDef{
				{
					machineType: "n2-standard-4",
					labels:      map[string]string{testCrdLabel: "template-ccc"},
					nodes: []nodeDef{
						{
							name: "node-1", cpu: 4000, mem: 16 * GB, isTemplate: true,
							pods: []podDef{
								{name: "pod-1", cpu: 1000, mem: 2 * GB},
							},
						},
					},
				},
			},
			expectedUpdates: []expectedUpdateDef{
				{crdName: "template-ccc", ruleIdx: "0", resource: "cpu", unit: "Cores", currentCount: 0, targetCount: 4, utilizationPercentage: 0},
				{crdName: "template-ccc", ruleIdx: "0", resource: "memory", unit: "GiB", currentCount: 0, targetCount: 16, utilizationPercentage: 0},
			},
		},
	}

	for name, testCase := range tests {
		tc := testCase
		t.Run(name, func(t *testing.T) {
			mockCrdLister := lister.NewMockCrdLister(tc.crds)
			mockCrdLister.SetCrdLabel(testCrdLabel)

			provider := &gke.GkeCloudProviderMock{}
			provider.On("IsAutopilotEnabled").Return(tc.isAutopilot)

			var nodeInfos []*framework.NodeInfo
			for _, ngDef := range tc.nodeGroups {
				ng := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
					MachineType: ngDef.machineType,
					Labels:      ngDef.labels,
				}).Build()

				for _, nDef := range ngDef.nodes {
					node := test.BuildTestNode(nDef.name, nDef.cpu, nDef.mem)
					if nDef.isTemplate {
						node.Annotations = map[string]string{gkelabels.NodeGeneratedFromTemplateAnnotation: "true"}
					}
					if nDef.gpu > 0 {
						test.AddGpusToNode(node, nDef.gpu)
					}
					if nDef.tpu > 0 {
						node.Status.Capacity[tpu.ResourceGoogleTPU] = *resource.NewQuantity(nDef.tpu, resource.DecimalSI)
						node.Status.Allocatable[tpu.ResourceGoogleTPU] = *resource.NewQuantity(nDef.tpu, resource.DecimalSI)
					}
					provider.On("GetNodeGpuConfig", node).Return(nDef.gpuConfig)
					provider.On("NodeGroupForNode", node).Return(ng, nil)

					var pods []*apiv1.Pod
					for _, pDef := range nDef.pods {
						pod := test.BuildTestPod(pDef.name, pDef.cpu, pDef.mem)
						if pDef.gpu > 0 {
							test.RequestGpuForPod(pod, pDef.gpu)
						}
						if pDef.tpu > 0 {
							pod.Spec.Containers[0].Resources.Requests[tpu.ResourceGoogleTPU] = *resource.NewQuantity(pDef.tpu, resource.DecimalSI)
						}
						pods = append(pods, pod)
					}
					nodeInfos = append(nodeInfos, framework.NewTestNodeInfo(node, pods...))
				}
			}

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, ni := range nodeInfos {
				if err := snapshot.AddNodeInfo(ni); err != nil {
					t.Fatalf("Failed to add node info to snapshot: %v", err)
				}
			}

			ctx := &context.AutoscalingContext{
				ClusterSnapshot: snapshot,
				CloudProvider:   provider,
			}

			resourceReportingChannel := make(chan UpdateMessage, 20)
			processor := &CrdResourceReportingProcessor{
				npcCrdLister: mockCrdLister,
				updatesCh:    resourceReportingChannel,
				matcher:      computeclass.NewMatcher(mockCrdLister, provider),
			}

			// Use synctest.Test so that timestamps returned by `time.Now()` would be deterministic.
			synctest.Test(t, func(t *testing.T) {
				err := processor.Process(ctx, &clusterstate.ClusterStateRegistry{}, time.Now())
				if err != nil {
					t.Fatalf("Process returned an error: %v", err)
				}
			})

			close(resourceReportingChannel) // Close the channel to signal that no more updates will be sent

			statuses := make(map[string]*FakeCrdStatus)
			for _, c := range tc.crds {
				var initial []expectedUpdateDef
				for _, is := range tc.initialStatus {
					if is.crdName == c.Name() {
						initial = append(initial, is)
					}
				}
				statuses[c.Name()] = &FakeCrdStatus{
					crdName:         c.Name(),
					recordedUpdates: initial,
				}
			}

			var actualUpdates []expectedUpdateDef
			for update := range resourceReportingChannel {
				if update.Id.CRDLabel != testCrdLabel {
					t.Errorf("Unexpected CRDLabel: %v", update.Id.CRDLabel)
				}
				status, ok := statuses[update.Id.CRDName]
				if !ok {
					status = &FakeCrdStatus{crdName: update.Id.CRDName}
					statuses[update.Id.CRDName] = status
				}
				update.Mutate(status)
			}

			for _, status := range statuses {
				actualUpdates = append(actualUpdates, status.recordedUpdates...)
			}

			sortOpt := cmpopts.SortSlices(func(a, b expectedUpdateDef) bool {
				if a.crdName != b.crdName {
					return a.crdName < b.crdName
				}
				if a.ruleIdx != b.ruleIdx {
					return a.ruleIdx < b.ruleIdx
				}
				return a.resource < b.resource
			})

			if diff := cmp.Diff(tc.expectedUpdates, actualUpdates, sortOpt, cmp.AllowUnexported(expectedUpdateDef{})); diff != "" {
				t.Errorf("Unexpected updates (-want +got):\n%s", diff)
			}
		})
	}
}

type FakeCrdStatus struct {
	crd.CRDStatus
	crdName         string
	recordedUpdates []expectedUpdateDef
}

func (s *FakeCrdStatus) UpdateRuleResourceInfo(ruleIdx string, info crd.ResourceInfo) {
	s.recordedUpdates = append(s.recordedUpdates, expectedUpdateDef{
		crdName:               s.crdName,
		ruleIdx:               ruleIdx,
		resource:              info.Name,
		unit:                  info.Unit,
		currentCount:          info.CurrentCount,
		targetCount:           info.TargetCount,
		utilizationPercentage: info.CurrentUtilizationPercentage,
	})
}

func (s *FakeCrdStatus) ResetAllResourceInfo() {
	s.recordedUpdates = nil
}

type podDef struct {
	name string
	cpu  int64
	mem  int64
	gpu  int64
	tpu  int64
}

type nodeDef struct {
	name       string
	cpu        int64
	mem        int64
	gpu        int64
	tpu        int64
	isTemplate bool
	gpuConfig  *cloudprovider.GpuConfig
	pods       []podDef
}

type nodeGroupDef struct {
	machineType string
	labels      map[string]string
	nodes       []nodeDef
}

type expectedUpdateDef struct {
	crdName               string
	ruleIdx               string
	resource              crd.ResourceName
	unit                  crd.ResourceUnit
	currentCount          int
	targetCount           int
	utilizationPercentage int
}
