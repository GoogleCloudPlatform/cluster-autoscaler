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

package processor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	appsv1 "k8s.io/api/apps/v1"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apilabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	v1 "k8s.io/client-go/listers/apps/v1"

	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	lookaheadbuffer_strategy "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer/strategy"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"

	calculator_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator/test"

	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"

	schedulermetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"

	taintutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestPodRequestsPerWorkloadID(t *testing.T) {
	allFamilies := map[string]bool{
		machinetypes.EK.Name():  true,
		machinetypes.E4A.Name(): true,
		machinetypes.E4.Name():  true,
	}

	testCases := []struct {
		desc                         string
		nodeInfos                    []*framework.NodeInfo
		ignoredTaints                []string
		crds                         []crd.CRD
		machineFamiliesWithLAEnabled map[string]bool
		expectedResourcesPerWID      map[string]map[string]apiv1.ResourceList
	}{
		{
			desc:                         "No nodes",
			nodeInfos:                    []*framework.NodeInfo{},
			machineFamiliesWithLAEnabled: allFamilies,
			expectedResourcesPerWID:      map[string]map[string]apiv1.ResourceList{},
		},
		{
			desc: "Pods on non-resizable nodes are ignored",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					ekvms_test.EkNode32("node-1", 1000, 1000),
					test.BuildTestPod("pod-1", 1000, 1*size.GiB),
				),
				framework.NewTestNodeInfo(
					test.BuildTestNode("node-2", 2000, 2000),
					test.BuildTestPod("pod-1", 1000, 1*size.GiB),
				),
				framework.NewTestNodeInfo(
					node32WithWorkloadSeparation(machinetypes.EK.Name(), "node-3", 1000, 1000),
					test.BuildTestPod("pod-1", 1000, 1*size.GiB),
				),
			},
			machineFamiliesWithLAEnabled: allFamilies,
			expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"": {
						apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
						apiv1.ResourceMemory: *resource.NewQuantity(1*size.GiB, resource.DecimalSI),
					},
					"NoSchedule:workload-separation:yes": {
						apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
						apiv1.ResourceMemory: *resource.NewQuantity(1*size.GiB, resource.DecimalSI),
					},
				},
			},
		},
		{
			desc: "System pods are not considered",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					ekvms_test.EkNode32("node-1", 1000, 1000),
					lookaheadbuffer.BuildTestLookaheadPod("la-pod-1", 1000, 1*size.GiB, machinetypes.EK.Name()),
					test.BuildTestPod("system-pod-1", 1000, 1*size.GiB, func(p *apiv1.Pod) { p.Namespace = metav1.NamespaceSystem }),
				),
			},
			machineFamiliesWithLAEnabled: allFamilies,
			expectedResourcesPerWID:      map[string]map[string]apiv1.ResourceList{},
		},
		{
			desc: "Multiple nodes with different machine families",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					ekvms_test.EkNode32("ek-node-1", 1000, 1000),
					test.BuildTestPod("pod-1", 1000, 1*size.GiB),
				),
				framework.NewTestNodeInfo(
					node32WithWorkloadSeparation(machinetypes.EK.Name(), "ek-node-2", 2000, 2000),
					test.BuildTestPod("pod-2", 2000, 2*size.GiB),
				),
				framework.NewTestNodeInfo(
					ekvms_test.E4aNode32("e4a-node-1", 1000, 1000),
					test.BuildTestPod("pod-3", 1000, 1*size.GiB),
				),
				framework.NewTestNodeInfo(
					ekvms_test.E4Node32("e4-node-1", 4000, 4000),
					test.BuildTestPod("pod-4", 4000, 4*size.GiB),
				),
			},
			machineFamiliesWithLAEnabled: allFamilies,
			expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
				machinetypes.EK.Name(): {
					"": {
						apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
						apiv1.ResourceMemory: *resource.NewQuantity(1*size.GiB, resource.DecimalSI),
					},
					"NoSchedule:workload-separation:yes": {
						apiv1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						apiv1.ResourceMemory: *resource.NewQuantity(2*size.GiB, resource.DecimalSI),
					},
				},
				machinetypes.E4A.Name(): {
					"": {
						apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
						apiv1.ResourceMemory: *resource.NewQuantity(1*size.GiB, resource.DecimalSI),
					},
				},
				machinetypes.E4.Name(): {
					"": {
						apiv1.ResourceCPU:    *resource.NewMilliQuantity(4000, resource.DecimalSI),
						apiv1.ResourceMemory: *resource.NewQuantity(4*size.GiB, resource.DecimalSI),
					},
				},
			},
		},
	}

	for _, family := range []string{machinetypes.EK.Name(), machinetypes.E4A.Name(), machinetypes.E4.Name()} {
		testCases = append(testCases, []struct {
			desc                         string
			nodeInfos                    []*framework.NodeInfo
			ignoredTaints                []string
			crds                         []crd.CRD
			machineFamiliesWithLAEnabled map[string]bool
			expectedResourcesPerWID      map[string]map[string]apiv1.ResourceList
		}{
			{
				desc: fmt.Sprintf("%s: One default workload ID node", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
						test.BuildTestPod("pod-2", 2000, 2*size.GiB),
					),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
					family: {
						"": {
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(3000, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(3*size.GiB, resource.DecimalSI),
						},
					},
				},
			},
			{
				desc: fmt.Sprintf("%s: One default workload ID node with custom taint - skipped", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						ekvms_test.NewNodeBuilderFromNode(node32(family, "node-1", 1000, 1000)).WithTaint(apiv1.Taint{
							Key:    "user-taint",
							Value:  "true",
							Effect: apiv1.TaintEffectNoSchedule,
						}).Build(),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
						test.BuildTestPod("pod-2", 2000, 2*size.GiB),
					),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID:      map[string]map[string]apiv1.ResourceList{},
			},
			{
				desc: fmt.Sprintf("%s: One default workload ID node with ignored taint", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						ekvms_test.NewNodeBuilderFromNode(node32(family, "node-1", 1000, 1000)).WithTaint(apiv1.Taint{
							Key:    "status-taint",
							Value:  "true",
							Effect: apiv1.TaintEffectNoSchedule,
						}).Build(),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
					),
				},
				ignoredTaints:                []string{"status-taint"},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
					family: {
						"": {
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(1*size.GiB, resource.DecimalSI),
						},
					},
				},
			},
			{
				desc: fmt.Sprintf("%s: One workload separated node", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32WithWorkloadSeparation(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB)),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
					family: {
						"NoSchedule:workload-separation:yes": {
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(1*size.GiB, resource.DecimalSI),
						},
					},
				},
			},
			{
				desc: fmt.Sprintf("%s: Multiple nodes", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
					),
					framework.NewTestNodeInfo(
						node32WithWorkloadSeparation(family, "node-1", 2000, 2000),
						test.BuildTestPod("pod-2", 2000, 2*size.GiB),
					),
					framework.NewTestNodeInfo(
						node8(family, "node-2", 4000, 4000),
						test.BuildTestPod("pod-1", 4000, 4*size.GiB),
					),
					framework.NewTestNodeInfo(
						node8WithWorkloadSeparation(family, "node-1", 8000, 8000),
						test.BuildTestPod("pod-1", 8000, 8*size.GiB),
					),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
					family: {
						"": {
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(5000, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(5*size.GiB, resource.DecimalSI),
						},
						"NoSchedule:workload-separation:yes": {
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(10000, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(10*size.GiB, resource.DecimalSI),
						},
					},
				},
			},
			{
				desc: fmt.Sprintf("%s: Autopilot compute class nodes", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(ekvms_test.NewNodeBuilderFromNode(node32(family, "node-1", 2000, 2000)).WithTaint(apiv1.Taint{
						Key:    gkelabels.ComputeClassLabel,
						Value:  "autopilot",
						Effect: apiv1.TaintEffectNoSchedule,
					}).WithLabel(gkelabels.ComputeClassLabel, "autopilot").Build(),
						test.BuildTestPod("pod-1", 2000, 2*size.GiB),
					),
					framework.NewTestNodeInfo(ekvms_test.NewNodeBuilderFromNode(node8(family, "node-2", 1000, 1000)).WithTaint(apiv1.Taint{
						Key:    gkelabels.ComputeClassLabel,
						Value:  "autopilot-spot", // Pods on autopilot-spot Compute Class aren't eligible for lookahead.
						Effect: apiv1.TaintEffectNoSchedule,
					}).WithLabel(gkelabels.ComputeClassLabel, "autopilot-spot").Build(),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
					),
				},
				crds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("autopilot"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithAutopilotManaged(),
						crd.WithRules([]rules.Rule{
							rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose"))),
						})),
					crd.NewTestCrd(
						crd.WithName("autopilot-spot"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithAutopilotManaged(),
						crd.WithRules([]rules.Rule{
							rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose")), rules.WithSpotRule(ptr.To(true))),
							rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose"))),
						})),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
					family: {
						"NoSchedule:cloud.google.com/compute-class:autopilot": {
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(2*size.GiB, resource.DecimalSI),
						},
					},
				},
			},
			{
				desc: fmt.Sprintf("%s: Autopilot-arm compute class nodes", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(ekvms_test.NewNodeBuilderFromNode(node32(family, "node-1", 2000, 2000)).WithTaint(apiv1.Taint{
						Key:    gkelabels.ComputeClassLabel,
						Value:  "autopilot-arm",
						Effect: apiv1.TaintEffectNoSchedule,
					}).WithLabel(gkelabels.ComputeClassLabel, "autopilot-arm").Build(),
						test.BuildTestPod("pod-1", 2000, 2*size.GiB),
					),
					framework.NewTestNodeInfo(ekvms_test.NewNodeBuilderFromNode(node8(family, "node-2", 1000, 1000)).WithTaint(apiv1.Taint{
						Key:    gkelabels.ComputeClassLabel,
						Value:  "autopilot-arm-spot", // Pods on autopilot-spot Compute Class aren't eligible for lookahead.
						Effect: apiv1.TaintEffectNoSchedule,
					}).WithLabel(gkelabels.ComputeClassLabel, "autopilot-arm-spot").Build(),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
					),
				},
				crds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("autopilot-arm"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithAutopilotManaged(),
						crd.WithRules([]rules.Rule{
							rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose-arm"))),
						})),
					crd.NewTestCrd(
						crd.WithName("autopilot-arm-spot"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithAutopilotManaged(),
						crd.WithRules([]rules.Rule{
							rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose-arm")), rules.WithSpotRule(ptr.To(true))),
							rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose-arm"))),
						})),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
					family: {
						"NoSchedule:cloud.google.com/compute-class:autopilot-arm": {
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(2*size.GiB, resource.DecimalSI),
						},
					},
				},
			},
			{
				desc: fmt.Sprintf("%s: Can combine workload separation and CCC", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(ekvms_test.NewNodeBuilderFromNode(node32(family, "node-1", 1000, 1000)).
						WithTaint(apiv1.Taint{
							Key:    gkelabels.ComputeClassLabel,
							Value:  "autopilot",
							Effect: apiv1.TaintEffectNoSchedule,
						}).
						WithLabel(gkelabels.ComputeClassLabel, "autopilot").
						WithTaint(apiv1.Taint{
							Key:    "workload-separation",
							Value:  "yes",
							Effect: apiv1.TaintEffectNoSchedule,
						}).
						WithLabel("workload-separation", "yes").Build(),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
					),
				},
				crds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("autopilot"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithAutopilotManaged(),
						crd.WithRules([]rules.Rule{
							rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose"))),
						})),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
					family: {
						"NoSchedule:cloud.google.com/compute-class:autopilot,NoSchedule:workload-separation:yes": {
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(1*size.GiB, resource.DecimalSI),
						},
					},
				},
			},
			{
				desc: fmt.Sprintf("%s: autopilot managed node taint doesn't prevent lookahead", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(ekvms_test.NewNodeBuilderFromNode(node32(family, "node-1", 1000, 1000)).
						WithTaint(apiv1.Taint{
							Key:    gkelabels.ComputeClassLabel,
							Value:  "autopilot",
							Effect: apiv1.TaintEffectNoSchedule,
						}).
						WithLabel(gkelabels.ComputeClassLabel, "autopilot").
						WithTaint(apiv1.Taint{
							Key:    "cloud.google.com/autopilot-managed-node",
							Value:  "true",
							Effect: apiv1.TaintEffectNoSchedule,
						}).
						WithLabel("cloud.google.com/autopilot-managed-node", "true").Build(),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
					),
				},
				crds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("autopilot"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithAutopilotManaged(),
						crd.WithRules([]rules.Rule{
							rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose"))),
						})),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID: map[string]map[string]apiv1.ResourceList{
					family: {
						"NoSchedule:cloud.google.com/autopilot-managed-node:true,NoSchedule:cloud.google.com/compute-class:autopilot": {
							apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
							apiv1.ResourceMemory: *resource.NewQuantity(1*size.GiB, resource.DecimalSI),
						},
					},
				},
			},
			{
				desc: fmt.Sprintf("%s: spot node - skipped", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						ekvms_test.NewNodeBuilderFromNode(node32(family, "spot", 32000, 128*size.GiB)).
							WithLabel(gkelabels.SpotLabel, gkelabels.PreemptionValue).Build(),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
						test.BuildTestPod("pod-2", 2000, 2*size.GiB),
					),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID:      map[string]map[string]apiv1.ResourceList{},
			},
			{
				desc: fmt.Sprintf("%s: preemptible node - skipped", family),
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						ekvms_test.NewNodeBuilderFromNode(node32(family, "spot", 32000, 128*size.GiB)).
							WithLabel(gkelabels.PreemptibleLabel, gkelabels.PreemptionValue).Build(),
						test.BuildTestPod("pod-1", 1000, 1*size.GiB),
						test.BuildTestPod("pod-2", 2000, 2*size.GiB),
					),
				},
				machineFamiliesWithLAEnabled: map[string]bool{family: true},
				expectedResourcesPerWID:      map[string]map[string]apiv1.ResourceList{},
			},
		}...)
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cccLister := lister.NewMockCrdListerWithLabel(tc.crds, gkelabels.ComputeClassLabel)
			mcp := machinetypes.NewMachineConfigProvider(nil)
			p := NewLookaheadPodInjectionProcessor(nil, nil, &mockWorkloadSeparationLimiter{limit: 10}, mcp, systempods.NewClassifier([]string{"kube-system"}), cccLister, calculator_test.NewWithProvider(mcp), nil)
			taintsConfig := newTaintsConfig(tc.ignoredTaints)
			cxWorkloadRequest := p.podRequestsPerMachineFamilyPerWorkloadID(tc.nodeInfos, &taintsConfig, tc.machineFamiliesWithLAEnabled)
			assert.Equal(t, tc.expectedResourcesPerWID, cxWorkloadRequest)
		})
	}
}

func TestHasEligibleComputeClass(t *testing.T) {
	tests := []struct {
		name string
		node *apiv1.Node
		crds []crd.CRD
		want bool
	}{
		{
			name: "no compute class",
			node: test.BuildTestNode("node-1", 1000, 1000),
			crds: []crd.CRD{},
			want: true,
		},
		{
			name: "ineligible compute class - not autopilot managed",
			node: ekvms_test.NewNodeBuilderFromNode(test.BuildTestNode("node-1", 1000, 1000)).
				WithLabel(gkelabels.ComputeClassLabel, "custom").Build(),
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("custom"),
					crd.WithLabel(gkelabels.ComputeClassLabel),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithMachineFamilyRule(ptr.To("ek"))),
					})),
			},
			want: false,
		},
		{
			name: "ineligible compute class - no rules",
			node: ekvms_test.NewNodeBuilderFromNode(test.BuildTestNode("node-1", 1000, 1000)).
				WithLabel(gkelabels.ComputeClassLabel, "custom").Build(),
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("custom"),
					crd.WithLabel(gkelabels.ComputeClassLabel),
					crd.WithAutopilotManaged(),
					crd.WithRules([]rules.Rule{})),
			},
			want: false,
		},
		{
			name: "ineligible compute class - lookahead not allowed for spot autopilot class",
			node: ekvms_test.NewNodeBuilderFromNode(test.BuildTestNode("node-1", 1000, 1000)).
				WithLabel(gkelabels.ComputeClassLabel, "autopilot-spot").Build(),
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("autopilot-spot"),
					crd.WithLabel(gkelabels.ComputeClassLabel),
					crd.WithAutopilotManaged(),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose")), rules.WithSpotRule(ptr.To(true))),
						rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose"))),
					})),
			},
			want: false,
		},
		{
			name: "ineligible compute class - unsupported pod family",
			node: ekvms_test.NewNodeBuilderFromNode(test.BuildTestNode("node-1", 1000, 1000)).
				WithLabel(gkelabels.ComputeClassLabel, "custom").Build(),
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("custom"),
					crd.WithLabel(gkelabels.ComputeClassLabel),
					crd.WithAutopilotManaged(),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithPodFamilyRule(ptr.To("unsupported"))),
					}),
				),
			},
			want: false,
		},
		{
			name: "eligible compute class",
			node: ekvms_test.NewNodeBuilderFromNode(test.BuildTestNode("node-1", 1000, 1000)).
				WithLabel(gkelabels.ComputeClassLabel, "autopilot").Build(),
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("autopilot"),
					crd.WithLabel(gkelabels.ComputeClassLabel),
					crd.WithAutopilotManaged(),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose"))),
					}),
				),
			},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lister := lister.NewMockCrdListerWithLabel(tc.crds, gkelabels.ComputeClassLabel)
			if got := hasEligibleComputeClass(tc.node, lister); got != tc.want {
				t.Errorf("hasEligibleComputeClass() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasSupportedTaints(t *testing.T) {
	testCases := []struct {
		name          string
		node          *apiv1.Node
		ignoredTaints []string
		workloadID    string

		want bool
	}{
		{
			name:          "no taints",
			node:          test.BuildTestNode("node-1", 1000, 1000),
			ignoredTaints: []string{},
			want:          true,
		},
		{
			name: "tainted node",
			node: ekvms_test.NewNodeBuilder("node-1", 1000, 1000).WithTaint(apiv1.Taint{
				Key:    "taint-key",
				Value:  "taint-value",
				Effect: apiv1.TaintEffectNoSchedule,
			}).Build(),
			ignoredTaints: []string{},
			want:          false,
		},
		{
			name: "node with ignored taint",
			node: ekvms_test.NewNodeBuilder("node-1", 1000, 1000).WithTaint(apiv1.Taint{
				Key:    "status-taint",
				Value:  "true",
				Effect: apiv1.TaintEffectNoSchedule,
			}).Build(),
			ignoredTaints: []string{"status-taint"},
			want:          true,
		},
		{
			name: "workload separation",
			node: ekvms_test.NewNodeBuilder("node-1", 1000, 1000).WithTaint(apiv1.Taint{
				Key:    "workload-separation",
				Value:  "yes",
				Effect: apiv1.TaintEffectNoSchedule,
			}).WithLabel("workload-separation", "yes").Build(),
			ignoredTaints: []string{},
			want:          true,
		},
		{
			name: "workload separation with ignored taint",
			node: ekvms_test.NewNodeBuilder("node-1", 1000, 1000).WithTaint(apiv1.Taint{
				Key:    "workload-separation",
				Value:  "yes",
				Effect: apiv1.TaintEffectNoSchedule,
			}).WithTaint(apiv1.Taint{
				Key:    "status-taint",
				Value:  "true",
				Effect: apiv1.TaintEffectNoSchedule,
			}).WithLabel("workload-separation", "yes").Build(),
			ignoredTaints: []string{"status-taint"},
			want:          true,
		},
		{
			name: "workload separation with extra taint",
			node: ekvms_test.NewNodeBuilder("node-1", 1000, 1000).WithTaint(apiv1.Taint{
				Key:    "workload-separation",
				Value:  "yes",
				Effect: apiv1.TaintEffectNoSchedule,
			}).WithTaint(apiv1.Taint{
				Key:    "taint-key",
				Value:  "taint-value",
				Effect: apiv1.TaintEffectNoSchedule,
			}).WithLabel(gkelabels.ComputeClassLabel, "autopilot").Build(),
			ignoredTaints: []string{},
			want:          false,
		},
		{
			name: "ccc is a type of workload separation",
			node: ekvms_test.NewNodeBuilder("node-1", 1000, 1000).WithTaint(apiv1.Taint{
				Key:    gkelabels.ComputeClassLabel,
				Value:  "autopilot",
				Effect: apiv1.TaintEffectNoSchedule,
			}).WithLabel(gkelabels.ComputeClassLabel, "autopilot").Build(),
			ignoredTaints: []string{},
			want:          true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taintsConfig := newTaintsConfig(tc.ignoredTaints)
			if got := hasSupportedTaints(tc.node, &taintsConfig); got != tc.want {
				t.Errorf("nodeMatchesWorkloadID() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSumNonSystemPodRequests(t *testing.T) {
	classifier := systempods.NewClassifier([]string{"kube-system"})
	tests := []struct {
		name     string
		nodeInfo *framework.NodeInfo
		want     apiv1.ResourceList
	}{
		{
			name:     "no pods",
			nodeInfo: framework.NewTestNodeInfo(test.BuildTestNode("node-1", 1000, 1000)),
			want:     apiv1.ResourceList{},
		},
		{
			name: "only system pods",
			nodeInfo: framework.NewTestNodeInfo(test.BuildTestNode("node-1", 1000, 1000),
				test.BuildTestPod("pod-1", 1000, 1000, func(p *apiv1.Pod) { p.Namespace = "kube-system" })),
			want: apiv1.ResourceList{},
		},
		{
			name: "default namespace pods",
			nodeInfo: framework.NewTestNodeInfo(test.BuildTestNode("node-1", 1000, 1000),
				test.BuildTestPod("pod-1", 1000, 1000)),
			want: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(1000, resource.DecimalSI),
			},
		},
		{
			name: "non-system custom namespace",
			nodeInfo: framework.NewTestNodeInfo(test.BuildTestNode("node-1", 1000, 1000),
				test.BuildTestPod("pod-1", 1000, 1000, func(p *apiv1.Pod) { p.Namespace = "custom-namespace" })),
			want: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(1000, resource.DecimalSI),
			},
		},
		{
			name: "mixed pods",
			nodeInfo: framework.NewTestNodeInfo(test.BuildTestNode("node-1", 1000, 1000),
				test.BuildTestPod("pod-1", 1000, 1000, func(p *apiv1.Pod) { p.Namespace = "kube-system" }),
				test.BuildTestPod("pod-2", 500, 500)),
			want: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(500, resource.DecimalSI),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sumNonSystemPodRequests(tc.nodeInfo, classifier)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCreateLookaheadPodsForWorkloadID(t *testing.T) {
	tests := []struct {
		name       string
		workloadID string
		requests   apiv1.ResourceList
		daemonsets []*appsv1.DaemonSet
		dsListErr  error
		want       []*apiv1.Pod
		wantErr    bool
	}{
		{
			name:       "no lookahead pods",
			workloadID: "",
			requests: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewMilliQuantity(0, resource.DecimalSI),
			},
			daemonsets: []*appsv1.DaemonSet{},
			want:       []*apiv1.Pod{},
		},
		{
			name:       "lookahead pod",
			workloadID: "",
			requests: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewMilliQuantity(32000, resource.DecimalSI),
			},
			daemonsets: []*appsv1.DaemonSet{},
			want: []*apiv1.Pod{
				lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, machinetypes.EK.Name()),
			},
		},
		{
			name:       "daemonset requests subtracted from lookahead pod",
			workloadID: "",
			requests: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewMilliQuantity(32000, resource.DecimalSI),
			},
			daemonsets: []*appsv1.DaemonSet{
				newDaemonSet("ds-1", 1, 100, 100, nil),
			},
			want: []*apiv1.Pod{
				lookaheadbuffer.BuildTestLookaheadPod("", 900, 900, machinetypes.EK.Name()),
			},
		},
		{
			name:       "daemonset requests greater than lookahead pod",
			workloadID: "",
			requests: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewMilliQuantity(32000, resource.DecimalSI),
			},
			daemonsets: []*appsv1.DaemonSet{
				newDaemonSet("ds-1", 1, 2000, 2000, nil), // Greater than lookahead.
			},
			want: []*apiv1.Pod{},
		},
		{
			name:       "daemonset with multiple containers",
			workloadID: "",
			requests: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewMilliQuantity(32000, resource.DecimalSI),
			},
			daemonsets: []*appsv1.DaemonSet{
				newDaemonSet("ds-1", 2, 100, 100, nil),
			},
			want: []*apiv1.Pod{
				lookaheadbuffer.BuildTestLookaheadPod("", 800, 800, machinetypes.EK.Name()),
			},
		},
		{
			name:       "multiple daemonsets",
			workloadID: "",
			requests: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewMilliQuantity(32000, resource.DecimalSI),
			},
			daemonsets: []*appsv1.DaemonSet{
				newDaemonSet("ds-1", 1, 100, 100, nil),
				newDaemonSet("ds-2", 1, 200, 200, nil),
			},
			want: []*apiv1.Pod{
				lookaheadbuffer.BuildTestLookaheadPod("", 700, 700, machinetypes.EK.Name()),
			},
		},
		{
			name:       "error listing daemonsets",
			workloadID: "",
			requests: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewMilliQuantity(32000, resource.DecimalSI),
			},
			daemonsets: []*appsv1.DaemonSet{},
			dsListErr:  errors.New("error listing daemonsets"),
			wantErr:    true,
		},
		{
			name:       "daemonset doesn't apply to workload separated lookahead",
			workloadID: "NoSchedule:workload-separation:yes",
			requests: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewMilliQuantity(32000, resource.DecimalSI),
			},
			daemonsets: []*appsv1.DaemonSet{
				newDaemonSet("ds-1", 1, 100, 100, nil),
			},
			want: []*apiv1.Pod{
				lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:workload-separation:yes", 1000, 1000, machinetypes.EK.Name()),
			},
		},
		{
			name:       "daemonset with wildcard toleration",
			workloadID: "NoSchedule:workload-separation:yes",
			requests: apiv1.ResourceList{
				apiv1.ResourceCPU: *resource.NewMilliQuantity(32000, resource.DecimalSI),
			},
			daemonsets: []*appsv1.DaemonSet{
				withToleration(newDaemonSet("ds-1", 1, 100, 100, nil), apiv1.Toleration{Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule}),
			},
			want: []*apiv1.Pod{
				lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:workload-separation:yes", 900, 900, machinetypes.EK.Name()),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			daemonSetLister := &mockDSLister{}
			daemonSetLister.On("List", apilabels.Everything()).Return(tt.daemonsets, tt.dsListErr)
			ctx := &ca_context.AutoscalingContext{
				AutoscalingKubeClients: ca_context.AutoscalingKubeClients{
					ListerRegistry: kubernetes.NewListerRegistry(nil, nil, nil, nil, daemonSetLister, nil, nil, nil, nil),
				},
			}

			mcp := machinetypes.NewMachineConfigProvider(nil)
			calculator := calculator_test.NewWithProvider(mcp)
			p := NewLookaheadPodInjectionProcessor(&fakeLookaheadPodProvider{}, nil, nil, mcp, nil, nil, calculator, nil)
			got, err := p.createLookaheadPodsForWorkloadID(tt.workloadID, tt.requests, machinetypes.EK.Name(), ctx)
			if tt.wantErr {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}

			assert.ElementsMatch(t, got, tt.want)
		})
	}
}

func TestProcess(t *testing.T) {
	schedulermetrics.Register()

	testCases := []struct {
		desc                   string
		launchStatus           lookaheadbuffer_strategy.Status
		maxWorkloadSeparations int
		crds                   []crd.CRD
		nodeInfos              []*framework.NodeInfo
		unschedulablePods      []*apiv1.Pod
		daemonSets             []*appsv1.DaemonSet
		fetchingDaemonSetsErr  error
		expectedPods           []*apiv1.Pod
		expectedErr            bool
	}{
		{
			desc:                   "No nodes",
			launchStatus:           lookaheadbuffer_strategy.Enabled,
			maxWorkloadSeparations: 10,
			nodeInfos:              []*framework.NodeInfo{},
			unschedulablePods:      []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
			expectedPods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 100, 100),
			},
		},
		{
			desc:                   "Non-resizable nodes",
			launchStatus:           lookaheadbuffer_strategy.Enabled,
			maxWorkloadSeparations: 10,
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					test.BuildTestNode("node-1", 2000, 2000),
					test.BuildTestPod("pod-1", 2000, 100)),
				framework.NewTestNodeInfo(
					test.BuildTestNode("node-2", 2000, 2000),
					test.BuildTestPod("pod-1", 2000, 100)),
			},
			unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
			expectedPods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 100, 100),
			},
		},
		{
			desc:                   "Multiple machine families - EK in default workload ID and E4A with workload separation",
			launchStatus:           lookaheadbuffer_strategy.Enabled,
			maxWorkloadSeparations: 10,
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					node32(machinetypes.EK.Name(), "node-1", 1000, 1000),
					test.BuildTestPod("pod-1", 32000, 100)),
				framework.NewTestNodeInfo(
					node32WithWorkloadSeparation(machinetypes.E4A.Name(), "node-2", 1000, 1000),
					test.BuildTestPod("pod-2", 32000, 100)),
			},
			unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
			expectedPods: []*apiv1.Pod{
				lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, machinetypes.EK.Name()),
				lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:workload-separation:yes", 1000, 1000, machinetypes.E4A.Name()),
				test.BuildTestPod("pod-1", 100, 100),
			},
		},
		{
			desc:                   "Multiple machine families - EK, E4A, E4 with CCC, workload separation, and non-resizable nodes",
			launchStatus:           lookaheadbuffer_strategy.Enabled,
			maxWorkloadSeparations: 10,
			crds: []crd.CRD{
				crd.NewTestCrd(
					crd.WithName("autopilot"),
					crd.WithLabel(gkelabels.ComputeClassLabel),
					crd.WithAutopilotManaged(),
					crd.WithRules([]rules.Rule{
						rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose"))),
					})),
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					node32(machinetypes.EK.Name(), "node-1", 1000, 1000),
					test.BuildTestPod("pod-1", 32000, 100)),
				framework.NewTestNodeInfo(
					node32WithWorkloadSeparation(machinetypes.E4A.Name(), "node-2", 1000, 1000),
					test.BuildTestPod("pod-2", 32000, 100)),
				framework.NewTestNodeInfo(
					ekvms_test.NewNodeBuilderFromNode(node32(machinetypes.E4.Name(), "node-3", 1000, 1000)).WithTaint(apiv1.Taint{
						Key:    gkelabels.ComputeClassLabel,
						Value:  "autopilot",
						Effect: apiv1.TaintEffectNoSchedule,
					}).WithLabel(gkelabels.ComputeClassLabel, "autopilot").Build(),
					test.BuildTestPod("pod-3", 32000, 100)),
				framework.NewTestNodeInfo(
					test.BuildTestNode("node-4", 2000, 2000),
					test.BuildTestPod("pod-4", 2000, 100)),
			},
			unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
			expectedPods: []*apiv1.Pod{
				lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, machinetypes.EK.Name()),
				lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:workload-separation:yes", 1000, 1000, machinetypes.E4A.Name()),
				lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:cloud.google.com/compute-class:autopilot", 1000, 1000, machinetypes.E4.Name()),
				test.BuildTestPod("pod-1", 100, 100),
			},
		},
		{
			desc:                   "Multiple machine families - not enough requests on one family",
			launchStatus:           lookaheadbuffer_strategy.Enabled,
			maxWorkloadSeparations: 10,
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(
					node32(machinetypes.EK.Name(), "node-1", 1000, 1000),
					test.BuildTestPod("pod-1", 32000, 100)),
				framework.NewTestNodeInfo(
					node8(machinetypes.E4A.Name(), "node-2", 1000, 1000),
					test.BuildTestPod("pod-2", 8000, 100)),
			},
			unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
			expectedPods: []*apiv1.Pod{
				lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, machinetypes.EK.Name()),
				test.BuildTestPod("pod-1", 100, 100),
			},
		},
	}

	for _, family := range []string{machinetypes.EK.Name(), machinetypes.E4A.Name(), machinetypes.E4.Name()} {
		testCases = append(testCases, []struct {
			desc                   string
			launchStatus           lookaheadbuffer_strategy.Status
			maxWorkloadSeparations int
			crds                   []crd.CRD
			nodeInfos              []*framework.NodeInfo
			unschedulablePods      []*apiv1.Pod
			daemonSets             []*appsv1.DaemonSet
			fetchingDaemonSetsErr  error
			expectedPods           []*apiv1.Pod
			expectedErr            bool
		}{
			{
				desc:                   fmt.Sprintf("%ss in one workload ID - Not enough %s pod requests for lookahead pods", family, family),
				launchStatus:           lookaheadbuffer_strategy.Enabled,
				maxWorkloadSeparations: 10,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node8(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 8000, 100)),
					framework.NewTestNodeInfo(
						test.BuildTestNode("node-2", 2000, 2000),
						test.BuildTestPod("pod-1", 8000, 100)),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss in two workload IDs - 32 %s CPUs per workload ID", family, family),
				launchStatus:           lookaheadbuffer_strategy.Enabled,
				maxWorkloadSeparations: 10,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
					framework.NewTestNodeInfo(
						node32WithWorkloadSeparation(family, "node-2", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, family),
					lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:workload-separation:yes", 1000, 1000, family),
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss in two workload IDs - 32 %s CPUs per workload ID - only default chosen due to maxWorkloadSeparations limit", family, family),
				launchStatus:           lookaheadbuffer_strategy.Enabled,
				maxWorkloadSeparations: 0,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
					framework.NewTestNodeInfo(
						node32WithWorkloadSeparation(family, "node-2", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, family),
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss in three workload IDs - limited to default and 1 extra workload separation", family),
				launchStatus:           lookaheadbuffer_strategy.Enabled,
				maxWorkloadSeparations: 1,
				crds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("autopilot"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithAutopilotManaged(),
						crd.WithRules([]rules.Rule{
							rules.NewRule(rules.WithPodFamilyRule(ptr.To("general-purpose"))),
						})),
				},
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
					framework.NewTestNodeInfo(
						node32WithWorkloadSeparation(family, "node-2", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
					framework.NewTestNodeInfo(
						ekvms_test.NewNodeBuilderFromNode(node32(family, "node-3", 32000, 128*giBToKiB*size.KiB)).WithTaint(apiv1.Taint{
							Key:    gkelabels.ComputeClassLabel,
							Value:  "autopilot",
							Effect: apiv1.TaintEffectNoSchedule,
						}).WithLabel(gkelabels.ComputeClassLabel, "autopilot").Build(),
						test.BuildTestPod("pod-1", 32000, 100)),
					framework.NewTestNodeInfo(
						ekvms_test.NewNodeBuilderFromNode(node32(family, "node-4", 32000, 128*giBToKiB*size.KiB)).WithTaint(apiv1.Taint{
							Key:    gkelabels.ComputeClassLabel,
							Value:  "autopilot",
							Effect: apiv1.TaintEffectNoSchedule,
						}).WithLabel(gkelabels.ComputeClassLabel, "autopilot").Build(),
						test.BuildTestPod("pod-1", 32000, 100)),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, family),
					lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:cloud.google.com/compute-class:autopilot", 1000, 1000, family, lookaheadbuffer.WithPosition(0)),
					lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:cloud.google.com/compute-class:autopilot", 1000, 1000, family, lookaheadbuffer.WithPosition(1)),
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss in one workload ID - 32 %s CPUs per workload ID - Lookahead disabled", family, family),
				launchStatus:           lookaheadbuffer_strategy.Disabled,
				maxWorkloadSeparations: 10,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss in one workload ID - 32 %s CPUs per workload ID - Status Unspecified", family, family),
				launchStatus:           lookaheadbuffer_strategy.Unspecified,
				maxWorkloadSeparations: 10,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss in two workload IDs - 32 %s CPUs per workload ID - DS with 2 containers each 200 mCPU 200 bytes", family, family),
				launchStatus:           lookaheadbuffer_strategy.Enabled,
				maxWorkloadSeparations: 10,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
					framework.NewTestNodeInfo(
						node32WithWorkloadSeparation(family, "node-2", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
				},
				daemonSets: []*appsv1.DaemonSet{
					newDaemonSet("ds-1", 2, 200, 200, nil),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					lookaheadbuffer.BuildTestLookaheadPod("", 600, 600, family),
					lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:workload-separation:yes", 1000, 1000, family),
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss in one workload ID - 32 %s CPUs per workload ID - DS with 0 containers", family, family),
				launchStatus:           lookaheadbuffer_strategy.Enabled,
				maxWorkloadSeparations: 10,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
				},
				daemonSets: []*appsv1.DaemonSet{
					newDaemonSet("ds-1", 0, 250, 200, nil),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, family),
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss in one workload ID - 32 default %s CPUs - DS bigger than lookahead pod - no error", family, family),
				launchStatus:           lookaheadbuffer_strategy.Enabled,
				maxWorkloadSeparations: 10,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
				},
				daemonSets: []*appsv1.DaemonSet{
					newDaemonSet("ds-1", 1, 2000, 2000, nil),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss in one workload ID - 32 default %s CPUs - error during fetching daemonSets", family, family),
				launchStatus:           lookaheadbuffer_strategy.Enabled,
				maxWorkloadSeparations: 10,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
				},
				fetchingDaemonSetsErr: errors.New("error fetching daemonSets"),
				unschedulablePods:     []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
			{
				desc:                   fmt.Sprintf("%ss: Mixed nodes - 64 default CPUs", family),
				launchStatus:           lookaheadbuffer_strategy.Enabled,
				maxWorkloadSeparations: 10,
				nodeInfos: []*framework.NodeInfo{
					framework.NewTestNodeInfo(
						node32(family, "node-1", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
					framework.NewTestNodeInfo(
						node8(family, "node-2", 1000, 1000),
						test.BuildTestPod("pod-1", 8000, 100)),
					framework.NewTestNodeInfo(
						node8(family, "node-3", 1000, 1000),
						test.BuildTestPod("pod-1", 8000, 100)),
					framework.NewTestNodeInfo(
						node8(family, "node-4", 1000, 1000),
						test.BuildTestPod("pod-1", 8000, 100)),
					framework.NewTestNodeInfo(
						node8(family, "node-5", 1000, 1000),
						test.BuildTestPod("pod-1", 8000, 100)),
					framework.NewTestNodeInfo(
						node32WithWorkloadSeparation(family, "node-6", 1000, 1000),
						test.BuildTestPod("pod-1", 32000, 100)),
					framework.NewTestNodeInfo(
						node8WithWorkloadSeparation(family, "node-7", 1000, 1000),
						test.BuildTestPod("pod-1", 8000, 100)),
					framework.NewTestNodeInfo(
						test.BuildTestNode("node-8", 2000, 2000),
						test.BuildTestPod("pod-1", 2000, 100)),
				},
				unschedulablePods: []*apiv1.Pod{test.BuildTestPod("pod-1", 100, 100)},
				expectedPods: []*apiv1.Pod{
					lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, family, lookaheadbuffer.WithPosition(0)),
					lookaheadbuffer.BuildTestLookaheadPod("", 1000, 1000, family, lookaheadbuffer.WithPosition(1)),
					lookaheadbuffer.BuildTestLookaheadPod("NoSchedule:workload-separation:yes", 1000, 1000, family),
					test.BuildTestPod("pod-1", 100, 100),
				},
			},
		}...)
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			snapshot, _, err := testsnapshot.NewTestSnapshotAndHandle()
			assert.NoError(t, err)
			if tc.nodeInfos != nil {
				for _, nodeInfo := range tc.nodeInfos {
					err := snapshot.AddNodeInfo(nodeInfo)
					if err != nil {
						t.Errorf("Failed to add NodeInfos to snapshot: %v", err)
					}
				}
			}

			daemonSetLister := &mockDSLister{}
			daemonSetLister.On("List", apilabels.Everything()).Return(tc.daemonSets, tc.fetchingDaemonSetsErr)
			ctx := &ca_context.AutoscalingContext{
				ClusterSnapshot: snapshot,
				AutoscalingKubeClients: ca_context.AutoscalingKubeClients{
					ListerRegistry: kubernetes.NewListerRegistry(nil, nil, nil, nil, daemonSetLister, nil, nil, nil, nil),
				},
			}

			laPodProvider := fakeLookaheadPodProvider{}
			strategyProvider := &mockStrategyProvider{}
			strategyProvider.On("Strategy", mock.Anything).Return(lookaheadbuffer_strategy.LookaheadPodStrategy{Status: tc.launchStatus}, nil)

			metrics := &mockMetrics{}
			metrics.On("UpdateLookaheadPodsCount", mock.Anything).Once()

			cccLister := lister.NewMockCrdListerWithLabel(tc.crds, gkelabels.ComputeClassLabel)
			mcp := machinetypes.NewMachineConfigProvider(nil)
			p := NewLookaheadPodInjectionProcessor(
				&laPodProvider,
				strategyProvider,
				&mockWorkloadSeparationLimiter{limit: tc.maxWorkloadSeparations},
				mcp,
				systempods.NewClassifier([]string{"kube-system"}),
				cccLister,
				calculator_test.NewWithProvider(mcp),
				metrics)

			actualPods, actualErr := p.Process(context.TODO(), ctx, tc.unschedulablePods)
			if tc.expectedErr {
				assert.Error(t, actualErr)
			} else {
				assert.NoError(t, actualErr)
			}

			assert.ElementsMatch(t, actualPods, tc.expectedPods)
			metrics.AssertExpectations(t)
		})
	}
}

func TestProcessMetricsOnErrors(t *testing.T) {
	testCases := []struct {
		desc             string
		launchStatus     lookaheadbuffer_strategy.Status
		calculatorErr    error
		listNodeInfosErr error
		expectedErr      string
	}{
		{
			desc:         "Processor Disabled - emits empty metric",
			launchStatus: lookaheadbuffer_strategy.Disabled,
		},
		{
			desc:          "Nil sampleNode - emits empty metric and returns error",
			launchStatus:  lookaheadbuffer_strategy.Enabled,
			calculatorErr: errors.New("calculator error"),
			expectedErr:   "sample node is nil",
		},
		{
			desc:             "ListNodeInfos error - emits empty metric and returns error",
			launchStatus:     lookaheadbuffer_strategy.Enabled,
			listNodeInfosErr: errors.New("snapshot error"),
			expectedErr:      "failed to list nodeInfos",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			strategyProvider := &mockStrategyProvider{}
			strategyProvider.On("Strategy", mock.Anything).Return(lookaheadbuffer_strategy.LookaheadPodStrategy{Status: tc.launchStatus}, nil)

			metrics := &mockMetrics{}
			// Expect nil for empty map from UpdateLookaheadPodsCount
			metrics.On("UpdateLookaheadPodsCount", map[size.Allocatable]int{}).Once()

			mcp := machinetypes.NewMachineConfigProvider(nil)
			mockCalc := &mockCalculator{
				Calculator:               calculator_test.NewWithProvider(mcp),
				getMaxResizableVmSizeErr: tc.calculatorErr,
			}
			p := NewLookaheadPodInjectionProcessor(
				nil,
				strategyProvider,
				nil,
				mcp,
				nil,
				nil,
				mockCalc,
				metrics)

			ctx := &ca_context.AutoscalingContext{}
			if tc.listNodeInfosErr != nil {
				ctx.ClusterSnapshot = &mockSnapshot{listNodeInfosErr: tc.listNodeInfosErr}
			} else {
				snapshot, _, _ := testsnapshot.NewTestSnapshotAndHandle()
				ctx.ClusterSnapshot = snapshot
			}

			_, err := p.Process(context.TODO(), ctx, nil)
			if tc.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			} else {
				assert.NoError(t, err)
			}
			metrics.AssertExpectations(t)
		})
	}
}

func TestEmitLookaheadPodsCountMetric(t *testing.T) {
	testCases := []struct {
		desc                 string
		pods                 []*apiv1.Pod
		expectedEmittedCount map[size.Allocatable]int
	}{
		{
			desc:                 "No pods",
			pods:                 []*apiv1.Pod{},
			expectedEmittedCount: map[size.Allocatable]int{},
		},
		{
			desc: "One pod",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 2000, 4*size.GiB),
			},
			expectedEmittedCount: map[size.Allocatable]int{
				{MilliCpus: 2000, KBytes: 4 * giBToKiB}: 1,
			},
		},
		{
			desc: "Multiple pods with same size",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 2000, 4*size.GiB),
				test.BuildTestPod("pod-2", 2000, 4*size.GiB),
				test.BuildTestPod("pod-3", 2000, 4*size.GiB),
			},
			expectedEmittedCount: map[size.Allocatable]int{
				{MilliCpus: 2000, KBytes: 4 * giBToKiB}: 3,
			},
		},
		{
			desc: "Multiple pods with different sizes",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 2000, 4*size.GiB),
				test.BuildTestPod("pod-2", 2500, 4.5*size.GiB),
				test.BuildTestPod("pod-3", 3000, 5*size.GiB),
				test.BuildTestPod("pod-4", 4000, 6*size.GiB),
			},
			expectedEmittedCount: map[size.Allocatable]int{
				{MilliCpus: 2000, KBytes: 4 * giBToKiB}: 1,
				{MilliCpus: 3000, KBytes: 8 * giBToKiB}: 2,
				{MilliCpus: 4000, KBytes: 8 * giBToKiB}: 1,
			},
		},
		{
			desc: "One pod with requests requiring rounding up",
			pods: []*apiv1.Pod{
				test.BuildTestPod("pod-1", 1200, 3*size.GiB),
			},
			expectedEmittedCount: map[size.Allocatable]int{
				{MilliCpus: 2000, KBytes: 4 * giBToKiB}: 1,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			metricsMock := &mockMetrics{}
			p := &LookaheadPodInjectionProcessor{
				metrics: metricsMock,
			}
			metricsMock.On("UpdateLookaheadPodsCount", tc.expectedEmittedCount).Once()
			p.emitLookaheadPodsCountMetric(tc.pods)
			metricsMock.AssertExpectations(t)
		})
	}
}

func TestDaemonSetRequests(t *testing.T) {
	testCases := []struct {
		desc             string
		daemonSet        *appsv1.DaemonSet
		expectedMilliCpu int64
		expectedMemBytes int64
	}{
		{
			desc:             "DaemonSet with no containers",
			daemonSet:        newDaemonSet("ds-no-resources", 0, 0, 0, nil),
			expectedMilliCpu: 0,
			expectedMemBytes: 0,
		},
		{
			desc:             "DaemonSet with one container",
			daemonSet:        newDaemonSet("ds-one-container", 1, 250, 200, nil),
			expectedMilliCpu: 250,
			expectedMemBytes: 200,
		},
		{
			desc:             "DaemonSet with two containers",
			daemonSet:        newDaemonSet("ds-two-containers", 2, 250, 200, nil),
			expectedMilliCpu: 500,
			expectedMemBytes: 400,
		},
		{
			desc:             "DaemonSet with init container with higher requests",
			daemonSet:        withInitContainers(newDaemonSet("ds-init-container-higher", 1, 100, 100, nil), 300, 400),
			expectedMilliCpu: 300,
			expectedMemBytes: 400,
		},
		{
			desc:             "DaemonSet with init container with lower requests",
			daemonSet:        withInitContainers(newDaemonSet("ds-init-container-lower", 1, 300, 400, nil), 100, 100),
			expectedMilliCpu: 300,
			expectedMemBytes: 400,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			resourceList := daemonSetRequests(tc.daemonSet)
			cpu := resourceList[apiv1.ResourceCPU]
			memory := resourceList[apiv1.ResourceMemory]
			assert.Equal(t, tc.expectedMilliCpu, cpu.MilliValue())
			assert.Equal(t, tc.expectedMemBytes, memory.Value())
		})
	}
}

func TestStringifyResourceList(t *testing.T) {
	testCases := []struct {
		name     string
		input    apiv1.ResourceList
		expected string
	}{
		{
			name:     "empty resource list",
			input:    apiv1.ResourceList{},
			expected: "map[]",
		},
		{
			name: "multiple resources",
			input: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(800, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(1024*1024*1024, resource.BinarySI),
			},
			expected: "map[cpu:800m memory:1Gi]",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := stringifyResourceList(tc.input)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

type fakeLookaheadPodProvider struct{}

// GetLookaheadPods returns lookahead pods number equal to floor(cpus/32).
func (s *fakeLookaheadPodProvider) GetLookaheadPods(cpus int, workloadID, machineFamily string) []*apiv1.Pod {
	laNum := cpus / 32
	return lookaheadbuffer.GenerateLookaheadPods(laNum, *resource.NewMilliQuantity(1000, resource.DecimalSI), *resource.NewQuantity(1000, resource.BinarySI), workloadID, machineFamily)
}

func node32(family, name string, milliCpu, bytes int64) *apiv1.Node {
	switch family {
	case machinetypes.EK.Name():
		return ekvms_test.EkNode32(name, milliCpu, bytes)
	case machinetypes.E4A.Name():
		return ekvms_test.E4aNode32(name, milliCpu, bytes)
	case machinetypes.E4.Name():
		return ekvms_test.E4Node32(name, milliCpu, bytes)
	default:
		panic("unknown machine family: " + family)
	}
}

func node8(family, name string, milliCpu, bytes int64) *apiv1.Node {
	switch family {
	case machinetypes.EK.Name():
		return ekvms_test.EkNode8(name, milliCpu, bytes)
	case machinetypes.E4A.Name():
		return ekvms_test.E4aNode8(name, milliCpu, bytes)
	case machinetypes.E4.Name():
		return ekvms_test.E4Node8(name, milliCpu, bytes)
	default:
		panic("unknown machine family: " + family)
	}
}

func node32WithWorkloadSeparation(family, name string, milliCpu, bytes int64) *apiv1.Node {
	return ekvms_test.NewNodeBuilderFromNode(node32(family, name, milliCpu, bytes)).WithTaint(apiv1.Taint{
		Key:    "workload-separation",
		Value:  "yes",
		Effect: apiv1.TaintEffectNoSchedule,
	}).WithLabel("workload-separation", "yes").Build()
}

func node8WithWorkloadSeparation(family, name string, milliCpu, bytes int64) *apiv1.Node {
	return ekvms_test.NewNodeBuilderFromNode(node8(family, name, milliCpu, bytes)).WithTaint(apiv1.Taint{
		Key:    "workload-separation",
		Value:  "yes",
		Effect: apiv1.TaintEffectNoSchedule,
	}).WithLabel("workload-separation", "yes").Build()
}

func newTaintsConfig(taintKeys []string) taintutils.TaintConfig {
	return taintutils.NewTaintConfig(config.AutoscalingOptions{
		StartupTaints: taintKeys,
	})
}

func newDaemonSet(name string, containersNum int, milliCPUPerContainer, memBytesPerContainer int64, selector map[string]string) *appsv1.DaemonSet {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID(name),
		},
		Spec: appsv1.DaemonSetSpec{
			Template: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec{
					NodeSelector: selector,
				},
			},
		},
	}
	for i := range containersNum {
		ds.Spec.Template.Spec.Containers = append(ds.Spec.Template.Spec.Containers, apiv1.Container{
			Name: fmt.Sprintf("container-%d", i),
			Resources: apiv1.ResourceRequirements{
				Requests: apiv1.ResourceList{
					apiv1.ResourceCPU:    *resource.NewMilliQuantity(milliCPUPerContainer, resource.DecimalSI),
					apiv1.ResourceMemory: *resource.NewQuantity(memBytesPerContainer, resource.BinarySI),
				},
			},
		})
	}
	return ds
}

func withInitContainers(ds *appsv1.DaemonSet, milliCPUPerContainer, memBytesPerContainer int64) *appsv1.DaemonSet {
	ds.Spec.Template.Spec.InitContainers = []apiv1.Container{{
		Name: "init-container",
		Resources: apiv1.ResourceRequirements{
			Requests: apiv1.ResourceList{
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(milliCPUPerContainer, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(memBytesPerContainer, resource.BinarySI),
			},
		},
	}}
	return ds
}

func withToleration(ds *appsv1.DaemonSet, toleration apiv1.Toleration) *appsv1.DaemonSet {
	ds.Spec.Template.Spec.Tolerations = append(ds.Spec.Template.Spec.Tolerations, toleration)
	return ds
}

type mockDSLister struct {
	mock.Mock
}

func (m *mockDSLister) List(selector apilabels.Selector) (ret []*appsv1.DaemonSet, err error) {
	args := m.Called(selector)
	return args.Get(0).([]*appsv1.DaemonSet), args.Error(1)
}

func (m *mockDSLister) DaemonSets(namespace string) v1.DaemonSetNamespaceLister {
	args := m.Called(namespace)
	return args.Get(0).(v1.DaemonSetNamespaceLister)
}

func (m *mockDSLister) GetPodDaemonSets(pod *apiv1.Pod) ([]*appsv1.DaemonSet, error) {
	args := m.Called(pod)
	return args.Get(0).([]*appsv1.DaemonSet), args.Error(1)
}

func (m *mockDSLister) GetHistoryDaemonSets(history *appsv1.ControllerRevision) ([]*appsv1.DaemonSet, error) {
	args := m.Called(history)
	return args.Get(0).([]*appsv1.DaemonSet), args.Error(1)
}

// mockStrategyProvider is a mock implementation of strategyProvider.
type mockStrategyProvider struct {
	mock.Mock
}

func (m *mockStrategyProvider) Strategy(machineFamily string) (lookaheadbuffer_strategy.LookaheadPodStrategy, error) {
	args := m.Called(machineFamily)
	return args.Get(0).(lookaheadbuffer_strategy.LookaheadPodStrategy), args.Error(1)
}

type mockWorkloadSeparationLimiter struct {
	limit int
}

func (m *mockWorkloadSeparationLimiter) Limit(requestsByFamily map[string]map[string]apiv1.ResourceList) map[string]map[string]apiv1.ResourceList {
	limited := map[string]map[string]apiv1.ResourceList{}
	for family, requests := range requestsByFamily {
		limited[family] = limitRequestsForFamily(requests, m.limit)
	}
	return limited
}

type mockCalculator struct {
	calculator.Calculator
	getMaxResizableVmSizeErr error
}

func (m *mockCalculator) GetMaxResizableVmSizeByMachineType(machineType string) (size.VmSize, error) {
	if m.getMaxResizableVmSizeErr != nil {
		return size.VmSize{}, m.getMaxResizableVmSizeErr
	}
	return m.Calculator.GetMaxResizableVmSizeByMachineType(machineType)
}

type mockSnapshot struct {
	clustersnapshot.ClusterSnapshot
	listNodeInfosErr error
}

func (m *mockSnapshot) ListNodeInfos() ([]*framework.NodeInfo, error) {
	return nil, m.listNodeInfosErr
}

type mockMetrics struct {
	mock.Mock
}

func (m *mockMetrics) UpdateLookaheadPodsCount(laPodsCount map[size.Allocatable]int) {
	m.MethodCalled("UpdateLookaheadPodsCount", laPodsCount)
}
