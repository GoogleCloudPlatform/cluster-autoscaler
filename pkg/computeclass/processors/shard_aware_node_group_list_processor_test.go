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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

func TestShardAwareNodeGroupListProcessor(t *testing.T) {
	testCrdLabel := "test-crd-label"
	defaultMachineType := "n1-standard-1"

	gkeManager := &gke.GkeManagerMock{}
	gkeManager.On("IsDataplaneV2Enabled").Return(true)

	testCases := []struct {
		name              string
		nodegroups        []cloudprovider.NodeGroup
		crds              []crd.CRD
		unschedulablePods []*apiv1.Pod
		wantNodegroups    []cloudprovider.NodeGroup
	}{
		{
			name: "Filter out non-CCC node groups and CCC B node groups if whole pod shard is CCC A",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool-2").
					SetGceRefName("ccc-pool-2").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-2"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(ccc.CrdType),
					crd.WithName("ccc-object-1"),
					crd.WithRules([]rules.Rule{rules.NewRule(rules.WithNodePoolsRule([]string{"ccc-pool"}))}),
					crd.WithAutoprovisioningEnabled()),
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(ccc.CrdType),
					crd.WithName("ccc-object-2"),
					crd.WithRules([]rules.Rule{rules.NewRule(rules.WithNodePoolsRule([]string{"ccc-pool-2"}))}),
					crd.WithAutoprovisioningEnabled()),
			},
			unschedulablePods: []*apiv1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod-ccc",
					},
					Spec: apiv1.PodSpec{
						NodeSelector: map[string]string{testCrdLabel: "ccc-object-1"},
					},
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
		},
		{
			name: "Filter out CCC node groups if whole pod shard is non-CCC",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(ccc.CrdType),
					crd.WithName("ccc-object-1"),
					crd.WithRules([]rules.Rule{rules.NewRule(rules.WithNodePoolsRule([]string{"ccc-pool"}))}),
					crd.WithAutoprovisioningEnabled()),
			},
			unschedulablePods: []*apiv1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod-standard",
					},
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
		},
		{
			name: "Do not filter if shard is mixed (Standard + CCC)",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(ccc.CrdType),
					crd.WithName("ccc-object-1"),
					crd.WithRules([]rules.Rule{rules.NewRule(rules.WithNodePoolsRule([]string{"ccc-pool"}))}),
					crd.WithAutoprovisioningEnabled()),
			},
			unschedulablePods: []*apiv1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod-plain",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod-ccc",
					},
					Spec: apiv1.PodSpec{
						NodeSelector: map[string]string{testCrdLabel: "ccc-object-1"},
					},
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
		},
		{
			name: "Keep CCC node group if whole pod shard is non-CCC but one pod tolerates it (Exists)",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(ccc.CrdType),
					crd.WithName("ccc-object-1"),
					crd.WithRules([]rules.Rule{rules.NewRule(rules.WithNodePoolsRule([]string{"ccc-pool"}))}),
					crd.WithAutoprovisioningEnabled()),
			},
			unschedulablePods: []*apiv1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod-standard-with-toleration",
					},
					Spec: apiv1.PodSpec{
						Tolerations: []apiv1.Toleration{
							{
								Key:      testCrdLabel,
								Operator: apiv1.TolerationOpExists,
							},
						},
					},
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
		},
		{
			name: "Keep CCC node group if whole pod shard is non-CCC but one pod tolerates it (Equal)",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(ccc.CrdType),
					crd.WithName("ccc-object-1"),
					crd.WithRules([]rules.Rule{rules.NewRule(rules.WithNodePoolsRule([]string{"ccc-pool"}))}),
					crd.WithAutoprovisioningEnabled()),
			},
			unschedulablePods: []*apiv1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod-standard-with-toleration",
					},
					Spec: apiv1.PodSpec{
						Tolerations: []apiv1.Toleration{
							{
								Key:      testCrdLabel,
								Operator: apiv1.TolerationOpEqual,
								Value:    "ccc-object-1",
							},
						},
					},
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
		},
		{
			name: "Filter out CCC node group if whole pod shard is non-CCC and pod tolerates different CCC",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(ccc.CrdType),
					crd.WithName("ccc-object-1"),
					crd.WithRules([]rules.Rule{rules.NewRule(rules.WithNodePoolsRule([]string{"ccc-pool"}))}),
					crd.WithAutoprovisioningEnabled()),
			},
			unschedulablePods: []*apiv1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod-standard-with-toleration",
					},
					Spec: apiv1.PodSpec{
						Tolerations: []apiv1.Toleration{
							{
								Key:      testCrdLabel,
								Operator: apiv1.TolerationOpEqual,
								Value:    "ccc-object-2", // Different CCC
							},
						},
					},
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
		},
		{
			name: "Keep CCC node group if whole pod shard is non-CCC but one pod has wildcard toleration",
			nodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
			crds: []crd.CRD{
				crd.NewTestCrd(crd.WithLabel(testCrdLabel),
					crd.WithCrdType(ccc.CrdType),
					crd.WithName("ccc-object-1"),
					crd.WithRules([]rules.Rule{rules.NewRule(rules.WithNodePoolsRule([]string{"ccc-pool"}))}),
					crd.WithAutoprovisioningEnabled()),
			},
			unschedulablePods: []*apiv1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod-standard-with-wildcard-toleration",
					},
					Spec: apiv1.PodSpec{
						Tolerations: []apiv1.Toleration{
							{
								Operator: apiv1.TolerationOpExists,
							},
						},
					},
				},
			},
			wantNodegroups: []cloudprovider.NodeGroup{
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("standard-pool").
					SetGceRefName("standard-pool").
					SetSpec(&gkeclient.NodePoolSpec{MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
				gke.NewTestGkeMigBuilder().
					SetNodePoolName("ccc-pool").
					SetGceRefName("ccc-pool").
					SetSpec(&gkeclient.NodePoolSpec{
						Labels:      map[string]string{testCrdLabel: "ccc-object-1"},
						MachineType: defaultMachineType}).
					SetGkeManager(gkeManager).
					Build(),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdLister(tc.crds)
			mockLister.SetCrdLabel(testCrdLabel)
			noOpProcessor := &nodegroups.NoOpNodeGroupListProcessor{}
			processor := NewShardAwareNodeGroupListProcessor(noOpProcessor, mockLister)

			node := apiv1.Node{}
			node.Labels = map[string]string{}
			for _, ng := range tc.nodegroups {
				gkeManager.On("GetMigTemplateNodeInfo", ng).Return(framework.NewTestNodeInfo(&node), nil)
			}

			gotNodegroups, _, err := processor.Process(nil, tc.nodegroups, nil, tc.unschedulablePods)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.wantNodegroups), len(gotNodegroups))
			for i := range gotNodegroups {
				assert.Equal(t, tc.wantNodegroups[i].Id(), gotNodegroups[i].Id())
			}
		})
	}
}
