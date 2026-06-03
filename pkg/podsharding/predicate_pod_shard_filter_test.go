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

package podsharding

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/autoprovisioning/machineselection"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

func TestGetExpansionMachineTypeName(t *testing.T) {
	var testCases = []struct {
		machineFamily       string
		expectedMachineType string
		gpuLabel            string
		tpuLabel            string
	}{
		{
			machineFamily:       "n1",
			expectedMachineType: machinetypes.N1.LargestAutoprovisionedMachineType(machinetypes.NoConstraints).Name,
		},
		{
			machineFamily:       "nXYZ",
			expectedMachineType: "t3", // this is done to match the test suite
			// as the test suite considers the last machine in the last as the largest
			// machine, whereas in actual family, the largest machine is chosen based
			// on approx. amount of its resources.
		},
		{
			machineFamily:       "n2",
			expectedMachineType: machinetypes.N2.LargestAutoprovisionedMachineType(machinetypes.NoConstraints).Name,
		},
		{
			expectedMachineType: machinetypes.N1.LargestAutoprovisionedMachineType(machinetypes.Constraints{GpuType: "nvidia-tesla-k80", CpuPlatform: machinetypes.AnyPlatform}).Name,
			gpuLabel:            "nvidia-tesla-k80",
		},
		{
			expectedMachineType: machinetypes.A2.LargestAutoprovisionedMachineType(machinetypes.Constraints{GpuType: machinetypes.NvidiaTeslaA100.Name(), CpuPlatform: machinetypes.AnyPlatform}).Name,
			gpuLabel:            machinetypes.NvidiaTeslaA100.Name(),
		},
		{
			expectedMachineType: machinetypes.A2.LargestAutoprovisionedMachineType(machinetypes.Constraints{GpuType: machinetypes.NvidiaA100_80gb.Name(), CpuPlatform: machinetypes.AnyPlatform}).Name,
			gpuLabel:            machinetypes.NvidiaA100_80gb.Name(),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.machineFamily, func(t *testing.T) {
			cloudProvider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineTypes("t1", "t2", "t3").
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()
			selector := machineselection.Selector{CloudProvider: cloudProvider}
			mp := make(map[string]podrequirements.Values)
			if testCase.machineFamily != "" {
				mp["cloud.google.com/machine-family"] = podrequirements.NewValues(testCase.machineFamily)
			}
			requirements := &podrequirements.Requirements{LabelReq: podrequirements.NewLabelRequirements(mp)}
			assert.Equal(t, testCase.expectedMachineType, getExpansionMachineTypeName(selector, requirements, testCase.gpuLabel, testCase.tpuLabel))
		})
	}
}

func TestPredicatePodShardFilterFilterPods(t *testing.T) {
	cccName := "ccc-name"
	tests := []struct {
		name             string
		selectedPodShard *PodShard
		allPodShards     []*PodShard
		pods             []*apiv1.Pod
		csnEnabled       bool
		want             PodFilteringResult
		wantErr          bool
	}{
		{
			name:             "simple test case without provreq",
			selectedPodShard: createTestPodShard("", []string{"label1", "label2"}, "1", "2"),
			allPodShards: []*PodShard{
				createTestPodShard("", []string{"label1", "label2"}, "1", "2"),
				createTestPodShard("", []string{"label1"}, "3", "4"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
				createTestPod("4"),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
					createTestPod("2"),
					createTestPod("3"),
					createTestPod("4"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "pods from provreq are not expanded with regular pods",
			selectedPodShard: createTestPodShard("provreq", []string{"label1", "label2"}, "1", "2"),
			allPodShards: []*PodShard{
				createTestPodShard("provreq", []string{"label1", "label2"}, "1", "2"),
				createTestPodShard("", []string{"label1"}, "3", "4"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
				createTestPod("4"),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
					createTestPod("2"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "pods from first provreq are not expanded, second provreq is ignored",
			selectedPodShard: createTestPodShard("provreq1", []string{"label1", "label2"}, "1", "2"),
			allPodShards: []*PodShard{
				createTestPodShard("provreq1", []string{"label1", "label2"}, "1", "2"),
				createTestPodShard("", []string{"label1"}, "3", "4"),
				createTestPodShard("provreq2", []string{"label1", "label2"}, "5", "6"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
				createTestPod("4"),
				createTestPod("5"),
				createTestPod("6"),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
					createTestPod("2"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "pods from regular shard are NOT expanded with provreq-shard",
			selectedPodShard: createTestPodShard("", []string{"label1"}, "3", "4"),
			allPodShards: []*PodShard{
				createTestPodShard("pr", []string{"label1"}, "1", "2"),
				createTestPodShard("", []string{"label1"}, "3", "4"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
				createTestPod("4"),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("3"),
					createTestPod("4"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "CSN pods from different buffers are not mixed",
			selectedPodShard: createTestCSNPodShard("buffer1", "1"),
			allPodShards: []*PodShard{
				createTestCSNPodShard("buffer1", "1"),
				createTestCSNPodShard("buffer2", "2"),
				createTestPodShard("", nil, "3"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
			},
			csnEnabled: true,
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "non-CSN pods are not expanded with CSN pods",
			selectedPodShard: createTestPodShard("", nil, "3"),
			allPodShards: []*PodShard{
				createTestCSNPodShard("buffer1", "1"),
				createTestPodShard("", nil, "3"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("3"),
			},
			csnEnabled: true,
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("3"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "CSN pods are not expanded with regular pods",
			selectedPodShard: createTestCSNPodShard("buffer1", "1"),
			allPodShards: []*PodShard{
				createTestCSNPodShard("buffer1", "1"),
				createTestPodShard("", nil, "3"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("3"),
			},
			csnEnabled: true,
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "CSN pods are expanded with regular pods when csnEnabled=false",
			selectedPodShard: createTestCSNPodShard("buffer1", "1"),
			allPodShards: []*PodShard{
				createTestCSNPodShard("buffer1", "1"),
				createTestPodShard("", nil, "3"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("3"),
			},
			csnEnabled: false,
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
					createTestPod("3"),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "plain pods leak into untainted CCC shard",
			selectedPodShard: createTestPodShardWithRequirement(nil, gkelabels.ComputeClassLabel, cccName, "pod-ccc-1"),
			allPodShards: []*PodShard{
				createTestPodShardWithRequirement(nil, gkelabels.ComputeClassLabel, cccName, "pod-ccc-1"),
				createTestPodShard("", nil, "pod-plain-2"),
			},
			pods: []*apiv1.Pod{
				addPodRequirement(createTestPod("pod-ccc-1"), gkelabels.ComputeClassLabel, cccName),
				createTestPod("pod-plain-2"),
			},
			csnEnabled: false,
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					addPodRequirement(createTestPod("pod-ccc-1"), gkelabels.ComputeClassLabel, cccName),
					createTestPod("pod-plain-2"),
				},
				ZoneAgnostic: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			machineTypes := []string{"n1-standard-1", "n1-standard-2", "n1-standard-4", "n1-standard-8"}
			machineTemplates := make(map[string]*framework.NodeInfo, len(machineTypes))
			for _, machineType := range machineTypes {
				frameworkNode := framework.NewTestNodeInfo(test.BuildTestNode(machineType+"-node", 1, 1))
				machineTemplates[machineType] = frameworkNode
			}

			ctx := &context.AutoscalingContext{
				CloudProvider:   gke.NewTestAutoprovisioningCloudProviderBuilder().WithMachineTypes(machineTypes...).WithNodeLocations("us-central1-c").WithAutopilotEnabled(true).Build(),
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
			}

			crdLister := lister.NewMockCrdLister([]crd.CRD{})
			p := NewPredicatePodShardFilter(crdLister, tt.csnEnabled)
			got, err := p.FilterPods(ctx, tt.selectedPodShard, tt.allPodShards, tt.pods)
			if (err != nil) != tt.wantErr {
				t.Errorf("PredicatePodShardFilter.FilterPods() error = %v\nwantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PredicatePodShardFilter.FilterPods() = %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestPredicatePodShardFilterFilterPodsZoneAgnosticity(t *testing.T) {
	crdLabel := gkelabels.ComputeClassLabel
	crdName := "ccc-name"
	crdNameOther := "ccc-other"
	machineTypes := []string{"n1-standard-1", "n1-standard-2", "n1-standard-4", "n1-standard-8"}

	locationRule := rules.NewRule(rules.WithLocationRule([]string{"us-central1-a"}))
	machineTypeRule := rules.NewRule(rules.WithMachineTypeRule(&machineTypes[0]))
	locationRuleCRD := crd.NewTestCrd(
		crd.WithRules([]rules.Rule{locationRule}),
		crd.WithLabel(crdLabel),
		crd.WithName(crdName),
		crd.WithAutoprovisioningEnabled(),
	)
	machineTypeRuleCRD := crd.NewTestCrd(
		crd.WithRules([]rules.Rule{machineTypeRule}),
		crd.WithLabel(crdLabel),
		crd.WithName(crdNameOther),
		crd.WithAutoprovisioningEnabled(),
	)

	tests := []struct {
		name             string
		selectedPodShard *PodShard
		allPodShards     []*PodShard
		pods             []*apiv1.Pod
		csnEnabled       bool
		want             PodFilteringResult
		wantErr          bool
	}{
		{
			name:             "simple test case without provreq",
			selectedPodShard: createTestPodShard("", []string{"label1", "label2"}, "1", "2"),
			allPodShards: []*PodShard{
				createTestPodShard("", []string{"label1", "label2"}, "1", "2"),
				createTestPodShard("", []string{"label1"}, "3", "4"),
			},
			pods: []*apiv1.Pod{
				createTestPod("1"),
				createTestPod("2"),
				createTestPod("3"),
				createTestPod("4"),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					createTestPod("1"),
					createTestPod("2"),
					createTestPod("3"),
					createTestPod("4"),
				},
				ZoneAgnostic: true,
			},
		},
		{
			name:             "ccc with zonal preferences turns off zone agnosticity",
			selectedPodShard: createTestPodShardWithRequirement([]string{"label1", "label2"}, crdLabel, crdName, "1", "2"),
			allPodShards: []*PodShard{
				createTestPodShardWithRequirement([]string{"label1", "label2"}, crdLabel, crdName, "1", "2"),
				createTestPodShardWithRequirement([]string{"label1"}, crdLabel, crdName, "3", "4"),
			},
			pods: []*apiv1.Pod{
				addPodRequirement(createTestPod("1"), crdLabel, crdName),
				addPodRequirement(createTestPod("2"), crdLabel, crdName),
				addPodRequirement(createTestPod("3"), crdLabel, crdName),
				addPodRequirement(createTestPod("4"), crdLabel, crdName),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					addPodRequirement(createTestPod("1"), crdLabel, crdName),
					addPodRequirement(createTestPod("2"), crdLabel, crdName),
					addPodRequirement(createTestPod("3"), crdLabel, crdName),
					addPodRequirement(createTestPod("4"), crdLabel, crdName),
				},
				ZoneAgnostic: false,
			},
		},
		{
			name:             "ccc without zonal preferences doesn't influence zone agnosticity",
			selectedPodShard: createTestPodShardWithRequirement([]string{"label1", "label2"}, crdLabel, crdNameOther, "1", "2"),
			allPodShards: []*PodShard{
				createTestPodShardWithRequirement([]string{"label1", "label2"}, crdLabel, crdNameOther, "1", "2"),
				createTestPodShardWithRequirement([]string{"label1"}, crdLabel, crdNameOther, "3", "4"),
			},
			pods: []*apiv1.Pod{
				addPodRequirement(createTestPod("1"), crdLabel, crdNameOther),
				addPodRequirement(createTestPod("2"), crdLabel, crdNameOther),
				addPodRequirement(createTestPod("3"), crdLabel, crdNameOther),
				addPodRequirement(createTestPod("4"), crdLabel, crdNameOther),
			},
			want: PodFilteringResult{
				Pods: []*apiv1.Pod{
					addPodRequirement(createTestPod("1"), crdLabel, crdNameOther),
					addPodRequirement(createTestPod("2"), crdLabel, crdNameOther),
					addPodRequirement(createTestPod("3"), crdLabel, crdNameOther),
					addPodRequirement(createTestPod("4"), crdLabel, crdNameOther),
				},
				ZoneAgnostic: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			machineTemplates := make(map[string]*framework.NodeInfo, len(machineTypes))
			for _, machineType := range machineTypes {
				frameworkNode := framework.NewTestNodeInfo(test.BuildTestNode(machineType+"-node", 1, 1))
				machineTemplates[machineType] = frameworkNode
			}
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithMachineTypes(machineTypes...).
				WithNodeLocations("us-central1-a", "us-central1-b").
				WithAutopilotEnabled(true).
				Build()
			ctx := &context.AutoscalingContext{
				CloudProvider:   provider,
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
			}
			crdLister := lister.NewMockCrdLister([]crd.CRD{locationRuleCRD, machineTypeRuleCRD})
			crdLister.SetCrdLabel(crdLabel)
			p := NewPredicatePodShardFilter(crdLister, tt.csnEnabled)
			got, err := p.FilterPods(ctx, tt.selectedPodShard, tt.allPodShards, tt.pods)
			if (err != nil) != tt.wantErr {
				t.Errorf("PredicatePodShardFilter.FilterPods() error = %v\nwantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PredicatePodShardFilter.FilterPods() = %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestPredicatePodShardFilterNoGPUSelector(t *testing.T) {
	podNoSelector := createTestGPUPod("podNoSelector")
	podK80 := createTestGPUPod("podK80")
	podP100 := createTestGPUPod("podP100")
	addPodRequirement(podK80, gkelabels.GPULabel, machinetypes.NvidiaTeslaK80.Name())
	addPodRequirement(podP100, gkelabels.GPULabel, machinetypes.NvidiaTeslaP100.Name())

	tests := []struct {
		name       string
		pods       []*apiv1.Pod
		wantShards map[string][]*apiv1.Pod // shardByGPULabel -> shardPods
	}{
		{
			name: "allPods",
			pods: []*apiv1.Pod{podNoSelector, podK80, podP100},
			wantShards: map[string][]*apiv1.Pod{
				machinetypes.NvidiaTeslaK80.Name():  {podK80, podNoSelector},
				machinetypes.NvidiaTeslaP100.Name(): {podP100, podNoSelector},
				"":                                  {podNoSelector},
			},
		},
		{
			name: "noGenericPods",
			pods: []*apiv1.Pod{podK80, podP100},
			wantShards: map[string][]*apiv1.Pod{
				machinetypes.NvidiaTeslaK80.Name():  {podK80},
				machinetypes.NvidiaTeslaP100.Name(): {podP100},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().WithAutoprovisioningEnabled(false).WithNodeLocations("us-central1-c").Build()
			sharder := NewGkePodSharder(provider, false, nil)
			ctx := &context.AutoscalingContext{CloudProvider: provider, ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t)}
			p := NewPredicatePodShardFilter(lister.NewMockCrdLister([]crd.CRD{}), false)

			shards := sharder.ComputePodShards(tc.pods)
			assert.Equal(t, len(tc.wantShards), len(shards))

			for _, shard := range shards {
				assert.True(t, shard.NodeGroupDescriptor.RequiresGPU)
				gpuLabel, _ := shard.NodeGroupDescriptor.SystemLabels[gkelabels.GPULabel]

				wantPods, ok := tc.wantShards[gpuLabel]
				assert.True(t, ok)

				got, err := p.FilterPods(ctx, shard, shards, tc.pods)
				assert.NoError(t, err)
				assert.ElementsMatch(t, wantPods, got.Pods, "mismatch for shard %q", gpuLabel)
			}
		})
	}
}

func createTestGPUPod(uid string) *apiv1.Pod {
	pod := createTestPod(uid)
	pod.Spec.Containers[0].Resources.Requests[apiv1.ResourceName(gpu.ResourceNvidiaGPU)] = resource.MustParse("1")
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{Key: gpu.ResourceNvidiaGPU, Operator: apiv1.TolerationOpExists, Effect: apiv1.TaintEffectNoSchedule})
	return pod
}
