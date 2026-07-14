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
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func TestPodSharderWithoutProvReq(t *testing.T) {
	testCases := []shardComputeFunctionTestCase{
		{
			name:                        "no gpu",
			pod:                         v1.Pod{},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{},
		},
		{
			name: "any gpu",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("3"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				RequiresGPU: true,
			},
		},
		{
			name: "k80",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						gkelabels.GPULabel: "nvidia-tesla-k80",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					gkelabels.GPULabel: "nvidia-tesla-k80",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("8"),
				},
			},
		},
		{
			name: "k80 with time sharing",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						gkelabels.GPULabel:                 "nvidia-tesla-k80",
						gkelabels.GPUSharingStrategyLabel:  "time-sharing",
						gkelabels.GPUMaxSharedClientsLabel: "3",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					gkelabels.GPULabel: "nvidia-tesla-k80",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("24"),
				},
			},
		},
		{
			name: "k80 with time sharing and default max time shared clients",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						gkelabels.GPULabel:                "nvidia-tesla-k80",
						gkelabels.GPUSharingStrategyLabel: "time-sharing",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					gkelabels.GPULabel: "nvidia-tesla-k80",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("16"),
				},
			},
		},
		{
			name: "GPU sharing and partitioning at once",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						gkelabels.GPULabel:                 "nvidia-tesla-a100",
						gkelabels.GPUSharingStrategyLabel:  "time-sharing",
						gkelabels.GPUPartitionSizeLabel:    "1g.5gb",
						gkelabels.GPUMaxSharedClientsLabel: "3",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					gkelabels.GPULabel: "nvidia-tesla-a100",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("336"),
				},
			},
		},
		{
			name: "k80 affinity",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      gkelabels.GPULabel,
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"nvidia-tesla-k80"},
											},
										},
									},
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					gkelabels.GPULabel: "nvidia-tesla-k80",
				},
				ExtraResources: map[string]resource.Quantity{
					gpu.ResourceNvidiaGPU: resource.MustParse("8"),
				},
			},
		},
		{
			name: "unknown gpu",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						gkelabels.GPULabel: "nvidia-tesla-blah",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									gpu.ResourceNvidiaGPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					gkelabels.GPULabel: "nvidia-tesla-blah",
				},
				ExtraResources: map[string]resource.Quantity{},
			},
		},
		{
			name: "any tpu",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									tpu.ResourceGoogleTPU: resource.MustParse("4"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					gkelabels.TPULabel: gkelabels.TpuV4LiteDeviceValue,
				},
				ExtraResources: map[string]resource.Quantity{
					tpu.ResourceGoogleTPU: resource.MustParse("4"),
				},
			},
		},
		{
			name: "tpu_device",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						gkelabels.TPULabel: gkelabels.TpuV4LiteDeviceValue,
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									tpu.ResourceGoogleTPU: resource.MustParse("4"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					gkelabels.TPULabel: gkelabels.TpuV4LiteDeviceValue,
				},
				ExtraResources: map[string]resource.Quantity{
					tpu.ResourceGoogleTPU: resource.MustParse("4"),
				},
			},
		},
		{
			name: "tpu with topology and accelerator count",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						gkelabels.TPULabel:              gkelabels.TpuV4LiteDeviceValue,
						gkelabels.TPUTopologyLabel:      "2x2",
						gkelabels.AcceleratorCountLabel: "4",
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									tpu.ResourceGoogleTPU: resource.MustParse("4"),
								},
							},
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{
					gkelabels.TPULabel:              gkelabels.TpuV4LiteDeviceValue,
					gkelabels.TPUTopologyLabel:      "2x2",
					gkelabels.AcceleratorCountLabel: "4",
				},
				ExtraResources: map[string]resource.Quantity{
					tpu.ResourceGoogleTPU: resource.MustParse("4"),
				},
			},
		},
		{
			name: "no_workload_separation",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{},
					Tolerations:  []v1.Toleration{},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{},
				Taints: []v1.Taint{},
			},
		},
		{
			name: "one_workload_label",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"workload": "w1",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "workload",
							Operator: v1.TolerationOpEqual,
							Value:    "w1",
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{
					"workload": "w1",
				},
				Taints: []v1.Taint{
					{
						Key:    "workload",
						Value:  "w1",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
		},
		{
			name: "one_workload_label_and_extra_label_without_matching_toleration",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"workload": "w1",
						"k":        "v",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "workload",
							Operator: v1.TolerationOpEqual,
							Value:    "w1",
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{},
				Taints: []v1.Taint{},
			},
		},
		{
			name: "one_workload_label_and_extra_label_without_matching_toleration_but_it_is_allowlisted",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{gkelabels.PTSDomainKeyAnnotation: "k"},
				},
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"workload": "w1",
						"k":        "v",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "workload",
							Operator: v1.TolerationOpEqual,
							Value:    "w1",
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{
					"workload": "w1",
				},
				Taints: []v1.Taint{
					{
						Key:    "workload",
						Value:  "w1",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
		},
		{
			name: "two_workload_labels",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"workload": "wungiel",
						"country":  "Poland",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "workload",
							Operator: v1.TolerationOpEqual,
							Value:    "wungiel",
							Effect:   v1.TaintEffectNoSchedule,
						},
						{
							Key:      "country",
							Operator: v1.TolerationOpExists,
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{
					"workload": "wungiel",
					"country":  "Poland",
				},
				Taints: []v1.Taint{
					{
						Key:    "workload",
						Value:  "wungiel",
						Effect: v1.TaintEffectNoSchedule,
					},
					{
						Key:    "country",
						Value:  "Poland",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
		},
	}
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	testPodSharder(t, NewGkePodSharder(provider, false, nil), testCases)
}

// TODO(b/343908373): add test for case from gkecl/1585885/comment/a8d6bb1e_1e104758
// i.e. gpu pod and cpu pod from a multiple PodSet ProvReq landing in separate shards
func TestProvisioningRequestShardComputeFunction(t *testing.T) {
	testCases := []shardComputeFunctionTestCase{
		{
			name: "no_provisioning_request",
			pod: v1.Pod{
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{},
				Taints: []v1.Taint{},
			},
		},
		{
			name: "provisioning_request_present, no class name",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
					},
				},
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{},
				Taints: []v1.Taint{},
			},
		},
		{
			name: "provisioning_request_present, unknown class name",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
						"cluster-autoscaler.kubernetes.io/provisioning-class-name":      "unknown",
					},
				},
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels:                map[string]string{},
				Taints:                []v1.Taint{},
				ProvisioningClassName: "unknown",
			},
		},
		{
			name: "provisioning_request_present, check-capacity provisioning class",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
						"cluster-autoscaler.kubernetes.io/provisioning-class-name":      "check-capacity.autoscaling.x-k8s.io",
					},
					Labels: map[string]string{"key": "value"},
				},
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels:                map[string]string{},
				Taints:                []v1.Taint{},
				ProvisioningClassName: provreqv1.ProvisioningClassCheckCapacity,
			},
		},
		{
			name: "two_workload_labels_and_provisioning_request",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
						"cluster-autoscaler.kubernetes.io/provisioning-class-name":      "queued-provisioning.gke.io",
					},
				},
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						"workload": "wungiel",
						"country":  "Poland",
					},
					Tolerations: []v1.Toleration{
						{
							Key:      "workload",
							Operator: v1.TolerationOpEqual,
							Value:    "wungiel",
							Effect:   v1.TaintEffectNoSchedule,
						},
						{
							Key:      "country",
							Operator: v1.TolerationOpExists,
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels: map[string]string{
					"workload": "wungiel",
					"country":  "Poland",
				},
				Taints: []v1.Taint{
					{
						Key:    "workload",
						Value:  "wungiel",
						Effect: v1.TaintEffectNoSchedule,
					},
					{
						Key:    "country",
						Value:  "Poland",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
				ProvisioningClassName: queuedwrapper.QueuedProvisioningClassName,
			},
		},
		{
			name: "provisioning_capacity_search_strategy",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
						"cluster-autoscaler.kubernetes.io/provisioning-class-name":      "queued-provisioning.gke.io",
						"autoscaling.gke.io/provisioning-capacity-search-strategy":      "OBTAINABILITY",
					},
				},
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				Labels:                             map[string]string{},
				Taints:                             []v1.Taint{},
				ProvisioningClassName:              queuedwrapper.QueuedProvisioningClassName,
				ProvisioningCapacitySearchStrategy: "OBTAINABILITY",
			},
		},
		{
			name: "strategy_ignored_for_non_queued",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "provisioning-request-name",
						"cluster-autoscaler.kubernetes.io/provisioning-class-name":      "some-other-provreq.ku6es.io",
						"autoscaling.gke.io/provisioning-capacity-search-strategy":      "OBTAINABILITY",
					},
				},
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				ProvisioningClassName: "some-other-provreq.ku6es.io",
			},
		},
	}
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	testPodSharder(t, NewGkePodSharder(provider, false, nil), testCases)
}

func TestCccShardComputeFunction(t *testing.T) {
	testCases := []shardComputeFunctionTestCase{
		{
			name: "CCC not specified, not set in shard",
			pod: v1.Pod{
				Spec: v1.PodSpec{},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{},
			},
		},
		{
			name: "default CCC specified, using default",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						gkelabels.ComputeClassLabel: gkelabels.DefaultNPCName,
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{gkelabels.ComputeClassLabel: gkelabels.DefaultNPCName},
			},
		},
		{
			name: "non default CCC specified, using specified",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					NodeSelector: map[string]string{
						gkelabels.ComputeClassLabel: "some-random-ccc",
					},
				},
			},
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				SystemLabels: map[string]string{gkelabels.ComputeClassLabel: "some-random-ccc"},
			},
		},
	}
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	testPodSharder(t, NewGkePodSharder(provider, false, nil), testCases)
}

func TestCSNShardComputeFunction(t *testing.T) {
	testCases := []struct {
		name                        string
		pod                         v1.Pod
		csnEnabled                  bool
		expectedNodeGroupDescriptor NodeGroupDescriptor
	}{
		{
			name: "csnEnabled=true, no buffer assignment",
			pod: v1.Pod{
				Spec: v1.PodSpec{},
			},
			csnEnabled:                  true,
			expectedNodeGroupDescriptor: NodeGroupDescriptor{},
		},
		{
			name: "csnEnabled=true, buffer assignment present",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						csn.BufferAssignmentKey: "ns_buffer",
					},
				},
			},
			csnEnabled: true,
			expectedNodeGroupDescriptor: NodeGroupDescriptor{
				CSNBufferID: "ns_buffer",
			},
		},
		{
			name: "csnEnabled=false, buffer assignment present but ignored",
			pod: v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						csn.BufferAssignmentKey: "ns_buffer",
					},
				},
			},
			csnEnabled:                  false,
			expectedNodeGroupDescriptor: NodeGroupDescriptor{},
		},
	}
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sharder := NewGkePodSharder(provider, tc.csnEnabled, nil)
			shards := sharder.ComputePodShards([]*v1.Pod{&tc.pod})
			if len(shards) != 1 {
				t.Fatalf("Expected to get precisely 1 shard, but got %d. Shards: %v", len(shards), shards)
			}
			assertNodeGroupDescriptorEqual(t, tc.expectedNodeGroupDescriptor, shards[0].NodeGroupDescriptor)
		})
	}
}

type shardComputeFunctionTestCase struct {
	name                        string
	pod                         v1.Pod
	expectedNodeGroupDescriptor NodeGroupDescriptor
}

func testPodSharder(t *testing.T, sharder PodSharder, testCases []shardComputeFunctionTestCase) {
	t.Helper()
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			shards := sharder.ComputePodShards([]*v1.Pod{&tc.pod})
			if len(shards) != 1 {
				t.Errorf("Expected to get precisely 1 shard, but got %d. Shards: %v", len(shards), shards)
				return
			}
			assertNodeGroupDescriptorEqual(t, tc.expectedNodeGroupDescriptor, shards[0].NodeGroupDescriptor)
		})
	}
}
