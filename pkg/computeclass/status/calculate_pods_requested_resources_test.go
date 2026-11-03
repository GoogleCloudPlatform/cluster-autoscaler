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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	capacitybuffer "sigs.k8s.io/cluster-autoscaler/pkg/processors/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestCalculatePodsRequestedResources(t *testing.T) {
	unrequestedFPGA := apiv1.ResourceName("example.com/unrequested-fpga")
	alwaysRestart := apiv1.ContainerRestartPolicyAlways
	isController := true
	deletionTime := metav1.Now()

	newTestNode := func() *apiv1.Node {
		node := test.BuildTestNode("test-node", 32000, 128*GB)
		test.AddGpusToNode(node, 8)
		node.Status.Capacity[tpu.ResourceGoogleTPU] = *resource.NewQuantity(4, resource.DecimalSI)
		node.Status.Allocatable[tpu.ResourceGoogleTPU] = *resource.NewQuantity(4, resource.DecimalSI)
		return node
	}

	multiContainerPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multi-container-pod",
			Namespace: "default",
			UID:       types.UID("uid-multi"),
		},
		Spec: apiv1.PodSpec{
			NodeName: "test-node",
			Containers: []apiv1.Container{
				{
					Name: "c1",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:              resource.MustParse("500m"),
							apiv1.ResourceMemory:           resource.MustParse("1Gi"),
							apiv1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
							gpu.ResourceNvidiaGPU:          resource.MustParse("2"),
						},
					},
				},
				{
					Name: "c2",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:              resource.MustParse("1500m"),
							apiv1.ResourceMemory:           resource.MustParse("3Gi"),
							apiv1.ResourceEphemeralStorage: resource.MustParse("5Gi"),
							tpu.ResourceGoogleTPU:          resource.MustParse("4"),
						},
					},
				},
			},
		},
	}

	initSidecarOverheadPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "init-sidecar-overhead-pod",
			Namespace: "default",
			UID:       types.UID("uid-init-sidecar"),
		},
		Spec: apiv1.PodSpec{
			NodeName: "test-node",
			Overhead: apiv1.ResourceList{
				apiv1.ResourceCPU:    resource.MustParse("250m"),
				apiv1.ResourceMemory: resource.MustParse("128Mi"),
				apiv1.ResourcePods:   resource.MustParse("1"),
			},
			InitContainers: []apiv1.Container{
				{
					Name: "regular-init",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:              resource.MustParse("2000m"),
							apiv1.ResourceMemory:           resource.MustParse("4Gi"),
							apiv1.ResourceEphemeralStorage: resource.MustParse("8Gi"),
						},
					},
				},
				{
					Name:          "sidecar-init",
					RestartPolicy: &alwaysRestart,
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:              resource.MustParse("500m"),
							apiv1.ResourceMemory:           resource.MustParse("512Mi"),
							apiv1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
						},
					},
				},
			},
			Containers: []apiv1.Container{
				{
					Name: "app",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:              resource.MustParse("1000m"),
							apiv1.ResourceMemory:           resource.MustParse("2Gi"),
							apiv1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
							gpu.ResourceNvidiaGPU:          resource.MustParse("2"),
						},
					},
				},
			},
		},
	}

	daemonSetPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ds-pod",
			Namespace: "kube-system",
			UID:       types.UID("uid-ds"),
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "DaemonSet", Name: "fluentbit", Controller: &isController},
			},
		},
		Spec: apiv1.PodSpec{
			NodeName: "test-node",
			Containers: []apiv1.Container{
				{
					Name: "agent",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:    resource.MustParse("100m"),
							apiv1.ResourceMemory: resource.MustParse("200Mi"),
						},
					},
				},
			},
		},
	}

	mirrorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mirror-pod",
			Namespace: "kube-system",
			UID:       types.UID("uid-mirror"),
			Annotations: map[string]string{
				apiv1.MirrorPodAnnotationKey: "hash-123",
			},
		},
		Spec: apiv1.PodSpec{
			NodeName: "test-node",
			Containers: []apiv1.Container{
				{
					Name: "static",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:     resource.MustParse("150m"),
							apiv1.ResourceMemory:  resource.MustParse("100Mi"),
							tpu.ResourceGoogleTPU: resource.MustParse("4"),
						},
					},
				},
			},
		},
	}

	terminatingPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "terminating-pod",
			Namespace:         "default",
			UID:               types.UID("uid-term"),
			DeletionTimestamp: &deletionTime,
		},
		Spec: apiv1.PodSpec{
			NodeName: "test-node",
			Containers: []apiv1.Container{
				{
					Name: "app",
					Resources: apiv1.ResourceRequirements{
						Requests: apiv1.ResourceList{
							apiv1.ResourceCPU:              resource.MustParse("750m"),
							apiv1.ResourceMemory:           resource.MustParse("1Gi"),
							apiv1.ResourceEphemeralStorage: resource.MustParse("3Gi"),
							gpu.ResourceNvidiaGPU:          resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	baseNode := newTestNode()
	balloonPod, err := operationtracker.GenerateBalloonPod(baseNode, resource.MustParse("1000m"), resource.MustParse("2Gi"), true)
	if err != nil {
		t.Fatalf("GenerateBalloonPod failed: %v", err)
	}
	lookaheadPod := lookaheadbuffer.GenerateLookaheadPods(1, resource.MustParse("1000m"), resource.MustParse("2Gi"), "wl-1", "n2")[0]
	lookaheadPod.Spec.NodeName = "test-node"

	unboundSimPod := test.BuildTestPod("unbound-sim-pod", 1000, 1*GB)
	test.RequestGpuForPod(unboundSimPod, 1)

	otherNodePod := test.BuildScheduledTestPod("other-node-pod", 1000, 1*GB, "other-node")

	provReqInjectedPod := test.BuildScheduledTestPod("prov-req-injected-pod", 1000, 1*GB, "test-node")
	provReqInjectedPod.Annotations = map[string]string{provreqv1.ProvisioningRequestPodAnnotationKey: "pr-1"}
	provReqInjectedPod.OwnerReferences = []metav1.OwnerReference{{Kind: "ProvisioningRequest", Name: "pr-1", Controller: &isController}}
	test.RequestGpuForPod(provReqInjectedPod, 1)

	provReqConsumerPod := test.BuildScheduledTestPod("prov-req-consumer-pod", 1000, 1*GB, "test-node")
	provReqConsumerPod.Annotations = map[string]string{provreqv1.ProvisioningRequestPodAnnotationKey: "pr-1"}
	test.RequestGpuForPod(provReqConsumerPod, 1)

	capacityReqPod := test.BuildScheduledTestPod("cap-req-pod", 1000, 1*GB, "test-node")
	capacityReqPod.Annotations = map[string]string{"cr.internal.gkeautoscaler.google.com/capacityrequest": "true"}

	activeMigrationPod := test.BuildScheduledTestPod("defrag-pod", 1000, 1*GB, "test-node")
	activeMigrationPod.Annotations = map[string]string{defrag.ActiveMigrationPodAnnotation: "true"}

	minCapacityPod := test.BuildScheduledTestPod("min-cap-pod", 1000, 1*GB, "test-node")
	minCapacityPod.Annotations = map[string]string{crd.MinCapacityFakePodAnnotation: "true"}

	capacityBufferPod := test.BuildScheduledTestPod("cap-buf-pod", 1000, 1*GB, "test-node")
	capacityBufferPod.Annotations = map[string]string{
		capacitybuffer.CapacityBufferFakePodAnnotationKey: capacitybuffer.CapacityBufferFakePodAnnotationValue,
	}

	genericFakePod := fake.WithFakePodAnnotation(test.BuildScheduledTestPod("generic-fake-pod", 1000, 1*GB, "test-node"))

	injectedPods := []*apiv1.Pod{
		balloonPod,
		lookaheadPod,
		unboundSimPod,
		otherNodePod,
		provReqInjectedPod,
		capacityReqPod,
		activeMigrationPod,
		minCapacityPod,
		capacityBufferPod,
		genericFakePod,
	}

	unnamedNode := newTestNode()
	unnamedNode.Name = ""
	unnamedNodeInfo := framework.NewTestNodeInfo(unnamedNode, unboundSimPod)

	// A preemptor pod waits on its nominated node until its victims leave, so it is
	// not bound yet (empty Spec.NodeName).
	nominatedPod := test.BuildTestPod("nominated-pod", 1000, 1*GB)
	nominatedPod.Status.NominatedNodeName = "test-node"
	test.RequestGpuForPod(nominatedPod, 1)

	tests := []struct {
		name     string
		nodeInfo *framework.NodeInfo
		want     map[apiv1.ResourceName]resource.Quantity
	}{
		{
			name: "nil_node_info",
			want: map[apiv1.ResourceName]resource.Quantity{
				apiv1.ResourceCPU:     resource.MustParse("0"),
				apiv1.ResourceMemory:  resource.MustParse("0"),
				gpu.ResourceNvidiaGPU: resource.MustParse("0"),
			},
		},
		{
			name:     "empty_node",
			nodeInfo: framework.NewTestNodeInfo(newTestNode()),
			want: map[apiv1.ResourceName]resource.Quantity{
				apiv1.ResourceCPU:              resource.MustParse("0"),
				apiv1.ResourceMemory:           resource.MustParse("0"),
				apiv1.ResourceEphemeralStorage: resource.MustParse("0"),
				gpu.ResourceNvidiaGPU:          resource.MustParse("0"),
				unrequestedFPGA:                resource.MustParse("0"),
			},
		},
		{
			name:     "multi_container_and_accelerators",
			nodeInfo: framework.NewTestNodeInfo(newTestNode(), multiContainerPod),
			want: map[apiv1.ResourceName]resource.Quantity{
				apiv1.ResourceCPU:              resource.MustParse("2000m"),
				apiv1.ResourceMemory:           resource.MustParse("4Gi"),
				apiv1.ResourceEphemeralStorage: resource.MustParse("15Gi"),
				gpu.ResourceNvidiaGPU:          resource.MustParse("2"),
				tpu.ResourceGoogleTPU:          resource.MustParse("4"),
				unrequestedFPGA:                resource.MustParse("0"),
			},
		},
		{
			name:     "init_sidecar_and_overhead",
			nodeInfo: framework.NewTestNodeInfo(newTestNode(), initSidecarOverheadPod),
			want: map[apiv1.ResourceName]resource.Quantity{
				// max(2000m, 500m + 1000m) + 250m overhead = 2250m.
				apiv1.ResourceCPU: resource.MustParse("2250m"),
				// max(4Gi, 512Mi + 2Gi) + 128Mi overhead = 4224Mi.
				apiv1.ResourceMemory: resource.MustParse("4224Mi"),
				// max(8Gi, 1Gi + 2Gi) = 8Gi.
				apiv1.ResourceEphemeralStorage: resource.MustParse("8Gi"),
				gpu.ResourceNvidiaGPU:          resource.MustParse("2"),
				unrequestedFPGA:                resource.MustParse("0"),
			},
		},
		{
			name:     "mixed_daemonset_mirror_and_terminating",
			nodeInfo: framework.NewTestNodeInfo(newTestNode(), initSidecarOverheadPod, daemonSetPod, mirrorPod, terminatingPod),
			want: map[apiv1.ResourceName]resource.Quantity{
				// 2250m + 100m + 150m + 750m = 3250m.
				apiv1.ResourceCPU: resource.MustParse("3250m"),
				// 4224Mi + 200Mi + 100Mi + 1024Mi = 5548Mi.
				apiv1.ResourceMemory:           resource.MustParse("5548Mi"),
				apiv1.ResourceEphemeralStorage: resource.MustParse("11Gi"),
				gpu.ResourceNvidiaGPU:          resource.MustParse("3"),
				tpu.ResourceGoogleTPU:          resource.MustParse("4"),
				unrequestedFPGA:                resource.MustParse("0"),
			},
		},
		{
			name:     "excludes_unbound_balloon_and_synthetic_pods",
			nodeInfo: framework.NewTestNodeInfo(newTestNode(), append([]*apiv1.Pod{initSidecarOverheadPod, daemonSetPod}, injectedPods...)...),
			want: map[apiv1.ResourceName]resource.Quantity{
				// Only initSidecarOverheadPod (2250m, 4224Mi, 8Gi, 2 GPU) + daemonSetPod (100m, 200Mi) are counted.
				apiv1.ResourceCPU:              resource.MustParse("2350m"),
				apiv1.ResourceMemory:           resource.MustParse("4424Mi"),
				apiv1.ResourceEphemeralStorage: resource.MustParse("8Gi"),
				gpu.ResourceNvidiaGPU:          resource.MustParse("2"),
				unrequestedFPGA:                resource.MustParse("0"),
			},
		},
		{
			name:     "excludes_pod_nominated_by_preemption",
			nodeInfo: framework.NewTestNodeInfo(newTestNode(), initSidecarOverheadPod, nominatedPod),
			want: map[apiv1.ResourceName]resource.Quantity{
				// Only initSidecarOverheadPod (2250m, 4224Mi, 2 GPU) is counted.
				apiv1.ResourceCPU:     resource.MustParse("2250m"),
				apiv1.ResourceMemory:  resource.MustParse("4224Mi"),
				gpu.ResourceNvidiaGPU: resource.MustParse("2"),
			},
		},
		{
			name:     "counts_pods_consuming_provisioning_request",
			nodeInfo: framework.NewTestNodeInfo(newTestNode(), initSidecarOverheadPod, provReqConsumerPod, provReqInjectedPod),
			want: map[apiv1.ResourceName]resource.Quantity{
				// initSidecarOverheadPod (2250m, 4224Mi, 2 GPU) + provReqConsumerPod (1000m, 1Gi, 1 GPU).
				apiv1.ResourceCPU:     resource.MustParse("3250m"),
				apiv1.ResourceMemory:  resource.MustParse("5248Mi"),
				gpu.ResourceNvidiaGPU: resource.MustParse("3"),
			},
		},
		{
			name:     "unnamed_node_ignores_unbound_pods",
			nodeInfo: unnamedNodeInfo,
			want: map[apiv1.ResourceName]resource.Quantity{
				apiv1.ResourceCPU:     resource.MustParse("0"),
				apiv1.ResourceMemory:  resource.MustParse("0"),
				gpu.ResourceNvidiaGPU: resource.MustParse("0"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for resName, wantQty := range tc.want {
				got, err := CalculatePodsRequestedResources(tc.nodeInfo, resName)
				if err != nil {
					t.Errorf("CalculatePodsRequestedResources(nodeInfo, %q) returned unexpected error: %v", resName, err)
					continue
				}
				if !got.Equal(wantQty) {
					t.Errorf("CalculatePodsRequestedResources(nodeInfo, %q) = %s, want %s", resName, got.String(), wantQty.String())
				}

			}
		})
	}
}
