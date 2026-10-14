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
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/metadata"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
	nodecontrollertesting "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	// Scale parameters matching the standby capacity buffers scalability test:
	// CL2_CCC_COUNT=5, CL2_BUFFER_MACHINE_TYPE=e2-standard-2 (2 vCPUs, 8Gi),
	// CL2_PODS_CPU_REQUEST=1100m, CL2_PODS_MEM_REQUEST=5Gi.
	benchNumCCCs           = 5
	benchNodeCPU           = 2000
	benchNodeMem           = 8 * GiB
	benchPodCPU            = 1100
	benchPodMem            = 5 * GiB
	benchComputeClassLabel = "cloud.google.com/compute-class"
)

// BenchmarkBufferConsumptionProcessor benchmarks BufferConsumptionProcessor.Process
// when scheduling unschedulable workload pods onto existing suspended CSN nodes
// across multiple ComputeClasses (simulating ci-kubernetes-e2e-gke-rapid-15k-capacity-buffers-standby-performance test).
//
// In each scenario, the cluster is pre-populated with suspended CSN nodes (e2-standard-2:
// 2 vCPUs, 8 GiB memory) distributed evenly across 5 ComputeClasses (ccc-0..ccc-4) and
// assigned to their corresponding CapacityBuffers. Unschedulable ReplicaSet pods (1100m CPU,
// 5 GiB memory, 1 pod per node) targeting those ComputeClasses are then passed to
// BufferConsumptionProcessor.Process, which ignores buffer assignments on suspended CSN
// nodes, simulates scheduling the workload pods onto them, and marks the utilized nodes
// as Consumed in both the CSNNodeController and ClusterSnapshot.
func BenchmarkBufferConsumptionProcessor(b *testing.B) {
	scenarios := []struct {
		name                string
		totalPods           int
		totalSuspendedNodes int
		totalNonCSNNodes    int
		numCCCs             int
	}{
		{
			// Simulates partial buffer consumption (CL2_DEPLOYMENT_REPLICAS=2000, CL2_BUFFER_REPLICAS=7000):
			// 2,000 workload pods (400 per CCC) scheduled across 7,000 suspended CSN nodes (1,400 per CCC).
			name:                "2kDeploymentPods_7kSuspendedNodes",
			totalPods:           2000,
			totalSuspendedNodes: 7000,
		},
		{
			// Simulates full buffer consumption:
			// 7,000 workload pods (1,400 per CCC) consuming all 7,000 suspended CSN nodes (1,400 per CCC).
			name:                "7kDeploymentPods_7kSuspendedNodes",
			totalPods:           7000,
			totalSuspendedNodes: 7000,
		},
		{
			// Simulates buffer exhaustion across 5 ComputeClasses with many unprioritized (non-CSN) nodes:
			// 2,000 workload pods (400 per CCC) when 100 suspended CSN nodes (20 per CCC) are available alongside 7,000 non-CSN nodes.
			name:                "2kDeploymentPods_100SuspendedNodes_7kNonCSNNodes_5CCCs",
			totalPods:           2000,
			totalSuspendedNodes: 100,
			totalNonCSNNodes:    7000,
			numCCCs:             5,
		},
		{
			// Simulates large-scale buffer exhaustion across 5 ComputeClasses with many unprioritized (non-CSN) nodes:
			// 7,000 workload pods (1,400 per CCC) when 100 suspended CSN nodes (20 per CCC) are available alongside 7,000 non-CSN nodes.
			name:                "7kDeploymentPods_100SuspendedNodes_7kNonCSNNodes_5CCCs",
			totalPods:           7000,
			totalSuspendedNodes: 100,
			totalNonCSNNodes:    7000,
			numCCCs:             5,
		},
		{
			// Simulates buffer exhaustion across 100 ComputeClasses with many unprioritized (non-CSN) nodes:
			// 2,000 workload pods (20 per CCC) when 100 suspended CSN nodes (1 per CCC) are available alongside 7,000 non-CSN nodes.
			name:                "2kDeploymentPods_100SuspendedNodes_7kNonCSNNodes_100CCCs",
			totalPods:           2000,
			totalSuspendedNodes: 100,
			totalNonCSNNodes:    7000,
			numCCCs:             100,
		},
		{
			// Simulates large-scale buffer exhaustion across 100 ComputeClasses with many unprioritized (non-CSN) nodes:
			// 7,000 workload pods (70 per CCC) when 100 suspended CSN nodes (1 per CCC) are available alongside 7,000 non-CSN nodes.
			name:                "7kDeploymentPods_100SuspendedNodes_7kNonCSNNodes_100CCCs",
			totalPods:           7000,
			totalSuspendedNodes: 100,
			totalNonCSNNodes:    7000,
			numCCCs:             100,
		},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			numCCCs := sc.numCCCs
			if numCCCs == 0 {
				numCCCs = benchNumCCCs
			}
			csnNodes := buildBenchmarkCSNNodes(b, sc.totalSuspendedNodes, numCCCs, true, csn.NodeStateSuspended)
			nonCSNNodes := buildBenchmarkNonCSNNodes(sc.totalNonCSNNodes)
			baseNodes := append(slices.Clone(csnNodes), nonCSNNodes...)
			templatePods := buildBenchmarkWorkloadPods(sc.totalPods, numCCCs)
			podLister := kubernetes.NewTestPodLister(nil)
			nodeLister := kubernetes.NewTestNodeLister(baseNodes)
			listerRegistry := kubernetes.NewListerRegistry(nodeLister, nil, podLister, nil, nil, nil, nil, nil, nil)
			expectedRemainingPods := max(0, sc.totalPods-sc.totalSuspendedNodes)

			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				clusterSnapshot, mockController := prepareBenchmarkSnapshotAndController(b, baseNodes, csn.NodeStateSuspended)
				autoscalingCtx := &ca_context.AutoscalingContext{
					ClusterSnapshot:      clusterSnapshot,
					ClusterStateRegistry: clusterstate.NewClusterStateRegistry(nil, nil, nil, nil, nil),
					AutoscalingKubeClients: ca_context.AutoscalingKubeClients{
						ListerRegistry: listerRegistry,
					},
				}

				processor := NewBufferConsumptionProcessor(mockController, experiments.NewMockManager())
				processor.metrics = &mockCSNMetrics{}
				pods := slices.Clone(templatePods)

				b.StartTimer()
				remainingPods, err := processor.Process(b.Context(), autoscalingCtx, pods)
				b.StopTimer()

				assert.NoError(b, err)
				assert.Len(b, remainingPods, expectedRemainingPods)
			}
			b.ReportMetric(b.Elapsed().Seconds()/float64(b.N), "s/op")
		})
	}
}

// buildBenchmarkNonCSNNodes creates totalNodes regular (non-CSN) nodes that do not have
// CSN labels or priorities.
func buildBenchmarkNonCSNNodes(totalNodes int) []*apiv1.Node {
	if totalNodes == 0 {
		return nil
	}
	nodes := make([]*apiv1.Node, totalNodes)
	for i := range totalNodes {
		nodes[i] = test.BuildTestNode(fmt.Sprintf("regular-node-%d", i), benchNodeCPU, benchNodeMem)
	}
	return nodes
}

// buildBenchmarkCSNNodes creates totalNodes CSN nodes distributed evenly across numCCCs
// ComputeClasses with soft workload separation labels/taints and optional buffer assignment.
func buildBenchmarkCSNNodes(b *testing.B, totalNodes, numCCCs int, assigned bool, state csn.NodeState) []*apiv1.Node {
	b.Helper()
	nodesPerCCC := totalNodes / numCCCs
	nodes := make([]*apiv1.Node, 0, totalNodes)

	for cccIdx := range numCCCs {
		cccName := fmt.Sprintf("ccc-%d", cccIdx)
		bufferID := fmt.Sprintf("default/capacity-buffer-%d", cccIdx)
		mutators := []nodeMutator{
			withLabelsMutator(map[string]string{
				benchComputeClassLabel:             cccName,
				metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
			}),
			withTaintsMutator(apiv1.Taint{
				Key:    benchComputeClassLabel,
				Value:  cccName,
				Effect: apiv1.TaintEffectNoSchedule,
			}),
		}
		if assigned {
			mutators = append(mutators, withBufferAssignmentMutator(bufferID))
		}

		for j := range nodesPerCCC {
			nodeName := fmt.Sprintf("csn-node-%d-%d", cccIdx, j)
			node := createTestNode(b, nodeName, benchNodeCPU, benchNodeMem, state, mutators...)
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// prepareBenchmarkSnapshotAndController creates a fresh ClusterSnapshot and MockCSNNodeController
// populated with deep copies of baseNodes in the specified CSN state for a single benchmark iteration.
func prepareBenchmarkSnapshotAndController(
	b *testing.B,
	baseNodes []*apiv1.Node,
	state csn.NodeState,
) (clustersnapshot.ClusterSnapshot, *nodecontrollertesting.MockCSNNodeController) {
	b.Helper()
	clusterSnapshot := testsnapshot.NewCustomTestSnapshotOrDie(b, store.NewDeltaSnapshotStore())
	csnNodes := make([]nodecontroller.CSNNode, 0, len(baseNodes))
	for _, node := range baseNodes {
		if err := clusterSnapshot.AddNodeInfo(framework.NewNodeInfo(node.DeepCopy(), nil)); err != nil {
			b.Fatalf("Failed to add node %q into cluster snapshot: %v", node.Name, err)
		}
		if csn.IsCSNNode(node) {
			csnNodes = append(csnNodes, nodecontroller.CSNNode{
				Name:         node.Name,
				DesiredState: state,
			})
		}
	}

	mockController := nodecontrollertesting.NewMockCSNNodeController(csnNodes)
	for _, n := range csnNodes {
		mockController.SetCurrentState(n.Name, state)
	}
	return clusterSnapshot, mockController
}

// buildBenchmarkPodForCCC creates a benchmark pod with 1100m CPU and 5Gi memory targeting
// the specified ComputeClass via nodeSelector and soft workload separation tolerations.
func buildBenchmarkPodForCCC(podName, cccName string) *apiv1.Pod {
	return test.BuildTestPod(
		podName,
		benchPodCPU,
		benchPodMem,
		withWorkloadSeparation(benchComputeClassLabel, cccName),
		withNodeSelector(map[string]string{benchComputeClassLabel: cccName}),
	)
}

// buildBenchmarkWorkloadPods creates totalPods unschedulable ReplicaSet workload pods
// distributed evenly across numCCCs ComputeClasses.
func buildBenchmarkWorkloadPods(totalPods, numCCCs int) []*apiv1.Pod {
	podsPerCCC := totalPods / numCCCs
	pods := make([]*apiv1.Pod, 0, totalPods)

	for cccIdx := range numCCCs {
		cccName := fmt.Sprintf("ccc-%d", cccIdx)
		groupName := fmt.Sprintf("group-%d", cccIdx)
		controllerUID := types.UID(fmt.Sprintf("replicaset-uid-%d", cccIdx))
		controllerName := fmt.Sprintf("replicaset-%d", cccIdx)

		for j := range podsPerCCC {
			podName := fmt.Sprintf("replicaset-%d-pod-%d", cccIdx, j)
			pod := buildBenchmarkPodForCCC(podName, cccName)
			pod.UID = types.UID(fmt.Sprintf("pod-uid-%d-%d", cccIdx, j))
			pod.Labels = map[string]string{"group": groupName}
			pod.OwnerReferences = []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       controllerName,
					UID:        controllerUID,
					Controller: ptr.To(true),
				},
			}
			pods = append(pods, pod)
		}
	}
	return pods
}
