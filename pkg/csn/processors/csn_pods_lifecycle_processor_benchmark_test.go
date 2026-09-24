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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
)

// BenchmarkCSNPodsLifecycleProcessor benchmarks CSNPodsLifecycleProcessor.Process
// when injecting and scheduling fake standby capacity buffer pods onto existing CSN nodes
// across 5 ComputeClasses (simulating ci-kubernetes-e2e-gke-rapid-15k-capacity-buffers-standby-performance test).
//
// It covers three main lifecycle stages:
//  1. FirstTime_UnassignedNodes: CSN nodes have just joined the cluster in the Chilling
//     state without buffer assignments. CSNPodsLifecycleProcessor schedules the CSN pods
//     onto the unassigned nodes (via scheduledCSNPodsOnUnassignedNodes using
//     NewLastIndexOrderMapping), assigns each node to its buffer, and marks them Suspendable.
//  2. FirstTime_AssignedNodes_NoHints: CSN nodes are already assigned to their respective
//     buffers and Suspended, and CSNPodsLifecycleProcessor schedules the CSN pods via
//     schedulePodsOnCSNNodes for the first time when HintingSimulator has no cached hints yet.
//  3. SecondTime_AssignedNodes_WithHints: CSN nodes are assigned and Suspended, and
//     CSNPodsLifecycleProcessor schedules the CSN pods on a subsequent loop where its
//     HintingSimulator already holds pod UID -> node name hints from a previous pass.
func BenchmarkCSNPodsLifecycleProcessor(b *testing.B) {
	scenarios := []struct {
		name         string
		totalCSNPods int
		totalNodes   int
		assigned     bool
		state        csn.NodeState
		withHints    bool
	}{
		{
			// First time scheduling 7,000 CSN pods onto 7,000 newly provisioned, unassigned Chilling CSN nodes.
			name:         "7kCSNPods_FirstTime_UnassignedNodes",
			totalCSNPods: 7000,
			totalNodes:   7000,
			assigned:     false,
			state:        csn.NodeStateChilling,
			withHints:    false,
		},
		{
			// First time scheduling 7,000 CSN pods onto 7,000 buffer-assigned Suspended CSN nodes without HintingSimulator hints.
			name:         "7kCSNPods_FirstTime_AssignedNodes_NoHints",
			totalCSNPods: 7000,
			totalNodes:   7000,
			assigned:     true,
			state:        csn.NodeStateSuspended,
			withHints:    false,
		},
		{
			// Second time scheduling 7,000 CSN pods onto 7,000 buffer-assigned Suspended CSN nodes with HintingSimulator hints populated.
			name:         "7kCSNPods_SecondTime_AssignedNodes_WithHints",
			totalCSNPods: 7000,
			totalNodes:   7000,
			assigned:     true,
			state:        csn.NodeStateSuspended,
			withHints:    true,
		},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			templateCSNPods, bufferRegistry := buildBenchmarkCSNPodsAndRegistry(sc.totalCSNPods)
			nodes := buildBenchmarkCSNNodes(b, sc.totalNodes, sc.assigned, sc.state)
			runCSNPodsLifecycleBenchmark(b, nodes, sc.state, templateCSNPods, bufferRegistry, sc.withHints)
		})
	}
}

func runCSNPodsLifecycleBenchmark(
	b *testing.B,
	baseNodes []*apiv1.Node,
	state csn.NodeState,
	templateCSNPods []*apiv1.Pod,
	bufferRegistry *fakepods.Registry,
	withHints bool,
) {
	b.Helper()
	expectedRemainingPods := max(0, len(templateCSNPods)-len(baseNodes))

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		processor := NewCSNPodsLifecycleProcessor(nil, nil, nil, bufferRegistry, 24*time.Hour, experiments.NewMockManager())
		processor.metrics = &mockCSNMetrics{}
		if withHints {
			// Execute one untimed pass so the processor's HintingSimulator is populated
			// with scheduling hints via the actual Process code path.
			runCSNPodsLifecycleIteration(b, processor, baseNodes, state, templateCSNPods, expectedRemainingPods, false)
		}
		runCSNPodsLifecycleIteration(b, processor, baseNodes, state, templateCSNPods, expectedRemainingPods, true)
	}
	b.ReportMetric(b.Elapsed().Seconds()/float64(b.N), "s/op")
}

func runCSNPodsLifecycleIteration(
	b *testing.B,
	processor *CSNPodsLifecycleProcessor,
	baseNodes []*apiv1.Node,
	state csn.NodeState,
	templateCSNPods []*apiv1.Pod,
	expectedRemainingPods int,
	timed bool,
) {
	b.Helper()
	b.StopTimer()
	clusterSnapshot, mockNodeController := prepareBenchmarkSnapshotAndController(b, baseNodes, state)
	processor.nodeController = mockNodeController
	processor.csnPodInjectionProcessor = &mockCapacityBufferPodListProcessor{
		podsToCreate: clonePods(templateCSNPods),
	}
	autoscalingCtx := &ca_context.AutoscalingContext{
		ClusterSnapshot:      clusterSnapshot,
		ClusterStateRegistry: clusterstate.NewClusterStateRegistry(nil, nil, nil, nil, nil),
	}

	if timed {
		b.StartTimer()
	}
	remainingPods, err := processor.Process(b.Context(), autoscalingCtx, nil)
	if timed {
		b.StopTimer()
	}

	assert.NoError(b, err)
	assert.Len(b, remainingPods, expectedRemainingPods)
}

// buildBenchmarkCSNPodsAndRegistry creates totalCSNPods fake CapacityBuffer pods distributed
// across benchNumCCCs standby CapacityBuffers and registers each pod UID in a fakepods.Registry.
func buildBenchmarkCSNPodsAndRegistry(totalCSNPods int) ([]*apiv1.Pod, *fakepods.Registry) {
	podsPerCCC := totalCSNPods / benchNumCCCs
	bufferRegistry := fakepods.NewRegistry(nil)
	templateCSNPods := make([]*apiv1.Pod, 0, totalCSNPods)

	for cccIdx := range benchNumCCCs {
		cccName := fmt.Sprintf("ccc-%d", cccIdx)
		bufferName := fmt.Sprintf("capacity-buffer-%d", cccIdx)
		bufferUID := types.UID(fmt.Sprintf("buffer-uid-%d", cccIdx))
		buffer := &v1beta1.CapacityBuffer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bufferName,
				Namespace: "default",
				UID:       bufferUID,
			},
			Status: v1beta1.CapacityBufferStatus{
				ProvisioningStrategy: ptr.To(capacitybuffers.ColdProvisioningStrategy),
			},
		}

		for j := range podsPerCCC {
			podName := fmt.Sprintf("capacity-buffer-%s-%d", bufferName, j)
			pod := buildBenchmarkPodForCCC(podName, cccName)
			pod.Namespace = "default"
			pod.UID = types.UID(fmt.Sprintf("%s-%d", bufferUID, j))
			bufferRegistry.SetCapacityBuffer(pod.UID, buffer)
			templateCSNPods = append(templateCSNPods, pod)
		}
	}
	return templateCSNPods, bufferRegistry
}

// clonePods deep-copies each pod because CSNPodsLifecycleProcessor.Process
// mutates injected pods in place (e.g., adding CSN labels, tolerations, and owner references).
func clonePods(pods []*apiv1.Pod) []*apiv1.Pod {
	cloned := make([]*apiv1.Pod, len(pods))
	for i, p := range pods {
		cloned[i] = p.DeepCopy()
	}
	return cloned
}
