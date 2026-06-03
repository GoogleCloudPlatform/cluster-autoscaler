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
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	capacitybufferpodlister "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
	nodecontrollertesting "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	cbmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/capacitybuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	"k8s.io/kubernetes/pkg/util/taints"
	clock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

type mockCapacityBufferPodListProcessor struct {
	podsToCreate []*apiv1.Pod
	err          error
}

func (p *mockCapacityBufferPodListProcessor) Process(ctx *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	return p.podsToCreate, p.err
}

func (p *mockCapacityBufferPodListProcessor) CleanUp() {}

func withWorkloadSeparation(k, v string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{
			Key:    k,
			Value:  v,
			Effect: apiv1.TaintEffectNoSchedule,
		})
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = make(map[string]string)
		}
		pod.Spec.NodeSelector[csn.SoftWorkloadSeparationKey] = csn.SoftWorkloadSeparationValue

	}
}

func withNodeSelector(selector map[string]string) func(*apiv1.Pod) {
	return func(pod *apiv1.Pod) {
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = make(map[string]string)
		}
		maps.Copy(pod.Spec.NodeSelector, selector)
	}
}

func withTimeAddedSuspendedTaintMutator(t time.Time) nodeMutator {
	return func(node *apiv1.Node) *apiv1.Node {
		for i, taint := range node.Spec.Taints {
			if taint.MatchTaint(&csn.SuspendedTaint) {
				node.Spec.Taints[i].TimeAdded = &metav1.Time{Time: t}
				return node
			}
		}
		taint := csn.SuspendedTaint
		taint.TimeAdded = &metav1.Time{Time: t}
		node.Spec.Taints = append(node.Spec.Taints, taint)
		return node
	}
}

func TestCSNPodsLifecycleProcess(t *testing.T) {
	testCases := []struct {
		name                      string
		initialNodes              []*apiv1.Node
		csnPods                   []*apiv1.Pod
		csnPodsBuffersNames       map[string]string
		csnPodsCreationErr        error
		unschedulablePods         []*apiv1.Pod
		nonSuspendableNodes       []string
		suspendErr                error
		expectedUnschedulablePods []string
		expectErr                 bool
		expectedAllSuspendedNodes []string
		expectedBufferAssignments map[string]string // Node name to buffer name
		csnPodsBuffersAnnotations map[string]map[string]string
		additionalNodeAssertions  func(t *testing.T, node *apiv1.Node)
		defaultRefreshFrequency   time.Duration
	}{
		{
			name:                      "No CSN pods to create",
			csnPods:                   []*apiv1.Pod{},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectErr:                 false,
		},
		{
			name: "CSN pods are created and scheduled on a CSN node",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateConsumed),
				create8CPUTestNode(t, "node-2", csn.NodeStateConsumed),
				create8CPUTestNode(t, "node-3", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
				create8CPUTestNode(t, "node-4", csn.NodeStateConsumed),
				create8CPUTestNode(t, "node-5", csn.NodeStateConsumed),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000),
				test.BuildTestPod("csn-p2", 2000, 2000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
				"csn-p2": "buffer",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-3"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-3": "buffer",
			},
		},
		{
			name: "CSN pods are not scheduled on nodes with memory > 208 GB",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer"),
					withLabelsMutator(map[string]string{labels.MemoryScalingLevelLabel: "209"})), // Not schedulable: memory > 208.
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer"),
					withLabelsMutator(map[string]string{labels.MemoryScalingLevelLabel: "208"})), // Schedulable: memory <= 208.
				create8CPUTestNode(t, "node-3", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer"),
					withLabelsMutator(map[string]string{labels.MemoryScalingLevelLabel: "207"})), // Schedulable: memory <= 208.
				create8CPUTestNode(t, "node-4", csn.NodeStateChilling,
					withBufferAssignmentMutator("ns/buffer")), // Schedulable: no memory label.
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 7000, 1000),
				test.BuildTestPod("csn-p2", 7000, 1000),
				test.BuildTestPod("csn-p3", 7000, 1000),
				test.BuildTestPod("csn-p4", 7000, 1000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
				"csn-p2": "buffer",
				"csn-p3": "buffer",
				"csn-p4": "buffer",
			},
			expectedUnschedulablePods: []string{"csn-p4"},
			expectedAllSuspendedNodes: []string{"node-2", "node-3", "node-4"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-2": "buffer",
				"node-3": "buffer",
				"node-4": "buffer",
			},
		},
		{
			name: "Prefer Chilling over suspended nodes",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended, withBufferAssignmentMutator("ns/buffer")),
				create8CPUTestNode(t, "node-2", csn.NodeStateSuspended, withBufferAssignmentMutator("ns/buffer")),
				create8CPUTestNode(t, "node-3", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
				create8CPUTestNode(t, "node-4", csn.NodeStateSuspended, withBufferAssignmentMutator("ns/buffer")),
				create8CPUTestNode(t, "node-5", csn.NodeStateSuspended, withBufferAssignmentMutator("ns/buffer")),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-1", "node-2", "node-3", "node-4", "node-5"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-3": "buffer",
			},
		},
		{
			name: "Schedule CSN pods on suspended node if no chilling nodes available",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateConsumed),
				create8CPUTestNode(t, "node-2", csn.NodeStateConsumed),
				create8CPUTestNode(t, "node-3", csn.NodeStateSuspended, withBufferAssignmentMutator("ns/buffer")),
				create8CPUTestNode(t, "node-4", csn.NodeStateConsumed),
				create8CPUTestNode(t, "node-5", csn.NodeStateConsumed),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000),
				test.BuildTestPod("csn-p2", 2000, 2000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
				"csn-p2": "buffer",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-3"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-3": "buffer",
			},
		},
		{
			name: "Not enough capacity on CSN nodes to schedule all CSN pods",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 6000, 1000),
				test.BuildTestPod("csn-p2", 9000, 2000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
				"csn-p2": "buffer",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1", "csn-p2"},
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-1": "buffer",
			},
		},
		{
			name: "Error during CSN pod creation",
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			csnPodsCreationErr:        errors.New("error creating csn pods"),
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{},
			expectErr:                 false,
		},
		{
			name: "Error during suspending nodes",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			suspendErr:                errors.New("error suspending nodes"),
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{},
			expectErr:                 true,
		},
		{
			name: "Some nodes are not suspendable",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 6000, 1000),
				test.BuildTestPod("csn-p2", 6000, 1000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
				"csn-p2": "buffer",
			},
			nonSuspendableNodes:       []string{"node-2"},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-1": "buffer",
				"node-2": "buffer",
			},
		},
		{
			name: "Don't schedule CSN pod on node with different buffer assignment",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling,
					withBufferAssignmentMutator("ns/buffer")),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer-2")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer-2",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1", "csn-p1"},
			expectedAllSuspendedNodes: []string{},
			expectedBufferAssignments: map[string]string{},
			expectErr:                 false,
		},
		{
			name: "Schedule CSN pod on node with matching buffer assignment",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling,
					withBufferAssignmentMutator("ns/buffer")),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-1": "buffer",
			},
		},
		{
			name: "Schedule CSN pods from different buffers to matching nodes",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer-1")),
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer-2")),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer-1")),
				test.BuildTestPod("csn-p2", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer-2")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer-1",
				"csn-p2": "buffer-2",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-1", "node-2"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-1": "buffer-1",
				"node-2": "buffer-2",
			},
		},
		{
			name: "CSN pod is not in any buffer",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer")),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 7000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer")),
				test.BuildTestPod("csn-p2", 7000, 1000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"}, // csn-2 is neither scheduled nor returned.
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-1": "buffer",
			},
		},
		{
			name: "Schedule CSN pods on nodes that don't have buffer assignments",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withLabelsMutator(map[string]string{"nodeId": "node-1"})), // We add the label and nodeSelector bec otherwise the order of expectedBufferAssignments will change making the test flaky.
				create8CPUTestNode(t, "node-2", csn.NodeStateChilling, withLabelsMutator(map[string]string{"nodeId": "node-2"})),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withNodeSelector(map[string]string{"nodeId": "node-1"}), withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer-1")),
				test.BuildTestPod("csn-p2", 1000, 1000, withNodeSelector(map[string]string{"nodeId": "node-1"}), withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer-1")),
				test.BuildTestPod("csn-p3", 1000, 1000, withNodeSelector(map[string]string{"nodeId": "node-2"}), withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer-2")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer-1",
				"csn-p2": "buffer-1",
				"csn-p3": "buffer-2",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-1", "node-2"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-1": "buffer-1",
				"node-2": "buffer-2",
			},
		},
		{
			name: "Nodes with unknown assignments have their assignment removed",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator(bufferAssignmentUnknown)),
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectErr:                 false,
			expectedBufferAssignments: nil,
		},
		{
			name: "Nodes with unknown assignments have their assignment overriden",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator(bufferAssignmentUnknown)),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-1": "buffer",
			},
		},
		{
			name: "Node is outdated and should be tainted with outdatedTaint",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended,
					withBufferAssignmentMutator("ns/buffer"),
					withTimeAddedSuspendedTaintMutator(time.Now().Add(-2*time.Hour))),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			csnPodsBuffersAnnotations: map[string]map[string]string{
				"csn-p1": {
					nodeRefreshFrequencyBufferAnnotation: "1h",
				},
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1", "csn-p1"}, // csn-p1 should be unschedulable because node-1 is outdated.
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{}, // No NEW buffer assignments.
			additionalNodeAssertions: func(t *testing.T, node *apiv1.Node) {
				if node.Name == "node-1" {
					assert.True(t, taints.TaintExists(node.Spec.Taints, &outdatedTaint))
					assert.Equal(t, csn.NodeStateConsumed, csn.ClassifyNode(node))
				}
			},
		},
		{
			name: "Node is outdated but has \"never\" refresh frequency, so it should not be refreshed",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended,
					withBufferAssignmentMutator("ns/buffer"),
					withTimeAddedSuspendedTaintMutator(time.Now().Add(-2*time.Hour))),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			csnPodsBuffersAnnotations: map[string]map[string]string{
				"csn-p1": {
					nodeRefreshFrequencyBufferAnnotation: "never",
				},
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-1": "buffer",
			},
			additionalNodeAssertions: func(t *testing.T, node *apiv1.Node) {
				if node.Name == "node-1" {
					assert.False(t, taints.TaintExists(node.Spec.Taints, &outdatedTaint))
					assert.Equal(t, csn.NodeStateSuspended, csn.ClassifyNode(node))
				}
			},
		},
		{
			name: "Node is outdated and should be tainted with outdatedTaint using default refresh frequency (24h)",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended,
					withBufferAssignmentMutator("ns/buffer"),
					withTimeAddedSuspendedTaintMutator(time.Now().Add(-25*time.Hour))),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1", "csn-p1"},
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{},
			additionalNodeAssertions: func(t *testing.T, node *apiv1.Node) {
				if node.Name == "node-1" {
					assert.True(t, taints.TaintExists(node.Spec.Taints, &outdatedTaint))
					assert.Equal(t, csn.NodeStateConsumed, csn.ClassifyNode(node))
				}
			},
		},
		{
			name: "Node is not outdated with default refresh frequency (24h) if suspended for 23h",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended,
					withBufferAssignmentMutator("ns/buffer"),
					withTimeAddedSuspendedTaintMutator(time.Now().Add(-23*time.Hour))),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1"},
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{
				"node-1": "buffer",
			},
			additionalNodeAssertions: func(t *testing.T, node *apiv1.Node) {
				if node.Name == "node-1" {
					assert.False(t, taints.TaintExists(node.Spec.Taints, &outdatedTaint))
					assert.Equal(t, csn.NodeStateSuspended, csn.ClassifyNode(node))
				}
			},
		},
		{
			name: "Node is outdated if refresh frequency annotation is invalid (falls back to default frequency)",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended,
					withBufferAssignmentMutator("ns/buffer"),
					withTimeAddedSuspendedTaintMutator(time.Now().Add(-48*time.Hour))),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			csnPodsBuffersAnnotations: map[string]map[string]string{
				"csn-p1": {
					nodeRefreshFrequencyBufferAnnotation: "invalid-duration",
				},
			},
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1", "csn-p1"},
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{},
			additionalNodeAssertions: func(t *testing.T, node *apiv1.Node) {
				if node.Name == "node-1" {
					assert.True(t, taints.TaintExists(node.Spec.Taints, &outdatedTaint))
					assert.Equal(t, csn.NodeStateConsumed, csn.ClassifyNode(node))
				}
			},
		},
		{
			name: "Node is outdated and should be tainted with outdatedTaint using CUSTOM default refresh frequency (1h)",
			initialNodes: []*apiv1.Node{
				create8CPUTestNode(t, "node-1", csn.NodeStateSuspended,
					withBufferAssignmentMutator("ns/buffer"),
					withTimeAddedSuspendedTaintMutator(time.Now().Add(-2*time.Hour))),
			},
			csnPods: []*apiv1.Pod{
				test.BuildTestPod("csn-p1", 1000, 1000, withWorkloadSeparation(csn.BufferAssignmentKey, "ns/buffer")),
			},
			csnPodsBuffersNames: map[string]string{
				"csn-p1": "buffer",
			},
			defaultRefreshFrequency:   1 * time.Hour,
			unschedulablePods:         []*apiv1.Pod{test.BuildTestPod("p1", 1000, 1000)},
			expectedUnschedulablePods: []string{"p1", "csn-p1"},
			expectedAllSuspendedNodes: []string{"node-1"},
			expectErr:                 false,
			expectedBufferAssignments: map[string]string{},
			additionalNodeAssertions: func(t *testing.T, node *apiv1.Node) {
				if node.Name == "node-1" {
					assert.True(t, taints.TaintExists(node.Spec.Taints, &outdatedTaint))
					assert.Equal(t, csn.NodeStateConsumed, csn.ClassifyNode(node))
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var csnNodes []nodecontroller.CSNNode
			for _, node := range tc.initialNodes {
				csnNodes = append(csnNodes, nodecontroller.CSNNode{Name: node.Name, DesiredState: csn.ClassifyNode(node)})
			}
			mockNodeController := nodecontrollertesting.NewMockCSNNodeController(csnNodes)
			mockNodeController.SetNonSuspendableNodes(tc.nonSuspendableNodes)
			mockNodeController.SetSuspendError(tc.suspendErr)

			csnPodInjectionProcessor := &mockCapacityBufferPodListProcessor{
				podsToCreate: tc.csnPods,
				err:          tc.csnPodsCreationErr,
			}

			bufferRegistry := fakepods.NewRegistry(nil)
			for _, pod := range tc.csnPods {
				if pod.UID == "" {
					pod.UID = types.UID("uid-" + pod.Name)
				}
				if bufferName, ok := tc.csnPodsBuffersNames[pod.Name]; ok {
					bufferRegistry.SetCapacityBuffer(pod.UID, &v1beta1.CapacityBuffer{
						ObjectMeta: metav1.ObjectMeta{
							Name:        bufferName,
							Namespace:   "ns",
							Annotations: tc.csnPodsBuffersAnnotations[pod.Name],
						},
					})
				}
			}

			defaultRefreshFrequency := tc.defaultRefreshFrequency
			if defaultRefreshFrequency == 0 {
				defaultRefreshFrequency = 24 * time.Hour
			}
			processor := NewCSNPodsLifecycleProcessor(mockNodeController, csnPodInjectionProcessor, nil, bufferRegistry, defaultRefreshFrequency)

			clusterSnapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			for _, node := range tc.initialNodes {
				nodeInfo := framework.NewNodeInfo(node, nil)
				err := clusterSnapshot.AddNodeInfo(nodeInfo)
				assert.NoError(t, err)
			}
			autoscalingContext := &context.AutoscalingContext{
				ClusterSnapshot:      clusterSnapshot,
				ClusterStateRegistry: clusterstate.NewClusterStateRegistry(nil, nil, nil, nil, nil),
			}
			remainingPods, err := processor.Process(autoscalingContext, tc.unschedulablePods)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.ElementsMatch(t, tc.expectedAllSuspendedNodes, mockNodeController.NodesWithState(csn.NodeStateSuspended), "Unexpected suspended nodes")
			remainingPodNames := []string{}
			for _, pod := range remainingPods {
				remainingPodNames = append(remainingPodNames, pod.Name)
			}
			assert.Equal(t, tc.expectedUnschedulablePods, remainingPodNames) // Order is important

			if tc.expectErr {
				return
			}

			actualBufferAssignments := mockNodeController.GetBufferAssignments()
			actualBufferAssignmentsNames := bufferAssignmentNames(actualBufferAssignments)
			assert.Equal(t, tc.expectedBufferAssignments, actualBufferAssignmentsNames, "Unexpected buffer assignments")

			allNodeInfos, err := clusterSnapshot.ListNodeInfos()
			assert.NoError(t, err)
			for _, ni := range allNodeInfos {
				node := ni.Node()
				if expectedBufferName, ok := tc.expectedBufferAssignments[node.Name]; ok {
					assert.Equal(t, fmt.Sprintf("ns/%s", expectedBufferName), csn.GetBufferIdFromNode(node), "Node %s in snapshot has wrong buffer assignment", node.Name)
				}
				if tc.additionalNodeAssertions != nil {
					tc.additionalNodeAssertions(t, node)
				}
			}

			if len(tc.csnPods) > 0 && tc.csnPodsCreationErr == nil {
				csnPodCreatedNames := make(map[string]bool)
				for _, pod := range tc.csnPods {
					csnPodCreatedNames[pod.Name] = true
				}
				scheduledPods := getAllScheduledPods(t, clusterSnapshot)
				allPods := append(remainingPods, slices.Collect(maps.Values(scheduledPods))...)

				for _, pod := range allPods {
					if _, isCsnPod := csnPodCreatedNames[pod.Name]; isCsnPod {
						bufferName, ok := tc.csnPodsBuffersNames[pod.Name]
						if !ok {
							continue
						}
						expectedBufferId := fmt.Sprintf("ns_%s", bufferName)
						assert.Equal(t, csn.SoftWorkloadSeparationValue, pod.Spec.NodeSelector[csn.SoftWorkloadSeparationKey])
						assert.Contains(t, pod.Spec.Tolerations, apiv1.Toleration{
							Key:    csn.SoftWorkloadSeparationKey,
							Value:  csn.SoftWorkloadSeparationValue,
							Effect: apiv1.TaintEffectPreferNoSchedule,
						})
						assert.Contains(t, pod.Spec.Tolerations, apiv1.Toleration{
							Key:    csn.SuspendedTaintKey,
							Value:  csn.SuspendedTaintValue,
							Effect: apiv1.TaintEffectNoSchedule,
						})
						assert.Equal(t, capacitybufferpodlister.CapacityBufferFakePodAnnotationValue, pod.Annotations[capacitybufferpodlister.CapacityBufferFakePodAnnotationKey])
						assert.Equal(t, csn.CSNPodAnnotationValue, pod.Annotations[csn.CSNPodAnnotationKey])

						assert.Len(t, pod.OwnerReferences, 1)
						assert.Equal(t, capacitybuffer.CapacityBufferKind, pod.OwnerReferences[0].Kind)
						assert.Equal(t, bufferName, pod.OwnerReferences[0].Name)

						if _, ok := scheduledPods[pod.Name]; ok {
							assert.Equal(t, expectedBufferId, pod.Spec.NodeSelector[csn.BufferAssignmentKey])
							assert.Contains(t, pod.Spec.Tolerations, apiv1.Toleration{
								Key:    csn.BufferAssignmentKey,
								Value:  expectedBufferId,
								Effect: apiv1.TaintEffectNoSchedule,
							})
						} else {
							assert.NotEqual(t, expectedBufferId, pod.Spec.NodeSelector[csn.BufferAssignmentKey])
							assert.NotContains(t, pod.Spec.Tolerations, apiv1.Toleration{
								Key:    csn.BufferAssignmentKey,
								Value:  expectedBufferId,
								Effect: apiv1.TaintEffectNoSchedule,
							})
						}
					}
				}
			}
		})
	}
}

func bufferAssignmentNames(bufferAssignments map[string]*v1beta1.CapacityBuffer) map[string]string {
	if bufferAssignments == nil {
		return nil
	}
	result := map[string]string{}
	for nodeName, buffer := range bufferAssignments {
		result[nodeName] = buffer.Name
	}
	return result

}

func getAllScheduledPods(t *testing.T, snapshot clustersnapshot.ClusterSnapshot) map[string]*apiv1.Pod {
	scheduledPods := map[string]*apiv1.Pod{}
	nodeInfos, err := snapshot.ListNodeInfos()
	assert.NoError(t, err)
	for _, ni := range nodeInfos {
		podInfos := ni.Pods()
		for _, p := range podInfos {
			scheduledPods[p.Pod.Name] = p.Pod
		}
	}
	return scheduledPods
}

func TestGetRefreshFrequency(t *testing.T) {
	testCases := []struct {
		name                    string
		annotation              string
		defaultRefreshFrequency time.Duration
		expectedDuration        time.Duration
		expectedEnabled         bool
	}{
		{
			name:                    "empty annotation returns default",
			annotation:              "",
			defaultRefreshFrequency: 24 * time.Hour,
			expectedDuration:        24 * time.Hour,
			expectedEnabled:         true,
		},
		{
			name:                    "empty annotation returns custom default",
			annotation:              "",
			defaultRefreshFrequency: 12 * time.Hour,
			expectedDuration:        12 * time.Hour,
			expectedEnabled:         true,
		},
		{
			name:                    "never annotation returns disabled",
			annotation:              "never",
			defaultRefreshFrequency: 24 * time.Hour,
			expectedDuration:        0,
			expectedEnabled:         false,
		},
		{
			name:                    "explicit duration returns duration",
			annotation:              "12h",
			defaultRefreshFrequency: 24 * time.Hour,
			expectedDuration:        12 * time.Hour,
			expectedEnabled:         true,
		},
		{
			name:                    "invalid duration returns default and enabled",
			annotation:              "invalid",
			defaultRefreshFrequency: 24 * time.Hour,
			expectedDuration:        24 * time.Hour,
			expectedEnabled:         true,
		},
		{
			name:                    "invalid duration returns custom default and enabled",
			annotation:              "invalid",
			defaultRefreshFrequency: 1 * time.Hour,
			expectedDuration:        1 * time.Hour,
			expectedEnabled:         true,
		},
		{
			name:                    "negative duration returns default and enabled",
			annotation:              "-1h",
			defaultRefreshFrequency: 24 * time.Hour,
			expectedDuration:        24 * time.Hour,
			expectedEnabled:         true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buffer := &v1beta1.CapacityBuffer{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						nodeRefreshFrequencyBufferAnnotation: tc.annotation,
					},
				},
			}
			if tc.annotation == "" {
				buffer.Annotations = nil
			}
			freq, enabled := getRefreshFrequency(buffer, tc.defaultRefreshFrequency)
			assert.Equal(t, tc.expectedDuration, freq)
			assert.Equal(t, tc.expectedEnabled, enabled)
		})
	}
}

func TestCSNPodsLifecycleProcess_PackingOnUnassignedNodes(t *testing.T) {
	// This test verifies that pods belonging to the same buffer are packed on the same nodes
	// when scheduled on unassigned nodes, instead of being spread across all available nodes.
	// We have 10 pods, each requiring 1600mCPU.
	// We have 4 chilling nodes, each with 8000mCPU capacity (can fit 5 pods).
	// All 10 pods should be scheduled on exactly 2 nodes.

	csnPods := []*apiv1.Pod{}
	for i := 0; i < 10; i++ {
		pod := test.BuildTestPod(fmt.Sprintf("csn-p-%d", i), 1600, 1000)
		csn.MakePodCSN(pod, "ns/buffer")
		csnPods = append(csnPods, pod)
	}

	initialNodes := []*apiv1.Node{
		create8CPUTestNode(t, "node-1", csn.NodeStateChilling),
		create8CPUTestNode(t, "node-2", csn.NodeStateChilling),
		create8CPUTestNode(t, "node-3", csn.NodeStateChilling),
		create8CPUTestNode(t, "node-4", csn.NodeStateChilling),
	}

	csnNodes := []nodecontroller.CSNNode{}
	for _, node := range initialNodes {
		csnNodes = append(csnNodes, nodecontroller.CSNNode{Name: node.Name, DesiredState: csn.ClassifyNode(node)})
	}
	mockNodeController := nodecontrollertesting.NewMockCSNNodeController(csnNodes)

	csnPodInjectionProcessor := &mockCapacityBufferPodListProcessor{
		podsToCreate: csnPods,
	}

	bufferRegistry := fakepods.NewRegistry(nil)
	buffer := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buffer",
			Namespace: "ns",
		},
	}
	for _, pod := range csnPods {
		bufferRegistry.SetCapacityBuffer(pod.UID, buffer)
	}

	processor := NewCSNPodsLifecycleProcessor(mockNodeController, csnPodInjectionProcessor, nil, bufferRegistry, 24*time.Hour)

	clusterSnapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
	for _, node := range initialNodes {
		nodeInfo := framework.NewNodeInfo(node, nil)
		err := clusterSnapshot.AddNodeInfo(nodeInfo)
		assert.NoError(t, err)
	}
	autoscalingContext := &context.AutoscalingContext{
		ClusterSnapshot:      clusterSnapshot,
		ClusterStateRegistry: clusterstate.NewClusterStateRegistry(nil, nil, nil, nil, nil),
	}
	remainingPods, err := processor.Process(autoscalingContext, nil)
	assert.NoError(t, err)
	assert.Empty(t, remainingPods)

	actualBufferAssignments := mockNodeController.GetBufferAssignments()
	assert.Equal(t, 2, len(actualBufferAssignments), "Expected exactly 2 nodes to be assigned to the buffer")

	for nodeName, assignedBuffer := range actualBufferAssignments {
		assert.Equal(t, "buffer", assignedBuffer.Name)
		ni, err := clusterSnapshot.GetNodeInfo(nodeName)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(ni.Pods()), "Expected node %s to have 5 pods", nodeName)
	}
}

type mockFakePodReactionTimeObserver struct {
	mock.Mock
}

func (m *mockFakePodReactionTimeObserver) ObserveCapacityBufferFakePodReactionTime(duration time.Duration, systemPod, hasPVC, hasCSI bool, reactionType metrics.ReactionType, provisioningType, allocationMode string) {
	m.Called(duration, systemPod, hasPVC, hasCSI, reactionType, provisioningType, allocationMode)
}

func TestCSNPodsLifecycleProcessor_Observer(t *testing.T) {
	pod1 := test.BuildTestPod("p1", 100, 100)
	pod1.UID = "p1-uid"
	buffer1 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buffer1",
			Namespace: "ns",
			UID:       "buffer1-uid",
		},
		Status: v1beta1.CapacityBufferStatus{
			ProvisioningStrategy: ptr.To(capacitybuffers.ColdProvisioningStrategy),
		},
	}

	csnPods := []*apiv1.Pod{pod1}
	mockNodeController := nodecontrollertesting.NewMockCSNNodeController(nil)
	csnPodInjectionProcessor := &mockCapacityBufferPodListProcessor{
		podsToCreate: csnPods,
	}

	bufferRegistry := fakepods.NewRegistry(nil)
	bufferRegistry.SetCapacityBuffer(pod1.UID, buffer1)

	fakeClock := clock.NewFakeClock(time.Now())
	mockObserver := &mockFakePodReactionTimeObserver{}
	classifier := systempods.NewClassifier([]string{"kube-system"})
	observer := cbmetrics.NewFakePodStateObserver(classifier, mockObserver, bufferRegistry, fakeClock, false)

	processor := NewCSNPodsLifecycleProcessor(mockNodeController, csnPodInjectionProcessor, observer, bufferRegistry, 24*time.Hour)

	clusterSnapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
	node1 := create8CPUTestNode(t, "node-1", csn.NodeStateChilling, withBufferAssignmentMutator("ns/buffer1"))
	err := clusterSnapshot.AddNodeInfo(framework.NewNodeInfo(node1, nil))
	assert.NoError(t, err)

	autoscalingContext := &context.AutoscalingContext{
		ClusterSnapshot:      clusterSnapshot,
		ClusterStateRegistry: clusterstate.NewClusterStateRegistry(nil, nil, nil, nil, nil),
	}

	// We expect ObserveCapacityBufferFakePodReactionTime to be called when the pod becomes schedulable.
	// Since it is immediately schedulable in our setup, it should be called during Process.
	mockObserver.On("ObserveCapacityBufferFakePodReactionTime", mock.Anything, false, false, false, metrics.NoActionNeeded, capacitybuffers.ColdProvisioningStrategy, mock.Anything).Return()

	_, err = processor.Process(autoscalingContext, nil)
	assert.NoError(t, err)

	mockObserver.AssertExpectations(t)
}

func TestAddOwnerReference(t *testing.T) {
	buffer := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-buffer",
			UID:  "test-buffer-uid",
		},
	}

	testCases := []struct {
		name                   string
		initialOwnerReferences []metav1.OwnerReference
		expectedOwnerRefsCount int
	}{
		{
			name:                   "No owner references",
			initialOwnerReferences: nil,
			expectedOwnerRefsCount: 1,
		},
		{
			name: "Different owner reference",
			initialOwnerReferences: []metav1.OwnerReference{
				{
					Kind: "Deployment",
					Name: "test-deploy",
					UID:  "test-deploy-uid",
				},
			},
			expectedOwnerRefsCount: 2,
		},
		{
			name: "Already has capacitybuffer owner reference",
			initialOwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: capacitybuffer.CapacityBufferApiVersion,
					Kind:       capacitybuffer.CapacityBufferKind,
					Name:       "test-buffer",
					UID:        "test-buffer-uid",
					Controller: ptr.To(true),
				},
			},
			expectedOwnerRefsCount: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: tc.initialOwnerReferences,
				},
			}
			addOwnerReference(pod, buffer)
			assert.Equal(t, tc.expectedOwnerRefsCount, len(pod.OwnerReferences))

			// Verify the CapacityBuffer owner reference is present
			found := false
			for _, ref := range pod.OwnerReferences {
				if ref.Kind == capacitybuffer.CapacityBufferKind {
					found = true
					assert.Equal(t, capacitybuffer.CapacityBufferApiVersion, ref.APIVersion)
					assert.Equal(t, buffer.Name, ref.Name)
					assert.Equal(t, buffer.UID, ref.UID)
					assert.True(t, *ref.Controller)
				}
			}
			assert.True(t, found)
		})
	}
}
