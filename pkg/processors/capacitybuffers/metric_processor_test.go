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

package capacitybuffers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	bufferslisters "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/client/listers/autoscaling.x-k8s.io/v1beta1"
	cbclient "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/client"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	capacitybufferpodlister "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
)

type MockMetrics struct {
	mock.Mock
}

func (m *MockMetrics) UpdateCapacityBufferPods(counts map[metrics.CapacityBufferPodsKey]int) {
	m.Called(counts)
}

func (m *MockMetrics) UpdateCapacityBuffersNumber(countsByType map[string]int) {
	m.Called(countsByType)
}

type MockCapacityBufferLister struct {
	mock.Mock
}

func (m *MockCapacityBufferLister) List(selector labels.Selector) (ret []*v1beta1.CapacityBuffer, err error) {
	args := m.Called(selector)
	return args.Get(0).([]*v1beta1.CapacityBuffer), args.Error(1)
}

func (m *MockCapacityBufferLister) CapacityBuffers(namespace string) bufferslisters.CapacityBufferNamespaceLister {
	return nil
}

func TestMetricProcessor_Process(t *testing.T) {
	strategy1 := "strategy1"
	strategy2 := "strategy2"

	cb1 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb1"},
		Status: v1beta1.CapacityBufferStatus{
			ProvisioningStrategy: &strategy1,
		},
	}
	cb2 := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb2"},
		Status: v1beta1.CapacityBufferStatus{
			ProvisioningStrategy: &strategy2,
		},
	}

	testCases := []struct {
		name              string
		registryMapping   map[types.UID]*v1beta1.CapacityBuffer
		capacityBuffers   []*v1beta1.CapacityBuffer
		nodeInfos         []*framework.NodeInfo
		unschedulablePods []*apiv1.Pod
		expectedCounts    map[metrics.CapacityBufferPodsKey]int
		expectedCbCounts  map[string]int
	}{
		{
			name: "mixed pods and states",
			registryMapping: map[types.UID]*v1beta1.CapacityBuffer{
				"pod1-buf1": cb1,
				"pod2-buf1": cb1,
				"pod3-buf1": cb1,
				"pod1-buf2": cb2,
			},
			capacityBuffers: []*v1beta1.CapacityBuffer{cb1, cb2},
			nodeInfos: []*framework.NodeInfo{
				func() *framework.NodeInfo {
					n := test.BuildTestNode("node-ready", 1000, 1000)
					n.Annotations = make(map[string]string)
					return framework.NewTestNodeInfo(n, createCapacityBufferPod("pod1-buf1"), test.BuildTestPod("pod4", 100, 100))
				}(),
				func() *framework.NodeInfo {
					n := test.BuildTestNode("node-upcoming", 1000, 1000)
					n.Annotations = map[string]string{annotations.NodeUpcomingAnnotation: "true"}
					return framework.NewTestNodeInfo(n, createCapacityBufferPod("pod2-buf1"), createCapacityBufferPod("pod3-buf1"))
				}(),
			},
			unschedulablePods: []*apiv1.Pod{
				createCapacityBufferPod("pod1-buf2"),
			},
			expectedCounts: map[metrics.CapacityBufferPodsKey]int{
				{ProvisioningStrategy: strategy1, State: metrics.CapacityBufferPodStateReady}:        1,
				{ProvisioningStrategy: strategy1, State: metrics.CapacityBufferPodStateProvisioning}: 2,
				{ProvisioningStrategy: strategy2, State: metrics.CapacityBufferPodStateNotReady}:     1,
			},
			expectedCbCounts: map[string]int{
				strategy1: 1,
				strategy2: 1,
			},
		},
		{
			name:            "pod missing from registry",
			registryMapping: map[types.UID]*v1beta1.CapacityBuffer{},
			capacityBuffers: []*v1beta1.CapacityBuffer{},
			unschedulablePods: []*apiv1.Pod{
				createCapacityBufferPod("pod1"),
			},
			expectedCounts:   map[metrics.CapacityBufferPodsKey]int{},
			expectedCbCounts: map[string]int{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			registry := fakepods.NewRegistry(nil)
			for uid, cb := range tc.registryMapping {
				registry.SetCapacityBuffer(uid, cb)
			}

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, nodeInfo := range tc.nodeInfos {
				assert.NoError(t, snapshot.AddNodeInfo(nodeInfo))
			}

			ctx := &ca_context.AutoscalingContext{
				ClusterSnapshot: snapshot,
			}

			mockMetrics := &MockMetrics{}
			mockMetrics.On("UpdateCapacityBufferPods", tc.expectedCounts).Return()
			mockMetrics.On("UpdateCapacityBuffersNumber", tc.expectedCbCounts).Return()

			mockLister := &MockCapacityBufferLister{}
			mockLister.On("List", labels.Everything()).Return(tc.capacityBuffers, nil)

			client, err := cbclient.NewCapacityBufferClient(nil, nil, mockLister, nil, nil, nil, nil, nil, nil, nil, nil)
			assert.NoError(t, err)

			processor := NewMetricProcessor(client, registry, mockMetrics)
			err = processor.ProcessMetrics(ctx, tc.unschedulablePods)
			assert.NoError(t, err)

			mockMetrics.AssertExpectations(t)
			mockLister.AssertExpectations(t)
		})
	}
}

func createCapacityBufferPod(name string) *apiv1.Pod {
	p := test.BuildTestPod(name, 100, 100)
	if p.Annotations == nil {
		p.Annotations = make(map[string]string)
	}
	p.Annotations[capacitybufferpodlister.CapacityBufferFakePodAnnotationKey] = capacitybufferpodlister.CapacityBufferFakePodAnnotationValue
	return p
}

func TestBufferKey(t *testing.T) {
	strategy := "test-strategy"
	cb := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb1"},
		Status: v1beta1.CapacityBufferStatus{
			ProvisioningStrategy: &strategy,
		},
	}
	cbNoStrategy := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{UID: "cb2"},
		Status:     v1beta1.CapacityBufferStatus{},
	}

	pod1 := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod1"}}
	pod2 := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod2"}}
	pod3 := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod3"}}
	pod4 := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod4"}}
	podUnknown := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "unknown"}}

	registry := fakepods.NewRegistry(nil)
	registry.SetCapacityBuffer("pod1", cb)
	registry.SetCapacityBuffer("pod2", cb)
	registry.SetCapacityBuffer("pod3", cb)
	registry.SetCapacityBuffer("pod4", cbNoStrategy)

	nodeReady := &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-ready"}}
	nodeUpcoming := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node-upcoming",
			Annotations: map[string]string{annotations.NodeUpcomingAnnotation: "true"},
		},
	}

	testCases := []struct {
		name     string
		pod      *apiv1.Pod
		node     *apiv1.Node
		expected *metrics.CapacityBufferPodsKey
	}{
		{
			name:     "pod not in registry",
			pod:      podUnknown,
			node:     nodeReady,
			expected: nil,
		},
		{
			name: "node is nil (NotReady state)",
			pod:  pod1,
			node: nil, // Represents an unschedulable pod.
			expected: &metrics.CapacityBufferPodsKey{
				ProvisioningStrategy: strategy,
				State:                metrics.CapacityBufferPodStateNotReady,
			},
		},
		{
			name: "node is upcoming (Provisioning state)",
			pod:  pod2,
			node: nodeUpcoming,
			expected: &metrics.CapacityBufferPodsKey{
				ProvisioningStrategy: strategy,
				State:                metrics.CapacityBufferPodStateProvisioning,
			},
		},
		{
			name: "node is ready (Ready state)",
			pod:  pod3,
			node: nodeReady,
			expected: &metrics.CapacityBufferPodsKey{
				ProvisioningStrategy: strategy,
				State:                metrics.CapacityBufferPodStateReady,
			},
		},
		{
			name: "no provisioning strategy (unknown strategy)",
			pod:  pod4, // This pod is attached to capacity buffer with no strategy.
			node: nodeReady,
			// If the buffer status is missing the strategy, it defaults to "unknown".
			expected: &metrics.CapacityBufferPodsKey{
				ProvisioningStrategy: unknownProvisioningStrategy,
				State:                metrics.CapacityBufferPodStateReady,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := bufferKey(tc.pod, tc.node, registry)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIsUpcomingNode(t *testing.T) {
	testCases := []struct {
		name     string
		node     *apiv1.Node
		expected bool
	}{
		{
			name: "upcoming node",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{annotations.NodeUpcomingAnnotation: "true"},
				},
			},
			expected: true,
		},
		{
			name: "ready node",
			node: &apiv1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			},
			expected: false,
		},
		{
			name:     "node with nil annotations",
			node:     &apiv1.Node{},
			expected: false,
		},
		{
			name:     "nil node",
			node:     nil,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isUpcomingNode(tc.node))
		})
	}
}
