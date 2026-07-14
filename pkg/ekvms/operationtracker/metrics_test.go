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

package operationtracker

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	clock "k8s.io/utils/clock/testing"
)

func TestObserveMaxSize(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			tests := []struct {
				name string
				ages []time.Duration
			}{
				{
					name: "One node with zero age",
					ages: []time.Duration{0},
				},
				{
					name: "One node with positive age",
					ages: []time.Duration{10},
				},
				{
					name: "Three nodes",
					ages: []time.Duration{10, 20, 50},
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					nodes := make(ResizableNodesSnapshot)
					for i := range len(tt.ages) {
						nodes[fmt.Sprintf("node-%d", i)] = ResizableNode{Node: &v1.Node{}, MachineFamily: family}
					}
					nodeSizeRecommender := mockNodeSizeRecommender{}
					metrics := mockPeriodicMetrics{}
					testClock := clock.NewFakeClock(testStartTime)
					nsm := &nodeStateManagerImpl{
						clock:    testClock,
						nodes:    nodes,
						provider: newMockResizingProvider(func(string) bool { return true }),
					}
					metricsExporter := newMetricsExporter(nsm, &nodeSizeRecommender, &metrics, testClock)
					metricsExporter.observeMaxSizeRecommendationAges()
					testClock.Sleep(metricsGracePeriod)
					for _, age := range tt.ages {
						creationTime := testClock.Now().Add(-1 * age)
						nodeSizeRecommender.On("MaxSize", mock.Anything).Return(&nodesizerecommender.MaxSizeRecommendation{CreationTime: creationTime}).Once()
					}
					for _, age := range tt.ages {
						metrics.On("ObserveMaxSizeRecommendationAge", age).Once()
					}
					metricsExporter.observeMaxSizeRecommendationAges()
					metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", len(tt.ages))
				})
			}
		})
	}
}

func TestObserveMaxSize_MissingRecommendation(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			nodeSizeRecommender := mockNodeSizeRecommender{}
			metrics := mockPeriodicMetrics{}
			testClock := clock.NewFakeClock(testStartTime)
			nsm := &nodeStateManagerImpl{
				clock: testClock,
				nodes: ResizableNodesSnapshot{
					"node-1": ResizableNode{Node: &v1.Node{}, MachineFamily: family},
				},
				provider: newMockResizingProvider(func(string) bool { return true }),
			}
			metricsExporter := newMetricsExporter(nsm, &nodeSizeRecommender, &metrics, testClock)
			metricsExporter.observeMaxSizeRecommendationAges()
			testClock.Sleep(metricsGracePeriod)
			nodeSizeRecommender.On("MaxSize", mock.Anything).Return(nil)
			metrics.On("ObserveMaxSizeRecommendationAge", testClock.Now().Sub(time.Time{})).Once()
			metricsExporter.observeMaxSizeRecommendationAges()
			metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 1)
		})
	}
}

func TestObserveMaxSize_GracePeriod(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			nodeSizeRecommender := mockNodeSizeRecommender{}
			metrics := mockPeriodicMetrics{}
			testClock := clock.NewFakeClock(testStartTime)
			nsm := &nodeStateManagerImpl{
				clock: testClock,
				nodes: ResizableNodesSnapshot{
					"node-1": ResizableNode{Node: &v1.Node{}, MachineFamily: family},
				},
				provider: newMockResizingProvider(func(string) bool { return true }),
			}
			metricsExporter := newMetricsExporter(nsm, &nodeSizeRecommender, &metrics, testClock)
			nodeSizeRecommender.On("MaxSize", mock.Anything).Return(nil)

			metricsExporter.observeMaxSizeRecommendationAges()
			metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 0)

			testClock.Sleep(metricsGracePeriod)

			metrics.On("ObserveMaxSizeRecommendationAge", testClock.Now().Sub(time.Time{})).Once()
			metricsExporter.observeMaxSizeRecommendationAges()
			metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 1)
		})
	}
}

func TestObserveMaxSize_EnableToDisable_ResetsTimer(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			nodeSizeRecommender := mockNodeSizeRecommender{}
			metrics := mockPeriodicMetrics{}
			testClock := clock.NewFakeClock(testStartTime)
			isEnabled := true
			nsm := &nodeStateManagerImpl{
				clock: testClock,
				nodes: ResizableNodesSnapshot{
					"node-1": ResizableNode{Node: &v1.Node{}, MachineFamily: family},
				},
				provider: newMockResizingProvider(func(string) bool { return isEnabled }),
			}
			metricsExporter := newMetricsExporter(nsm, &nodeSizeRecommender, &metrics, testClock)
			nodeSizeRecommender.On("MaxSize", mock.Anything).Return(&nodesizerecommender.MaxSizeRecommendation{CreationTime: testStartTime})

			testClock.Sleep(metricsGracePeriod)
			metrics.On("ObserveMaxSizeRecommendationAge", mock.Anything).Once()
			metricsExporter.observeMaxSizeRecommendationAges()
			metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 1)

			isEnabled = false

			metricsExporter.observeMaxSizeRecommendationAges()
			metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 1)

			//Enabled, but in grace period, no metrics
			isEnabled = true
			testClock.Sleep(1 * time.Minute)

			metricsExporter.observeMaxSizeRecommendationAges()
			metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 1)

			testClock.Sleep(metricsGracePeriod)
			metrics.On("ObserveMaxSizeRecommendationAge", mock.Anything).Once()
			metricsExporter.observeMaxSizeRecommendationAges()
			metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 2)
		})
	}
}

func TestObserveMaxSize_DisableWithNoNodes_ResetsTimer(t *testing.T) {
	nodeSizeRecommender := mockNodeSizeRecommender{}
	metrics := mockPeriodicMetrics{}
	testClock := clock.NewFakeClock(testStartTime)
	isEnabled := true
	nodes := ResizableNodesSnapshot{
		"node-1": ResizableNode{Node: &v1.Node{}, MachineFamily: "ek"},
	}
	nsm := &nodeStateManagerImpl{
		clock:    testClock,
		nodes:    nodes,
		provider: newMockResizingProvider(func(string) bool { return isEnabled }),
	}
	metricsExporter := newMetricsExporter(nsm, &nodeSizeRecommender, &metrics, testClock)
	nodeSizeRecommender.On("MaxSize", mock.Anything).Return(&nodesizerecommender.MaxSizeRecommendation{CreationTime: testStartTime})

	testClock.Sleep(metricsGracePeriod)
	metrics.On("ObserveMaxSizeRecommendationAge", mock.Anything).Once()
	metricsExporter.observeMaxSizeRecommendationAges()
	metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 1)

	// Disable resizing AND remove nodes.
	isEnabled = false
	delete(nodes, "node-1")

	metricsExporter.observeMaxSizeRecommendationAges()
	metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 1)

	// Re-enable resizing and add node back.
	isEnabled = true
	nodes["node-1"] = ResizableNode{Node: &v1.Node{}, MachineFamily: "ek"}

	// Should be in grace period now because it was reset.
	testClock.Sleep(1 * time.Minute)
	metricsExporter.observeMaxSizeRecommendationAges()
	metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 1)

	// Wait for grace period to expire.
	testClock.Sleep(metricsGracePeriod)
	metrics.On("ObserveMaxSizeRecommendationAge", mock.Anything).Once()
	metricsExporter.observeMaxSizeRecommendationAges()
	metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 2)
}

func TestObserveMaxSize_NodeGracePeriod(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			nodeSizeRecommender := mockNodeSizeRecommender{}
			metrics := mockPeriodicMetrics{}
			testClock := clock.NewFakeClock(testStartTime)
			nsm := &nodeStateManagerImpl{
				clock: testClock,
				nodes: ResizableNodesSnapshot{
					"node-1": ResizableNode{Node: &v1.Node{}, MachineFamily: family},
				},
				provider: newMockResizingProvider(func(string) bool { return true }),
			}
			metricsExporter := newMetricsExporter(nsm, &nodeSizeRecommender, &metrics, testClock)
			nodeSizeRecommender.On("MaxSize", mock.Anything).Return(&nodesizerecommender.MaxSizeRecommendation{CreationTime: testStartTime})

			metricsExporter.observeMaxSizeRecommendationAges()
			metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 0)

			testClock.Sleep(metricsNodeGracePeriod)

			metrics.On("ObserveMaxSizeRecommendationAge", testClock.Now().Sub(testStartTime)).Once()
			metricsExporter.observeMaxSizeRecommendationAges()
			metrics.AssertNumberOfCalls(t, "ObserveMaxSizeRecommendationAge", 1)
		})
	}
}

func TestObserveMaxSize_Disabled(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			nodeSizeRecommender := mockNodeSizeRecommender{}
			metrics := mockPeriodicMetrics{}
			testClock := clock.NewFakeClock(testStartTime)
			nsm := &nodeStateManagerImpl{
				clock: testClock,
				nodes: ResizableNodesSnapshot{
					"node-1": ResizableNode{Node: &v1.Node{}, MachineFamily: family},
				},
				provider: newMockResizingProvider(func(string) bool { return false }),
			}
			metricsExporter := newMetricsExporter(nsm, &nodeSizeRecommender, &metrics, testClock)
			testClock.Sleep(metricsNodeGracePeriod)

			metricsExporter.observeMaxSizeRecommendationAges()

			metrics.AssertNotCalled(t, "ObserveMaxSizeRecommendationAge", mock.Anything)
		})
	}
}

type mockPeriodicMetrics struct {
	mock.Mock
}

func (m *mockPeriodicMetrics) ObserveMaxSizeRecommendationAge(age time.Duration) {
	m.Called(age)
}

func (m *mockPeriodicMetrics) UpdateEkBackoffStatus(isBackedOff bool) {
	m.Called(isBackedOff)
}
