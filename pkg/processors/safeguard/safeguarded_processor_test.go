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

package safeguard

import (
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	podutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/context"
	cb "sigs.k8s.io/cluster-autoscaler/pkg/processors/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/fake"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

var registerOnce sync.Once

func TestProcessor(t *testing.T) {
	registerOnce.Do(metrics.RegisterAll)
	now := time.Now()
	longPendingTime := now.Add(-11 * time.Minute)
	youngPendingTime := now.Add(-5 * time.Minute)

	podOldScheduled := test.BuildTestPod("pod-old-scheduled", 100, 1)
	podOldScheduled.CreationTimestamp = metav1.Time{Time: longPendingTime}

	podYoungScheduled := test.BuildTestPod("pod-young-scheduled", 100, 1)
	podYoungScheduled.CreationTimestamp = metav1.Time{Time: youngPendingTime}

	podOldUnscheduled := test.BuildTestPod("pod-old-unscheduled", 100, 1)
	podOldUnscheduled.CreationTimestamp = metav1.Time{Time: longPendingTime}

	podYoungUnscheduled := test.BuildTestPod("pod-young-unscheduled", 100, 1)
	podYoungUnscheduled.CreationTimestamp = metav1.Time{Time: youngPendingTime}

	podFakeOldScheduled := test.BuildTestPod("pod-fake-old-scheduled", 100, 1)
	podFakeOldScheduled.CreationTimestamp = metav1.Time{Time: longPendingTime}
	podFakeOldScheduled.Annotations = map[string]string{fake.FakePodAnnotationKey: fake.FakePodAnnotationValue}

	podCbFakeOldScheduled := test.BuildTestPod("pod-cb-fake-old-scheduled", 100, 1)
	podCbFakeOldScheduled.CreationTimestamp = metav1.Time{Time: longPendingTime}
	podCbFakeOldScheduled.Annotations = map[string]string{cb.CapacityBufferFakePodAnnotationKey: "true"}

	podPrFakeOldScheduled := test.BuildTestPod("pod-pr-fake-old-scheduled", 100, 1)
	podPrFakeOldScheduled.CreationTimestamp = metav1.Time{Time: longPendingTime}
	podPrFakeOldScheduled.OwnerReferences = []metav1.OwnerReference{{Kind: "ProvisioningRequest"}}

	podMinCapFakeOldScheduled := test.BuildTestPod("pod-mincap-fake-old-scheduled", 100, 1)
	podMinCapFakeOldScheduled.CreationTimestamp = metav1.Time{Time: longPendingTime}
	podMinCapFakeOldScheduled.Annotations = map[string]string{cc_processors.MinCapacityFakePodAnnotation: "true"}

	context := &context.AutoscalingContext{
		AutoscalingOptions: config.AutoscalingOptions{
			ExpendablePodsPriorityCutoff: 0,
		},
	}

	node := test.BuildTestNode("node-1", 100, 100)
	test.SetNodeReadyState(node, true, time.Now())
	// In the simulator snapshot, we add the "Scheduled" pods to the ready node's scheduled list.
	podInfoOldScheduled := framework.NewPodInfo(podOldScheduled, nil)
	podInfoYoungScheduled := framework.NewPodInfo(podYoungScheduled, nil)
	podInfoFakeOldScheduled := framework.NewPodInfo(podFakeOldScheduled, nil)
	podInfoCbFakeOldScheduled := framework.NewPodInfo(podCbFakeOldScheduled, nil)
	podInfoPrFakeOldScheduled := framework.NewPodInfo(podPrFakeOldScheduled, nil)
	podInfoMinCapFakeOldScheduled := framework.NewPodInfo(podMinCapFakeOldScheduled, nil)
	nodeInfo := framework.NewNodeInfo(node, nil, podInfoOldScheduled, podInfoYoungScheduled, podInfoFakeOldScheduled, podInfoCbFakeOldScheduled, podInfoPrFakeOldScheduled, podInfoMinCapFakeOldScheduled)

	context.ClusterSnapshot = &mockClusterSnapshot{
		nodeInfos: []*framework.NodeInfo{nodeInfo},
	}

	testCases := []struct {
		desc       string
		restore    bool
		before     []*v1.Pod
		after      []*v1.Pod
		want       []*v1.Pod
		wantMetric float64
	}{
		// Group 1: No dropped pods.
		// Verifies that when no pods are dropped, the processor is a no-op (no metrics are recorded, no restorations are performed).
		{
			desc:       "No pods dropped",
			restore:    true,
			before:     []*v1.Pod{podOldScheduled, podYoungScheduled},
			after:      []*v1.Pod{podOldScheduled, podYoungScheduled},
			want:       []*v1.Pod{podOldScheduled, podYoungScheduled},
			wantMetric: 0,
		},

		// Group 2: Simulator Scheduled Pods (Candidates for false positives).
		// Verifies that a pod scheduled in the simulator triggers the safeguard ONLY if it is old (pending > 10m).
		// - podOldScheduled: Old, scheduled in simulator -> Triggers.
		//   - If restore=true: Metric is incremented AND the pod is restored to the unschedulable list.
		//   - If restore=false: Metric is incremented BUT the pod remains dropped.
		// - podYoungScheduled: Young, scheduled in simulator -> Ignored because of age.
		{
			desc:       "Old scheduled pod dropped - restored (restore=true)",
			restore:    true,
			before:     []*v1.Pod{podOldScheduled, podYoungScheduled},
			after:      []*v1.Pod{podYoungScheduled},
			want:       []*v1.Pod{podYoungScheduled, podOldScheduled},
			wantMetric: 1,
		},
		{
			desc:       "Old scheduled pod dropped - not restored (restore=false)",
			restore:    false,
			before:     []*v1.Pod{podOldScheduled, podYoungScheduled},
			after:      []*v1.Pod{podYoungScheduled},
			want:       []*v1.Pod{podYoungScheduled},
			wantMetric: 1,
		},
		{
			desc:       "Young scheduled pod dropped - ignored (too young)",
			restore:    true,
			before:     []*v1.Pod{podOldScheduled, podYoungScheduled},
			after:      []*v1.Pod{podOldScheduled},
			want:       []*v1.Pod{podOldScheduled},
			wantMetric: 0,
		},

		// Group 3: Simulator Unscheduled Pods.
		// Verifies that pods NOT scheduled in the simulator are ignored, regardless of age.
		{
			desc:       "Old unscheduled pod dropped - ignored",
			restore:    true,
			before:     []*v1.Pod{podOldUnscheduled, podYoungScheduled},
			after:      []*v1.Pod{podYoungScheduled},
			want:       []*v1.Pod{podYoungScheduled},
			wantMetric: 0,
		},
		{
			desc:       "Young unscheduled pod dropped - ignored",
			restore:    true,
			before:     []*v1.Pod{podYoungUnscheduled, podYoungScheduled},
			after:      []*v1.Pod{podYoungScheduled},
			want:       []*v1.Pod{podYoungScheduled},
			wantMetric: 0,
		},

		// Group 4: Fake / Synthetic Pods (Should be ignored regardless of age or readiness).
		{
			desc:       "Old lookahead/controller fake pod dropped - ignored",
			restore:    true,
			before:     []*v1.Pod{podFakeOldScheduled, podYoungScheduled},
			after:      []*v1.Pod{podYoungScheduled},
			want:       []*v1.Pod{podYoungScheduled},
			wantMetric: 0,
		},
		{
			desc:       "Old capacity buffer fake pod dropped - ignored",
			restore:    true,
			before:     []*v1.Pod{podCbFakeOldScheduled, podYoungScheduled},
			after:      []*v1.Pod{podYoungScheduled},
			want:       []*v1.Pod{podYoungScheduled},
			wantMetric: 0,
		},
		{
			desc:       "Old ProvisioningRequest fake pod dropped - ignored",
			restore:    true,
			before:     []*v1.Pod{podPrFakeOldScheduled, podYoungScheduled},
			after:      []*v1.Pod{podYoungScheduled},
			want:       []*v1.Pod{podYoungScheduled},
			wantMetric: 0,
		},
		{
			desc:       "Old MinCapacity fake pod dropped - ignored",
			restore:    true,
			before:     []*v1.Pod{podMinCapFakeOldScheduled, podYoungScheduled},
			after:      []*v1.Pod{podYoungScheduled},
			want:       []*v1.Pod{podYoungScheduled},
			wantMetric: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			metrics.ResetAllForTest()
			mockProcessor := &mockStaticPodListProcessor{after: tc.after}
			processor := NewSafeguardedPodListProcessor(mockProcessor, metrics.FilterOutSchedulable, tc.restore)
			processor.falsePositiveFirstSeen = map[podutil.PodKey]time.Time{
				{UID: podOldScheduled.UID, Name: podOldScheduled.Name}:     longPendingTime,
				{UID: podOldUnscheduled.UID, Name: podOldUnscheduled.Name}: longPendingTime,
			}
			processor.lastSeen = map[podutil.PodKey]time.Time{
				{UID: podOldScheduled.UID, Name: podOldScheduled.Name}:     now,
				{UID: podOldUnscheduled.UID, Name: podOldUnscheduled.Name}: now,
			}

			got, err := processor.Process(context, tc.before)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if diff := cmp.Diff(tc.want, got, cmp.Comparer(func(a, b *v1.Pod) bool {
				return a.UID == b.UID && a.Name == b.Name
			})); diff != "" {
				t.Errorf("Process returned unexpected diff (-want +got):\n%s", diff)
			}

			metricVal, err := metrics.GetFilteredFalsePositiveSchedulablePodsEventsForTest(metrics.FilterOutSchedulable)
			if err != nil {
				t.Fatalf("failed to retrieve metric value: %v", err)
			}
			if metricVal != tc.wantMetric {
				t.Errorf("Process updated metric to %v, want %v", metricVal, tc.wantMetric)
			}
		})
	}
}

type mockStaticPodListProcessor struct {
	after []*v1.Pod
}

func (m *mockStaticPodListProcessor) Process(ctx *context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	return m.after, nil
}

func (m *mockStaticPodListProcessor) CleanUp() {}

type mockClusterSnapshot struct {
	clustersnapshot.ClusterSnapshot
	nodeInfos []*framework.NodeInfo
}

func (m *mockClusterSnapshot) ListNodeInfos() ([]*framework.NodeInfo, error) {
	return m.nodeInfos, nil
}
