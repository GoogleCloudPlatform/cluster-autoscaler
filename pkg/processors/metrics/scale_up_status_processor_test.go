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

package metrics_processors

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	provreq_pods "k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/pods"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	kubeutil "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
)

// SETUP

type testObserver struct {
	longUnschedulable                    int
	unschedulableDurations               []time.Duration
	schedulingDurations                  map[string][]time.Duration
	schedulingDurationsPerCcc            map[string][]time.Duration
	schedulingDurationsPerAllocationMode map[string][]time.Duration
	longUnSchedLabels                    []map[string]string
	unSchedDurationLabels                []map[string]string
}

func (to *testObserver) observePodUnschedulableDuration(duration time.Duration, labels map[string]string) {
	to.unschedulableDurations = append(to.unschedulableDurations, duration)
	to.unSchedDurationLabels = append(to.unSchedDurationLabels, labels)
}

func (to *testObserver) observePodSchedulingDuration(duration time.Duration, stockout, entityName, allocationMode string) {
	to.schedulingDurations[stockout] = append(to.schedulingDurations[stockout], duration)
	to.schedulingDurationsPerAllocationMode[allocationMode] = append(to.schedulingDurationsPerAllocationMode[allocationMode], duration)
	to.schedulingDurationsPerCcc[entityName] = append(to.schedulingDurationsPerCcc[entityName], duration)
}

type mockNpcCrdLister struct {
	npc_lister.Lister
	crdName string
	err     error
}

func (m *mockNpcCrdLister) PodCrd(pod *apiv1.Pod) (crd.CRD, string, error) {
	return nil, m.crdName, m.err
}

func (to *testObserver) setLongUnschedulablePodCount(l labelAndCount) {
	to.longUnschedulable = l.count
	for i := 0; i < l.count; i++ {
		to.longUnSchedLabels = append(to.longUnSchedLabels, l.labels)
	}
}

func setUpProcessor(autoscalerStart time.Time) (*PodStatusAggregator, *testObserver, *ScaleUpStatusMetricsProcessor, filter.MetricsFilter) {
	aggregator := NewPodStatusAggregator()
	observer := &testObserver{
		schedulingDurations:                  make(map[string][]time.Duration),
		schedulingDurationsPerAllocationMode: make(map[string][]time.Duration),
		schedulingDurationsPerCcc:            make(map[string][]time.Duration),
		longUnSchedLabels:                    make([]map[string]string, 0),
		unSchedDurationLabels:                make([]map[string]string, 0),
	}
	f := filter.NewMetricsFilter()
	mockLister := &mockNpcCrdLister{}
	processor := NewScaleUpStatusMetricsProcessor(aggregator, f, mockLister)
	processor.observer = observer
	processor.autoscalerStartTime = autoscalerStart
	return aggregator, observer, processor, f
}

func setUpContext(scheduledPods []*apiv1.Pod) *context.AutoscalingContext {
	// This is a quickfix, ideally given pods should be already recognized as 'scheduled'.
	for _, pod := range scheduledPods {
		pod.Spec.NodeName = "test-node"
	}
	lister := kubeutil.NewTestPodLister(scheduledPods)
	context := &context.AutoscalingContext{}
	listerRegistry := kubeutil.NewListerRegistry(nil, nil, lister, nil, nil, nil, nil, nil, nil)
	context.ListerRegistry = listerRegistry
	return context
}

func makePendingPod(uid string, pendingSince time.Time) *apiv1.Pod {
	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(uid),
		},
		Status: apiv1.PodStatus{
			Conditions: []apiv1.PodCondition{
				{
					Type:               apiv1.PodScheduled,
					Status:             apiv1.ConditionFalse,
					Reason:             apiv1.PodReasonUnschedulable,
					LastTransitionTime: metav1.NewTime(pendingSince),
				},
			},
		},
	}
}

func schedulePod(pod *apiv1.Pod, scheduleTime time.Time) {
	if len(pod.Status.Conditions) == 0 {
		pod.Status.Conditions = make([]apiv1.PodCondition, 1)
	}
	pod.Status.Conditions[0] = apiv1.PodCondition{
		Type:               apiv1.PodScheduled,
		Status:             apiv1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(scheduleTime),
	}
	pod.Spec.NodeName = "test-node"
}

// LONG UNSCHEDULABLE TESTS

func TestLongUnschedulable(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	longUnschedulable := makePendingPod("longUnschedulable", now.Add(-2*longUnschedulableThreshold))

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{longUnschedulable}

	scaleUpStatus := &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}
	context := setUpContext([]*apiv1.Pod{})

	processor.Process(context, scaleUpStatus)
	assert.Equal(t, 1, observer.longUnschedulable)

	// Make sure running again doesn't increase the metric
	processor.Process(context, scaleUpStatus)
	assert.Equal(t, 1, observer.longUnschedulable)

	// Make sure removing a pod decreases the metric
	aggregator.Unschedulable = []*apiv1.Pod{}
	processor.Process(context, scaleUpStatus)
	assert.Equal(t, 0, observer.longUnschedulable)
}

func TestNoLongUnschedulableAfterRestart(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-1 * time.Minute)
	longUnschedulable := makePendingPod("longUnschedulable", now.Add(-2*longUnschedulableThreshold))

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{longUnschedulable}

	scaleUpStatus := &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}
	context := setUpContext([]*apiv1.Pod{})

	processor.Process(context, scaleUpStatus)
	// We only count pod as unschedulable since CA restart, which was too recent
	// to have longUnschedulable pods
	assert.Equal(t, 0, observer.longUnschedulable)
}

func TestNoLongUnschedulableIfCantBeHelped(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	longUnschedulable := makePendingPod("longUnschedulable", now.Add(-2*longUnschedulableThreshold))

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{longUnschedulable}

	scaleUpStatusUnhelpable := &status.ScaleUpStatus{
		Result: status.ScaleUpNotNeeded,
		PodsRemainUnschedulable: []status.NoScaleUpInfo{
			{Pod: longUnschedulable},
		},
	}
	scaleUpStatusAfterUnrelatedScaleUp := &status.ScaleUpStatus{
		Result: status.ScaleUpSuccessful,
	}
	context := setUpContext([]*apiv1.Pod{})

	processor.Process(context, scaleUpStatusUnhelpable)
	// Pod is pending, but CA can't help it
	// Not our fault, no faul
	assert.Equal(t, 0, observer.longUnschedulable)

	processor.Process(context, scaleUpStatusAfterUnrelatedScaleUp)
	// Pod is still pending and it's not explicitly reported as unhelpable.
	// The pod may still be unhelpable if we're in an ongoing backoff-retry
	// loop due to stockout or hitting GCE quota.
	// We shouldn't report longUnschedulable just yet.
	assert.Equal(t, 0, observer.longUnschedulable)

	later := now.Add(unhelpableGracePeriod + time.Minute)
	processor.Process(context, scaleUpStatusAfterUnrelatedScaleUp)
	// Ok, the pod hasn't been reported as unhelpable for a while.
	// We should report it as longUnschedulable again.
	processor.processImpl(context, scaleUpStatusAfterUnrelatedScaleUp, later)
	assert.Equal(t, 1, observer.longUnschedulable)
}

// HISTOGRAM TESTS

func TestPodScheduled(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("PoddyThePrettyPod", now.Add(-1*time.Minute))

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}
	context := setUpContext([]*apiv1.Pod{})

	// Poddy is spotted!
	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// Poddy is scheduled at t+2
	later := now.Add(2 * time.Minute)
	schedulePod(pod, later)
	aggregator.Unschedulable = []*apiv1.Pod{}
	context = setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, scaleUpStatus, later.Add(1*time.Minute))
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 1, len(observer.schedulingDurations["false"]))
	// unschedulable since now-1, scheduled at now+2
	assert.Equal(t, 3*time.Minute, observer.schedulingDurations["false"][0])
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	assert.Equal(t, 3*time.Minute, observer.unschedulableDurations[0])
}

func TestPodScheduledAfterBeingSchedulable(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("PoddyNoLongerSchedulesInstantly", now.Add(-1*time.Minute))

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}
	context := setUpContext([]*apiv1.Pod{})

	// Poddy is spotted!
	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// Poddy is now schedulable
	later := now.Add(2 * time.Minute)
	processor.processImpl(context, scaleUpStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// Poddy is scheduled
	evenLater := later.Add(2 * time.Minute)
	schedulePod(pod, evenLater)
	// Poddy was still not scheduled at the start of the loop!
	// What a slacker. Good that our processor should handle that just fine.
	// So we won't do this:
	// aggregator.Unschedulable = []*apiv1.Pod{}
	context = setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, scaleUpStatus, evenLater.Add(1*time.Minute))
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 1, len(observer.schedulingDurations["false"]))
	// this metric doesn't care about what CA thinks - it's how long it takes to
	// really schedule pod 1 (before now) + 2 (later) + 2 (evenLater) = 5
	assert.Equal(t, 5*time.Minute, observer.schedulingDurations["false"][0])
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	assert.Equal(t, 5*time.Minute, observer.unschedulableDurations[0])
}

func TestPodScheduledAfterBeingSchedulableAndUnschedulableAgain(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("HungryPoddyStarvedByScheduler", now.Add(-1*time.Minute))

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}
	context := setUpContext([]*apiv1.Pod{})

	// Poddy is spotted!
	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// Poddy is now schedulable
	later := now.Add(2 * time.Minute)
	processor.processImpl(context, scaleUpStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// And unschedulable again
	later = later.Add(2 * time.Minute)
	processor.processImpl(context, scaleUpStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// Poddy is scheduled
	evenLater := later.Add(2 * time.Minute)
	schedulePod(pod, evenLater)
	aggregator.Unschedulable = []*apiv1.Pod{}
	context = setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, scaleUpStatus, evenLater.Add(1*time.Minute))
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 1, len(observer.schedulingDurations["false"]))
	// this metric still doesn't care whether Poddy was ever schedulable
	// 1 (before now) + 2 (schedulable) + 2 (unschedulable) + 2 (evenLater) = 7
	assert.Equal(t, 7*time.Minute, observer.schedulingDurations["false"][0])
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	// apparently CA incorrectly predicted scheduler's behavior and Poddy
	// was actually unschedulable all that time
	assert.Equal(t, 7*time.Minute, observer.unschedulableDurations[0])
}

func TestPodScheduledAfterBeingUnhelpable(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("LittleRedRidingPoddy", now.Add(-5*time.Minute))

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaryScaleUpStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotNeeded,
		PodsRemainUnschedulable: []status.NoScaleUpInfo{
			{Pod: pod},
		},
	}
	context := setUpContext([]*apiv1.Pod{})

	// Poddy is spotted! The wolf must be nearby, autoscaler really can't help.
	processor.processImpl(context, scaryScaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// The wolf is still there.
	later := now.Add(2 * time.Minute)
	processor.processImpl(context, scaryScaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// Finally! The wolf is gone.
	happyScaleUpStatus := &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}
	later = later.Add(2 * time.Minute)
	processor.processImpl(context, happyScaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// And Poddy has scheduled in grandma's house (at t+5)
	evenLater := later.Add(time.Minute)
	schedulePod(pod, evenLater)
	aggregator.Unschedulable = []*apiv1.Pod{}
	context = setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, happyScaleUpStatus, evenLater.Add(time.Minute))
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 1, len(observer.schedulingDurations["false"]))
	// This is still real scheduling time: 5 (before 'now') + 2 + 2 + 1
	assert.Equal(t, 10*time.Minute, observer.schedulingDurations["false"][0])
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	// But autoscaler could only help once the wolf was gone.
	// 2 + 2 + 1 minutes since we last saw him.
	assert.Equal(t, 5*time.Minute, observer.unschedulableDurations[0])
}

func TestUnschedulableDurationWithPodFilterableIssues(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("p1", now.Add(-5*time.Minute))

	aggregator, observer, processor, filter := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{pod},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	context := setUpContext([]*apiv1.Pod{})

	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	filter.ObserveNodeGroupFilterableIssue("ng1")

	later := now.Add(2 * time.Minute)
	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}
	context = setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, newStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))
}

func TestForgetOldPods(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("p1", now.Add(-5*time.Minute))

	aggregator, observer, processor, filter := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{pod},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng3", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	context := setUpContext([]*apiv1.Pod{})

	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	filter.ObserveNodeGroupStockOut("ng1")

	later := now.Add(2 * time.Minute)
	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}
	context = setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, newStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 1, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	assert.Equal(t, []map[string]string{{
		outOfResourceMetricLabel:          "false",
		gpuMetricLabel:                    "false",
		placementMetricLabel:              "TYPE_UNSPECIFIED",
		consumingProvisioningRequestLabel: "",
		tpuMetricLabel:                    "false",
		deviceAllocationModeMetricLabel:   podutils.AllocationModeNone.String(),
	}}, observer.unSchedDurationLabels)
}

func TestUnschedulableDurationWithPreemptionVmRequired(t *testing.T) {
	for _, preemptionLabel := range []string{labels.PreemptibleLabel, labels.SpotLabel} {
		t.Run(preemptionLabel, func(t *testing.T) {
			now := time.Now()
			autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
			pod := makePendingPod("p1", now.Add(-5*time.Minute))
			pod.Spec.NodeSelector = make(map[string]string)
			pod.Spec.NodeSelector[preemptionLabel] = labels.PreemptionValue

			aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
			aggregator.Unschedulable = []*apiv1.Pod{pod}

			scaleUpStatus := &status.ScaleUpStatus{
				Result:               status.ScaleUpSuccessful,
				PodsTriggeredScaleUp: []*apiv1.Pod{pod},
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
					},
					{
						Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
					},
				},
			}
			context := setUpContext([]*apiv1.Pod{})

			processor.processImpl(context, scaleUpStatus, now)
			assert.Equal(t, 0, observer.longUnschedulable)
			assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
			assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
			assert.Equal(t, 0, len(observer.unschedulableDurations))

			later := now.Add(2 * time.Minute)
			newStatus := &status.ScaleUpStatus{
				Result: status.ScaleUpNotTried,
			}
			context = setUpContext([]*apiv1.Pod{pod})
			processor.processImpl(context, newStatus, later)
			assert.Equal(t, 0, observer.longUnschedulable)
			assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
			assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
			assert.Equal(t, 0, len(observer.unschedulableDurations))
		})
	}
}

func TestPodStockoutIssuesOnlyOneNodeGroupStockout(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("p1", now.Add(-5*time.Minute))

	aggregator, observer, processor, filter := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{pod},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng3", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	context := setUpContext([]*apiv1.Pod{})

	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	filter.ObserveNodeGroupStockOut("ng1")

	later := now.Add(2 * time.Minute)
	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}
	context = setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, newStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 1, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	assert.Equal(t, []map[string]string{{
		outOfResourceMetricLabel:          "false",
		gpuMetricLabel:                    "false",
		placementMetricLabel:              "TYPE_UNSPECIFIED",
		consumingProvisioningRequestLabel: "",
		tpuMetricLabel:                    "false",
		deviceAllocationModeMetricLabel:   podutils.AllocationModeNone.String(),
	}}, observer.unSchedDurationLabels)
}

func TestPodIsConsumingProvisioningRequest(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("p1", now.Add(2*time.Minute))
	pod.Annotations = map[string]string{
		provreq_pods.DeprecatedProvisioningRequestPodAnnotationKey: "test-name-of-the-provisioning-request",
	}

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}
	context := setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, newStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 1, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	assert.Equal(t, []map[string]string{{
		outOfResourceMetricLabel:          "false",
		gpuMetricLabel:                    "false",
		placementMetricLabel:              "TYPE_UNSPECIFIED",
		consumingProvisioningRequestLabel: "test-name-of-the-provisioning-request",
		tpuMetricLabel:                    "false",
		deviceAllocationModeMetricLabel:   podutils.AllocationModeNone.String(),
	}}, observer.unSchedDurationLabels)
}

func TestPodIsUsingDeviceAllocationMode(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("p1", now.Add(2*time.Minute))
	pod.Spec.ResourceClaims = []apiv1.PodResourceClaim{
		{
			Name: "test-resource-claim",
		},
	}

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}
	context := setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, newStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 1, len(observer.schedulingDurations))
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	assert.Equal(t, []map[string]string{{
		outOfResourceMetricLabel:          "false",
		gpuMetricLabel:                    "false",
		tpuMetricLabel:                    "false",
		placementMetricLabel:              "TYPE_UNSPECIFIED",
		consumingProvisioningRequestLabel: "",
		deviceAllocationModeMetricLabel:   podutils.AllocationModeDra.String(),
	}}, observer.unSchedDurationLabels)
}

func TestPodIsNotProcessedByScheduler(t *testing.T) {
	now := time.Now()
	defaultAutoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	for name, tc := range map[string]struct {
		createdAt                          time.Time
		scheduledAt                        time.Time
		processedAt                        time.Time
		autoscalerStart                    time.Time
		initialLongUnschedulable           int
		initialSchedulingDurationsLength   int
		initialUnschedulingDurations       []time.Duration
		processedLongUnschedulable         int
		processedSchedulingDurationsLength int
		processedUnschedulingDurations     []time.Duration
		laterLongUnschedulable             int
		laterSchedulingDurationsLength     int
		laterUnschedulingDurations         []time.Duration
	}{
		"Pod is scheduled normally": {
			autoscalerStart:                defaultAutoscalerStart,
			createdAt:                      now.Add(-5 * time.Minute),
			processedAt:                    now,
			scheduledAt:                    now.Add(5 * time.Minute),
			laterSchedulingDurationsLength: 1,
			laterUnschedulingDurations:     []time.Duration{10 * time.Minute},
		},
		"Pod is scheduled too late -  Marked later as pending by scheduler": {
			autoscalerStart:                defaultAutoscalerStart,
			createdAt:                      now.Add(-longUnschedulableThreshold),
			processedAt:                    now.Add(1 * time.Minute),
			scheduledAt:                    now.Add(longUnschedulableThreshold),
			processedLongUnschedulable:     1,
			laterSchedulingDurationsLength: 1,
			laterUnschedulingDurations:     []time.Duration{2 * longUnschedulableThreshold},
		},
		"Pod is scheduled too late - Not marked as pending": {
			// Delay is just above `longUnschedulableThreshold` so that it's marked as long unschedulable pod.
			// Due to the condition checking for greater than only and not equality.
			createdAt:                      now.Add(-longUnschedulableThreshold - 5*time.Minute),
			scheduledAt:                    now.Add(longUnschedulableThreshold),
			autoscalerStart:                defaultAutoscalerStart,
			initialLongUnschedulable:       1,
			laterSchedulingDurationsLength: 1,
			laterUnschedulingDurations:     []time.Duration{2*longUnschedulableThreshold + 5*time.Minute},
		},
		"Pod is unschedulable before Autoscaler starts": {
			createdAt:                      now.Add(-longUnschedulableThreshold),
			scheduledAt:                    now.Add(longUnschedulableThreshold),
			autoscalerStart:                now,
			laterSchedulingDurationsLength: 1,
			// Generally it should be 2xlongUnschedulableThreshold, but since autoscaler starts at `now`.
			// The time before that is ignored.
			laterUnschedulingDurations: []time.Duration{longUnschedulableThreshold},
		},
	} {
		t.Run(name, func(t *testing.T) {
			pod := makePendingPod("p1", now)
			pod.Status.Conditions = []apiv1.PodCondition{}
			pod.CreationTimestamp = metav1.NewTime(tc.createdAt)

			aggregator, observer, processor, _ := setUpProcessor(tc.autoscalerStart)
			aggregator.Unschedulable = []*apiv1.Pod{pod}
			scaleUpStatus := &status.ScaleUpStatus{
				Result: status.ScaleUpNotTried,
			}
			context := setUpContext([]*apiv1.Pod{})
			processor.processImpl(context, scaleUpStatus, now)
			assert.Equal(t, tc.initialLongUnschedulable, observer.longUnschedulable)
			assert.Equal(t, tc.initialSchedulingDurationsLength, len(observer.schedulingDurations))
			assert.Equal(t, tc.initialUnschedulingDurations, observer.unschedulableDurations)

			// Pod gets processed by scheduler
			if !tc.processedAt.IsZero() {
				pod.Status.Conditions = append(pod.Status.Conditions, apiv1.PodCondition{
					Type:               apiv1.PodScheduled,
					Status:             apiv1.ConditionFalse,
					Reason:             apiv1.PodReasonUnschedulable,
					LastTransitionTime: metav1.NewTime(tc.processedAt),
				})
				processor.processImpl(context, scaleUpStatus, tc.processedAt)
				assert.Equal(t, tc.processedLongUnschedulable, observer.longUnschedulable)
				assert.Equal(t, tc.processedSchedulingDurationsLength, len(observer.schedulingDurations))
				assert.Equal(t, tc.processedUnschedulingDurations, observer.unschedulableDurations)
			}

			// Pod got scheduled later
			aggregator.Unschedulable = []*apiv1.Pod{}
			schedulePod(pod, tc.scheduledAt)
			context = setUpContext([]*apiv1.Pod{pod})
			processor.processImpl(context, scaleUpStatus, tc.scheduledAt)
			assert.Equal(t, tc.laterLongUnschedulable, observer.longUnschedulable)
			assert.Equal(t, tc.laterSchedulingDurationsLength, len(observer.schedulingDurations))
			assert.Equal(t, tc.laterUnschedulingDurations, observer.unschedulableDurations)
		})
	}
}

func TestGPUPodMetric(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	gpuPod := makePendingPod("p1", now.Add(-5*time.Minute))
	one := resource.NewQuantity(1, resource.BinarySI)
	gpuPod.Spec.Containers = append(gpuPod.Spec.Containers, apiv1.Container{
		Resources: apiv1.ResourceRequirements{
			Requests: apiv1.ResourceList{
				gpu.ResourceNvidiaGPU: *one,
			},
		},
	})

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{gpuPod}

	scaleUpStatus := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{gpuPod},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	context := setUpContext([]*apiv1.Pod{})

	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	later := now.Add(2 * time.Minute)
	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}
	context = setUpContext([]*apiv1.Pod{gpuPod})
	processor.processImpl(context, newStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 1, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	assert.Equal(t, []map[string]string{{
		outOfResourceMetricLabel:          "false",
		gpuMetricLabel:                    "true",
		placementMetricLabel:              "TYPE_UNSPECIFIED",
		consumingProvisioningRequestLabel: "",
		tpuMetricLabel:                    "false",
		deviceAllocationModeMetricLabel:   podutils.AllocationModeExtendedResources.String(),
	}}, observer.unSchedDurationLabels)
}

func TestLongUnschedulableWithFilterableIssues(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("p1", now.Add(-5*time.Minute))

	aggregator, observer, processor, filter := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{pod},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	context := setUpContext([]*apiv1.Pod{})

	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	filter.ObserveNodeGroupFilterableIssue("ng1")

	later := now.Add(20 * time.Minute)
	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}
	processor.processImpl(context, newStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))
}

func TestLongUnschedulableWithClusterScaledToZero(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("p1", now.Add(-5*time.Minute))

	aggregator, observer, processor, metricsFilter := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{pod},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	ctx := setUpContext([]*apiv1.Pod{})

	processor.processImpl(ctx, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	later := now.Add(2 * longUnschedulableThreshold)
	evenLater := later.Add(2 * longUnschedulableThreshold)
	newStatus := &status.ScaleUpStatus{Result: status.ScaleUpNotTried}

	// If the cluster is scaled to zero we do not report long unschedulable.
	metricsFilter.ObserveScaleToZero(nil, nil, nil, true)
	processor.processImpl(ctx, newStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// Pods are not long unschedulable if the cluster has just stopped
	// being scaled to zero.
	metricsFilter.ObserveScaleToZero(nil, nil, nil, false)
	processor.processImpl(ctx, newStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// Pods are long usnchedulable if the cluster has stooped
	// being scaled to zero long ago.
	processor.processImpl(ctx, newStatus, evenLater)
	assert.Equal(t, 1, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))
}

func TestPodStockoutIssuesAllNodeGroupsStockout(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("p1", now.Add(-5*time.Minute))

	aggregator, observer, processor, filter := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{pod},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng3", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	context := setUpContext([]*apiv1.Pod{})

	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	filter.ObserveNodeGroupStockOut("ng1")
	filter.ObserveNodeGroupStockOut("ng2")
	filter.ObserveNodeGroupStockOut("ng3")

	later := now.Add(2 * time.Minute)
	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}
	context = setUpContext([]*apiv1.Pod{pod})
	processor.processImpl(context, newStatus, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 1, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	assert.Equal(t, []map[string]string{{
		outOfResourceMetricLabel:          "true",
		gpuMetricLabel:                    "false",
		placementMetricLabel:              "TYPE_UNSPECIFIED",
		consumingProvisioningRequestLabel: "",
		tpuMetricLabel:                    "false",
		deviceAllocationModeMetricLabel:   podutils.AllocationModeNone.String(),
	}}, observer.unSchedDurationLabels)
}

func TestLongUnschedulableWithDSPods(t *testing.T) {
	tcs := []struct {
		desc, ownerKind       string
		expectedPodViolations int
	}{
		{
			desc:                  "DaemonSet pod is filtered out",
			ownerKind:             "DaemonSet",
			expectedPodViolations: 0,
		},
		{
			desc:                  "StatefulSet pod is not filtered out",
			ownerKind:             "StatefulSet",
			expectedPodViolations: 1,
		},
	}
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			pod := makePendingPod("p1", now.Add(-longUnschedulableThreshold-1))
			pod.OwnerReferences = []metav1.OwnerReference{{Kind: tc.ownerKind}}
			aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
			aggregator.Unschedulable = []*apiv1.Pod{pod}

			scaleUpStatus := &status.ScaleUpStatus{
				Result:               status.ScaleUpSuccessful,
				PodsTriggeredScaleUp: []*apiv1.Pod{},
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
					},
					{
						Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
					},
					{
						Group: test.NewTestNodeGroup("ng3", 0, 10, 3, true, false, "a", nil, nil),
					},
				},
			}
			context := setUpContext([]*apiv1.Pod{})

			processor.processImpl(context, scaleUpStatus, now)
			assert.Equal(t, tc.expectedPodViolations, observer.longUnschedulable)
			assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
			assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
			assert.Equal(t, 0, len(observer.unschedulableDurations))

			later := now.Add(2 * time.Minute)
			newStatus := &status.ScaleUpStatus{
				Result: status.ScaleUpNotTried,
			}
			context = setUpContext([]*apiv1.Pod{pod})
			processor.processImpl(context, newStatus, later)
			assert.Equal(t, 0, observer.longUnschedulable)
			assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
			assert.Equal(t, tc.expectedPodViolations, len(observer.schedulingDurations["false"]))
			assert.Equal(t, tc.expectedPodViolations, len(observer.unschedulableDurations))
		})
	}
}

func TestLongUnschedulableWithDifferentSchedulers(t *testing.T) {

	tcs := []struct {
		desc, scheduler       string
		expectedPodViolations int
	}{
		{
			desc:                  "Unknown scheduler shouldn't be accounted for",
			scheduler:             "unknown",
			expectedPodViolations: 0,
		},
		{
			desc:                  "empty scheduler should be accounted for",
			scheduler:             "",
			expectedPodViolations: 1,
		},
		{
			desc:                  "default-scheduler scheduler should be accounted for",
			scheduler:             "default-scheduler",
			expectedPodViolations: 1,
		},
		{
			desc:                  "gke.io/default-scheduler scheduler should be accounted for",
			scheduler:             "gke.io/default-scheduler",
			expectedPodViolations: 1,
		},
		{
			desc:                  "gke.io/optimize-utilization-scheduler scheduler should be accounted for",
			scheduler:             "gke.io/optimize-utilization-scheduler",
			expectedPodViolations: 1,
		},
	}
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)

	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			pod := makePendingPod("p1", now.Add(-longUnschedulableThreshold-1))
			pod.Spec.SchedulerName = tc.scheduler
			aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
			aggregator.Unschedulable = []*apiv1.Pod{pod}

			scaleUpStatus := &status.ScaleUpStatus{
				Result:               status.ScaleUpSuccessful,
				PodsTriggeredScaleUp: []*apiv1.Pod{pod},
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
					},
					{
						Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
					},
					{
						Group: test.NewTestNodeGroup("ng3", 0, 10, 3, true, false, "a", nil, nil),
					},
				},
			}
			context := setUpContext([]*apiv1.Pod{})

			processor.processImpl(context, scaleUpStatus, now)
			assert.Equal(t, tc.expectedPodViolations, observer.longUnschedulable)
			assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
			assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
			assert.Equal(t, 0, len(observer.unschedulableDurations))

			later := now.Add(2 * time.Minute)
			newStatus := &status.ScaleUpStatus{
				Result: status.ScaleUpNotTried,
			}
			context = setUpContext([]*apiv1.Pod{pod})
			processor.processImpl(context, newStatus, later)
			assert.Equal(t, 0, observer.longUnschedulable)
			assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
			assert.Equal(t, tc.expectedPodViolations, len(observer.schedulingDurations["false"]))
			assert.Equal(t, tc.expectedPodViolations, len(observer.unschedulableDurations))
		})
	}
}

func TestLongUnschedulablePodStockoutIssuesAllNodeGroupsStockout(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("p1", now.Add(-5*time.Minute))

	aggregator, observer, processor, filter := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{pod},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng3", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	context := setUpContext([]*apiv1.Pod{})

	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	filter.ObserveNodeGroupStockOut("ng1")
	filter.ObserveNodeGroupStockOut("ng2")
	filter.ObserveNodeGroupStockOut("ng3")

	later := now.Add(70 * time.Minute)
	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}

	processor.processImpl(context, newStatus, later)
	assert.Equal(t, 1, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))
	assert.Equal(t, []map[string]string{{
		outOfResourceMetricLabel:          "true",
		gpuMetricLabel:                    "false",
		placementMetricLabel:              "TYPE_UNSPECIFIED",
		consumingProvisioningRequestLabel: "",
		tpuMetricLabel:                    "false",
		deviceAllocationModeMetricLabel:   podutils.AllocationModeNone.String(),
	}}, observer.longUnSchedLabels)
}

func TestMultipleNodeGroupsScaleUpWithStockout(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod1 := makePendingPod("p1", now.Add(-5*time.Minute))
	pod2 := makePendingPod("p2", now.Add(-5*time.Minute))

	aggregator, observer, processor, filter := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod1, pod2}

	scaleUpStatus1 := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{pod1},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
			},
			{
				Group: test.NewTestNodeGroup("ng2", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	scaleUpStatus2 := &status.ScaleUpStatus{
		Result:               status.ScaleUpSuccessful,
		PodsTriggeredScaleUp: []*apiv1.Pod{pod1, pod2},
		ScaleUpInfos: []nodegroupset.ScaleUpInfo{
			{
				Group: test.NewTestNodeGroup("ng3", 0, 10, 3, true, false, "a", nil, nil),
			},
		},
	}
	context := setUpContext([]*apiv1.Pod{})

	processor.processImpl(context, scaleUpStatus1, now)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	later := now.Add(10 * time.Minute)

	processor.processImpl(context, scaleUpStatus2, later)
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 0, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	filter.ObserveNodeGroupStockOut("ng1")
	filter.ObserveNodeGroupStockOut("ng2")

	newStatus := &status.ScaleUpStatus{
		Result: status.ScaleUpNotTried,
	}

	aggregator.Unschedulable = []*apiv1.Pod{pod2}
	context = setUpContext([]*apiv1.Pod{pod1})

	processor.processImpl(context, newStatus, later.Add(time.Minute))
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 1, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 1, len(observer.unschedulableDurations))
	assert.Equal(t, []map[string]string{{
		outOfResourceMetricLabel:          "true",
		gpuMetricLabel:                    "false",
		placementMetricLabel:              "TYPE_UNSPECIFIED",
		consumingProvisioningRequestLabel: "",
		tpuMetricLabel:                    "false",
		deviceAllocationModeMetricLabel:   podutils.AllocationModeNone.String(),
	}}, observer.unSchedDurationLabels)

	// Ng3 now has a stockout and then gets scheduled, causing new metrics for p2
	context = setUpContext([]*apiv1.Pod{pod1, pod2})
	aggregator.Unschedulable = []*apiv1.Pod{}
	filter.ObserveNodeGroupStockOut("ng3")
	processor.processImpl(context, newStatus, later.Add(2*time.Minute))
	assert.Equal(t, 0, observer.longUnschedulable)
	assert.Equal(t, 2, len(observer.schedulingDurations["true"]))
	assert.Equal(t, 0, len(observer.schedulingDurations["false"]))
	assert.Equal(t, 2, len(observer.unschedulableDurations))
	assert.Equal(t, []map[string]string{
		{
			outOfResourceMetricLabel:          "true",
			gpuMetricLabel:                    "false",
			placementMetricLabel:              "TYPE_UNSPECIFIED",
			consumingProvisioningRequestLabel: "",
			tpuMetricLabel:                    "false",
			deviceAllocationModeMetricLabel:   podutils.AllocationModeNone.String(),
		},
		{
			outOfResourceMetricLabel:          "true",
			gpuMetricLabel:                    "false",
			placementMetricLabel:              "TYPE_UNSPECIFIED",
			consumingProvisioningRequestLabel: "",
			tpuMetricLabel:                    "false",
			deviceAllocationModeMetricLabel:   podutils.AllocationModeNone.String(),
		},
	}, observer.unSchedDurationLabels)
}

func TestLabelCounter(t *testing.T) {
	c := labelCounter{}
	c.increment(map[string]string{"a": "1"})
	c.increment(map[string]string{"a": "1", "b": "1"})
	c.increment(map[string]string{"a": "1", "b": "2"})
	c.increment(map[string]string{"a": "1", "b": "1"})
	c.increment(map[string]string{"a": "1"})

	var called int
	f := func(l labelAndCount) {
		called++
		m := (map[string]string)(l.labels)
		if reflect.DeepEqual(m, map[string]string{"a": "1", "b": "1"}) {
			assert.Equal(t, 2, l.count)
		} else if reflect.DeepEqual(m, map[string]string{"a": "1", "b": "2"}) {
			assert.Equal(t, 1, l.count)
		} else if reflect.DeepEqual(m, map[string]string{"a": "1"}) {
			assert.Equal(t, 2, l.count)
		} else {
			assert.NoError(t, errors.New("expected condition"))
		}
	}

	c.process(f)
	assert.Equal(t, 3, called)
}

func TestPodSchedulingDurationPerAllocationMode(t *testing.T) {
	for name, tc := range map[string]struct {
		setupPod       func(*apiv1.Pod)
		allocationMode podutils.DeviceAllocationMode
	}{
		"None": {
			setupPod:       func(p *apiv1.Pod) {},
			allocationMode: podutils.AllocationModeNone,
		},
		"ExtendedResources": {
			setupPod: func(p *apiv1.Pod) {
				p.Spec.Containers = []apiv1.Container{
					{
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								"nvidia.com/gpu": resource.MustParse("1"),
							},
						},
					},
				}
			},
			allocationMode: podutils.AllocationModeExtendedResources,
		},
		"DRA": {
			setupPod: func(p *apiv1.Pod) {
				p.Spec.ResourceClaims = []apiv1.PodResourceClaim{
					{Name: "test-claim"},
				}
			},
			allocationMode: podutils.AllocationModeDra,
		},
		"ExtendedResourcesDra": {
			setupPod: func(p *apiv1.Pod) {
				p.Spec.Containers = []apiv1.Container{
					{
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								"test.com/resource": resource.MustParse("1"),
							},
						},
					},
				}
				p.Status.ExtendedResourceClaimStatus = &apiv1.PodExtendedResourceClaimStatus{
					ResourceClaimName: "test-resource-claim",
					RequestMappings: []apiv1.ContainerExtendedResourceRequest{
						{
							ContainerName: "test-container",
							ResourceName:  "test.com/resource",
							RequestName:   "req-0",
						},
					},
				}
			},
			allocationMode: podutils.AllocationModeExtendedResourcesDra,
		},
		"Mixed": {
			setupPod: func(p *apiv1.Pod) {
				p.Spec.ResourceClaims = []apiv1.PodResourceClaim{
					{Name: "test-claim-2"},
				}
				p.Spec.Containers = []apiv1.Container{
					{
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								"nvidia.com/gpu": resource.MustParse("1"),
							},
						},
					},
				}
			},
			allocationMode: podutils.AllocationModeMixed,
		},
	} {
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
			pod := makePendingPod("p1", now.Add(-5*time.Minute))
			tc.setupPod(pod)

			aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
			aggregator.Unschedulable = []*apiv1.Pod{pod}

			scaleUpStatus := &status.ScaleUpStatus{
				Result:               status.ScaleUpSuccessful,
				PodsTriggeredScaleUp: []*apiv1.Pod{pod},
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
					},
				},
			}
			context := setUpContext([]*apiv1.Pod{})

			// First process call handles unschedulable pod
			processor.processImpl(context, scaleUpStatus, now)
			assert.Equal(t, 0, len(observer.schedulingDurationsPerAllocationMode))

			// Schedule the pod and process again
			later := now.Add(2 * time.Minute)
			newStatus := &status.ScaleUpStatus{
				Result: status.ScaleUpNotTried,
			}
			schedulePod(pod, later)
			context = setUpContext([]*apiv1.Pod{pod})

			processor.processImpl(context, newStatus, later)

			assert.Equal(t, 1, len(observer.schedulingDurationsPerAllocationMode))
			assert.Equal(t, 1, len(observer.schedulingDurationsPerAllocationMode[tc.allocationMode.String()]))
		})
	}
}

func TestPodScheduled_CCC(t *testing.T) {
	for name, tc := range map[string]struct {
		returnedCCC string
		expectError bool
	}{
		"Propagates CCC name from lister": {
			returnedCCC: "my-custom-ccc",
		},
		"Propagates empty string if lister returns no CCC": {
			returnedCCC: "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
			pod := makePendingPod("p1", now.Add(-5*time.Minute))

			aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
			aggregator.Unschedulable = []*apiv1.Pod{pod}

			processor.npcCrdLister = &mockNpcCrdLister{crdName: tc.returnedCCC}

			scaleUpStatus := &status.ScaleUpStatus{
				Result:               status.ScaleUpSuccessful,
				PodsTriggeredScaleUp: []*apiv1.Pod{pod},
				ScaleUpInfos: []nodegroupset.ScaleUpInfo{
					{
						Group: test.NewTestNodeGroup("ng1", 0, 10, 3, true, false, "a", nil, nil),
					},
				},
			}
			context := setUpContext([]*apiv1.Pod{})

			processor.processImpl(context, scaleUpStatus, now)

			later := now.Add(2 * time.Minute)
			newStatus := &status.ScaleUpStatus{
				Result: status.ScaleUpNotTried,
			}
			schedulePod(pod, later)
			context = setUpContext([]*apiv1.Pod{pod})

			processor.processImpl(context, newStatus, later)

			assert.Equal(t, 1, len(observer.schedulingDurationsPerCcc))
			assert.Equal(t, 1, len(observer.schedulingDurationsPerCcc[tc.returnedCCC]))
		})
	}
}

func TestPodScheduled_NegativeDurationCappedAtZero(t *testing.T) {
	now := time.Now()
	autoscalerStart := now.Add(-10 * longUnschedulableThreshold)
	pod := makePendingPod("RacePod", now.Add(-5*time.Minute))

	aggregator, observer, processor, _ := setUpProcessor(autoscalerStart)
	aggregator.Unschedulable = []*apiv1.Pod{pod}

	scaleUpStatus := &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded}
	context := setUpContext([]*apiv1.Pod{})

	// 1. Pod is spotted as pending
	processor.processImpl(context, scaleUpStatus, now)
	assert.Equal(t, 0, len(observer.unschedulableDurations))

	// 2. Pod is scheduled in reality in the past (e.g., before unschedulableSince)
	// to trigger a negative schedulingDuration
	scheduleTime := now.Add(-10 * time.Minute)
	schedulePod(pod, scheduleTime)

	// 3. CA simulation still thinks it is unschedulable at t+3 (stale snapshot)
	// and puts it in PodsRemainUnschedulable.
	later := now.Add(3 * time.Minute)
	scaleUpStatusUnhelpable := &status.ScaleUpStatus{
		Result: status.ScaleUpNotNeeded,
		PodsRemainUnschedulable: []status.NoScaleUpInfo{
			{Pod: pod},
		},
	}

	// But context lister returns it as scheduled!
	context = setUpContext([]*apiv1.Pod{pod})

	aggregator.Unschedulable = []*apiv1.Pod{} // It's scheduled, so not in unschedulable aggregator anymore.

	processor.processImpl(context, scaleUpStatusUnhelpable, later)

	// Verify that both durations are capped at 0 instead of being negative.
	assert.Len(t, observer.unschedulableDurations, 1)
	assert.Equal(t, 0*time.Second, observer.unschedulableDurations[0])

	assert.Len(t, observer.schedulingDurations["false"], 1)
	assert.Equal(t, 0*time.Second, observer.schedulingDurations["false"][0])
}
