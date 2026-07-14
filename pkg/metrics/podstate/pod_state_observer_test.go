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

package podstate

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	cb "k8s.io/autoscaler/cluster-autoscaler/processors/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	fakepods "k8s.io/autoscaler/cluster-autoscaler/simulator/fake"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	npc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/ccc"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	podv1 "k8s.io/kubernetes/pkg/api/v1/pod"
	clock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	podstatetypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/podstate/types"
)

type testReactionMetric struct {
	duration       time.Duration
	systemPod      bool
	hasPVC         bool
	hasCSI         bool
	reactionType   metrics.ReactionType
	allocationType string
}

type testReactionTimeMetrics struct {
	calls []testReactionMetric
}

func (m *testReactionTimeMetrics) ObserveFirstReactionTime(duration time.Duration, systemPod bool, hasPVC bool, hasCSI bool, reactionType metrics.ReactionType, allocationType string) {
	m.calls = append(m.calls, testReactionMetric{
		duration:       duration,
		systemPod:      systemPod,
		hasPVC:         hasPVC,
		hasCSI:         hasCSI,
		reactionType:   reactionType,
		allocationType: allocationType,
	})
}

type eventType int

const (
	newPodEvent             eventType = iota
	scaleUpEvent            eventType = iota
	updatePodEvent          eventType = iota
	deletePodEvent          eventType = iota
	unhelpablePodEvent      eventType = iota
	timeoutEvent            eventType = iota
	cleanUpEvent            eventType = iota
	classifiedAsSchedulable eventType = iota
)

type event struct {
	t       time.Time
	et      eventType
	pod     *v1.Pod
	scaleUp *status.ScaleUpStatus
}

func TestPodStateObserver(t *testing.T) {
	// Temporarily override the metric registration function to prevent duplicate registration in tests.
	oldRegisterFunc := RegisterPendingPodsCollectorFunc
	RegisterPendingPodsCollectorFunc = func(collectFunc PendingPodsCalculationFunc) {}
	t.Cleanup(func() {
		RegisterPendingPodsCollectorFunc = oldRegisterFunc
	})
	var st = time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	// Note: pu - pod unschedulable, ps - pod scheduled, p - neither scheduled, nor unschedulable
	var p = test.BuildTestPod("p1", 100, 1)
	var p2 = test.BuildTestPod("p2", 200, 2)
	var pu = test.BuildTestPod("p1", 100, 1, markPodUnschedulableWithTime(st))
	var pu5sec = test.BuildTestPod("p1", 100, 1, markPodUnschedulableWithTime(st.Add(5*time.Second)))
	var pu2 = test.BuildTestPod("p2", 200, 2, markPodUnschedulableWithTime(st))
	var pu5sec2 = test.BuildTestPod("p2", 200, 2, markPodUnschedulableWithTime(st.Add(5*time.Second)))
	var pu3 = test.BuildTestPod("p3", 200, 2, markPodUnschedulableWithTime(st), func(p *v1.Pod) { p.Namespace = metav1.NamespaceSystem })
	var ps = test.BuildScheduledTestPod("p1", 100, 1, "some node")
	var ps2 = test.BuildScheduledTestPod("p2", 200, 2, "some node")
	var ps3 = test.BuildTestPod("p3", 200, 2, func(p *v1.Pod) {
		p.Namespace = metav1.NamespaceSystem
		p.Spec.NodeName = "some node"
	})
	var puBeforeCaStart = test.BuildTestPod("p1", 100, 1, markPodUnschedulableWithTime(st.Add(-time.Second*20)))
	var pvu1 = test.BuildTestPod("pv1", 200, 2, markPodUnschedulableWithTime(st), func(p *v1.Pod) {
		p.Spec.Volumes = append(p.Spec.Volumes, BuildPVCVolume())
	})
	var pvu2 = test.BuildTestPod("pv2", 200, 2, markPodUnschedulableWithTime(st), func(p *v1.Pod) {
		p.Spec.Volumes = append(p.Spec.Volumes, BuildCSIVolume())
	})
	var pvu3 = test.BuildTestPod("pv3", 200, 2, markPodUnschedulableWithTime(st), func(p *v1.Pod) {
		p.Spec.Volumes = append(p.Spec.Volumes, BuildPVCVolume())
		p.Spec.Volumes = append(p.Spec.Volumes, BuildCSIVolume())
	})
	var pvs1 = test.BuildTestPod("pv1", 200, 2, func(p *v1.Pod) {
		p.Spec.Volumes = append(p.Spec.Volumes, BuildPVCVolume())
		p.Spec.NodeName = "some node"
	})
	var pvs2 = test.BuildTestPod("pv2", 200, 2, func(p *v1.Pod) {
		p.Spec.Volumes = append(p.Spec.Volumes, BuildCSIVolume())
		p.Spec.NodeName = "some node"
	})
	var pvs3 = test.BuildTestPod("pv3", 200, 2, func(p *v1.Pod) {
		p.Spec.Volumes = append(p.Spec.Volumes, BuildPVCVolume())
		p.Spec.Volumes = append(p.Spec.Volumes, BuildCSIVolume())
		p.Spec.NodeName = "some node"
	})
	var capacityBuffersFakePod = buildPodWithAnnotations("capacityBuffersFakePod", map[string]string{cb.CapacityBufferFakePodAnnotationKey: cb.CapacityBufferFakePodAnnotationValue})
	var proactiveScaleupFakePod = buildPodWithAnnotations("proactiveScaleupFakePod", map[string]string{fakepods.FakePodAnnotationKey: fakepods.FakePodAnnotationValue})
	var pds = test.BuildTestPod("pds", 100, 1, markPodUnschedulableWithTime(st), func(p *v1.Pod) {
		p.OwnerReferences = []metav1.OwnerReference{
			{
				Kind:       "DaemonSet",
				Name:       "ds",
				Controller: ptr.To(true),
			},
		}
	})
	var lowPriorityPod = test.BuildTestPod("lowPriorityPod", 100, 1, markPodUnschedulableWithTime(st), func(p *v1.Pod) {
		p.Spec.Priority = ptr.To(-int32(11))
	})
	var unSchedDraPod = test.BuildTestPod("draPod", 100, 1, markPodUnschedulableWithTime(st), func(p *v1.Pod) {
		p.Spec.ResourceClaims = []v1.PodResourceClaim{
			{
				Name:              "dra-claim",
				ResourceClaimName: ptr.To("dra-claim"),
			},
		}
	})
	var schedDraPod = test.BuildTestPod("draPod", 100, 1, func(p *v1.Pod) {
		p.Spec.ResourceClaims = []v1.PodResourceClaim{
			{
				Name:              "dra-claim",
				ResourceClaimName: ptr.To("dra-claim"),
			},
		}
		p.Spec.NodeName = "node"
	})
	var unSchedExtendedResourcesPod = test.BuildTestPod("extendedResourcesPod", 100, 1, markPodUnschedulableWithTime(st), func(p *v1.Pod) {
		p.Spec.Containers[0].Resources.Requests["resource.com/gpu"] = *resource.NewQuantity(1, resource.DecimalSI)
	})
	var schedExtendedResourcesPod = test.BuildTestPod("extendedResourcesPod", 100, 1, func(p *v1.Pod) {
		p.Spec.Containers[0].Resources.Requests["resource.com/gpu"] = *resource.NewQuantity(1, resource.DecimalSI)
		p.Spec.NodeName = "node"
	})
	var unSchedExtendedResourcesDraPod = test.BuildTestPod("extendedResourcesDraPod", 100, 1, markPodUnschedulableWithTime(st), func(p *v1.Pod) {
		p.Spec.Containers[0].Resources.Requests["resource.com/gpu"] = *resource.NewQuantity(1, resource.DecimalSI)
		p.Status.ExtendedResourceClaimStatus = &v1.PodExtendedResourceClaimStatus{
			ResourceClaimName: "dra-claim",
			RequestMappings: []v1.ContainerExtendedResourceRequest{
				{
					ContainerName: "container1",
					ResourceName:  "resource.com/gpu",
					RequestName:   "req-0",
				},
			},
		}
	})
	var schedExtendedResourcesDraPod = test.BuildTestPod("extendedResourcesDraPod", 100, 1, func(p *v1.Pod) {
		p.Spec.Containers[0].Resources.Requests["resource.com/gpu"] = *resource.NewQuantity(1, resource.DecimalSI)
		p.Status.ExtendedResourceClaimStatus = &v1.PodExtendedResourceClaimStatus{
			ResourceClaimName: "dra-claim",
			RequestMappings: []v1.ContainerExtendedResourceRequest{
				{
					ContainerName: "container1",
					ResourceName:  "resource.com/gpu",
					RequestName:   "req-0",
				},
			},
		}
		p.Spec.NodeName = "node"
	})
	var unSchedMixedAllocationModePod = test.BuildTestPod("mixedAllocationModePod", 100, 1, markPodUnschedulableWithTime(st), func(p *v1.Pod) {
		p.Spec.Containers[0].Resources.Requests["resource.com/gpu"] = *resource.NewQuantity(1, resource.DecimalSI)
		p.Spec.ResourceClaims = append(p.Spec.ResourceClaims, v1.PodResourceClaim{
			Name:              "dra-claim",
			ResourceClaimName: ptr.To("dra-claim"),
		})
	})
	var schedMixedAllocationModePod = test.BuildTestPod("mixedAllocationModePod", 100, 1, func(p *v1.Pod) {
		p.Spec.Containers[0].Resources.Requests["resource.com/gpu"] = *resource.NewQuantity(1, resource.DecimalSI)
		p.Spec.ResourceClaims = append(p.Spec.ResourceClaims, v1.PodResourceClaim{
			Name:              "dra-claim",
			ResourceClaimName: ptr.To("dra-claim"),
		})
		p.Spec.NodeName = "node"
	})

	const periodicCheckFrequency = time.Minute * 90
	testCases := []struct {
		desc                      string
		events                    []event
		observedReactionType      metrics.ReactionType
		observedSystemPod         bool
		observedPVC               bool
		observedCSI               bool
		observedReactionTimes     []time.Duration
		observedAllocationType    string
		expectedFinalReactionType map[types.UID]metrics.ReactionType
	}{
		{
			desc: "Ignores DaemonSet pod",
			events: []event{
				{t: st, et: newPodEvent, pod: pds, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pds}}},
			},
			observedReactionType: metrics.NoReaction,
		},
		{
			desc: "Ignores expendable pod",
			events: []event{
				{t: st, et: newPodEvent, pod: lowPriorityPod, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{lowPriorityPod}}},
			},
			observedReactionType: metrics.NoReaction,
		},
		{
			desc: "Reports scale up",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu}}},
			},
			observedReactionType:   metrics.ScaleUp,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 15},
		},
		{
			desc: "Reports scale up for system pod",
			events: []event{
				{t: st, et: newPodEvent, pod: pu3, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{ps3}}},
			},
			observedReactionType:   metrics.ScaleUp,
			observedSystemPod:      true,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 15},
		},
		{
			desc: "If pod was created before CA restart, it uses CA start time",
			events: []event{
				{t: st.Add(-time.Second * 20), et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu}}},
			},
			observedReactionType:   metrics.ScaleUp,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 15},
		},
		{
			desc: "Reports scale up for many pods",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st, et: newPodEvent, pod: p2, scaleUp: nil},
				{t: st.Add(time.Second * 5), et: updatePodEvent, pod: pu5sec2, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu, pu5sec2}}},
			},
			observedReactionType:   metrics.ScaleUp,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 15, time.Second * 10},
		},
		{
			desc: "Reports scale up for pods and excludes fake pods",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st, et: newPodEvent, pod: proactiveScaleupFakePod, scaleUp: nil},
				{t: st, et: newPodEvent, pod: capacityBuffersFakePod, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu, proactiveScaleupFakePod, capacityBuffersFakePod}}},
			},
			observedReactionType:   metrics.ScaleUp,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 15},
		},
		{
			desc: "Reports only first scale up when there are many",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 10), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu}}},
				{t: st.Add(time.Second * 20), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu}}},
			},
			observedReactionType:   metrics.ScaleUp,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 10},
		},
		{
			desc: "Reports scheduled pod",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: ps, scaleUp: nil},
			},
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20},
		},
		{
			desc: "Reports scheduled pod with PVC Volume",
			events: []event{
				{t: st, et: newPodEvent, pod: pvu1, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: pvs1, scaleUp: nil},
			},
			observedPVC:            true,
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20},
		},
		{
			desc: "Reports scheduled pod with CSI Volume",
			events: []event{
				{t: st, et: newPodEvent, pod: pvu2, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: pvs2, scaleUp: nil},
			},
			observedCSI:            true,
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20},
		},
		{
			desc: "Reports scheduled pod with PVC and CSI Volumes",
			events: []event{
				{t: st, et: newPodEvent, pod: pvu3, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: pvs3, scaleUp: nil},
			},
			observedPVC:            true,
			observedCSI:            true,
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20},
		},
		{
			desc: "Reports scheduled many pod",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 5), et: newPodEvent, pod: pu5sec2, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: ps, scaleUp: nil},
				{t: st.Add(time.Second * 30), et: updatePodEvent, pod: ps2, scaleUp: nil},
			},
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20, time.Second * 25},
		},
		{
			desc: "Does not report scheduled after scale up",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 10), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu}}},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: ps, scaleUp: nil},
			},
			observedReactionType:   metrics.ScaleUp,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 10},
		},
		{
			desc: "Reports unhelpable pod",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 25), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pu, RejectedNodeGroups: nil, SkippedNodeGroups: nil}}}},
			},
			observedReactionType:   metrics.Unhelpable,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 25},
		},
		{
			desc: "Reports only first reaction",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 10), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pu, RejectedNodeGroups: nil, SkippedNodeGroups: nil}}}},
				{t: st.Add(time.Second * 20), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu}}},
			},
			observedReactionType:   metrics.Unhelpable,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 10},
		},
		{
			desc: "Reports deleted pod",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: deletePodEvent, pod: pu, scaleUp: nil},
			},
			observedReactionType:   metrics.Deleted,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20},
		},
		{
			desc: "Not report deleted scheduled pod",
			events: []event{
				{t: st, et: newPodEvent, pod: ps, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: deletePodEvent, pod: ps, scaleUp: nil},
			},
		},
		{
			desc: "Reports timeout pod",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(periodicCheckFrequency), et: timeoutEvent, pod: nil, scaleUp: nil},
			},
			observedReactionType:   metrics.Timeout,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{metrics.MaxReactionTime},
		},
		{
			desc: "Reports timeout pods during clean up",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Minute), et: newPodEvent, pod: pu2, scaleUp: nil},
				{t: st.Add(metrics.MaxReactionTime + time.Minute), et: cleanUpEvent, pod: nil, scaleUp: nil},
			},
			observedReactionType:   metrics.Timeout,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{metrics.MaxReactionTime, metrics.MaxReactionTime},
		},
		{
			desc: "Reports no action needed",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: classifiedAsSchedulable, pod: pu, scaleUp: nil},
			},
			observedReactionType:   metrics.NoActionNeeded,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 15},
		},
		{
			desc: "Reaction type is updated for reported pods",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu}}},
				{t: st.Add(time.Second * 30), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: pu}}}},
			},
			observedReactionType:   metrics.ScaleUp,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 15},
			expectedFinalReactionType: map[types.UID]metrics.ReactionType{
				pu.UID: metrics.Unhelpable,
			},
		},
		{
			desc: "Reaction time is 0 if pod was never marked as unschedulable",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{p}}},
			},
			observedReactionType:   metrics.ScaleUp,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{0},
		},
		{
			desc: "Reaction times are reported only starting from when pod is marked as unschedulable",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st.Add(time.Second * 5), et: updatePodEvent, pod: pu5sec, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: ps, scaleUp: nil},
			},
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 15},
		},
		{
			desc: "Do not set the unschedulableSince time if it was already set before",
			events: []event{
				{t: st, et: newPodEvent, pod: pu, scaleUp: nil},
				{t: st.Add(time.Second * 5), et: updatePodEvent, pod: pu5sec, scaleUp: nil},
				{t: st.Add(time.Second * 10), et: updatePodEvent, pod: ps, scaleUp: nil},
			},
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 10},
		},
		{
			desc: "If pod was created before CA restart, it uses CA start time",
			events: []event{
				{t: st, et: newPodEvent, pod: puBeforeCaStart, scaleUp: nil},
				{t: st.Add(time.Second * 15), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{pu}}},
			},
			observedReactionType:   metrics.ScaleUp,
			observedAllocationType: podutils.AllocationModeNone.String(),
			observedReactionTimes:  []time.Duration{time.Second * 15},
		},
		{
			desc: "Reports scheduled pod with extended resources",
			events: []event{
				{t: st, et: newPodEvent, pod: unSchedExtendedResourcesPod, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: schedExtendedResourcesPod, scaleUp: nil},
			},
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeExtendedResources.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20},
		},
		{
			desc: "Reports scheduled pod with DRA",
			events: []event{
				{t: st, et: newPodEvent, pod: unSchedDraPod, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: schedDraPod, scaleUp: nil},
			},
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeDra.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20},
		},
		{
			desc: "Reports scheduled pod with extended resources and DRA",
			events: []event{
				{t: st, et: newPodEvent, pod: unSchedExtendedResourcesDraPod, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: schedExtendedResourcesDraPod, scaleUp: nil},
			},
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeExtendedResourcesDra.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20},
		},
		{
			desc: "Reports scheduled pod with mixed allocation mode",
			events: []event{
				{t: st, et: newPodEvent, pod: unSchedMixedAllocationModePod, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: schedMixedAllocationModePod, scaleUp: nil},
			},
			observedReactionType:   metrics.Scheduled,
			observedAllocationType: podutils.AllocationModeMixed.String(),
			observedReactionTimes:  []time.Duration{time.Second * 20},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				informerClientSet := fake.NewSimpleClientset()
				informerFactory := informers.NewSharedInformerFactory(informerClientSet, 0)
				testMetrics := &testReactionTimeMetrics{}
				fakeClock := clock.NewFakeClock(st)
				podsClassifier := systempods.NewClassifier([]string{metav1.NamespaceSystem})
				psObserver, err := newPodStateObserver(informerFactory, testMetrics, podsClassifier, &mockNpcCrdLister{}, periodicCheckFrequency, fakeClock, false, false, -10)
				stopCh := make(chan struct{})
				defer close(stopCh)
				informerFactory.Start(stopCh)
				_ = informerFactory.WaitForCacheSync(stopCh)
				for _, e := range tc.events {
					fakeClock.SetTime(e.t)
					switch e.et {
					case newPodEvent:
						e.pod.CreationTimestamp.Time = fakeClock.Now()
						_, err = informerClientSet.CoreV1().Pods(e.pod.Namespace).Create(context.TODO(), e.pod, metav1.CreateOptions{})
					case updatePodEvent:
						_, err = informerClientSet.CoreV1().Pods(e.pod.Namespace).Update(context.TODO(), e.pod, metav1.UpdateOptions{})
					case deletePodEvent:
						err = informerClientSet.CoreV1().Pods(e.pod.Namespace).Delete(context.TODO(), e.pod.Name, metav1.DeleteOptions{})
					case scaleUpEvent:
						psObserver.Process(&ca_context.AutoscalingContext{}, e.scaleUp)
					case timeoutEvent:
						psObserver.reportTimeoutPods()
					case cleanUpEvent:
						psObserver.CleanUp()
					case classifiedAsSchedulable:
						psObserver.ObserveReaction([]*v1.Pod{e.pod}, metrics.NoActionNeeded)
					}
					assert.NoError(t, err)
					synctest.Wait()
				}
				if tc.observedReactionType == metrics.NoReaction {
					assert.Empty(t, testMetrics.calls)
				} else {
					expectedCalls := make([]testReactionMetric, 0, len(tc.observedReactionTimes))
					for _, observedReactionTime := range tc.observedReactionTimes {
						expectedCalls = append(expectedCalls, testReactionMetric{
							duration:       observedReactionTime,
							systemPod:      tc.observedSystemPod,
							hasPVC:         tc.observedPVC,
							hasCSI:         tc.observedCSI,
							reactionType:   tc.observedReactionType,
							allocationType: tc.observedAllocationType,
						})
					}
					assert.ElementsMatch(t, expectedCalls, testMetrics.calls)
				}
				if len(tc.expectedFinalReactionType) > 0 {
					for uid, expectedReactionType := range tc.expectedFinalReactionType {
						finalState, found := psObserver.reportedPodStates[uid]
						if !found {
							finalState, found = psObserver.unreportedPodStates[uid]
						}
						assert.True(t, found, "Pod state not found for UID %s", uid)
						assert.Equal(t, expectedReactionType, finalState.reactionType)
					}
				}
			})
		})
	}
}

func TestCalculatePendingPods(t *testing.T) {
	testCases := []struct {
		name                string
		unreportedPodStates map[types.UID]*podStateData
		reportedPodStates   map[types.UID]*podStateData
		want                []podstatetypes.PendingPodsMetric
	}{
		{
			name:                "no pending pods",
			unreportedPodStates: map[types.UID]*podStateData{},
			reportedPodStates:   map[types.UID]*podStateData{},
			want: []podstatetypes.PendingPodsMetric{
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0},
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0},
			},
		},
		{
			name: "one unprocessed non-system pod",
			unreportedPodStates: map[types.UID]*podStateData{
				"p1": {reactionType: metrics.NoReaction, systemPod: false},
			},
			reportedPodStates: map[types.UID]*podStateData{},
			want: []podstatetypes.PendingPodsMetric{
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0},
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 1},
				{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0},
			},
		},
		{
			name: "complex scenario",
			unreportedPodStates: map[types.UID]*podStateData{
				"p1":  {reactionType: metrics.NoReaction, systemPod: false},
				"p1s": {reactionType: metrics.NoReaction, systemPod: true},
			},
			reportedPodStates: map[types.UID]*podStateData{
				"p2":  {reactionType: metrics.ScaleUp, systemPod: false},
				"p3":  {reactionType: metrics.ScaleUp, systemPod: false},
				"p4":  {reactionType: metrics.Unhelpable, systemPod: false},
				"p5":  {reactionType: metrics.NoActionNeeded, systemPod: false},
				"p6":  {reactionType: metrics.Timeout, systemPod: false},
				"p7":  {reactionType: metrics.Scheduled, systemPod: false}, // Should be ignored.
				"p2s": {reactionType: metrics.EkUpsize, systemPod: true},
				"p3s": {reactionType: metrics.Unhelpable, systemPod: true},
				"p4s": {reactionType: metrics.Unhelpable, systemPod: true},
				"p5s": {reactionType: metrics.NoActionNeeded, systemPod: true},
				"p6s": {reactionType: metrics.Deleted, systemPod: true}, // Should be ignored.
			},
			want: []podstatetypes.PendingPodsMetric{
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 1},
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 2},
				{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 2},
				{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 1},
				{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 1},
				{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 2},
				{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 1},
				{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 1},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			observer := &PodStateObserver{
				npcCrdLister:        &mockNpcCrdLister{},
				unreportedPodStates: tc.unreportedPodStates,
				reportedPodStates:   tc.reportedPodStates,
			}
			result := observer.calculatePendingPods()
			assert.ElementsMatch(t, tc.want, result)
		})
	}
}

func TestCalculatePendingPodsLifecycle(t *testing.T) {
	// Temporarily override the metric registration function to prevent duplicate registration in tests.
	oldRegisterFunc := RegisterPendingPodsCollectorFunc
	RegisterPendingPodsCollectorFunc = func(collectFunc PendingPodsCalculationFunc) {}
	t.Cleanup(func() {
		RegisterPendingPodsCollectorFunc = oldRegisterFunc
	})
	var st = time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	var p = test.BuildTestPod("p1", 100, 1)
	var p2 = test.BuildTestPod("p2", 200, 2)
	var p3 = test.BuildTestPod("p3", 200, 2, func(p *v1.Pod) { p.Namespace = metav1.NamespaceSystem })
	var ps = test.BuildScheduledTestPod("p1", 100, 1, "some node")
	testCases := []struct {
		name   string
		events []event
		want   []podstatetypes.PendingPodsMetric
	}{
		{
			name:   "no pending pods",
			events: []event{},
			want: []podstatetypes.PendingPodsMetric{
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0},
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0},
			},
		},
		{
			name: "one unprocessed non-system pod",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsMetric{
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0},
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 1},
				{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0},
			},
		},
		{
			name: "pod gets scheduled",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: ps, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsMetric{
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0},
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0},
			},
		},
		{
			name: "pod gets deleted",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: deletePodEvent, pod: p, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsMetric{
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0},
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0},
			},
		},
		{
			name: "pod times out",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st.Add(metrics.MaxReactionTime + time.Second), et: timeoutEvent, pod: nil, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsMetric{
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0},
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 1},
				{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0},
			},
		},
		{
			name: "complex scenario",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st.Add(time.Second * 1), et: newPodEvent, pod: p2, scaleUp: nil},
				{t: st.Add(time.Second * 2), et: newPodEvent, pod: p3, scaleUp: nil}, // system pod
				{t: st.Add(time.Second * 10), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{p}}},
				{t: st.Add(time.Second * 11), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: p2}}}},
				{t: st.Add(time.Second * 12), et: classifiedAsSchedulable, pod: p3, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsMetric{
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0},
				{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 1},
				{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0},
				{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 1},
				{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0},
				{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0},
				{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 1},
				{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				informerClientSet := fake.NewSimpleClientset()
				informerFactory := informers.NewSharedInformerFactory(informerClientSet, 0)
				testMetrics := &testReactionTimeMetrics{}
				fakeClock := clock.NewFakeClock(st)
				podsClassifier := systempods.NewClassifier([]string{metav1.NamespaceSystem})
				observer, err := newPodStateObserver(informerFactory, testMetrics, podsClassifier, &mockNpcCrdLister{}, time.Minute, fakeClock, false, false, -10)
				assert.NoError(t, err)
				stopCh := make(chan struct{})
				defer close(stopCh)
				informerFactory.Start(stopCh)
				_ = informerFactory.WaitForCacheSync(stopCh)
				for _, e := range tc.events {
					fakeClock.SetTime(e.t)
					switch e.et {
					case newPodEvent:
						e.pod.CreationTimestamp.Time = fakeClock.Now()
						_, err = informerClientSet.CoreV1().Pods(e.pod.Namespace).Create(context.TODO(), e.pod, metav1.CreateOptions{})
					case updatePodEvent:
						_, err = informerClientSet.CoreV1().Pods(e.pod.Namespace).Update(context.TODO(), e.pod, metav1.UpdateOptions{})
					case deletePodEvent:
						err = informerClientSet.CoreV1().Pods(e.pod.Namespace).Delete(context.TODO(), e.pod.Name, metav1.DeleteOptions{})
					case scaleUpEvent:
						observer.Process(&ca_context.AutoscalingContext{}, e.scaleUp)
					case timeoutEvent:
						observer.reportTimeoutPods()
					case classifiedAsSchedulable:
						observer.ObserveReaction([]*v1.Pod{e.pod}, metrics.NoActionNeeded)
					}
					assert.NoError(t, err)
					synctest.Wait()
				}
				result := observer.calculatePendingPods()
				assert.ElementsMatch(t, tc.want, result)
			})
		})
	}
}

func TestCalculatePendingPodsPerCcc(t *testing.T) {
	testCases := []struct {
		name                string
		unreportedPodStates map[types.UID]*podStateData
		reportedPodStates   map[types.UID]*podStateData
		crds                []crd.CRD
		want                []podstatetypes.PendingPodsPerCccMetric
	}{
		{
			name:                "no pending pods",
			unreportedPodStates: map[types.UID]*podStateData{},
			reportedPodStates:   map[types.UID]*podStateData{},
			want:                []podstatetypes.PendingPodsPerCccMetric{},
		},
		{
			name:                "no pending pods, but CCCs exist",
			unreportedPodStates: map[types.UID]*podStateData{},
			reportedPodStates:   map[types.UID]*podStateData{},
			crds:                []crd.CRD{mockCrd{name: "test-ccc"}},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
			},
		},
		{
			name: "one unprocessed non-system pod",
			unreportedPodStates: map[types.UID]*podStateData{
				"p1": {reactionType: metrics.NoReaction, systemPod: false, cccName: "test-ccc"},
			},
			reportedPodStates: map[types.UID]*podStateData{},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 1}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
			},
		},
		{
			name: "complex scenario",
			unreportedPodStates: map[types.UID]*podStateData{
				"p1":  {reactionType: metrics.NoReaction, systemPod: false, cccName: "test-ccc"},
				"p1s": {reactionType: metrics.NoReaction, systemPod: true, cccName: "test-ccc-2"},
			},
			reportedPodStates: map[types.UID]*podStateData{
				"p2":  {reactionType: metrics.ScaleUp, systemPod: false, cccName: "test-ccc"},
				"p3":  {reactionType: metrics.ScaleUp, systemPod: false, cccName: "test-ccc"},
				"p4":  {reactionType: metrics.Unhelpable, systemPod: false, cccName: "test-ccc-2"},
				"p5":  {reactionType: metrics.NoActionNeeded, systemPod: false, cccName: "test-ccc-2"},
				"p6":  {reactionType: metrics.Timeout, systemPod: false, cccName: "test-ccc"},
				"p7":  {reactionType: metrics.Scheduled, systemPod: false, cccName: "test-ccc"}, // Should be ignored.
				"p2s": {reactionType: metrics.EkUpsize, systemPod: true, cccName: "test-ccc-2"},
				"p3s": {reactionType: metrics.Unhelpable, systemPod: true, cccName: "test-ccc-2"},
				"p4s": {reactionType: metrics.Unhelpable, systemPod: true, cccName: "test-ccc"},
				"p5s": {reactionType: metrics.NoActionNeeded, systemPod: true, cccName: "test-ccc"},
				"p6s": {reactionType: metrics.Deleted, systemPod: true, cccName: "test-ccc-2"}, // Should be ignored.
			},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 2}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 1}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 2}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 1}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 1}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 1}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 1}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 1}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 1}},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			observer := &PodStateObserver{
				npcCrdLister:        &mockNpcCrdLister{crds: tc.crds},
				unreportedPodStates: tc.unreportedPodStates,
				reportedPodStates:   tc.reportedPodStates,
			}
			result := observer.calculatePendingPodsPerCcc()
			assert.ElementsMatch(t, tc.want, result)
		})
	}
}

func TestCalculatePendingPodsPerCccLifecycle(t *testing.T) {
	// Temporarily override the metric registration function to prevent duplicate registration in tests.
	oldRegisterFunc := RegisterPendingPodsPerCccCollectorFunc
	RegisterPendingPodsPerCccCollectorFunc = func(collectFunc ccc.PendingPodsPerCccCalculationFunc) {}
	t.Cleanup(func() {
		RegisterPendingPodsPerCccCollectorFunc = oldRegisterFunc
	})
	var st = time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	var p = test.BuildTestPod("p1", 100, 1, withComputeClass("test-ccc"))
	var p2 = test.BuildTestPod("p2", 200, 2, withComputeClass("test-ccc-2"))
	var p3 = test.BuildTestPod("p3", 200, 2, withComputeClass("test-ccc"), func(p *v1.Pod) { p.Namespace = metav1.NamespaceSystem })
	var ps = test.BuildScheduledTestPod("p1", 100, 1, "some node")
	var pn = test.BuildTestPod("pn", 100, 1)
	testCases := []struct {
		name        string
		events      []event
		crds        []crd.CRD
		want        []podstatetypes.PendingPodsPerCccMetric
		fallbackCcc string
	}{
		{
			name:   "no pending pods",
			events: []event{},
			want:   []podstatetypes.PendingPodsPerCccMetric{},
		},
		{
			name: "one unprocessed non-system pod",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 1}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
			},
		},
		{
			name: "one unprocessed non-system pod without CCC (without default)",
			events: []event{
				{t: st, et: newPodEvent, pod: pn, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 1}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
			},
			fallbackCcc: "",
		},
		{
			name: "one unprocessed non-system pod without CCC (with default)",
			events: []event{
				{t: st, et: newPodEvent, pod: pn, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "default", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "default", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "default", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "default", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "default", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "default", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 1}},
				{CccName: "default", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "default", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
			},
			fallbackCcc: "default",
		},
		{
			name: "pod gets scheduled",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: updatePodEvent, pod: ps, scaleUp: nil},
			},
			crds: []crd.CRD{mockCrd{name: "test-ccc"}},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
			},
		},
		{
			name: "pod gets deleted",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st.Add(time.Second * 20), et: deletePodEvent, pod: p, scaleUp: nil},
			},
			crds: []crd.CRD{mockCrd{name: "test-ccc"}},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
			},
		},
		{
			name: "pod times out",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st.Add(metrics.MaxReactionTime + time.Second), et: timeoutEvent, pod: nil, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 1}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
			},
		},
		{
			name: "complex scenario",
			events: []event{
				{t: st, et: newPodEvent, pod: p, scaleUp: nil},
				{t: st, et: newPodEvent, pod: pn, scaleUp: nil},
				{t: st.Add(time.Second * 1), et: newPodEvent, pod: p2, scaleUp: nil},
				{t: st.Add(time.Second * 2), et: newPodEvent, pod: p3, scaleUp: nil}, // system pod
				{t: st.Add(time.Second * 10), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsTriggeredScaleUp: []*v1.Pod{p}}},
				{t: st.Add(time.Second * 11), et: scaleUpEvent, pod: nil, scaleUp: &status.ScaleUpStatus{PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: p2}}}},
				{t: st.Add(time.Second * 12), et: classifiedAsSchedulable, pod: p3, scaleUp: nil},
			},
			want: []podstatetypes.PendingPodsPerCccMetric{
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 1}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 1}},
				{CccName: "test-ccc", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 1}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "test-ccc-2", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: true, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.ProvisioningInProgress, SystemPod: false, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: true, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.UnableToProvision, SystemPod: false, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: true, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.Unprocessed, SystemPod: false, Count: 1}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: true, Count: 0}},
				{CccName: "", PendingPodsMetric: podstatetypes.PendingPodsMetric{Kind: podstatetypes.NoActionTaken, SystemPod: false, Count: 0}},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				informerClientSet := fake.NewSimpleClientset()
				informerFactory := informers.NewSharedInformerFactory(informerClientSet, 0)
				testMetrics := &testReactionTimeMetrics{}
				fakeClock := clock.NewFakeClock(st)
				podsClassifier := systempods.NewClassifier([]string{metav1.NamespaceSystem})
				observer, err := newPodStateObserver(informerFactory, testMetrics, podsClassifier, &mockNpcCrdLister{fallbackCcc: tc.fallbackCcc, crds: tc.crds}, time.Minute, fakeClock, false, false, -10)
				assert.NoError(t, err)
				stopCh := make(chan struct{})
				defer close(stopCh)
				informerFactory.Start(stopCh)
				_ = informerFactory.WaitForCacheSync(stopCh)
				for _, e := range tc.events {
					fakeClock.SetTime(e.t)
					switch e.et {
					case newPodEvent:
						e.pod.CreationTimestamp.Time = fakeClock.Now()
						_, err = informerClientSet.CoreV1().Pods(e.pod.Namespace).Create(context.TODO(), e.pod, metav1.CreateOptions{})
					case updatePodEvent:
						_, err = informerClientSet.CoreV1().Pods(e.pod.Namespace).Update(context.TODO(), e.pod, metav1.UpdateOptions{})
					case deletePodEvent:
						err = informerClientSet.CoreV1().Pods(e.pod.Namespace).Delete(context.TODO(), e.pod.Name, metav1.DeleteOptions{})
					case scaleUpEvent:
						observer.Process(&ca_context.AutoscalingContext{}, e.scaleUp)
					case timeoutEvent:
						observer.reportTimeoutPods()
					case classifiedAsSchedulable:
						observer.ObserveReaction([]*v1.Pod{e.pod}, metrics.NoActionNeeded)
					}
					assert.NoError(t, err)
					synctest.Wait()
				}
				result := observer.calculatePendingPodsPerCcc()
				assert.ElementsMatch(t, tc.want, result)
			})
		})
	}
}

func BuildPVCVolume() v1.Volume {
	vol := v1.Volume{}
	vol.Name = "TestVolume"
	vol.VolumeSource.PersistentVolumeClaim = &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: "TestPVC",
	}
	return vol
}
func BuildCSIVolume() v1.Volume {
	vol := v1.Volume{}
	vol.Name = "TestVolume"
	vol.VolumeSource.CSI = &v1.CSIVolumeSource{
		Driver: "TestDriver",
	}
	return vol
}

func markPodUnschedulableWithTime(ts time.Time) func(*v1.Pod) {
	return func(pod *v1.Pod) {
		test.MarkUnschedulable()(pod)
		_, condition := podv1.GetPodCondition(&pod.Status, v1.PodScheduled)
		condition.LastTransitionTime.Time = ts
	}
}

func buildPodWithAnnotations(podName string, annotations map[string]string) *v1.Pod {
	pod := test.BuildTestPod(podName, 100, 1)
	pod.Annotations = annotations
	return pod
}

func withNodeSelector(k, v string) func(*v1.Pod) {
	return func(pod *v1.Pod) {
		if pod.Spec.NodeSelector == nil {
			pod.Spec.NodeSelector = make(map[string]string)
		}
		pod.Spec.NodeSelector[k] = v
	}
}

func withComputeClass(computeClass string) func(*v1.Pod) {
	return withNodeSelector("cloud.google.com/compute-class", computeClass)
}

type mockNpcCrdLister struct {
	npc_lister.Lister
	fallbackCcc string
	crds        []crd.CRD
}

func (m *mockNpcCrdLister) PodCrd(pod *v1.Pod) (crd.CRD, string, error) {
	if name, ok := pod.Spec.NodeSelector[gkelabels.ComputeClassLabel]; ok {
		return nil, name, nil
	}
	return nil, m.fallbackCcc, nil
}

func (m *mockNpcCrdLister) ListCrds() ([]crd.CRD, error) {
	return m.crds, nil
}

func (m *mockNpcCrdLister) Default() (string, string, bool) {
	if m.fallbackCcc != "" {
		return m.fallbackCcc, "cloud.google.com/compute-class", true
	}
	return "", "", false
}

type mockCrd struct {
	crd.CRD
	name string
}

func (m mockCrd) Name() string {
	return m.name
}
