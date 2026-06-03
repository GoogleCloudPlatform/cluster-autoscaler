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

package capacitybuffer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	clock "k8s.io/utils/clock/testing"
)

type mockFakePodReactionTimeObserver struct {
	mock.Mock
}

func (m *mockFakePodReactionTimeObserver) ObserveCapacityBufferFakePodReactionTime(duration time.Duration, systemPod bool, hasPVC bool, hasCSI bool, reactionType metrics.ReactionType, provisioningType, allocationMode string) {
	m.Called(duration, systemPod, hasPVC, hasCSI, reactionType, provisioningType, allocationMode)
}

type scaleUpStep struct {
	t       time.Duration
	scaleUp *status.ScaleUpStatus
}

type schedulableStep struct {
	t    time.Duration
	pods []*v1.Pod
}

type injectedPodsStep struct {
	t    time.Duration
	pods map[*v1.Pod]*v1beta1.CapacityBuffer
}

type caLoop struct {
	t                time.Duration
	schedulableSteps []schedulableStep
	scaleUps         []scaleUpStep
	injectedPods     []injectedPodsStep
}

func TestFakePodStateObserver_EndToEnd(t *testing.T) {
	st := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)

	// Helper to build a buffer
	buildBuffer := func(name string, replicas int32) *v1beta1.CapacityBuffer {
		val := replicas
		return &v1beta1.CapacityBuffer{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				UID:  types.UID(name + "-uid"),
			},
			Status: v1beta1.CapacityBufferStatus{
				Replicas: &val,
			},
		}
	}

	buffer1 := buildBuffer("buffer1", 1)
	buffer2 := buildBuffer("buffer2", 1)
	buffer3 := buildBuffer("buffer3", 1)

	pod1 := test.BuildTestPod("pod1", 100, 100)
	pod1.UID = "pod1-uid"
	pod2 := test.BuildTestPod("pod2", 100, 100)
	pod2.UID = "pod2-uid"
	pod3 := test.BuildTestPod("pod3", 100, 100)
	pod3.UID = "pod3-uid"

	// System pod
	podSys := test.BuildTestPod("podSys", 100, 100)
	podSys.UID = "podSys-uid"
	podSys.Namespace = "kube-system"

	// Pod with PVC
	podPVC := test.BuildTestPod("podPVC", 100, 100)
	podPVC.UID = "podPVC-uid"
	podPVC.Spec.Volumes = []v1.Volume{
		{
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: "claim",
				},
			},
		},
	}

	// Pod with CSI
	podCSI := test.BuildTestPod("podCSI", 100, 100)
	podCSI.UID = "podCSI-uid"
	podCSI.Spec.Volumes = []v1.Volume{
		{
			VolumeSource: v1.VolumeSource{
				CSI: &v1.CSIVolumeSource{
					Driver: "driver",
				},
			},
		},
	}

	coldStrategy := capacitybuffers.ColdProvisioningStrategy
	coldBuffer := buildBuffer("coldBuffer", 1)
	coldBuffer.Status.ProvisioningStrategy = &coldStrategy

	testCases := []struct {
		name                 string
		loops                []caLoop
		expectedObservations []expectedObservation
	}{
		{
			name: "Pod starts as schedulable (immediate NoActionNeeded)",
			loops: []caLoop{
				{
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{{t: 1 * time.Second, pods: []*v1.Pod{pod1}}},
					scaleUps:         []scaleUpStep{},
				},
				// Next loop, still schedulable
				{
					t: 10 * time.Second,
					injectedPods: []injectedPodsStep{{
						t: 10 * time.Second,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{{t: 11 * time.Second, pods: []*v1.Pod{pod1}}},
					scaleUps:         []scaleUpStep{},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             1 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
		{
			name: "Pod waits then triggers scale up",
			loops: []caLoop{
				// Initial: AwaitEvaluation (pending)
				{
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{}, // Not schedulable yet
					scaleUps: []scaleUpStep{{
						t: 1 * time.Second,
						scaleUp: &status.ScaleUpStatus{
							PodsAwaitEvaluation: []*v1.Pod{pod1},
						},
					}},
				},
				// +11s: TriggeredScaleUp
				{
					t: 10 * time.Second,
					injectedPods: []injectedPodsStep{{
						t: 10 * time.Second,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{},
					scaleUps: []scaleUpStep{{
						t: 12 * time.Second,
						scaleUp: &status.ScaleUpStatus{
							PodsTriggeredScaleUp: []*v1.Pod{pod1},
						},
					}},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             12 * time.Second,
					reactionType:         metrics.ScaleUp,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
		{
			name: "Pod waits then becomes Unhelpable",
			loops: []caLoop{
				// Initial: AwaitEvaluation
				{
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{},
					scaleUps: []scaleUpStep{{
						t: 1 * time.Second,
						scaleUp: &status.ScaleUpStatus{
							PodsAwaitEvaluation: []*v1.Pod{pod1},
						},
					}},
				},
				// +11s: RemainUnschedulable (Unhelpable)
				{
					t: 10 * time.Second,
					injectedPods: []injectedPodsStep{{
						t: 10 * time.Second,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{{t: 11 * time.Second, pods: []*v1.Pod{}}},
					scaleUps: []scaleUpStep{{
						t: 11 * time.Second,
						scaleUp: &status.ScaleUpStatus{
							PodsRemainUnschedulable: []status.NoScaleUpInfo{
								{Pod: pod1},
							},
						},
					}},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             11 * time.Second,
					reactionType:         metrics.Unhelpable,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
		{
			name: "Metric reported when all pods are schedulable",
			loops: []caLoop{
				{
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
							pod2: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{{
						t:    1 * time.Second,
						pods: []*v1.Pod{pod1, pod2},
					}},
					scaleUps: []scaleUpStep{},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             1 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
				{
					duration:             1 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
		{
			name: "Metric reported when all pods have scaling decision (schedulable or triggered scale up)",
			loops: []caLoop{
				{ // decision for pod1
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
							pod2: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{{
						t:    1 * time.Second,
						pods: []*v1.Pod{pod1},
					}},
					scaleUps: []scaleUpStep{{
						t:       2 * time.Second,
						scaleUp: &status.ScaleUpStatus{},
					}},
				},
				{ // decision for pod1 and pod2 - reporting metric
					t: 10 * time.Second,
					injectedPods: []injectedPodsStep{{
						t: 10 * time.Second,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
							pod2: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{{
						t:    11 * time.Second,
						pods: []*v1.Pod{pod1},
					}},
					scaleUps: []scaleUpStep{{
						t: 12 * time.Second,
						scaleUp: &status.ScaleUpStatus{
							PodsTriggeredScaleUp: []*v1.Pod{pod2},
						},
					}},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             12 * time.Second, // metric is reported only when all pods have scaling decision
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
				{
					duration:             12 * time.Second,
					reactionType:         metrics.ScaleUp,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
		{
			name: "Timeout",
			loops: []caLoop{
				{ // pod1 (0s)
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{},
					scaleUps:         []scaleUpStep{},
				},
				{ // pod2 (+10s)
					t: 10 * time.Second,
					injectedPods: []injectedPodsStep{{
						t: 10 * time.Second,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
							pod2: buffer1,
							pod3: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{},
					scaleUps:         []scaleUpStep{},
				},
				{ // pod3 (+20s)
					t: 20 * time.Second,
					injectedPods: []injectedPodsStep{{
						t: 20 * time.Second,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
							pod2: buffer1,
							pod3: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{},
					scaleUps:         []scaleUpStep{},
				},
				// timeout: pod1 after injection, pod2 during schedulable processing, pod3 during scale up processing
				{
					t: metrics.MaxReactionTime + 1*time.Second,
					injectedPods: []injectedPodsStep{{
						t: metrics.MaxReactionTime + 1*time.Second,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1, // pod1 should time out after injection
							pod2: buffer1,
							pod3: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{{ // pod2 will not time out when processing schedulable
						t:    metrics.MaxReactionTime + 23*time.Second,
						pods: []*v1.Pod{pod1, pod2},
					}},
					scaleUps: []scaleUpStep{{ // pod3 will not time out when processing scale up
						t: metrics.MaxReactionTime + 23*time.Second,
						scaleUp: &status.ScaleUpStatus{
							PodsTriggeredScaleUp: []*v1.Pod{pod3}, // despite scale up reported as timeout
						},
					}},
				},
			},
			expectedObservations: []expectedObservation{
				{ // pod1
					duration:             metrics.MaxReactionTime + 1*time.Second,
					reactionType:         metrics.Timeout,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
				{ // pod2
					duration:             metrics.MaxReactionTime + 13*time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
				{ // pod3
					duration:             metrics.MaxReactionTime + 13*time.Second,
					reactionType:         metrics.ScaleUp,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
		{
			name: "Two buffers separate pods",
			loops: []caLoop{
				// Loop 1: Pod1 in Buffer1 (Schedulable), Pod2 in Buffer2 (Schedulable)
				{
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
							pod2: buffer2,
						},
					}},
					schedulableSteps: []schedulableStep{{t: 1 * time.Second, pods: []*v1.Pod{pod1, pod2}}},
					scaleUps:         []scaleUpStep{},
				},
				// Loop 2: Pod1 triggers ScaleUp (was Schedulable, now ScaleUp -> treated as new pod), Pod2 stays Schedulable (not reported again)
				{
					t: 10 * time.Second,
					injectedPods: []injectedPodsStep{{
						t: 10 * time.Second,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
							pod2: buffer2,
						},
					}},
					schedulableSteps: []schedulableStep{{t: 12 * time.Second, pods: []*v1.Pod{pod2}}}, // Pod2 is still schedulable
					scaleUps: []scaleUpStep{{
						t: 12 * time.Second,
						scaleUp: &status.ScaleUpStatus{
							PodsTriggeredScaleUp: []*v1.Pod{pod1},
						},
					}},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             1 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
				{
					duration:             1 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
				{
					duration:             2 * time.Second, // Time resets as it was previously Schedulable
					reactionType:         metrics.ScaleUp,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
		{
			name: "Cold capacity buffer pod",
			loops: []caLoop{
				{
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: coldBuffer,
						},
					}},
					schedulableSteps: []schedulableStep{{t: 1 * time.Second, pods: []*v1.Pod{pod1}}},
					scaleUps:         []scaleUpStep{},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             1 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffers.ColdProvisioningStrategy,
				},
			},
		},
		{
			name: "Classifications",
			loops: []caLoop{
				{
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							podSys: buffer1,
							podPVC: buffer2,
							podCSI: buffer3,
						},
					}},
					schedulableSteps: []schedulableStep{{
						t:    1 * time.Second,
						pods: []*v1.Pod{podSys},
					}},
					scaleUps: []scaleUpStep{{
						t: 1 * time.Second,
						scaleUp: &status.ScaleUpStatus{
							PodsTriggeredScaleUp:    []*v1.Pod{podPVC},
							PodsRemainUnschedulable: []status.NoScaleUpInfo{{Pod: podCSI}},
						},
					}},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             1 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					systemPod:            true,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
				{
					duration:             1 * time.Second,
					reactionType:         metrics.ScaleUp,
					hasPVC:               true,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
				{
					duration:             1 * time.Second,
					reactionType:         metrics.Unhelpable,
					hasCSI:               true,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
		{
			name: "Buffer disappears and reappears",
			loops: []caLoop{
				// Loop 1: Buffer appears, pod is awaiting evaluation (not reported yet)
				{
					t: 0,
					injectedPods: []injectedPodsStep{{
						t: 0,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{},
					scaleUps:         []scaleUpStep{},
				},
				// Loop 2: Buffer disappears
				{
					t: 10 * time.Second,
					injectedPods: []injectedPodsStep{
						{
							t: 20 * time.Second,
							// empty list of pods to inject
						},
					},
					schedulableSteps: []schedulableStep{},
					scaleUps:         []scaleUpStep{},
				},
				// Loop 3: Buffer reappears, should be reported as a new pod (0s duration)
				{
					t: 20 * time.Second,
					injectedPods: []injectedPodsStep{{
						t: 20 * time.Second,
						pods: map[*v1.Pod]*v1beta1.CapacityBuffer{
							pod1: buffer1,
						},
					}},
					schedulableSteps: []schedulableStep{{t: 21 * time.Second, pods: []*v1.Pod{pod1}}},
					scaleUps:         []scaleUpStep{},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             1 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
		{
			name: "Multiple buffers injected in different calls in the same loop",
			loops: []caLoop{
				{
					t: 0,
					injectedPods: []injectedPodsStep{
						{
							t:    0,
							pods: map[*v1.Pod]*v1beta1.CapacityBuffer{pod1: buffer1},
						},
						{
							t:    5 * time.Second,
							pods: map[*v1.Pod]*v1beta1.CapacityBuffer{pod2: buffer2},
						},
					},
					schedulableSteps: []schedulableStep{
						{
							t:    10 * time.Second,
							pods: []*v1.Pod{pod1, pod2},
						},
					},
					scaleUps: []scaleUpStep{},
				},
			},
			expectedObservations: []expectedObservation{
				{
					duration:             10 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
				{
					duration:             5 * time.Second,
					reactionType:         metrics.NoActionNeeded,
					provisioningStrategy: capacitybuffer.ActiveProvisioningStrategy,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeClock := clock.NewFakeClock(st)
			mockObserver := &mockFakePodReactionTimeObserver{}
			registry := fakepods.NewRegistry(nil)
			classifier := systempods.NewClassifier([]string{"kube-system"})

			observer := NewFakePodStateObserver(classifier, mockObserver, registry, fakeClock, true)

			for _, eo := range tc.expectedObservations {
				mockObserver.On("ObserveCapacityBufferFakePodReactionTime", eo.duration, eo.systemPod, eo.hasPVC, eo.hasCSI, eo.reactionType, eo.provisioningStrategy, podutils.AllocationModeNone.String()).Return()
			}

			for _, loop := range tc.loops {
				observer.Reset()

				registry.Clear()
				fakeClock.SetTime(st.Add(loop.t))
				for _, step := range loop.injectedPods {
					fakeClock.SetTime(st.Add(step.t))
					pods := make([]*v1.Pod, 0, len(step.pods))
					for p, buffer := range step.pods {
						registry.SetCapacityBuffer(p.UID, buffer)
						pods = append(pods, p)
					}
					observer.ObserveInjectedPods(pods)
				}

				for _, step := range loop.schedulableSteps {
					fakeClock.SetTime(st.Add(step.t))
					observer.ObserveSchedulablePods(step.pods)
				}

				for _, step := range loop.scaleUps {
					fakeClock.SetTime(st.Add(step.t))
					observer.Process(&ca_context.AutoscalingContext{}, step.scaleUp)
				}
			}

			mockObserver.AssertExpectations(t)
		})
	}
}

type expectedObservation struct {
	duration             time.Duration
	reactionType         metrics.ReactionType
	systemPod            bool
	hasPVC               bool
	hasCSI               bool
	provisioningStrategy string
}
