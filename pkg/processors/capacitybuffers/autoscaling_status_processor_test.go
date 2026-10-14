/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Modifications:
- Changed package name from capacitybuffer to capacitybuffers.
- Copied bufferOption, newTestBuffer, withBufferName, withBufferUID, and withStatusReadyReplicas test helpers from upstream pod_list_processor_test.go.
*/

package capacitybuffers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	. "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

type bufferOption func(*v1beta1.CapacityBuffer)

func newTestBuffer(opts ...bufferOption) *v1beta1.CapacityBuffer {
	b := &v1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
		},
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func withBufferName(name string) bufferOption {
	return func(b *v1beta1.CapacityBuffer) {
		b.Name = name
	}
}

func withBufferUID(uid types.UID) bufferOption {
	return func(b *v1beta1.CapacityBuffer) {
		b.UID = uid
	}
}

func withStatusReadyReplicas(readyReplicas int32) bufferOption {
	return func(b *v1beta1.CapacityBuffer) {
		b.Status.ReadyReplicas = &readyReplicas
	}
}

type fakeReadyReplicasUpdater struct {
	updated map[string]int32
}

func (f *fakeReadyReplicasUpdater) Update(buffer *v1beta1.CapacityBuffer, readyReplicas int32) {
	f.updated[buffer.Name] = readyReplicas
}

func TestCapacityBufferAutoscalingStatusProcessor(t *testing.T) {
	const namespace = "default"

	existingNode := BuildTestNode("existing", 1000, 1000)
	upcomingNode := BuildTestNode("upcoming", 1000, 1000)
	upcomingNode.Annotations = map[string]string{annotations.NodeUpcomingAnnotation: "true"}

	type fakePodSpec struct {
		name     string
		nodeName string
		buffer   string
	}

	testCases := []struct {
		name        string
		buffers     []*v1beta1.CapacityBuffer
		processed   []string
		fakePods    []fakePodSpec
		wantUpdated map[string]int32
	}{
		{
			name: "empty registry, no updates",
			buffers: []*v1beta1.CapacityBuffer{
				newTestBuffer(
					withBufferName("b1"),
					withBufferUID("b1-uid"),
					withStatusReadyReplicas(3),
				),
			},
			wantUpdated: map[string]int32{},
		},
		{
			name: "fake pods on existing node are counted",
			buffers: []*v1beta1.CapacityBuffer{
				newTestBuffer(
					withBufferName("b1"),
					withBufferUID("b1-uid"),
				),
			},
			processed: []string{"b1"},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "existing", buffer: "b1"},
				{name: "p2", nodeName: "existing", buffer: "b1"},
			},
			wantUpdated: map[string]int32{"b1": 2},
		},
		{
			name: "fake pods on upcoming node are not counted",
			buffers: []*v1beta1.CapacityBuffer{
				newTestBuffer(
					withBufferName("b1"),
					withBufferUID("b1-uid"),
				),
			},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "existing", buffer: "b1"},
				{name: "p2", nodeName: "upcoming", buffer: "b1"},
			},
			wantUpdated: map[string]int32{"b1": 1},
		},
		{
			name: "processed buffer without fake pods gets 0",
			buffers: []*v1beta1.CapacityBuffer{
				newTestBuffer(
					withBufferName("b1"),
					withBufferUID("b1-uid"),
					withStatusReadyReplicas(2),
				),
			},
			processed:   []string{"b1"},
			wantUpdated: map[string]int32{"b1": 0},
		},
		{
			name: "processed buffer with fake pods only on upcoming nodes gets 0",
			buffers: []*v1beta1.CapacityBuffer{
				newTestBuffer(
					withBufferName("b1"),
					withBufferUID("b1-uid"),
					withStatusReadyReplicas(2),
				),
			},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "upcoming", buffer: "b1"},
			},
			wantUpdated: map[string]int32{"b1": 0},
		},
		{
			name: "unprocessed buffer is left unchanged",
			buffers: []*v1beta1.CapacityBuffer{
				newTestBuffer(
					withBufferName("b1"),
					withBufferUID("b1-uid"),
				),
				newTestBuffer(
					withBufferName("unprocessed"),
					withBufferUID("unprocessed-uid"),
					withStatusReadyReplicas(5),
				),
			},
			processed: []string{"b1"},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "existing", buffer: "b1"},
			},
			wantUpdated: map[string]int32{"b1": 1},
		},
		{
			name: "multiple buffers",
			buffers: []*v1beta1.CapacityBuffer{
				newTestBuffer(
					withBufferName("b1"),
					withBufferUID("b1-uid"),
				),
				newTestBuffer(
					withBufferName("b2"),
					withBufferUID("b2-uid"),
					withStatusReadyReplicas(4),
				),
				newTestBuffer(
					withBufferName("b3"),
					withBufferUID("b3-uid"),
					withStatusReadyReplicas(1),
				),
			},
			processed: []string{"b3"},
			fakePods: []fakePodSpec{
				{name: "p1", nodeName: "existing", buffer: "b1"},
				{name: "p2", nodeName: "existing", buffer: "b2"},
				{name: "p3", nodeName: "existing", buffer: "b2"},
				{name: "p4", nodeName: "upcoming", buffer: "b2"},
			},
			wantUpdated: map[string]int32{"b1": 1, "b2": 2, "b3": 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buffersByName := make(map[string]*v1beta1.CapacityBuffer, len(tc.buffers))
			for _, buffer := range tc.buffers {
				buffersByName[buffer.Name] = buffer
			}

			registry := fakepods.NewRegistry(nil)
			for _, name := range tc.processed {
				registry.MarkProcessed(buffersByName[name])
			}
			var pods []*apiv1.Pod
			for _, fp := range tc.fakePods {
				pod := BuildTestPod(fp.name, 100, 100, WithNamespace(namespace), WithNodeName(fp.nodeName))
				registry.SetCapacityBuffer(pod.UID, buffersByName[fp.buffer])
				pods = append(pods, pod)
			}

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			err := snapshot.SetClusterState(context.Background(), []*apiv1.Node{existingNode, upcomingNode}, pods, nil, nil)
			assert.NoError(t, err)

			updater := &fakeReadyReplicasUpdater{updated: make(map[string]int32)}
			processor := NewCapacityBufferAutoscalingStatusProcessor(updater, registry)
			autoscalingCtx := &ca_context.AutoscalingContext{ClusterSnapshot: snapshot}
			err = processor.Process(t.Context(), autoscalingCtx, nil, time.Now())
			assert.NoError(t, err)
			assert.Equal(t, tc.wantUpdated, updater.updated)
		})
	}
}
