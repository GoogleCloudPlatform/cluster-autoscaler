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

package podkind

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	cr_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/apis/internal.autoscaling.gke.io/v1"
	cr_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/capacityrequests/utils"
	npc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	capacitybuffer "sigs.k8s.io/cluster-autoscaler/pkg/processors/capacitybuffer"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/fake"
)

func podWithAnnotations(name string, annotations map[string]string) *apiv1.Pod {
	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: annotations,
		},
	}
}

// capacityRequestPod returns the synthetic pod that the capacity request
// processor injects for a CapacityRequest.
func capacityRequestPod(t *testing.T) *apiv1.Pod {
	t.Helper()
	cr := &cr_types.CapacityRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "cr-1", Namespace: "default"},
		Spec:       cr_types.CapacityRequestSpec{Capacity: apiv1.PodSpec{}},
	}
	state := cr_utils.NewCapacityRequestState(nil)
	state.Update([]*cr_types.CapacityRequest{cr})
	pod, found := state.CapacityRequestToPod(cr)
	if !found {
		t.Fatalf("No pod created for CapacityRequest %s/%s", cr.Namespace, cr.Name)
	}
	return pod
}

func TestOf(t *testing.T) {
	lookaheadPod := lookaheadbuffer.GenerateLookaheadPods(1, resource.MustParse("1"), resource.MustParse("1Gi"), "workload-1")[0]

	csnPod := podWithAnnotations("csn-pod", nil)
	csn.MakePodCSN(csnPod, "buffer-1")

	testCases := []struct {
		name string
		pod  *apiv1.Pod
		want Kind
	}{
		{
			name: "real pending pod",
			pod:  podWithAnnotations("user-pod", nil),
			want: PodPending,
		},
		{
			name: "nil pod",
			pod:  nil,
			want: UnrecognizedFake,
		},
		{
			name: "min capacity pod",
			pod:  podWithAnnotations("min-capacity-pod", map[string]string{npc_processors.MinCapacityFakePodAnnotation: "true"}),
			want: MinCapacity,
		},
		{
			name: "active migration pod",
			pod:  podWithAnnotations("migration-pod", map[string]string{defrag.ActiveMigrationPodAnnotation: "true"}),
			want: ActiveMigration,
		},
		{
			name: "capacity buffer pod",
			pod: podWithAnnotations("capacity-buffer-pod", map[string]string{
				capacitybuffer.CapacityBufferFakePodAnnotationKey: capacitybuffer.CapacityBufferFakePodAnnotationValue,
			}),
			want: CapacityBuffer,
		},
		{
			name: "standby capacity (CSN) pod is a capacity buffer pod",
			pod:  csnPod,
			want: CapacityBuffer,
		},
		{
			name: "GKE ProvisioningRequest pod",
			pod:  podWithAnnotations("prov-req-pod", map[string]string{provreqv1.ProvisioningRequestPodAnnotationKey: "pr-1"}),
			want: ProvisioningRequest,
		},
		{
			name: "CapacityRequest pod",
			pod:  capacityRequestPod(t),
			want: CapacityRequest,
		},
		{
			name: "OSS ProvisioningRequest pod with deprecated annotation",
			pod: podWithAnnotations("prov-req-pod-deprecated", map[string]string{
				"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "pr-1",
			}),
			want: ProvisioningRequest,
		},
		{
			name: "EK VM lookahead pod is internal",
			pod:  lookaheadPod,
			want: Internal,
		},
		{
			name: "OSS proactive scale-up pod copy",
			pod: podWithAnnotations("user-pod-copy-1", map[string]string{
				fake.FakePodAnnotationKey: fake.FakePodAnnotationValue,
			}),
			want: UnrecognizedFake,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Of(tc.pod))
		})
	}
}

func TestIsSynthetic(t *testing.T) {
	assert.False(t, IsSynthetic(podWithAnnotations("user-pod", nil)))
	assert.True(t, IsSynthetic(podWithAnnotations("migration-pod", map[string]string{defrag.ActiveMigrationPodAnnotation: "true"})))
	assert.True(t, IsSynthetic(podWithAnnotations("fake-pod", map[string]string{fake.FakePodAnnotationKey: fake.FakePodAnnotationValue})))
}
