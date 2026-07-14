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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
)

func TestPodGroupProto(t *testing.T) {
	controller := &PodController{Uid: "cuid1", Name: "c1", Kind: "ck1", ApiVersion: "v1"}
	pod := &Pod{Name: "pod1337", Uid: "pid1337", Controller: controller}

	napReason1 := &Message{Id: NoScaleUpMigUnknownReason}
	napReason2 := &Message{Id: NoScaleUpMigUnknownReason, Params: []string{"a", "b"}}

	migReason1 := &MigExplanation{
		Mig:    &GkeMig{Id: "mig1", Name: "mig1", Zone: "z1", NodePoolName: "np1"},
		Reason: &Message{Id: NoScaleUpMigUnknownReason},
	}
	migReason2 := &MigExplanation{
		Mig:    &GkeMig{Id: "mig2", Name: "mig2", Zone: "z1", NodePoolName: "np2"},
		Reason: &Message{Id: NoScaleUpMigUnknownReason, Params: []string{"c", "d"}},
	}

	expectedPodGroupProto := &vispb.PodGroup{
		SamplePod: &vispb.Pod{
			Name: "pod1337",
			Controller: &vispb.PodController{
				ApiVersion: "v1",
				Kind:       "ck1",
				Name:       "c1",
			},
		},
		TotalPodCount: 1337,
	}

	for _, testCase := range []struct {
		name          string
		group         *PodGroupExplanation
		expectedProto *vispb.PodGroupExplanation
	}{
		{
			name: "no reasons",
			group: &PodGroupExplanation{
				SamplePod: pod,
				PodCount:  1337,
			},
			expectedProto: &vispb.PodGroupExplanation{
				PodGroup: expectedPodGroupProto,
			},
		},
		{
			name: "with reasons",
			group: &PodGroupExplanation{
				SamplePod:  pod,
				PodCount:   1337,
				MigReasons: map[string]*MigExplanation{"mig1": migReason1, "mig2": migReason2},
				NapReasons: []*Message{napReason1, napReason2},
			},
			expectedProto: &vispb.PodGroupExplanation{
				PodGroup: expectedPodGroupProto,
				RejectedMigs: []*vispb.MigExplanation{
					{
						Mig:    &vispb.Mig{Name: "mig1", Nodepool: "np1", Zone: "z1"},
						Reason: &vispb.ParametrizedMessage{MessageId: "no.scale.up.mig.unknown.reason"},
					},
					{
						Mig:    &vispb.Mig{Name: "mig2", Nodepool: "np2", Zone: "z1"},
						Reason: &vispb.ParametrizedMessage{MessageId: "no.scale.up.mig.unknown.reason", Parameters: []string{"c", "d"}},
					},
				},
				NapFailureReasons: []*vispb.ParametrizedMessage{
					{MessageId: "no.scale.up.mig.unknown.reason"},
					{MessageId: "no.scale.up.mig.unknown.reason", Parameters: []string{"a", "b"}},
				},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			computedProto := testCase.group.Proto()

			assert.Equal(t, testCase.expectedProto.PodGroup, computedProto.PodGroup)

			if testCase.expectedProto.RejectedMigs == nil {
				assert.Nil(t, computedProto.RejectedMigs)
			} else {
				assert.ElementsMatch(t, testCase.expectedProto.RejectedMigs, computedProto.RejectedMigs)
			}

			if testCase.expectedProto.NapFailureReasons == nil {
				assert.Nil(t, computedProto.NapFailureReasons)
			} else {
				assert.ElementsMatch(t, testCase.expectedProto.NapFailureReasons, computedProto.NapFailureReasons)
			}
		})
	}
}
