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

package noscaleup

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

func TestAddingPods(t *testing.T) {
	controller1 := &vistypes.PodController{Uid: "cuid1", Name: "c1", Kind: "ck1", ApiVersion: "v1"}
	controller2 := &vistypes.PodController{Uid: "cuid2", Name: "c2", Kind: "ck1", ApiVersion: "v1"}
	migReason11 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
		Reason: &vistypes.Message{Id: 0, Params: []string{"a"}},
	}
	migReason12 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
		Reason: &vistypes.Message{Id: 0, Params: []string{"a", "a"}},
	}
	migReason13 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
		Reason: &vistypes.Message{Id: 1, Params: []string{"a"}},
	}
	migReason21 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig2", Name: "mig2", NodePoolName: "np2", Zone: "z1"},
		Reason: &vistypes.Message{Id: 1, Params: []string{"a"}},
	}
	napReason1 := &vistypes.Message{Id: 2, Params: []string{"b"}}
	napReason2 := &vistypes.Message{Id: 2, Params: []string{"b", "b"}}
	napReason3 := &vistypes.Message{Id: 3, Params: []string{"b"}}
	skippedMigReason1 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
		Reason: &vistypes.Message{Id: 5, Params: []string{"s"}},
	}
	skippedMigReason2 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig2", Name: "mig2", NodePoolName: "np2", Zone: "z1"},
		Reason: &vistypes.Message{Id: 6, Params: []string{"s", "s"}},
	}

	pgm := NewPodGroupMap()

	// Add pods with controllers present. Pods from each for loop should end up in a separate pod group (8 for loops = 8 pod groups in total).

	// First pod group.
	for i := 0; i < 100; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid1" + strconv.Itoa(i), Controller: controller1}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason11}, []*vistypes.Message{napReason1}, nil)
	}
	// Different owner.
	for i := 0; i < 200; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid2" + strconv.Itoa(i), Controller: controller2}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason11}, []*vistypes.Message{napReason1}, nil)
	}
	// Different mig explanation (different reason id).
	for i := 0; i < 300; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid3" + strconv.Itoa(i), Controller: controller1}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason12}, []*vistypes.Message{napReason1}, nil)
	}
	// Different mig explanation (same reason id but different params).
	for i := 0; i < 400; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid4" + strconv.Itoa(i), Controller: controller1}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason13}, []*vistypes.Message{napReason1}, nil)
	}
	// Different number of mig explanations.
	for i := 0; i < 500; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid5" + strconv.Itoa(i), Controller: controller1}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason11, "mig2": migReason21}, []*vistypes.Message{napReason1}, nil)
	}
	// Different nap explanation (different reason id).
	for i := 0; i < 600; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid6" + strconv.Itoa(i), Controller: controller1}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason11}, []*vistypes.Message{napReason2}, nil)
	}
	// Different nap explanation (same reason id but different params).
	for i := 0; i < 700; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid7" + strconv.Itoa(i), Controller: controller1}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason11}, []*vistypes.Message{napReason3}, nil)
	}
	// Different number of nap explanations.
	for i := 0; i < 800; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid8" + strconv.Itoa(i), Controller: controller1}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason11}, []*vistypes.Message{napReason1, napReason2}, nil)
	}
	// Different skipped migs (one skipped MIG).
	for i := 0; i < 150; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid9" + strconv.Itoa(i), Controller: controller1}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason11}, []*vistypes.Message{napReason1}, map[string]*vistypes.MigExplanation{"mig1": skippedMigReason1})
	}
	// Different skipped migs (multiple skipped MIGs).
	for i := 0; i < 250; i++ {
		pod := &vistypes.Pod{Name: "pod" + strconv.Itoa(i), Uid: "pid10" + strconv.Itoa(i), Controller: controller1}
		pgm.AddPod(pod, map[string]*vistypes.MigExplanation{"mig1": migReason11}, []*vistypes.Message{napReason1}, map[string]*vistypes.MigExplanation{"mig1": skippedMigReason1, "mig2": skippedMigReason2})
	}

	// Add pods without a controller. Each added pod should constitute its own pod group (3 pods = 3 pod groups in total).
	individualPod1 := &vistypes.Pod{Name: "pod1", Uid: "pid1"}
	individualPod2 := &vistypes.Pod{Name: "pod2", Uid: "pid2"}
	individualPod3 := &vistypes.Pod{Name: "pod3", Uid: "pid3"}
	pgm.AddPod(individualPod1, map[string]*vistypes.MigExplanation{"mig1": migReason11}, []*vistypes.Message{napReason1}, nil)
	pgm.AddPod(individualPod2, map[string]*vistypes.MigExplanation{"mig1": migReason11}, []*vistypes.Message{napReason1}, nil)
	pgm.AddPod(individualPod3, map[string]*vistypes.MigExplanation{"mig1": migReason12}, []*vistypes.Message{napReason1}, nil)

	// Check that correct pod groups are emitted by the structure.
	expectedGroups := map[string]*vistypes.PodGroupExplanation{
		"gid1": {
			SamplePod:  &vistypes.Pod{Name: "pod0", Uid: "pid10", Controller: controller1},
			PodCount:   100,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason11},
			NapReasons: []*vistypes.Message{napReason1},
		},
		"gid2": {
			SamplePod:  &vistypes.Pod{Name: "pod0", Uid: "pid20", Controller: controller2},
			PodCount:   200,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason11},
			NapReasons: []*vistypes.Message{napReason1},
		},
		"gid3": {
			SamplePod:  &vistypes.Pod{Name: "pod0", Uid: "pid30", Controller: controller1},
			PodCount:   300,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason12},
			NapReasons: []*vistypes.Message{napReason1},
		},
		"gid4": {
			SamplePod:  &vistypes.Pod{Name: "pod0", Uid: "pid40", Controller: controller1},
			PodCount:   400,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason13},
			NapReasons: []*vistypes.Message{napReason1},
		},
		"gid5": {
			SamplePod:  &vistypes.Pod{Name: "pod0", Uid: "pid50", Controller: controller1},
			PodCount:   500,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason11, "mig2": migReason21},
			NapReasons: []*vistypes.Message{napReason1},
		},
		"gid6": {
			SamplePod:  &vistypes.Pod{Name: "pod0", Uid: "pid60", Controller: controller1},
			PodCount:   600,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason11},
			NapReasons: []*vistypes.Message{napReason2},
		},
		"gid7": {
			SamplePod:  &vistypes.Pod{Name: "pod0", Uid: "pid70", Controller: controller1},
			PodCount:   700,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason11},
			NapReasons: []*vistypes.Message{napReason3},
		},
		"gid8": {
			SamplePod:  &vistypes.Pod{Name: "pod0", Uid: "pid80", Controller: controller1},
			PodCount:   800,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason11},
			NapReasons: []*vistypes.Message{napReason1, napReason2},
		},
		"gid12": {
			SamplePod:         &vistypes.Pod{Name: "pod0", Uid: "pid90", Controller: controller1},
			PodCount:          150,
			MigReasons:        map[string]*vistypes.MigExplanation{"mig1": migReason11},
			NapReasons:        []*vistypes.Message{napReason1},
			SkippedMigReasons: map[string]*vistypes.MigExplanation{"mig1": skippedMigReason1},
		},
		"gid13": {
			SamplePod:         &vistypes.Pod{Name: "pod0", Uid: "pid100", Controller: controller1},
			PodCount:          250,
			MigReasons:        map[string]*vistypes.MigExplanation{"mig1": migReason11},
			NapReasons:        []*vistypes.Message{napReason1},
			SkippedMigReasons: map[string]*vistypes.MigExplanation{"mig1": skippedMigReason1, "mig2": skippedMigReason2},
		},
		"gid9": {
			SamplePod:  &vistypes.Pod{Name: "pod1", Uid: "pid1"},
			PodCount:   1,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason11},
			NapReasons: []*vistypes.Message{napReason1},
		},
		"gid10": {
			SamplePod:  &vistypes.Pod{Name: "pod2", Uid: "pid2"},
			PodCount:   1,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason11},
			NapReasons: []*vistypes.Message{napReason1},
		},
		"gid11": {
			SamplePod:  &vistypes.Pod{Name: "pod3", Uid: "pid3"},
			PodCount:   1,
			MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason12},
			NapReasons: []*vistypes.Message{napReason1},
		},
	}
	groups := pgm.GetPodGroups()

	assert.Equal(t, len(expectedGroups), len(groups))
	for _, group := range groups {
		var expectedGroupId string
		expectedGroupFound := false

		for groupId, expectedGroup := range expectedGroups {
			if !assert.ObjectsAreEqual(expectedGroup.NapReasons, group.NapReasons) {
				continue
			}
			if !assert.ObjectsAreEqual(expectedGroup.MigReasons, group.MigReasons) {
				continue
			}
			if !assert.ObjectsAreEqual(expectedGroup.SkippedMigReasons, group.SkippedMigReasons) {
				continue
			}
			if !assert.ObjectsAreEqual(expectedGroup.PodCount, group.PodCount) {
				continue
			}
			if !assert.ObjectsAreEqual(expectedGroup.SamplePod, group.SamplePod) {
				continue
			}
			expectedGroupFound = true
			expectedGroupId = groupId
			break
		}

		if expectedGroupFound {
			delete(expectedGroups, expectedGroupId)
		} else {
			assert.Fail(t, "Unexpected pod group: %v", group)
		}
	}
}

func TestComputePodGroupExplanationSignature(t *testing.T) {
	controller := &vistypes.PodController{Uid: "controllerUID", Name: "c1", Kind: "ck1", ApiVersion: "v1"}
	podWithController := &vistypes.Pod{Name: "podWithController", Uid: "podWithControllerUID", Controller: controller}
	podWithoutController := &vistypes.Pod{Name: "podWithoutController", Uid: "podWithoutControllerUID"}

	napReason1 := &vistypes.Message{Id: 1}
	napReason2 := &vistypes.Message{Id: 2, Params: []string{"a", "b"}}

	migReason1 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
		Reason: &vistypes.Message{Id: 3},
	}
	migReason2 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig2", Name: "mig2", NodePoolName: "np2", Zone: "z1"},
		Reason: &vistypes.Message{Id: 4, Params: []string{"c", "d"}},
	}

	skippedMigReason1 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
		Reason: &vistypes.Message{Id: 5},
	}
	skippedMigReason2 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig2", Name: "mig2", NodePoolName: "np2", Zone: "z1"},
		Reason: &vistypes.Message{Id: 6, Params: []string{"e", "f"}},
	}

	for _, testCase := range []struct {
		name              string
		pod               *vistypes.Pod
		migReasons        map[string]*vistypes.MigExplanation
		skippedMigs       map[string]*vistypes.MigExplanation
		napReasons        []*vistypes.Message
		expectedSignature vistypes.PodGroupExplanationSignature
	}{
		{
			name: "pod without controller",
			pod:  podWithoutController,
			expectedSignature: vistypes.PodGroupExplanationSignature{
				ControllerOrPodUid: "podWithoutControllerUID",
			},
		},
		{
			name: "pod with controller",
			pod:  podWithController,
			expectedSignature: vistypes.PodGroupExplanationSignature{
				ControllerOrPodUid: "controllerUID",
			},
		},
		{
			name:       "pod with controller + reasons",
			pod:        podWithController,
			migReasons: map[string]*vistypes.MigExplanation{"mig2": migReason2, "mig1": migReason1},
			napReasons: []*vistypes.Message{napReason1, napReason2},
			expectedSignature: vistypes.PodGroupExplanationSignature{
				ControllerOrPodUid:  "controllerUID",
				MigReasonsSignature: "mig1,3\nmig2,4,c,d\n",
				NapReasonsSignature: "1\n2,a,b\n",
			},
		},
		{
			name:        "pod with controller + reasons + skipped reasons",
			pod:         podWithController,
			migReasons:  map[string]*vistypes.MigExplanation{"mig2": migReason2, "mig1": migReason1},
			skippedMigs: map[string]*vistypes.MigExplanation{"mig2": skippedMigReason2, "mig1": skippedMigReason1},
			napReasons:  []*vistypes.Message{napReason1, napReason2},
			expectedSignature: vistypes.PodGroupExplanationSignature{
				ControllerOrPodUid:   "controllerUID",
				MigReasonsSignature:  "mig1,3\nmig2,4,c,d\n",
				NapReasonsSignature:  "1\n2,a,b\n",
				SkippedMigsSignature: "mig1,5\nmig2,6,e,f\n",
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pg := vistypes.PodGroupExplanation{
				SamplePod:         testCase.pod,
				MigReasons:        testCase.migReasons,
				NapReasons:        testCase.napReasons,
				SkippedMigReasons: testCase.skippedMigs,
			}
			assert.Equal(t, testCase.expectedSignature, pg.Signature())
		})
	}
}
