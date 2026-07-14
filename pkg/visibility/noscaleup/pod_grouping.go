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
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

// PodGroupMap can be used to group pods by their controller and explanation.
type PodGroupMap map[vistypes.PodGroupExplanationSignature]*vistypes.PodGroupExplanation

// AddPod adds pod with the specified reasons to the pod group structure. If there's no matching group
// in the structure already, a new group is added with this pod as an example. Otherwise, the pod count
// in the matching group is increased.
func (pgm PodGroupMap) AddPod(pod *vistypes.Pod, migReasons map[string]*vistypes.MigExplanation, napReasons []*vistypes.Message, skippedMigReasons map[string]*vistypes.MigExplanation) {
	expl := &vistypes.PodGroupExplanation{
		// The sample pod is currently always the first pod from the group. Perhaps there are other strategies worth considering?
		SamplePod:         pod,
		PodCount:          1,
		MigReasons:        migReasons,
		NapReasons:        napReasons,
		SkippedMigReasons: skippedMigReasons,
	}
	sig := expl.Signature()

	if _, found := pgm[sig]; !found {
		// This is the first pod from this group.
		pgm[sig] = expl
	} else {
		pgm[sig].PodCount++
	}
}

// GetPodGroups returns a list of all pod group explanations in the structure.
func (pgm PodGroupMap) GetPodGroups() []*vistypes.PodGroupExplanation {
	var result []*vistypes.PodGroupExplanation
	for _, podGroup := range pgm {
		result = append(result, podGroup)
	}
	return result
}

// NewPodGroupMap creates a new empty pod grouping structure.
func NewPodGroupMap() PodGroupMap {
	return make(PodGroupMap)
}
