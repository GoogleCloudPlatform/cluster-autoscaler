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

package fleetefficiency

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const GkeTopologyZoneLabel = "topology.gke.io/zone"

func isZonalKey(key string) bool {
	return key == apiv1.LabelTopologyZone || key == apiv1.LabelFailureDomainBetaZone || key == GkeTopologyZoneLabel
}

// zonalConstraint returns a human-readable description of the first zonal, topology or stateful constraint
// found on the pod, and whether any was found.
func zonalConstraint(pod *apiv1.Pod) (string, bool) {
	if pod == nil {
		return "", false
	}

	// 1. Zonal NodeSelector
	for k := range pod.Spec.NodeSelector {
		if isZonalKey(k) {
			return fmt.Sprintf("zonal node selector %q", k), true
		}
	}

	if affinity := pod.Spec.Affinity; affinity != nil {
		// 2. Zonal NodeAffinity
		if nodeAffinity := affinity.NodeAffinity; nodeAffinity != nil {
			if req := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution; req != nil {
				for _, term := range req.NodeSelectorTerms {
					for _, expr := range term.MatchExpressions {
						if isZonalKey(expr.Key) {
							return fmt.Sprintf("zonal node affinity %q", expr.Key), true
						}
					}
				}
			}
		}

		// 3. Zonal PodAffinity / PodAntiAffinity
		hasZonalTopologyKey := func(terms []apiv1.PodAffinityTerm) bool {
			for _, term := range terms {
				if isZonalKey(term.TopologyKey) {
					return true
				}
			}
			return false
		}
		if podAffinity := affinity.PodAffinity; podAffinity != nil && hasZonalTopologyKey(podAffinity.RequiredDuringSchedulingIgnoredDuringExecution) {
			return "pod affinity with zonal topology key", true
		}
		if podAntiAffinity := affinity.PodAntiAffinity; podAntiAffinity != nil && hasZonalTopologyKey(podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) {
			return "pod anti-affinity with zonal topology key", true
		}
	}

	// 4. Zonal TopologySpreadConstraints (only hard DoNotSchedule constraints restrict placement)
	for _, tsc := range pod.Spec.TopologySpreadConstraints {
		if tsc.WhenUnsatisfiable == apiv1.DoNotSchedule && isZonalKey(tsc.TopologyKey) {
			return "topology spread constraint with zonal topology key", true
		}
	}

	// 5. Stateful pod with persistent volume
	if podrequirements.IsPodStateful(pod) {
		return "stateful pod with persistent volume", true
	}

	return "", false
}
