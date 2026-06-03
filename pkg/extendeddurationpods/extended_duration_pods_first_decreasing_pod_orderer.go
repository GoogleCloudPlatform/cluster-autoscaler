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

package extendeddurationpods

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

// ExtendedDurationPodsFirstDecreasingPodOrderer is an extension of estimator.EstimationPodOrderer
// which implements priority to EDPs to the ordering of pods for binpacking
// If no EDPs exists, then the Orderer falls back to being a DecreasingPodOrderer
type ExtendedDurationPodsFirstDecreasingPodOrderer struct {
	*estimator.DecreasingPodOrderer
}

// NewExtendedDurationPodsFirstDecreasingPodOrderer returns a new object for ExtendedDurationPodsFirstDecreasingPodOrderer which is implementing
// estimator.EstimationPodOrderer
func NewExtendedDurationPodsFirstDecreasingPodOrderer() *ExtendedDurationPodsFirstDecreasingPodOrderer {
	return &ExtendedDurationPodsFirstDecreasingPodOrderer{
		DecreasingPodOrderer: estimator.NewDecreasingPodOrderer()}
}

func (g *ExtendedDurationPodsFirstDecreasingPodOrderer) Order(podGroups []estimator.PodEquivalenceGroup, nodeTemplate *framework.NodeInfo, nodeGroup cloudprovider.NodeGroup) []estimator.PodEquivalenceGroup {
	if nodeTemplate.Node() == nil {
		return g.DecreasingPodOrderer.Order(podGroups, nodeTemplate, nodeGroup)
	}
	if _, f := nodeTemplate.Node().Labels[labels.ExtendedDurationPodsLabel]; !f {
		return g.DecreasingPodOrderer.Order(podGroups, nodeTemplate, nodeGroup)
	}

	// we'd like to process edps before "other pods", however, while doing so we'd
	// still want to process each set of pods with the original scoring which
	// is a critical part of binpacking
	extendedDurationPods, otherPods := filterExtendedDurationPodGroups(podGroups)
	// process edp pods
	processedEDPs := g.DecreasingPodOrderer.Order(extendedDurationPods, nodeTemplate, nodeGroup)
	// process other pods
	processedOtherPods := g.DecreasingPodOrderer.Order(otherPods, nodeTemplate, nodeGroup)
	// we are prioritising edps before other pods
	allProcessedPods := append(processedEDPs, processedOtherPods...)

	return allProcessedPods
}

// filterExtendedDurationPodGroups returns the split of pod groups into EDP and non-EDP slices
func filterExtendedDurationPodGroups(podGroups []estimator.PodEquivalenceGroup) ([]estimator.PodEquivalenceGroup, []estimator.PodEquivalenceGroup) {
	edps := []estimator.PodEquivalenceGroup{}
	others := []estimator.PodEquivalenceGroup{}

	for _, group := range podGroups {
		if edpSelector := EdpSelector(group.Exemplar()); edpSelector == "" {
			others = append(others, group)
			continue
		}
		edps = append(edps, group)
	}

	return edps, others
}
