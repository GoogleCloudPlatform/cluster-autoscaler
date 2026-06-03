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

package scaleup

import (
	"maps"
	"slices"
	"sort"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/equivalence"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	klog "k8s.io/klog/v2"
)

// ProvReqGroup embeds Pod equivalence groups with an ID of their respective ProvReq.
// Note: there can be multiple Pod equivalence groups for a single ProvReq only if it has multiple PodSets.
type ProvReqGroup struct {
	ID        pods.ProvReqID
	PodGroups []*equivalence.PodGroup
}

// PartialOption represents an expansion option for a particular ProvisioningRequest.
type PartialOption struct {
	ProvReqID pods.ProvReqID
	NodeCount int
	Pods      []*apiv1.Pod
}

// CompositeOption contains a cumulative expansion option with division by separate
// ProvisioningRequest suboptions.
type CompositeOption struct {
	expander.Option
	partialOptions []PartialOption
}

// buildProvReqGroups does two things with the Pod equivalence groups:
//  1. Groups all Pod equivalence groups by their respective Provisioning Request into
//     a single ProvReqGroup wrapper alongside the ID of their corresponding Provisioning Request.
//  2. It sorts the groups by the ProvisioningRequest's creation time,
//     so that the oldest ones are handled first in order to guarantee
//     fairness.
func buildProvReqGroups(podGroups []*equivalence.PodGroup) []*ProvReqGroup {
	resultMap := map[pods.ProvReqID]*ProvReqGroup{}
	for _, pg := range podGroups {
		// Equivalence groups building takes controller into account, so we can just use the first Pod, as they all will be from the same ProvReq.
		prName, ok := pods.ProvisioningRequestName(pg.Pods[0])
		if !ok {
			// This should not happen at all since we are calling this function
			// inside the ProvReq-specific orchestrator implementation only
			// where we already know that we're working on ProvReq-based Pod
			// equivalence groups but we check this  and ignore such pod groups
			// if, by any chance, the orchestrator logic changes in the future.
			klog.Errorf("Pod %s/%s is not owned by any ProvReq", pg.Pods[0].Namespace, pg.Pods[0].Name)
			continue
		}
		prNamespace := pg.Pods[0].GetNamespace()
		prID := pods.ProvReqID{Name: prName, Namespace: prNamespace}
		if resultMap[prID] == nil {
			resultMap[prID] = &ProvReqGroup{
				ID:        prID,
				PodGroups: []*equivalence.PodGroup{},
			}
		}
		resultMap[prID].PodGroups = append(resultMap[prID].PodGroups, pg)
	}

	result := slices.Collect(maps.Values(resultMap))
	sort.Slice(result, func(i, j int) bool {
		return result[i].PodGroups[0].Pods[0].GetCreationTimestamp().Compare(result[j].PodGroups[0].Pods[0].GetCreationTimestamp().Time) < 0
	})
	return result
}
