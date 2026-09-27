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

package processor

import (
	"slices"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

type workloadSeparationLimiter struct {
	resizableVmLimiters map[string]resizableVmLimitProvider
}

type workloadIDRequestsPair struct {
	workloadID string
	resources  apiv1.ResourceList
}

type resizableVmLimitProvider interface {
	provide() int
}

func NewWorkloadSeparationLimiter(experimentsManager experiments.Manager, laWorkloadSeparationsConfigFlags map[string]int, experimentFlags map[string]string, componentVersion version.Version) *workloadSeparationLimiter {
	resizableVmLimiters := map[string]resizableVmLimitProvider{}
	for family, defaultLimit := range laWorkloadSeparationsConfigFlags {
		resizableVmLimiters[family] = newProvider(experimentsManager, experimentFlags[family], defaultLimit, componentVersion)
	}

	return &workloadSeparationLimiter{resizableVmLimiters}
}

// Limit filters the workload separation requests per machine family to the top N by CPU,
// always preserving the default workload ID ("").
func (w *workloadSeparationLimiter) Limit(requestsByFamily map[string]map[string]apiv1.ResourceList) map[string]map[string]apiv1.ResourceList {
	limitedRequests := map[string]map[string]apiv1.ResourceList{}
	for machineFamily, requests := range requestsByFamily {
		limitProvider, ok := w.resizableVmLimiters[machineFamily]
		limit := 0
		if ok {
			limit = limitProvider.provide()
		}
		limitedRequests[machineFamily] = limitRequestsForFamily(requests, limit)
	}
	return limitedRequests
}

func limitRequestsForFamily(requests map[string]apiv1.ResourceList, limit int) map[string]apiv1.ResourceList {
	defaultWID, defaultExists := requests[""]
	delete(requests, "")

	// TODO(b/421106616): Set of workload IDs with lookahead is recomputed every loop. A cluster
	// with more workload IDs than `maxWorkloadSeparations` might have some groups moving between having lookahead and not having it.
	// This could lead to extra node churn. This is an edge-case and probably not worth handling right now.
	requests = selectLargestRequests(requests, limit)

	if defaultExists {
		// Add default workload ID back, if it existed in the first place.
		requests[""] = defaultWID
	}
	return requests
}

func selectLargestRequests(requests map[string]apiv1.ResourceList, limit int) map[string]apiv1.ResourceList {
	if len(requests) <= limit {
		result := make(map[string]apiv1.ResourceList, len(requests))
		for k, v := range requests {
			result[k] = v
		}
		return result
	}

	pairs := make([]workloadIDRequestsPair, 0, len(requests))
	for k, v := range requests {
		pairs = append(pairs, workloadIDRequestsPair{k, v})
	}
	slices.SortFunc(pairs, func(a, b workloadIDRequestsPair) int {
		aCpu := a.resources.Cpu().MilliValue()
		bCpu := b.resources.Cpu().MilliValue()
		if aCpu != bCpu {
			return int(aCpu - bCpu)
		}
		return strings.Compare(b.workloadID, a.workloadID)
	})
	slices.Reverse(pairs)
	topRequests := pairs[:min(limit, len(pairs))]
	limited := make(map[string]apiv1.ResourceList, len(topRequests))
	for _, pair := range topRequests {
		limited[pair.workloadID] = pair.resources
	}

	return limited
}
