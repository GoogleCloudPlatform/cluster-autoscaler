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

package capacitybuffers

import (
	"slices"

	ccapiv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	quota "k8s.io/apiserver/pkg/quota/v1"
)

// bufferMatchPair pairs a desired specification slot with an existing or in-flight buffer representation.
// If Desired is nil, Actual represents an extraneous candidate that should be deleted or evicted.
// If Actual is nil, Desired represents a slot that needs a new CapacityBuffer created via API.
type bufferMatchPair struct {
	Desired *ccapiv1.ComputeClassBuffer
	Actual  *internalCapacityBuffer
}

// matchCapacityBuffers matches desired buffers to the existing / being created candidate buffers.
func matchCapacityBuffers(
	desired []ccapiv1.ComputeClassBuffer,
	candidates []internalCapacityBuffer,
) []bufferMatchPair {
	// desiredToCandidates maps an index in the desired array to a pointer to selected candidate
	desiredToCandidates := make([]*internalCapacityBuffer, len(desired))
	// usedCandidates stores whether candidate at index j is already matched with a desired buffer
	usedCandidates := make([]bool, len(candidates))

	// prioritize oldest buffers when matching
	slices.SortStableFunc(candidates, func(a, b internalCapacityBuffer) int {
		return a.CreationTimestamp.Compare(b.CreationTimestamp)
	})

	matchBuffers := func(matchCondition func(desired *ccapiv1.ComputeClassBuffer, candidate *internalCapacityBuffer) bool) {
		for i := range desired {
			if desiredToCandidates[i] != nil {
				continue
			}
			buf := &desired[i]
			for j := range candidates {
				if usedCandidates[j] {
					continue
				}
				rep := &candidates[j]
				if matchCondition(buf, rep) {
					desiredToCandidates[i] = rep
					usedCandidates[j] = true
					break
				}
			}
		}
	}

	// Step 1: Exact content matching (ProvisioningStrategy + exact resource limits match)
	matchBuffers(func(d *ccapiv1.ComputeClassBuffer, c *internalCapacityBuffer) bool {
		return c.ProvisioningStrategy == d.ProvisioningStrategy && quota.Equals(c.Limits, d.Limits)
	})

	// Step 2: Soft matching (Same ProvisioningStrategy across remaining unpaired slots)
	matchBuffers(func(d *ccapiv1.ComputeClassBuffer, c *internalCapacityBuffer) bool {
		return c.ProvisioningStrategy == d.ProvisioningStrategy
	})

	pairs := make([]bufferMatchPair, 0, len(desired)+len(candidates))
	// capture all matched pairs and those desired buffers that had no candidate selected for them
	for i := range desired {
		pairs = append(pairs, bufferMatchPair{
			Desired: &desired[i],
			Actual:  desiredToCandidates[i],
		})
	}

	// capture all candidates that do not match to any desired buffer
	for j := range candidates {
		if !usedCandidates[j] {
			pairs = append(pairs, bufferMatchPair{
				Desired: nil,
				Actual:  &candidates[j],
			})
		}
	}

	return pairs
}
