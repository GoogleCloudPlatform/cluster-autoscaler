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
	"testing"
	"time"

	ccapiv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	quota "k8s.io/apiserver/pkg/quota/v1"
)

func TestMatchCapacityBuffers(t *testing.T) {
	oldTime := time.Now().Add(-1 * time.Hour)
	newTime := time.Now().Add(-1 * time.Minute)

	limits1 := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
	limits2 := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}

	testCases := []struct {
		name       string
		desired    []ccapiv1.ComputeClassBuffer
		candidates []internalCapacityBuffer
		validate   func(t *testing.T, pairs []bufferMatchPair)
	}{
		{
			name:       "empty inputs",
			desired:    nil,
			candidates: nil,
			validate: func(t *testing.T, pairs []bufferMatchPair) {
				if len(pairs) != 0 {
					t.Errorf("expected 0 pairs, got %d", len(pairs))
				}
			},
		},
		{
			name: "exact soft matching",
			// Both match exact specs, verify old is picked up for step 1 matching.
			desired: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: "Spot", Limits: limits1},
			},
			candidates: []internalCapacityBuffer{
				{Name: "c-new", ProvisioningStrategy: "Spot", Limits: limits1, CreationTimestamp: newTime},
				{Name: "c-old", ProvisioningStrategy: "Spot", Limits: limits1, CreationTimestamp: oldTime},
			},
			validate: func(t *testing.T, pairs []bufferMatchPair) {
				if len(pairs) != 2 {
					t.Fatalf("expected 2 pairs (1 matched, 1 orphaned), got %d", len(pairs))
				}

				var matched *bufferMatchPair
				var orphaned *bufferMatchPair
				for i := range pairs {
					if pairs[i].Desired != nil && pairs[i].Actual != nil {
						matched = &pairs[i]
					}
					if pairs[i].Desired == nil && pairs[i].Actual != nil {
						orphaned = &pairs[i]
					}
				}

				if matched == nil || matched.Actual.Name != "c-old" {
					t.Errorf("expected c-old to match desired buffer")
				}
				if orphaned == nil || orphaned.Actual.Name != "c-new" {
					t.Errorf("expected c-new to be orphaned")
				}
			},
		},
		{
			name: "soft matching fallback",
			// limits do not match, so it falls back to soft matching. Pick the oldest soft match.
			desired: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: "Spot", Limits: limits2}, // No exact match possible
			},
			candidates: []internalCapacityBuffer{
				{Name: "c-new", ProvisioningStrategy: "Spot", Limits: limits1, CreationTimestamp: newTime},
				{Name: "c-old", ProvisioningStrategy: "Spot", Limits: limits1, CreationTimestamp: oldTime},
			},
			validate: func(t *testing.T, pairs []bufferMatchPair) {
				var matched *bufferMatchPair
				var orphaned *bufferMatchPair
				for i := range pairs {
					if pairs[i].Desired != nil && pairs[i].Actual != nil {
						matched = &pairs[i]
					}
					if pairs[i].Desired == nil && pairs[i].Actual != nil {
						orphaned = &pairs[i]
					}
				}

				if matched == nil || matched.Actual.Name != "c-old" {
					t.Errorf("expected c-old to match via step 2")
				}
				if orphaned == nil || orphaned.Actual.Name != "c-new" {
					t.Errorf("expected c-new to be orphaned")
				}
			},
		},
		{
			name: "exact and soft combined",
			desired: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: "Spot", Limits: limits1}, // EXACT MATCH AVAILABLE
				{ProvisioningStrategy: "Spot", Limits: limits2}, // NO EXACT MATCH
			},
			candidates: []internalCapacityBuffer{
				{Name: "c-exact-new", ProvisioningStrategy: "Spot", Limits: limits1, CreationTimestamp: newTime},
				{Name: "c-exact-old", ProvisioningStrategy: "Spot", Limits: limits1, CreationTimestamp: oldTime},
				{Name: "c-soft-old", ProvisioningStrategy: "Spot", Limits: nil, CreationTimestamp: oldTime},
			},
			validate: func(t *testing.T, pairs []bufferMatchPair) {
				var desiredLimits1Match, desiredLimits2Match, orphan *internalCapacityBuffer

				for _, p := range pairs {
					if p.Desired != nil && quota.Equals(p.Desired.Limits, limits1) {
						desiredLimits1Match = p.Actual
					}
					if p.Desired != nil && quota.Equals(p.Desired.Limits, limits2) {
						desiredLimits2Match = p.Actual
					}
					if p.Desired == nil && p.Actual != nil {
						orphan = p.Actual
					}
				}

				if desiredLimits1Match == nil || desiredLimits1Match.Name != "c-exact-old" {
					t.Errorf("limits1 desired should exact match oldest exact candidate c-exact-old")
				}

				if desiredLimits2Match == nil || desiredLimits2Match.Name != "c-soft-old" {
					t.Errorf("limits2 desired should soft match the oldest available candidate c-soft-old")
				}

				if orphan == nil || orphan.Name != "c-exact-new" {
					t.Errorf("expected c-exact-new to be orphaned")
				}
			},
		},
		{
			name: "no match creates entirely orphaned actuals and nil actual desired",
			desired: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: "Spot", Limits: limits1},
			},
			candidates: []internalCapacityBuffer{
				{Name: "c-standard", ProvisioningStrategy: "Standard", Limits: limits1},
			},
			validate: func(t *testing.T, pairs []bufferMatchPair) {
				if len(pairs) != 2 {
					t.Fatalf("expected 2 pairs (1 creation, 1 deletion), got %d", len(pairs))
				}
				for _, p := range pairs {
					if p.Desired != nil && p.Actual != nil {
						t.Errorf("expected no successful matches")
					}
				}
			},
		},
		{
			name: "duplicate candidate names properly matches one and orphans the duplicate",
			desired: []ccapiv1.ComputeClassBuffer{
				{ProvisioningStrategy: "Spot", Limits: limits1},
			},
			candidates: []internalCapacityBuffer{
				{Name: "c-dup", ProvisioningStrategy: "Spot", Limits: limits1, CreationTimestamp: oldTime},
				{Name: "c-dup", ProvisioningStrategy: "Spot", Limits: limits1, CreationTimestamp: newTime},
			},
			validate: func(t *testing.T, pairs []bufferMatchPair) {
				if len(pairs) != 2 {
					t.Fatalf("expected 2 pairs (1 matched, 1 orphan), got %d", len(pairs))
				}
				var matched, orphan *internalCapacityBuffer
				for _, p := range pairs {
					if p.Desired != nil && p.Actual != nil {
						matched = p.Actual
					}
					if p.Desired == nil && p.Actual != nil {
						orphan = p.Actual
					}
				}
				if matched == nil || matched.CreationTimestamp != oldTime {
					t.Errorf("expected oldest candidate to match")
				}
				if orphan == nil || orphan.CreationTimestamp != newTime {
					t.Errorf("expected duplicate candidate to be orphaned")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := matchCapacityBuffers(tc.desired, tc.candidates)
			tc.validate(t, res)
		})
	}
}
