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

package gke

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

type mockNodeGroupFilter struct {
	ownedMigs map[string]bool
	calls     int
}

func (m *mockNodeGroupFilter) IsNodeGroupOwned(nodeGroup cloudprovider.NodeGroup) bool {
	m.calls++
	return m.ownedMigs[nodeGroup.(*GkeMig).gceRef.Name]
}

type mockMigLister struct {
	migListerInterface
	migs    []gce.Mig
	gkeMigs []*GkeMig
}

func (m *mockMigLister) GetMigs() []gce.Mig {
	return m.migs
}

func (m *mockMigLister) GetGkeMigs() []*GkeMig {
	return m.gkeMigs
}

func buildGkeMig(name, zone string) *GkeMig {
	return NewGkeMig(gce.GceRef{Project: "proj", Zone: zone, Name: name}, "", nil)
}

func TestFilteredMigLister(t *testing.T) {
	stdMig := buildGkeMig("std-pool-1", "us-central1-a")
	solverMig := buildGkeMig("solver-pool-1", "us-central1-b")
	plainMig := buildGkeMig("plain-pool-1", "us-central1-c")

	mockBase := &mockMigLister{
		gkeMigs: []*GkeMig{stdMig, solverMig, plainMig},
		migs:    []gce.Mig{stdMig, solverMig, plainMig},
	}

	testCases := []struct {
		name      string
		ownedMigs map[string]bool
		wantNames []string
	}{
		{
			name:      "Solver CA instance filters MIGs",
			ownedMigs: map[string]bool{"solver-pool-1": true},
			wantNames: []string{"solver-pool-1"},
		},
		{
			name:      "Standard CA instance filters MIGs and includes plain MIGs",
			ownedMigs: map[string]bool{"std-pool-1": true, "plain-pool-1": true},
			wantNames: []string{"std-pool-1", "plain-pool-1"},
		},
		{
			name:      "Fallback mode instance includes all MIGs",
			ownedMigs: map[string]bool{"std-pool-1": true, "solver-pool-1": true, "plain-pool-1": true},
			wantNames: []string{"std-pool-1", "solver-pool-1", "plain-pool-1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filteredMig := NewFilteredMigLister(mockBase, &mockNodeGroupFilter{ownedMigs: tc.ownedMigs}, testAutoscalerNames)

			var gotNames []string
			for _, mig := range filteredMig.GetGkeMigs() {
				gotNames = append(gotNames, mig.gceRef.Name)
			}
			assert.ElementsMatch(t, tc.wantNames, gotNames)
		})
	}
}

func TestFilteredMigLister_OwnershipCache(t *testing.T) {
	migA := buildGkeMig("pool-a-z1", "us-central1-a")
	migB := buildGkeMig("pool-b-z1", "us-central1-b")

	base := &mockMigLister{
		gkeMigs: []*GkeMig{migA, migB},
		migs:    []gce.Mig{migA, migB},
	}
	mockFilter := &mockNodeGroupFilter{
		ownedMigs: map[string]bool{"pool-a-z1": true},
	}
	filteredMig := NewFilteredMigLister(base, mockFilter, testAutoscalerNames)

	// First GetGkeMigs evaluates both MIGs and populates migOwned.
	assert.Len(t, filteredMig.GetGkeMigs(), 1)
	assert.Equal(t, 2, mockFilter.calls)

	// Subsequent GetGkeMigs, GetMigs, and IsMigOwned calls hit the cache.
	assert.Len(t, filteredMig.GetGkeMigs(), 1)
	assert.Len(t, filteredMig.GetMigs(), 1)
	assert.True(t, filteredMig.IsMigOwned(migA))
	assert.False(t, filteredMig.IsMigOwned(migB))
	assert.Equal(t, 2, mockFilter.calls, "filter should not be called again before cache invalidation")

	// After InvalidateOwnershipCache, ownership is re-evaluated.
	mockFilter.ownedMigs["pool-b-z1"] = true
	filteredMig.InvalidateOwnershipCache()
	assert.Len(t, filteredMig.GetGkeMigs(), 2)
	assert.True(t, filteredMig.IsMigOwned(migB))
	assert.Equal(t, 4, mockFilter.calls)
}

var testAutoscalerNames = []string{"autoscaling.gke.io/standard"}

func TestNewFilteredMigLister_DisabledReturnsBase(t *testing.T) {
	base := &mockMigLister{}
	filter := &mockNodeGroupFilter{}
	assert.Same(t, base, NewFilteredMigLister(base, filter, nil), "empty autoscalerNames should disable filtering")
	assert.Same(t, base, NewFilteredMigLister(base, nil, testAutoscalerNames), "nil filter should disable filtering")
	assert.IsType(t, &FilteredMigLister{}, NewFilteredMigLister(base, filter, testAutoscalerNames))
}
