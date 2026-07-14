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

package machinetypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSource_updateCache(t *testing.T) {
	s := &Source{
		mfCache:     make(map[string]MachineFamily),
		updateCount: 0,
	}

	familyA := NewTestMachineFamily("familyA", nil, UnknownPlatform, UnknownPlatform, nil, nil)
	identicalFamilyA := NewTestMachineFamily("familyA", nil, UnknownPlatform, UnknownPlatform, nil, nil)

	// Update 1: Adding a new family should increment counter
	s.updateCache(familyA, "v1")
	_, count1 := s.Snapshot()
	assert.Equal(t, uint64(1), count1, "Adding a new family should increment counter")

	// Update 2: Identical update should NOT increment counter
	s.updateCache(identicalFamilyA, "v1")
	_, count2 := s.Snapshot()
	assert.Equal(t, uint64(1), count2, "Identical update should be dropped")

	// Update 3: Mutating an existing family should increment counter
	mutatedFamilyA := NewTestMachineFamily("familyA", nil, UnknownPlatform, UnknownPlatform, nil, []string{"pd-ssd"})
	s.updateCache(mutatedFamilyA, "v2")
	_, count3 := s.Snapshot()
	assert.Equal(t, uint64(2), count3, "Structural mutation should increment counter")

	// Update 4: Removing a non-existent family should NOT increment counter
	s.removeFromCache("familyB", "v1")
	_, count4 := s.Snapshot()
	assert.Equal(t, uint64(2), count4, "Removing non-existent family should be dropped")

	// Update 5: Removing an existing family should increment counter
	s.removeFromCache("familyA", "v2")
	_, count5 := s.Snapshot()
	assert.Equal(t, uint64(3), count5, "Removing existing family should increment counter")
}
