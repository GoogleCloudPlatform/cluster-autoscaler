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

func TestMachineConfigurationCacheUpdate(t *testing.T) {
	cache := newMachineConfigurationCache()
	newFamilies := map[string]MachineFamily{
		"A2":         A2,
		"new family": {name: "new family"},
	}

	assert.True(t, len(cache.machineFamilies()) > 2)
	cache.update(newFamilies)

	newCachedFamilies := cache.machineFamilies()
	assert.True(t, len(newCachedFamilies) == 2)

	a2, found := newCachedFamilies["A2"]
	assert.True(t, found)
	assert.Equal(t, A2, a2)

	newFamily, found := newCachedFamilies["new family"]
	assert.True(t, found)
	assert.Equal(t, MachineFamily{name: "new family"}, newFamily)
}
