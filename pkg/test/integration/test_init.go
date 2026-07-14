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

// This file contains early initialization logic and workarounds required for the
// integration testing environment (like synctest) to function correctly.
package integration

import (
	"k8s.io/client-go/features"
)

type integrationFeatureGates struct {
	original features.Gates
}

func (i integrationFeatureGates) Enabled(key features.Feature) bool {
	if key == features.WatchListClient {
		// TODO: b/523096639 - Remove after regenerating our custom informers with updated generator
		// Disable client-go watchlist feature gate in integration tests.
		// Custom generated informers (e.g. UpdateInfo) do not use ToListWatcherWithWatchListSemantics,
		// so the Reflector cannot detect that the fake clientset (ObjectTracker) does not support
		// watchlist. This causes the reflector to wait forever for a bookmark event that is never sent,
		// causing flaky hangs in tests. Setting KUBE_FEATURE_WatchListClient=false forces fallback to list.
		return false
	}
	return i.original.Enabled(key)
}

func init() {
	// Because client-go caches its feature gates during its own initialization phase,
	// setting environment variables (like KUBE_FEATURE_WatchListClient=false) is often
	// too late to take effect. We use ReplaceFeatureGates to dynamically inject overrides.
	features.ReplaceFeatureGates(integrationFeatureGates{original: features.FeatureGates()})
}
