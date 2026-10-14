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

// PlannedLocationsProvider computes the locations that a node pool created from a not yet existing MIG would span.
type PlannedLocationsProvider interface {
	// PlannedNodePoolLocations returns the locations that a node pool created from the given (not yet existing)
	// MIG would span, using the same computation as node pool creation.
	PlannedNodePoolLocations(mig *GkeMig) ([]string, error)
}
