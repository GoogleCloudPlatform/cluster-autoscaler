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

package interfaces

// ExperimentFlagCache caches the experiment flag for a length of a CA loop.
// The value is guaranteed to stay consistent across CA main loop (excluding async code outside of the main loop).
type ExperimentFlagCache[T bool | string] interface {
	RefreshValue()
	Get() T
}

type ResizableVmAutoprovisioningProvider interface {
	IsResizableVmEnabledInAutopilot(machineFamily string) bool
	ResizingEnabled(machineFamily string) bool
	IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool
	IsExtendedFallbacksEnabled() bool
	Refresh()
	NodesCount(machineFamily string) int
	HasActiveResizableNodes() bool
}
