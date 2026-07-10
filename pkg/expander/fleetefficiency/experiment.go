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

package fleetefficiency

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
)

// IsFleetEfficiencyEnabled checks if the fleet efficiency strategy is enabled.
func IsFleetEfficiencyEnabled(gceFlexAdvisorEnabled bool, experimentsManager experiments.Manager) bool {
	if !gceFlexAdvisorEnabled {
		return false
	}
	if experimentsManager == nil {
		return false
	}
	if !flexadvisor.IsFlexAdvisorProcessingEnabled(experimentsManager) {
		return false
	}
	return experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FleetEfficiencyStrategyMinCAVersionFlag, true) &&
		experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.FleetEfficiencyStrategyEnabledFlag, true)
}
