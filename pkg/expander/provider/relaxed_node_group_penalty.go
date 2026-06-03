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

package provider

import "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"

type experimentRelaxedNodeGroupPenaltyChecker struct {
	experimentsManager experiments.Manager
	autopilotEnabled   bool
}

// NewRelaxedNodeGroupPenaltyChecker constructs a relaxed node group penalty checker
// which is dynamically configurable via a experiment.
func NewRelaxedNodeGroupPenaltyChecker(experimentsManager experiments.Manager, autopilotEnabled bool) *experimentRelaxedNodeGroupPenaltyChecker {
	return &experimentRelaxedNodeGroupPenaltyChecker{
		experimentsManager: experimentsManager,
		autopilotEnabled:   autopilotEnabled,
	}
}

// Enabled determines whether relaxed node group penalty should be used when scoring scale-up options.
func (c *experimentRelaxedNodeGroupPenaltyChecker) Enabled() bool {
	if c.autopilotEnabled {
		return true
	}
	if c.experimentsManager == nil {
		return false
	}
	return c.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.RelaxedNodeGroupCreationPenalty, false)
}
