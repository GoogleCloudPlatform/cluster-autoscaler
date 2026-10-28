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

package rules

import (
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

// MinEstimatedUptimeRule is an interface for rules with minimum estimated Spot uptime.
// It is not a node group matching constraint, but exposes the setting to Flex Advisor.
type MinEstimatedUptimeRule interface {
	BaseRule
	MinEstimatedUptimeMinutes() *int
}

type minEstimatedUptimeRule struct {
	minEstimatedUptimeMinutes *int
}

// Matches returns true. MinEstimatedUptimeMinutes is not a matching constraint.
func (r *minEstimatedUptimeRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	return true
}

// MinEstimatedUptimeMinutes returns the minimum estimated uptime in minutes of the rule.
func (r *minEstimatedUptimeRule) MinEstimatedUptimeMinutes() *int {
	if r == nil {
		return nil
	}
	return r.minEstimatedUptimeMinutes
}

// WithMinEstimatedUptimeRule returns a RuleOption adding MinEstimatedUptimeRule.
func WithMinEstimatedUptimeRule(minEstimatedUptimeMinutes *int) RuleOption {
	return func(r *rule) {
		r.minEstimatedUptimeRule.minEstimatedUptimeMinutes = minEstimatedUptimeMinutes
	}
}
