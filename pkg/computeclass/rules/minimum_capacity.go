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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

// MinimumCapacityRule is an interface for rules with MinimumCapacity defined.
type MinimumCapacityRule interface {
	BaseRule
	TargetNodeCount() *int
}

type minimumCapacityRule struct {
	targetNodeCount *int
}

// Matches returns true if the nodegroup is matching.
// MinimumCapacity is not a matching constraint, so it always returns true.
func (r *minimumCapacityRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	return true
}

// TargetNodeCount returns the TargetNodeCount specified by rule.
func (r *minimumCapacityRule) TargetNodeCount() *int {
	if r == nil {
		return nil
	}
	return r.targetNodeCount
}

// WithTargetNodeCountRule returns RuleOption setting TargetNodeCount.
func WithTargetNodeCountRule(targetNodeCount *int) RuleOption {
	return func(r *rule) {
		r.minimumCapacityRule.targetNodeCount = targetNodeCount
	}
}
