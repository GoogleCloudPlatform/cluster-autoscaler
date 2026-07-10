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
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

// AllocationStrategyRule is an interface for rules with an AllocationStrategy.
// It is not a constraint, but exposes the strategy to other components.
type AllocationStrategyRule interface {
	BaseRule
	AllocationStrategy() *v1.AllocationStrategy
}

type allocationStrategyRule struct {
	strategy *v1.AllocationStrategy
}

// Matches returns true. AllocationStrategy is not a constraint.
func (r *allocationStrategyRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	return true
}

// AllocationStrategy returns the AllocationStrategy of the rule.
func (r *allocationStrategyRule) AllocationStrategy() *v1.AllocationStrategy {
	return r.strategy
}

// WithAllocationStrategyRule returns RuleOption adding AllocationStrategyRule.
func WithAllocationStrategyRule(strategy *v1.AllocationStrategy) RuleOption {
	return func(r *rule) {
		r.allocationStrategyRule.strategy = strategy
	}
}
