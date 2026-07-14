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
	"k8s.io/klog/v2"
)

type PlacementPolicyRule interface {
	BaseRule
	PlacementPolicy() string
}

type placementPolicyRule struct {
	policy string
}

// Matches returns true if the nodegroup matches given policy.
func (p *placementPolicyRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}
	if p.policy == "" {
		return true
	}
	if mig.Spec() == nil {
		return false
	}

	return mig.Spec().PlacementGroup.Policy == p.policy
}

// PlacementPolicy returns the placement policy of rule.
func (p *placementPolicyRule) PlacementPolicy() string {
	return p.policy
}

// WithPlacementPolicyRule returns RuleOption adding PlacementPolicyRule.
func WithPlacementPolicyRule(policy string) RuleOption {
	return func(r *rule) {
		r.placementPolicyRule.policy = policy
	}
}
