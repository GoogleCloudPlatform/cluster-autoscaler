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

// NodePoolsRule is an interface for rules with nodepools.
type NodePoolsRule interface {
	BaseRule
	NodePoolNames() []string
}

type nodePoolsRule struct {
	nodePoolNames []string
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *nodePoolsRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}
	for _, name := range r.nodePoolNames {
		if mig.NodePoolName() == name {
			return true
		}
	}
	return false
}

// NodePoolNames returns names of nodepools.
func (r *nodePoolsRule) NodePoolNames() []string {
	return r.nodePoolNames
}

// WithNodePoolsRule returns RuleOption adding NodePoolsRule.
func WithNodePoolsRule(names []string) RuleOption {
	return func(r *rule) {
		r.nodePoolsRule = nodePoolsRule{nodePoolNames: names}
	}
}
