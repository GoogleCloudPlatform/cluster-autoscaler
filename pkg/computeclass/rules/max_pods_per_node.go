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

// MaxPodsPerNodeRule is an interface for rules with max pods per node.
type MaxPodsPerNodeRule interface {
	BaseRule
	MaxPodsPerNode() int
}

type maxPodsPerNodeRule struct {
	maxPodsPerNode *int
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *maxPodsPerNodeRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	return r.maxPodsPerNode == nil || *r.maxPodsPerNode == int(mig.Spec().MaxPodsPerNode)
}

// MaxPodsPerNode returns max pods per node of rule.
func (r *maxPodsPerNodeRule) MaxPodsPerNode() int {
	if r.maxPodsPerNode == nil {
		return 0
	}
	return *r.maxPodsPerNode
}

// WithMaxPodsPerNodeRule returns RuleOption setting MaxPodsPerNodeRule.
func WithMaxPodsPerNodeRule(mppn *int) RuleOption {
	return func(r *rule) {
		r.maxPodsPerNodeRule.maxPodsPerNode = mppn
	}
}
