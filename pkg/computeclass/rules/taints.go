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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/klog/v2"
)

// TaintsRule is an interface for rules with taints.
type TaintsRule interface {
	BaseRule
	UserDefinedTaints() []apiv1.Taint
}

type taintsRule struct {
	taints []apiv1.Taint
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *taintsRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	for _, t := range r.taints {
		foundTaint := false
		for _, taint := range mig.Spec().Taints {
			if t.Key == taint.Key && t.Value == taint.Value && t.Effect == taint.Effect {
				foundTaint = true
				break
			}
		}
		if !foundTaint {
			return false
		}
	}
	return true
}

// UserDefinedTaints returns taints defined in priority.
func (r *taintsRule) UserDefinedTaints() []apiv1.Taint {
	return r.taints
}

// WithTaintsRule returns RuleOption adding TaintsRule.
func WithTaintsRule(taints []apiv1.Taint) RuleOption {
	return func(r *rule) {
		r.taintsRule.taints = taints
	}
}
