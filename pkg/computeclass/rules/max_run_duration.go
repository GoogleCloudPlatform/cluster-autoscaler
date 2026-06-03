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
	"fmt"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/klog/v2"
)

// MaxRunDurationRule is an interface for rules with max run duration.
type MaxRunDurationRule interface {
	BaseRule
	MaxRunDurationSeconds() *int
}

type maxRunDurationRule struct {
	maxRunDurationSeconds *int
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *maxRunDurationRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}
	return r.maxRunDurationSeconds == nil || mig.Spec().MaxRunDurationInSeconds == fmt.Sprintf("%d", *r.maxRunDurationSeconds)
}

// MaxRunDurationSeconds return MaxRunDurationSeconds of rule.
func (r *maxRunDurationRule) MaxRunDurationSeconds() *int {
	return r.maxRunDurationSeconds
}

// WithMaxRunDurationRule returns RuleOption adding MaxRunDurationRule.
func WithMaxRunDurationRule(maxRunDurationSeconds *int) RuleOption {
	return func(r *rule) {
		r.maxRunDurationRule.maxRunDurationSeconds = maxRunDurationSeconds
	}
}
