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

// LabelsRule is an interface for rules with labels.
type LabelsRule interface {
	BaseRule
	UserDefinedLabels() map[string]string
}

type labelsRule struct {
	labels map[string]string
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *labelsRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	for key, value := range r.labels {
		if mig.Spec().Labels[key] != value {
			return false
		}
	}
	return true
}

// UserDefinedLabels returns labels defined in priority.
func (r *labelsRule) UserDefinedLabels() map[string]string {
	return r.labels
}

// WithLabelsRule returns RuleOption adding LabelsRule.
func WithLabelsRule(labels map[string]string) RuleOption {
	return func(r *rule) {
		r.labelsRule.labels = labels
	}
}
