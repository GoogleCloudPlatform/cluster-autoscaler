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

// SelfServiceRule is an interface for rules with self-service.
type SelfServiceRule interface {
	BaseRule
	SelfServiceMetadata() map[string]string
}

type selfServiceRule struct {
	selfServiceMetadata map[string]string
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *selfServiceRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}
	for labelKey, labelValue := range r.selfServiceMetadata {
		if value, found := mig.Spec().SelfServiceMetadata[labelKey]; !found || labelValue != value {
			return false
		}
	}
	return true
}

func (r *selfServiceRule) SelfServiceMetadata() map[string]string {
	return r.selfServiceMetadata
}

// WithSelfServiceRule returns RuleOption adding SelfServiceRule.
func WithSelfServiceRule(selfServiceLabels map[string]string) RuleOption {
	return func(r *rule) {
		r.selfServiceRule.selfServiceMetadata = selfServiceLabels
	}
}
