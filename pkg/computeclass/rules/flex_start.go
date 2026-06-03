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

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/klog/v2"
)

// FlexStartRule is an interface for rules with Flex Start provisioning model.
type FlexStartRule interface {
	BaseRule
	FlexStartEnabled() bool
	FlexStartNodeRecyclingLeadTimeSeconds() *int
}

type nodeRecyclingConfig struct {
	leadTimeSeconds *int
}

type flexStartRule struct {
	enabled       bool
	nodeRecycling *nodeRecyclingConfig
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *flexStartRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	if !r.enabled {
		return true
	}

	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	ruleLeadTimeSeconds := ""
	if r.nodeRecycling != nil && r.nodeRecycling.leadTimeSeconds != nil {
		ruleLeadTimeSeconds = fmt.Sprintf("%d", *r.nodeRecycling.leadTimeSeconds)
	}
	migLeadTimeSeconds := ""
	if val, found := mig.Spec().Labels[labels.NodeRecycleLeadTimeSecondsLabelKey]; found {
		migLeadTimeSeconds = val
	}

	return mig.Spec().FlexStart == r.enabled && ruleLeadTimeSeconds == migLeadTimeSeconds
}

// FlexStartEnabled returns FlexStartEnabled of rule.
func (r *flexStartRule) FlexStartEnabled() bool {
	return r.enabled
}

// FlexStartEnabled returns NodeRecycling LeadTimeSeconds of rule.
func (r *flexStartRule) FlexStartNodeRecyclingLeadTimeSeconds() *int {
	if r.nodeRecycling == nil || r.nodeRecycling.leadTimeSeconds == nil {
		return nil
	}
	return r.nodeRecycling.leadTimeSeconds
}

// WithFlexStartRule returns RuleOption adding FlexStartRule.
func WithFlexStartRule(enabled bool, nodeRecycling *v1.NodeRecyclingConfig) RuleOption {
	return func(r *rule) {
		r.flexStartRule.enabled = enabled
		if nodeRecycling == nil || nodeRecycling.LeadTimeSeconds == nil {
			return
		}
		r.flexStartRule.nodeRecycling = &nodeRecyclingConfig{leadTimeSeconds: nodeRecycling.LeadTimeSeconds}
	}
}
