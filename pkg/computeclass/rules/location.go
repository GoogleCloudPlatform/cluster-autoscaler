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
	"maps"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/klog/v2"
)

// NodePoolsRule is an interface for rules with nodepools.
type LocationRule interface {
	BaseRule
	Zones() []string
}

type locationRule struct {
	zones []string
}

// Matches returns true if the nodegroup is within one of the nodepools.
func (r *locationRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	if mig.Spec() == nil {
		return false
	}

	// b/400378689
	// The logic here might be revisited in the future. See go/ccc-zones-summary for details.
	if len(r.zones) == 0 {
		return true
	}

	// Perform an equality check between sets of rule zones and nodegroup's node pool zones.
	nodePoolLocationsMap := make(map[string]bool)
	for _, loc := range mig.Spec().Locations {
		nodePoolLocationsMap[loc] = true
	}
	ruleLocationsMap := make(map[string]bool)
	for _, loc := range r.zones {
		ruleLocationsMap[loc] = true
	}

	return maps.Equal(nodePoolLocationsMap, ruleLocationsMap)
}

// Zones returns names of zones.
func (r *locationRule) Zones() []string {
	return r.zones
}

// WithLocationRule returns RuleOption adding LocationRule.
func WithLocationRule(zones []string) RuleOption {
	return func(r *rule) {
		r.locationRule = locationRule{zones: zones}
	}
}
