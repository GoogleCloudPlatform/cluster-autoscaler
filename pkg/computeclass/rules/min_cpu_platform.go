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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

// MinCpuPlatformRule is an interface for rules with min cpu platform.
type MinCpuPlatformRule interface {
	BaseRule
	MinCpuPlatform() (machinetypes.CpuPlatform, error)
	MinCpuPlatformString() string
}

type minCpuPlatformRule struct {
	minCpuPlatformString *string
}

// Matches returns true if the nodegroup matches min cpu platform rule.
func (r *minCpuPlatformRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	minCpuPlatform, _ := r.MinCpuPlatform()
	if minCpuPlatform == machinetypes.AnyPlatform {
		return true
	}

	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("Expected GkeMig; got %s.", nodeGroup.Id())
		return false
	}

	if mig.Spec() == nil {
		klog.Errorf("Passed GkeMig does not have spec set; mig id: %s.", nodeGroup.Id())
		return false
	}

	migMinCpuPlatform, err := machinetypes.ToCpuPlatform(mig.Spec().MinCpuPlatform)
	if err != nil {
		klog.Errorf("Mig with id %s. Error while parsing min cpu platform: %v.", nodeGroup.Id(), err)
		return false
	}

	return machinetypes.PlatformIsAtLeast(migMinCpuPlatform, minCpuPlatform)
}

// MinCpuPlatform returns min cpu platform of the rule.
func (r *minCpuPlatformRule) MinCpuPlatform() (machinetypes.CpuPlatform, error) {
	// If name is not set we consider it to be AnyPlatform.
	if r.minCpuPlatformString == nil {
		return machinetypes.AnyPlatform, nil
	}
	minCpuPlatform, err := machinetypes.ToCpuPlatform(*r.minCpuPlatformString)
	return minCpuPlatform, err
}

// MinCpuPlatformString returns min cpu platform of the rule in the form of a string.
func (r *minCpuPlatformRule) MinCpuPlatformString() string {
	if r.minCpuPlatformString == nil {
		return ""
	}
	return *r.minCpuPlatformString
}

// WithMinCpuPlatformRule returns RuleOption adding MinCpuPlatformRule.
func WithMinCpuPlatformRule(minCpuPlatformString *string) RuleOption {
	return func(r *rule) {
		r.minCpuPlatformString = minCpuPlatformString
	}
}
