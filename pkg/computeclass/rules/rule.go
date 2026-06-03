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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

// gkeNodeGroup interface lists GkeMig methods used by rules.
type gkeNodeGroup interface {
	GceRef() gce.GceRef
	MachineType() string
	NodePoolName() string
	Spec() *gkeclient.NodePoolSpec
	NodeTpuCount() (int64, error)
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// BaseRule has a matching method shared by all rule sub-interfaces.
type BaseRule interface {
	Matches(cloudprovider.NodeGroup) bool
}

// Rule is an interface representing CRD rules.
type Rule interface {
	BaseRule
	NodePoolsRule
	LocationRule
	LocationZoneTypesRule
	MachineSpecRule
	ReservationsRule
	StorageRule
	GpuRule
	TpuRule
	NodeSystemConfigRule
	MaxPodsPerNodeRule
	MaxRunDurationRule
	FlexStartRule
	SelfServiceRule
	PodFamilyRule
	MinCpuPlatformRule
	LabelsRule
	TaintsRule
	PlacementPolicyRule
	MinimumCapacityRule
}

// rule is an implementations grouping all the supported rule features.
type rule struct {
	nodePoolsRule
	locationRule
	locationZoneTypesRule
	machineSpecRule
	reservationsRule
	storageRule
	gpuRule
	tpuRule
	nodeSystemConfigRule
	maxPodsPerNodeRule
	maxRunDurationRule
	flexStartRule
	selfServiceRule
	podFamilyRule
	minCpuPlatformRule
	labelsRule
	taintsRule
	placementPolicyRule
	minimumCapacityRule
}

func (r *rule) Matches(group cloudprovider.NodeGroup) bool {
	if r == nil {
		return false
	}
	// NodePool rules are a special case, here we don't care about any other matching.
	if len(r.NodePoolNames()) > 0 {
		matched := r.nodePoolsRule.Matches(group)
		klog.V(5).Infof("nodePoolsRule.Matches(%v): %v", group.Id(), matched)
		return matched
	}

	matchResults := []struct {
		name    string
		matched bool
	}{
		{"locationRule", r.locationRule.Matches(group)},
		{"locationZoneTypesRule", r.locationZoneTypesRule.Matches(group)},
		{"machineSpecRule", r.machineSpecRule.Matches(group)},
		{"reservationsRule", r.reservationsRule.Matches(group)},
		{"storageRule", r.storageRule.Matches(group)},
		{"gpuRule", r.gpuRule.Matches(group)},
		{"tpuRule", r.tpuRule.Matches(group)},
		{"nodeSystemConfigRule", r.nodeSystemConfigRule.Matches(group)},
		{"maxPodsPerNodeRule", r.maxPodsPerNodeRule.Matches(group)},
		{"maxRunDurationRule", r.maxRunDurationRule.Matches(group)},
		{"flexStartRule", r.flexStartRule.Matches(group)},
		{"selfServiceRule", r.selfServiceRule.Matches(group)},
		{"podFamilyRule", r.podFamilyRule.Matches(group)},
		{"minCpuPlatformRule", r.minCpuPlatformRule.Matches(group)},
		{"labelsRule", r.labelsRule.Matches(group)},
		{"taintsRule", r.taintsRule.Matches(group)},
		{"placementPolicyRule", r.placementPolicyRule.Matches(group)},
		{"minimumCapacityRule", r.minimumCapacityRule.Matches(group)},
	}

	allMatched := true
	for _, res := range matchResults {
		klog.V(5).Infof("%s.Matches(%v): %v", res.name, group.Id(), res.matched)
		if !res.matched {
			allMatched = false
		}
	}
	return allMatched
}

// RuleOption is a method modifying the underlying rule
type RuleOption func(r *rule)

func NewRule(options ...RuleOption) Rule {
	r := &rule{}
	for _, option := range options {
		option(r)
	}
	return r
}

func NewMachineSpecRule(machineFamily *string, spot *bool, minCores, minMemoryGb *int) Rule {
	return NewRule(
		WithMachineFamilyRule(machineFamily),
		WithSpotRule(spot),
		WithMinCoresRule(minCores),
		WithMinMemoryGbRule(minMemoryGb),
	)
}
