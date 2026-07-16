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

package computeclass

import (
	"reflect"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/klog/v2"
)

const (
	ScaleUpAnyway = "ScaleUpAnyway"
	DoNotScaleUp  = "DoNotScaleUp"
)

// organizerCloudProvider provides the required methods from GkeCloudProvider.
type organizerCloudProvider interface {
	IsResizableVmEnabledInAutopilot(machineFamily string) bool
	IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool
	IsExtendedFallbacksEnabled() bool
	IsAutopilotEnabled() bool
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// Organizer organizes node groups by matching Crds and priority rules.
type Organizer interface {
	OrganizeByCrds([]cloudprovider.NodeGroup, []crd.CRD) [][][]cloudprovider.NodeGroup
}

// organizer implements Organizer interface.
type organizer struct {
	provider organizerCloudProvider
	matcher  Matcher
}

// NewOrganizer returns the default implementation of Organizer interface.
func NewOrganizer(lister lister.Lister, provider organizerCloudProvider) Organizer {
	return &organizer{
		provider: provider,
		matcher:  NewMatcher(lister, provider),
	}
}

// OrganizeByCrds organizes the node groups by CRDs.
func (o *organizer) OrganizeByCrds(nodeGroups []cloudprovider.NodeGroup, crds []crd.CRD) [][][]cloudprovider.NodeGroup {
	var organizedByCrd [][][]cloudprovider.NodeGroup
	for _, c := range crds {
		if c == nil || (reflect.ValueOf(c).Kind() == reflect.Ptr && reflect.ValueOf(c).IsNil()) {
			continue
		}
		var matchingNodeGroups, nonMatchingNodeGroups []cloudprovider.NodeGroup
		for _, ng := range nodeGroups {
			if o.matcher.MatchesCrdLabel(ng, c) {
				if o.matcher.MatchesCrdConfig(ng, c) {
					matchingNodeGroups = append(matchingNodeGroups, ng)
				} else {
					klog.Warningf("node group %v does not match crd %v config and will not be used", ng.Id(), c.Name())
				}
			} else {
				nonMatchingNodeGroups = append(nonMatchingNodeGroups, ng)
			}
		}

		partialOrganizedNodeGroups := o.organizeByRules(matchingNodeGroups, c)
		if len(partialOrganizedNodeGroups) > 0 {
			organizedByCrd = append(organizedByCrd, partialOrganizedNodeGroups)
		}
		nodeGroups = nonMatchingNodeGroups
	}
	if len(nodeGroups) > 0 {
		if o.provider.IsResizableVmEnabledInAutopilot(machinetypes.EK.Name()) {
			// When EKs are enabled, prioritize EKs over E2s for NGs without CCC label.
			organizedByFamily := o.organizeByMachineFamily(nodeGroups, machinetypes.EK)
			organizedByCrd = append(organizedByCrd, organizedByFamily)
		} else {
			organizedByCrd = append(organizedByCrd, [][]cloudprovider.NodeGroup{nodeGroups})
		}
	}
	return organizedByCrd
}

func (o *organizer) organizeByRules(nodeGroups []cloudprovider.NodeGroup, crd crd.CRD) [][]cloudprovider.NodeGroup {
	var organizedNodeGroups [][]cloudprovider.NodeGroup
	for _, ruleGroup := range crd.GroupedRules() {
		var matchingNodeGroups, nonMatchingNodeGroups []cloudprovider.NodeGroup
		var matchedRule rules.Rule
		for _, ng := range nodeGroups {
			matched := false
			for _, rule := range ruleGroup {
				if rule == nil || (reflect.ValueOf(rule).Kind() == reflect.Ptr && reflect.ValueOf(rule).IsNil()) {
					continue
				}
				if rule.Matches(ng) {
					matched = true
					matchedRule = rule
					break
				}
			}
			if matched {
				matchingNodeGroups = append(matchingNodeGroups, ng)
			} else {
				nonMatchingNodeGroups = append(nonMatchingNodeGroups, ng)
			}
		}
		if len(matchingNodeGroups) > 0 {
			if prioritizedFamilies := o.prioritizedFamiliesForRule(matchedRule); len(prioritizedFamilies) > 0 {
				organizedByFamilies := o.organizeByMachineFamily(matchingNodeGroups, prioritizedFamilies...)
				organizedNodeGroups = append(organizedNodeGroups, organizedByFamilies...)
			} else {
				organizedNodeGroups = append(organizedNodeGroups, matchingNodeGroups)
			}
		}
		nodeGroups = nonMatchingNodeGroups
	}

	if len(nodeGroups) > 0 {
		if crd.ScaleUpAnyway() {
			organizedNodeGroups = append(organizedNodeGroups, nodeGroups)
		} else {
			klog.V(5).Infof("Filtered out %d node groups due to scale up anyway disabled", len(nodeGroups))
		}
	}

	return organizedNodeGroups
}

// prioritizedFamiliesForRule returns the ordered list of machine families that should be prioritized
// for the given rule, based on the current provider configuration.
func (o *organizer) prioritizedFamiliesForRule(rule rules.Rule) []machinetypes.MachineFamily {
	if rule == nil {
		return nil
	}
	switch rule.PodFamilyName() {
	case rules.GeneralPurposePodFamily:
		var families []machinetypes.MachineFamily
		if o.provider.IsResizableVmWithinPodFamilyEnabled(machinetypes.EK.Name()) {
			families = append(families, machinetypes.EK)
		}
		families = append(families, machinetypes.E2)
		if o.provider.IsExtendedFallbacksEnabled() {
			families = append(families, rules.ExtendedFallbacks...)
		}

		return families
	case rules.GeneralPurposeArmPodFamily:
		if families, err := rule.PodFamilyMachineFamilies(); err == nil {
			return families
		}
	}
	return nil
}

// organizeByMachineFamily prioritizes candidate node groups into absolute scale-up tiers based on an ordered list of machine families.
// Each element of the returned outer slice represents a distinct priority tier evaluated sequentially by the Cluster Autoscaler expander.
// Node groups within the same inner slice are treated as equal-priority alternatives.
func (o *organizer) organizeByMachineFamily(nodeGroups []cloudprovider.NodeGroup, prioritizedFamilies ...machinetypes.MachineFamily) [][]cloudprovider.NodeGroup {
	var organizedNodeGroups [][]cloudprovider.NodeGroup
	buckets := make([][]cloudprovider.NodeGroup, len(prioritizedFamilies))
	var unbucketedNodeGroups []cloudprovider.NodeGroup

	for _, nodeGroup := range nodeGroups {
		mig, ok := nodeGroup.(*gke.GkeMig)
		if !ok {
			klog.Errorf("organizeByMachineFamily expected GkeMig; got %q; will not prioritize the node group", nodeGroup.Id())
			unbucketedNodeGroups = append(unbucketedNodeGroups, nodeGroup)
			continue
		}
		migMachineFamily, err := o.provider.MachineConfigProvider().GetMachineFamilyFromMachineName(mig.Spec().MachineType)
		if err != nil {
			klog.Errorf("organizeByMachineFamily couldn't get machine family for mig %q, will not prioritize the node group: %v", mig.Id(), err)
			unbucketedNodeGroups = append(unbucketedNodeGroups, nodeGroup)
			continue
		}
		matched := false
		for i, family := range prioritizedFamilies {
			if migMachineFamily.Equal(family) {
				buckets[i] = append(buckets[i], nodeGroup)
				matched = true
				break
			}
		}
		if !matched {
			unbucketedNodeGroups = append(unbucketedNodeGroups, nodeGroup)
		}
	}

	for _, bucket := range buckets {
		if len(bucket) > 0 {
			organizedNodeGroups = append(organizedNodeGroups, bucket)
		}
	}
	if len(unbucketedNodeGroups) > 0 {
		organizedNodeGroups = append(organizedNodeGroups, unbucketedNodeGroups)
	}

	return organizedNodeGroups
}
