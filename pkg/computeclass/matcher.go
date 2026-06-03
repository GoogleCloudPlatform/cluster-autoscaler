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
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/klog/v2"
)

// matcherCloudProvider provides the required methods from GkeCloudProvider.
type matcherCloudProvider interface {
	IsAutopilotEnabled() bool
}

// matcherNodeGroup interface lists GkeMig methods used by matcher.
type matcherNodeGroup interface {
	Spec() *gkeclient.NodePoolSpec
}

// Matcher matches node groups to Crds.
type Matcher interface {
	MatchesCrdLabel(cloudprovider.NodeGroup, crd.CRD) bool
	MatchesCrdConfig(cloudprovider.NodeGroup, crd.CRD) bool
	FirstMatchedRule(cloudprovider.NodeGroup, crd.CRD) (bool, int, rules.Rule)
	FirstMatchedRuleGroup(cloudprovider.NodeGroup, crd.CRD) (bool, int, rules.Rule)
}

// matcher implements Matcher interface.
type matcher struct {
	lister   lister.Lister
	provider matcherCloudProvider
}

// NewMatcher returns the default implementation of Matcher interface.
func NewMatcher(lister lister.Lister, provider matcherCloudProvider) Matcher {
	return &matcher{
		lister:   lister,
		provider: provider,
	}
}

// MatchesCrdLabel checks if the nodegroup matches to CRD
func (m *matcher) MatchesCrdLabel(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) bool {
	c, cName, err := m.lister.NodeGroupCrd(nodeGroup)
	if err != nil {
		klog.Errorf("no matching crd for Nodegroup %v: , err: %v", nodeGroup.Id(), err)
		return false
	}
	if c == nil || cName == "" {
		return false
	}
	return c.Label() == crd.Label() && cName == crd.Name()
}

// MatchesCrdConfig checks if the nodegroup matches to CRD config
func (m *matcher) MatchesCrdConfig(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) bool {
	if !m.MatchesCrdLabel(nodeGroup, crd) {
		return false
	}

	mig, ok := nodeGroup.(matcherNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	if mig.Spec() == nil {
		return false
	}

	for k, v := range crd.SelfServiceMetadata() {
		value, found := mig.Spec().SelfServiceMetadata[k]
		if !found {
			return false
		}
		if value != v {
			// Certain self-service features can be overridden on a rule level.
			// We accept a mismatch if the value is explicitly overridden by any of the rules.
			validOverride := false
			for _, rule := range crd.Rules() {
				if overrideMap := rule.SelfServiceMetadata(); overrideMap != nil {
					if overrideVal, ok := overrideMap[k]; ok && overrideVal == value {
						validOverride = true
						break
					}
				}
			}
			if !validOverride {
				return false
			}
		}
	}

	for k, v := range crd.UserDefinedLabels() {
		if value, found := mig.Spec().Labels[k]; !found || value != v {
			return false
		}
	}

	for _, t := range crd.UserDefinedTaints() {
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

	if !m.provider.IsAutopilotEnabled() {
		value, found := mig.Spec().Labels[gkelabels.ManagedNodeLabel]
		autopilotManagedMig := found && value == "true"
		if crd.AutopilotManaged() != autopilotManagedMig {
			return false
		}
	}

	if crd.ServiceAccount() != "" && mig.Spec().ServiceAccount != crd.ServiceAccount() {
		return false
	}

	if crd.ImageType() != "" && !strings.EqualFold(mig.Spec().ImageType, crd.ImageType()) {
		return false
	}

	if crd.NodeVersion() != "" && mig.Spec().NodeVersion != crd.NodeVersion() {
		return false
	}

	if !hasMatchingTpuDriverMode(mig.Spec(), crd) {
		return false
	}

	if crd.ArchitectureTaintBehavior() != "" {
		specArchTaintBehavior := mig.Spec().ArchTaintBehavior
		if specArchTaintBehavior == "" {
			specArchTaintBehavior = gkeclient.DefaultArchTaintBehavior
		}
		if crd.ArchitectureTaintBehavior() != specArchTaintBehavior {
			return false
		}
	}

	return true
}

// FirstMatchedRule returns first matched rule if the nodegroup matches CRD
// config and any rule in the CRD matches the nodegroup.  It returns true if
// a rule was matched, along with a rule index and rule. It returns false if
// no rule was matched or if the nodegroup does not match CRD or its config.
func (m *matcher) FirstMatchedRule(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) (bool, int, rules.Rule) {
	if nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
		klog.Error("received nil node group")
		return false, 0, nil
	}

	if !m.MatchesCrdConfig(nodeGroup, crd) {
		return false, 0, nil
	}

	for idx, rule := range crd.Rules() {
		if rule.Matches(nodeGroup) {
			return true, idx, rule
		}
	}

	return false, 0, nil
}

// FirstMatchedRuleGroup returns the index of the first rule group where at least one rule matches the nodegroup.
// It returns true if a rule was matched, along with the group index and the matched rule.
// It returns false if no rule was matched or if the nodegroup does not match CRD or its config.
func (m *matcher) FirstMatchedRuleGroup(nodeGroup cloudprovider.NodeGroup, crd crd.CRD) (bool, int, rules.Rule) {
	if nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
		klog.Error("received nil node group")
		return false, 0, nil
	}

	if !m.MatchesCrdConfig(nodeGroup, crd) {
		return false, 0, nil
	}

	for groupIdx, ruleGroup := range crd.GroupedRules() {
		for _, rule := range ruleGroup {
			if rule.Matches(nodeGroup) {
				return true, groupIdx, rule
			}
		}
	}

	return false, 0, nil
}

// hasMatchingTpuDriverMode checks whether node pool has proper labels set for the given tpu driver mode.
// It needs to check DRA enablement label in both cases as otherwise device plugin configuration
// may get matched to a DRA node pool.
func hasMatchingTpuDriverMode(nodePoolSpec *gkeclient.NodePoolSpec, crdObj crd.CRD) bool {
	if crdObj.TpuDriverMode() == crd.TpuDriverModeDynamicResourceAllocation {
		return nodePoolSpec.Labels[gkelabels.DraTpuNodeLabel] == "true"
	}

	if crdObj.TpuDriverMode() == crd.TpuDriverModeDevicePlugin {
		return nodePoolSpec.Labels[gkelabels.DraTpuNodeLabel] == ""
	}

	return true
}
