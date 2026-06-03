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

package conditions

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
)

// noRuleMatchingCheck checks if no rule in Crd is matching for a matching mig.
type noRuleMatchingCheck struct {
	matcher computeclass.Matcher
}

func (ch *noRuleMatchingCheck) checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition {
	migsMatchingLabel := filterMigsMatchingCrdLabel(ch.matcher, c, migs)
	for _, mig := range migsMatchingLabel {
		priorityFound, _, _ := ch.matcher.FirstMatchedRule(mig, c)
		if !priorityFound && !c.ScaleUpAnyway() {
			return NoRuleMatchingCondition(mig.NodePoolName())
		}
	}
	return nil
}

func (ch *noRuleMatchingCheck) conditionType() string {
	return NodepoolMisconfiguredCondition
}

// crdLabelNotMatchingCheck checks if crd label is not matching.
type crdLabelNotMatchingCheck struct {
	matcher computeclass.Matcher
}

func (ch *crdLabelNotMatchingCheck) checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition {
	for _, name := range getNamesFromNodepoolRule(c) {
		mig, exists := migs[name]
		if !exists {
			continue
		}
		if !ch.matcher.MatchesCrdLabel(mig, c) {
			return CrdLabelNotMatchingCondition(mig.NodePoolName())
		}
	}
	return nil
}

func (ch *crdLabelNotMatchingCheck) conditionType() string {
	return NodepoolMisconfiguredCondition
}

// taintMissingCheck checks if crd taint is missing.
type taintMissingCheck struct {
	lister  lister.Lister
	matcher computeclass.Matcher
}

func (ch *taintMissingCheck) checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition {
	if defaultCrd, defaultLabel, found := ch.lister.Default(); found {
		if c.Name() == defaultCrd && c.Label() == defaultLabel {
			return nil
		}
	}
	migsMatchingLabel := filterMigsMatchingCrdLabel(ch.matcher, c, migs)
	for _, mig := range migsMatchingLabel {
		_, err := crd.NodeGroupCrdTaint(mig, c.Label())
		if err != nil {
			return TaintMissingCondition(mig.NodePoolName())
		}
	}
	return nil
}

func (ch *taintMissingCheck) conditionType() string {
	return NodepoolMisconfiguredCondition
}

// taintValueNotMatchingCheck checks if crd taint value doesn't match to crd name.
type taintValueNotMatchingCheck struct {
	lister  lister.Lister
	matcher computeclass.Matcher
}

func (ch *taintValueNotMatchingCheck) checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition {
	if defaultCrd, defaultLabel, found := ch.lister.Default(); found {
		if c.Name() == defaultCrd && c.Label() == defaultLabel {
			return nil
		}
	}
	migsMatchingLabel := filterMigsMatchingCrdLabel(ch.matcher, c, migs)
	for _, mig := range migsMatchingLabel {
		value, err := crd.NodeGroupCrdTaint(mig, c.Label())
		if err == nil && value != c.Name() {
			return TaintValueNotMatchingCondition(mig.NodePoolName())
		}
	}
	return nil
}

func (ch *taintValueNotMatchingCheck) conditionType() string {
	return NodepoolMisconfiguredCondition
}

// nodepoolWillNeverScaleUpCheck checks if a nodepool will never scaleup.
type nodepoolWillNeverScaleUpCheck struct {
	matcher computeclass.Matcher
}

func (ch *nodepoolWillNeverScaleUpCheck) checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition {
	if c.ScaleUpAnyway() {
		return nil
	}
	migsMatchingLabel := filterMigsMatchingCrdLabel(ch.matcher, c, migs)
	for _, mig := range migsMatchingLabel {
		priorityFound, _, _ := ch.matcher.FirstMatchedRule(mig, c)
		if !priorityFound {
			return NodepoolWillNeverScaleUpCondition(mig.NodePoolName())
		}
	}
	return nil
}

func (ch *nodepoolWillNeverScaleUpCheck) conditionType() string {
	return NodepoolMisconfiguredCondition
}

// multipleCrdTaintsCheck checks if a nodepool contains more than one Crd taints.
type multipleCrdTaintsCheck struct {
	matcher computeclass.Matcher
}

func (ch *multipleCrdTaintsCheck) checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition {
	migsMatchingLabel := filterMigsMatchingCrdLabel(ch.matcher, c, migs)
	for _, mig := range migsMatchingLabel {
		crdTaints, err := crd.NodeGroupCrdTaints(mig, c.Label())
		if err == nil {
			// Check if all Crd Taints are all the same.
			for _, t := range crdTaints {
				if t.Value != crdTaints[0].Value {
					return MultipleCrdTaintsCondition(mig.NodePoolName())
				}
			}
		}
	}
	return nil
}

func (ch *multipleCrdTaintsCheck) conditionType() string {
	return NodepoolMisconfiguredCondition
}

// getNamesFromNodepoolRule returns a slice of nodepool names from Nodepools PR.
func getNamesFromNodepoolRule(c crd.CRD) []string {
	var filteredNames []string
	for _, rule := range c.Rules() {
		filteredNames = append(filteredNames, rule.NodePoolNames()...)
	}
	return filteredNames
}

// filterMigsMatchingCrdLabel filters the nodepools which have Crd label.
func filterMigsMatchingCrdLabel(matcher computeclass.Matcher, c crd.CRD, migs map[string]*gke.GkeMig) []*gke.GkeMig {
	var matchingMigs []*gke.GkeMig
	for _, mig := range migs {
		if matcher.MatchesCrdLabel(mig, c) {
			matchingMigs = append(matchingMigs, mig)
		}
	}
	return matchingMigs
}
