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
)

// napDisabledAndNoMatchingNodegroupsCheck checks if nap disabled and no matching nodepools.
type napDisabledAndNoMatchingNodegroupsCheck struct {
	matcher computeclass.Matcher
}

func (ch *napDisabledAndNoMatchingNodegroupsCheck) checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition {
	napEnabled := c.AutoprovisioningEnabled()
	if napEnabled {
		return nil
	}
	migsMatchingLabel := filterMigsMatchingCrdLabel(ch.matcher, c, migs)
	for _, mig := range migsMatchingLabel {
		if priorityFound, _, _ := ch.matcher.FirstMatchedRule(mig, c); priorityFound || c.ScaleUpAnyway() {
			return nil
		}
	}
	return NapDisabledAndNoMatchingNodegroupsCondition()
}

func (ch *napDisabledAndNoMatchingNodegroupsCheck) conditionType() string {
	return CrdMisconfiguredCondition
}
