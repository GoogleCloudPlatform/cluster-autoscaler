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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/klog/v2"
)

// locationNotEnabledForAutoprovisioningCheck checks if locations in zonal preferences config are outside of autoprovisioning locations.
type locationNotEnabledForAutoprovisioningCheck struct {
	provider CloudProvider
}

func (ch *locationNotEnabledForAutoprovisioningCheck) checkCrd(c crd.CRD, migs map[string]*gke.GkeMig) *metav1.Condition {
	if !ch.provider.IsNodeAutoprovisioningEnabled() {
		return nil
	}
	availableZones, err := ch.provider.GetAllZones()
	if err != nil {
		klog.Errorf("error fetching all available zones in the cluster: %v", err)
		return nil
	}
	autoprovisioningLocations := make(map[string]bool)
	for _, loc := range availableZones {
		autoprovisioningLocations[loc] = true
	}

	for _, rule := range c.Rules() {
		for _, zone := range rule.Zones() {
			if _, exists := autoprovisioningLocations[zone]; !exists {
				return LocationNotEnabledForAutoprovisioningCondition(zone, availableZones)
			}
		}
	}
	return nil
}

func (ch *locationNotEnabledForAutoprovisioningCheck) conditionType() string {
	return CrdMisconfiguredCondition
}
