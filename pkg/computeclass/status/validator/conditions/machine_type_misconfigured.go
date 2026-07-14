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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
)

// machineTypeConfigChecker checks if machine type configuration is valid.
type machineTypeConfigChecker struct {
	provider CloudProvider
}

func (ch *machineTypeConfigChecker) checkRule(rule rules.Rule) *metav1.Condition {
	var zones []string
	if len(rule.Zones()) > 0 {
		// If zones are available in the rule, use them.
		zones = rule.Zones()
	} else {
		// If zones are not specified, fallback to autoprovisioning locations.
		zones = ch.provider.GetAutoprovisioningLocations()
	}

	if len(zones) <= 0 {
		return nil
	}

	machineType := rule.MachineType()

	// Skipping for undefined machine types.
	if machineType == "" {
		return nil
	}

	var lastError error
	for _, zone := range zones {
		err := ch.provider.ValidateMachineTypeConfig(machineType, zone)
		if err == nil {
			return nil
		}
		lastError = err
	}

	return UnavailableMachineTypeCondition(machineType, lastError)
}

func (ch *machineTypeConfigChecker) conditionType() string {
	return CrdMisconfiguredCondition
}
