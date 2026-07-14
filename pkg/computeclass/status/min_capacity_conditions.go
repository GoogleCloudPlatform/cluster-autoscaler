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

package status

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Conditions for min capacity provisioning status.
const (
	MinCapacityProvisioning = "MinCapacityProvisioning"
	MinCapacityProvisioned  = "MinCapacityProvisioned"
)

// Reasons for min capacity provisioning conditions.
const (
	ProvisioningStarted    = "ProvisioningStarted"
	ProvisioningInProgress = "ProvisioningInProgress"
	ProvisioningFailed     = "ProvisioningFailed"
	ProvisioningComplete   = "ProvisioningComplete"
)

// Messages for min capacity provisioning conditions.
const (
	MinimumCapacityProvisioningFailedMessage = "Provisioning failed: %s."
)

func MinCapacityProvisioningCondition(status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return &metav1.Condition{
		Type:               MinCapacityProvisioning,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

func MinCapacityProvisionedCondition(status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return &metav1.Condition{
		Type:               MinCapacityProvisioned,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}
