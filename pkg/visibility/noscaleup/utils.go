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

package noscaleup

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/orchestrator"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
)

// isRemovedByFlexAdvisor returns true if the node group was rejected during bin-packing
// due to FlexAdvisor capacity limits.
//
// When bin-packing produces 0 expansion options (ScaleUpNoOptionsAvailable), OSS CA's
// markAllGroupsAsUnschedulable marks all SchedulableGroups with NoScaleUpOptionsAvailableReason
// and places them into RejectedNodeGroups. Checking both NoScaleUpOptionsAvailableReason and
// WasNodeGroupRemovedByFlexAdvisor ensures we only attribute a node group rejection to FlexAdvisor
// when scale-up actually failed due to 0 options and FlexAdvisor removed this specific node group.
func isRemovedByFlexAdvisor(migId string, reasons status.Reasons, faLimiter flexadvisor.ScaleUpLimiterTracker) bool {
	return reasons == orchestrator.NoScaleUpOptionsAvailableReason &&
		faLimiter != nil &&
		faLimiter.WasNodeGroupRemovedByFlexAdvisor(migId)
}
