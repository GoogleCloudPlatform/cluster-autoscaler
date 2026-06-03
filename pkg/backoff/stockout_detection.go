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

package backoff

import (
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

var stockoutErrs = map[string]bool{
	"RESOURCE_POOL_EXHAUSTED":                   true,
	"ZONE_RESOURCE_POOL_EXHAUSTED":              true,
	"ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS": true,
}

func IsStockout(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo) bool {
	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return false
	}

	isQuotaError := strings.Contains(errorInfo.ErrorCode, "QUOTA")
	// MIGs using reservation do not have stockouts, resource based backoff
	// shouldn't be applied in such case. Quota errors still should be respected
	// and applied widely as resource based backoff.
	if mig.UsesReservation() && !isQuotaError {
		return false
	}

	// OutOfResources is always a stockout
	if errorInfo.ErrorClass == cloudprovider.OutOfResourcesErrorClass {
		return true
	}

	// INSUFFICIENT_CAPACITY is classified as ErrorInvalidReservation, but it sometimes happens in case
	// there is no capacity at all in a given zone. If the node does not use reservations but we still
	// get an InvalidReservation, we should treat it as a stockout
	// TODO(b/421851595): change the logic so that it's classified as OutOfResources in OSS
	if !mig.UsesReservation() && (errorInfo.ErrorCode == gce.ErrorInvalidReservation || strings.Contains(errorInfo.ErrorMessage, "INSUFFICIENT_CAPACITY")) {
		return true
	}

	// Failed scaleups reported by error_reporter.go are unable to specify the ErrorClass, we have to fallback to ErrorCode
	// TODO(b/421836942): fix this, so that those errors are reported as OutOfResources
	usesErrorReporter := mig.FlexStartNonQueued() || mig.IsMultiHostTpuMig()
	isResourcePoolExhausted := stockoutErrs[errorInfo.ErrorCode]
	return usesErrorReporter && (isResourcePoolExhausted || isQuotaError)
}
