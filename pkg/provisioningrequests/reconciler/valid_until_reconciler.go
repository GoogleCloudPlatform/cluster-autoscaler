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

package reconciler

import (
	"strconv"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	klog "k8s.io/klog/v2"
)

const (
	validUntilExceededFailedReason  = "WaitTimeExceeded"
	validUntilExceededFailedMessage = "Provisioning Request could not provision queued instances in the allocated time."
	validUntilSecondsParameterKey   = "ValidUntilSeconds"
)

type validUntilReconciler struct {
	prClient provreqClient
}

func NewValidUntilReconciler(prClient provreqClient) reconcilingProcessor {
	return &validUntilReconciler{
		prClient: prClient,
	}
}

func (r *validUntilReconciler) reconcileRequests(in *reconcilingInput) (map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, error) {
	hasElapsed := func(pr *provreqwrapper.ProvisioningRequest) bool {
		parameter, found := pr.Spec.Parameters[validUntilSecondsParameterKey]
		if !found {
			return false
		}
		seconds, err := strconv.ParseInt(string(parameter), 10, 64)
		if err != nil || seconds <= 0 {
			klog.Warningf("Invalid ValidUntilSeconds parameter in Provisioning Request %s/%s: %s", pr.Namespace, pr.Name, string(parameter))
			return false
		}
		if pr.CreationTimestamp.IsZero() {
			klog.Warningf("Provisioning Request %s/%s has zero CreationTimestamp", pr.Namespace, pr.Name)
			return false
		}
		expirationTime := pr.CreationTimestamp.Time.Add(time.Duration(seconds) * time.Second)
		return in.now.After(expirationTime)
	}

	for _, state := range allNonTerminalStates {
		failProvReqsWithProperty(r.prClient, in.prs[state], hasElapsed, validUntilExceededFailedReason, validUntilExceededFailedMessage, in.now)
		removeReconciled("validUntilReconciler", "failExpiredProvReqs", in.prs, state, hasElapsed)
	}

	return in.prs, nil
}

func (r *validUntilReconciler) queuedNodesImmunityStartInvalidate() {}

func (r *validUntilReconciler) nodeHasScaleDownImmunity(*apiv1.Node, *QueuedProvisioningMigSpec, time.Time) bool {
	return false
}
