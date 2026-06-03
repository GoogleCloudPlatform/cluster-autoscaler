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
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	klog "k8s.io/klog/v2"
)

const (
	// initializedProvReqsLimit denotes the number of Provisioning Requests allowed to be initialized in a single refresh to avoid starving the scale-up logic
	initializedProvReqsLimit = 10

	// failMissingPodTemplatesDuration duration after which Provisioning Requests should be considered failed due to missing PodTemplates.
	failMissingPodTemplatesDuration            = 2 * time.Minute
	missingPodTemplatesFailedMessageTemplate   = "Provisioning Request failed because there were %s"
	missingPodTemplatesWillFailMessageTemplate = "Provisioning Request will fail soon as it is %s"
	missingPodTemplatesReason                  = "MissingPodTemplates"
)

type provisioningRequestInitializer struct {
	prClient provreqClient
}

func (r *provisioningRequestInitializer) reconcileRequests(in *reconcilingInput) (map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, error) {
	r.initializeProvisioningRequests(in.prs[provreqstate.UninitializedState], in.now)
	in.prs[provreqstate.UninitializedState] = nil

	r.updatePendingProvReqsMissingPodTemplates(in.prs[provreqstate.PendingState], in.now)
	in.prs[provreqstate.PendingState] = nil

	return in.prs, nil
}

func (r *provisioningRequestInitializer) queuedNodesImmunityStartInvalidate() {}

func (r *provisioningRequestInitializer) nodeHasScaleDownImmunity(*apiv1.Node, *QueuedProvisioningMigSpec, time.Time) bool {
	return false
}

// initializeProvisioningRequests changes the state of new, "Uninitialized" Provisioning Requests to "Pending" by initializing their Conditions.
func (r *provisioningRequestInitializer) initializeProvisioningRequests(uninitializedProvReqs []*provreqwrapper.ProvisioningRequest, now time.Time) {
	initializedPRs := 0
	for _, pr := range uninitializedProvReqs {
		if initializedPRs >= initializedProvReqsLimit {
			break
		}

		// Initialize Provisioning Request
		if err := provreqstate.SetState(pr, provreqstate.PendingState, v1.NewTime(now)); err != nil {
			klog.Errorf("error initializing Provisioning Request %s/%s: %v", pr.Namespace, pr.Name, err)
			continue
		}
		if _, err := r.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
			klog.Errorf("error while updating Provisioning Request %s/%s: %v", pr.Namespace, pr.Name, err)
		}
		initializedPRs++
	}
}

// updatePendingProvReqsMissingPodTemplates fails Provisioning Requests with PodTemplates missing longer than `failMissingPodTemplatesDuration`.
func (r *provisioningRequestInitializer) updatePendingProvReqsMissingPodTemplates(pendingProvReqs []*provreqwrapper.ProvisioningRequest, now time.Time) {
	for _, pr := range pendingProvReqs {
		if _, err := pr.PodSets(); err != nil {
			klog.Warningf("Provisioning Request %s/%s in state %s has %s", pr.Namespace, pr.Name, provreqstate.PendingState, err.Error())
			err = r.updateProvReqToFailedDueToMissingPodTemplates(pr, err, now)
			if err != nil {
				klog.Errorf("Error while marking Pending Provisioning Request %s/%s as failed: %v", pr.Namespace, pr.Name, err)
			}
		}
	}
}

func (r *provisioningRequestInitializer) updateProvReqToFailedDueToMissingPodTemplates(provReq *provreqwrapper.ProvisioningRequest, err error, now time.Time) error {
	if provReq.CreationTimestamp.Time.Add(failMissingPodTemplatesDuration).After(now) {
		if changed := provreqstate.UpdateOrSetProvisioningRequestCondition(provReq, prv1.Accepted, v1.ConditionFalse, missingPodTemplatesReason, fmt.Sprintf(missingPodTemplatesWillFailMessageTemplate, err.Error()), v1.NewTime(now)); !changed {
			return nil
		}
	} else {
		if err := provreqstate.SetStateCustomReasonMessage(provReq, provreqstate.FailedState, missingPodTemplatesReason, fmt.Sprintf(missingPodTemplatesFailedMessageTemplate, err.Error()), v1.NewTime(now)); err != nil {
			return fmt.Errorf("failed to fail the Provisioning Request: %w", err)
		}
	}
	_, err = r.prClient.UpdateProvisioningRequest(provReq.ProvisioningRequest)
	return err
}
