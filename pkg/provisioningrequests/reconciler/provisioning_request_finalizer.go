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
	"time"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	klog "k8s.io/klog/v2"
)

const (
	// terminalProvisioningRequestTTL is the duration after which provisioned/failed Provisioning Request will be deleted
	terminalProvisioningRequestTTL            = 7 * 24 * time.Hour
	defaultDeletedProvisioningRequestsPerLoop = 20
)

type provisioningRequestFinalizer struct {
	prClient provreqClient
}

func (r *provisioningRequestFinalizer) reconcileRequests(in *reconcilingInput) (map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, error) {

	r.reconcileProvisionedAndBookingExpiredProvReqs(in.prs, in.now)
	in.prs[provreqstate.ProvisionedState] = nil
	in.prs[provreqstate.BookingExpiredState] = nil

	r.cleanUpOldTerminalProvReqs(in.prs, in.now)
	in.prs[provreqstate.CapacityRevokedState] = nil
	in.prs[provreqstate.FailedState] = nil

	return in.prs, nil
}

type stateTransition struct {
	sourceState      provreqstate.ProvisioningRequestState
	targetState      provreqstate.ProvisioningRequestState
	timeToTransition func(pr *provreqwrapper.ProvisioningRequest) time.Duration
}

// reconcileProvisionedAndBookingExpiredProvReqs reconciles Provisioning Requests for which the capacity was already provisioned
func (r *provisioningRequestFinalizer) reconcileProvisionedAndBookingExpiredProvReqs(provisioningRequestMap map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, now time.Time) {
	stateTransitions := []stateTransition{
		{
			// Mark the Provisioning Request as BookingExpired after the booking duration passes,
			// this denotes the fact that CA will no longer shield those nodes from scale-down.
			sourceState: provreqstate.ProvisionedState,
			targetState: provreqstate.BookingExpiredState,
			timeToTransition: func(_ *provreqwrapper.ProvisioningRequest) time.Duration {
				return provreqstate.BookingDuration
			},
		},
		{
			// Mark the Provisioning Request as CapacityRevoked after the max run duration passes,
			// as the VMs were deleted by the GCP.
			sourceState: provreqstate.BookingExpiredState,
			targetState: provreqstate.CapacityRevokedState,
			timeToTransition: func(pr *provreqwrapper.ProvisioningRequest) time.Duration {
				qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
				if mrd, _ := qpr.MaxRunDuration(); mrd != nil {
					return *mrd
				}
				// TODO(b/430258330): does this work for BulkMigs? Or do we have to retrieve MRD form MIG/NP?
				return queuedwrapper.DefaultMaxRunDuration
			},
		},
	}

	for _, st := range stateTransitions {
		for _, pr := range provisioningRequestMap[st.sourceState] {
			// Measure the change from the Provisioned state.
			transitionTime, err := getLastTransitionTimestamp(pr, provreqstate.ProvisionedState)
			if err != nil {
				klog.Errorf("Couldn't retrieve timestamp of %q condition for Provisioning Request %s/%s: %v", provreqstate.ProvisionedState, pr.Namespace, pr.Name, err)
				continue
			}
			stateDuration := st.timeToTransition(pr)
			if now.Before(transitionTime.Add(stateDuration)) {
				continue
			}

			if err = provreqstate.SetState(pr, st.targetState, v1.NewTime(now)); err != nil {
				klog.Errorf("Error while modifying Provisioning Request %s/%s during setting state to %q: %v", pr.Namespace, pr.Name, st.targetState, err)
				continue
			}
			if _, err = r.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
				klog.Errorf("Error while updating Provisioning Request %s/%s during setting state to %q: %v", pr.Namespace, pr.Name, st.targetState, err)
				continue
			}
		}
	}
}

// cleanUpOldTerminalProvReqs deletes up to `defaultDeletedProvisioningRequestsPerLoop` old Provisioning Requests in CapacityRevoked or Failed states.
func (r *provisioningRequestFinalizer) cleanUpOldTerminalProvReqs(provisioningRequestMap map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, now time.Time) {
	deletedProvisioningRequests := 0
	terminalStates := []provreqstate.ProvisioningRequestState{provreqstate.CapacityRevokedState, provreqstate.FailedState}
	for _, provReqState := range terminalStates {
		for i, pr := range provisioningRequestMap[provReqState] {
			if deletedProvisioningRequests >= defaultDeletedProvisioningRequestsPerLoop {
				klog.Infof("Provisioning Request Delete calls quota of %d in this loop was exhausted, there were %d more %q ProvisioningRequests which might need clean up, they'll be handled in the next loop.", defaultDeletedProvisioningRequestsPerLoop, len(provisioningRequestMap[provReqState])-i, provReqState)
				break
			}

			prTooOld, terminalTransitionTime := isProvisioningRequestOld(pr, now)
			if !prTooOld {
				continue
			}

			err := r.prClient.DeleteProvisioningRequest(pr.ProvisioningRequest)
			if err != nil {
				klog.Warningf("Couldn't delete old %s Provisioning Request %s/%s: %v", provReqState, pr.Namespace, pr.Name, err)
				continue
			}
			klog.V(4).Infof("Cleaned up old Provisioning Request %s/%s which was in %s state since %v", pr.Namespace, pr.Name, provReqState, terminalTransitionTime)
			deletedProvisioningRequests++
		}
	}
}

// isProvisioningRequestOld returns `true` when the Provisioning Request was in CapacityRevoked or Failed state for more than `terminalProvisioningRequestTTL`
func isProvisioningRequestOld(pr *provreqwrapper.ProvisioningRequest, now time.Time) (bool, v1.Time) {
	for _, cond := range pr.Status.Conditions {
		if cond.Status != v1.ConditionTrue {
			continue
		}
		if cond.Type != prv1.CapacityRevoked && cond.Type != prv1.Failed {
			continue
		}
		terminalTransitionTime := cond.LastTransitionTime
		if terminalTransitionTime.Add(terminalProvisioningRequestTTL).Before(now) {
			return true, terminalTransitionTime
		}
	}
	return false, v1.Time{}
}

func (r *provisioningRequestFinalizer) queuedNodesImmunityStartInvalidate() {}

func (r *provisioningRequestFinalizer) nodeHasScaleDownImmunity(*apiv1.Node, *QueuedProvisioningMigSpec, time.Time) bool {
	return false
}
