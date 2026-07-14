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
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler/reasons"
	klog "k8s.io/klog/v2"
)

var (
	failedStates = []resizerequestclient.ResizeRequestState{
		resizerequestclient.ResizeRequestStateDeleting,
		resizerequestclient.ResizeRequestStateCancelled,
		resizerequestclient.ResizeRequestStateFailed,
	}
)

func (r *resizeRequestReconciler) updateObtainabilityStrategyProvisioningRequestInAcceptedState(rrs []resizerequestclient.ResizeRequestStatus, pr *provreqwrapper.ProvisioningRequest) (bool, error) {
	timestampV1 := v1.NewTime(r.now)
	rrsByState := make(map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus)
	for _, rr := range rrs {
		rrsByState[rr.State] = append(rrsByState[rr.State], rr)
	}
	logObservabilityStrategyProvReqStatus(pr, rrsByState)

	shouldUpdate := false
	var selectedRR *resizerequestclient.ResizeRequestStatus
	var err error

	switch {
	case len(rrsByState[resizerequestclient.ResizeRequestStateSucceeded]) > 0:
		// Since the Resize Request(s) state is as expected, reset the `firstUnsuccessfulReconciliationMap` entry
		delete(r.firstUnsuccessfulReconciliationMap, pods.GetProvReqID(pr))
		shouldUpdate, selectedRR, err = r.updateToProvisionedObtainabilityStrategy(rrsByState, pr, timestampV1)

	case len(rrsByState[resizerequestclient.ResizeRequestStateAccepted]) > 0 || len(rrsByState[resizerequestclient.ResizeRequestStateProvisioning]) > 0:
		// Since the Resize Request(s) state is as expected, reset the `firstUnsuccessfulReconciliationMap` entry
		delete(r.firstUnsuccessfulReconciliationMap, pods.GetProvReqID(pr))
		shouldUpdate, err = r.updateToAcceptedObtainabilityStrategy(rrsByState, pr, timestampV1)

	default:
		shouldUpdate, err = r.updateToFailedObtainabilityStrategy(rrsByState, pr, timestampV1)
	}

	if err != nil {
		return false, fmt.Errorf("failed to modify the Provisioning Request: %w", err)
	}
	if shouldUpdate {
		_, err = r.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest)
		if err == nil && selectedRR != nil {
			// We overwrite scale down immunity as the last step only after a successful update of the Provisioning Request.
			// In case of failed ProvReq update, we want to avoid removing scale down immunity from some zones,
			// because the following attempt in the next loop might select another Resize Request,
			// and updating immunity could cause a scale down.
			r.overwriteScaleDownImmunity(selectedRR.Name, selectedRR.Zone, timestampV1)
		}
		// Since we've updated the state, reset the `firstUnsuccessfulReconciliationMap` entry
		delete(r.firstUnsuccessfulReconciliationMap, pods.GetProvReqID(pr))
	}

	return shouldUpdate, err
}

func logObservabilityStrategyProvReqStatus(pr *provreqwrapper.ProvisioningRequest, rrsByState map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus) {
	states := []string{}
	rrName := ""
	for state, rrs := range rrsByState {
		zones := []string{}
		for _, rr := range rrs {
			zones = append(zones, rr.Zone)
			rrName = rr.Name
		}
		states = append(states, fmt.Sprintf("%s: %s", state, strings.Join(zones, ",")))
	}

	// example: OBTAINABILITY strategy Provisioning Request default/acc-provisioned: Resize Requests with name "gke-default-acc-provisioned-408ac6c1e929cc97" state per zone: [Succeeded: us-east7-b,us-east7-c,us-east7-d; Accepted: us-east7-e]
	klog.Infof("OBTAINABILITY strategy Provisioning Request %s/%s: Resize Requests with name %q state per zone: [%s]", pr.Namespace, pr.Name, rrName, strings.Join(states, "; "))
}

func (r *resizeRequestReconciler) updateToProvisionedObtainabilityStrategy(rrsByState map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus, pr *provreqwrapper.ProvisioningRequest, timestampV1 v1.Time) (bool, *resizerequestclient.ResizeRequestStatus, error) {
	selectedRR := rrsByState[resizerequestclient.ResizeRequestStateSucceeded][0]
	klog.Infof("Selected provisioned Resize Request %s for OBTAINABILITY strategy Provisioning Request %s/%s", selectedRR.Ref(), pr.Name, pr.Namespace)

	overprovisionedZones := r.cancelResizeRequests(pr, rrsByState, selectedRR)
	if len(overprovisionedZones) > 0 {
		klog.Infof("Overprovisioning was done for OBTAINABILITY strategy Provisioning Request %s/%s in zones %v", pr.Name, pr.Namespace, overprovisionedZones)
	}

	err := setProvisionedWithFinalDetails(pr, selectedRR.MigName, selectedRR.Zone, overprovisionedZones, timestampV1)
	if err != nil {
		return false, nil, err
	}
	return true, &selectedRR, nil
}

func (r *resizeRequestReconciler) updateToAcceptedObtainabilityStrategy(rrsByState map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus, pr *provreqwrapper.ProvisioningRequest, timestampV1 v1.Time) (bool, error) {
	shouldCorrectDetails, err := r.correctHTNAPDetails(rrsByState, pr)
	if err != nil {
		return false, err
	}
	shouldObservabilityUpdate := r.observabilityStatusUpdate(rrsByState, pr, timestampV1)

	// We wouldn't necessarily have to clean them up here since they don't take up quota,
	// but otherwise they would only get cleaned up when Provisioning Requests goes Provisioned/Failed.
	r.cleanUpFailedResizeRequests(rrsByState, pr)

	return shouldCorrectDetails || shouldObservabilityUpdate, nil
}

func (r *resizeRequestReconciler) updateToFailedObtainabilityStrategy(rrsByState map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus, pr *provreqwrapper.ProvisioningRequest, timestampV1 v1.Time) (bool, error) {
	if !r.shouldFailProvReqOrRecordUnreconciled(pr) {
		return false, nil
	}
	// FYI: RR clean up will happen in the next loop in deleteInvalidResizeRequestsAndFailInvalidProvReqs, so no need to call it here
	// i.e. PR will be in Failed state, so RRs won't match with it anymore, so they'll be cleaned up
	reason, message := getReasonMessageFromRRs(rrsByState)
	err := provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, reason, message, timestampV1)
	if err != nil {
		return false, fmt.Errorf("failed to modify the Provisioning Request: %w", err)
	}
	return true, nil
}

func (r *resizeRequestReconciler) cancelResizeRequests(pr *provreqwrapper.ProvisioningRequest, rrsByState map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus, selectedRR resizerequestclient.ResizeRequestStatus) []string {
	overprovisionedZones := []string{}
	for _, rrInState := range rrsByState {
		for _, rr := range rrInState {
			if rr.State == resizerequestclient.ResizeRequestStateSucceeded && rr.Zone != selectedRR.Zone {
				klog.Infof("[WIP] Overprovisioning was done by RR %s, add details", rr.Ref())
				overprovisionedZones = append(overprovisionedZones, rr.Zone)
			}
			// AdvanceResizeRequestCleanUp might update AlreadyReportedState to ToBeReportedState when Cancel operation finishes successfully, but for FSQ we want to just ignore that.
			// We only want to log about RR clean up once, when we first time trigger AdvanceResizeRequestCleanUp, so we only care about the initial state and always want to set AlreadyReportedState.
			shouldLog := r.rrClient.ReportState(rr) != resizerequestclient.AlreadyReportedState
			// We're counting the deletion API calls, but not respecting the limit, so that we prevent overprovisioning.
			// This potentially means more API calls and spending more time on reconciliation here.
			r.rrDeleteCalls++
			// TODO(b/486109144): AdvanceResizeRequestCleanUp ignores failed cancel due to provisioning error for FSNQ, but for FSQ OBTAINABILITY strategy we'll want to get it to count it as overprovisioning. It'd require updating function signature to accept a parameter
			err := r.rrClient.AdvanceResizeRequestCleanUp(context.Background(), rr)
			if err != nil {
				// TODO(b/486109144): AdvanceResizeRequestCleanUp currently doesn't return overprovisioning error and just ignores it, so this will never happen
				if overprovisioningError(err, rr) {
					overprovisionedZones = append(overprovisionedZones, rr.Zone)
				}
				klog.Errorf("Provisioning Request %s/%s: couldn't trigger clean up for Resize Request %s in state %s: %v", pr.Namespace, pr.Name, rr.Ref(), rr.State, err)
				continue
			}
			if shouldLog {
				klog.Infof("Provisioning Request %s/%s: triggered clean up of Resize Request %s in state %s; zone %s got provisioned", pr.Namespace, pr.Name, rr.Ref(), rr.State, selectedRR.Zone)
			}
			r.rrClient.SetReportState(rr, resizerequestclient.AlreadyReportedState)
		}
	}
	return overprovisionedZones
}

func overprovisioningError(err error, rr resizerequestclient.ResizeRequestStatus) bool {
	// TODO(b/486109144): confirm whether failure due to overprovisioning will be a condition not met error on an active RR
	isConditionNotMet := resizerequestclient.IsConditionNotMetErr(err)

	overprovisionableStates := []resizerequestclient.ResizeRequestState{
		resizerequestclient.ResizeRequestStateAccepted,
		resizerequestclient.ResizeRequestStateCreating,
		resizerequestclient.ResizeRequestStateProvisioning,
	}
	return isConditionNotMet && slices.Contains(overprovisionableStates, rr.State)
}

func setProvisionedWithFinalDetails(pr *provreqwrapper.ProvisioningRequest, selectedMig, selectedZone string, overprovisionedZones []string, timestampV1 v1.Time) error {
	finalDetails := &queuedwrapper.ProvisioningClassDetails{
		NodeGroupName: selectedMig,
		SelectedZone:  selectedZone,
	}
	if len(overprovisionedZones) > 0 {
		finalDetails.OverprovisionedZones = overprovisionedZones
	}
	err := provreqstate.SetProvisioningClassDetails(pr, finalDetails)
	if err != nil {
		return err
	}
	return provreqstate.SetState(pr, provreqstate.ProvisionedState, timestampV1)
}

func (r *resizeRequestReconciler) overwriteScaleDownImmunity(rrName, zone string, timestampV1 v1.Time) {
	// The immunity was granted for Accepted state, i.e. based on creation timestamp, so we're updating it to
	// Provisioned timestamp with location restriction, to reduce overprovisioning as soon as possible.
	r.resizeRequestNodesImmunityStart[rrName] = NewQueuedNodeImmunityEntry(timestampV1.Time, false, zone)
}

func (r *resizeRequestReconciler) observabilityStatusUpdate(rrsByState map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus, pr *provreqwrapper.ProvisioningRequest, timestampV1 v1.Time) bool {
	for _, rr := range rrsByState[resizerequestclient.ResizeRequestStateAccepted] {
		if len(rr.LastAttemptErrors) <= 0 {
			continue
		}

		// If `acceptedProvReqUpdatesLimit=10` is not enough, consider increasing it via experiment ProvisioningRequestAcceptedUpdatesPerLoopLimitFlag.
		// FYI: If multiple Resize Requests have different error (which is probably not very likely), this might cause flickering between them if we choose a different RR in the next loop.
		// Sorting RRs / aggregating errors might not help as some RR with an unique error could fail, disappear and then cause observability update anyway.
		// We could permit e.g. only a single observability update per OBTAINABILITY strategy ProvReq, from Provisoned=False default reason/message to whatever we get first
		rrErrorInfo := reasons.GetDwsErrorInfoFromLastAttemptErrors(rr.Ref(), rr.LastAttemptErrors)
		conditionChanged := provreqstate.UpdateOrSetProvisioningRequestCondition(pr, prv1.Provisioned, v1.ConditionFalse, rrErrorInfo.Reason, rrErrorInfo.Message, timestampV1)
		return conditionChanged
	}
	return false
}

func (r *resizeRequestReconciler) correctHTNAPDetails(rrsByState map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus, pr *provreqwrapper.ProvisioningRequest) (bool, error) {
	qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
	if qpr.NodePoolAutoProvisioned() != nil && *qpr.NodePoolAutoProvisioned() == "false" {
		return false, nil
	}

	// Get an example MIG GceRef (they all belong to the same node pool) and list of all RRs' MIG names
	migRef := gce.GceRef{}
	migs := []string{}
	for _, rrs := range rrsByState {
		for _, rr := range rrs {
			migRef = gce.GceRef{Name: rr.MigName, Zone: rr.Zone, Project: rr.ProjectID}
			migs = append(migs, rr.MigName)
		}
	}

	nodePoolName := r.migs[migRef].NodePoolName()
	klog.Infof("[WIP] Node pool name for MIG %s is %s", migRef.Name, nodePoolName)
	if qpr.NodePoolName() != nil && *qpr.NodePoolName() == nodePoolName {
		// Details are already correct, no update needed
		return false, nil
	}

	correctedDetails := &queuedwrapper.ProvisioningClassDetails{
		NodePoolName:        nodePoolName,
		CommittedNodeGroups: migs,
	}
	klog.Infof("Correcting Provisioning Request %s/%s NAP-related details: %q from %v to %v and %q from %v to %v",
		pr.Namespace, pr.Name, "NodePoolName", nilSafeStr(qpr.NodePoolName()), nodePoolName, "CommittedNodeGroups", nilSafeStr(qpr.CommittedNodeGroups()), migs)
	err := provreqstate.SetProvisioningClassDetails(pr, correctedDetails)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *resizeRequestReconciler) cleanUpFailedResizeRequests(rrsByState map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus, pr *provreqwrapper.ProvisioningRequest) {
	for _, state := range failedStates {
		for _, rr := range rrsByState[state] {
			if r.rrDeleteCalls >= defaultDeletedResizeRequestsPerLoop {
				break
			}
			shouldLog := r.rrClient.ReportState(rr) != resizerequestclient.AlreadyReportedState
			r.rrDeleteCalls++
			err := r.rrClient.AdvanceResizeRequestCleanUp(context.Background(), rr)
			if err != nil {
				klog.Errorf("Provisioning Request %s/%s: couldn't trigger clean up for Resize Request %s in state %s: %v", pr.Namespace, pr.Name, rr.Ref(), rr.State, err)
				continue
			}
			if shouldLog {
				klog.Infof("Provisioning Request %s/%s: triggered clean up for failed Resize Request %s in state %s; there are still other active Resize Requests for this ProvReq", pr.Namespace, pr.Name, rr.Ref(), rr.State)
			}
			r.rrClient.SetReportState(rr, resizerequestclient.AlreadyReportedState)
		}
	}
}

func getReasonMessageFromRRs(rrs map[resizerequestclient.ResizeRequestState][]resizerequestclient.ResizeRequestStatus) (string, string) {
	switch {
	case len(rrs[resizerequestclient.ResizeRequestStateFailed]) > 0 || len(rrs[resizerequestclient.ResizeRequestStateCancelled]) > 0:
		// FYI: GroupResizeRequestErrors makes sure everything is logged, so we don't have to do it here
		_, errInfo := reasons.GroupResizeRequestErrors(append(rrs[resizerequestclient.ResizeRequestStateFailed], rrs[resizerequestclient.ResizeRequestStateCancelled]...), time.Duration(0), time.Time{})
		return errInfo.Reason, errInfo.Message

	case len(rrs[resizerequestclient.ResizeRequestStateDeleting]) > 0:
		return deletingReason, deletingMessage

	case len(rrs[resizerequestclient.ResizeRequestStateCreating]) > 0:
		return creatingReason, creatingMessage
	}

	return missingResizeRequestFailedReason, missingResizeRequestFailedMessage
}

func nilSafeStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
