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
	"time"

	apiv1 "k8s.io/api/core/v1"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler/reasons"
	klog "k8s.io/klog/v2"
)

const (
	defaultFailedProvisioningRequestsMissingRRsPerLoop = 10
	missingResizeRequestFailedReason                   = "MissingResizeRequest"
	missingResizeRequestFailedMessage                  = "The corresponding Resize Request was missing."

	defaultDeletedResizeRequestsPerLoop = defaultDeletedProvisioningRequestsPerLoop + 5
	duplicateResizeRequestFailedReason  = "DuplicateResizeRequestName"
	duplicateResizeRequestFailedMessage = "Multiple Resize Requests had the same name."

	// terminalResizeRequestTTL is the duration after which terminal state Resize Request will be deleted
	terminalResizeRequestTTL = 1 * time.Hour

	// missingResizeRequestKey is used as a key for Accepted Provisioning Requests missing their corresponding Resize Request in the `invalidProvisioningRequests` map of PRs to be Failed
	missingResizeRequestKey = "missing#rr#key!"
)

type provreqClient interface {
	UpdateProvisioningRequest(*prv1.ProvisioningRequest) (*prv1.ProvisioningRequest, error)
	DeleteProvisioningRequest(*prv1.ProvisioningRequest) error
	ProvisioningRequests() ([]*provreqwrapper.ProvisioningRequest, error)
	ProvisioningRequest(string, string) (*provreqwrapper.ProvisioningRequest, error)
}

// resizeRequestReconciler reconciles Provisioning Requests with their corresponding Resize Requests.
type resizeRequestReconciler struct {
	prClient provreqClient
	rrClient resizerequestclient.ResizeRequestClient
	// resizeRequestNameFunc returns name of ResizeRequest based on the given namespace and name of ProvisioningRequest; it's kept as an attribute for convenient testing.
	resizeRequestNameFunc func(*provreqwrapper.ProvisioningRequest) string

	// firstUnsuccessfulReconciliationMap is a map of timestamps indicating per Provisioning Request `what was the first unsuccessful reconciliation attempt timestamp?`
	// (unsuccessful meaning the Resize Request had an unexpected state or was missing)
	// If the next reconciliation attempt is:
	// * successful: clear that entry in the map,
	// * unsuccessful: check if `maxUnreconciledPeriod` has passed since the first attempt:
	//     * if yes, fail the Provisioning Request,
	//     * otherwise log a warning and ignore until the next check.
	firstUnsuccessfulReconciliationMap map[pods.ProvReqID]time.Time

	resizeRequestNodesImmunityStart map[string]QueuedNodeImmunityEntry
	now                             time.Time
	resizeRequests                  map[string][]resizerequestclient.ResizeRequestStatus
	migs                            map[gce.GceRef]common.GkeMigWrapper
	// rrDeleteCalls is the number of times AdvanceResizeRequestCleanUp has been called in a single loop (i.e. single reconcileRequests call)
	rrDeleteCalls int
}

func provisioningRequestResizeRequestName(pr *provreqwrapper.ProvisioningRequest) string {
	qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
	if rrn := qpr.ResizeRequestName(); rrn != nil {
		return *rrn
	}
	return resizerequestclient.ResizeRequestName(pr.Namespace, pr.Name)
}

// nodeHasScaleDownImmunity returns true if the given node has scale down immunity based on the resizeRequestNodesImmunityStart map entry or creation timestamp
func (r *resizeRequestReconciler) nodeHasScaleDownImmunity(node *apiv1.Node, migSpec *QueuedProvisioningMigSpec, now time.Time) bool {
	if migSpec == nil || migSpec.ProvisioningMode != ResizeRequestProvisioningMode {
		return false
	}

	resizeRequestName, found := node.Labels[gkelabels.ProvisioningRequestLabelKey]
	if !found || r.resizeRequestNodesImmunityStart == nil {
		// Use creation timestamp fallback: gcp-controller-manager will sometimes fail to add the label in a reasonable time (b/358300381) or the immunity map is currently invalidated
		return isNodeImmune(migSpec.ProvisioningMode, node, migSpec.Immunity, QueuedNodeImmunityEntry{useNodeCreationTimestamp: true}, now)
	}

	immunityEntry, found := r.resizeRequestNodesImmunityStart[resizeRequestName]
	if !found {
		return false
	}
	return isNodeImmune(migSpec.ProvisioningMode, node, migSpec.Immunity, immunityEntry, now)
}

// reconcileRequests updates statuses of Provisioning Requests based on states of their corresponding Resize Requests
// and filters out Provisioning Requests which it handled from remaining `provReqsToReconcile`.
func (r *resizeRequestReconciler) reconcileRequests(in *reconcilingInput) (map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, error) {
	invalidProvisioningRequests := map[string][]*provreqwrapper.ProvisioningRequest{}
	err := r.retrieveResizeRequests(in.rrMigs, in.now)
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch Resize Requests: %w", err)
	}

	// Immunity has to be updated at the beginning because we modify provReqsToReconcile
	r.resizeRequestNodesImmunityStartUpdate(in.prs)

	r.filterUnreadyProvReqs(in.prs)
	r.reconcilePendingProvReqs(in.prs, invalidProvisioningRequests)

	r.reconcileActiveProvReqsAndFilterOutOldRRs(in.prs, in.acceptedProvReqUpdatesPerLoopLimit, invalidProvisioningRequests)
	r.ignoreRecentlyTerminalResizeRequests(in.prs)

	r.failProvisioningRequestsMissingResizeRequests(invalidProvisioningRequests[missingResizeRequestKey])
	r.ignoreResizeRequestsCorrespondingToOutOfSyncProvReqs(in.prsOutOfSync)
	r.deleteInvalidResizeRequestsAndFailInvalidProvReqs(invalidProvisioningRequests)

	r.resizeRequests = nil
	r.migs = nil
	return in.prs, nil
}

func (r *resizeRequestReconciler) queuedNodesImmunityStartInvalidate() {
	r.resizeRequestNodesImmunityStart = nil
}

func (r *resizeRequestReconciler) resizeRequestNodesImmunityStartUpdate(provisioningRequestMap map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest) {
	rrnameKeyFn := func(pr *provreqwrapper.ProvisioningRequest) (string, bool) {
		if !isResizeRequestProvisioningMode(pr) {
			return "", false
		}
		return r.resizeRequestNameFunc(pr), true
	}
	r.resizeRequestNodesImmunityStart = refreshQueuedProvisioningNodesImmunityStart(provisioningRequestMap, rrnameKeyFn)
}

func getLastTransitionTimestamp(pr *provreqwrapper.ProvisioningRequest, provReqState provreqstate.ProvisioningRequestState) (time.Time, error) {
	var condition string
	switch provReqState {
	case provreqstate.AcceptedState:
		condition = prv1.Accepted
	case provreqstate.ProvisionedState:
		condition = prv1.Provisioned
	case provreqstate.BookingExpiredState:
		condition = prv1.BookingExpired
	case provreqstate.CapacityRevokedState:
		condition = prv1.CapacityRevoked
	case provreqstate.FailedState:
		condition = prv1.Failed
	default:
		return time.Time{}, fmt.Errorf("Cannot retrieve condition transition timestamp for Provisioning Request %s/%s in state %q", pr.Namespace, pr.Name, provReqState)
	}
	if foundCondition := k8sapimeta.FindStatusCondition(pr.Status.Conditions, condition); foundCondition != nil {
		return foundCondition.LastTransitionTime.Time, nil
	}
	return time.Time{}, fmt.Errorf("Provisioning Request %s/%s is missing requested condition %q, present conditions: %+v", pr.Namespace, pr.Name, condition, pr.Status.Conditions)
}

// filterUnreadyProvReqs filters out Uninitialized Provisioning Requests which were recently recreated and have a corresponding Resize Request from a previous Provisioning Request with the same name.
// They should be ignored until the Resize Requests are cleaned up.
func (r *resizeRequestReconciler) filterUnreadyProvReqs(provisioningRequestMap map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest) {
	hasResizeRequest := func(pr *provreqwrapper.ProvisioningRequest) bool {
		_, found := r.resizeRequests[r.resizeRequestNameFunc(pr)]
		return found
	}
	removeReconciled("resizeRequestReconciler", "filterUnreadyProvReqs", provisioningRequestMap, provreqstate.UninitializedState, hasResizeRequest)
}

// reconcilePendingProvReqs reconciles pending Provisioning Requests:
// Ignore PRs without Resize Request name set - these were just not picked up for scale up yet.
// For ones that CA tried to queue but could not finish due to restart or time-out:
//   - If the Resize Request was created, CA will move the Pending PR to Accepted state.
//   - If the Resize Request was not created, CA will:
//   - for regular single-zone ProvReqs: cleanup the Resize Request and MIG names, so that CA can handle the Provisioning Request again (scale up was synchronous, so the RR won't appear).
//   - for OBTAINABILITY strategy ProvReqs: scale up was asynchronous, so the RRs might appear after some time - wait with a timeout; if no RRs appear, move PR to Failed state.
func (r *resizeRequestReconciler) reconcilePendingProvReqs(provisioningRequestMap map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, invalidProvisioningRequests map[string][]*provreqwrapper.ProvisioningRequest) {
	var err error

	reconciled := sets.Set[pods.ProvReqID]{}
	for _, pr := range provisioningRequestMap[provreqstate.PendingState] {
		if !isResizeRequestProvisioningMode(pr) {
			continue
		}
		qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
		// Ignore PRs without Resize Request name set - these were just not picked up for scale up yet
		if qpr.ResizeRequestName() == nil || *qpr.ResizeRequestName() == "" {
			continue
		}
		reconciled.Insert(pods.GetProvReqID(pr))

		rrName := *qpr.ResizeRequestName()
		rrs := r.resizeRequests[rrName]
		// Regular single zone ProvReq shouldn't have multiple corresponding RRs
		if qpr.DefaultStrategy() && len(rrs) > 1 {
			invalidProvisioningRequests[rrName] = append(invalidProvisioningRequests[rrName], pr)
			klog.Warningf("ProvisioningRequest %s/%s in state %q has multiple matching Resize Requests with name %s.", pr.Namespace, pr.Name, provreqstate.PendingState, rrName)
			continue
		}

		if len(rrs) > 0 {
			err = updateProvisioningRequestToAcceptedState(r.prClient, pr, r.now)
			if err != nil {
				klog.Errorf("Error while updating Pending Provisioning Request %s/%s during reconciling with Resize Request %s: %v", pr.Namespace, pr.Name, rrName, err)
			} else {
				klog.Infof("Reconciled pending Provisioning Request %s/%s with Resize Request %s.", pr.Namespace, pr.Name, rrName)
			}
			delete(r.firstUnsuccessfulReconciliationMap, pods.GetProvReqID(pr))
			delete(r.resizeRequests, rrName)
			continue
		}

		if qpr.DefaultStrategy() {
			err = clearProvisioningRequestDetails(r.prClient, pr)
			if err != nil {
				klog.Errorf("Error while clearing Pending Provisioning Request %s/%s Resize Request and MIG name: %v", pr.Namespace, pr.Name, err)
			}
		}

		if qpr.ObtainabilityStrategy() {
			shouldFail := r.shouldFailProvReqOrRecordUnreconciled(pr)
			klog.Infof("ProvisioningRequest %s/%s with %s %s in state %q is commited to %q Resize Request, but no matching Resize Request found. shouldFail: %v",
				pr.Namespace, pr.Name, queuedwrapper.CapacitySearchStrategyKey, queuedwrapper.CapacitySearchStrategyObtainability, provreqstate.PendingState, rrName, shouldFail)
			if shouldFail {
				invalidProvisioningRequests[missingResizeRequestKey] = append(invalidProvisioningRequests[missingResizeRequestKey], pr)
			}
		}
	}
	removeReconciled("resizeRequestReconciler", "recoverPendingProvReqs", provisioningRequestMap, provreqstate.PendingState, reconciledFilterFn(reconciled))
}

// reconcileActiveProvReqsAndFilterOutOldRRs reconciles Provisioning Requests that were already queued with their corresponding Resize Requests
func (r *resizeRequestReconciler) reconcileActiveProvReqsAndFilterOutOldRRs(provisioningRequestMap map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, updatesLimit int, invalidProvisioningRequests map[string][]*provreqwrapper.ProvisioningRequest) {
	// Reconciling `Accepted` Provisioning Requests, because:
	// - `Pending` are handled by `ProvisioningRequestManager` (mostly, `reconcilePendingProvReqs` handles partially assigned PRs)
	// - `Provisioned`/`Failed` are terminal states handled in `cleanUpOldProvReqs` method
	reconciled := sets.Set[pods.ProvReqID]{}

	observabilityUpdates := 0
	for _, pr := range provisioningRequestMap[provreqstate.AcceptedState] {
		if !isResizeRequestProvisioningMode(pr) {
			continue
		}
		reconciled.Insert(pods.GetProvReqID(pr))

		qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
		rrName := r.resizeRequestNameFunc(pr)
		if observabilityUpdates >= updatesLimit {
			delete(r.resizeRequests, rrName)
			continue
		}
		rrs := r.resizeRequests[rrName]
		if len(rrs) > 1 && qpr.DefaultStrategy() {
			invalidProvisioningRequests[rrName] = append(invalidProvisioningRequests[rrName], pr)
			klog.Warningf("ProvisioningRequest %s/%s in state %q has multiple matching Resize Requests.", pr.Namespace, pr.Name, provreqstate.AcceptedState)
			continue
		}
		if len(rrs) == 0 {
			shouldFail := r.shouldFailProvReqOrRecordUnreconciled(pr)
			klog.Warningf("ProvisioningRequest %s/%s in state %q doesn't have a matching Resize Request %q. shouldFail: %v", pr.Namespace, pr.Name, provreqstate.AcceptedState, rrName, shouldFail)
			if shouldFail {
				invalidProvisioningRequests[missingResizeRequestKey] = append(invalidProvisioningRequests[missingResizeRequestKey], pr)
			}
			continue
		}

		attemptedUpdate := false
		var err error

		if qpr.DefaultStrategy() {
			attemptedUpdate, err = r.updateProvisioningRequestInAcceptedState(rrs[0], pr)
		}
		if qpr.ObtainabilityStrategy() {
			attemptedUpdate, err = r.updateObtainabilityStrategyProvisioningRequestInAcceptedState(rrs, pr)
		}
		if err != nil {
			klog.Errorf("Error while updating Provisioning Request %s/%s during reconciling with Resize Request %s: %v", pr.Namespace, pr.Name, rrName, err)
		}
		if attemptedUpdate {
			observabilityUpdates++
		}
		delete(r.resizeRequests, rrName)
	}
	removeReconciled("resizeRequestReconciler", "reconcileActiveProvReqsAndFilterOutOldRRs", provisioningRequestMap, provreqstate.AcceptedState, reconciledFilterFn(reconciled))
}

// isResizeRequestProvisioningMode returns whether Provisioning Request should be considered as one using Resize Request API.
func isResizeRequestProvisioningMode(pr *provreqwrapper.ProvisioningRequest) bool {
	qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
	if qpr.ProvisioningMode() == nil {
		// For Resize Requests created before ProvisioningMode was introduced - Resize Request was the only available backend.
		state := provreqstate.StateOfProvisioningRequest(pr)
		return state != provreqstate.UninitializedState && state != provreqstate.PendingState
	}
	return *qpr.ProvisioningMode() == queuedwrapper.ProvisioningModeResizeRequest
}

func removeReconciled(reconciler, caller string, prs map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, state provreqstate.ProvisioningRequestState, del func(pr *provreqwrapper.ProvisioningRequest) bool) {
	if len(prs[state]) == 0 {
		return
	}
	loggingQuota := logging.ProvisioningRequestsLoggingQuota()

	prs[state] = slices.DeleteFunc(prs[state], func(pr *provreqwrapper.ProvisioningRequest) bool {
		deleting := del(pr)
		if deleting {
			klogx.V(1).UpTo(loggingQuota).Infof("%s.%s: processed Provisioning Request %s/%s", reconciler, caller, pr.Namespace, pr.Name)
		}
		return del(pr)
	})

	klogx.V(1).Over(loggingQuota).Infof("%s.%s: processed also %d other Provisioning Requests", reconciler, caller, -loggingQuota.Left())
}

func reconciledFilterFn(prs sets.Set[pods.ProvReqID]) func(*provreqwrapper.ProvisioningRequest) bool {
	return func(pr *provreqwrapper.ProvisioningRequest) bool {
		return prs.Has(pods.GetProvReqID(pr))
	}
}

// ignoreRecentlyTerminalResizeRequests filters out from deletion Resize Requests (corresponding to Provisioned/BookingExpired/CapacityRevoked/Failed Provisioning Requests) which haven't reached the clean up TTL yet
func (r *resizeRequestReconciler) ignoreRecentlyTerminalResizeRequests(provisioningRequestMap map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest) {
	provisionedStates := []provreqstate.ProvisioningRequestState{provreqstate.ProvisionedState, provreqstate.BookingExpiredState, provreqstate.CapacityRevokedState, provreqstate.FailedState}
	for _, st := range provisionedStates {
		for _, pr := range provisioningRequestMap[st] {
			if !isResizeRequestProvisioningMode(pr) {
				continue
			}
			qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
			if qpr.ObtainabilityStrategy() {
				// We shouldn't ignore any RRs corresponding to Obtainability strategy PRs but just clean them up right away.
				// Obtainability strategy Provisioning Requests can be in a terminal Provisioned state and still have some corresponding Resize Requests in active states,
				// e.g. when 1 RR gets Provisioned, but 2 RRs are still in Accepted state and the initial Cancel operation call failed for some reason.
				continue
			}

			rrName := r.resizeRequestNameFunc(pr)
			// If corresponding Resize Request was in terminal state for less than terminalResizeRequestTTL, filter it out from deletion.
			// Otherwise, let it get deleted in the `deleteInvalidResizeRequests` function.
			if !isResizeRequestOld(pr, r.now) {
				delete(r.resizeRequests, rrName)
			}
		}
	}
}

// ignoreResizeRequestsCorrespondingToOutOfSyncProvReqs filters out resize requests associated with provisioning requests that have
// a corresponding upcoming node pool from the processing in this loop.
// Upcoming node pools are initialized asynchronously, which involves creating resize requests and marking
// related provisioning requests as accepted. A state fetched by reconciler might be thus inconsistent, for example
// the resize requests are already created but provisioning requests are still pending.
func (r *resizeRequestReconciler) ignoreResizeRequestsCorrespondingToOutOfSyncProvReqs(outOfSyncPRs map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest) {
	ignored := 0
	for _, provReqState := range reconcilableStates {
		for _, pr := range outOfSyncPRs[provReqState] {
			// Note: As Pending outOfSyncPRs won't have Provisioning Mode set yet, this might include Bulk Mig ProvReqs.
			// It doesn't make any issues here - the Resize Requests just won't exist, so only an overestimated number of ignored RRs might get logged.
			rrName := r.resizeRequestNameFunc(pr)
			delete(r.resizeRequests, rrName)
			ignored++
		}
	}
	klog.Infof("Ignoring potentially %d upcoming resize requests in the reconciling loop (though some of them might have yet undeclared provisioning mode)", ignored)
}

// isResizeRequestOld returns `true` when the Resize Request was in terminal Succeeded or Failed state for more than `terminalResizeRequestTTL`
func isResizeRequestOld(pr *provreqwrapper.ProvisioningRequest, now time.Time) bool {
	for _, cond := range pr.Status.Conditions {
		if cond.Status != v1.ConditionTrue {
			continue
		}
		if cond.Type != prv1.Provisioned && cond.Type != prv1.Failed {
			continue
		}
		if cond.LastTransitionTime.Add(terminalResizeRequestTTL).Before(now) {
			return true
		}
	}
	return false
}

func (r *resizeRequestReconciler) failProvisioningRequestsMissingResizeRequests(prs []*provreqwrapper.ProvisioningRequest) {
	failedProvisioningRequests := 0
	for _, pr := range prs {
		if failedProvisioningRequests >= defaultFailedProvisioningRequestsMissingRRsPerLoop {
			break
		}
		if err := provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, missingResizeRequestFailedReason, missingResizeRequestFailedMessage, v1.NewTime(r.now)); err != nil {
			klog.Errorf("Error while modifying Provisioning Request %s/%s missing Resize Request: %v", pr.Namespace, pr.Name, err)
			continue
		}
		if _, err := r.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
			klog.Errorf("Error while updating Provisioning Request %s/%s missing Resize Request: %v", pr.Namespace, pr.Name, err)
			continue
		}
		delete(r.firstUnsuccessfulReconciliationMap, pods.GetProvReqID(pr))
		klog.Warningf("ProvisioningRequest %s/%s Failed because its corresponding Resize Request was missing.", pr.Namespace, pr.Name)
		failedProvisioningRequests++
	}
}

// deleteInvalidResizeRequestsAndFailInvalidProvReqs deletes the duplicated ResizeRequests (all duplicates with the same name, even if that's more than `defaultDeletedResizeRequestsPerLoop` allows), fails their corresponding Provisioning Requests and deletes the unreconciled ResizeRequests which don't have a ProvisioningRequest.
func (r *resizeRequestReconciler) deleteInvalidResizeRequestsAndFailInvalidProvReqs(invalidProvisioningRequests map[string][]*provreqwrapper.ProvisioningRequest) {
	ctx := context.Background()
	for _, rrs := range r.resizeRequests {
		if r.rrDeleteCalls >= defaultDeletedResizeRequestsPerLoop {
			break
		}

		rrName := rrs[0].Name // They all have the same name
		if len(rrs) > 1 {
			klog.Infof("%d instances of GKE managed Resize Request with name %q will be cleaned up.", len(rrs), rrName)
		}

		for _, pr := range invalidProvisioningRequests[rrName] {
			if err := provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, duplicateResizeRequestFailedReason, duplicateResizeRequestFailedMessage, v1.NewTime(r.now)); err != nil {
				klog.Errorf("Error while modifying Provisioning Request %s/%s during reconciling with Resize Request %s: %v", pr.Namespace, pr.Name, rrName, err)
				continue
			}
			if _, err := r.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
				klog.Errorf("Error while updating Provisioning Request %s/%s during reconciling with Resize Request %s: %v", pr.Namespace, pr.Name, rrName, err)
				continue
			}
			klog.Warningf("ProvisioningRequest %s/%s Failed because there were multiple Resize Requests with the same name.", pr.Namespace, pr.Name)
		}

		for _, rr := range rrs {
			// AdvanceResizeRequestCleanUp might update AlreadyReportedState to ToBeReportedState when Cancel operation finishes successfully, but for FSQ we want to just ignore that.
			// We only want to log about RR clean up once, when we first time trigger AdvanceResizeRequestCleanUp, so we only care about the initial state and always want to set AlreadyReportedState.
			shouldLog := r.rrClient.ReportState(rr) != resizerequestclient.AlreadyReportedState
			r.rrDeleteCalls++
			err := r.rrClient.AdvanceResizeRequestCleanUp(ctx, rr)
			if err != nil {
				klog.Errorf("Couldn't trigger clean up for Resize Request %s: %v", rr.Ref(), err)
				continue
			}
			if shouldLog {
				if rr.State == resizerequestclient.ResizeRequestStateSucceeded || rr.State == resizerequestclient.ResizeRequestStateFailed || rr.State == resizerequestclient.ResizeRequestStateCancelled {
					klog.Infof("Resize Request %s in terminal state %s, triggered delete operation.", rr.Ref(), rr.State)
				} else {
					klog.Warningf("Resize Request %s in state %s doesn't have a corresponding Provisioning Request, triggered cancel/delete operation.", rr.Ref(), rr.State)
				}
			}
			r.rrClient.SetReportState(rr, resizerequestclient.AlreadyReportedState)
		}
	}
}

// retrieveResizeRequests returns GKE managed ResizeRequests and separates the duplicate ones.
func (r *resizeRequestReconciler) retrieveResizeRequests(migs map[gce.GceRef]common.GkeMigWrapper, now time.Time) error {
	resizeRequests := make(map[string][]resizerequestclient.ResizeRequestStatus)

	for mig := range migs {
		resReqsInMig, err := r.rrClient.ResizeRequests(context.Background(), mig)
		if err != nil {
			return fmt.Errorf("couldn't retrieve Resize Requests from mig %q in zone %q: %w", mig.Name, mig.Zone, err)
		}

		for _, rr := range resReqsInMig {
			if !resizerequestclient.IsProvisioningRequestManagedResizeRequest(rr.Name) {
				continue
			}
			resizeRequests[rr.Name] = append(resizeRequests[rr.Name], rr)
		}
	}
	r.now = now
	r.migs = migs
	r.resizeRequests = resizeRequests
	r.rrDeleteCalls = 0
	return nil
}

const (
	deletingMessage  = "Provisioning Request failed because Resize Request was being deleted."
	deletingReason   = "InternalErrorResizeRequestWasBeingDeleted"
	cancelledMessage = "Provisioning Request failed because Resize Request got cancelled."
	cancelledReason  = "InternalErrorResizeRequestGotCancelled"
	creatingMessage  = "Provisioning Request failed because Resize Request was creating."
	creatingReason   = "InternalErrorResizeRequestWasCreating"
)

// updateProvisioningRequestInAcceptedState returns the updated Provisioning Request, a boolean denoting whether there was a k8s API call attempt and an error.
func (r *resizeRequestReconciler) updateProvisioningRequestInAcceptedState(resizeReq resizerequestclient.ResizeRequestStatus, pr *provreqwrapper.ProvisioningRequest) (bool, error) {
	timestampV1 := v1.NewTime(r.now)
	var err error
	switch resizeReq.State {
	case resizerequestclient.ResizeRequestStateAccepted, resizerequestclient.ResizeRequestStateProvisioning:
		// Since the Resize Request state is as expected, reset the `firstUnsuccessfulReconciliationMap` entry
		delete(r.firstUnsuccessfulReconciliationMap, pods.GetProvReqID(pr))
		if len(resizeReq.LastAttemptErrors) <= 0 {
			return false, nil
		}
		rrErrorInfo := reasons.GetDwsErrorInfoFromLastAttemptErrors(resizeReq.Ref(), resizeReq.LastAttemptErrors)
		if conditionChanged := provreqstate.UpdateOrSetProvisioningRequestCondition(pr, prv1.Provisioned, v1.ConditionFalse, rrErrorInfo.Reason, rrErrorInfo.Message, timestampV1); !conditionChanged {
			return false, nil
		}
	case resizerequestclient.ResizeRequestStateSucceeded:
		err = provreqstate.SetState(pr, provreqstate.ProvisionedState, timestampV1)
		var nodePoolAutoProvisioned string
		qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
		if autoProvisioned := qpr.NodePoolAutoProvisioned(); autoProvisioned != nil {
			nodePoolAutoProvisioned = *autoProvisioned
		}
		metrics.Metrics.ObserveProvisioningRequestQueueWaitDurationSeconds(nodePoolAutoProvisioned, time.Since(pr.CreationTimestamp.Time))
	case resizerequestclient.ResizeRequestStateFailed:
		rrErrorInfo := reasons.GetDwsErrorInfoFromResizeRequestErrors(resizeReq.Ref(), resizeReq.Errors, reasons.DefaultSurfacedErrorsLimit)
		err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, rrErrorInfo.Reason, rrErrorInfo.Message, timestampV1)
	case resizerequestclient.ResizeRequestStateDeleting:
		klog.Warningf("Provisioning Request %s/%s in state %s has its corresponding Resize Request %s in Deleting state.", pr.Namespace, pr.Name, provreqstate.AcceptedState, resizeReq.Name)
		if !r.shouldFailProvReqOrRecordUnreconciled(pr) {
			return false, nil
		}
		err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, deletingReason, deletingMessage, timestampV1)
	case resizerequestclient.ResizeRequestStateCancelled:
		klog.Warningf("Provisioning Request %s/%s in state %s has its corresponding Resize Request %s in Cancelled state.", pr.Namespace, pr.Name, provreqstate.AcceptedState, resizeReq.Name)
		if !r.shouldFailProvReqOrRecordUnreconciled(pr) {
			return false, nil
		}
		err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, cancelledReason, cancelledMessage, timestampV1)
	case resizerequestclient.ResizeRequestStateCreating:
		klog.Warningf("Provisioning Request %s/%s in state %s has its corresponding Resize Request %s in Creating state.", pr.Namespace, pr.Name, provreqstate.AcceptedState, resizeReq.Name)
		if !r.shouldFailProvReqOrRecordUnreconciled(pr) {
			return false, nil
		}
		err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, creatingReason, creatingMessage, timestampV1)
	default: // unknown Resize Request states
		err = fmt.Errorf("Provisioning Request %s/%s has state %q and corresponding Resize Request %s has %q state", pr.Namespace, pr.Name, provreqstate.AcceptedState, resizeReq.Name, resizeReq.State)
	}
	if err != nil {
		return false, fmt.Errorf("failed to modify the Provisioning Request: %w", err)
	}
	_, err = r.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest)
	// Since we've updated the state, reset the `firstUnsuccessfulReconciliationMap` entry
	delete(r.firstUnsuccessfulReconciliationMap, pods.GetProvReqID(pr))
	return true, err
}

func (r *resizeRequestReconciler) shouldFailProvReqOrRecordUnreconciled(pr *provreqwrapper.ProvisioningRequest) bool {
	return shouldFailProvReqOrRecordUnreconciled(r.firstUnsuccessfulReconciliationMap, pr, r.now)
}
