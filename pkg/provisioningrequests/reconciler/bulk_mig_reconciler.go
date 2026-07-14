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
	"k8s.io/apimachinery/pkg/util/sets"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/bulkmig"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler/reasons"
	klog "k8s.io/klog/v2"
)

const (
	missingDetailsReason          = "InternalErrorMissingDetails"
	missingDetailsMessage         = "Provisioning Request was Accepted but didn't have identifying details assigned."
	missingBulkMigFailedReason    = "InternalErrorMissingBulkMig"
	missingBulkMigFailedMessage   = "The corresponding Bulk Mig was missing."
	noQueueingInProgressReason    = "InternalErrorBulkMigNotInProgress"
	noQueueingInProgressMessage   = "The corresponding Bulk Mig doesn't have queueing in progress."
	bulkMigUnexpectedStateReason  = "InternalErrorBulkMigUnexpectedState"
	bulkMigUnexpectedStateMessage = "The corresponding Bulk Mig has unexpected state."
)

type bulkMigReconciler struct {
	prClient           provreqClient
	bulkMigClient      bulkmig.GceMigClient
	experimentsManager experiments.Manager
	projectId          string

	firstUnsuccessfulReconciliationMap map[pods.ProvReqID]time.Time

	bulkMigNodesImmunityStart map[string]QueuedNodeImmunityEntry
	bulkMigsStatuses          map[gce.GceRef]bulkmig.Status
}

func (r *bulkMigReconciler) enabled() bool {
	return r.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ProvisioningRequestBulkMigsFlag, false)
}

func (r *bulkMigReconciler) reconcileRequests(in *reconcilingInput) (map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, error) {
	if !r.enabled() {
		return in.prs, nil
	}
	r.fetchBulkMigStatuses(in.bulkMigs)

	r.bulkMigNodesImmunityStartUpdate(in.prs)

	r.recoverPendingProvReqs(in.prs, in.now)
	r.reconcileAcceptedProvReqs(in.prs, in.acceptedProvReqUpdatesPerLoopLimit, in.now)
	r.resolveUnreconciledBulkMigs(in.prsOutOfSync, bulkMigsByRRLabel(in.bulkMigs))

	r.bulkMigsStatuses = nil
	return in.prs, nil
}

func (r *bulkMigReconciler) reconcileAcceptedProvReqs(provReqsToReconcile map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, updatesLimit int, now time.Time) {
	if provReqsToReconcile == nil {
		return
	}
	reconciled := sets.Set[pods.ProvReqID]{}
	apiCalls := 0
	for _, pr := range provReqsToReconcile[provreqstate.AcceptedState] {
		if isBulkMigProvisioningMode(pr) {
			// ProvReq might not get reconciled due to acceptedProvReqUpdatesLimit, in that case we just ignore it,
			// but it's treated as reconciled anyway so that the following reconcilers won't try to handle it
			reconciled.Insert(pods.GetProvReqID(pr))

			if apiCalls < updatesLimit {
				if ok := r.reconcileAccepted(pr, now); ok {
					apiCalls++
				}
			} else {
				// Delete corresponding BulkMig from the map to ignore it in further processing
				migRef, _, _, _ := r.bulkMigForProvReq(pr)
				delete(r.bulkMigsStatuses, migRef)
			}
		}
	}
	removeReconciled("bulkMigReconciler", "reconcileAcceptedProvReqs", provReqsToReconcile, provreqstate.AcceptedState, reconciledFilterFn(reconciled))
}

// isBulkMigProvisioningMode returns whether Provisioning Request should be considered as one using Bulk Mig API.
func isBulkMigProvisioningMode(pr *provreqwrapper.ProvisioningRequest) bool {
	qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
	return qpr.ProvisioningMode() != nil && *qpr.ProvisioningMode() == queuedwrapper.ProvisioningModeBulkMig
}

func (r *bulkMigReconciler) reconcileAccepted(pr *provreqwrapper.ProvisioningRequest, now time.Time) bool {
	var err error
	timestampV1 := v1.NewTime(now)

	migRef, bmig, found, noDetailsErr := r.bulkMigForProvReq(pr)
	delete(r.bulkMigsStatuses, migRef)

	switch {
	case noDetailsErr != nil:
		klog.Errorf("Couldn't retrieve Bulk Mig corresponding to Provisioning Request %s/%s: %v", pr.Namespace, pr.Name, noDetailsErr)
		err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, missingDetailsReason, missingDetailsMessage, timestampV1)

	case !found:
		klog.Warningf("Provisioning Request %s/%s is missing corresponding Bulk Mig %v", pr.Namespace, pr.Name, migRef)
		if !r.shouldFailProvReqOrRecordUnreconciled(pr, now) {
			return false
		}
		err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, missingBulkMigFailedReason, missingBulkMigFailedMessage, timestampV1)

	case bmig.InProgress && bmig.TargetSize > 0:
		errorInfo := reasons.GetDwsErrorInfoFromLastProgressCheckErrors(bmig.Ref.Name, bmig.LastProgressCheckErrors)
		if conditionChanged := provreqstate.UpdateOrSetProvisioningRequestCondition(pr, prv1.Provisioned, v1.ConditionFalse, errorInfo.Reason, errorInfo.Message, timestampV1); !conditionChanged {
			// Since the BulkMig state is as expected, reset the `firstUnsuccessfulReconciliationMap` entry
			delete(r.firstUnsuccessfulReconciliationMap, pods.GetProvReqID(pr))
			return false
		}

	case !bmig.InProgress && bmig.TargetSize > 0:
		err = provreqstate.SetState(pr, provreqstate.ProvisionedState, timestampV1)

	case !bmig.InProgress && bmig.TargetSize == 0:
		klog.Warningf("Provisioning Request %s/%s corresponding Bulk Mig %v doesn't have queueing in progress", pr.Namespace, pr.Name, migRef)
		if !r.shouldFailProvReqOrRecordUnreconciled(pr, now) {
			return false
		}
		err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, noQueueingInProgressReason, noQueueingInProgressMessage, timestampV1)

	case bmig.InProgress && bmig.TargetSize == 0:
		klog.Errorf("Provisioning Request %s/%s corresponding Bulk Mig %v has unexpected state: queueing in progress and targetSize = 0", pr.Namespace, pr.Name, migRef)
		err = provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, bulkMigUnexpectedStateReason, bulkMigUnexpectedStateMessage, timestampV1)
	}
	if err != nil {
		klog.Errorf("Failed to modify the Provisioning Request  %s/%s during reconciling with bulkMig %s: %v", pr.Namespace, pr.Name, migRef, err)
		return false
	}

	_, err = r.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest)
	if err != nil {
		klog.Errorf("Error while updating Provisioning Request %s/%s during reconciling with bulkMig %s: %v", pr.Namespace, pr.Name, migRef, err)
	}
	// Since we've updated the state, reset the `firstUnsuccessfulReconciliationMap` entry
	delete(r.firstUnsuccessfulReconciliationMap, pods.GetProvReqID(pr))
	return true
}

func (r *bulkMigReconciler) shouldFailProvReqOrRecordUnreconciled(pr *provreqwrapper.ProvisioningRequest, now time.Time) bool {
	return shouldFailProvReqOrRecordUnreconciled(r.firstUnsuccessfulReconciliationMap, pr, now)
}

func (r *bulkMigReconciler) recoverPendingProvReqs(provReqsToReconcile map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, now time.Time) {
	if provReqsToReconcile == nil {
		return
	}
	var err error
	reconciled := sets.Set[pods.ProvReqID]{}
	for _, pr := range provReqsToReconcile[provreqstate.PendingState] {
		if !isBulkMigProvisioningMode(pr) {
			continue
		}
		migRef, bmig, found, noDetailsErr := r.bulkMigForProvReq(pr)
		if noDetailsErr != nil {
			continue
		}
		reconciled.Insert(pods.GetProvReqID(pr))

		if found && bmig.InProgress {
			err = updateProvisioningRequestToAcceptedState(r.prClient, pr, now)
			if err != nil {
				klog.Errorf("Error while updating Pending Provisioning Request %s/%s during reconciling with BulkMig %s: %v", pr.Namespace, pr.Name, migRef, err)
			} else {
				klog.Infof("Reconciled pending Provisioning Request %s/%s with BulkMig %s.", pr.Namespace, pr.Name, migRef)
			}
			delete(r.bulkMigsStatuses, migRef)
			continue
		}
		err = clearProvisioningRequestDetails(r.prClient, pr)
		if err != nil {
			klog.Errorf("Error while clearing Pending Provisioning Request %s/%s MIG name details: %v", pr.Namespace, pr.Name, err)
		}
	}
	removeReconciled("bulkMigReconciler", "recoverPendingProvReqs", provReqsToReconcile, provreqstate.PendingState, reconciledFilterFn(reconciled))
}

// resolveUnreconciledBulkMigs checks whether any of the unreconciled BulkMigs has bulk instance operation in progress.
// If yes, the operation is cancelled by setting TargetSize to 0 due to missing Provisioning Request.
// This method assumes bulkMigReconciler receives only QueuedProvisioning BulkMigs' refs, as provided by `queuedProvisioningMigGceRefs` in GKE manager.
func (r *bulkMigReconciler) resolveUnreconciledBulkMigs(provReqsOutOfSync map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, bulkMigsByRRLabel map[string]gce.GceRef) {
	r.ignoreBulkMigsCorrespondingToOutOfSyncProvReqs(provReqsOutOfSync, bulkMigsByRRLabel)

	for _, bulkMig := range r.bulkMigsStatuses {
		if !bulkMig.InProgress {
			continue
		}

		klog.Warningf("Bulk Mig %s doesn't have a corresponding Provisioning Request but has bulk instance operation is in progress. Operation will be cancelled, i.e. TargetSize will be set to 0.", bulkMig.Ref)
		err := r.bulkMigClient.SetZeroTargetSize(bulkMig.Ref)
		if err != nil {
			klog.Errorf("Couldn't set mig %s target size to 0: %v", bulkMig.Ref, err)
		}
	}
}

func (r *bulkMigReconciler) ignoreBulkMigsCorrespondingToOutOfSyncProvReqs(provReqsOutOfSync map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, bulkMigsByRRLabel map[string]gce.GceRef) {
	for _, state := range reconcilableStates {
		for _, pr := range provReqsOutOfSync[state] {
			// If the Provisioning Request has the details populated, the MIG can be found by NodeGroupName.
			migRef, _, _, noDetailsErr := r.bulkMigForProvReq(pr)
			if noDetailsErr == nil {
				delete(r.bulkMigsStatuses, migRef)
				continue
			}
			// Otherwise, if the Provisioning Request details are not populated, the MIG can be found by the RR label.
			// Bulk MIGs don't use Resize Requests for scale ups, but do have the RR label set.
			// This behavior is explained in ProvisioningRequestGenerator.UpdateNodePoolSpec.
			rrName := resizerequestclient.ResizeRequestName(pr.Namespace, pr.Name)
			if migRef, found := bulkMigsByRRLabel[rrName]; found {
				delete(r.bulkMigsStatuses, migRef)
				continue
			}
		}
	}
}

func (r *bulkMigReconciler) bulkMigForProvReq(pr *provreqwrapper.ProvisioningRequest) (gce.GceRef, bulkmig.Status, bool, error) {
	qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
	if qpr.NodeGroupName() == nil || *qpr.NodeGroupName() == "" || qpr.SelectedZone() == nil || *qpr.SelectedZone() == "" {
		return gce.GceRef{}, bulkmig.Status{}, false, fmt.Errorf("Provisioning Requests %s/%s is missing NodeGroupName/SelectedZone, got details: %+v", pr.Namespace, pr.Name, pr.Status.ProvisioningClassDetails)
	}
	if qpr.ResizeRequestName() != nil && *qpr.ResizeRequestName() != "" {
		return gce.GceRef{}, bulkmig.Status{}, false, fmt.Errorf("Provisioning Requests %s/%s has Resize Request details: %+v", pr.Namespace, pr.Name, pr.Status.ProvisioningClassDetails)
	}

	migRef := gce.GceRef{Name: *qpr.NodeGroupName(), Zone: *qpr.SelectedZone(), Project: r.projectId}
	bulkMig, ok := r.bulkMigsStatuses[migRef]
	return migRef, bulkMig, ok, nil
}

func (r *bulkMigReconciler) fetchBulkMigStatuses(bulkMigs map[gce.GceRef]common.GkeMigWrapper) {
	bulkMigsStatuses := make(map[gce.GceRef]bulkmig.Status, len(bulkMigs))
	for migRef := range bulkMigs {
		bulkMigStatus, err := r.bulkMigClient.BulkMigStatus(migRef)
		if err != nil {
			klog.Errorf("Couldn't fetch beta BulkMigStatus for mig %+v; %v", migRef, err)
		}
		bulkMigsStatuses[migRef] = bulkMigStatus
	}
	r.bulkMigsStatuses = bulkMigsStatuses
}

func (r *bulkMigReconciler) nodeHasScaleDownImmunity(node *apiv1.Node, migSpec *QueuedProvisioningMigSpec, now time.Time) bool {
	if !r.enabled() || migSpec == nil || migSpec.ProvisioningMode != BulkMigProvisioningMode {
		return false
	}

	if r.bulkMigNodesImmunityStart == nil {
		// Use creation timestamp fallback if the immunity map is currently invalidated
		return isNodeImmune(migSpec.ProvisioningMode, node, migSpec.Immunity, QueuedNodeImmunityEntry{useNodeCreationTimestamp: true}, now)
	}

	migName := migSpec.GceRef.Name
	immunityEntry, found := r.bulkMigNodesImmunityStart[migName]
	if !found {
		return false
	}
	return isNodeImmune(migSpec.ProvisioningMode, node, migSpec.Immunity, immunityEntry, now)
}

func (r *bulkMigReconciler) bulkMigNodesImmunityStartUpdate(provReqsToReconcile map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest) {
	migNameKeyFn := func(pr *provreqwrapper.ProvisioningRequest) (string, bool) {
		if !isBulkMigProvisioningMode(pr) {
			return "", false
		}
		qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
		if qpr.NodeGroupName() == nil || *qpr.NodeGroupName() == "" {
			klog.Warningf("Provisioning Request  %s/%s in state %s with Bulk Mig provisioning mode didn't have MIG name detail set", pr.Namespace, pr.Name, provreqstate.StateOfProvisioningRequest(pr))
			return "", false
		}
		return *qpr.NodeGroupName(), true
	}
	r.bulkMigNodesImmunityStart = refreshQueuedProvisioningNodesImmunityStart(provReqsToReconcile, migNameKeyFn)
}

func (r *bulkMigReconciler) queuedNodesImmunityStartInvalidate() {
	if !r.enabled() {
		return
	}
	r.bulkMigNodesImmunityStart = nil
}

func bulkMigsByRRLabel(bulkMigs map[gce.GceRef]common.GkeMigWrapper) map[string]gce.GceRef {
	// Bulk MIGs don't use Resize Requests for scale ups, but do have the RR label set.
	// This behavior is explained in ProvisioningRequestGenerator.UpdateNodePoolSpec.
	bulkMigsByRRLabel := make(map[string]gce.GceRef)

	for migRef, mig := range bulkMigs {
		if spec := mig.Spec(); spec != nil && spec.Labels != nil {
			if rrName, found := spec.Labels[gkelabels.ProvisioningRequestLabelKey]; found {
				bulkMigsByRRLabel[rrName] = migRef
			}
		}
	}

	return bulkMigsByRRLabel
}
