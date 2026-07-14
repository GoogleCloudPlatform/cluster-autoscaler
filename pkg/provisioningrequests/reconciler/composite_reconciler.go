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
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/bulkmig"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	klog "k8s.io/klog/v2"
)

const (
	// maxUnreconciledPeriod denotes the time during which the GCE object corresponding to the given Provisioning Request can be missing or in an 'incorrect' state
	// This is necessary to correctly handle the upgrade of QueuedProvisioning nodepool, which can temporarily cause unexpected states
	maxUnreconciledPeriod = 2 * time.Minute
	// MaxObtainabilityStrategyUnreconciledPeriod needs to be longer than maxUnreconciledPeriod, because we need to account for HTNAP having to create the node pool and finish the scale up
	// TODO(b/495348466): consider setting this via experiment to enable addressing scenarios where the 10min turns out not to be enough
	MaxObtainabilityStrategyUnreconciledPeriod = 10 * time.Minute
	// defaultAcceptedProvReqUpdatesLimit limits the updates per refresh to avoid starving the scale-up logic
	defaultAcceptedProvReqUpdatesLimit = 10
)

var (
	reconcilableStates = []provreqstate.ProvisioningRequestState{
		provreqstate.UninitializedState,
		provreqstate.PendingState,
		provreqstate.AcceptedState,
		provreqstate.ProvisionedState,
		provreqstate.BookingExpiredState,
		provreqstate.CapacityRevokedState,
		provreqstate.FailedState,
	}

	allNonTerminalStates = []provreqstate.ProvisioningRequestState{
		provreqstate.UninitializedState,
		provreqstate.PendingState,
		provreqstate.AcceptedState,
	}
)

type QueuedProvisioningMode int

const (
	// ResizeRequestProvisioningMode denotes the Queued Provisioning Mig uses Resize Request API to provision capacity.
	ResizeRequestProvisioningMode QueuedProvisioningMode = iota
	// ResizeRequestMode denotes the Queued Provisioning Mig uses Bulk Mig API to provision capacity.
	BulkMigProvisioningMode
)

func (mode QueuedProvisioningMode) String() string {
	switch mode {
	case ResizeRequestProvisioningMode:
		return "Resize Request"
	case BulkMigProvisioningMode:
		return "Bulk Mig"
	default:
		return fmt.Sprintf("InvalidQueuedProvisioningMode=%d", mode)
	}
}

// QueuedProvisioningMigSpec contains GkeMig specs needed to decide the node's scale down immunity.
type QueuedProvisioningMigSpec struct {
	// GceRef is the Mig's GCE object reference.
	GceRef gce.GceRef
	// ProvisioningMode denotes whether the Queued Provisioning Mig uses Bulk Mig or Resize Request API to provision capacity.
	ProvisioningMode QueuedProvisioningMode
	// Immunity is the additional scale down immunity duration which is granted to the node.
	Immunity time.Duration
}

type compositeProvisioningRequestReconciler struct {
	prClient           provreqClient
	prCache            *provreqcache.QueuedProvisioningCache
	experimentsManager experiments.Manager

	reconcilers []reconcilingProcessor
}

type reconcilingInput struct {
	rrMigs, bulkMigs                   map[gce.GceRef]common.GkeMigWrapper
	prs, prsOutOfSync                  map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest
	acceptedProvReqUpdatesPerLoopLimit int
	now                                time.Time
}

type reconcilingProcessor interface {
	reconcileRequests(in *reconcilingInput) (map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest, error)
	queuedNodesImmunityStartInvalidate()
	nodeHasScaleDownImmunity(node *apiv1.Node, migSpec *QueuedProvisioningMigSpec, now time.Time) bool
}

func NewCompositeProvisioningRequestReconciler(client provreqClient, prCache *provreqcache.QueuedProvisioningCache, rrClient resizerequestclient.ResizeRequestClient, bulkMigClient bulkmig.GceMigClient, experimentsManager experiments.Manager, projectId string) (*compositeProvisioningRequestReconciler, error) {
	if client == nil {
		return nil, errors.New("cannot create compositeProvisioningRequestReconciler: Provisioning Request client is nil")
	}
	if prCache == nil {
		return nil, errors.New("cannot create compositeProvisioningRequestReconciler: Provisioning Request cache is nil")
	}
	if experimentsManager == nil {
		return nil, errors.New("cannot create compositeProvisioningRequestReconciler: Experiments Manager is nil")
	}
	if rrClient == nil {
		return nil, errors.New("cannot create compositeProvisioningRequestReconciler: Resize Request client is nil")
	}
	if bulkMigClient == nil {
		return nil, errors.New("cannot create compositeProvisioningRequestReconciler: Bulk Mig client is nil")
	}
	return &compositeProvisioningRequestReconciler{
		prClient:           client,
		prCache:            prCache,
		experimentsManager: experimentsManager,
		reconcilers: []reconcilingProcessor{
			// Feature-guard reconciler fails ProvReq with the given property when the relevant experiment is disabled,
			// thus it should be ran as the 1st one to prevent further processing of those ProvReqs.
			NewFeatureGuardReconciler(client, experimentsManager),
			// Valid-until reconciler fails ProvReq when ValidUntilSeconds has elapsed since its CreationTime.
			NewValidUntilReconciler(client),
			NewObtainabilityZoneSelectorReconciler(client),
			// IMPORTANT: Since each reconcilingProcessor filters out ProvisioningRequest which it handled, the order of processors is important
			// This is due to Uninitialized/Pending Provisioning Requests being indistinguishable in terms of using Resize Requests or Bulk Mig API,
			// this is only decided when Provisioning Request gets chosen for scale up and Accepted, not earlier.
			&resizeRequestReconciler{
				prClient:                           client,
				rrClient:                           rrClient,
				resizeRequestNameFunc:              provisioningRequestResizeRequestName,
				resizeRequestNodesImmunityStart:    nil,
				firstUnsuccessfulReconciliationMap: map[pods.ProvReqID]time.Time{},
			},
			&bulkMigReconciler{
				prClient:                           client,
				bulkMigClient:                      bulkMigClient,
				experimentsManager:                 experimentsManager,
				projectId:                          projectId,
				firstUnsuccessfulReconciliationMap: map[pods.ProvReqID]time.Time{},
			},
			&provisioningRequestInitializer{
				prClient: client,
			},
			&provisioningRequestFinalizer{
				prClient: client,
			},
		},
	}, nil
}

// ReconcileRequests updates statuses of Provisioning Requests:
// -  based on states of their corresponding Resize Requests for regular QueuedProvisioning migs
// -  based on states of their corresponding `bulkInstanceOperation` status for BulkMig QueuedProvisioning migs
// - initializes new Provisioning Requests which were not Accepted (i.e. not chosen for scale up) yet, thus were not assigned the ResizeRequest/BulkMig.
// - updates Provisioned -> BookingExpired -> CapacityRevoked and cleans up CapacityRevoked/Failed Provisioning Requests
func (r *compositeProvisioningRequestReconciler) ReconcileRequests(rrQueuedMigs, bulkQueuedMigs map[gce.GceRef]common.GkeMigWrapper, now time.Time) error {
	prs, provReqsOutOfSync, err := r.fetchActionableProvisioningRequests()
	if err != nil {
		r.queuedNodesImmunityStartInvalidate()
		return fmt.Errorf("compositeProvisioningRequestReconciler: couldn't fetch Provisioning Requests: %w", err)
	}
	accUpdatesPerLoop := r.experimentsManager.EvaluateIntFlagOrFailsafe(experiments.ProvisioningRequestAcceptedUpdatesPerLoopLimitFlag, defaultAcceptedProvReqUpdatesLimit)

	for i, reconciler := range r.reconcilers {
		prs, err = reconciler.reconcileRequests(&reconcilingInput{
			rrMigs:                             rrQueuedMigs,
			bulkMigs:                           bulkQueuedMigs,
			prs:                                prs,
			prsOutOfSync:                       provReqsOutOfSync,
			acceptedProvReqUpdatesPerLoopLimit: accUpdatesPerLoop,
			now:                                now,
		})
		if err != nil {
			return fmt.Errorf("compositeProvisioningRequestReconciler: got error when reconciling requests from  reconciler %T (%d/%d): %w", reconciler, i, len(r.reconcilers), err)
		}
		klogx.V(4).Infof("ProvReqs left to reconcile after reconciler[%d]: %T: %s", i, reconciler, prsLeft(prs))
	}

	return nil
}

func prsLeft(prsMap map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest) string {
	left := []string{}
	for state, prs := range prsMap {
		left = append(left, fmt.Sprintf("%s: %d", state, len(prs)))
	}
	return strings.Join(left, ", ")
}

// fetchActionableProvisioningRequests fetches all PRs first to avoid reconciling the same PR twice in the same loop.
// Some of the PRs may be in an inconsistent state relative to the existing resize requests ("out of sync") due to
// being covered by an asynchronous autoprovisioning (HTNAP) and initial scale-up flow. There are two possibilities for a PR:
//   - A corresponding node pool is upcoming or was recently finished creating - the PR and a corresponding RR shouldn't be reconciled,
//     as it will be properly processed when the node pool creation finishes.
//   - All async node pool operations finished, so the PR can be fully processed.
func (r *compositeProvisioningRequestReconciler) fetchActionableProvisioningRequests() (
	provReqsToReconcile map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest,
	provReqsOutOfSync map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest,
	err error,
) {
	provReqsToReconcile = map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{}
	provReqsOutOfSync = map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{}
	for _, prState := range reconcilableStates {
		prsInState, err := provreqstate.ProvisioningRequestsInState(r.prClient, prState)
		if err != nil {
			return nil, nil, fmt.Errorf("couldn't retrieve Provisioning Requests in state %q: %w", prState, err)
		}

		withUpcomingNP, withoutUpcomingNP := r.prCache.SplitByAsyncNAPStatus(prsInState)
		provReqsToReconcile[prState] = withoutUpcomingNP
		provReqsOutOfSync[prState] = withUpcomingNP
	}

	sortAcceptedProvReqsByLastUpdate(provReqsToReconcile[provreqstate.AcceptedState])

	return provReqsToReconcile, provReqsOutOfSync, nil
}

func sortAcceptedProvReqsByLastUpdate(acceptedProvReqs []*provreqwrapper.ProvisioningRequest) {
	// As we limit API calls to update Accepted ProvReqs in a single loop,
	// sort Accepted Provisioning Requests in order of the oldest Provisioned condition timestamp - the last observability status update.
	// If there's no Provisioned condition yet, use the Accepted condition timestamp.
	sort.Slice(acceptedProvReqs, func(i, j int) bool {
		iLastObservabilityUpdate := lastObservabilityUpdateTimestamp(acceptedProvReqs[i])
		jLastObservabilityUpdate := lastObservabilityUpdateTimestamp(acceptedProvReqs[j])
		return iLastObservabilityUpdate.Before(jLastObservabilityUpdate)
	})
}

func lastObservabilityUpdateTimestamp(pr *provreqwrapper.ProvisioningRequest) time.Time {
	if lastObservabilityUpdate, err := getLastTransitionTimestamp(pr, provreqstate.ProvisionedState); err == nil {
		return lastObservabilityUpdate
	}
	if accepteStateTransitionTime, err := getLastTransitionTimestamp(pr, provreqstate.AcceptedState); err == nil {
		return accepteStateTransitionTime
	}
	return pr.CreationTimestamp.Time
}

func (r *compositeProvisioningRequestReconciler) queuedNodesImmunityStartInvalidate() {
	for _, reconciler := range r.reconcilers {
		reconciler.queuedNodesImmunityStartInvalidate()
	}
}

// QueuedProvisioningNodeHasScaleDownImmunity returns true if any of the reconcilers returns true,
// i.e. any of the reconcilers decided that the provided QueuedProvisioning node still shouldn't get scaled down (additionalImmunity hasn't ran out yet).
// Currently only resizeRequestReconciler and bulkMigReconciler are able to grant immunity.
func (r *compositeProvisioningRequestReconciler) QueuedProvisioningNodeHasScaleDownImmunity(node *apiv1.Node, migSpec *QueuedProvisioningMigSpec, now time.Time) bool {
	for _, reconciler := range r.reconcilers {
		if reconciler.nodeHasScaleDownImmunity(node, migSpec, now) {
			return true
		}
	}
	return false
}

type QueuedNodeImmunityEntry struct {
	immunityStartTimestamp   time.Time
	useNodeCreationTimestamp bool
	location                 string
}

func NewQueuedNodeImmunityEntry(immunityStartTimestamp time.Time, useNodeCreationTimestamp bool, location string) QueuedNodeImmunityEntry {
	return QueuedNodeImmunityEntry{
		immunityStartTimestamp:   immunityStartTimestamp,
		useNodeCreationTimestamp: useNodeCreationTimestamp,
		location:                 location,
	}
}

func (e *QueuedNodeImmunityEntry) ImmunityStartTimestamp() time.Time {
	return e.immunityStartTimestamp
}

func (e *QueuedNodeImmunityEntry) UseNodeCreationTimestamp() bool {
	return e.useNodeCreationTimestamp
}

func refreshQueuedProvisioningNodesImmunityStart(
	provisioningRequestMap map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest,
	keyFromProvReqFn func(*provreqwrapper.ProvisioningRequest) (string, bool),
) map[string]QueuedNodeImmunityEntry {
	immunityStart := map[string]QueuedNodeImmunityEntry{}

	// For Provisioned PRs, retrieve Provisioned timestamps
	for _, pr := range provisioningRequestMap[provreqstate.ProvisionedState] {
		key, ok := keyFromProvReqFn(pr)
		if !ok {
			continue
		}
		provisionedTime, err := getLastTransitionTimestamp(pr, provreqstate.ProvisionedState)
		if err != nil {
			klog.Errorf("Couldn't retrieve immunity timestamp for Provisioning Request %s/%s for key %q: %v", pr.Namespace, pr.Name, key, err)
			continue
		}
		selectedZone := ""
		if sz := queuedwrapper.ToQueuedProvisioningRequest(*pr).SelectedZone(); sz != nil {
			selectedZone = *sz
		}
		immunityStart[key] = NewQueuedNodeImmunityEntry(provisionedTime, false, selectedZone)
	}

	// For Accepted PRs, measure immunity from node CreationTimestamp to avoid deleting nodes while request is still being provisioned.
	// Note: That means all the machines from Provisioning Request have to get provisioned in 10 minutes,
	// otherwise the already provisioned nodes might start getting scaled-down if unused.
	// As the machines are getting provisioned atomically, they should always get provisioned in the 10 minute period.
	// Previously, we were filtering them out indefinitely when corresponding ProvisioningRequest was Accepted,
	// causing a bug where nodes weren't scaled down in case of reusing the Provisioning Request name, see b/347220836.
	for _, pr := range provisioningRequestMap[provreqstate.AcceptedState] {
		key, ok := keyFromProvReqFn(pr)
		if !ok {
			continue
		}
		immunityStart[key] = NewQueuedNodeImmunityEntry(time.Time{}, true, "")
	}
	return immunityStart
}

// isNodeImmune returns true and logs when the Node is immune from scaled down, i.e. the additionalImmunity hasn't yet ran out since immunityStart
func isNodeImmune(mode QueuedProvisioningMode, node *apiv1.Node, additionalImmunity time.Duration, immunityEntry QueuedNodeImmunityEntry, now time.Time) bool {
	if immunityEntry.location != "" && (node.Labels == nil || node.Labels[apiv1.LabelTopologyZone] != immunityEntry.location) {
		return false
	}

	basedOn := "creation"
	immunityStart := node.CreationTimestamp.Time
	if !immunityEntry.useNodeCreationTimestamp {
		basedOn = "queuedNodesImmunityStart"
		immunityStart = immunityEntry.immunityStartTimestamp
	}

	immunityEnd := immunityStart.Add(additionalImmunity)
	if now.Before(immunityEnd) {
		klog.Infof("Filtering %s node %s out from scale down based on %s timestamp from %v until %v, immunity time left: %v", mode, node.Name, basedOn, immunityStart, immunityEnd, immunityEnd.Sub(now))
		return true
	}
	return false
}

func shouldFailProvReqOrRecordUnreconciled(unreconciledSince map[pods.ProvReqID]time.Time, pr *provreqwrapper.ProvisioningRequest, now time.Time) bool {
	prID := pods.GetProvReqID(pr)
	t, found := unreconciledSince[prID]
	if !found {
		unreconciledSince[prID] = now
		return false
	}
	unreconciledPeriod := maxUnreconciledPeriod
	if queuedwrapper.ToQueuedProvisioningRequest(*pr).ObtainabilityStrategy() {
		unreconciledPeriod = MaxObtainabilityStrategyUnreconciledPeriod
	}
	return now.After(t.Add(unreconciledPeriod))
}

func updateProvisioningRequestToAcceptedState(prClient provreqClient, provReq *provreqwrapper.ProvisioningRequest, now time.Time) error {
	err := provreqstate.SetState(provReq, provreqstate.AcceptedState, v1.NewTime(now))
	if err != nil {
		return fmt.Errorf("failed to modify the Provisioning Request: %w", err)
	}
	_, err = prClient.UpdateProvisioningRequest(provReq.ProvisioningRequest)
	return err
}

func clearProvisioningRequestDetails(prClient provreqClient, provReq *provreqwrapper.ProvisioningRequest) error {
	if err := provreqstate.ClearProvisioningClassDetails(provReq); err != nil {
		return fmt.Errorf("failed to clear ProvReq details for %s/%s, including Resize Request and MIG names of the Provisioning Request: %w", provReq.Namespace, provReq.Name, err)
	}
	_, err := prClient.UpdateProvisioningRequest(provReq.ProvisioningRequest)
	return err
}

func failProvReqsWithProperty(prClient provreqClient, prsInState []*provreqwrapper.ProvisioningRequest, propertyFn func(*provreqwrapper.ProvisioningRequest) bool, failureReason, failureMessage string, now time.Time) {
	for _, pr := range prsInState {
		if propertyFn(pr) {
			if err := provreqstate.ForceSetStateCustomReasonMessage(pr, provreqstate.FailedState, failureReason, failureMessage, v1.NewTime(now)); err != nil {
				klog.Errorf("Got error when updating Provisioning Request %s/%s to Failed state with reason %q and message %q: %v",
					pr.Namespace, pr.Name, failureReason, failureMessage, err)
				continue
			}
			if _, err := prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
				klog.Errorf("Got error when making an Update API call for Provisioning Request %s/%s to Failed state with reason %q message %q: %v",
					pr.Namespace, pr.Name, failureReason, failureMessage, err)
			}
		}
	}
}
