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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/bulkmig"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func TestReconcileBulkMigs(t *testing.T) {
	acceptedImmunityEntry := QueuedNodeImmunityEntry{time.Time{}, true, ""}
	tests := []struct {
		name                     string
		pr                       *provreqwrapper.ProvisioningRequest
		bulkMig                  bulkmig.Status
		reconciliationMap        map[pods.ProvReqID]time.Time
		wantState                provreqstate.ProvisioningRequestState
		wantReasonMessage        map[string]metav1.Condition
		wantBulkMigImmunityStart map[string]QueuedNodeImmunityEntry
		wantPrUnreconciled       bool
		wantReconciliationMap    map[pods.ProvReqID]time.Time
		wantProvReqDetailsUnset  bool
	}{
		{
			name:                     "nonBulkMig_AcceptedPR_ignore",
			pr:                       provReqInState("default", "acc1", "gke-default-acc1-9f5a3766909d1de9", "rr-mig-name", provreqstate.AcceptedState),
			wantState:                provreqstate.AcceptedState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{},
			wantPrUnreconciled:       true,
		},
		{
			name:                     "nonBulkMig_PendingPR_ignore",
			pr:                       provReqInState("default", "acc1", "gke-default-acc1-9f5a3766909d1de9", "rr-mig-name", provreqstate.PendingState),
			wantState:                provreqstate.PendingState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{},
			wantPrUnreconciled:       true,
		},
		{
			name:                     "prPending_noDetails_ignore",
			pr:                       provReqInState("default", "pen1", "", "", provreqstate.PendingState),
			wantState:                provreqstate.PendingState,
			wantProvReqDetailsUnset:  true,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{},
			wantPrUnreconciled:       true,
		},
		{
			name:                     "prPending_resizeRequestDetails_ignore",
			pr:                       provReqInState("default", "pen1", "rr-name", "rr-mig-name", provreqstate.PendingState),
			wantState:                provreqstate.PendingState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{},
			wantPrUnreconciled:       true,
		},
		{
			name:                     "prPending_bulkMigInProgress_recoveredToAccepted",
			pr:                       bulkProvReqInState("default", "penToAcc", "bulk-mig-name", provreqstate.PendingState),
			bulkMig:                  bulkMigStatus("bulk-mig-name", true, 127),
			wantState:                provreqstate.AcceptedState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{},
		},
		{
			name:                     "prPending_bulkMigNotInProgress_clearedDetails",
			pr:                       bulkProvReqInState("default", "penToAcc", "bulk-mig-name", provreqstate.PendingState),
			bulkMig:                  bulkMigStatus("bulk-mig-name", false, 127),
			wantState:                provreqstate.PendingState,
			wantProvReqDetailsUnset:  true,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{},
		},
		{
			name:                     "prPending_noBulkMig_clearedDetails",
			pr:                       bulkProvReqInState("default", "penCleared", "bulk-mig-name", provreqstate.PendingState),
			wantState:                provreqstate.PendingState,
			wantProvReqDetailsUnset:  true,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{},
		},
		{
			name:      "notAcceptedState_BulkMig_PR_ignore",
			pr:        bulkProvReqInState("default", "prov1", "bulk-mig-name", provreqstate.ProvisionedState, provreqstate.WithSelectedZone("us-central1-c")),
			wantState: provreqstate.ProvisionedState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": {
					immunityStartTimestamp:   exampleInitTime.Add(2 * exampleTimeInc),
					useNodeCreationTimestamp: false,
					location:                 "us-central1-c",
				},
			},
			wantPrUnreconciled: true,
		},
		{
			name:      "prAcceptedToFailed_MissingDetailsBulk_MigUnidentifiable",
			pr:        bulkProvReqInState("default", "acc-pr-no-mig-detail", "", provreqstate.AcceptedState),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "InternalErrorMissingDetails",
					Message: "Provisioning Request was Accepted but didn't have identifying details assigned.",
				},
			},
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{},
		},
		{
			name: "prAcceptedToFailed_MissingBulkMig",
			pr:   bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp.Add(-1*time.Second - maxUnreconciledPeriod),
			},
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "InternalErrorMissingBulkMig",
					Message: "The corresponding Bulk Mig was missing.",
				},
			},
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": acceptedImmunityEntry,
			},
		},
		{
			name:      "prAccepted_MissingBulkMig_saveUnsuccessfulReconciliationEntry",
			pr:        bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": acceptedImmunityEntry,
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp,
			},
		},
		{
			name: "prAccepted_MissingBulkMig_dontFail_recentUnsuccessfulReconciliationEntry",
			pr:   bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp.Add(-1 * time.Minute),
			},
			wantState: provreqstate.AcceptedState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": acceptedImmunityEntry,
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp.Add(-1 * time.Minute),
			},
		},
		{
			name: "prAcceptedToAccepted_ObservabilityUpdate",
			pr:   bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			bulkMig: bulkMigStatus("bulk-mig-name", true, 127,
				[]resizerequestclient.DwsStatusError{{
					Code: "QUOTA_EXCEEDED", Message: "Quota 'SSD_TOTAL_GB' exceeded.  Limit: 125.0 in region us-central1."}}...),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp.Add(-10 * time.Second),
			},
			wantState: provreqstate.AcceptedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "QuotaExceeded",
					Message: "Quota 'SSD_TOTAL_GB' exceeded.  Limit: 125.0 in region us-central1.",
				},
			},
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": acceptedImmunityEntry,
			},
		},
		{
			name: "prAcceptedToProvisioned",
			pr:   bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp.Add(-10 * time.Second),
			},
			bulkMig:   bulkMigStatus("bulk-mig-name", false, 17),
			wantState: provreqstate.ProvisionedState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": acceptedImmunityEntry,
			},
		},
		{
			name:      "prAccepted_NoQueueingInProgress_saveUnsuccessfulReconciliationEntry",
			pr:        bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			bulkMig:   bulkMigStatus("bulk-mig-name", false, 0),
			wantState: provreqstate.AcceptedState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": acceptedImmunityEntry,
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp,
			},
		},
		{
			name: "prAccepted_NoQueueingInProgress_dontFail_recentUnsuccessfulReconciliationEntry",
			pr:   bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp.Add(-10 * time.Second),
			},
			bulkMig:   bulkMigStatus("bulk-mig-name", false, 0),
			wantState: provreqstate.AcceptedState,
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": acceptedImmunityEntry,
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp.Add(-10 * time.Second),
			},
		},
		{
			name: "prAcceptedToFailed_NoQueueingInProgress",
			pr:   bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pr"}: recentTimestamp.Add(-10*time.Second - maxUnreconciledPeriod),
			},
			bulkMig:   bulkMigStatus("bulk-mig-name", false, 0),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "InternalErrorBulkMigNotInProgress",
					Message: "The corresponding Bulk Mig doesn't have queueing in progress.",
				},
			},
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": acceptedImmunityEntry,
			},
		},
		{
			name:      "prAcceptedToFailed_BulkMigUnexpectedState",
			pr:        bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			bulkMig:   bulkMigStatus("bulk-mig-name", true, 0),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "InternalErrorBulkMigUnexpectedState",
					Message: "The corresponding Bulk Mig has unexpected state.",
				},
			},
			wantBulkMigImmunityStart: map[string]QueuedNodeImmunityEntry{
				"bulk-mig-name": acceptedImmunityEntry,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fakeClient := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, tt.pr)
			fakeBulkMigClient := bulkmig.NewBulkMigClientFake([]bulkmig.Status{tt.bulkMig})

			r := &bulkMigReconciler{
				prClient:                           fakeClient,
				bulkMigClient:                      fakeBulkMigClient,
				experimentsManager:                 experiments.NewMockManager(experiments.ProvisioningRequestBulkMigsFlag),
				projectId:                          "project",
				firstUnsuccessfulReconciliationMap: map[pods.ProvReqID]time.Time{},
			}
			if tt.reconciliationMap != nil {
				r.firstUnsuccessfulReconciliationMap = tt.reconciliationMap
			}

			inputMap := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{
				provreqstate.StateOfProvisioningRequest(tt.pr): {tt.pr},
			}

			gotUnreconciled, err := r.reconcileRequests(&reconcilingInput{
				bulkMigs:                           map[gce.GceRef]common.GkeMigWrapper{tt.bulkMig.Ref: &common.FakeGkeMigWrapper{}},
				prs:                                inputMap,
				acceptedProvReqUpdatesPerLoopLimit: defaultAcceptedProvReqUpdatesLimit,
				now:                                recentTimestamp,
			})
			assert.NoError(t, err, tt)

			if tt.wantPrUnreconciled {
				wantUnreconciledMap := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{
					provreqstate.StateOfProvisioningRequest(tt.pr): {tt.pr},
				}
				assert.Subset(t, wantUnreconciledMap, gotUnreconciled)
				assert.Subset(t, gotUnreconciled, wantUnreconciledMap)
			} else {
				for _, v := range gotUnreconciled {
					assert.Empty(t, v)
				}
			}

			newPR, err := fakeClient.ProvisioningRequestNoCache(tt.pr.Namespace, tt.pr.Name)
			assert.NoError(t, err, tt)
			assert.Equal(t, tt.wantState, provreqstate.StateOfProvisioningRequest(newPR))
			_, found := provreqstate.GetProvisioningClassDetails(queuedwrapper.ToQueuedProvisioningRequest(*newPR))
			assert.Equal(t, tt.wantProvReqDetailsUnset, !found)

			if tt.wantReasonMessage != nil {
				for conditionType, expectedCondition := range tt.wantReasonMessage {
					conditions := newPR.Status.Conditions
					if foundCondition := k8sapimeta.FindStatusCondition(conditions, conditionType); foundCondition != nil {
						assert.Equal(t, expectedCondition.Status, foundCondition.Status)
						assert.Equal(t, expectedCondition.Reason, foundCondition.Reason)
						assert.Equal(t, expectedCondition.Message, foundCondition.Message)
					}
				}
			}
			assert.Equal(t, tt.wantBulkMigImmunityStart, r.bulkMigNodesImmunityStart)
			if tt.wantReconciliationMap == nil {
				tt.wantReconciliationMap = map[pods.ProvReqID]time.Time{}
			}
			assert.Equal(t, tt.wantReconciliationMap, r.firstUnsuccessfulReconciliationMap)
		})
	}
}

func TestReconcileBulkMigsMissingProvReqs(t *testing.T) {
	tests := []struct {
		name            string
		bulkMig         bulkmig.Status
		bulkMigPoolSpec *gkeclient.NodePoolSpec
		outOfSyncPr     *provreqwrapper.ProvisioningRequest
		wantTargetSize  int64
		wantInProgress  bool
	}{
		{
			name:           "MissingProvReq_ BulkMigNotInProgress_keep_ZeroTargetSize",
			bulkMig:        bulkMigStatus("bulk-mig-name", false, 0),
			wantTargetSize: 0,
			wantInProgress: false,
		},
		{
			name:           "MissingProvReq_ BulkMigNotInProgress_keep_NonZeroTargetSize",
			bulkMig:        bulkMigStatus("bulk-mig-name", false, 17),
			wantTargetSize: 17,
			wantInProgress: false,
		},
		{
			name:           "MissingProvReq_ BulkMigInProgress_set_ZeroTargetSize",
			bulkMig:        bulkMigStatus("bulk-mig-name", true, 17),
			wantTargetSize: 0,
			wantInProgress: false,
		},
		{
			name:           "OutOfSyncProvReq_AcceptedHasMigDetail_BulkMigInProgress_keep_NonZeroTargetSize",
			outOfSyncPr:    bulkProvReqInState("default", "acc-oosync", "bulk-mig-name", provreqstate.AcceptedState),
			bulkMig:        bulkMigStatus("bulk-mig-name", true, 17),
			wantTargetSize: 17,
			wantInProgress: true,
		},
		{
			name:           "OutOfSyncProvReq_PendingHasMigDetail_BulkMigInProgress_keep_NonZeroTargetSize",
			outOfSyncPr:    bulkProvReqInState("default", "pending-oosync", "bulk-mig-name", provreqstate.PendingState),
			bulkMig:        bulkMigStatus("bulk-mig-name", true, 17),
			wantTargetSize: 17,
			wantInProgress: true,
		},
		{
			name:        "OutOfSyncProvReq_PendingMissingMigDetail_BulkMigInProgress_keep_NonZeroTargetSize",
			outOfSyncPr: bulkProvReqInState("default", "pending-oosync-no-detail", "" /* missing MIG name detail */, provreqstate.PendingState),
			bulkMig:     bulkMigStatus("bulk-mig-name", true, 17),
			bulkMigPoolSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.ProvisioningRequestLabelKey: resizerequestclient.ResizeRequestName("default", "pending-oosync-no-detail"),
				},
			},
			wantTargetSize: 17,
			wantInProgress: true,
		},
		{
			name:           "OutOfSyncProvReq_PendingMissingMigDetail_BulkMigNotFoundNoLabels_set_ZeroTargetSize",
			outOfSyncPr:    bulkProvReqInState("default", "pending-oosync-no-detail", "" /* missing MIG name detail */, provreqstate.PendingState),
			bulkMig:        bulkMigStatus("bulk-mig-name", true, 17),
			wantTargetSize: 0,
			wantInProgress: false,
		},
		{
			name:        "OutOfSyncProvReq_PendingMissingMigDetail_BulkMigNotFoundDifferentLabelValue_set_ZeroTargetSize",
			outOfSyncPr: bulkProvReqInState("default", "pending-oosync-no-detail", "" /* missing MIG name detail */, provreqstate.PendingState),
			bulkMig:     bulkMigStatus("bulk-mig-name", true, 17),
			bulkMigPoolSpec: &gkeclient.NodePoolSpec{
				Labels: map[string]string{
					gkelabels.ProvisioningRequestLabelKey: "abc",
				},
			},
			wantTargetSize: 0,
			wantInProgress: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			provReqsOutOfSync := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{}
			if tt.outOfSyncPr != nil {
				provReqsOutOfSync[provreqstate.StateOfProvisioningRequest(tt.outOfSyncPr)] = []*provreqwrapper.ProvisioningRequest{tt.outOfSyncPr}
			}
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t)
			fakeBulkMigClient := bulkmig.NewBulkMigClientFake([]bulkmig.Status{tt.bulkMig})

			r := &bulkMigReconciler{
				prClient:           fakeClient,
				bulkMigClient:      fakeBulkMigClient,
				experimentsManager: experiments.NewMockManager(experiments.ProvisioningRequestBulkMigsFlag),
				projectId:          "project",
			}

			bulkMigs := map[gce.GceRef]common.GkeMigWrapper{tt.bulkMig.Ref: &common.FakeGkeMigWrapper{PoolSpec: tt.bulkMigPoolSpec}}
			gotUnreconciled, err := r.reconcileRequests(&reconcilingInput{
				bulkMigs:                           bulkMigs,
				prsOutOfSync:                       provReqsOutOfSync,
				acceptedProvReqUpdatesPerLoopLimit: defaultAcceptedProvReqUpdatesLimit,
				now:                                recentTimestamp,
			})
			assert.NoError(t, err, tt)
			assert.Nil(t, gotUnreconciled)

			gotBulkMig, err := fakeBulkMigClient.BulkMigStatus(tt.bulkMig.Ref)
			assert.NoError(t, err, tt)
			assert.Equal(t, tt.wantTargetSize, gotBulkMig.TargetSize)
			assert.Equal(t, tt.wantInProgress, gotBulkMig.InProgress)
		})
	}
}

func TestBulkMigNodeHasScaleDownImmunity(t *testing.T) {
	testNow := exampleInitTime
	testAdditionalImmunity := 9 * time.Minute
	newNodeTimestamp := testNow.Add(-1 * time.Minute)
	oldNodeTimestamp := testNow.Add(-1*time.Minute - testAdditionalImmunity)

	tests := []struct {
		name                          string
		bulkMigNodesImmunityStartMap  map[string]QueuedNodeImmunityEntry
		node                          *apiv1.Node
		resizeRequestProvisioningMode bool
		wantImmune                    bool
	}{
		{
			name:                          "ResizeRequestProvisioningMode_noImmunity",
			node:                          basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr"),
			resizeRequestProvisioningMode: true,
			wantImmune:                    false,
		},
		{
			name:                         "newNode_nilMap_useCreationTimestamp_immune",
			bulkMigNodesImmunityStartMap: nil,
			node:                         basicTestNode("bulk-node-new", newNodeTimestamp),
			wantImmune:                   true,
		},
		{
			name:                         "oldNode_nilMap_useCreationTimestamp_noImmunity",
			bulkMigNodesImmunityStartMap: nil,
			node:                         basicTestNode("bulk-node-old", oldNodeTimestamp),
			wantImmune:                   false,
		},
		{
			name:                         "node_immunityEntryNotFound_noImmunity",
			bulkMigNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{},
			node:                         basicTestNode("bulk-node-new", newNodeTimestamp),
			wantImmune:                   false,
		},
		{
			name: "node_useImmunityEntryTimestamp_immune",
			bulkMigNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-mig": {
					immunityStartTimestamp:   newNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			node:       basicTestNode("bulk-node-new", newNodeTimestamp),
			wantImmune: true,
		},
		{
			name: "node_useImmunityEntryTimestamp_noImmunity",
			bulkMigNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-mig": {
					immunityStartTimestamp:   oldNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			node:       basicTestNode("bulk-node-new", newNodeTimestamp),
			wantImmune: false,
		},
		{
			name: "newNode_useNodeCreationTimestampFromImmunityEntry_immune",
			bulkMigNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-mig": {
					immunityStartTimestamp:   time.Time{},
					useNodeCreationTimestamp: true,
				},
			},
			node:       basicTestNode("bulk-node-new", newNodeTimestamp),
			wantImmune: true,
		},
		{
			name: "oldNode_useNodeCreationTimestampFromImmunityEntry_noImmunity",
			bulkMigNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-mig": {
					immunityStartTimestamp:   time.Time{},
					useNodeCreationTimestamp: true,
				},
			},
			node:       basicTestNode("bulk-node-old", oldNodeTimestamp),
			wantImmune: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fakeClient := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t)
			fakeBulkMigClient := bulkmig.NewBulkMigClientFake(nil)
			r := &bulkMigReconciler{
				prClient:                  fakeClient,
				bulkMigClient:             fakeBulkMigClient,
				experimentsManager:        experiments.NewMockManager(experiments.ProvisioningRequestBulkMigsFlag),
				projectId:                 "project",
				bulkMigNodesImmunityStart: tt.bulkMigNodesImmunityStartMap,
			}
			provisioningMode := BulkMigProvisioningMode
			if tt.resizeRequestProvisioningMode {
				provisioningMode = ResizeRequestProvisioningMode
			}

			migSpec := &QueuedProvisioningMigSpec{
				GceRef:           gce.GceRef{Name: "some-mig"},
				ProvisioningMode: provisioningMode,
				Immunity:         testAdditionalImmunity}
			got := r.nodeHasScaleDownImmunity(tt.node, migSpec, testNow)
			assert.Equal(t, tt.wantImmune, got)
		})
	}
}

func TestReconcileBulkMigsObservabilityQuota(t *testing.T) {

	tests := []struct {
		name                                 string
		count, wantProvisioned, wantAccepted int
	}{
		{
			name:            "AcceptedProvReqs_underQuota",
			count:           5,
			wantProvisioned: 5,
			wantAccepted:    0,
		},
		{
			name:            "AcceptedProvReqs_overQuota",
			count:           2 * defaultAcceptedProvReqUpdatesLimit,
			wantProvisioned: defaultAcceptedProvReqUpdatesLimit,
			wantAccepted:    defaultAcceptedProvReqUpdatesLimit,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.count, tt.wantAccepted+tt.wantProvisioned, "invalid test setup")

			prs, fakeClient := initAcceptedPrs(t, tt.count)
			migRefs, fakeBulkMigClient := initSucceededBulkMigs(tt.count)
			r := initReconciler(fakeClient, fakeBulkMigClient)

			gotUnreconciled, err := r.reconcileRequests(&reconcilingInput{
				bulkMigs:                           migRefs,
				prs:                                prs,
				acceptedProvReqUpdatesPerLoopLimit: defaultAcceptedProvReqUpdatesLimit,
				now:                                recentTimestamp,
			})
			assert.NoError(t, err, tt)
			for _, v := range gotUnreconciled {
				assert.Empty(t, v)
			}

			newPRs, err := fakeClient.ProvisioningRequestsNoCache()
			assert.NoError(t, err, tt)

			gotAccepted := 0
			gotProvisioned := 0
			for _, pr := range newPRs {
				switch st := provreqstate.StateOfProvisioningRequest(pr); st {
				case provreqstate.AcceptedState:
					gotAccepted++
				case provreqstate.ProvisionedState:
					gotProvisioned++
				default:
					t.Fatalf("Got unexpected Provisioning Request %q state: %s", pr.Name, st)
				}
			}
			assert.Equal(t, tt.wantProvisioned, gotProvisioned)
			assert.Equal(t, tt.wantAccepted, gotAccepted)
		})
	}
}

func bulkMigStatus(name string, inProgress bool, targetSize int, errors ...resizerequestclient.DwsStatusError) bulkmig.Status {
	return bulkmig.Status{
		ID:                         uint64(12717127),
		Ref:                        gce.GceRef{Name: name, Zone: "us-central1-c", Project: "project"},
		InProgress:                 inProgress,
		LastProgressCheckErrors:    errors,
		LastProgressCheckTimestamp: time.Time{},
		TargetSize:                 int64(targetSize),
	}
}

func bulkProvReqInState(namespace, name, nodeGroupName string, state provreqstate.ProvisioningRequestState, opts ...provreqstate.ProvReqOption) *provreqwrapper.ProvisioningRequest {
	return provreqstate.ProvisioningRequestInStateForTests(namespace, name, "", nodeGroupName, state, exampleInitTime, exampleTimeInc, append(opts, provreqstate.WithBulkMigProvisioningMode())...)
}

func initReconciler(prClient provreqClient, bulkMigClient bulkmig.GceMigClient) *bulkMigReconciler {
	return &bulkMigReconciler{
		prClient:                           prClient,
		bulkMigClient:                      bulkMigClient,
		experimentsManager:                 experiments.NewMockManager(experiments.ProvisioningRequestBulkMigsFlag),
		projectId:                          "project",
		firstUnsuccessfulReconciliationMap: map[pods.ProvReqID]time.Time{},
	}
}

func initAcceptedPrs(t *testing.T, count int) (map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest,
	*provreqclient.ProvisioningRequestClient) {
	prs := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{
		provreqstate.AcceptedState: {},
	}
	for i := range count {
		prs[provreqstate.AcceptedState] = append(prs[provreqstate.AcceptedState], bulkProvReqInState("default", fmt.Sprintf("acc-pr-%d", i), fmt.Sprintf("bulk-mig-name-%d", i), provreqstate.AcceptedState))
	}

	return prs, provreqclient.NewFakeProvisioningRequestClient(context.Background(), t, prs[provreqstate.AcceptedState]...)
}

func initSucceededBulkMigs(count int) (map[gce.GceRef]common.GkeMigWrapper, bulkmig.GceMigClient) {
	bulkMigs := []bulkmig.Status{}
	migRefs := map[gce.GceRef]common.GkeMigWrapper{}
	for i := range count {
		mig := bulkMigStatus(fmt.Sprintf("bulk-mig-name-%d", i), false, 17)
		bulkMigs = append(bulkMigs, mig)
		migRefs[mig.Ref] = &common.FakeGkeMigWrapper{}
	}

	return migRefs, bulkmig.NewBulkMigClientFake(bulkMigs)
}
