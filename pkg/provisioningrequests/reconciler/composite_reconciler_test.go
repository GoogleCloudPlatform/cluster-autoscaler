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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/bulkmig"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func TestReconcileRequests(t *testing.T) {
	rrMigRef := &gce.GceRef{Project: "testProject", Name: "gke-test-cluster-example-test2-840da30b-grp", Zone: "us-east7-b"}

	tests := []struct {
		name                     string
		pr                       *provreqwrapper.ProvisioningRequest
		rrs                      []resizerequestclient.ResizeRequestStatus
		bulkMig                  bulkmig.Status
		wantState                provreqstate.ProvisioningRequestState
		wantProvReqDetailsNotSet bool
		wantRRsRemaining         int
		wantBulkMigInProgress    bool
		wantBulkMigTargetSize    int64
	}{
		// These test cases show the importance of the order of the reconcilers.
		{
			name:                     "PR_stays_Uninitialized_because_it_has_an_existing_resize_request",
			pr:                       provReqInState("default", "acc-uninitialized", "", "", provreqstate.UninitializedState),
			rrs:                      exampleResizeRequestStatus("gke-default-acc-uninitialized-e091730e3c51702b", resizerequestclient.ResizeRequestStateProvisioning, rrMigRef.Name, rrMigRef.Zone),
			wantState:                provreqstate.UninitializedState,
			wantProvReqDetailsNotSet: true,
			wantRRsRemaining:         0,
		},
		// Some basic test cases verifying the composite processor flow.
		{
			name:                  "PR_with_RR_stays_Accepted_stray_BulkMig_cancelled",
			pr:                    provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", rrMigRef.Name, provreqstate.AcceptedState),
			rrs:                   exampleResizeRequestStatus("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, rrMigRef.Name, rrMigRef.Zone),
			bulkMig:               bulkMigStatus("stray-bulk-mig-inprogress", true, 127),
			wantState:             provreqstate.AcceptedState,
			wantRRsRemaining:      1,
			wantBulkMigInProgress: false,
			wantBulkMigTargetSize: 0,
		},
		{
			name:                  "PR_with_BulkMig_stays_Accepted_stray_RR_cleaned_up",
			pr:                    bulkProvReqInState("default", "acc-pr", "bulk-mig-name", provreqstate.AcceptedState),
			rrs:                   exampleResizeRequestStatus("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, rrMigRef.Name, rrMigRef.Zone),
			bulkMig:               bulkMigStatus("bulk-mig-name", true, 17),
			wantState:             provreqstate.AcceptedState,
			wantRRsRemaining:      0,
			wantBulkMigInProgress: true,
			wantBulkMigTargetSize: 17,
		},
		{
			name:      "PR_Provisioned_recently_stays_Provisioned",
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "prov3", "gke-default-prov3-693c08ad911d8c40", rrMigRef.Name, provreqstate.ProvisionedState, exampleInitTime, exampleTimeInc),
			wantState: provreqstate.ProvisionedState,
		},
		{
			name:             "recovered_Pending_to_Accepted_RR_mode",
			pr:               provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", rrMigRef.Name, provreqstate.PendingState),
			rrs:              exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, rrMigRef.Name, rrMigRef.Zone),
			wantState:        provreqstate.AcceptedState,
			wantRRsRemaining: 1,
		},
		{
			name:                  "recovered_Pending_to_Accepted_BulkMig_mode",
			pr:                    bulkProvReqInState("default", "penToAcc", "bulk-mig-name", provreqstate.PendingState),
			bulkMig:               bulkMigStatus("bulk-mig-name", true, 7),
			wantState:             provreqstate.AcceptedState,
			wantBulkMigInProgress: true,
			wantBulkMigTargetSize: 7,
		},
		{
			name:                     "reset_details_Pending_RR_mode",
			pr:                       provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", rrMigRef.Name, provreqstate.PendingState),
			wantState:                provreqstate.PendingState,
			wantProvReqDetailsNotSet: true,
		},
		{
			name:                     "reset_details_Pending_BulkMig_mode",
			pr:                       bulkProvReqInState("default", "penToAcc", "bulk-mig-name", provreqstate.PendingState),
			bulkMig:                  bulkMigStatus("bulk-mig-name", false, 0),
			wantState:                provreqstate.PendingState,
			wantProvReqDetailsNotSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tt.pr)
			prCache := provreqcache.NewQueuedProvisioningCache(fakeClient)

			rrs := map[string][]resizerequestclient.ResizeRequestStatus{rrMigRef.Name: {}}
			if tt.rrs != nil {
				rrs[rrMigRef.Name] = append(rrs[rrMigRef.Name], tt.rrs...)
			}
			fakeResReqClient := resizerequestclient.NewResizeRequestClientFake(rrs, exampleInitTime)

			bmigs := []bulkmig.Status{}
			bmigRefs := map[gce.GceRef]common.GkeMigWrapper{}
			if tt.bulkMig.Ref.Name != "" {
				bmigs = append(bmigs, tt.bulkMig)
				bmigRefs[tt.bulkMig.Ref] = &common.FakeGkeMigWrapper{}
			}
			fakeBulkMigClient := bulkmig.NewBulkMigClientFake(bmigs)
			fakeexperimentsManager := experiments.NewMockManager(experiments.ProvisioningRequestBulkMigsFlag)
			r, err := NewCompositeProvisioningRequestReconciler(fakeClient, prCache, fakeResReqClient, fakeBulkMigClient, fakeexperimentsManager, "project")
			assert.NoError(t, err, tt)

			err = r.ReconcileRequests(map[gce.GceRef]common.GkeMigWrapper{*rrMigRef: &common.FakeGkeMigWrapper{}}, bmigRefs, recentTimestamp)
			assert.NoError(t, err, tt)

			gotPR, err := fakeClient.ProvisioningRequestNoCache(tt.pr.Namespace, tt.pr.Name)
			assert.NoError(t, err, tt)
			assert.Equal(t, tt.wantState, provreqstate.StateOfProvisioningRequest(gotPR))
			_, found := provreqstate.GetProvisioningClassDetails(queuedwrapper.ToQueuedProvisioningRequest(*gotPR))
			assert.Equal(t, tt.wantProvReqDetailsNotSet, !found)

			gotRRs, err := fakeResReqClient.ResizeRequests(ctx, gce.GceRef{Name: rrMigRef.Name})
			assert.NoError(t, err)
			assert.Equal(t, tt.wantRRsRemaining, len(gotRRs))

			gotMig, err := fakeBulkMigClient.BulkMigStatus(tt.bulkMig.Ref)
			if tt.bulkMig.Ref.Name != "" {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantBulkMigTargetSize, gotMig.TargetSize)
				assert.Equal(t, tt.wantBulkMigInProgress, gotMig.InProgress)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestQueuedProvisioningNodeHasScaleDownImmunity(t *testing.T) {
	testNow := exampleInitTime
	migName := "mig-name"
	rrName := "rr-name"
	testAdditionalImmunity := 9 * time.Minute
	newNodeTimestamp := testNow.Add(-1 * time.Minute)
	oldNodeTimestamp := testNow.Add(-1*time.Minute - testAdditionalImmunity)

	tests := []struct {
		name                               string
		node                               *apiv1.Node
		bulkMigNodesImmunityStartMap       map[string]QueuedNodeImmunityEntry
		resizeRequestNodesImmunityStartMap map[string]QueuedNodeImmunityEntry
		bulkMigProvisioningMode            bool
		wantImmune                         bool
	}{
		{
			name:                         "bulkMigMode_noImmunity",
			node:                         basicTestNode("bulk-node-new", newNodeTimestamp),
			bulkMigNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{},
			bulkMigProvisioningMode:      true,
			wantImmune:                   false,
		},
		{
			name:                               "resizeRequestMode_noImmunity",
			node:                               basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, rrName),
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{},
			bulkMigProvisioningMode:            false,
			wantImmune:                         false,
		},
		{
			name: "new_bulkMigMode_immunity",
			node: basicTestNode("bulk-node-new", newNodeTimestamp),
			bulkMigNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				migName: {
					immunityStartTimestamp:   newNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				rrName: {
					immunityStartTimestamp:   oldNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			bulkMigProvisioningMode: true,
			wantImmune:              true,
		},
		{
			name: "old_bulkMigMode_noImmunity",
			node: basicTestNode("bulk-node-old", oldNodeTimestamp),
			bulkMigNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				migName: {
					immunityStartTimestamp:   oldNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				rrName: {
					immunityStartTimestamp:   newNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			bulkMigProvisioningMode: true,
			wantImmune:              false,
		},
		{
			name: "new_resizeRequestMode_immunity",
			node: basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, rrName, withLocation("location")),
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				rrName: {
					immunityStartTimestamp:   newNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			wantImmune: true,
		},
		{
			name: "new_resizeRequestMode_matchingLocation_immunity",
			node: basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, rrName, withLocation("location")),
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				rrName: {
					immunityStartTimestamp:   newNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
					location:                 "location",
				},
			},
			wantImmune: true,
		},
		{
			name: "new_resizeRequestMode_differentLocation_noImmunity",
			node: basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, rrName, withLocation("another-location")),
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				rrName: {
					immunityStartTimestamp:   newNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
					location:                 "location",
				},
			},
			wantImmune: false,
		},
		{
			name:                         "old_resizeRequestMode_noImmunity",
			node:                         basicTestNodeWithResizeRequest("rr-node-old", oldNodeTimestamp, rrName),
			bulkMigNodesImmunityStartMap: nil,
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				rrName: {
					immunityStartTimestamp:   oldNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			bulkMigProvisioningMode: false,
			wantImmune:              false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fakeClient := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t)
			prCache := provreqcache.NewQueuedProvisioningCache(fakeClient)
			fakeResReqClient := resizerequestclient.NewResizeRequestClientFake(nil, testNow)
			fakeBulkMigClient := bulkmig.NewBulkMigClientFake(nil)
			fakeexperimentsManager := experiments.NewMockManager(experiments.ProvisioningRequestBulkMigsFlag)
			r, err := NewCompositeProvisioningRequestReconciler(fakeClient, prCache, fakeResReqClient, fakeBulkMigClient, fakeexperimentsManager, "project")
			assert.NoError(t, err, tt)
			err = overrideImmunityMaps(t, r, tt.resizeRequestNodesImmunityStartMap, tt.bulkMigNodesImmunityStartMap)
			assert.NoError(t, err, tt)
			provisioningMode := ResizeRequestProvisioningMode
			if tt.bulkMigProvisioningMode {
				provisioningMode = BulkMigProvisioningMode
			}

			migSpec := &QueuedProvisioningMigSpec{
				GceRef:           gce.GceRef{Name: migName},
				ProvisioningMode: provisioningMode,
				Immunity:         testAdditionalImmunity}
			got := r.QueuedProvisioningNodeHasScaleDownImmunity(tt.node, migSpec, testNow)
			assert.Equal(t, tt.wantImmune, got)
		})
	}
}

func overrideImmunityMaps(t *testing.T, compRec *compositeProvisioningRequestReconciler, rrImmunityMap, bulkImmunityMap map[string]QueuedNodeImmunityEntry) error {
	overrides := 0
	for _, r := range compRec.reconcilers {
		switch typedRec := r.(type) {
		case *resizeRequestReconciler:
			typedRec.resizeRequestNodesImmunityStart = rrImmunityMap
			overrides++
		case *bulkMigReconciler:
			typedRec.bulkMigNodesImmunityStart = bulkImmunityMap
			overrides++
		}
	}
	if overrides != 2 {
		return fmt.Errorf("Expected %d immunity map overrides for testing, got %d", 2, overrides)
	}
	return nil
}

func TestReconcileRequestsLimitAcceptedProvisioningRequestsUpdates(t *testing.T) {
	testNow := exampleInitTime.Add(time.Hour)
	mig := &gce.GceRef{Name: "gke-test-cluster-example-test17-127-grp", Zone: "us-central1-b"}
	prNameFunc := func(i int) string {
		return fmt.Sprintf("prAccepted%d", i)
	}
	rrNameFunc := func(i int) string {
		return resizerequestclient.ResizeRequestName(metav1.NamespaceDefault, prNameFunc(i))
	}
	acceptedProvReqFunc := func(i int) *provreqwrapper.ProvisioningRequest {
		return provreqstate.ProvisioningRequestInStateForTests(
			metav1.NamespaceDefault, prNameFunc(i),
			rrNameFunc(i), mig.Name,
			provreqstate.AcceptedState,
			exampleInitTime.Add(time.Minute*time.Duration(i)), exampleTimeInc)
	}
	acceptedProvReqOldConditionSetUpFunc := func(i int) *provreqwrapper.ProvisioningRequest {
		pr := provreqstate.ProvisioningRequestInStateForTests(
			metav1.NamespaceDefault, prNameFunc(i),
			rrNameFunc(i), mig.Name,
			provreqstate.AcceptedState,
			exampleInitTime.Add(time.Minute*time.Duration(i)), exampleTimeInc) // The lower the index the older ProvReq, so it'll get updated faster
		pr.Status.Conditions = append(pr.Status.Conditions,
			metav1.Condition{
				LastTransitionTime: metav1.NewTime(exampleInitTime.Add(time.Minute*time.Duration(i) + exampleTimeInc - 17*time.Second)),
				Message:            provreqstate.ProvisionedInitMessage,
				ObservedGeneration: 0,
				Reason:             provreqstate.ProvisionedInitReason,
				Status:             metav1.ConditionFalse,
				Type:               provreqv1.Provisioned,
			},
			metav1.Condition{
				LastTransitionTime: metav1.NewTime(exampleInitTime.Add(time.Minute*time.Duration(i) + exampleTimeInc - 17*time.Second)),
				Message:            "Provisioning Request hasn't failed.",
				ObservedGeneration: 0,
				Reason:             "NotFailed",
				Status:             metav1.ConditionFalse,
				Type:               provreqv1.Failed,
			},
		)
		return pr
	}
	acceptedQuotaErrorProvReqFunc := func(i int) *provreqwrapper.ProvisioningRequest {
		pr := provreqstate.ProvisioningRequestInStateForTests(
			metav1.NamespaceDefault, prNameFunc(i),
			rrNameFunc(i), mig.Name,
			provreqstate.AcceptedState,
			exampleInitTime.Add(time.Minute*time.Duration(i)), exampleTimeInc)
		pr.Status.Conditions = append(pr.Status.Conditions, metav1.Condition{
			LastTransitionTime: metav1.NewTime(exampleInitTime.Add(time.Minute*time.Duration(i) + exampleTimeInc + 13*time.Second)),
			Message:            "Quota 'SSD_TOTAL_GB' exceeded.  Limit: 125.0 in region us-central1.",
			ObservedGeneration: 1,
			Reason:             "QuotaExceeded",
			Status:             metav1.ConditionFalse,
			Type:               provreqv1.Provisioned,
		})
		return pr
	}
	quotaLastAttemptError := []resizerequestclient.DwsStatusError{{Code: "QUOTA_EXCEEDED", Message: "Quota 'SSD_TOTAL_GB' exceeded.  Limit: 125.0 in region us-central1."}}

	tests := []struct {
		name                  string
		prs                   []*provreqwrapper.ProvisioningRequest
		rrs                   []resizerequestclient.ResizeRequestStatus
		wantPRsWithQuotaError []int
		wantPRsJustAccepted   []int
	}{
		{
			name: "Accepted ProvReqs (no observability info yet) and Accepted Resize Requests without LastAttemptErrors - still no observability info",
			prs: []*provreqwrapper.ProvisioningRequest{
				acceptedProvReqOldConditionSetUpFunc(9),
				acceptedProvReqFunc(1),
				acceptedProvReqOldConditionSetUpFunc(8),
				acceptedProvReqFunc(2),
				acceptedProvReqFunc(6),
				acceptedProvReqFunc(7),
				acceptedProvReqFunc(10),
				acceptedProvReqOldConditionSetUpFunc(4),
				acceptedProvReqFunc(3),
				acceptedProvReqOldConditionSetUpFunc(0),
				acceptedProvReqOldConditionSetUpFunc(12),
				acceptedProvReqFunc(5),
				acceptedProvReqFunc(11),
			},
			rrs:                 rrsAcceptedRangeInclusive(0, 12, mig.Name, mig.Zone, rrNameFunc),
			wantPRsJustAccepted: intRangeInclusive(0, 12),
		},
		{
			name: "Accepted ProvReqs (no observability info yet) and Accepted Resize Requests with LastAttemptErrors - only `observabilityUpdatesLimit = 10` will get updated",
			prs: []*provreqwrapper.ProvisioningRequest{
				acceptedProvReqOldConditionSetUpFunc(9),
				acceptedProvReqFunc(1),
				acceptedProvReqFunc(8),
				acceptedProvReqFunc(2),
				acceptedProvReqOldConditionSetUpFunc(6),
				acceptedProvReqFunc(7),
				acceptedProvReqOldConditionSetUpFunc(10),
				acceptedProvReqFunc(4),
				acceptedProvReqFunc(3),
				acceptedProvReqOldConditionSetUpFunc(0),
				acceptedProvReqOldConditionSetUpFunc(12),
				acceptedProvReqFunc(5),
				acceptedProvReqFunc(11),
			},
			rrs:                   rrsAcceptedRangeInclusive(0, 12, mig.Name, mig.Zone, rrNameFunc, quotaLastAttemptError...),
			wantPRsWithQuotaError: intRangeInclusive(0, 9),
			wantPRsJustAccepted:   intRangeInclusive(10, 12),
		},
		{
			name: "Accepted ProvReqs and Accepted Resize Requests with LastAttemptErrors - those without observability info will get updated",
			prs: []*provreqwrapper.ProvisioningRequest{
				acceptedQuotaErrorProvReqFunc(9),
				acceptedQuotaErrorProvReqFunc(1),
				acceptedProvReqFunc(8),
				acceptedQuotaErrorProvReqFunc(2),
				acceptedProvReqFunc(6),
				acceptedQuotaErrorProvReqFunc(7),
				acceptedProvReqFunc(10),
				acceptedQuotaErrorProvReqFunc(4),
				acceptedProvReqFunc(3),
				acceptedProvReqOldConditionSetUpFunc(0),
				acceptedProvReqFunc(12),
				acceptedProvReqOldConditionSetUpFunc(5),
				acceptedProvReqFunc(11),
			},
			rrs:                   rrsAcceptedRangeInclusive(0, 12, mig.Name, mig.Zone, rrNameFunc, quotaLastAttemptError...),
			wantPRsWithQuotaError: intRangeInclusive(0, 12),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Truef(t, len(tt.wantPRsJustAccepted)+len(tt.wantPRsWithQuotaError) == len(tt.prs), "Please make sure you specified the expected state for all Provisioning Requests in test case %q", tt.name)

			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tt.prs...)

			prCache := provreqcache.NewQueuedProvisioningCache(fakeClient)
			fakeResReqClient := resizerequestclient.NewResizeRequestClientFake(
				map[string][]resizerequestclient.ResizeRequestStatus{mig.Name: tt.rrs}, testNow)
			fakeBulkMigClient := bulkmig.NewBulkMigClientFake(nil)
			fakeexperimentsManager := experiments.NewMockManager(experiments.ProvisioningRequestBulkMigsFlag)
			r, err := NewCompositeProvisioningRequestReconciler(fakeClient, prCache, fakeResReqClient, fakeBulkMigClient, fakeexperimentsManager, "project")
			assert.NoError(t, err, tt)

			provReqsToReconcile := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{}
			for _, pr := range tt.prs {
				state := provreqstate.StateOfProvisioningRequest(pr)
				if provReqsToReconcile[state] == nil {
					provReqsToReconcile[state] = []*provreqwrapper.ProvisioningRequest{}
				}
				provReqsToReconcile[state] = append(provReqsToReconcile[state], pr)
			}

			err = r.ReconcileRequests(map[gce.GceRef]common.GkeMigWrapper{*mig: &common.FakeGkeMigWrapper{}}, nil, testNow)

			assert.NoError(t, err, tt)

			for _, prNameIndex := range tt.wantPRsJustAccepted {
				prName := prNameFunc(prNameIndex)
				prRef, err := fakeClient.ProvisioningRequestNoCache(metav1.NamespaceDefault, prName)
				assert.NoError(t, err)
				assert.Equal(t, provreqstate.AcceptedState, provreqstate.StateOfProvisioningRequest(prRef))
				res, msg := observabilityReasonMessage(prRef)
				assert.True(t, res == "" || res == provreqstate.ProvisionedInitReason)
				assert.True(t, msg == "" || msg == provreqstate.ProvisionedInitMessage)
			}

			for _, prNameIndex := range tt.wantPRsWithQuotaError {
				prName := prNameFunc(prNameIndex)
				prRef, err := fakeClient.ProvisioningRequestNoCache(metav1.NamespaceDefault, prName)
				assert.NoError(t, err)
				assert.Equal(t, provreqstate.AcceptedState, provreqstate.StateOfProvisioningRequest(prRef))
				res, msg := observabilityReasonMessage(prRef)
				assert.Equal(t, "QuotaExceeded", res)
				assert.Equal(t, "Quota 'SSD_TOTAL_GB' exceeded.  Limit: 125.0 in region us-central1.", msg)
			}
		})
	}
}

func intRangeInclusive(beg, end int) []int {
	res := make([]int, 0, end-beg+1)
	for i := beg; i <= end; i++ {
		res = append(res, i)
	}
	return res
}

func rrsAcceptedRangeInclusive(beg, end int, migName, migZone string, rrNameFunc func(i int) string, errs ...resizerequestclient.DwsStatusError) []resizerequestclient.ResizeRequestStatus {
	indexes := intRangeInclusive(beg, end)
	rrs := make([]resizerequestclient.ResizeRequestStatus, 0, len(indexes))

	for i := range indexes {
		var rr []resizerequestclient.ResizeRequestStatus
		if errs != nil {
			rr = exampleResizeRequestStatusWithLastAttemptError(rrNameFunc(i), resizerequestclient.ResizeRequestStateAccepted, migName, migZone, errs)
		} else {
			rr = exampleResizeRequestStatus(rrNameFunc(i), resizerequestclient.ResizeRequestStateAccepted, migName, migZone)
		}
		rrs = append(rrs, rr...)
	}
	return rrs
}
