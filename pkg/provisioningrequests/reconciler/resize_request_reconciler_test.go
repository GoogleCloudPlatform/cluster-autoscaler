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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/common"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

var (
	exampleInitTime       = time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC)
	exampleResReqInitTime = exampleInitTime.Add(2 * exampleTimeInc)
	exampleTimeInc        = 3 * time.Minute

	// at `recentTimestamp` terminal ProvReqs/ResReqs will reach the terminal state, but will still be new enough to not get deleted
	recentTimestamp                                      = exampleInitTime.Add(5 * exampleTimeInc)
	failUnreconciledPRTimestamp                          = recentTimestamp.Add(-maxUnreconciledPeriod - time.Minute)
	failUnreconciledObtainabilityStrategyPRTimestamp     = recentTimestamp.Add(-MaxObtainabilityStrategyUnreconciledPeriod - time.Minute)
	dontFailUnreconciledPRTimestamp                      = recentTimestamp.Add(-maxUnreconciledPeriod + 10*time.Second)
	dontFailUnreconciledObtainabilityStrategyPRTimestamp = recentTimestamp.Add(-MaxObtainabilityStrategyUnreconciledPeriod + 10*time.Second)

	exampleErrorMessage = "some other error"
	exampleError        = []resizerequestclient.DwsStatusError{{Code: "SOME_OTHER_CODE", Message: exampleErrorMessage}}

	stockoutErrorMessage = "test stockout message"
	stockoutError        = []resizerequestclient.DwsStatusError{{
		Code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
		Message: stockoutErrorMessage,
	}}
	quotaErrorMessage = "test quota exceeded message"
	quotaError        = []resizerequestclient.DwsStatusError{{
		Code:    "QUOTA_EXCEEDED",
		Message: quotaErrorMessage,
	}}

	napTrue       = queuedwrapper.AutoprovisionedStatusFromBool(true)
	napFalse      = queuedwrapper.AutoprovisionedStatusFromBool(false)
	testNpName    = "real-np-name"
	testProjectID = "test-project-id"
)

func exampleResizeRequestStatusWithTimestamp(name string, state resizerequestclient.ResizeRequestState, migName, zone string, creationTimestamp time.Time) []resizerequestclient.ResizeRequestStatus {
	return []resizerequestclient.ResizeRequestStatus{{
		ProjectID:    testProjectID,
		ID:           uint64(12312412),
		Name:         name,
		CreationTime: creationTimestamp,
		ResizeBy:     42,
		State:        state,
		MigName:      migName,
		Zone:         zone,
	}}
}

func exampleResizeRequestStatusWithLastAttemptError(name string, state resizerequestclient.ResizeRequestState, migName, zone string, laErrors []resizerequestclient.DwsStatusError) []resizerequestclient.ResizeRequestStatus {
	return []resizerequestclient.ResizeRequestStatus{{
		ProjectID:         testProjectID,
		ID:                uint64(12312412),
		Name:              name,
		CreationTime:      exampleResReqInitTime,
		ResizeBy:          42,
		State:             state,
		MigName:           migName,
		Zone:              zone,
		LastAttemptErrors: laErrors,
	}}
}
func exampleResizeRequestStatusWithError(name string, state resizerequestclient.ResizeRequestState, migName, zone string, errors []resizerequestclient.DwsStatusError) []resizerequestclient.ResizeRequestStatus {
	return []resizerequestclient.ResizeRequestStatus{{
		ProjectID:    testProjectID,
		ID:           uint64(12312412),
		Name:         name,
		CreationTime: exampleResReqInitTime,
		ResizeBy:     42,
		State:        state,
		MigName:      migName,
		Zone:         zone,
		Errors:       errors,
	}}
}

func exampleResizeRequestStatus(name string, state resizerequestclient.ResizeRequestState, migName, zone string) []resizerequestclient.ResizeRequestStatus {
	return exampleResizeRequestStatusWithTimestamp(name, state, migName, zone, exampleResReqInitTime)
}

func duplicateExampleResizeRequestStatus(name string, state resizerequestclient.ResizeRequestState, migName, zone string, copies int) []resizerequestclient.ResizeRequestStatus {
	rr := exampleResizeRequestStatus(name, state, migName, zone)[0]
	res := make([]resizerequestclient.ResizeRequestStatus, 0, copies)
	for range copies {
		res = append(res, rr)
	}
	return res
}

func provReqInState(namespace, name, resizeRequestName, nodeGroupName string, state provreqstate.ProvisioningRequestState, opts ...provreqstate.ProvReqOption) *provreqwrapper.ProvisioningRequest {
	return provreqstate.ProvisioningRequestInStateForTests(namespace, name, resizeRequestName, nodeGroupName, state, exampleInitTime, exampleTimeInc, opts...)
}

func obtainabilityStrategyProvReqInState(namespace, name string, state provreqstate.ProvisioningRequestState, opts ...provreqstate.ProvReqOption) *provreqwrapper.ProvisioningRequest {
	return provreqstate.ProvisioningRequestInStateForTests(namespace, name, "", "", state, exampleInitTime, exampleTimeInc, append(opts, provreqstate.WithObtainabilityStrategy())...)
}

func provReqInStateWithoutPodTemplates(namespace, name, resizeRequestName, nodeGroupName string, state provreqstate.ProvisioningRequestState, creationTime time.Time, opts ...provreqstate.ProvReqOption) *provreqwrapper.ProvisioningRequest {
	pr := provreqstate.ProvisioningRequestInStateForTests(namespace, name, resizeRequestName, nodeGroupName, state, creationTime, exampleTimeInc, provreqstate.WithCreationTime(creationTime))
	return provreqwrapper.NewProvisioningRequest(pr.ProvisioningRequest, []*v1.PodTemplate{})
}

func TestReconcileResizeRequests(t *testing.T) {
	mig := &gce.GceRef{Project: testProjectID, Name: "gke-test-cluster-example-mig-grp", Zone: "us-east7-b"}
	mig2 := &gce.GceRef{Project: testProjectID, Name: "gke-test-cluster-example-mig2-grp", Zone: "us-east7-c"}
	mig3 := &gce.GceRef{Project: testProjectID, Name: "gke-test-cluster-example-mig3-grp", Zone: "us-east7-d"}
	mig4 := &gce.GceRef{Project: testProjectID, Name: "gke-test-cluster-example-mig4-grp", Zone: "us-east7-e"}
	acceptedImmunityEntry := QueuedNodeImmunityEntry{time.Time{}, true, ""}
	tests := []struct {
		name                             string
		pr                               *provreqwrapper.ProvisioningRequest
		rrs                              []resizerequestclient.ResizeRequestStatus
		prHasUpcomingNP                  bool
		prHasFreshlyInitializedNP        bool
		reconciliationMap                map[pods.ProvReqID]time.Time
		wantState                        provreqstate.ProvisioningRequestState
		wantProvReqDetailsNotSet         bool
		wantOneOfImmunityStartMaps       []map[string]QueuedNodeImmunityEntry
		wantReasonMessage                map[string]metav1.Condition
		wantReconciliationMap            map[pods.ProvReqID]time.Time
		wantAllResizeRequestsDeleted     bool
		wantResizeRequestsDeletedInZones []string
		wantPrUnreconciled               bool
		wantOneOfDetailSets              []*queuedwrapper.ProvisioningClassDetails
	}{
		{
			name:               "prAccepted_BulkMig_ignore",
			pr:                 bulkProvReqInState("default", "acc1", mig.Name, provreqstate.AcceptedState),
			wantState:          provreqstate.AcceptedState,
			wantPrUnreconciled: true,
		},
		{
			name:      "prAcceptedToAcceptedWhenCreating",
			pr:        provReqInState("default", "acc1", "gke-default-acc1-9f5a3766909d1de9", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-default-acc1-9f5a3766909d1de9", resizerequestclient.ResizeRequestStateCreating, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc1-9f5a3766909d1de9": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: recentTimestamp,
			},
		},
		{
			name:      "prAcceptedToAcceptedWhenCreatingAgain, before deadline",
			pr:        provReqInState("default", "acc1", "gke-default-acc1-9f5a3766909d1de9", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-default-acc1-9f5a3766909d1de9", resizerequestclient.ResizeRequestStateCreating, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: dontFailUnreconciledPRTimestamp,
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc1-9f5a3766909d1de9": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: dontFailUnreconciledPRTimestamp,
			},
		},
		{
			name:      "prAcceptedToAcceptedWhenCreatingAgain, after deadline, fail Provisioning Request and clear map entry",
			pr:        provReqInState("default", "acc1", "gke-default-acc1-9f5a3766909d1de9", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-default-acc1-9f5a3766909d1de9", resizerequestclient.ResizeRequestStateCreating, mig.Name, mig.Zone),
			wantState: provreqstate.FailedState,
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: failUnreconciledPRTimestamp,
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc1-9f5a3766909d1de9": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "prAcceptedToAccepted",
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
		},
		{
			name:      "prAcceptedToAccepted after previously unreconciled - clear the unsuccessful reconciliation map entry",
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "acc2"}: recentTimestamp.Add(-20 * time.Second),
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "prAcceptedToProvisioned after previously unreconciled - clear the unsuccessful reconciliation map entry",
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
			wantState: provreqstate.ProvisionedState,
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "acc2"}: failUnreconciledPRTimestamp,
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "prAcceptedToFailed after previously unreconciled - clear the unsuccessful reconciliation map entry",
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateFailed, mig.Name, mig.Zone),
			wantState: provreqstate.FailedState,
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "acc2"}: failUnreconciledPRTimestamp,
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "prAcceptedToAcceptedLastAttemptErrorsQuotaExceeded after previously unreconciled - clear the unsuccessful reconciliation map entry",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, quotaError),
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "acc2"}: failUnreconciledPRTimestamp,
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "prAcceptedToAcceptedLastAttemptErrorsQuotaExceeded after previously unreconciled - clear the unsuccessful reconciliation map entry",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, quotaError),
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "acc2"}: failUnreconciledPRTimestamp,
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "QuotaExceeded",
					Message: quotaErrorMessage,
				},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "prAcceptedToAcceptedLastAttemptErrorsQuotaExceeded",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, quotaError),
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "QuotaExceeded",
					Message: quotaErrorMessage,
				},
			},
		},
		{
			name:      "prAcceptedToAcceptedLastAttemptErrorsResourceExhaustedError",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, stockoutError),
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "ResourcePoolExhausted",
					Message: stockoutErrorMessage,
				},
			},
		},
		{
			name:      "prAcceptedToAcceptedLastAttemptErrorsBothQuotaExceededAndResourcePoolExhausted",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, append(quotaError, stockoutError...)),
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "QuotaExceeded",
					Message: quotaErrorMessage,
				},
			},
		},
		{
			name:      "prAcceptedToAcceptedLastAttemptErrorsBothQuotaExceededAndResourcePoolExhaustedDifferentOrder",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-non-default-acc2-4768d7d40620309b", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, append(stockoutError, quotaError...)),
			pr:        provReqInState("non-default", "acc2", "gke-non-default-acc2-4768d7d40620309b", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-acc2-4768d7d40620309b": acceptedImmunityEntry},
			},
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "QuotaExceeded",
					Message: quotaErrorMessage,
				},
			},
		},
		{
			name:      "prAcceptedToAccepted - Provisioning MIG",
			pr:        provReqInState("default", "acc-provisioning", "gke-default-acc-provisioning-563e1448f671fce0", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-default-acc-provisioning-563e1448f671fce0", resizerequestclient.ResizeRequestStateProvisioning, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioning-563e1448f671fce0": acceptedImmunityEntry},
			},
		},
		{
			name:      "prAcceptedToAccepted - Provisioning MIG - LastAttemptErrors QuotaExceeded",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-default-acc-provisioning-563e1448f671fce0", resizerequestclient.ResizeRequestStateProvisioning, mig.Name, mig.Zone, quotaError),
			pr:        provReqInState("non-default", "acc-provisioning", "gke-default-acc-provisioning-563e1448f671fce0", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioning-563e1448f671fce0": acceptedImmunityEntry},
			},
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "QuotaExceeded",
					Message: quotaErrorMessage,
				},
			},
		},
		{
			name:      "prAcceptedToAccepted - Provisioning MIG - LastAttemptErrors QuotaExceeded",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-default-acc-provisioning-563e1448f671fce0", resizerequestclient.ResizeRequestStateProvisioning, mig.Name, mig.Zone, stockoutError),
			pr:        provReqInState("non-default", "acc-provisioning", "gke-default-acc-provisioning-563e1448f671fce0", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioning-563e1448f671fce0": acceptedImmunityEntry},
			},
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "ResourcePoolExhausted",
					Message: stockoutErrorMessage,
				},
			},
		},
		{
			name:                         "prAcceptedToFailed -  2 duplicate ResizeRequest",
			pr:                           provReqInState("default", "acc-provisioning", "gke-default-acc-provisioning-563e1448f671fce0", mig.Name, provreqstate.AcceptedState),
			rrs:                          duplicateExampleResizeRequestStatus("gke-default-acc-provisioning-563e1448f671fce0", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, 2),
			wantAllResizeRequestsDeleted: true,
			wantState:                    provreqstate.FailedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioning-563e1448f671fce0": acceptedImmunityEntry},
			},
		},
		{
			name:                         "prAcceptedToFailed -  5 duplicate ResizeRequest",
			pr:                           provReqInState("default", "acc-provisioning", "gke-default-acc-provisioning-563e1448f671fce0", mig.Name, provreqstate.AcceptedState),
			rrs:                          duplicateExampleResizeRequestStatus("gke-default-acc-provisioning-563e1448f671fce0", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, 5),
			wantAllResizeRequestsDeleted: true,
			wantState:                    provreqstate.FailedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioning-563e1448f671fce0": acceptedImmunityEntry},
			},
		},
		{
			name:      "prAcceptedToProvisioned",
			pr:        provReqInState("default", "acc-provisioned", "gke-default-acc-provisioned-408ac6c1e929cc97", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
			wantState: provreqstate.ProvisionedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioned-408ac6c1e929cc97": acceptedImmunityEntry},
			},
		},
		{
			name:      "prAcceptedToFailed",
			pr:        provReqInState("default", "acc-failed", "gke-default-acc-failed-7ef6fc87b2835237", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateFailed, mig.Name, mig.Zone),
			wantState: provreqstate.FailedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-failed-7ef6fc87b2835237": acceptedImmunityEntry},
			},
		},
		{
			name:      "prAcceptedToAccepted - LastAttemptErrors QuotaExceeded",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-default-acc-quota-issues-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, quotaError),
			pr:        provReqInState("default", "acc-quota-issues", "gke-default-acc-quota-issues-7ef6fc87b2835237", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-quota-issues-7ef6fc87b2835237": acceptedImmunityEntry},
			},
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "QuotaExceeded",
					Message: quotaErrorMessage,
				},
			},
		},
		{
			name:      "prAcceptedToAccepted - LastAttemptErrors ResourcePoolExhausted",
			rrs:       exampleResizeRequestStatusWithLastAttemptError("gke-default-acc-resource-quota-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, stockoutError),
			pr:        provReqInState("default", "acc-resource-quota", "gke-default-acc-resource-quota-7ef6fc87b2835237", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-resource-quota-7ef6fc87b2835237": acceptedImmunityEntry},
			},
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "ResourcePoolExhausted",
					Message: stockoutErrorMessage,
				},
			},
		},
		{
			name:      "prAcceptedToFailedDeleting - 1st time seeing Deleting state, do not fail ProvReq",
			pr:        provReqInState("non-default", "prov-failed2", "gke-non-default-prov-failed2-a133d0823e98767c", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-prov-failed2-a133d0823e98767c", resizerequestclient.ResizeRequestStateDeleting, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-prov-failed2-a133d0823e98767c": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "prov-failed2"}: recentTimestamp,
			},
		},
		{
			name:      "prAcceptedToFailedDeleting - another time seeing Deleting state, but still before deadline, do not fail ProvReq",
			pr:        provReqInState("non-default", "prov-failed2", "gke-non-default-prov-failed2-a133d0823e98767c", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-prov-failed2-a133d0823e98767c", resizerequestclient.ResizeRequestStateDeleting, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-prov-failed2-a133d0823e98767c": acceptedImmunityEntry},
			},
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "prov-failed2"}: dontFailUnreconciledPRTimestamp,
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "prov-failed2"}: dontFailUnreconciledPRTimestamp,
			},
		},
		{
			name:      "prAcceptedToFailedDeleting - another time seeing Deleting state, now after deadline, so fail ProvReq and clear map entry",
			pr:        provReqInState("non-default", "prov-failed2", "gke-non-default-prov-failed2-a133d0823e98767c", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-prov-failed2-a133d0823e98767c", resizerequestclient.ResizeRequestStateDeleting, mig.Name, mig.Zone),
			wantState: provreqstate.FailedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-prov-failed2-a133d0823e98767c": acceptedImmunityEntry},
			},
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "prov-failed2"}: failUnreconciledPRTimestamp,
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "prAcceptedToFailedCancelled - 1st time seeing Deleting state, do not fail ProvReq",
			pr:        provReqInState("non-default", "prov-failed2", "gke-non-default-prov-failed2-a133d0823e98767c", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-prov-failed2-a133d0823e98767c", resizerequestclient.ResizeRequestStateCancelled, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-prov-failed2-a133d0823e98767c": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "prov-failed2"}: recentTimestamp,
			},
		},
		{
			name:      "prAcceptedToFailedCancelled - another time seeing Deleting state, but still before deadline, do not fail ProvReq",
			pr:        provReqInState("non-default", "prov-failed2", "gke-non-default-prov-failed2-a133d0823e98767c", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-prov-failed2-a133d0823e98767c", resizerequestclient.ResizeRequestStateCancelled, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-prov-failed2-a133d0823e98767c": acceptedImmunityEntry},
			},
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "prov-failed2"}: dontFailUnreconciledPRTimestamp,
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "prov-failed2"}: dontFailUnreconciledPRTimestamp,
			},
		},
		{
			name:      "prAcceptedToFailedCancelled - another time seeing Deleting state, now after deadline, so fail ProvReq and clear map entry",
			pr:        provReqInState("non-default", "prov-failed2", "gke-non-default-prov-failed2-a133d0823e98767c", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-non-default-prov-failed2-a133d0823e98767c", resizerequestclient.ResizeRequestStateCancelled, mig.Name, mig.Zone),
			wantState: provreqstate.FailedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-non-default-prov-failed2-a133d0823e98767c": acceptedImmunityEntry},
			},
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "non-default", Name: "prov-failed2"}: failUnreconciledPRTimestamp,
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "prAcceptedToAcceptedUnknown",
			pr:        provReqInState("default", "prov2", "gke-default-prov2-693c08ad911d8c40", mig.Name, provreqstate.AcceptedState),
			rrs:       exampleResizeRequestStatus("gke-default-prov2-693c08ad911d8c40", "unknownState", mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-prov2-693c08ad911d8c40": acceptedImmunityEntry},
			},
		},
		{
			name:      "PR_stays_Provisioned_because_rrReconciler_doesnt_handle_Provisioned_PRs_but_RR_is_recent_so_not_deleted",
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "prov3", "gke-default-prov3-693c08ad911d8c40", mig.Name, provreqstate.ProvisionedState, exampleInitTime.Add(-provreqstate.BookingDuration), exampleTimeInc, provreqstate.WithSelectedZone(mig.Zone)),
			rrs:       exampleResizeRequestStatus("gke-default-prov3-693c08ad911d8c40", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
			wantState: provreqstate.ProvisionedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-prov3-693c08ad911d8c40": {exampleInitTime.Add(-provreqstate.BookingDuration + 2*exampleTimeInc), false, mig.Zone}},
			},
			wantPrUnreconciled: true,
		},
		{
			name:      "PR_stays_Provisioned_because_rrReconciler_doesnt_handle_Provisioned_PRs_but_RR_is_old_so_gets_deleted",
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "prov3", "gke-default-prov3-693c08ad911d8c40", mig.Name, provreqstate.ProvisionedState, exampleInitTime.Add(-terminalResizeRequestTTL), exampleTimeInc, provreqstate.WithSelectedZone(mig.Zone)),
			rrs:       exampleResizeRequestStatus("gke-default-prov3-693c08ad911d8c40", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
			wantState: provreqstate.ProvisionedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-prov3-693c08ad911d8c40": {exampleInitTime.Add(-terminalResizeRequestTTL + 2*exampleTimeInc), false, mig.Zone}},
			},
			wantAllResizeRequestsDeleted: true,
			wantPrUnreconciled:           true,
		},
		{
			name:      "Succeeded ResizeRequest with CapacityRevoked ProvisioningRequest - RR shouldn't get deleted and PR shouldn't change state",
			pr:        provReqInState("default", "prov2", "gke-default-prov2-693c08ad911d8c40", mig.Name, provreqstate.CapacityRevokedState),
			rrs:       exampleResizeRequestStatus("gke-default-prov2-693c08ad911d8c40", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
			wantState: provreqstate.CapacityRevokedState,

			wantPrUnreconciled: true,
		},
		{
			name:      "Succeeded terminalResizeRequestTTL old ResizeRequest with CapacityRevoked ProvisioningRequest - RR should get deleted and PR shouldn't change state",
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "prov2", "gke-default-prov2-693c08ad911d8c40", mig.Name, provreqstate.CapacityRevokedState, exampleInitTime.Add(-terminalResizeRequestTTL), exampleTimeInc),
			rrs:       exampleResizeRequestStatusWithTimestamp("gke-default-prov2-693c08ad911d8c40", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone, exampleResReqInitTime.Add(-terminalResizeRequestTTL)),
			wantState: provreqstate.CapacityRevokedState,

			wantAllResizeRequestsDeleted: true,
			wantPrUnreconciled:           true,
		},
		{
			name:      "Accepted ProvisioningRequest missing Resize Request - 1st time observed, do not fail ProvReq",
			pr:        provReqInState("default", "acc1", "gke-default-acc1-9f5a3766909d1de9", mig.Name, provreqstate.AcceptedState),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc1-9f5a3766909d1de9": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: recentTimestamp,
			},
		},
		{
			name: "Accepted ProvisioningRequest missing Resize Request - another time observed, still before deadline, do not fail ProvReq",
			pr:   provReqInState("default", "acc1", "gke-default-acc1-9f5a3766909d1de9", mig.Name, provreqstate.AcceptedState),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: dontFailUnreconciledPRTimestamp,
			},
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc1-9f5a3766909d1de9": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: dontFailUnreconciledPRTimestamp,
			},
		},
		{
			name: "Accepted ProvisioningRequest missing Resize Request - another time observed, after deadline, fail ProvReq",
			pr:   provReqInState("default", "acc1", "gke-default-acc1-9f5a3766909d1de9", mig.Name, provreqstate.AcceptedState),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: failUnreconciledPRTimestamp,
			},
			wantState: provreqstate.FailedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc1-9f5a3766909d1de9": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "Failed ResizeRequest with Failed ProvisioningRequest - RR shouldn't get deleted and PR shouldn't change state",
			pr:        provReqInState("default", "prov-failed", "gke-default-prov-failed-2ba917d6dc64dab7", mig.Name, provreqstate.FailedState),
			rrs:       exampleResizeRequestStatus("gke-default-prov-failed-2ba917d6dc64dab7", resizerequestclient.ResizeRequestStateFailed, mig.Name, mig.Zone),
			wantState: provreqstate.FailedState,

			wantPrUnreconciled: true,
		},
		{
			name:      "Failed terminalResizeRequestTTL old ResizeRequest with Failed ProvisioningRequest - RR should get deleted and PR shouldn't change state",
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "prov-failed", "gke-default-prov-failed-2ba917d6dc64dab7", mig.Name, provreqstate.FailedState, exampleInitTime.Add(-terminalResizeRequestTTL), exampleTimeInc),
			rrs:       exampleResizeRequestStatusWithTimestamp("gke-default-prov-failed-2ba917d6dc64dab7", resizerequestclient.ResizeRequestStateFailed, mig.Name, mig.Zone, exampleResReqInitTime.Add(-terminalResizeRequestTTL)),
			wantState: provreqstate.FailedState,

			wantAllResizeRequestsDeleted: true,
			wantPrUnreconciled:           true,
		},
		{
			name:      "Succeeded ResizeRequest with Failed ProvisioningRequest - recent RR shouldn't get deleted and PR shouldn't change state",
			pr:        provReqInState("default", "prov-failed", "gke-default-prov-failed-2ba917d6dc64dab7", mig.Name, provreqstate.FailedState),
			rrs:       exampleResizeRequestStatus("gke-default-prov-failed-2ba917d6dc64dab7", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
			wantState: provreqstate.FailedState,

			wantPrUnreconciled: true,
		},
		{
			name:                         "Succeeded ResizeRequest with Failed ProvisioningRequest - old RR should get deleted and PR shouldn't change state",
			pr:                           provreqstate.ProvisioningRequestInStateForTests("default", "prov-failed", "gke-default-prov-failed-2ba917d6dc64dab7", mig.Name, provreqstate.FailedState, exampleInitTime.Add(-terminalResizeRequestTTL), exampleTimeInc),
			rrs:                          exampleResizeRequestStatus("gke-default-prov-failed-2ba917d6dc64dab7", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
			wantAllResizeRequestsDeleted: true,
			wantState:                    provreqstate.FailedState,

			wantPrUnreconciled: true,
		},
		{
			name:      "prPendingWithRRNameWithExistingRR_goesAccepted",
			pr:        provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", mig.Name, provreqstate.PendingState),
			rrs:       exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
		},
		{
			name:                     "prPendingWithoutDetails_ignore",
			pr:                       provReqInState("default", "acc-pending", "", "", provreqstate.PendingState),
			wantState:                provreqstate.PendingState,
			wantPrUnreconciled:       true,
			wantProvReqDetailsNotSet: true,
		},
		{
			name:      "prPending_WithoutRRName_ignore",
			pr:        provReqInState("default", "acc-pending", "", mig.Name, provreqstate.PendingState),
			wantState: provreqstate.PendingState,

			wantPrUnreconciled: true,
		},
		{
			name:                      "PR pending with recently initialized NP",
			pr:                        provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", mig.Name, provreqstate.PendingState),
			rrs:                       exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
			prHasFreshlyInitializedNP: true,
			wantState:                 provreqstate.PendingState,
		},
		{
			name:            "PR pending with upcoming NP",
			pr:              provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", mig.Name, provreqstate.PendingState),
			rrs:             exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
			prHasUpcomingNP: true,
			wantState:       provreqstate.PendingState,
		},
		{
			name:            "PR accepted with upcoming NP",
			pr:              provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", mig.Name, provreqstate.AcceptedState),
			rrs:             exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
			prHasUpcomingNP: true,
			wantState:       provreqstate.AcceptedState,
		},
		{
			name:      "prPendingWithRRNameWithProvisionedRR_goesAccepted",
			pr:        provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", mig.Name, provreqstate.PendingState),
			rrs:       exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
		},
		{
			name:      "prPendingWithRRNameWithFailedRR_goesAccepted",
			pr:        provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", mig.Name, provreqstate.PendingState),
			rrs:       exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateFailed, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
		},
		{
			name:                         "prPendingWithRRNameAndTwoRRs_goesFailed",
			pr:                           provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", mig.Name, provreqstate.PendingState),
			rrs:                          duplicateExampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateProvisioning, mig.Name, mig.Zone, 2),
			wantAllResizeRequestsDeleted: true,
			wantState:                    provreqstate.FailedState,
		},
		{
			name:                     "prPendingWithoutPodTemplates_old_ignored",
			pr:                       provReqInStateWithoutPodTemplates("default", "acc-pending", "", "", provreqstate.PendingState, exampleInitTime),
			wantPrUnreconciled:       true,
			wantProvReqDetailsNotSet: true,
			wantState:                provreqstate.PendingState,
		},
		{
			name:                     "prPendingWithoutPodTemplates_recent_ignored",
			pr:                       provReqInStateWithoutPodTemplates("default", "acc-pending", "", "", provreqstate.PendingState, recentTimestamp),
			wantPrUnreconciled:       true,
			wantProvReqDetailsNotSet: true,
			wantState:                provreqstate.PendingState,
		},
		{
			name:                     "prPendingWithNonExistentRRNameToPending",
			pr:                       provReqInState("default", "acc-pending", "gke-default-acc-failed-7ef6fc87b2835237", mig.Name, provreqstate.PendingState),
			wantState:                provreqstate.PendingState,
			wantProvReqDetailsNotSet: true,
		},
		{
			name:                     "prUninitializedToUninitialized_RR_reconciled_doesnt_initialize_ignored",
			pr:                       provReqInState("default", "acc-uninitialized", "", "", provreqstate.UninitializedState),
			wantPrUnreconciled:       true,
			wantState:                provreqstate.UninitializedState,
			wantProvReqDetailsNotSet: true,
		},
		{
			name:                         "prUninitializedToUninitialized_has_Resize_Request_not_ignored",
			pr:                           provReqInState("default", "acc-uninitialized", "", "", provreqstate.UninitializedState),
			rrs:                          exampleResizeRequestStatus("gke-default-acc-uninitialized-e091730e3c51702b", resizerequestclient.ResizeRequestStateProvisioning, mig.Name, mig.Zone),
			wantAllResizeRequestsDeleted: true,
			wantState:                    provreqstate.UninitializedState,
			wantProvReqDetailsNotSet:     true,
		},
		{
			name:                         "prUninitializedToUninitializedDueToDuplicates",
			pr:                           provReqInState("default", "acc-uninitialized", "", "", provreqstate.UninitializedState),
			rrs:                          duplicateExampleResizeRequestStatus("gke-default-acc-uninitialized-e091730e3c51702b", resizerequestclient.ResizeRequestStateProvisioning, mig.Name, mig.Zone, 2),
			wantAllResizeRequestsDeleted: true,
			wantState:                    provreqstate.UninitializedState,
			wantProvReqDetailsNotSet:     true,
		},
		// Obtainability capacity search strategy PRs
		{
			name:                     "prPending_ObtainabilityStrategy_WithoutDetails_ignore",
			pr:                       obtainabilityStrategyProvReqInState("default", "acc-pending", provreqstate.PendingState),
			wantState:                provreqstate.PendingState,
			wantPrUnreconciled:       true,
			wantProvReqDetailsNotSet: true,
		},
		{
			name:      "prPending_ObtainabilityStrategy_WithRRNameWithExistingRR_goesAccepted",
			pr:        obtainabilityStrategyProvReqInState("default", "acc-pending", provreqstate.PendingState, withObtainabilityStrategyCommittedDetails("gke-default-acc-failed-7ef6fc87b2835237", []*gce.GceRef{mig})),
			rrs:       exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
		},
		{
			name: "prPending_ObtainabilityStrategy_WithRRNameWithProvisionedRR_goesAccepted",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-pending", provreqstate.PendingState, withObtainabilityStrategyCommittedDetails("gke-default-acc-failed-7ef6fc87b2835237", []*gce.GceRef{mig})),
			rrs:  exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-pending"}: dontFailUnreconciledObtainabilityStrategyPRTimestamp,
			},
			wantState:             provreqstate.AcceptedState,
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name:      "prPending_ObtainabilityStrategy_WithRRNameWithAccepted_goesAccepted",
			pr:        obtainabilityStrategyProvReqInState("default", "acc-pending", provreqstate.PendingState, withObtainabilityStrategyCommittedDetails("gke-default-acc-failed-7ef6fc87b2835237", []*gce.GceRef{mig})),
			rrs:       exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
			wantState: provreqstate.AcceptedState,
		},
		{
			name: "prPending_ObtainabilityStrategy_WithRRNameAndMultipleRRs_goesAccepted",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-pending", provreqstate.PendingState, withObtainabilityStrategyCommittedDetails("gke-default-acc-failed-7ef6fc87b2835237", []*gce.GceRef{mig, mig2, mig3, mig4})),
			rrs: slices.Concat(
				exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
				exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateSucceeded, mig2.Name, mig2.Zone),
				exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateFailed, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-failed-7ef6fc87b2835237", resizerequestclient.ResizeRequestStateAccepted, mig4.Name, mig4.Zone),
			),
			wantState: provreqstate.AcceptedState,
		},
		{
			name:                     "prPending_ObtainabilityStrategyWithoutPodTemplates_old_ignored",
			pr:                       provReqInStateWithoutPodTemplates("default", "acc-pending", "", "", provreqstate.PendingState, exampleInitTime, provreqstate.WithObtainabilityStrategy()),
			wantPrUnreconciled:       true,
			wantProvReqDetailsNotSet: true,
			wantState:                provreqstate.PendingState,
		},
		{
			name: "prPending_ObtainabilityStrategy_missingRRs_stillNew_staysPending",
			pr:   obtainabilityStrategyProvReqInState("default", "acc1", provreqstate.PendingState, withObtainabilityStrategyCommittedDetails("gke-default-acc-failed-7ef6fc87b2835237", []*gce.GceRef{mig, mig2})),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: dontFailUnreconciledObtainabilityStrategyPRTimestamp,
			},
			wantState: provreqstate.PendingState,
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: dontFailUnreconciledObtainabilityStrategyPRTimestamp,
			},
		},
		{
			name:      "prPending_ObtainabilityStrategy_missingRRs_afterDeadline_goesFailed",
			pr:        obtainabilityStrategyProvReqInState("default", "acc1", provreqstate.PendingState, withObtainabilityStrategyCommittedDetails("gke-default-acc-failed-7ef6fc87b2835237", []*gce.GceRef{mig, mig2})),
			wantState: provreqstate.FailedState,
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc1"}: failUnreconciledObtainabilityStrategyPRTimestamp,
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name: "recent_pr_ObtainabilityStrategy_FailedRRs_staysAccepted",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-obtainability", provreqstate.AcceptedState, withObtainabilityStrategyCommittedDetails("gke-default-acc-obtainability-66b8646058ff7027", []*gce.GceRef{mig, mig2, mig3, mig4})),
			rrs: slices.Concat(
				exampleResizeRequestStatusWithError("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateFailed, mig.Name, mig.Zone, exampleError),
				exampleResizeRequestStatusWithLastAttemptError("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCancelled, mig2.Name, mig2.Zone, append(stockoutError, quotaError...)),
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateDeleting, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCreating, mig4.Name, mig4.Zone),
			),
			wantState: provreqstate.AcceptedState,
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-obtainability"}: dontFailUnreconciledObtainabilityStrategyPRTimestamp,
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-obtainability-66b8646058ff7027": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-obtainability"}: dontFailUnreconciledObtainabilityStrategyPRTimestamp,
			},
		},
		{
			name: "old_pr_ObtainabilityStrategy_FailedRRsAsMainError_goesFailed",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-obtainability", provreqstate.AcceptedState, withObtainabilityStrategyCommittedDetails("gke-default-acc-obtainability-66b8646058ff7027", []*gce.GceRef{mig, mig2, mig3, mig4})),
			rrs: slices.Concat(
				exampleResizeRequestStatusWithError("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateFailed, mig.Name, mig.Zone, exampleError),
				exampleResizeRequestStatusWithLastAttemptError("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCancelled, mig2.Name, mig2.Zone, append(stockoutError, quotaError...)),
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateDeleting, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCreating, mig4.Name, mig4.Zone),
			),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-obtainability"}: failUnreconciledObtainabilityStrategyPRTimestamp,
			},
			wantAllResizeRequestsDeleted: false, // they'll be deleted later, when they reach terminal TTL
			wantState:                    provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "InternalError",
					Message: "Received unrecognized error: [SOME_OTHER_CODE] \"some other error\"",
				},
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-obtainability-66b8646058ff7027": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name: "prOldFailed_ObtainabilityStrategy_RRs_cleanUpFailedRRs",
			pr:   provreqstate.ProvisioningRequestInStateForTests("default", "acc-obtainability", "gke-default-acc-obtainability-66b8646058ff7027", "", provreqstate.FailedState, exampleInitTime.Add(-terminalResizeRequestTTL), exampleTimeInc, provreqstate.WithObtainabilityStrategy()),
			rrs: slices.Concat(
				exampleResizeRequestStatusWithTimestamp("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateFailed, mig.Name, mig.Zone, exampleResReqInitTime.Add(-terminalResizeRequestTTL)),
				exampleResizeRequestStatusWithTimestamp("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCancelled, mig2.Name, mig2.Zone, exampleResReqInitTime.Add(-terminalResizeRequestTTL)),
				exampleResizeRequestStatusWithTimestamp("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateDeleting, mig3.Name, mig3.Zone, exampleResReqInitTime.Add(-terminalResizeRequestTTL)),
				exampleResizeRequestStatusWithTimestamp("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCreating, mig4.Name, mig4.Zone, exampleResReqInitTime.Add(-terminalResizeRequestTTL)),
			),
			wantState:                    provreqstate.FailedState,
			wantAllResizeRequestsDeleted: true,
			wantPrUnreconciled:           true,
		},
		{
			name: "old_pr_ObtainabilityStrategy_CancelledRRsAsMainError_goesFailed",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-obtainability", provreqstate.AcceptedState, withObtainabilityStrategyCommittedDetails("gke-default-acc-obtainability-66b8646058ff7027", []*gce.GceRef{mig2, mig3, mig4})),
			rrs: slices.Concat(
				exampleResizeRequestStatusWithLastAttemptError("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCancelled, mig2.Name, mig2.Zone, append(stockoutError, quotaError...)),
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateDeleting, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCreating, mig4.Name, mig4.Zone),
			),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-obtainability"}: failUnreconciledObtainabilityStrategyPRTimestamp,
			},
			wantAllResizeRequestsDeleted: false, // they'll be deleted later, when they reach terminal TTL
			wantState:                    provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "QuotaExceeded",
					Message: "test quota exceeded message",
				},
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-obtainability-66b8646058ff7027": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name: "old_pr_ObtainabilityStrategy_DeletingRRsAsMainError_goesFailed",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-obtainability", provreqstate.AcceptedState, withObtainabilityStrategyCommittedDetails("gke-default-acc-obtainability-66b8646058ff7027", []*gce.GceRef{mig2, mig3, mig4})),
			rrs: slices.Concat(
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCreating, mig4.Name, mig4.Zone),
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateDeleting, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateDeleting, mig2.Name, mig2.Zone),
			),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-obtainability"}: failUnreconciledObtainabilityStrategyPRTimestamp,
			},
			wantAllResizeRequestsDeleted: false, // they'll be deleted later, when they reach terminal TTL
			wantState:                    provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "InternalErrorResizeRequestWasBeingDeleted",
					Message: "Provisioning Request failed because Resize Request was being deleted.",
				},
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-obtainability-66b8646058ff7027": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name: "old_pr_ObtainabilityStrategy_CreatingRRsAsMainError_goesFailed",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-obtainability", provreqstate.AcceptedState, withObtainabilityStrategyCommittedDetails("gke-default-acc-obtainability-66b8646058ff7027", []*gce.GceRef{mig3, mig4})),
			rrs: slices.Concat(
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCreating, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-obtainability-66b8646058ff7027", resizerequestclient.ResizeRequestStateCreating, mig4.Name, mig4.Zone),
			),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-obtainability"}: failUnreconciledObtainabilityStrategyPRTimestamp,
			},
			wantAllResizeRequestsDeleted: false, // they'll be deleted later, when they reach terminal TTL
			wantState:                    provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "InternalErrorResizeRequestWasCreating",
					Message: "Provisioning Request failed because Resize Request was creating.",
				},
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-obtainability-66b8646058ff7027": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name: "old_pr_ObtainabilityStrategy_missingRRs_goesFailed",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-obtainability", provreqstate.AcceptedState, withObtainabilityStrategyCommittedDetails("gke-default-acc-obtainability-66b8646058ff7027", []*gce.GceRef{mig3, mig4})),
			reconciliationMap: map[pods.ProvReqID]time.Time{
				{Namespace: "default", Name: "acc-obtainability"}: failUnreconciledObtainabilityStrategyPRTimestamp,
			},
			wantAllResizeRequestsDeleted: false, // they'll be deleted later, when they reach terminal TTL
			wantState:                    provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "MissingResizeRequest",
					Message: "The corresponding Resize Request was missing.",
				},
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-obtainability-66b8646058ff7027": acceptedImmunityEntry},
			},
			wantReconciliationMap: map[pods.ProvReqID]time.Time{},
		},
		{
			name: "obtainabilityStrategy_prAcceptedToProvisioned_1ProvisionedRR",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-provisioned", provreqstate.AcceptedState, withObtainabilityStrategyCommittedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3})),
			rrs: slices.Concat(
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig2.Name, mig2.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateFailed, mig3.Name, mig3.Zone),
			),
			wantState: provreqstate.ProvisionedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioned-408ac6c1e929cc97": {recentTimestamp, false, mig.Zone}},
			},
			wantAllResizeRequestsDeleted: true,
			wantOneOfDetailSets: []*queuedwrapper.ProvisioningClassDetails{
				patchDetails(committedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3}),
					&queuedwrapper.ProvisioningClassDetails{
						SelectedZone:  mig.Zone,
						NodeGroupName: mig.Name,
					}),
			},
		},
		{
			name: "obtainabilityStrategy_prAcceptedToProvisioned_multipleProvisionedRRs",
			pr:   obtainabilityStrategyProvReqInState("default", "acc-provisioned", provreqstate.AcceptedState, withObtainabilityStrategyCommittedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4})),
			rrs: slices.Concat(
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateSucceeded, mig2.Name, mig2.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateSucceeded, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig4.Name, mig4.Zone),
			),
			wantState: provreqstate.ProvisionedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioned-408ac6c1e929cc97": {recentTimestamp, false, mig.Zone}},
				{"gke-default-acc-provisioned-408ac6c1e929cc97": {recentTimestamp, false, mig2.Zone}},
				{"gke-default-acc-provisioned-408ac6c1e929cc97": {recentTimestamp, false, mig3.Zone}},
			},
			wantAllResizeRequestsDeleted: true,
			wantOneOfDetailSets: []*queuedwrapper.ProvisioningClassDetails{
				patchDetails(committedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4}),
					&queuedwrapper.ProvisioningClassDetails{
						SelectedZone:         mig.Zone,
						NodeGroupName:        mig.Name,
						OverprovisionedZones: []string{mig2.Zone, mig3.Zone},
					}),
				patchDetails(committedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4}),
					&queuedwrapper.ProvisioningClassDetails{
						SelectedZone:         mig2.Zone,
						NodeGroupName:        mig2.Name,
						OverprovisionedZones: []string{mig.Zone, mig3.Zone},
					}),
				patchDetails(committedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4}),
					&queuedwrapper.ProvisioningClassDetails{
						SelectedZone:         mig3.Zone,
						NodeGroupName:        mig3.Name,
						OverprovisionedZones: []string{mig.Zone, mig2.Zone},
					}),
			},
		},
		{
			name: "obtainabilityStrategy_prProvisioned_cleanUpResizeRequests_wantUnreconciled",
			pr: obtainabilityStrategyProvReqInState("default", "acc-provisioned", provreqstate.ProvisionedState,
				provreqstate.WithDetails(patchDetails(committedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4}),
					&queuedwrapper.ProvisioningClassDetails{
						SelectedZone:         mig.Zone,
						NodeGroupName:        mig.Name,
						OverprovisionedZones: []string{mig2.Zone},
					}))),
			rrs: slices.Concat(
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateSucceeded, mig.Name, mig.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateSucceeded, mig2.Name, mig2.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateCancelled, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig4.Name, mig4.Zone),
			),
			wantState:          provreqstate.ProvisionedState,
			wantPrUnreconciled: true,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioned-408ac6c1e929cc97": {exampleInitTime.Add(exampleTimeInc * 2), false, mig.Zone}},
			},
			wantAllResizeRequestsDeleted: true,
		},
		// TODO(b/486109144): test when Cancel failed with conditionNotMet (overprovisioning) if feasible (possibly in a separate test, TBD)
		{
			name: "obtainabilityStrategy_prAcceptedToAccepted_htnapDetailsCorrection",
			pr: obtainabilityStrategyProvReqInState("default", "acc-provisioned", provreqstate.AcceptedState,
				withObtainabilityStrategyCommittedDetails("gke-default-acc-provisioned-408ac6c1e929cc97",
					[]*gce.GceRef{{Name: "tmp-mig1", Zone: mig.Zone}, {Name: "tmp-mig2", Zone: mig2.Zone}, {Name: "tmp-mig3", Zone: mig3.Zone}, {Name: "tmp-mig4", Zone: mig4.Zone}}),
				provreqstate.WithDetails(&queuedwrapper.ProvisioningClassDetails{NodePoolName: "tmp-np-name", NodePoolAutoProvisioned: napTrue})),
			rrs: slices.Concat(
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig2.Name, mig2.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig4.Name, mig4.Zone),
			),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioned-408ac6c1e929cc97": acceptedImmunityEntry},
			},
			wantAllResizeRequestsDeleted: false,
			wantOneOfDetailSets: []*queuedwrapper.ProvisioningClassDetails{
				patchDetails(committedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4}),
					&queuedwrapper.ProvisioningClassDetails{
						CommittedZones:          []string{mig.Zone, mig2.Zone, mig3.Zone, mig4.Zone},
						NodePoolName:            testNpName,
						NodePoolAutoProvisioned: napTrue,
					}),
			},
		},
		{
			name: "obtainabilityStrategy_prAcceptedToAccepted_observabilityUpdate",
			pr: obtainabilityStrategyProvReqInState("default", "acc-provisioned", provreqstate.AcceptedState,
				withObtainabilityStrategyCommittedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4})),
			rrs: slices.Concat(
				exampleResizeRequestStatusWithLastAttemptError("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, quotaError),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig2.Name, mig2.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig4.Name, mig4.Zone),
			),
			wantState: provreqstate.AcceptedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "QuotaExceeded",
					Message: quotaErrorMessage,
				},
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioned-408ac6c1e929cc97": acceptedImmunityEntry},
			},
			wantAllResizeRequestsDeleted: false,
			wantOneOfDetailSets: []*queuedwrapper.ProvisioningClassDetails{
				patchDetails(committedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4}),
					&queuedwrapper.ProvisioningClassDetails{
						CommittedNodeGroups: []string{mig.Name, mig2.Name, mig3.Name, mig4.Name},
						CommittedZones:      []string{mig.Zone, mig2.Zone, mig3.Zone, mig4.Zone},
						NodePoolName:        testNpName,
					}),
			},
		},
		{
			name: "obtainabilityStrategy_prAcceptedToAccepted_htnapDetailsCorrection_observabilityUpdate_failedRRsCleanup",
			pr: obtainabilityStrategyProvReqInState("default", "acc-provisioned", provreqstate.AcceptedState,
				withObtainabilityStrategyCommittedDetails("gke-default-acc-provisioned-408ac6c1e929cc97",
					[]*gce.GceRef{{Name: "tmp-mig1", Zone: mig.Zone}, {Name: "tmp-mig2", Zone: mig2.Zone}, {Name: "tmp-mig3", Zone: mig3.Zone}, {Name: "tmp-mig4", Zone: mig4.Zone}}),
				provreqstate.WithDetails(&queuedwrapper.ProvisioningClassDetails{NodePoolName: "tmp-np-name", NodePoolAutoProvisioned: napTrue})),
			rrs: slices.Concat(
				exampleResizeRequestStatusWithLastAttemptError("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone, quotaError),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateFailed, mig2.Name, mig2.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateCancelled, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateDeleting, mig4.Name, mig4.Zone),
			),
			wantState: provreqstate.AcceptedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Provisioned: {
					Status:  metav1.ConditionFalse,
					Reason:  "QuotaExceeded",
					Message: quotaErrorMessage,
				},
			},
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioned-408ac6c1e929cc97": acceptedImmunityEntry},
			},
			wantResizeRequestsDeletedInZones: []string{mig2.Zone, mig3.Zone, mig4.Zone},
			wantOneOfDetailSets: []*queuedwrapper.ProvisioningClassDetails{
				patchDetails(committedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4}),
					&queuedwrapper.ProvisioningClassDetails{
						CommittedNodeGroups:     []string{mig.Name, mig2.Name, mig3.Name, mig4.Name},
						CommittedZones:          []string{mig.Zone, mig2.Zone, mig3.Zone, mig4.Zone},
						NodePoolName:            testNpName,
						NodePoolAutoProvisioned: napTrue,
					}),
			},
		},
		{
			name: "obtainabilityStrategy_prAcceptedToAccepted_noUpdate",
			pr: obtainabilityStrategyProvReqInState("default", "acc-provisioned", provreqstate.AcceptedState,
				withObtainabilityStrategyCommittedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4})),
			rrs: slices.Concat(
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig.Name, mig.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig2.Name, mig2.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig3.Name, mig3.Zone),
				exampleResizeRequestStatus("gke-default-acc-provisioned-408ac6c1e929cc97", resizerequestclient.ResizeRequestStateAccepted, mig4.Name, mig4.Zone),
			),
			wantState: provreqstate.AcceptedState,
			wantOneOfImmunityStartMaps: []map[string]QueuedNodeImmunityEntry{
				{"gke-default-acc-provisioned-408ac6c1e929cc97": acceptedImmunityEntry},
			},
			wantAllResizeRequestsDeleted: false,
			wantOneOfDetailSets: []*queuedwrapper.ProvisioningClassDetails{
				patchDetails(committedDetails("gke-default-acc-provisioned-408ac6c1e929cc97", []*gce.GceRef{mig, mig2, mig3, mig4}),
					&queuedwrapper.ProvisioningClassDetails{
						CommittedNodeGroups: []string{mig.Name, mig2.Name, mig3.Name, mig4.Name},
						CommittedZones:      []string{mig.Zone, mig2.Zone, mig3.Zone, mig4.Zone},
						NodePoolName:        testNpName,
					}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tt.pr)
			mockResReqClient := resizerequestclient.NewResizeRequestClientMock()
			if tt.wantAllResizeRequestsDeleted {
				for _, rr := range tt.rrs {
					mockResReqClient.On("AdvanceResizeRequestCleanUp", ctx, rr).Return(nil).Once()
					mockResReqClient.On("ReportState", rr).Return(resizerequestclient.UnspecifiedReportState).Once()
					mockResReqClient.On("SetReportState", rr, resizerequestclient.AlreadyReportedState).Once()
				}
			}
			if len(tt.wantResizeRequestsDeletedInZones) > 0 {
				for _, zone := range tt.wantResizeRequestsDeletedInZones {
					for _, rr := range tt.rrs {
						if rr.Zone == zone {
							mockResReqClient.On("AdvanceResizeRequestCleanUp", ctx, rr).Return(nil).Once()
							mockResReqClient.On("ReportState", rr).Return(resizerequestclient.UnspecifiedReportState).Once()
							mockResReqClient.On("SetReportState", rr, resizerequestclient.AlreadyReportedState).Once()
						}
					}
				}
			}

			migs := map[gce.GceRef]common.GkeMigWrapper{
				*mig:  &common.FakeGkeMigWrapper{NPName: testNpName},
				*mig2: &common.FakeGkeMigWrapper{NPName: testNpName},
				*mig3: &common.FakeGkeMigWrapper{NPName: testNpName},
				*mig4: &common.FakeGkeMigWrapper{NPName: testNpName},
			}
			rrsPerMig := map[gce.GceRef][]resizerequestclient.ResizeRequestStatus{}
			for _, rr := range tt.rrs {
				migRef := gce.GceRef{Project: testProjectID, Name: rr.MigName, Zone: rr.Zone}
				if _, ok := migs[migRef]; !ok {
					t.Fatalf("MIG %v not found in migs map, please fix the test case %s", migRef, tt.name)
				}
				rrsPerMig[migRef] = append(rrsPerMig[migRef], rr)
			}
			for migRef := range migs {
				mockResReqClient.On("ResizeRequests", ctx, migRef).Return(rrsPerMig[migRef], nil).Once()
			}

			r := newTestResizeRequestReconciler(fakeClient, mockResReqClient)
			if tt.reconciliationMap != nil {
				r.firstUnsuccessfulReconciliationMap = tt.reconciliationMap
			}

			prState := provreqstate.StateOfProvisioningRequest(tt.pr)
			inputPrsToReconcile := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{}
			provReqsWithRRsToIgnore := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{}
			if tt.prHasFreshlyInitializedNP || tt.prHasUpcomingNP {
				provReqsWithRRsToIgnore[prState] = []*provreqwrapper.ProvisioningRequest{tt.pr}
			} else {
				inputPrsToReconcile[prState] = []*provreqwrapper.ProvisioningRequest{tt.pr}
			}

			gotUnreconciled, err := r.reconcileRequests(&reconcilingInput{
				rrMigs:                             migs,
				prs:                                inputPrsToReconcile,
				prsOutOfSync:                       provReqsWithRRsToIgnore,
				acceptedProvReqUpdatesPerLoopLimit: defaultAcceptedProvReqUpdatesLimit,
				now:                                recentTimestamp,
			})
			assert.NoError(t, err, tt)
			if tt.wantPrUnreconciled {
				assert.Subset(t, inputPrsToReconcile, gotUnreconciled)
				assert.Subset(t, gotUnreconciled, inputPrsToReconcile)
			} else {
				for _, v := range gotUnreconciled {
					assert.Empty(t, v)
				}
			}
			mock.AssertExpectationsForObjects(t, mockResReqClient)

			matchingOption := 0
			newPR, err := fakeClient.ProvisioningRequestNoCache(tt.pr.Namespace, tt.pr.Name)
			if tt.wantState != "" {
				assert.NoError(t, err, tt)
				assert.Equal(t, tt.wantState, provreqstate.StateOfProvisioningRequest(newPR))
				_, found := provreqstate.GetProvisioningClassDetails(queuedwrapper.ToQueuedProvisioningRequest(*newPR))
				assert.Equal(t, tt.wantProvReqDetailsNotSet, !found)
				if tt.wantOneOfDetailSets != nil {
					matchingOption = verifyDetails(t, tt.wantOneOfDetailSets, newPR)
				}
			} else { // Provisioning Request is expected to be missing
				assert.Error(t, err, tt)
			}
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
			if tt.wantOneOfImmunityStartMaps == nil {
				assert.Empty(t, r.resizeRequestNodesImmunityStart)
			} else {
				assert.Equal(t, tt.wantOneOfImmunityStartMaps[matchingOption], r.resizeRequestNodesImmunityStart)
			}

			if tt.wantReconciliationMap == nil {
				assert.Empty(t, r.firstUnsuccessfulReconciliationMap)
			} else {
				assert.Equal(t, tt.wantReconciliationMap, r.firstUnsuccessfulReconciliationMap)
			}
		})
	}
}

func observabilityReasonMessage(pr *provreqwrapper.ProvisioningRequest) (string, string) {
	if cond := k8sapimeta.FindStatusCondition(pr.Status.Conditions, provreqv1.Provisioned); cond != nil {
		return cond.Reason, cond.Message
	}
	return "", ""
}

func TestReconcileRequestsHandleResizeRequest(t *testing.T) {
	mig := &gce.GceRef{Project: testProjectID, Name: "gke-test-cluster-example-test2-840da30b-grp", Zone: "us-east7-b"}

	tests := []struct {
		name                      string
		pr                        *provreqwrapper.ProvisioningRequest
		rrs                       []resizerequestclient.ResizeRequestStatus
		prHasUpcomingNP           bool
		prHasFreshlyInitializedNP bool
		wantPRState               provreqstate.ProvisioningRequestState
	}{
		{
			name: "resizeRequestMissingProvisioningRequest without duplicate RRs - deleted",
			rrs:  exampleResizeRequestStatus(resizerequestclient.ResizeRequestName("default", "missing"), resizerequestclient.ResizeRequestStateProvisioning, mig.Name, mig.Zone),
		},
		{
			name:            "resize request with upcoming MIG and pending PR",
			pr:              provReqInState("default", "missing", resizerequestclient.ResizeRequestName("default", "missing"), mig.Name, provreqstate.PendingState),
			prHasUpcomingNP: true,
			wantPRState:     provreqstate.PendingState,
		},
		{
			name:            "resize request with upcoming MIG and accepted PR",
			pr:              provReqInState("default", "missing", resizerequestclient.ResizeRequestName("default", "missing"), mig.Name, provreqstate.AcceptedState),
			prHasUpcomingNP: true,
			wantPRState:     provreqstate.AcceptedState,
		},
		{
			name:                      "resize request with freshly initialized MIG and pending PR",
			pr:                        provReqInState("default", "missing", resizerequestclient.ResizeRequestName("default", "missing"), mig.Name, provreqstate.PendingState),
			prHasFreshlyInitializedNP: true,
			wantPRState:               provreqstate.PendingState,
		},
		{
			name:                      "resize request with freshly initialized MIG and accepted PR",
			pr:                        provReqInState("default", "missing", resizerequestclient.ResizeRequestName("default", "missing"), mig.Name, provreqstate.AcceptedState),
			prHasFreshlyInitializedNP: true,
			wantPRState:               provreqstate.AcceptedState,
		},
		{
			name:        "ProvisioningRequest with duplicate RRs over quota - delete all",
			pr:          provReqInState("default", "acc1", "gke-default-acc1-9f5a3766909d1de9", mig.Name, provreqstate.AcceptedState),
			rrs:         duplicateExampleResizeRequestStatus("gke-default-acc1-9f5a3766909d1de9", resizerequestclient.ResizeRequestStateCreating, mig.Name, mig.Zone, defaultDeletedResizeRequestsPerLoop),
			wantPRState: provreqstate.FailedState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			prs := []*provreqwrapper.ProvisioningRequest{}
			if tt.pr != nil {
				prs = append(prs, tt.pr)
			}
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, prs...)
			mockResReqClient := resizerequestclient.NewResizeRequestClientMock()
			for _, rr := range tt.rrs {
				mockResReqClient.On("AdvanceResizeRequestCleanUp", ctx, rr).Return(nil).Once()
				mockResReqClient.On("ReportState", rr).Return(resizerequestclient.UnspecifiedReportState).Once()
				mockResReqClient.On("SetReportState", rr, resizerequestclient.AlreadyReportedState).Once()
			}
			mockResReqClient.On("ResizeRequests", ctx, *mig).Return(tt.rrs, nil).Once()
			r := newTestResizeRequestReconciler(fakeClient, mockResReqClient)

			prsToReconcile := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{}
			provReqsWithRRsToIgnore := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{}

			if tt.pr != nil {
				prState := provreqstate.StateOfProvisioningRequest(tt.pr)
				if tt.prHasUpcomingNP || tt.prHasFreshlyInitializedNP {
					provReqsWithRRsToIgnore[prState] = []*provreqwrapper.ProvisioningRequest{tt.pr}
				} else {
					prsToReconcile[prState] = []*provreqwrapper.ProvisioningRequest{tt.pr}
				}
			}

			gotUnreconciled, err := r.reconcileRequests(&reconcilingInput{
				rrMigs:                             map[gce.GceRef]common.GkeMigWrapper{*mig: &common.FakeGkeMigWrapper{}},
				prs:                                prsToReconcile,
				prsOutOfSync:                       provReqsWithRRsToIgnore,
				acceptedProvReqUpdatesPerLoopLimit: defaultAcceptedProvReqUpdatesLimit,
				now:                                recentTimestamp,
			})
			for _, v := range gotUnreconciled {
				assert.Empty(t, v)
			}
			assert.NoError(t, err)
			mock.AssertExpectationsForObjects(t, mockResReqClient)

			if tt.pr != nil {
				reconciledPR, err := fakeClient.ProvisioningRequestNoCache(tt.pr.Namespace, tt.pr.Name)
				assert.NoError(t, err, tt)
				assert.Equal(t, tt.wantPRState, provreqstate.StateOfProvisioningRequest(reconciledPR))
			}
		})
	}
}

func TestReconcileRequestsProvisioningRequestsWithTheSameResizeRequestName(t *testing.T) {
	testResizeRequestName := "gke-const-name-693c08ad911d8c40"

	mig1 := &gce.GceRef{Project: testProjectID, Name: "gke-test-cluster-example-test1-840da30b-grp", Zone: "us-east7-b"}
	mig2 := &gce.GceRef{Project: testProjectID, Name: "gke-test-cluster-example-test2-840da30b-grp", Zone: "us-east7-b"}
	pr1 := provReqInState("default", "acc1a", testResizeRequestName, mig1.Name, provreqstate.AcceptedState)
	rr1 := exampleResizeRequestStatus(testResizeRequestName, resizerequestclient.ResizeRequestStateCreating, mig1.Name, mig1.Zone)
	pr2 := provReqInState("default", "acc1b", testResizeRequestName, mig2.Name, provreqstate.AcceptedState)
	rr2 := exampleResizeRequestStatus(testResizeRequestName, resizerequestclient.ResizeRequestStateCreating, mig2.Name, mig2.Zone)

	ctx := context.Background()
	fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, pr1, pr2)
	mockResReqClient := resizerequestclient.NewResizeRequestClientMock()
	mockResReqClient.On("AdvanceResizeRequestCleanUp", ctx, rr1[0]).Return(nil).Once()
	mockResReqClient.On("ReportState", rr1[0]).Return(resizerequestclient.UnspecifiedReportState).Once()
	mockResReqClient.On("SetReportState", rr1[0], resizerequestclient.AlreadyReportedState).Once()
	mockResReqClient.On("AdvanceResizeRequestCleanUp", ctx, rr2[0]).Return(nil).Once()
	mockResReqClient.On("ReportState", rr2[0]).Return(resizerequestclient.UnspecifiedReportState).Once()
	mockResReqClient.On("SetReportState", rr2[0], resizerequestclient.AlreadyReportedState).Once()
	mockResReqClient.On("ResizeRequests", ctx, *mig1).Return(rr1, nil).Once()
	mockResReqClient.On("ResizeRequests", ctx, *mig2).Return(rr2, nil).Once()
	r := newTestResizeRequestReconciler(fakeClient, mockResReqClient)
	r.resizeRequestNameFunc = func(*provreqwrapper.ProvisioningRequest) string {
		return testResizeRequestName
	}

	prs := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{provreqstate.AcceptedState: {pr1, pr2}}

	gotUnreconciled, err := r.reconcileRequests(&reconcilingInput{
		rrMigs:                             map[gce.GceRef]common.GkeMigWrapper{*mig1: &common.FakeGkeMigWrapper{}, *mig2: &common.FakeGkeMigWrapper{}},
		prs:                                prs,
		acceptedProvReqUpdatesPerLoopLimit: defaultAcceptedProvReqUpdatesLimit,
		now:                                recentTimestamp,
	})
	for _, v := range gotUnreconciled {
		assert.Empty(t, v)
	}
	assert.NoError(t, err)
	mock.AssertExpectationsForObjects(t, mockResReqClient)

	pr1Ref, err := fakeClient.ProvisioningRequestNoCache(pr1.Namespace, pr1.Name)
	assert.NoError(t, err)
	pr2Ref, err := fakeClient.ProvisioningRequestNoCache(pr2.Namespace, pr2.Name)
	assert.NoError(t, err)
	assert.Equal(t, provreqstate.FailedState, provreqstate.StateOfProvisioningRequest(pr1Ref))
	assert.Equal(t, provreqstate.FailedState, provreqstate.StateOfProvisioningRequest(pr2Ref))
}

func TestResizeRequestNodeHasScaleDownImmunity(t *testing.T) {
	testNow := exampleInitTime
	testAdditionalImmunity := 9 * time.Minute
	newNodeTimestamp := testNow.Add(-1 * time.Minute)
	oldNodeTimestamp := testNow.Add(-1*time.Minute - testAdditionalImmunity)

	tests := []struct {
		name                               string
		resizeRequestNodesImmunityStartMap map[string]QueuedNodeImmunityEntry
		node                               *apiv1.Node
		bulkProvisioningMode               bool
		wantImmune                         bool
	}{
		{
			name:                 "bulkProvisioningMode_noImmunity",
			node:                 basicTestNode("bulk-node", newNodeTimestamp),
			bulkProvisioningMode: true,
			wantImmune:           false,
		},
		{
			name:                               "resizeRequestName_notFound_newNode_useCreationTimestamp_immune",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{},
			node:                               basicTestNode("rr-node-new", newNodeTimestamp),
			wantImmune:                         true,
		},
		{
			name:                               "resizeRequestName_notFound_oldNode_noImmunity",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{},
			node:                               basicTestNode("rr-node-old", oldNodeTimestamp),
			wantImmune:                         false,
		},
		{
			name:                               "newNode_nilMap_useCreationTimestamp_immune",
			resizeRequestNodesImmunityStartMap: nil,
			node:                               basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr"),
			wantImmune:                         true,
		},
		{
			name:                               "oldNode_nilMap_noImmunity",
			resizeRequestNodesImmunityStartMap: nil,
			node:                               basicTestNodeWithResizeRequest("rr-node-old", oldNodeTimestamp, "some-rr"),
			wantImmune:                         false,
		},
		{
			name:                               "node_immunityEntryNotFound_noImmunity",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{},
			node:                               basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr"),
			wantImmune:                         false,
		},
		{
			name: "node_useImmunityEntryTimestamp_immune",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-rr": {
					immunityStartTimestamp:   newNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			node:       basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr"),
			wantImmune: true,
		},
		{
			name: "node_useImmunityEntryTimestamp_noImmunity",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-rr": {
					immunityStartTimestamp:   oldNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
				},
			},
			node:       basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr"),
			wantImmune: false,
		},
		{
			name: "node_useImmunityEntryTimestamp_matchingLocation_immune",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-rr": {
					immunityStartTimestamp:   newNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
					location:                 "zone-a",
				},
			},
			node:       basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr", withLocation("zone-a")),
			wantImmune: true,
		},
		{
			name: "oldNode_useImmunityEntryTimestamp_matchingLocation_noImmunity",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-rr": {
					immunityStartTimestamp:   oldNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
					location:                 "zone-a",
				},
			},
			node:       basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr", withLocation("zone-a")),
			wantImmune: false,
		},
		{
			name: "node_useImmunityEntryTimestamp_differentLocation_noImmunity",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-rr": {
					immunityStartTimestamp:   newNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
					location:                 "some-other-zone",
				},
			},
			node:       basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr", withLocation("zone-a")),
			wantImmune: false,
		},
		{
			name: "oldNode_useImmunityEntryTimestamp_differentLocation_noImmunity",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-rr": {
					immunityStartTimestamp:   oldNodeTimestamp.Add(-1 * time.Minute),
					useNodeCreationTimestamp: false,
					location:                 "some-other-zone",
				},
			},
			node:       basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr", withLocation("zone-a")),
			wantImmune: false,
		},
		{
			name: "newNode_useNodeCreationTimestampFromImmunityEntry_immune",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-rr": {
					immunityStartTimestamp:   time.Time{},
					useNodeCreationTimestamp: true,
				},
			},
			node:       basicTestNodeWithResizeRequest("rr-node-new", newNodeTimestamp, "some-rr"),
			wantImmune: true,
		},
		{
			name: "oldNode_useNodeCreationTimestampFromImmunityEntry_noImmunity",
			resizeRequestNodesImmunityStartMap: map[string]QueuedNodeImmunityEntry{
				"some-rr": {
					immunityStartTimestamp:   time.Time{},
					useNodeCreationTimestamp: true,
				},
			},
			node:       basicTestNodeWithResizeRequest("rr-node-new", oldNodeTimestamp, "some-rr"),
			wantImmune: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fakeClient := provreqclient.NewFakeProvisioningRequestClient(context.Background(), t)
			fakeResReqClient := resizerequestclient.NewResizeRequestClientFake(nil, testNow)
			r := newTestResizeRequestReconciler(fakeClient, fakeResReqClient)
			r.resizeRequestNodesImmunityStart = tt.resizeRequestNodesImmunityStartMap

			provisioningMode := ResizeRequestProvisioningMode
			if tt.bulkProvisioningMode {
				provisioningMode = BulkMigProvisioningMode
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

func verifyDetails(t *testing.T, want []*queuedwrapper.ProvisioningClassDetails, pr *provreqwrapper.ProvisioningRequest) int {
	qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
	verifyOption := func(i int, opt *queuedwrapper.ProvisioningClassDetails) bool {
		return stringEqual(t, i, "NodeGroupName", opt.NodeGroupName, qpr.NodeGroupName()) &&
			stringEqual(t, i, "ResizeRequestName", opt.ResizeRequestName, qpr.ResizeRequestName()) &&
			stringEqual(t, i, "NodePoolName", opt.NodePoolName, qpr.NodePoolName()) &&
			stringEqual(t, i, "AcceleratorType", opt.AcceleratorType, qpr.AcceleratorType()) &&
			stringEqual(t, i, "SelectedZone", opt.SelectedZone, qpr.SelectedZone()) &&
			stringEqual(t, i, "NodePoolAutoProvisioned", string(opt.NodePoolAutoProvisioned), qpr.NodePoolAutoProvisioned()) &&
			stringEqual(t, i, "PodTemplateName", opt.PodTemplateName, qpr.PodTemplateName()) &&
			stringEqual(t, i, "ProvisioningMode", opt.ProvisioningMode, qpr.ProvisioningMode()) &&
			stringListEqual(t, i, "CommittedZones", opt.CommittedZones, qpr.CommittedZones()) &&
			stringListEqual(t, i, "CommittedNodeGroups", opt.CommittedNodeGroups, qpr.CommittedNodeGroups()) &&
			stringListEqual(t, i, "OverprovisionedZones", opt.OverprovisionedZones, qpr.OverprovisionedZones())
	}
	for i, w := range want {
		if verifyOption(i, w) {
			return i
		}
	}
	t.Fatalf("No matching detail options found")
	return 0
}

func splitStrList(s *string) []string {
	if s == nil || *s == "" {
		return nil
	}
	return strings.Split(*s, ",")
}

func stringListEqual(t *testing.T, opt int, field string, want []string, got *string) bool {
	gotList := splitStrList(got)
	slices.Sort(want)
	slices.Sort(gotList)
	if !slices.Equal(want, gotList) {
		t.Logf("option %d field %q values don't match, want: %v, got: %v", opt, field, want, gotList)
		return false
	}
	return true
}

func stringEqual(t *testing.T, opt int, field, want string, got *string) bool {
	if want == "" && got == nil {
		return true
	}
	if want != "" && got != nil && want == *got {
		return true
	}
	if got == nil {
		t.Logf("option %d field %q values don't match, want: %q, got: nil", opt, field, want)
	} else {
		t.Logf("option %d field %q values don't match, want: %q, got: %q", opt, field, want, *got)
	}
	return false
}

func withObtainabilityStrategyCommittedDetails(rrName string, migs []*gce.GceRef) provreqstate.ProvReqOption {
	return provreqstate.WithDetails(committedDetails(rrName, migs))
}

func committedDetails(rrName string, migs []*gce.GceRef) *queuedwrapper.ProvisioningClassDetails {
	zones := make([]string, 0, len(migs))
	migNames := make([]string, 0, len(migs))
	for _, mig := range migs {
		zones = append(zones, mig.Zone)
		migNames = append(migNames, mig.Name)
	}
	return &queuedwrapper.ProvisioningClassDetails{
		NodePoolName:            testNpName,
		AcceleratorType:         "nvidia-gpu-t4",
		NodePoolAutoProvisioned: napFalse,
		PodTemplateName:         "pod-template",
		ProvisioningMode:        queuedwrapper.ProvisioningModeResizeRequest,
		ResizeRequestName:       rrName,
		CommittedZones:          zones,
		CommittedNodeGroups:     migNames,
		// SelectedZone and NodeGroupName are set only on Provisioned state.
	}
}

func patchDetails(src *queuedwrapper.ProvisioningClassDetails, patch *queuedwrapper.ProvisioningClassDetails) *queuedwrapper.ProvisioningClassDetails {
	if patch.AcceleratorType != "" {
		src.AcceleratorType = patch.AcceleratorType
	}
	if patch.NodePoolName != "" {
		src.NodePoolName = patch.NodePoolName
	}
	if patch.NodePoolAutoProvisioned != queuedwrapper.AutoprovisionedUnset {
		src.NodePoolAutoProvisioned = patch.NodePoolAutoProvisioned
	}
	if patch.PodTemplateName != "" {
		src.PodTemplateName = patch.PodTemplateName
	}
	if patch.ProvisioningMode != "" {
		src.ProvisioningMode = patch.ProvisioningMode
	}
	if patch.ResizeRequestName != "" {
		src.ResizeRequestName = patch.ResizeRequestName
	}
	if patch.CommittedZones != nil {
		src.CommittedZones = patch.CommittedZones
	}
	if patch.CommittedNodeGroups != nil {
		src.CommittedNodeGroups = patch.CommittedNodeGroups
	}
	if patch.SelectedZone != "" {
		src.SelectedZone = patch.SelectedZone
	}
	if patch.NodeGroupName != "" {
		src.NodeGroupName = patch.NodeGroupName
	}
	if patch.OverprovisionedZones != nil {
		src.OverprovisionedZones = patch.OverprovisionedZones
	}
	return src
}

type nodeOpts func(*apiv1.Node)

func basicTestNode(name string, creationTime time.Time, opts ...nodeOpts) *apiv1.Node {
	node := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(creationTime),
			Labels: map[string]string{
				apiv1.LabelTopologyZone: "zone",
			},
		},
	}
	for _, opt := range opts {
		opt(node)
	}
	return node
}

func withLocation(location string) nodeOpts {
	return func(n *apiv1.Node) {
		if n.Labels == nil {
			n.Labels = make(map[string]string)
		}
		n.Labels[apiv1.LabelTopologyZone] = location
	}
}
func withResizeRequest(resizeRequest string) nodeOpts {
	return func(n *apiv1.Node) {
		if n.Labels == nil {
			n.Labels = make(map[string]string)
		}
		n.Labels[gkelabels.ProvisioningRequestLabelKey] = resizeRequest
	}
}

func basicTestNodeWithResizeRequest(name string, creationTime time.Time, resizeRequest string, opts ...nodeOpts) *apiv1.Node {
	return basicTestNode(name, creationTime, append([]nodeOpts{withResizeRequest(resizeRequest)}, opts...)...)
}

func initPRCache(c *provreqclient.ProvisioningRequestClient, prsWithUpcomingNP, prsWithFreshlyInitializedNP []pods.ProvReqID) *provreqcache.QueuedProvisioningCache {
	prCache := provreqcache.NewQueuedProvisioningCache(c)
	for _, id := range append(prsWithUpcomingNP, prsWithFreshlyInitializedNP...) {
		prCache.RegisterUpcomingProvReq(id)
	}
	for _, id := range prsWithFreshlyInitializedNP {
		prCache.UnregisterUpcomingProvReq(id)
	}
	return prCache
}

func newTestResizeRequestReconciler(fakeClient provreqClient, mockResReqClient resizerequestclient.ResizeRequestClient) *resizeRequestReconciler {
	return &resizeRequestReconciler{
		prClient:                           fakeClient,
		rrClient:                           mockResReqClient,
		resizeRequestNameFunc:              provisioningRequestResizeRequestName,
		resizeRequestNodesImmunityStart:    nil,
		firstUnsuccessfulReconciliationMap: map[pods.ProvReqID]time.Time{},
	}
}
