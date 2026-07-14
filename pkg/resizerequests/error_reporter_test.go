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

package resizerequests

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/utils"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	nodegroupchange "k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupconfig"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/client-go/kubernetes/fake"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	rrclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler/reasons"
)

var (
	olderCreationTime = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	newerCreationTime = time.Date(2023, 1, 1, 0, 30, 0, 0, time.UTC)

	defaultMaxNodeProvisionTime = 15 * time.Minute

	permissionDeniedErrorMessage = "test permission denied message"
	permissionDeniedError        = []rrclient.ResizeRequestOperationError{{Code: "PERMISSION_DENIED", Message: permissionDeniedErrorMessage}}

	unsupportedOperationErrorMessage = "test unsupported operation message"
	unsupportedOperationError        = []rrclient.ResizeRequestOperationError{{Code: "UNSUPPORTED_OPERATION", Message: unsupportedOperationErrorMessage}}

	unknownErrorMessage        = "some error"
	unknownError               = []rrclient.ResizeRequestOperationError{{Code: "SOME_CODE", Message: unknownErrorMessage}}
	anotherUnknownErrorMessage = "some other error"
	anotherUnknownError        = []rrclient.ResizeRequestOperationError{{Code: "SOME_OTHER_CODE", Message: anotherUnknownErrorMessage}}

	stockoutErrorMessage = "test stockout message"
	stockoutError        = []rrclient.DwsStatusError{{
		Code:    "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS",
		Message: stockoutErrorMessage,
	}}
	quotaErrorMessage = "test quota exceeded message"
	quotaError        = []rrclient.DwsStatusError{{
		Code:    "QUOTA_EXCEEDED",
		Message: quotaErrorMessage,
	}}
	quotaErrorMessage2 = "test another quota exceeded message"
	quotaError2        = []rrclient.DwsStatusError{{
		Code:    "QUOTA_EXCEEDED",
		Message: quotaErrorMessage2,
	}}
	resourcePoolErrorMessage = "test resource pool message"
	resourcePoolError        = []rrclient.DwsStatusError{{
		Code:    "RESOURCE_POOL_EXHAUSTED",
		Message: resourcePoolErrorMessage,
	}}
	permissionsErrorMessage = "test permissions error message."
	permissionsError        = []rrclient.DwsStatusError{{
		Code:    "PERMISSIONS_ERROR",
		Message: permissionsErrorMessage,
	}}

	ipSpaceExhaustedErrorMessage = "test IP space exhausted message"
	ipSpaceExhaustedError        = []rrclient.DwsStatusError{{
		Code:    "IP_SPACE_EXHAUSTED",
		Message: ipSpaceExhaustedErrorMessage,
	}}

	limitExceededErrorMessage = "test limit exceeded message"
	limitExceededError        = []rrclient.DwsStatusError{{
		Code:    "LIMIT_EXCEEDED",
		Message: limitExceededErrorMessage,
	}}

	invalidReservationError = []rrclient.DwsStatusError{{
		Code:    "INVALID_RESERVATION",
		Message: "Zone does not currently have sufficient capacity for the requested resources",
	}}
	reservationNotFoundError = []rrclient.DwsStatusError{{
		Code:    "RESERVATION_NOT_FOUND",
		Message: "Specified reservation abcd does not exist",
	}}
	reservationNotReadyError = []rrclient.DwsStatusError{{
		Code:    "RESERVATION_NOT_READY",
		Message: "Cannot use reservation, it requires reservation to be in READY state",
	}}
	reservationCapacityExceededError = []rrclient.DwsStatusError{{
		Code:    "RESERVATION_CAPACITY_EXCEEDED",
		Message: "Specified reservation xyz does not have available resources for the request.",
	}}
	reservationIncompatibleError = []rrclient.DwsStatusError{{
		Code:    "RESERVATION_INCOMPATIBLE",
		Message: "No available resources in specified reservations",
	}}
)

func TestReportResizeRequestsErrors(t *testing.T) {
	tests := map[string]struct {
		tpuType                                     string
		tpuMultihost                                bool
		multihostTpuCapacityCheckWaitTimeExpEnabled bool
		resizeRequests                              []rrclient.ResizeRequestStatus
		expectedFailedEventCount                    int
	}{

		"Non TPU mig with one successful resize request": {
			tpuType:      "",
			tpuMultihost: false,
			resizeRequests: []rrclient.ResizeRequestStatus{
				{ID: 42, Name: "test-rr", ResizeBy: 5, State: rrclient.ResizeRequestStateSucceeded, CreationTime: newerCreationTime},
			},
			expectedFailedEventCount: 0,
		},
		"TPU mig with one failed resize request": {
			tpuType:      "test-tpu",
			tpuMultihost: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				{ID: 42, Name: "test-rr", ResizeBy: 3, State: rrclient.ResizeRequestStateFailed, CreationTime: newerCreationTime, Errors: []rrclient.DwsStatusError{{Code: "TestError", Message: "TestErrorMessage"}}},
			},
			expectedFailedEventCount: 1,
		},
		"Non TPU mig with one failed resize request": {
			tpuType:      "",
			tpuMultihost: false,
			resizeRequests: []rrclient.ResizeRequestStatus{
				{ID: 42, Name: "test-rr", ResizeBy: 3, State: rrclient.ResizeRequestStateFailed, CreationTime: newerCreationTime, Errors: []rrclient.DwsStatusError{{Code: "TestError", Message: "TestErrorMessage"}}},
			},
			expectedFailedEventCount: 0,
		},
		"TPU mig with one successful resize request": {
			tpuType:      "test-tpu",
			tpuMultihost: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				{ID: 42, Name: "test-rr", ResizeBy: 5, State: rrclient.ResizeRequestStateSucceeded, CreationTime: newerCreationTime},
			},
			expectedFailedEventCount: 0,
		},
		"TPU mig with two failed resize request": {
			tpuType:      "test-tpu",
			tpuMultihost: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				{ID: 42, Name: "test-rr", ResizeBy: 3, State: rrclient.ResizeRequestStateFailed, CreationTime: olderCreationTime, Errors: []rrclient.DwsStatusError{{Code: "TestError", Message: "TestErrorMessage"}}},
				{ID: 84, Name: "test-rr-2", ResizeBy: 5, State: rrclient.ResizeRequestStateFailed, CreationTime: newerCreationTime, Errors: []rrclient.DwsStatusError{{Code: "TestError", Message: "TestErrorMessage"}}},
			},
			expectedFailedEventCount: 1,
		},
		"TPU mig with one successful and one failed resize request, newest without error": {
			tpuType:      "test-tpu",
			tpuMultihost: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				{ID: 42, Name: "test-rr", ResizeBy: 5, State: rrclient.ResizeRequestStateFailed, CreationTime: olderCreationTime, Errors: []rrclient.DwsStatusError{{Code: "TestError", Message: "TestErrorMessage"}}},
				{ID: 84, Name: "test-rr-2", ResizeBy: 3, State: rrclient.ResizeRequestStateSucceeded, CreationTime: newerCreationTime},
			},
			expectedFailedEventCount: 0,
		},
		"TPU mig with one successful and one failed resize request, newest with error": {
			tpuType:      "test-tpu",
			tpuMultihost: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				{ID: 42, Name: "test-rr", ResizeBy: 5, State: rrclient.ResizeRequestStateSucceeded, CreationTime: olderCreationTime},
				{ID: 84, Name: "test-rr-2", ResizeBy: 3, State: rrclient.ResizeRequestStateFailed, CreationTime: newerCreationTime, Errors: []rrclient.DwsStatusError{{Code: "TestError", Message: "TestErrorMessage"}}},
			},
			expectedFailedEventCount: 1,
		},
		"TPU mig, capacityCheckWaitTime exp enabled - also report 1 event": {
			multihostTpuCapacityCheckWaitTimeExpEnabled: true,
			tpuType:      "test-tpu",
			tpuMultihost: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				{ID: 42, Name: "rr-test", ResizeBy: 5, State: rrclient.ResizeRequestStateSucceeded, CreationTime: olderCreationTime},
				{ID: 84, Name: "rr-test-rr-2", ResizeBy: 3, State: rrclient.ResizeRequestStateFailed, CreationTime: newerCreationTime, Errors: []rrclient.DwsStatusError{{Code: "TestError", Message: "TestErrorMessage"}}},
			},
			expectedFailedEventCount: 1,
		},
	}
	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			fakeClient := &fake.Clientset{}
			recorder := kube_record.NewFakeRecorder(5)
			fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", recorder, true, "my-cool-configmap")
			nodeGroupConfigProcessor := nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 15 * time.Minute})
			provider := &gke.GkeCloudProviderMock{}
			observersList := nodegroupchange.NewNodeGroupChangeObserversList()
			clusterStateRegistry := clusterstate.NewNotifiedClusterStateRegistry(
				provider,
				fakeLogRecorder,
				backoff.NewIdBasedExponentialBackoff(5*time.Minute, 10*time.Minute, 15*time.Minute),
				nodeGroupConfigProcessor,
				&templateNodeInfoRegistry{},
				clusterstate.WithAsyncNodeGroupStateChecker(asyncnodegroups.NewDefaultAsyncNodeGroupStateChecker()),
				clusterstate.WithScaleStateNotifier(observersList),
			)

			context := &ca_context.AutoscalingContext{
				ClusterStateRegistry:   clusterStateRegistry,
				AutoscalingKubeClients: ca_context.AutoscalingKubeClients{Recorder: recorder, LogRecorder: fakeLogRecorder},
			}

			gkeManagerMock := &gke.GkeManagerMock{}
			mig := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Name: "test-mig", Project: "test-proj", Zone: "us-east1-c"}).SetGkeManager(gkeManagerMock).SetExist(true).SetSpec(&gkeclient.NodePoolSpec{TpuType: tc.tpuType, TpuMultiHost: tc.tpuMultihost}).Build()
			gkeManagerMock.On("ResizeRequests", mig).Return(tc.resizeRequests, nil)
			gkeManagerMock.On("CapacityCheckWaitTimeSeconds", mig).Return(10*time.Hour, nil)

			gkeManagerMock.On("AdvanceResizeRequestCleanUp", mock.AnythingOfType("resizerequestclient.ResizeRequestStatus")).Return(nil)
			gkeManagerMock.On("ScaleDownUnreadyTimeOverride", mig).Return(time.Duration(0), false)
			gkeManagerMock.On("DeleteResizeRequest", mock.AnythingOfType("resizerequestclient.ResizeRequestStatus")).Return(nil)
			provider.On("GetGkeMigs").Return([]*gke.GkeMig{mig})
			// Register only the latest resize request
			clusterStateRegistry.RegisterScaleUp(mig, int(tc.resizeRequests[0].ResizeBy), time.Now())

			enabledExperimentFlags := []string{}
			if tc.multihostTpuCapacityCheckWaitTimeExpEnabled {
				enabledExperimentFlags = []string{experiments.CapacityCheckWaitTimeSecondsMultiHostTpuEnabledFlag}
			}
			errorReporter := NewErrorReporter(experiments.NewMockManager(enabledExperimentFlags...))
			errorReporter.Init(context, provider, observersList)
			errorReporter.Refresh()
			assert.Equal(t, tc.expectedFailedEventCount, len(recorder.Events))
		})
	}
}

func TestHandleFailedResizeRequestScaleUps(t *testing.T) {
	testTime := func() time.Time {
		return time.Date(2024, 12, 4, 13, 14, 15, 0, time.UTC)
	}

	tests := []struct {
		name                          string
		nonFlexStartNonQueuedMig      bool
		bulkProvisioningMig           bool
		multihostTpuMig               bool
		flexStartNonQueuedExpDisabled bool
		scaleUpAlreadyFinished        bool

		failedResizeRequestsCreations map[error]int
		resizeRequests                []rrclient.ResizeRequestStatus
		// Non DWS Flex Start Non-Queued managed Resize Requests, e.g. created externally by user
		nonFlexResizeRequests []rrclient.ResizeRequestStatus
		isAlreadyReported     map[string]rrclient.ResizeRequestReportState
		wantRRDeletions       []string

		// we expect any error of wantAnyBackoffErrInfo, as for creation failures we don't have guaranteed ordering
		wantAnyBackoffErrInfo []cloudprovider.InstanceErrorInfo
		wantEvents            []string
	}{
		{
			name:                     "non Flex Start Non-Queued MIG",
			nonFlexStartNonQueuedMig: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-15*time.Minute-10*time.Second), testRROptions{lastAttemptErrors: quotaError}),
				newTestRR(2, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-3*time.Minute)),
				newTestRR(2, 1, rrclient.ResizeRequestStateSucceeded, testTime().Add(-3*time.Minute)),
				newTestRR(2, 2, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: limitExceededError}),
			},
			wantRRDeletions:       []string{},
			wantEvents:            nil,
			wantAnyBackoffErrInfo: nil,
		},
		{
			name:                          "experiment disabled - flex start errors are not reported",
			flexStartNonQueuedExpDisabled: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-15*time.Minute-10*time.Second), testRROptions{lastAttemptErrors: stockoutError}),
				newTestRR(2, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-3*time.Minute)),
				newTestRR(2, 1, rrclient.ResizeRequestStateSucceeded, testTime().Add(-3*time.Minute)),
				newTestRR(2, 2, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: limitExceededError}),
			},
			wantRRDeletions:       []string{},
			wantEvents:            nil,
			wantAnyBackoffErrInfo: nil,
		},
		{
			name: "no failures",
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-3*time.Minute), testRROptions{lastAttemptErrors: append(quotaError, stockoutError...)}),
				newTestRR(1, 1, rrclient.ResizeRequestStateSucceeded, testTime().Add(-3*time.Minute)),
				newTestRR(1, 2, rrclient.ResizeRequestStateSucceeded, testTime().Add(-3*time.Minute)),
				newTestRR(1, 3, rrclient.ResizeRequestStateAccepted, testTime().Add(-3*time.Minute)),
			},
			wantRRDeletions: []string{
				testRRName(1, 1),
				testRRName(1, 2),
			},
		},
		{
			name: "already reported cancelled and failed rrs - no events",
			isAlreadyReported: map[string]rrclient.ResizeRequestReportState{
				testRRName(1, 0): rrclient.AlreadyReportedState,
				testRRName(1, 1): rrclient.AlreadyReportedState,
				testRRName(1, 3): rrclient.AlreadyReportedState,
			},
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: limitExceededError}),
				newTestRR(1, 1, rrclient.ResizeRequestStateCancelled, testTime().Add(-3*time.Minute), testRROptions{lastAttemptErrors: stockoutError}),
				newTestRR(1, 2, rrclient.ResizeRequestStateAccepted, testTime().Add(-3*time.Minute)),
				newTestRR(1, 3, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: resourcePoolError}),
			},
			wantRRDeletions: []string{
				testRRName(1, 0),
				testRRName(1, 1),
				testRRName(1, 3),
			},
		},
		{
			name:                "bulk provisioning - no events",
			bulkProvisioningMig: true,
			// It's impossible for BulkMigs to have ResizeRequests this is only to illustrate the bulk case
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-15*time.Minute-10*time.Second), testRROptions{lastAttemptErrors: quotaError}),
				newTestRR(2, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-3*time.Minute)),
				newTestRR(2, 1, rrclient.ResizeRequestStateSucceeded, testTime().Add(-3*time.Minute)),
				newTestRR(2, 2, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: limitExceededError}),
			},
			wantRRDeletions:       []string{},
			wantEvents:            nil,
			wantAnyBackoffErrInfo: nil,
		},
		{
			name:                   "scale up finished, but there's still an existing Resize Request - delete it",
			scaleUpAlreadyFinished: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateCancelled, testTime().Add(-15*time.Minute-10*time.Second)),
			},
			isAlreadyReported: map[string]rrclient.ResizeRequestReportState{
				testRRName(1, 0): rrclient.AlreadyReportedState,
			},
			wantRRDeletions: []string{
				testRRName(1, 0),
			},
		},
		{
			name: "failed rr creates, no backoff",
			failedResizeRequestsCreations: map[error]int{
				// This is one of the failure reasons that doesnt't trigger NP backoff
				&rrclient.ResizeRequestOperationMultiError{Errors: unsupportedOperationError}: 5,
			},
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-3*time.Minute)),
			},
			wantEvents: []string{
				flexScaleUpFailedOnTrigger(5, "UnsupportedOperation", unsupportedOperationErrorMessage),
			},
		},
		{
			name: "multiple failed rr creates with the same main error",
			failedResizeRequestsCreations: map[error]int{
				&rrclient.ResizeRequestOperationMultiError{Errors: append(unknownError, permissionDeniedError...)}:        2,
				&rrclient.ResizeRequestOperationMultiError{Errors: append(anotherUnknownError, permissionDeniedError...)}: 3,
			},
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-3*time.Minute)),
				newTestRR(2, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-4*time.Minute)),
			},
			wantEvents: []string{
				// Events got grouped together by main error
				flexScaleUpFailedOnTrigger(5, "PermissionDenied", permissionDeniedErrorMessage),
			},
			wantAnyBackoffErrInfo: []cloudprovider.InstanceErrorInfo{{
				ErrorClass: cloudprovider.OtherErrorClass,
				ErrorCode:  "OTHER", ErrorMessage: "",
			}, {
				ErrorClass: cloudprovider.OtherErrorClass,
				ErrorCode:  "OTHER", ErrorMessage: "",
			},
			},
		},
		{
			name: "failed rr creates and failed rrs",
			failedResizeRequestsCreations: map[error]int{
				&rrclient.ResizeRequestOperationMultiError{Errors: permissionDeniedError}: 4,
			},
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateSucceeded, testTime().Add(-10*time.Minute)),
				newTestRR(1, 1, rrclient.ResizeRequestStateFailed, testTime().Add(-10*time.Minute), testRROptions{errors: quotaError}),
				newTestRR(1, 2, rrclient.ResizeRequestStateCancelled, testTime().Add(-10*time.Minute)),
				newTestRR(1, 3, rrclient.ResizeRequestStateCancelled, testTime().Add(-10*time.Minute), testRROptions{lastAttemptErrors: quotaError2}),
				newTestRR(3, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-16*time.Minute), testRROptions{lastAttemptErrors: append(resourcePoolError, stockoutError...)}),
				newTestRR(2, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-3*time.Minute)),
				newTestRR(4, 0, rrclient.ResizeRequestStateCreating, testTime().Add(-2*time.Second)),
			},
			nonFlexResizeRequests: []rrclient.ResizeRequestStatus{
				{ID: 999999, Name: "some-rr-unknown-origin", ResizeBy: 1, State: rrclient.ResizeRequestStateFailed, CreationTime: testTime().Add(-3 * time.Minute), Errors: limitExceededError},
				{ID: 999998, Name: "gke-prov-req-rr", ResizeBy: 1, State: rrclient.ResizeRequestStateAccepted, CreationTime: testTime().Add(-3 * time.Minute)},
				{ID: 999997, Name: "rr-for-tpu", ResizeBy: 1, State: rrclient.ResizeRequestStateAccepted, CreationTime: testTime().Add(-3 * time.Minute)},
			},
			wantRRDeletions: []string{
				testRRName(1, 0),
				testRRName(1, 1),
				testRRName(1, 2),
				testRRName(1, 3),
				testRRName(3, 0),
			},
			wantEvents: []string{
				flexScaleUpFailedOnTrigger(4, "PermissionDenied", permissionDeniedErrorMessage),
				flexScaleUpFailedTestEvent(1, "RequestWasCancelled", "Request was unexpectedly cancelled, no errors were provided."),
				// Both QuotaExceeded events got combined by their reason, their messages appended
				flexScaleUpFailedTestEvent(2, "QuotaExceeded", reasons.MultipleErrorsMessage([]string{quotaErrorMessage, quotaErrorMessage2})),
			},
			wantAnyBackoffErrInfo: []cloudprovider.InstanceErrorInfo{{
				ErrorClass: cloudprovider.OtherErrorClass,
				ErrorCode:  "OTHER", ErrorMessage: "",
			}, {
				ErrorClass: cloudprovider.OtherErrorClass,
				ErrorCode:  "OTHER", ErrorMessage: "",
			}, {
				ErrorClass: cloudprovider.OtherErrorClass,
				ErrorCode:  "QUOTA_EXCEEDED", ErrorMessage: reasons.MultipleErrorsMessage([]string{quotaErrorMessage, quotaErrorMessage2}),
			}},
		},
		{
			name: "failed rrs with different errors ",
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: append(ipSpaceExhaustedError, limitExceededError...)}),
				newTestRR(1, 1, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: append(permissionsError, limitExceededError...)}),
				newTestRR(1, 2, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: resourcePoolError}),
				newTestRR(3, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-15*time.Minute-10*time.Second)),
				newTestRR(2, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-1*time.Minute)),
			},
			wantRRDeletions: []string{
				testRRName(1, 0),
				testRRName(1, 1),
				testRRName(1, 2),
				testRRName(3, 0),
			},
			wantEvents: []string{
				// LimitExceeded Errors are grouped by main error
				flexScaleUpFailedTestEvent(2, "LimitExceeded", limitExceededErrorMessage),
				flexScaleUpFailedTestEvent(1, "ResourcePoolExhausted", resourcePoolErrorMessage),
			},
			wantAnyBackoffErrInfo: []cloudprovider.InstanceErrorInfo{
				{
					ErrorClass: cloudprovider.OtherErrorClass,
					ErrorCode:  "LIMIT_EXCEEDED", ErrorMessage: limitExceededErrorMessage,
				},
				{
					ErrorClass: cloudprovider.OutOfResourcesErrorClass,
					ErrorCode:  "RESOURCE_POOL_EXHAUSTED", ErrorMessage: resourcePoolErrorMessage,
				},
			},
		},
		{
			name: "timeouted with cancel in progress and finished - report only finished",
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-16*time.Minute), testRROptions{lastAttemptErrors: quotaError}),
				newTestRR(1, 1, rrclient.ResizeRequestStateAccepted, testTime().Add(-16*time.Minute), testRROptions{lastAttemptErrors: quotaError2}),
			},
			wantRRDeletions: []string{
				testRRName(1, 0),
				testRRName(1, 1),
			},
			isAlreadyReported: map[string]rrclient.ResizeRequestReportState{
				testRRName(1, 0): rrclient.ToBeReportedState,
			},
			wantEvents: []string{
				flexScaleUpFailedTestEvent(1, "QuotaExceeded", quotaErrorMessage),
			},
			wantAnyBackoffErrInfo: []cloudprovider.InstanceErrorInfo{{
				ErrorClass: cloudprovider.OutOfResourcesErrorClass,
				ErrorCode:  "QUOTA_EXCEEDED", ErrorMessage: quotaErrorMessage,
			}},
		},
		{
			name:            "multi-host TPU mig - failed rrs with different errors ",
			multihostTpuMig: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: limitExceededError, resizeBy: 4}),
				newTestRR(1, 1, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: limitExceededError, resizeBy: 4}),
				newTestRR(1, 2, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: resourcePoolError, resizeBy: 8}),
				newTestRR(3, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-15*time.Minute-10*time.Second), testRROptions{resizeBy: 2}),
				newTestRR(2, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-1*time.Minute), testRROptions{resizeBy: 1}),
			},
			wantRRDeletions: []string{
				testRRName(1, 0),
				testRRName(1, 1),
				testRRName(1, 2),
				testRRName(3, 0),
			},
			wantEvents: []string{
				// LimitExceeded Errors are grouped by main error
				flexScaleUpFailedTestEvent(8, "LimitExceeded", limitExceededErrorMessage),
				flexScaleUpFailedTestEvent(8, "ResourcePoolExhausted", resourcePoolErrorMessage),
			},
			wantAnyBackoffErrInfo: []cloudprovider.InstanceErrorInfo{
				{
					ErrorClass: cloudprovider.OtherErrorClass,
					ErrorCode:  "LIMIT_EXCEEDED", ErrorMessage: limitExceededErrorMessage,
				},
				{
					ErrorClass: cloudprovider.OutOfResourcesErrorClass,
					ErrorCode:  "RESOURCE_POOL_EXHAUSTED", ErrorMessage: resourcePoolErrorMessage,
				},
			},
		},
		{
			name:                     "non flex-start multi-host TPU mig - failed rrs with different errors",
			multihostTpuMig:          true,
			nonFlexStartNonQueuedMig: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(1, 0, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: limitExceededError, resizeBy: 4}),
				newTestRR(1, 1, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: limitExceededError, resizeBy: 4}),
				newTestRR(1, 2, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: resourcePoolError, resizeBy: 8}),
				newTestRR(3, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-15*time.Minute-10*time.Second), testRROptions{resizeBy: 2}),
				newTestRR(2, 0, rrclient.ResizeRequestStateAccepted, testTime().Add(-1*time.Minute), testRROptions{resizeBy: 1}),
			},
			wantRRDeletions: []string{
				testRRName(1, 0),
				testRRName(1, 1),
				testRRName(1, 2),
				testRRName(3, 0),
			},
			wantEvents: []string{
				// LimitExceeded Errors are grouped by main error
				atomicScaleUpFailedTestEvent(8, "LimitExceeded", limitExceededErrorMessage),
				atomicScaleUpFailedTestEvent(8, "ResourcePoolExhausted", resourcePoolErrorMessage),
			},
			wantAnyBackoffErrInfo: []cloudprovider.InstanceErrorInfo{
				{
					ErrorClass: cloudprovider.OtherErrorClass,
					ErrorCode:  "LIMIT_EXCEEDED", ErrorMessage: limitExceededErrorMessage,
				},
				{
					ErrorClass: cloudprovider.OutOfResourcesErrorClass,
					ErrorCode:  "RESOURCE_POOL_EXHAUSTED", ErrorMessage: resourcePoolErrorMessage,
				},
			},
		},
		{
			name:                     "non flex-start multi-host TPU mig - failed rrs with reservation errors",
			multihostTpuMig:          true,
			nonFlexStartNonQueuedMig: true,
			resizeRequests: []rrclient.ResizeRequestStatus{
				newTestRR(0, 0, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: invalidReservationError, resizeBy: 4}),
				newTestRR(1, 0, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: reservationNotFoundError, resizeBy: 4}),
				newTestRR(2, 0, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: reservationNotReadyError, resizeBy: 4}),
				newTestRR(3, 0, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: reservationCapacityExceededError, resizeBy: 4}),
				newTestRR(4, 0, rrclient.ResizeRequestStateFailed, testTime().Add(-3*time.Minute), testRROptions{errors: reservationIncompatibleError, resizeBy: 4}),
			},
			wantRRDeletions: []string{
				testRRName(0, 0),
				testRRName(1, 0),
				testRRName(2, 0),
				testRRName(3, 0),
				testRRName(4, 0),
			},
			wantEvents: []string{
				// LimitExceeded Errors are grouped by main error
				atomicScaleUpFailedTestEvent(4, "InvalidReservation", invalidReservationError[0].Message),
				atomicScaleUpFailedTestEvent(4, "ReservationNotFound", reservationNotFoundError[0].Message),
				atomicScaleUpFailedTestEvent(4, "ReservationNotReady", reservationNotReadyError[0].Message),
				atomicScaleUpFailedTestEvent(4, "ReservationCapacityExceeded", reservationCapacityExceededError[0].Message),
				atomicScaleUpFailedTestEvent(4, "ReservationIncompatible", reservationIncompatibleError[0].Message),
			},
			// TODO(b/421361443): errorCode instead errorReason, will be fixed with gkecl/1391580
			wantAnyBackoffErrInfo: []cloudprovider.InstanceErrorInfo{
				{
					ErrorClass: cloudprovider.OtherErrorClass,
					ErrorCode:  "INVALID_RESERVATION", ErrorMessage: invalidReservationError[0].Message,
				},
				{
					ErrorClass: cloudprovider.OtherErrorClass,
					ErrorCode:  "RESERVATION_NOT_FOUND", ErrorMessage: reservationNotFoundError[0].Message,
				},
				{
					ErrorClass: cloudprovider.OtherErrorClass,
					ErrorCode:  "RESERVATION_NOT_READY", ErrorMessage: reservationNotReadyError[0].Message,
				},
				{
					ErrorClass: cloudprovider.OtherErrorClass,
					ErrorCode:  "RESERVATION_CAPACITY_EXCEEDED", ErrorMessage: reservationCapacityExceededError[0].Message,
				},
				{
					ErrorClass: cloudprovider.OtherErrorClass,
					ErrorCode:  "RESERVATION_INCOMPATIBLE", ErrorMessage: reservationIncompatibleError[0].Message,
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Mock GKE manager calls
			// Mocks for retrieving gpuResourceName, gpuType
			// TODO(b/392582248): migrate off GkeManagerMock and introduce a new fake GkeManager instead
			gkeManagerMock := &gke.GkeManagerMock{}
			gkeManagerMock.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(&apiv1.Node{}), nil).Maybe()
			gkeManagerMock.On("IsDataplaneV2Enabled").Return(false).Maybe()
			gkeManagerMock.On("ScaleDownUnreadyTimeOverride", mock.AnythingOfType("*gke.GkeMig")).Return(time.Duration(0), false).Maybe()

			// Mock DWS Flex Start MIG
			tpuType := ""
			if tc.multihostTpuMig {
				tpuType = "tpu-type"
			}
			mig := gke.NewTestGkeMigBuilder().
				SetGceRef(gce.GceRef{Name: "dws-mig", Project: "test-proj", Zone: "us-east1-c"}).
				SetGkeManager(gkeManagerMock).
				SetExist(true).
				SetNodePoolName("dws-np").
				SetSpec(&gkeclient.NodePoolSpec{
					FlexStart:               true,
					TpuType:                 tpuType,
					TpuMultiHost:            tc.multihostTpuMig,
					MaxRunDurationInSeconds: "172800", /* 48h */
					Labels: map[string]string{
						labels.CapacityCheckWaitTimeSecondsLabel: "900", /* 15min */
					}}).Build()
			gkeManagerMock.On("CapacityCheckWaitTimeSeconds", mig).Return(15*time.Minute, nil)

			if tc.nonFlexStartNonQueuedMig {
				mig.Spec().FlexStart = false
				mig.Spec().MaxRunDurationInSeconds = ""
			}
			if tc.bulkProvisioningMig {
				mig.Spec().FlexStart = true
				mig.Spec().MachineType = "a4x-highgpu-4g"
				mig.Spec().PlacementGroup.Policy = "a4x-policy"
			}

			// Initialization
			fakeClient := &fake.Clientset{}
			recorder := kube_record.NewFakeRecorder(10)
			fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", recorder, true, "my-cool-configmap")
			nodeGroupConfigProcessor := nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 15 * time.Minute})
			provider := &gke.GkeCloudProviderMock{}
			observersList := nodegroupchange.NewNodeGroupChangeObserversList()
			ni, _ := mig.TemplateNodeInfo()
			csr := clusterstate.NewNotifiedClusterStateRegistry(
				provider,
				fakeLogRecorder,
				backoff.NewIdBasedExponentialBackoff(5*time.Minute, 10*time.Minute, 15*time.Minute),
				nodeGroupConfigProcessor,
				&templateNodeInfoRegistry{templates: map[string]*framework.NodeInfo{
					mig.Id(): ni,
				}},
				clusterstate.WithAsyncNodeGroupStateChecker(asyncnodegroups.NewDefaultAsyncNodeGroupStateChecker()),
				clusterstate.WithScaleStateNotifier(observersList),
			)

			context := &ca_context.AutoscalingContext{
				ClusterStateRegistry:   csr,
				AutoscalingKubeClients: ca_context.AutoscalingKubeClients{Recorder: recorder, LogRecorder: fakeLogRecorder},
			}

			// Mock provider calls
			provider.On("GetAvailableGPUTypes").Return(map[string]struct{}{}).Maybe()
			provider.On("GetNodeGpuConfig", mock.Anything).Return(&cloudprovider.GpuConfig{ExtendedResourceName: gpu.ResourceNvidiaGPU, Type: labels.NvidiaTeslaA100}).Maybe()
			provider.On("GetGkeMigs").Return([]*gke.GkeMig{mig})

			// Mock Resize Requests (possibly with errors/lastAttemptErrors) and failed Resize Request creations
			gkeManagerMock.On("ResizeRequests", mig).Return(append(tc.resizeRequests, tc.nonFlexResizeRequests...), nil).Once()
			gkeManagerMock.On("ResetFailedResizeRequestsCreation", mig.GceRef()).Return(tc.failedResizeRequestsCreations).Maybe()

			// Mock ResizeRequests report state and Delete calls
			wantResReqDeletionsMap := toStrSet(tc.wantRRDeletions)
			for _, rr := range tc.resizeRequests {
				gkeManagerMock.SetReportState(rr, tc.isAlreadyReported[rr.Name])
				if wantResReqDeletionsMap[rr.Name] {
					gkeManagerMock.On("AdvanceResizeRequestCleanUp", rr).Return(nil)
				}
			}

			// Initialize Error reporter
			enabledExperimentFlags := []string{experiments.FlexStartNonQueuedEnabledFlag, experiments.CapacityCheckWaitTimeSecondsMultiHostTpuEnabledFlag}
			if tc.flexStartNonQueuedExpDisabled {
				enabledExperimentFlags = []string{}
			}
			errorReporter := NewErrorReporter(experiments.NewMockManager(enabledExperimentFlags...))
			errorReporter.Init(context, provider, observersList)
			errorReporter.now = testTime

			if !tc.scaleUpAlreadyFinished {
				// Error reporter handles handles Failed FlexStart Non-Queued scale ups and cleans up Resize Requests,
				// so it needs some scale up to actually already be in progress, thus
				// Register a scale up initial size and UpdateNodes in the cluster state registry
				errorReporter.migHadEmptyResizeRequestsList[mig.GceRef()] = true
				csr.RegisterScaleUp(mig, calcInitialScaleUpSize(tc.failedResizeRequestsCreations, tc.resizeRequests, tc.isAlreadyReported), testTime().Add(-3*time.Minute-10*time.Second))
			} else {
				// Scale up has already finished so we don't register it, instead: on last ErrorReporter.Refresh we saw existing Resize Requests, so we set `migHadResizeRequestsOnLastListCall`
				// to clean up remaining Resize Requests
				errorReporter.migHadEmptyResizeRequestsList[mig.GceRef()] = false
			}

			// Trigger the tested Refresh method
			errorReporter.Refresh()
			close(recorder.Events)

			// Verify results
			// Check all expected calls were made
			if !tc.flexStartNonQueuedExpDisabled && !tc.nonFlexStartNonQueuedMig && !tc.bulkProvisioningMig {
				mock.AssertExpectationsForObjects(t, provider)
				mock.AssertExpectationsForObjects(t, gkeManagerMock)
				// Verify report states
				for _, rr := range tc.resizeRequests {
					switch {
					case tc.isAlreadyReported[rr.Name] == rrclient.ToBeReportedState || rr.State == rrclient.ResizeRequestStateCancelled || rr.State == rrclient.ResizeRequestStateFailed:
						assert.Equal(t, rrclient.AlreadyReportedState, gkeManagerMock.ResReqReportState[rr.Name])
					case rr.State == rrclient.ResizeRequestStateSucceeded:
						assert.Equal(t, rrclient.CleanUpOnlyState, gkeManagerMock.ResReqReportState[rr.Name])
					default:
						assert.Equal(t, rrclient.UnspecifiedReportState, gkeManagerMock.ResReqReportState[rr.Name])
					}
				}
			}

			// Check all expected events were emitted
			assert.Equal(t, len(tc.wantEvents), len(recorder.Events))
			gotEvents := []string{}
			for event := range recorder.Events {
				gotEvents = append(gotEvents, event)
			}
			gotEvents = reasons.SortGroupedEventsMessages(gotEvents)
			wantEvents := reasons.SortGroupedEventsMessages(tc.wantEvents)
			assert.ElementsMatch(t, wantEvents, gotEvents)

			// Check backoff
			backoffStatus := csr.BackoffStatusForNodeGroup(mig, testTime())
			assert.Equal(t, tc.wantAnyBackoffErrInfo != nil, backoffStatus.IsBackedOff)
			if tc.wantAnyBackoffErrInfo != nil {
				assert.Contains(t, tc.wantAnyBackoffErrInfo, backoffStatus.ErrorInfo)
			} else {
				assert.Equal(t, cloudprovider.InstanceErrorInfo{}, backoffStatus.ErrorInfo)
			}
		})
	}
}

func atomicScaleUpFailedTestEvent(count int, reason, message string) string {
	return fmt.Sprintf("Warning ScaleUpFailed Failed adding %d nodes to group https://www.googleapis.com/compute/v1/projects/test-proj/zones/us-east1-c/instanceGroups/dws-mig via  scale up due to %q: %s", count, reason, message)
}

func flexScaleUpFailedTestEvent(count int, reason, message string) string {
	return fmt.Sprintf("Warning FlexScaleUpFailed Failed adding %d nodes to group https://www.googleapis.com/compute/v1/projects/test-proj/zones/us-east1-c/instanceGroups/dws-mig via Flex scale up due to %q: %s", count, reason, message)
}

func flexScaleUpFailedOnTrigger(count int, reason, message string) string {
	return fmt.Sprintf("Warning FlexScaleUpFailedOnTrigger Failed adding %d nodes to group https://www.googleapis.com/compute/v1/projects/test-proj/zones/us-east1-c/instanceGroups/dws-mig via Flex scale up due to %q: %s", count, reason, message)
}

// calcInitialScaleUpSize calculates the current scale up size for the MIG saved in CSR preceding the errorReporter.Refresh based on the Resize Request and thier failed creations
func calcInitialScaleUpSize(failedResizeRequestsCreations map[error]int, resizeRequests []rrclient.ResizeRequestStatus, isAlreadyReported map[string]rrclient.ResizeRequestReportState) int {
	initScaleUpSize := 0
	// These RRs failed at creation, but still got accounted for in the scale up delta
	for _, count := range failedResizeRequestsCreations {
		initScaleUpSize += count
	}

	// Each RR contributes to scale up size,
	// but the `alreadyBeingDeleted` ones were subtracted in a previous Refresh already,
	// so the scale up size shouldn't include them
	for _, rr := range resizeRequests {
		if isAlreadyReported[rr.Name] != rrclient.AlreadyReportedState {
			initScaleUpSize++
		}
	}
	return initScaleUpSize
}

type testRROptions struct {
	resizeBy          int64
	errors            []rrclient.DwsStatusError
	lastAttemptErrors []rrclient.DwsStatusError
}

func testRRName(scaleUpID int, index uint8) string {
	return fmt.Sprintf("flex-scale-up-%d-%d", scaleUpID, index)
}

func newTestRR(scaleUpID int, index uint8, state rrclient.ResizeRequestState, creationTimestamp time.Time, options ...testRROptions) rrclient.ResizeRequestStatus {
	rr := rrclient.ResizeRequestStatus{
		// base is 256 and index has uint8 type, i.e. [0;255] range to prevent setting the same ID for different RRs
		ID:           uint64(scaleUpID*256 + int(index)),
		Name:         testRRName(scaleUpID, index),
		ResizeBy:     1,
		State:        state,
		CreationTime: creationTimestamp,
	}
	if options != nil {
		rr.ResizeBy = max(options[0].resizeBy, 1)
		rr.Errors = options[0].errors
		rr.LastAttemptErrors = options[0].lastAttemptErrors
	}
	return rr
}

func toStrSet(strs []string) map[string]bool {
	res := map[string]bool{}
	for _, str := range strs {
		res[str] = true
	}
	return res
}

type templateNodeInfoRegistry struct {
	templates map[string]*framework.NodeInfo
}

func (t *templateNodeInfoRegistry) GetNodeInfo(id string) (*framework.NodeInfo, bool) {
	ni, found := t.templates[id]
	return ni, found
}
