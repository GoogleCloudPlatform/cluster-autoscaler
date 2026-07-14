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

package provreqstate

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func TestStateOfProvisioningRequest(t *testing.T) {
	tests := []struct {
		name          string
		pr            *provreqwrapper.ProvisioningRequest
		expectedState ProvisioningRequestState
	}{
		{
			name: "No conditions - not yet acknowledged by CA",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: UninitializedState,
		},
		{
			name: "AcceptedCondition is false",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
						},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: PendingState,
		},
		{
			name: "AcceptedCondition is true",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
						},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: AcceptedState,
		},
		{
			name: "AcceptedCondition is true with Provisioned set to false",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
						},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: AcceptedState,
		},
		{
			name: "Missing Accepted condition, deprecated present instead",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionFalse},
						},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "Pending state - old set up",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: PendingState,
		},
		{
			name: "Accepted state - old set up",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: AcceptedState,
		},
		{
			name: "Provisioned state - old set up",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: ProvisionedState,
		},
		{
			name: "Provisioned state - new set up",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: ProvisionedState,
		},
		{
			name: "BookingExpired state - new set up",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pr-1",
						Namespace: "test-1",
					},
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.BookingExpired, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: BookingExpiredState,
		},
		{
			name: "CapacityRevoked state - new set up",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pr-2",
						Namespace: "test-2",
					},
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.BookingExpired, Status: metav1.ConditionTrue},
							{Type: prv1.CapacityRevoked, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: CapacityRevokedState,
		},
		{
			name: "BookingExpired without Provisioned - invalid",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.BookingExpired, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "BookingExpired without Accepted - invalid",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.BookingExpired, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "BookingExpired with Failed - invalid",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.BookingExpired, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "CapacityRevoked without Accepted - invalid",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.BookingExpired, Status: metav1.ConditionTrue},
							{Type: prv1.CapacityRevoked, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "CapacityRevoked without Provisioned - invalid",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.BookingExpired, Status: metav1.ConditionTrue},
							{Type: prv1.CapacityRevoked, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "CapacityRevoked without BookingExpired - invalid",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.BookingExpired, Status: metav1.ConditionFalse},
							{Type: prv1.CapacityRevoked, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "CapacityRevoked with Failed - invalid",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.BookingExpired, Status: metav1.ConditionTrue},
							{Type: prv1.CapacityRevoked, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "Provisioned but not Accepted - Invalid state old set up",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "Provisioned but not Accepted - Invalid state new set up",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "Failed and Accepted with Provisioned set to False",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: FailedState,
		},
		{
			name: "Failed and Accepted",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: FailedState,
		},
		{
			name: "Failed state without Provisioned",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: FailedState,
		},
		{
			name: "Failed state with Provisioned",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: FailedState,
		},
		{
			name: "Multiple Accepted conditions",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "Multiple true incompatible conditions",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "Unrecognized condition type present",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: "UnrecognizedType", Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "Condition with 'Unknown' status present",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionUnknown},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: InvalidState,
		},
		{
			name: "Pending state with deprecated False Provisioning condition",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: PendingState,
		},
		{
			name: "Pending state with deprecated True Provisioning condition",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: PendingState,
		},
		{
			name: "Accepted state with deprecated False Provisioning condition",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: AcceptedState,
		},
		{
			name: "Accepted state with deprecated True Provisioning condition",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: AcceptedState,
		},
		{
			name: "Provisioned state with deprecated False Provisioning condition",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: ProvisionedState,
		},
		{
			name: "Provisioned state with deprecated True Provisioning condition",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: ProvisionedState,
		},
		{
			name: "Failed state with deprecated False Provisioning condition",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: FailedState,
		},
		{
			name: "Failed state with deprecated True Provisioning condition",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionTrue}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState: FailedState,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := StateOfProvisioningRequest(tc.pr); got != tc.expectedState {
				t.Errorf("stateOfProvisioningRequest: %s = %v, want %v", tc.name, got, tc.expectedState)
			}
		})
	}
}

func TestStatusOfProvisioningRequest(t *testing.T) {
	tests := []struct {
		name           string
		pr             *provreqwrapper.ProvisioningRequest
		expectedState  ProvisioningRequestState
		expectedReason string
	}{
		{
			name: "No conditions - not yet acknowledged by CA - uninitialized state - Beta",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState:  UninitializedState,
			expectedReason: string(UninitializedState),
		},
		{
			name: "Accepted = false - Beta",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
						},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState:  PendingState,
			expectedReason: string(PendingState),
		},
		{
			name: "Pending state - Beta",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState:  PendingState,
			expectedReason: string(PendingState),
		},
		{
			name: "Accepted state - Beta",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue, Reason: acceptedReason},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState:  AcceptedState,
			expectedReason: acceptedReason,
		},
		{
			name: "Provisioned state - Beta",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue, Reason: provisionedReason},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState:  ProvisionedState,
			expectedReason: provisionedReason,
		},
		{
			name: "Provisioned but not Accepted - Invalid state - Beta",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionTrue, Reason: provisionedReason},
							{Type: prv1.Failed, Status: metav1.ConditionFalse}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState:  InvalidState,
			expectedReason: string(InvalidState),
		},
		{
			name: "Failed and Accepted - Beta",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionTrue},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionTrue, Reason: defaultFailedReason}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState:  FailedState,
			expectedReason: defaultFailedReason,
		},
		{
			name: "Failed only state - Beta",
			pr: provreqwrapper.NewProvisioningRequest(
				&prv1.ProvisioningRequest{
					Status: prv1.ProvisioningRequestStatus{
						Conditions: []metav1.Condition{
							{Type: prv1.Accepted, Status: metav1.ConditionFalse},
							{Type: DeprecatedProvisioningCondition, Status: metav1.ConditionFalse},
							{Type: prv1.Provisioned, Status: metav1.ConditionFalse},
							{Type: prv1.Failed, Status: metav1.ConditionTrue, Reason: defaultFailedReason}},
					},
				},
				[]*apiv1.PodTemplate{},
			),
			expectedState:  FailedState,
			expectedReason: defaultFailedReason,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status := StatusOfProvisioningRequest(tc.pr)
			if status.State != tc.expectedState || status.Reason != tc.expectedReason {
				t.Errorf("StatusOfProvisioningRequest: %s: got %q state and %q reason, want %q state and %q reason", tc.name, status.State, status.Reason, tc.expectedState, tc.expectedReason)

			}
		})
	}
}

func TestSetProvisioningClassDetails(t *testing.T) {
	exampleInitTime := time.Date(2022, 1, 2, 1, 2, 3, 4, time.UTC)
	exampleTimeInc := time.Minute
	migName := "gke-test-cluster-example-test2-840da30b-grp"
	resizeRequestName := "gke-default-acc1-9f5a3766909d1de9"

	tests := []struct {
		name              string
		pr                *provreqwrapper.ProvisioningRequest
		nodeGroupName     string
		resizeRequestName string
		wantError         bool
	}{
		{
			name:              "Assign Pending Provisioning Request - Beta",
			pr:                ProvisioningRequestWithConditionsForTests("", "PendingState", conditionsForState(PendingState, exampleInitTime, exampleTimeInc), "", ""),
			nodeGroupName:     migName,
			resizeRequestName: resizeRequestName,
		},
		{
			name:      "Assign already assigned Pending Provisioning Request, fail - Beta",
			pr:        ProvisioningRequestWithConditionsForTests("", "AssignedPending", conditionsForState(PendingState, exampleInitTime, exampleTimeInc), resizeRequestName, migName),
			wantError: true,
		},
		{
			name:      "Assign not Pending (e.g. Accepted) Provisioning Request, fail - Beta",
			pr:        ProvisioningRequestWithConditionsForTests("", "Accepted", conditionsForState(AcceptedState, exampleInitTime, exampleTimeInc), resizeRequestName, migName),
			wantError: true,
		},
		{
			name:              "Assign Pending Provisioning Request - Obtainability Strategy",
			pr:                ProvisioningRequestWithConditionsForTests("", "PendingState", conditionsForState(PendingState, exampleInitTime, exampleTimeInc), "", "", WithObtainabilityStrategy()),
			nodeGroupName:     migName,
			resizeRequestName: resizeRequestName,
		},
		{
			name: "Assign already assigned Pending Provisioning Request - Obtainability Strategy",
			pr: ProvisioningRequestWithConditionsForTests("", "AssignedPending", conditionsForState(PendingState, exampleInitTime, exampleTimeInc), "", "", WithObtainabilityStrategy(),
				WithDetails(&queuedwrapper.ProvisioningClassDetails{
					ResizeRequestName:       "some-rr-name",
					NodePoolAutoProvisioned: queuedwrapper.AutoprovisionedStatusFromBool(true),
				})),
			nodeGroupName:     migName,
			resizeRequestName: resizeRequestName,
		},
		{
			name: "Assign not Pending (e.g. Accepted) Provisioning Request - Obtainability Strategy",
			pr: ProvisioningRequestWithConditionsForTests("", "Accepted", conditionsForState(AcceptedState, exampleInitTime, exampleTimeInc), "", "", WithObtainabilityStrategy(),
				WithDetails(&queuedwrapper.ProvisioningClassDetails{
					NodeGroupName:           "some-node-group-name",
					NodePoolAutoProvisioned: queuedwrapper.AutoprovisionedStatusFromBool(true),
				})),
			nodeGroupName:     migName,
			resizeRequestName: resizeRequestName,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			details := &queuedwrapper.ProvisioningClassDetails{
				NodeGroupName:           tt.nodeGroupName,
				ResizeRequestName:       tt.resizeRequestName,
				NodePoolName:            fmt.Sprintf("np-%s", tt.nodeGroupName),
				AcceleratorType:         fmt.Sprintf("gpu-type-%d", i),
				SelectedZone:            fmt.Sprintf("europe-west%d-c", i),
				NodePoolAutoProvisioned: queuedwrapper.AutoprovisionedStatusFromBool(true),
				PodTemplateName:         fmt.Sprintf("pod-template-%d", i),
				ProvisioningMode:        queuedwrapper.ProvisioningModeResizeRequest,
			}
			err := SetProvisioningClassDetails(tt.pr, details)
			qpr := queuedwrapper.ToQueuedProvisioningRequest(*tt.pr)
			if tt.wantError {
				assert.Error(t, err, tt)
			} else {
				assert.NoError(t, err, tt)
				assert.Equal(t, *qpr.ResizeRequestName(), tt.resizeRequestName)
				assert.Equal(t, *qpr.NodeGroupName(), tt.nodeGroupName)
				assert.Equal(t, *qpr.NodePoolName(), details.NodePoolName)
				assert.Equal(t, *qpr.AcceleratorType(), details.AcceleratorType)
				assert.Equal(t, *qpr.SelectedZone(), details.SelectedZone)
				assert.Equal(t, *qpr.NodePoolAutoProvisioned(), "true")
				assert.Equal(t, *qpr.PodTemplateName(), details.PodTemplateName)
				assert.Equal(t, *qpr.ProvisioningMode(), details.ProvisioningMode)
			}
		})
	}
}

func mustParse(t *testing.T, timeStr string) metav1.Time {
	t.Helper()
	parsedTime, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		t.Fatalf("Couldn't parse test time %q: %v", timeStr, err)
	}
	return metav1.NewTime(parsedTime)
}

var (
	initTime             = "2023-01-12T12:13:03Z"
	firstTransitionTime  = "2023-01-12T12:13:33Z"
	secondTransitionTime = "2023-01-12T12:14:03Z"
	thirdTransitionTime  = "2023-01-12T12:15:03Z"
)

func TestUpdateOrSetProvisioningRequestCondition(t *testing.T) {
	initV1Time := mustParse(t, initTime)
	firstTransitionV1Time := mustParse(t, firstTransitionTime)
	secondTransitionV1Time := mustParse(t, secondTransitionTime)
	thirdTransitionV1Time := mustParse(t, thirdTransitionTime)

	tests := []struct {
		name                  string
		pr                    *provreqwrapper.ProvisioningRequest
		conditionType         string
		conditionStatus       metav1.ConditionStatus
		customReason          string
		customMessage         string
		transitionTime        metav1.Time
		expectedPr            *provreqwrapper.ProvisioningRequest
		wantConditionsUpdated bool
	}{
		{
			name: "Update Accepted condition status, message and reason - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			conditionType:   prv1.Accepted,
			conditionStatus: metav1.ConditionTrue,
			customMessage:   "Provisioning Request was successfully queued.",
			customReason:    "SuccessfullyQueued",
			transitionTime:  firstTransitionV1Time,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			wantConditionsUpdated: true,
		},
		{
			name: "Update Accepted condition message - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			conditionType:   prv1.Accepted,
			conditionStatus: metav1.ConditionTrue,
			customMessage:   "Accepted condition message changed.",
			customReason:    "SuccessfullyQueued",
			transitionTime:  secondTransitionV1Time,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(secondTransitionV1Time, "Accepted condition message changed.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			wantConditionsUpdated: true,
		},
		{
			name: "Update Provisioned condition message and reason - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			conditionType:   prv1.Provisioned,
			conditionStatus: metav1.ConditionFalse,
			customMessage:   "Quota 'A2_CPUS' exceeded.  Limit: 192.0 in region us-central1.",
			customReason:    "QuotaExceeded",
			transitionTime:  secondTransitionV1Time,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(secondTransitionV1Time, "Quota 'A2_CPUS' exceeded.  Limit: 192.0 in region us-central1.", 0, "QuotaExceeded", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			wantConditionsUpdated: true,
		},
		{
			name: "Update Failed condition message and reason - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request has failed.", 0, "Failed", metav1.ConditionFalse, prv1.Failed),
				}),
			conditionType:   prv1.Failed,
			conditionStatus: metav1.ConditionTrue,
			customMessage:   "Requested resource is unavailable in this zone.",
			customReason:    "ResourceNotInZone",
			transitionTime:  secondTransitionV1Time,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(secondTransitionV1Time, "Requested resource is unavailable in this zone.", 0, "ResourceNotInZone", metav1.ConditionTrue, prv1.Failed),
				}),
			wantConditionsUpdated: true,
		},
		{
			name: "Nothing changed on Accepted condition - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Requested resource is unavailable in this zone.", 0, "ResourceNotInZone", metav1.ConditionTrue, prv1.Failed),
				}),
			conditionType:   prv1.Accepted,
			conditionStatus: metav1.ConditionTrue,
			customMessage:   "Provisioning Request was successfully queued.",
			customReason:    "SuccessfullyQueued",
			transitionTime:  secondTransitionV1Time,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Requested resource is unavailable in this zone.", 0, "ResourceNotInZone", metav1.ConditionTrue, prv1.Failed),
				}),
			wantConditionsUpdated: false,
		},
		{
			name: "Nothing changed on Provisioned condition - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(secondTransitionV1Time, "Quota 'A2_CPUS' exceeded.  Limit: 192.0 in region us-central1.", 0, "QuotaExceeded", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Requested resource is unavailable in this zone.", 0, "ResourceNotInZone", metav1.ConditionTrue, prv1.Failed),
				}),
			conditionType:   prv1.Provisioned,
			conditionStatus: metav1.ConditionFalse,
			customMessage:   "Quota 'A2_CPUS' exceeded.  Limit: 192.0 in region us-central1.",
			customReason:    "QuotaExceeded",
			transitionTime:  thirdTransitionV1Time,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(secondTransitionV1Time, "Quota 'A2_CPUS' exceeded.  Limit: 192.0 in region us-central1.", 0, "QuotaExceeded", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Requested resource is unavailable in this zone.", 0, "ResourceNotInZone", metav1.ConditionTrue, prv1.Failed),
				}),
			wantConditionsUpdated: false,
		},
		{
			name: "Nothing changed on Failed condition - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Requested resource is unavailable in this zone.", 0, "ResourceNotInZone", metav1.ConditionTrue, prv1.Failed),
				}),
			conditionType:   prv1.Failed,
			conditionStatus: metav1.ConditionTrue,
			customMessage:   "Requested resource is unavailable in this zone.",
			customReason:    "ResourceNotInZone",
			transitionTime:  secondTransitionV1Time,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("TestProvisioningRequestName",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Requested resource is unavailable in this zone.", 0, "ResourceNotInZone", metav1.ConditionTrue, prv1.Failed),
				}),
			wantConditionsUpdated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditionUpdated := UpdateOrSetProvisioningRequestCondition(tt.pr, tt.conditionType, tt.conditionStatus, tt.customReason, tt.customMessage, tt.transitionTime)
			assert.Equal(t, tt.wantConditionsUpdated, conditionUpdated)
			if diff := cmp.Diff(tt.expectedPr.ProvisioningRequest, tt.pr.ProvisioningRequest); diff != "" {
				t.Errorf("Wrong updated ProvisioningRequest.V1Beta in %q diff (-want +got):\n%s", tt.name, diff)
			}
		})
	}
}

func TestSetStateCustomReasonMessage(t *testing.T) {
	initV1Time := mustParse(t, initTime)
	firstTransitionV1Time := mustParse(t, firstTransitionTime)
	secondTransitionV1Time := mustParse(t, secondTransitionTime)
	thirdTransitionV1Time := mustParse(t, thirdTransitionTime)

	tests := []struct {
		name           string
		pr             *provreqwrapper.ProvisioningRequest
		expectedPr     *provreqwrapper.ProvisioningRequest
		state          ProvisioningRequestState
		customReason   string
		customMessage  string
		transitionTime metav1.Time
		wantError      bool
	}{
		{
			name: "Update from Unintialized to Pending state - not permitted with custom reason/message - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("UninitializedToPending",
				[]metav1.Condition{}),
			customMessage:  "Custom Pending message.",
			state:          PendingState,
			transitionTime: thirdTransitionV1Time,
			wantError:      true,
		},
		{
			name: "Update from Uninitialized to Accepted state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("UninitializedToAcceptedState",
				[]metav1.Condition{}),
			state:          AcceptedState,
			customMessage:  "Example custom message.",
			transitionTime: thirdTransitionV1Time,
			wantError:      true,
		},
		{
			name: "Update from Uninitialized to Provisioned state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("UninitializedToProvisionedState",
				[]metav1.Condition{}),
			state:          ProvisionedState,
			customReason:   "ExampleCustomReason",
			transitionTime: thirdTransitionV1Time,
			wantError:      true,
		},
		{
			name: "Update from Pending (all False conditions) to Failed state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("PendingStateAllFalse",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("PendingStateAllFalse",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(firstTransitionV1Time, "Requested resource is unavailable in this zone.", 0, "ResourceNotInZone", metav1.ConditionTrue, prv1.Failed),
				}),
			state:          FailedState,
			customMessage:  "Requested resource is unavailable in this zone.",
			customReason:   "ResourceNotInZone",
			transitionTime: firstTransitionV1Time,
		},
		{
			name: "Update from Accepted to Failed state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedStateFails",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedStateFails",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(secondTransitionV1Time, "Provisioning Request has failed.", 0, "QueueingTimeExceeded", metav1.ConditionTrue, prv1.Failed),
				}),
			customReason:   "QueueingTimeExceeded",
			state:          FailedState,
			transitionTime: secondTransitionV1Time,
		},
		{
			name: "Update from Accepted to Provisioned state (Conditions order in PR shouldn't matter) - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedStateGetsProvisioned",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
				}),
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedStateGetsProvisioned",
				[]metav1.Condition{
					condition(secondTransitionV1Time, "Provisioning Request was successfully provisioned.", 0, "ResizeRequestSucceeded", metav1.ConditionTrue, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
				}),
			state:          ProvisionedState,
			customReason:   "ResizeRequestSucceeded",
			transitionTime: secondTransitionV1Time,
		},
		{
			name: "Update from Accepted to Provisioned state with deprecated Provisioning condition present - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedStateGetsProvisioned",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request isn't being provisioned.", 0, "NotProvisioning", metav1.ConditionFalse, DeprecatedProvisioningCondition),
				}),
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedStateGetsProvisioned",
				[]metav1.Condition{
					condition(thirdTransitionV1Time, "Provisioning Request was successfully provisioned.", 0, "ResizeRequestSucceeded", metav1.ConditionTrue, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request isn't being provisioned.", 0, "NotProvisioning", metav1.ConditionFalse, DeprecatedProvisioningCondition),
				}),
			state:          ProvisionedState,
			customReason:   "ResizeRequestSucceeded",
			transitionTime: thirdTransitionV1Time,
		},
		{
			name: "Update from Failed to Failed - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("FailedStateToFailedState",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(thirdTransitionV1Time, "Provisioning Request has failed.", 1, "Failed", metav1.ConditionTrue, prv1.Failed),
				}),
			state:          FailedState,
			transitionTime: thirdTransitionV1Time,
			wantError:      true,
		},
		{
			name: "Update from Accepted to Accepted state with new reason - changes reason - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedToAccepted",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedToAccepted",
				[]metav1.Condition{
					condition(secondTransitionV1Time, "Provisioning Request was successfully queued.", 0, "CustomAcceptedReason", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			customReason:   "CustomAcceptedReason",
			state:          AcceptedState,
			transitionTime: secondTransitionV1Time,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SetStateCustomReasonMessage(tt.pr, tt.state, tt.customReason, tt.customMessage, tt.transitionTime)

			if tt.wantError {
				assert.Error(t, err, tt)
			} else {
				assert.NoError(t, err, tt)

				if diff := cmp.Diff(tt.expectedPr.ProvisioningRequest, tt.pr.ProvisioningRequest); diff != "" {
					t.Errorf("Wrong updated ProvisioningRequest.V1Beta in %q diff (-want +got):\n%s", tt.name, diff)
				}
			}
		})
	}
}

func TestSetState(t *testing.T) {
	initV1Time := mustParse(t, initTime)
	firstTransitionV1Time := mustParse(t, firstTransitionTime)
	secondTransitionV1Time := mustParse(t, secondTransitionTime)
	thirdTransitionV1Time := mustParse(t, thirdTransitionTime)

	tests := []struct {
		name           string
		pr             *provreqwrapper.ProvisioningRequest
		state          ProvisioningRequestState
		expectedPr     *provreqwrapper.ProvisioningRequest
		transitionTime metav1.Time
		wantError      bool
	}{
		{
			name: "Update from Uninitialized to Pending state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("UninitializedToPendingState",
				[]metav1.Condition{}),
			state: PendingState,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("UninitializedToPendingState",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
				}),
			transitionTime: initV1Time,
		},
		{
			name: "Update from Uninitialized to Provisioned state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("UninitializedToProvisioningState",
				[]metav1.Condition{}),
			state:          ProvisionedState,
			transitionTime: thirdTransitionV1Time,
			wantError:      true,
		},
		{
			name: "Update from Uninitialized to Failed state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("UninitializedToFailedState",
				[]metav1.Condition{}),
			state:          FailedState,
			transitionTime: thirdTransitionV1Time,
			wantError:      true,
		},
		{
			name: "Update from Pending (all False conditions) to Failed state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("PendingStateAllFalse",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			state: FailedState,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("PendingStateAllFalse",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(firstTransitionV1Time, "Provisioning Request has failed.", 0, "Failed", metav1.ConditionTrue, prv1.Failed),
				}),
			transitionTime: firstTransitionV1Time,
		},
		{
			name: "Update from Pending to Accepted state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("PendingToAccepted",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request isn't being provisioned.", 0, "NotProvisioning", metav1.ConditionFalse, DeprecatedProvisioningCondition),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			state: AcceptedState,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("PendingToAccepted",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request isn't being provisioned.", 0, "NotProvisioning", metav1.ConditionFalse, DeprecatedProvisioningCondition),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			transitionTime: firstTransitionV1Time,
		},
		{
			name: "Update from Accepted to Provisioned state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedToProvisioning",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			state: ProvisionedState,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedToProvisioning",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(secondTransitionV1Time, "Provisioning Request was successfully provisioned.", 0, "Provisioned", metav1.ConditionTrue, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			transitionTime: secondTransitionV1Time,
		},
		{
			name: "Update from Accepted to Failed state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedToFailed",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			state: FailedState,
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedToFailed",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(secondTransitionV1Time, "Provisioning Request has failed.", 0, "Failed", metav1.ConditionTrue, prv1.Failed),
				}),
			transitionTime: secondTransitionV1Time,
		},
		{
			name: "Update from Accepted to Accepted state - permitted - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedToAccepted",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			expectedPr: basicProvisioningRequestBetaWithConditionsForTests("AcceptedToAccepted",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 0, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			state:          AcceptedState,
			transitionTime: secondTransitionV1Time,
		},
		{
			name: "Update from CapacityRevoked to Failed state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("CapacityRevokedToFailed",
				[]metav1.Condition{
					condition(firstTransitionV1Time, "Provisioning Request was successfully queued.", 1, "SuccessfullyQueued", metav1.ConditionTrue, prv1.Accepted),
					condition(secondTransitionV1Time, "Provisioning Request was successfully provisioned.", 1, "Provisioned", metav1.ConditionTrue, prv1.Provisioned),
					condition(thirdTransitionV1Time, bookingExpiredMessage, 1, bookingExpiredReason, metav1.ConditionTrue, prv1.BookingExpired),
					condition(thirdTransitionV1Time, capacityRevokedMessage, 1, capacityRevokedReason, metav1.ConditionTrue, prv1.CapacityRevoked),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			state:          FailedState,
			transitionTime: thirdTransitionV1Time,
			wantError:      true,
		},
		{
			name: "Update from Pending to Uninitialized state - Beta",
			pr: basicProvisioningRequestBetaWithConditionsForTests("PendingToUninitialized",
				[]metav1.Condition{
					condition(initV1Time, "Provisioning Request wasn't accepted.", 0, "NotAccepted", metav1.ConditionFalse, prv1.Accepted),
					condition(initV1Time, "Provisioning Request wasn't provisioned.", 0, "NotProvisioned", metav1.ConditionFalse, prv1.Provisioned),
					condition(initV1Time, "Provisioning Request hasn't failed.", 0, "NotFailed", metav1.ConditionFalse, prv1.Failed),
				}),
			state:          UninitializedState,
			transitionTime: thirdTransitionV1Time,
			wantError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SetState(tt.pr, tt.state, tt.transitionTime)

			if tt.wantError {
				assert.Error(t, err, tt)
			} else {
				assert.NoError(t, err, tt)
				if diff := cmp.Diff(tt.expectedPr.ProvisioningRequest, tt.pr.ProvisioningRequest); diff != "" {
					t.Errorf("Wrong updated ProvisioningRequest.V1Beta in %q diff (-want +got):\n%s", tt.name, diff)
				}
			}
		})
	}
}

func basicProvisioningRequestBetaWithConditionsForTests(name string, conditions []metav1.Condition, opts ...ProvReqOption) *provreqwrapper.ProvisioningRequest {
	return ProvisioningRequestWithConditionsForTests("default", name, conditions, "", "", opts...)
}

func TestClearProvisioningClassDetails(t *testing.T) {
	initTime := time.Date(2022, 11, 12, 13, 14, 15, 0, time.UTC)
	timeIncrease := time.Minute
	tests := []struct {
		name    string
		pr      *provreqwrapper.ProvisioningRequest
		wantErr bool
		wantPR  *provreqwrapper.ProvisioningRequest
	}{
		{
			name:   "simple test - Beta",
			pr:     ProvisioningRequestInStateForTests("default", "PendingState", "", "", PendingState, initTime, timeIncrease),
			wantPR: ProvisioningRequestInStateForTests("default", "PendingState", "", "", PendingState, initTime, timeIncrease),
		},
		{
			name:   "names are cleared - Beta",
			pr:     ProvisioningRequestInStateForTests("default", "PendingState", "test-name-resize-request", "test-name-mig", PendingState, initTime, timeIncrease),
			wantPR: ProvisioningRequestInStateForTests("default", "PendingState", "", "", PendingState, initTime, timeIncrease),
		},
		{
			name:    "accepted PR fails to update - Beta",
			pr:      ProvisioningRequestInStateForTests("default", "AcceptedState", "test-name-resize-request", "test-name-mig", AcceptedState, initTime, timeIncrease),
			wantPR:  ProvisioningRequestInStateForTests("default", "AcceptedState", "test-name-resize-request", "test-name-mig", AcceptedState, initTime, timeIncrease),
			wantErr: true,
		},
		{
			name:    "failed PR fails to update - Beta",
			pr:      ProvisioningRequestInStateForTests("default", "FailedState", "test-name-resize-request", "test-name-mig", FailedState, initTime, timeIncrease),
			wantPR:  ProvisioningRequestInStateForTests("default", "FailedState", "test-name-resize-request", "test-name-mig", FailedState, initTime, timeIncrease),
			wantErr: true,
		},
		{
			name:    "provisioned PR fails to update - Beta",
			pr:      ProvisioningRequestInStateForTests("default", "ProvisionedState", "test-name-resize-request", "test-name-mig", ProvisionedState, initTime, timeIncrease),
			wantPR:  ProvisioningRequestInStateForTests("default", "ProvisionedState", "test-name-resize-request", "test-name-mig", ProvisionedState, initTime, timeIncrease),
			wantErr: true,
		},
		{
			name:    "invalid PR fails to update - Beta",
			pr:      ProvisioningRequestInStateForTests("default", "InvalidState", "test-name-resize-request", "test-name-mig", InvalidState, initTime, timeIncrease),
			wantPR:  ProvisioningRequestInStateForTests("default", "InvalidState", "test-name-resize-request", "test-name-mig", InvalidState, initTime, timeIncrease),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ClearProvisioningClassDetails(tt.pr); (err != nil) != tt.wantErr {
				t.Errorf("ClearProvisioningClassDetails() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.wantPR.ProvisioningRequest, tt.pr.ProvisioningRequest); diff != "" {
				t.Errorf("Expected ProvisioningRequests.V1Beta differ, diff (-want + got):\n%s", diff)
			}
		})
	}
}
