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
	"testing"

	"github.com/stretchr/testify/assert"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func TestInitialize(t *testing.T) {
	tests := []struct {
		name                         string
		pr                           *provreqwrapper.ProvisioningRequest
		expectedState                provreqstate.ProvisioningRequestState
		expectedProvReqDetailsNotSet bool
		expectedReasonMessage        map[string]metav1.Condition
	}{
		{
			name:                         "prUninitializedToPending",
			pr:                           provReqInState("default", "acc-uninitialized", "", "", provreqstate.UninitializedState),
			expectedState:                provreqstate.PendingState,
			expectedProvReqDetailsNotSet: true,
		},
		{
			name:                         "prPendingWithoutPodTemplates - failed as it is old",
			pr:                           provReqInStateWithoutPodTemplates("default", "acc-pending", "", "", provreqstate.PendingState, exampleInitTime),
			expectedProvReqDetailsNotSet: true,
			expectedState:                provreqstate.FailedState,
			expectedReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "MissingPodTemplates",
					Message: "Provisioning Request failed because there were missing pod templates, 1 pod templates were referenced, 1 templates were missing: acc-pending-pod-template-0",
				},
			}},
		{
			name:                         "prPendingWithoutPodTemplates - ignored as it is recent",
			pr:                           provReqInStateWithoutPodTemplates("default", "acc-pending", "", "", provreqstate.PendingState, recentTimestamp),
			expectedProvReqDetailsNotSet: true,
			expectedState:                provreqstate.PendingState,
			expectedReasonMessage: map[string]metav1.Condition{
				provreqv1.Accepted: {
					Status:  metav1.ConditionFalse,
					Reason:  "MissingPodTemplates",
					Message: "Provisioning Request will fail soon as it is missing pod templates, 1 pod templates were referenced, 1 templates were missing: acc-pending-pod-template-0",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tt.pr)

			prInitializer := &provisioningRequestInitializer{prClient: fakeClient}
			prs := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{
				provreqstate.StateOfProvisioningRequest(tt.pr): {tt.pr},
			}

			gotUnreconciled, err := prInitializer.reconcileRequests(&reconcilingInput{
				prs: prs,
				now: recentTimestamp,
			})
			assert.NoError(t, err)
			for _, v := range gotUnreconciled {
				assert.Empty(t, v)
			}

			newPR, err := fakeClient.ProvisioningRequestNoCache(tt.pr.Namespace, tt.pr.Name)
			assert.NoError(t, err, tt)
			assert.Equal(t, tt.expectedState, provreqstate.StateOfProvisioningRequest(newPR))
			details, found := provreqstate.GetProvisioningClassDetails(queuedwrapper.ToQueuedProvisioningRequest(*newPR))
			assert.Equal(t, tt.expectedProvReqDetailsNotSet, !found, "Found unexpected details: %v", details)
			if tt.expectedReasonMessage != nil {
				for conditionType, expectedCondition := range tt.expectedReasonMessage {
					conditions := newPR.Status.Conditions
					if foundCondition := k8sapimeta.FindStatusCondition(conditions, conditionType); foundCondition != nil {
						assert.Equal(t, expectedCondition.Status, foundCondition.Status)
						assert.Equal(t, expectedCondition.Reason, foundCondition.Reason)
						assert.Equal(t, expectedCondition.Message, foundCondition.Message)
					}
				}
			}
		})
	}
}
