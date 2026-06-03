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
	"time"

	"github.com/stretchr/testify/assert"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

var (
	baseTime      = time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	timeIncrement = 10 * time.Second
)

func withValidUntilSeconds(seconds string) provreqstate.ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		if pr.Spec.Parameters == nil {
			pr.Spec.Parameters = map[string]provreqv1.Parameter{}
		}
		pr.Spec.Parameters[validUntilSecondsParameterKey] = provreqv1.Parameter(seconds)
	}
}

func TestValidUntilReconciler(t *testing.T) {
	tests := []struct {
		name               string
		state              provreqstate.ProvisioningRequestState
		opts               []provreqstate.ProvReqOption
		now                time.Time
		wantState          provreqstate.ProvisioningRequestState
		wantReasonMessage  map[string]metav1.Condition
		wantPrUnreconciled bool
	}{
		{
			name:               "no_parameter_not_elapsed",
			state:              provreqstate.UninitializedState,
			opts:               []provreqstate.ProvReqOption{provreqstate.WithCreationTime(baseTime)},
			now:                baseTime.Add(100 * time.Second),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name:               "invalid_parameter_not_elapsed",
			state:              provreqstate.UninitializedState,
			opts:               []provreqstate.ProvReqOption{provreqstate.WithCreationTime(baseTime), withValidUntilSeconds("invalid")},
			now:                baseTime.Add(100 * time.Second),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name:               "valid_parameter_not_elapsed",
			state:              provreqstate.UninitializedState,
			opts:               []provreqstate.ProvReqOption{provreqstate.WithCreationTime(baseTime), withValidUntilSeconds("300")},
			now:                baseTime.Add(100 * time.Second),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name:      "valid_parameter_elapsed_uninitialized",
			state:     provreqstate.UninitializedState,
			opts:      []provreqstate.ProvReqOption{provreqstate.WithCreationTime(baseTime), withValidUntilSeconds("300")},
			now:       baseTime.Add(301 * time.Second),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "WaitTimeExceeded",
					Message: "Provisioning Request could not provision queued instances in the allocated time.",
				},
			},
		},
		{
			name:      "valid_parameter_elapsed_pending",
			state:     provreqstate.PendingState,
			opts:      []provreqstate.ProvReqOption{provreqstate.WithCreationTime(baseTime), withValidUntilSeconds("300")},
			now:       baseTime.Add(301 * time.Second),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "WaitTimeExceeded",
					Message: "Provisioning Request could not provision queued instances in the allocated time.",
				},
			},
		},
		{
			name:      "valid_parameter_elapsed_accepted",
			state:     provreqstate.AcceptedState,
			opts:      []provreqstate.ProvReqOption{provreqstate.WithCreationTime(baseTime), withValidUntilSeconds("300")},
			now:       baseTime.Add(301 * time.Second),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "WaitTimeExceeded",
					Message: "Provisioning Request could not provision queued instances in the allocated time.",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			pr := provreqstate.ProvisioningRequestInStateForTests("default", "test-pr", "", "", tt.state, baseTime, timeIncrement, tt.opts...)
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, pr)

			reconciler := NewValidUntilReconciler(fakeClient)
			prs := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{
				tt.state: {pr},
			}

			gotUnreconciled, err := reconciler.reconcileRequests(&reconcilingInput{
				prs: prs,
				now: tt.now,
			})
			assert.NoError(t, err)

			if tt.wantPrUnreconciled {
				assert.Subset(t, prs, gotUnreconciled)
				assert.Subset(t, gotUnreconciled, prs)
			} else {
				for _, v := range gotUnreconciled {
					assert.Empty(t, v)
				}
			}

			newPR, err := fakeClient.ProvisioningRequestNoCache(pr.Namespace, pr.Name)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantState, provreqstate.StateOfProvisioningRequest(newPR))

			if tt.wantReasonMessage != nil {
				for conditionType, expectedCondition := range tt.wantReasonMessage {
					conditions := newPR.Status.Conditions
					if foundCondition := k8sapimeta.FindStatusCondition(conditions, conditionType); foundCondition != nil {
						assert.Equal(t, expectedCondition.Status, foundCondition.Status)
						assert.Equal(t, expectedCondition.Reason, foundCondition.Reason)
						assert.Equal(t, expectedCondition.Message, foundCondition.Message)
					} else {
						t.Errorf("Condition %s not found in conditions: %+v", conditionType, conditions)
					}
				}
			}
		})
	}
}
