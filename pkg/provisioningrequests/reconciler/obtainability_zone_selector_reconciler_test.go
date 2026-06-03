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
	apiv1 "k8s.io/api/core/v1"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
)

func withZoneNodeSelector() provreqstate.ProvReqOption {
	return func(pr *queuedwrapper.ProvisioningRequest) {
		if len(pr.PodTemplates) == 0 {
			pr.PodTemplates = []*apiv1.PodTemplate{{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
			}}
			pr.Spec.PodSets = []provreqv1.PodSet{{
				PodTemplateRef: provreqv1.Reference{Name: "default"},
			}}
		}
		if pr.PodTemplates[0].Template.Spec.NodeSelector == nil {
			pr.PodTemplates[0].Template.Spec.NodeSelector = make(map[string]string)
		}
		pr.PodTemplates[0].Template.Spec.NodeSelector[apiv1.LabelTopologyZone] = "us-central1-a"
	}
}

func TestObtainabilityZoneSelectorGuard(t *testing.T) {
	tests := []struct {
		name               string
		pr                 *provreqwrapper.ProvisioningRequest
		wantState          provreqstate.ProvisioningRequestState
		wantReasonMessage  map[string]metav1.Condition
		wantPrUnreconciled bool
	}{
		{
			name:      "obtainabilityStrategy_and_zoneSelector_uninitialized_wantFailed",
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy(), withZoneNodeSelector()),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "ObtainabilityStrategyAndZoneIncompatible",
					Message: "OBTAINABILITY capacity search strategy and zonal node selector are incompatible and cannot be used together, please use only one of the two",
				},
			},
		},
		{
			name:               "obtainabilityStrategy_only_wantUnreconciled",
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy()),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name:               "zoneSelector_only_wantUnreconciled",
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc, withZoneNodeSelector()),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name:      "obtainabilityStrategy_and_zoneSelector_pending_wantFailed",
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "pending", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy(), withZoneNodeSelector()),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "ObtainabilityStrategyAndZoneIncompatible",
					Message: "OBTAINABILITY capacity search strategy and zonal node selector are incompatible and cannot be used together, please use only one of the two",
				},
			},
		},
		{
			name:      "obtainabilityStrategy_and_zoneSelector_accepted_wantFailed",
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "accepted", "", "", provreqstate.AcceptedState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy(), withZoneNodeSelector()),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "ObtainabilityStrategyAndZoneIncompatible",
					Message: "OBTAINABILITY capacity search strategy and zonal node selector are incompatible and cannot be used together, please use only one of the two",
				},
			},
		},
		{
			name:               "neither_wantUnreconciled",
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tt.pr)

			guard := NewObtainabilityZoneSelectorReconciler(fakeClient)
			prs := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{
				provreqstate.StateOfProvisioningRequest(tt.pr): {tt.pr},
			}

			gotUnreconciled, err := guard.reconcileRequests(&reconcilingInput{
				prs: prs,
				now: recentTimestamp,
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

			newPR, err := fakeClient.ProvisioningRequestNoCache(tt.pr.Namespace, tt.pr.Name)
			assert.NoError(t, err, tt)
			assert.Equal(t, tt.wantState, provreqstate.StateOfProvisioningRequest(newPR))
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
		})
	}
}
