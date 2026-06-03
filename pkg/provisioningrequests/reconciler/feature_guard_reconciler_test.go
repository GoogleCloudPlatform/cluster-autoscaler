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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
)

func TestFeatureGuard(t *testing.T) {
	tests := []struct {
		name                    string
		pr                      *provreqwrapper.ProvisioningRequest
		experimentFlagsOverride map[string]bool
		wantState               provreqstate.ProvisioningRequestState
		wantReasonMessage       map[string]metav1.Condition
		wantPrUnreconciled      bool
	}{
		{
			name: "multiplePodSets_prUninitializedToFailed_expDisabled",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestMultiplePodSetsEnabledFlag: false,
			},
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc, provreqstate.WithMultiplePodSets(3)),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "MultiplePodSetsUnsupported",
					Message: "Provisioning Request doesn't support multiple PodSets, please define only 1.",
				},
			},
		},
		{
			name:               "multiplePodSets_prUninitialized_wantUnreconciled_expEnabled",
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc, provreqstate.WithMultiplePodSets(3)),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name: "singlePodSet_prUninitialized_wantUnreconciled_expDisabled",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestMultiplePodSetsEnabledFlag: false,
			},
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name:               "singlePodSet_prUninitialized_wantUnreconciled_noExp_meansExpEnabled",
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name: "multiplePodSets_prPendingToFailed_expDisabled",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestMultiplePodSetsMinCAVersionFlag: false,
			},
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "pending", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc, provreqstate.WithMultiplePodSets(3)),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "MultiplePodSetsUnsupported",
					Message: "Provisioning Request doesn't support multiple PodSets, please define only 1.",
				},
			},
		},
		{
			name:               "multiplePodSets_prPending_wantUnreconciled_expEnabled",
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "pending", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc, provreqstate.WithMultiplePodSets(3)),
			wantState:          provreqstate.PendingState,
			wantPrUnreconciled: true,
		},
		{
			name: "singlePodSet_prPending_wantUnreconciled_expDisabled",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestMultiplePodSetsMinCAVersionFlag: false,
			}, pr: provreqstate.ProvisioningRequestInStateForTests("default", "pending", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
			wantState:          provreqstate.PendingState,
			wantPrUnreconciled: true,
		},
		{
			name:               "singlePodSet_prPending_wantUnreconciled_expEnabled",
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "pending", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc),
			wantState:          provreqstate.PendingState,
			wantPrUnreconciled: true,
		},
		{
			name: "obtainabilityStrategy_prUninitializedToFailed_expDisabled",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestObtainabilityStrategyEnabledFlag: false,
			},
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy()),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "ObtainabilityStrategyUnsupported",
					Message: "Provisioning Request doesn't support OBTAINABILITY capacity search strategy, please remove the parameter.",
				},
			},
		},
		{
			name:               "obtainabilityStrategy_expEnabled_wantUnreconciled",
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy()),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name: "obtainabilityStrategy_expDisabled_wantFailed",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestObtainabilityStrategyMinCAVersionFlag: false,
			},
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy()),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "ObtainabilityStrategyUnsupported",
					Message: "Provisioning Request doesn't support OBTAINABILITY capacity search strategy, please remove the parameter.",
				},
			},
		},
		{
			name:               "singleZone_expEnabled_wantUnreconciled",
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name: "singleZone_expDisabled_wantUnreconciled",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestObtainabilityStrategyEnabledFlag: false,
			},
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
		{
			name: "obtainabilityStrategy_prPending_expDisabled_wantFailed",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestObtainabilityStrategyMinCAVersionFlag: false,
			},
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "pending", "", "", provreqstate.PendingState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy()),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "ObtainabilityStrategyUnsupported",
					Message: "Provisioning Request doesn't support OBTAINABILITY capacity search strategy, please remove the parameter.",
				},
			},
		},
		{
			name: "obtainabilityStrategy_prAccepted_expDisabled_wantFailed",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestObtainabilityStrategyMinCAVersionFlag: false,
			},
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "accepted", "", "", provreqstate.AcceptedState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy()),
			wantState: provreqstate.FailedState,
			wantReasonMessage: map[string]metav1.Condition{
				provreqv1.Failed: {
					Status:  metav1.ConditionTrue,
					Reason:  "ObtainabilityStrategyUnsupported",
					Message: "Provisioning Request doesn't support OBTAINABILITY capacity search strategy, please remove the parameter.",
				},
			},
		},
		{
			name: "allFeatures_expsDisabled_wantFailed",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestObtainabilityStrategyMinCAVersionFlag: false,
				experiments.ProvisioningRequestMultiplePodSetsMinCAVersionFlag:       false,
			},
			pr:        provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc, provreqstate.WithObtainabilityStrategy(), provreqstate.WithMultiplePodSets(3)),
			wantState: provreqstate.FailedState,
		},
		{
			name: "noFeatures_expsDisabled_wantUnreconciled",
			experimentFlagsOverride: map[string]bool{
				experiments.ProvisioningRequestObtainabilityStrategyMinCAVersionFlag: false,
				experiments.ProvisioningRequestMultiplePodSetsMinCAVersionFlag:       false,
			},
			pr:                 provreqstate.ProvisioningRequestInStateForTests("default", "uninitialized", "", "", provreqstate.UninitializedState, exampleInitTime, exampleTimeInc),
			wantState:          provreqstate.UninitializedState,
			wantPrUnreconciled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tt.pr)

			featureGuard := NewFeatureGuardReconciler(fakeClient, experiments.NewMockManagerWithOptions(version.Version{}, tt.experimentFlagsOverride, nil))
			prs := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{
				provreqstate.StateOfProvisioningRequest(tt.pr): {tt.pr},
			}

			gotUnreconciled, err := featureGuard.reconcileRequests(&reconcilingInput{
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
