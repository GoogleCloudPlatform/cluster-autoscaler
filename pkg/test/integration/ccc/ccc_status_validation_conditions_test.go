/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ccc_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/validator/conditions"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
)

// TestCCCStatusValidationConditions verifies that configuration validation errors
// (e.g., invalid machine family or GPU configs) are successfully caught, formatted,
// and deduplicated into precise CRD conditions.
func TestCCCStatusValidationConditions(t *testing.T) {
	testCases := []struct {
		name                string
		priorities          []v1.Priority
		wantHealthStatus    metav1.ConditionStatus
		wantMisconfigReason string
	}{
		{
			name: "ValidConfiguration",
			priorities: []v1.Priority{
				{
					MachineFamily: ptr.To("n1"),
					MinCores:      ptr.To(2),
					MinMemoryGb:   ptr.To(4),
				},
			},
			wantHealthStatus: metav1.ConditionTrue,
		},
		{
			name: "InvalidMachineFamily",
			priorities: []v1.Priority{
				{
					MachineFamily: ptr.To("nonexistent-family"),
					MinCores:      ptr.To(2),
					MinMemoryGb:   ptr.To(4),
				},
			},
			wantHealthStatus:    metav1.ConditionFalse,
			wantMisconfigReason: conditions.MachineFamilyNotFoundReason,
		},
		{
			name: "InvalidGpuConfig",
			priorities: []v1.Priority{
				{
					MachineFamily: ptr.To("n1"),
					Gpu: &v1.GPU{
						Type:  "nvidia-tesla-invalid",
						Count: 128,
					},
					MinCores:    ptr.To(2),
					MinMemoryGb: ptr.To(4),
				},
			},
			wantHealthStatus:    metav1.ConditionFalse,
			wantMisconfigReason: conditions.NoSuitableMachineExistsReason,
		},
	}

	t.Run("RuleValidation_LegacyReporting", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				crdObj := ccc.NewComputeClassBuilder(tc.name).
					WithNapEnabled().
					WithPriorities(tc.priorities...).
					Build()

				nodePool := integration.DefaultNodePool()

				testConfig := integration.NewTestConfig().
					WithNodePools(nodePool).
					WithCccCrds(crdObj).
					WithOverrides(
						integration.WithEnhancedCrdStatusReporting(false),
						integration.WithCccNodeAutoprovisioningEnabled(),
					)

				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					infra := integration.SetupInfrastructure(ctx, t)

					autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
					assert.NoError(t, err)
					defer integration_synctest.TearDown(cancel)

					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)

					updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, tc.name, metav1.GetOptions{})
					assert.NoError(t, err)
					conds := updatedCCC.Status.Conditions

					healthCond := k8sapimeta.FindStatusCondition(conds, conditions.HealthCondition)
					assert.NotNil(t, healthCond, "Health condition should be present")
					if healthCond != nil {
						assert.Equal(t, tc.wantHealthStatus, healthCond.Status, "Health condition status mismatch")
					}

					misconfigCond := k8sapimeta.FindStatusCondition(conds, conditions.CrdMisconfiguredCondition)
					if tc.wantMisconfigReason == "" {
						assert.Nil(t, misconfigCond, "No CrdMisconfigured condition should exist for valid configs")
					} else {
						assert.NotNil(t, misconfigCond, "CrdMisconfigured condition should be present")
						if misconfigCond != nil {
							assert.Equal(t, metav1.ConditionTrue, misconfigCond.Status)
							assert.Equal(t, tc.wantMisconfigReason, misconfigCond.Reason)
						}
					}
				})
			})
		}
	})

	t.Run("RuleValidation_EnhancedReporting", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				crdObj := ccc.NewComputeClassBuilder(tc.name).
					WithNapEnabled().
					WithPriorities(tc.priorities...).
					Build()

				nodePool := integration.DefaultNodePool()

				testConfig := integration.NewTestConfig().
					WithNodePools(nodePool).
					WithCccCrds(crdObj).
					WithOverrides(
						integration.WithEnhancedCrdStatusReporting(true),
						integration.WithCccNodeAutoprovisioningEnabled(),
					)

				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					infra := integration.SetupInfrastructure(ctx, t)

					autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
					assert.NoError(t, err)
					defer integration_synctest.TearDown(cancel)

					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)
					time.Sleep(30 * time.Second)

					updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, tc.name, metav1.GetOptions{})
					assert.NoError(t, err)
					conds := updatedCCC.Status.Conditions

					healthCond := k8sapimeta.FindStatusCondition(conds, conditions.HealthCondition)
					assert.NotNil(t, healthCond, "Health condition should be present")
					if healthCond != nil {
						assert.Equal(t, tc.wantHealthStatus, healthCond.Status, "Health condition status mismatch")
					}

					misconfigCond := k8sapimeta.FindStatusCondition(conds, conditions.CrdMisconfiguredCondition)
					if tc.wantMisconfigReason == "" {
						assert.Nil(t, misconfigCond, "No CrdMisconfigured condition should exist for valid configs")
					} else {
						assert.NotNil(t, misconfigCond, "CrdMisconfigured condition should be present")
						if misconfigCond != nil {
							assert.Equal(t, metav1.ConditionTrue, misconfigCond.Status)
							assert.Equal(t, tc.wantMisconfigReason, misconfigCond.Reason)
						}
					}
				})
			})
		}
	})

	t.Run("ConditionLifecycle_DeduplicationAndSpuriousUpdatePrevention", func(t *testing.T) {
		crdObj := ccc.NewComputeClassBuilder("dedup-ccc").
			WithNapEnabled().
			WithPriorities(v1.Priority{
				MachineFamily: ptr.To("nonexistent-dedup"),
				MinCores:      ptr.To(2),
				MinMemoryGb:   ptr.To(4),
			}).
			Build()

		nodePool := integration.DefaultNodePool()

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePool).
			WithCccCrds(crdObj).
			WithOverrides(
				integration.WithEnhancedCrdStatusReporting(false),
				integration.WithCccNodeAutoprovisioningEnabled(),
			)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			infra := integration.SetupInfrastructure(ctx, t)

			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			assert.NoError(t, err)
			defer integration_synctest.TearDown(cancel)

			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Millisecond)

			updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, "dedup-ccc", metav1.GetOptions{})
			assert.NoError(t, err)
			initialConds := updatedCCC.Status.Conditions
			assert.Equal(t, 2, len(initialConds), "Should have exactly 2 conditions: Health and CrdMisconfigured")

			infra.Fakes.CccClient.ClearActions()

			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 3*time.Minute)

			for _, action := range infra.Fakes.CccClient.Actions() {
				assert.NotEqual(t, "update", action.GetVerb(), "Expected no update actions for spurious update prevention")
				assert.NotEqual(t, "patch", action.GetVerb(), "Expected no patch actions for spurious update prevention")
			}

			updatedCCC, err = infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, "dedup-ccc", metav1.GetOptions{})
			assert.NoError(t, err)
			finalConds := updatedCCC.Status.Conditions
			assert.Equal(t, 2, len(finalConds), "Conditions must remain fully deduplicated with no duplicated error entries")
		})
	})
}
