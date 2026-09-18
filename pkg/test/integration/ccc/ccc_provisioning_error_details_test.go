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

package ccc_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/history"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	testccc "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestCCCProvisioningErrorDetails(t *testing.T) {
	statusMessage := "Quota 'CPUS' exceeded. Limit: 24.0 in region us-central1."

	testCases := []struct {
		name              string
		experimentEnabled bool
		wantConditionType string
		wantContains      string
		wantNotContains   string
	}{
		{
			// A quota failure on the MIG is a stockout, so the whole priority gets
			// backed off (full cooldown).
			name:              "ExperimentEnabled_ZoneError",
			experimentEnabled: true,
			wantConditionType: history.ConditionTypeNodeProvisioningInCooldown,
			wantContains:      statusMessage,
			wantNotContains:   "",
		},
		{
			name:              "ExperimentDisabled_ZoneError",
			experimentEnabled: false,
			wantConditionType: history.ConditionTypeNodeProvisioningInCooldown,
			wantContains:      "",
			wantNotContains:   statusMessage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crdName := "default"
			crdObj := testccc.NewComputeClassBuilder(crdName).
				WithWhenUnsatisfiable("ScaleUpAnyway").
				WithPriorities(v1.Priority{
					MachineFamily: ptr.To("n1"),
					MinCores:      ptr.To(2),
					MinMemoryGb:   ptr.To(4),
				}).
				Build()

			np := integration.EmptyNodePool("default-pool").
				WithMachineType("n1-standard-2").
				WithCCCLabel(crdName).
				WithSize(0).
				WithMin(0).
				WithMax(10).
				Build()

			testConfig := integration.NewTestConfig().
				WithNodePools(np).
				WithCccCrds(crdObj).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithClusterDefaultComputeClassEnabled(),
				).
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithCccNodeAutoprovisioningEnabled(),
					integration.WithEnhancedCrdStatusReporting(true),
				)

			if tc.experimentEnabled {
				testConfig = testConfig.WithOverrides(integration.WithProvisioningErrorDetailsEnabled(true))
			} else {
				testConfig = testConfig.WithOverrides(integration.WithProvisioningErrorDetailsEnabled(false))
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				// Force instance creation to fail with a stockout error for the MIG
				infra.Fakes.GceService.SetCreateInstanceForMigError("default-pool", cloudprovider.InstanceErrorInfo{
					ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
					ErrorCode:    "QUOTA_EXCEEDED",
					ErrorMessage: statusMessage,
				})

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				pod := tu.BuildTestPod("my-pod", 1000, 1000, tu.MarkUnschedulable())
				pod.Spec.NodeSelector = map[string]string{
					"cloud.google.com/compute-class": "default",
				}
				pod.Spec.Tolerations = []corev1.Toleration{
					{
						Key:      "cloud.google.com/compute-class",
						Operator: corev1.TolerationOpEqual,
						Value:    "default",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				}
				infra.Fakes.K8s.AddPod(pod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)

				updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, crdName, metav1.GetOptions{})
				assert.NoError(t, err)

				var suspendCond *metav1.Condition
				for _, ps := range updatedCCC.Status.PriorityStatuses {
					cond := k8sapimeta.FindStatusCondition(ps.Conditions, tc.wantConditionType)
					if cond != nil {
						suspendCond = cond
						break
					}
				}

				assert.NotNil(t, suspendCond, "Expected condition %q not found", tc.wantConditionType)

				if suspendCond != nil {
					if tc.wantContains != "" {
						assert.Contains(t, suspendCond.Message, tc.wantContains)
					}
					if tc.wantNotContains != "" {
						assert.NotContains(t, suspendCond.Message, tc.wantNotContains)
					}
				}
			})
		})
	}
}
