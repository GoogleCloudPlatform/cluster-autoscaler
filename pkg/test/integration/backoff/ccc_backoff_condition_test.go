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

package backoff_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	k8sapimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

const (
	conditionsFlushInterval = status.BatchFlushInterval + 10*time.Second
)

// TestNodeProvisioningCooldownConditions verifies that when a priority rule enters backoff
// (e.g., due to quota, stockout, or internal errors during scale up), CrdBackoffObserver sets
// the appropriate condition ("ProvisioningSuspended" or "ProvisioningConstrained") and translated
// reason on the ComputeClass CRD status. It also verifies that once the backoff duration expires,
// the condition is cleaned up.
func TestNodeProvisioningCooldownConditions(t *testing.T) {
	testCases := []struct {
		name              string
		errorCode         string
		wantConditionType string
		wantReason        string
		wantMessageSubstr string
	}{
		{
			name:              "QuotaExceeded triggers full cooldown (ProvisioningSuspended)",
			errorCode:         "QUOTA_EXCEEDED",
			wantConditionType: "ProvisioningSuspended",
			wantReason:        "QuotaExceeded",
			wantMessageSubstr: "NodeProvisioning associated with this priority failed due to the QuotaExceeded error",
		},
		{
			name:              "Unknown quota error triggers full cooldown with InternalError reason",
			errorCode:         "QUOTA_UNKNOWN_ERROR",
			wantConditionType: "ProvisioningSuspended",
			wantReason:        "InternalError",
			wantMessageSubstr: "NodeProvisioning associated with this priority failed due to the InternalError error",
		},
		{
			name:              "Resource pool exhausted triggers partial cooldown (ProvisioningConstrained) with OutOfResources reason",
			errorCode:         "ZONE_RESOURCE_POOL_EXHAUSTED",
			wantConditionType: "ProvisioningConstrained",
			wantReason:        "OutOfResources",
			wantMessageSubstr: "NodeProvisioning of the node pools associated with this priority failed due to the OutOfResources error",
		},
		{
			name:              "IP space exhausted triggers partial cooldown (ProvisioningConstrained) with IpSpaceExhausted reason",
			errorCode:         "IP_SPACE_EXHAUSTED",
			wantConditionType: "ProvisioningConstrained",
			wantReason:        "IpSpaceExhausted",
			wantMessageSubstr: "NodeProvisioning of the node pools associated with this priority failed due to the IpSpaceExhausted error",
		},
		{
			name:              "Reservation capacity exceeded triggers partial cooldown (ProvisioningConstrained) with ReservationCapacityExceeded reason",
			errorCode:         "RESERVATION_CAPACITY_EXCEEDED",
			wantConditionType: "ProvisioningConstrained",
			wantReason:        "ReservationCapacityExceeded",
			wantMessageSubstr: "NodeProvisioning of the node pools associated with this priority failed due to the ReservationCapacityExceeded error",
		},
		{
			name:              "Invalid reservation triggers partial cooldown (ProvisioningConstrained) with InvalidReservation reason",
			errorCode:         "INVALID_RESERVATION",
			wantConditionType: "ProvisioningConstrained",
			wantReason:        "InvalidReservation",
			wantMessageSubstr: "NodeProvisioning of the node pools associated with this priority failed due to the InvalidReservation error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cccName := "cooldown-condition-ccc"
			cc := ccc.NewComputeClassBuilder(cccName).
				WithNapEnabled().
				AddPriority(v1.Priority{
					MachineType: ptr.To("n1-standard-4"),
				}).
				AddPriority(v1.Priority{
					MachineType: ptr.To("n2-standard-4"),
				}).
				Build()

			np1 := integration.EmptyNodePool("pool-1").
				WithMachineType("n1-standard-4").
				WithCCCLabel(cccName).
				WithCCCTaint(cccName).
				Build()
			np1.Locations = []string{"us-central1-a", "us-central1-b"}

			np2 := integration.EmptyNodePool("pool-2").
				WithMachineType("n2-standard-4").
				WithCCCLabel(cccName).
				WithCCCTaint(cccName).
				Build()
			np2.Locations = []string{"us-central1-a", "us-central1-b"}

			nodePools := []*gke_api_beta.NodePool{np1, np2}

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithEnhancedCrdStatusReportingEnabled(),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithClusterZones("us-central1-a", "us-central1-b"),
				).
				WithRegionToZones(map[string][]string{"us-central1": {"us-central1-a", "us-central1-b"}}).
				WithCccCrds(cc)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)

				// Set error for zone us-central1-a
				infra.Fakes.GceService.SetCreateInstanceForZoneError("us-central1-a", cloudprovider.InstanceErrorInfo{
					ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
					ErrorCode:    tc.errorCode,
					ErrorMessage: "Simulated error in zone a",
				})

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)

				// Create a pod requesting the CCC and targeting us-central1-a.
				p1 := tu.BuildTestPod("pod-1", 3000, 4000, tu.MarkUnschedulable(), pod.WithCCC(cccName), pod.WithNodeSelectorEntry("topology.kubernetes.io/zone", "us-central1-a"))
				infra.Fakes.K8s.AddPod(p1)

				// Loop 1: Autoscaler attempts scale up for Priority 0 in us-central1-a and encounters error.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

				// Loop 2: Autoscaler attempts fallback or processes scale up error.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

				// Flush status aggregator
				time.Sleep(conditionsFlushInterval)

				// Verify ComputeClass status contains the expected condition for Priority 0.
				got, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
				assert.NoError(t, err)

				if assert.NotEmpty(t, got.Status.PriorityStatuses, "PriorityStatuses should not be empty") {
					cooldownCond := k8sapimeta.FindStatusCondition(got.Status.PriorityStatuses[0].Conditions, tc.wantConditionType)
					if assert.NotNil(t, cooldownCond, "Condition %s should be present on Priority 0", tc.wantConditionType) {
						assert.Equal(t, metav1.ConditionTrue, cooldownCond.Status)
						assert.Equal(t, tc.wantReason, cooldownCond.Reason)
						assert.Contains(t, cooldownCond.Message, tc.wantMessageSubstr)
					}
				}

				// Phase 2: Clear error, advance time past backoff duration (5 minutes), and run autoscaler loop again.
				infra.Fakes.GceService.SetCreateInstanceForZoneError("us-central1-a", cloudprovider.InstanceErrorInfo{})
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Minute+30*time.Second)

				time.Sleep(conditionsFlushInterval)

				gotAfter, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
				assert.NoError(t, err)

				if len(gotAfter.Status.PriorityStatuses) > 0 {
					cooldownCond := k8sapimeta.FindStatusCondition(gotAfter.Status.PriorityStatuses[0].Conditions, tc.wantConditionType)
					assert.Nil(t, cooldownCond, "Condition %s should be cleared after backoff expires", tc.wantConditionType)
				}
			})
		})
	}
}
