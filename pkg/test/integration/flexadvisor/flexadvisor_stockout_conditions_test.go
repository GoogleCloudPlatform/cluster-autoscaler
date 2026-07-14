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

package flexadvisor

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	ccc_api "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
)

const (
	// conditionsFlushInterval is set slightly higher than the status Aggregator's BatchFlushInterval
	// to ensure virtual time advances enough for the background status aggregator to flush dirty status updates.
	conditionsFlushInterval = status.BatchFlushInterval + 10*time.Second
)

// TestStockOutConditionsEmitted verifies that when GCE Flex Advisor reports capacity stockouts or
// partial capacity exclusions for a ComputeClass (CCC) priority rule, Cluster Autoscaler (CA)
// correctly generates and emits the corresponding status conditions on the ComputeClass resource.
//
// Specifically, this test evaluates three scenarios:
//  1. MachineType priority (Total Stockout): A priority defined by MachineType where all candidate
//     instance configurations (e.g., across multiple zones or provisioning modes) have 0 capacity.
//     Expects a "ProvisioningSuspended" condition with reason "CapacityConstrained".
//  2. NodePools priority (Total Stockout): A priority defined by an explicit node pool list where the
//     node pool's configuration has 0 capacity. Expects "ProvisioningSuspended".
//  3. Partially filtered priority (Partial Stockout): A priority where only a subset of candidate
//     instance configurations are filtered out by active cooldowns (e.g., Spot is out of stock, but
//     Standard/On-Demand remains available). Expects a "ProvisioningConstrained" condition explaining
//     that a subset (1/2) of configurations are excluded.
//
// Test Workflow:
//   - Initializes a ComputeClass CRD and an empty node pool matching the priority rule.
//   - Configures the test environment with Node Auto-Provisioning (NAP) locations (required so CA can
//     evaluate zone availability for MachineType rules without explicit node pools), Flex Advisor, and
//     enhanced CRD status reporting enabled.
//   - Injects mock Flex Advisor capacity guidance simulating stockout or partial exclusion.
//   - Runs two autoscaler cycles: Loop 1 registers the scope and initiates background cache fetching;
//     Loop 2 evaluates against the populated cache and generates the condition.
//   - Advances virtual time to trigger the background status aggregator flush, and asserts that the
//     ComputeClass status in the API server matches the expected condition.
func TestStockOutConditionsEmitted(t *testing.T) {
	testCases := []struct {
		name            string
		cccName         string
		priority        ccc_api.Priority
		guidance        fake.CapacityGuidance
		expectedType    string
		expectedMessage string
	}{
		{
			name:    "MachineType priority",
			cccName: "test-ccc",
			priority: ccc_api.Priority{
				MachineType: ptr.To("n2-standard-4"),
			},
			guidance:        fake.NewGuidance("n2-standard-4").WithCapacity(0),
			expectedType:    "ProvisioningSuspended",
			expectedMessage: "No matching configuration is available (0/2 available) due to active capacity cooldowns across requested zones.",
		},
		{
			name:    "NodePools priority",
			cccName: "test-ccc-np",
			priority: ccc_api.Priority{
				Nodepools: []string{"pool-1"},
			},
			guidance:        fake.NewGuidance("n2-standard-4").WithCapacity(0),
			expectedType:    "ProvisioningSuspended",
			expectedMessage: "No matching configuration is available (0/1 available) due to active capacity cooldowns across requested zones.",
		},
		{
			name:    "Partially filtered priority",
			cccName: "test-ccc-partial",
			priority: ccc_api.Priority{
				MachineType: ptr.To("n2-standard-4"),
			},
			guidance: fake.CapacityGuidance{
				MachineType:        ptr.To("n2-standard-4"),
				ProvisioningMode:   ptr.To(instanceavailability.Spot),
				InstanceCount:      0,
				GcePreferenceScore: 0.5,
			},
			expectedType:    "ProvisioningConstrained",
			expectedMessage: "A subset (1/2) of configurations are excluded due to active capacity cooldowns across requested zones.",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cccObj := ccc.NewComputeClassBuilder(tc.cccName).
				WithLabel(gkelabels.ComputeClassLabel, tc.cccName).
				WithPriorities(tc.priority).
				Build()

			nodePools := []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-1").
					WithMachineType("n2-standard-4").
					WithCCCLabel(tc.cccName).
					WithCCCTaint(tc.cccName).
					Build(),
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithClusterOverrides(
					// AutoprovisioningLocations are required so that rules specifying MachineType without
					// explicit node pools can discover available zones from the cluster configuration.
					integration.WithAutoprovisioningLocations("us-central1-b"),
					integration.WithClusterAutoProvisioningEnabled(),
				).
				WithOverrides(
					integration.WithFlexAdvisorEnabled(),
					integration.WithEnhancedCrdStatusReportingEnabled(),
					integration.WithAutoProvisioningEnabled(),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(tc.guidance)

				_, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Create(ctx, cccObj, metav1.CreateOptions{})
				assert.NoError(t, err)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Add an unschedulable pending pod that selects this ComputeClass to trigger scale-up evaluation.
				pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithNodeSelector(map[string]string{
					gkelabels.ComputeClassLabel: tc.cccName,
				}))
				infra.Fakes.K8s.AddPod(pod)

				// Loop 1: Register the ComputeClass scope and initiate background cache fetching from Flex Advisor.
				// Because cache fetching occurs asynchronously, stockout conditions are not emitted in the first pass.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Loop 2: Re-run autoscaler cycle. Now that the Flex Advisor cache is populated with stockout guidance,
				// CA detects the capacity constraint and queues the corresponding status condition in the aggregator.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Empty(t, updatedPod.Spec.NodeName, "Expected standard-pod to remain unschedulable")

				// Advance virtual time by conditionsFlushInterval (40s) to trigger the background status aggregator
				// to flush queued condition updates and patch the ComputeClass status in the API server.
				time.Sleep(conditionsFlushInterval)

				got, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, tc.cccName, metav1.GetOptions{})
				assert.NoError(t, err)

				expectedStatus := ccc.NewComputeClassStatusBuilder().
					WithPriorityStatuses(
						ccc.NewPriorityStatusBuilder("0").
							WithConditions(
								ccc.NewConditionBuilder(tc.expectedType).
									WithReason("CapacityConstrained").
									WithMessage(tc.expectedMessage).
									Build(),
							).
							Build(),
					).
					Build()

				ccc.AssertComputeClassConditions(t, expectedStatus, got.Status, "ComputeClassStatus mismatch")
			})
		})
	}
}

// TestStockOutConditionsClearedWhenHealthy verifies the recovery lifecycle of ComputeClass status conditions:
// when capacity is initially exhausted (stockout), a stockout condition ("ProvisioningSuspended") is emitted;
// when cloud provider capacity subsequently recovers and nodes begin provisioning, the stockout condition
// is cleared and replaced by an active scale-up progress condition ("NodeProvisioningInProgress").
//
// Test Workflow:
//   - Initializes a ComputeClass CRD ("test-ccc") targeting machine type "n2-standard-4" and a matching node pool.
//   - Configures the environment with Flex Advisor, Node Auto-Provisioning (NAP), and enhanced CRD status reporting.
//   - Phase 1 (Stockout):
//     1. Simulates a total zone stockout by configuring Flex Advisor mock guidance to return 0 available instances.
//     2. Adds an unschedulable pod selecting "test-ccc" to trigger scale-up evaluation.
//     3. Runs two autoscaler cycles (scope registration + cache evaluation) and flushes the status aggregator.
//     4. Asserts that the CRD status displays "ProvisioningSuspended" with reason "CapacityConstrained".
//   - Phase 2 (Recovery & Scale-Up):
//     1. Simulates capacity recovery in the cloud provider by updating Flex Advisor guidance to 1 available instance.
//     2. Advances virtual time by 10s (exceeding the Flex Advisor cache refresh interval) and executes an autoscaler
//     cycle. The background worker fetches the updated guidance, recognizes available capacity, and triggers node creation.
//     3. Flushes the status aggregator and asserts that "ProvisioningSuspended" is cleared, and is replaced by
//     "NodeProvisioningInProgress" (Reason: "PodPending") confirming healthy node provisioning.
func TestStockOutConditionsClearedWhenHealthy(t *testing.T) {
	cccObj := ccc.NewComputeClassBuilder("test-ccc").
		WithLabel(gkelabels.ComputeClassLabel, "test-ccc").
		WithPriorities(ccc_api.Priority{
			MachineType: ptr.To("n2-standard-4"),
		}).
		Build()

	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-1").
			WithMachineType("n2-standard-4").
			WithCCCLabel("test-ccc").
			WithCCCTaint("test-ccc").
			Build(),
	}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithClusterOverrides(
			integration.WithAutoprovisioningLocations("us-central1-b"),
			integration.WithClusterAutoProvisioningEnabled(),
		).
		WithOverrides(
			integration.WithFlexAdvisorEnabled(),
			integration.WithEnhancedCrdStatusReportingEnabled(),
			integration.WithAutoProvisioningEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		_, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Create(ctx, cccObj, metav1.CreateOptions{})
		assert.NoError(t, err)

		// --- PHASE 1: Simulating Total Capacity Stockout ---
		// Configure Flex Advisor to return 0 available instances for n2-standard-4.
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(fake.NewGuidance("n2-standard-4").WithCapacity(0))

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithNodeSelector(map[string]string{
			gkelabels.ComputeClassLabel: "test-ccc",
		}))
		infra.Fakes.K8s.AddPod(pod)

		// Loop 1: Register the ComputeClass scope and initiate background cache fetching from Flex Advisor.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Loop 2: Evaluate scale-up against the populated stockout cache. CA detects 0 capacity and queues
		// a "ProvisioningSuspended" stockout condition in the status aggregator.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Advance virtual time by conditionsFlushInterval (40s) so the background status aggregator flushes
		// queued condition updates to the Kubernetes API server.
		time.Sleep(conditionsFlushInterval)

		// Verify that Phase 1 captured the stockout condition in the CRD status patch.
		got, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, "test-ccc", metav1.GetOptions{})
		assert.NoError(t, err)

		expectedStatus := ccc.NewComputeClassStatusBuilder().
			WithPriorityStatuses(
				ccc.NewPriorityStatusBuilder("0").
					WithConditions(
						ccc.NewConditionBuilder("ProvisioningSuspended").
							WithReason("CapacityConstrained").
							WithMessage("No matching configuration is available (0/2 available) due to active capacity cooldowns across requested zones.").
							Build(),
					).
					Build(),
			).
			Build()

		ccc.AssertComputeClassConditions(t, expectedStatus, got.Status, "ComputeClassStatus mismatch in Phase 1 (Stockout)")

		// --- PHASE 2: Capacity Recovery and Active Node Provisioning ---
		// Simulate capacity recovery in the cloud provider by updating Flex Advisor guidance to 1 available instance.
		infra.Fakes.FlexAdvisorClient.ClearCapacityGuidances()
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n2-standard-4").WithCapacity(1),
		)

		// Loop 3: Advance virtual clock by 10s (exceeding the 10s Flex Advisor cache refresh interval) and run the autoscaler.
		// The background worker fetches the updated guidance, recognizes available capacity, and triggers scale-up (adding 1 node).
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		// Advance virtual time by conditionsFlushInterval (40s) so the aggregator flushes the updated progress conditions.
		time.Sleep(conditionsFlushInterval)

		// Verify that the previous "ProvisioningSuspended" stockout condition was cleared and replaced by
		// "NodeProvisioningInProgress" (Reason: "PodPending"), confirming healthy scale-up progress reporting.
		got, err = infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, "test-ccc", metav1.GetOptions{})
		assert.NoError(t, err)

		expectedStatusPhase2 := ccc.NewComputeClassStatusBuilder().
			WithPriorityStatuses(
				ccc.NewPriorityStatusBuilder("0").
					WithConditions(
						ccc.NewConditionBuilder("NodeProvisioningInProgress").
							WithReason("PodPending").
							WithMessage("NodeProvisioning associated with this priority triggered due to pending pods. 1 new nodes will be added with config: {NodePool: pool-1, MachineType: n2-standard-4, Zones: us-central1-b}").
							Build(),
					).
					Build(),
			).
			Build()

		ccc.AssertComputeClassConditions(t, expectedStatusPhase2, got.Status, "ComputeClassStatus mismatch in Phase 2 (Recovery)")
	})
}
