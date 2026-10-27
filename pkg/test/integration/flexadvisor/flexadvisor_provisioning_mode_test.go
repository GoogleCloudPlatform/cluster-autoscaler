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

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	provisioningModeTestCcc = "test-ccc"
	provisioningModeTestPod = "test-pod"
)

// stockedOut returns a Flex Advisor guidance reporting zero capacity for every instance
// configuration of the given provisioning mode (any machine type, any zone).
func stockedOut(mode instanceavailability.ProvisioningMode) fake.CapacityGuidance {
	return fake.CapacityGuidance{ProvisioningMode: &mode, InstanceCount: 0, GcePreferenceScore: fake.DefaultZonalScore}
}

// withSpotToleration tolerates the taint of Spot node pools, like the GKE webhook does for Spot pods.
func withSpotToleration() func(*apiv1.Pod) {
	return pod.WithTolerationValue(gkelabels.SpotLabel, apiv1.TolerationOpEqual, gkelabels.PreemptionValue, apiv1.TaintEffectNoSchedule)
}

// capacityConditionsOfPriority returns the capacity (CapacityConstrained) conditions reported on the
// given priority of the ComputeClass, without their transition timestamps; nil if there are none.
func capacityConditionsOfPriority(cc *v1.ComputeClass, priorityIndex int) []metav1.Condition {
	if len(cc.Status.PriorityStatuses) <= priorityIndex {
		return nil
	}
	var conditions []metav1.Condition
	for _, condition := range cc.Status.PriorityStatuses[priorityIndex].Conditions {
		if condition.Reason != "CapacityConstrained" {
			continue
		}
		condition.LastTransitionTime = metav1.Time{}
		conditions = append(conditions, condition)
	}
	return conditions
}

// TestFlexAdvisorRuleProvisioningModeFollowsSpotProperty verifies that a ComputeClass priority rule is
// provisioned exclusively from the capacity of the provisioning mode it asks for: On-Demand when `spot`
// is omitted or false, Spot when `spot` is true. Flex Advisor capacity of the other provisioning mode is
// irrelevant to the rule: it is neither used to provision nodes nor reflected in the ComputeClass status.
//
// Each scenario stocks out one provisioning mode of the whole fleet while leaving the other available:
//   - the rule's mode is available: a NAP node pool of that mode is created and the pod is scheduled;
//     the priority reports no capacity condition, even though the other mode is stocked out,
//   - the rule's mode is stocked out: nothing is provisioned and the pod stays pending; the priority
//     reports ProvisioningSuspended for its single configuration (0/1 available), even though the
//     other mode has plenty of capacity.
//
// With the FlexAdvisorSkipUnneededSpotConfigs kill-switch disabled, rules that omit `spot` are evaluated
// against Spot capacity as well (legacy behavior): the scale-up decisions do not change, but the stockout
// of either mode surfaces as a misleading ProvisioningConstrained condition (1/2 configurations excluded).
func TestFlexAdvisorRuleProvisioningModeFollowsSpotProperty(t *testing.T) {
	ruleModeStockout := ccc.NewConditionBuilder("ProvisioningSuspended").
		WithReason("CapacityConstrained").
		WithMessage("No matching configuration is available (0/1 available) due to active capacity cooldowns across requested zones.").
		Build()
	legacyPartialStockout := ccc.NewConditionBuilder("ProvisioningConstrained").
		WithReason("CapacityConstrained").
		WithMessage("A subset (1/2) of configurations are excluded due to active capacity cooldowns across requested zones.").
		Build()

	testCases := map[string]struct {
		spot               *bool
		stockedOutMode     instanceavailability.ProvisioningMode
		experimentDisabled bool
		// expectedNodePoolSpot is the Spot setting of the NAP node pool expected to be created for the pod;
		// nil means that nothing is expected to be provisioned and the pod is expected to stay pending.
		expectedNodePoolSpot *bool
		expectedCondition    *metav1.Condition
	}{
		"spot omitted, On-Demand stocked out - nothing is provisioned": {
			spot:                 nil,
			stockedOutMode:       instanceavailability.Standard,
			expectedNodePoolSpot: nil,
			expectedCondition:    &ruleModeStockout,
		},
		"spot false, On-Demand stocked out - nothing is provisioned": {
			spot:                 new(false),
			stockedOutMode:       instanceavailability.Standard,
			expectedNodePoolSpot: nil,
			expectedCondition:    &ruleModeStockout,
		},
		"spot omitted, Spot stocked out - On-Demand node pool is provisioned": {
			spot:                 nil,
			stockedOutMode:       instanceavailability.Spot,
			expectedNodePoolSpot: new(false),
			expectedCondition:    nil,
		},
		"spot true, On-Demand stocked out - Spot node pool is provisioned": {
			spot:                 new(true),
			stockedOutMode:       instanceavailability.Standard,
			expectedNodePoolSpot: new(true),
			expectedCondition:    nil,
		},
		"spot true, Spot stocked out - nothing is provisioned": {
			spot:                 new(true),
			stockedOutMode:       instanceavailability.Spot,
			expectedNodePoolSpot: nil,
			expectedCondition:    &ruleModeStockout,
		},
		"experiment disabled, spot omitted, On-Demand stocked out - nothing is provisioned, partial stockout reported": {
			spot:                 nil,
			stockedOutMode:       instanceavailability.Standard,
			experimentDisabled:   true,
			expectedNodePoolSpot: nil,
			expectedCondition:    &legacyPartialStockout,
		},
		"experiment disabled, spot omitted, Spot stocked out - On-Demand node pool is provisioned, partial stockout reported": {
			spot:                 nil,
			stockedOutMode:       instanceavailability.Spot,
			experimentDisabled:   true,
			expectedNodePoolSpot: new(false),
			expectedCondition:    &legacyPartialStockout,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cccCrd := ccc.NewComputeClassBuilder(provisioningModeTestCcc).
				WithNodePoolAutoCreation(true).
				WithPriorities(v1.Priority{
					MachineType: new("n2-standard-4"),
					Spot:        tc.spot,
				}).
				Build()

			testConfig := integration.NewTestConfig().
				WithCccCrds(cccCrd).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithAutoprovisioningLocations(ZoneB),
				).
				WithOverrides(
					integration.WithMaxMemoryTotal(140*1024*1024*1024),
					integration.WithAutoProvisioningEnabled(),
					integration.WithFlexAdvisorEnabled(),
					integration.WithEnhancedCrdStatusReportingEnabled(),
				)
			if tc.experimentDisabled {
				testConfig = testConfig.WithExperimentOverrides(map[string]bool{
					experiments.FlexAdvisorSkipUnneededSpotConfigsEnabledFlag: false,
				}, nil)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(stockedOut(tc.stockedOutMode))

				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)
				defer integration_synctest.TearDown(cancel)

				PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, provisioningModeTestCcc)

				// The pod tolerates Spot nodes, so that it could be scheduled on a Spot node pool if CA
				// (wrongly) provisioned one for a rule that does not ask for Spot.
				testPod := tu.BuildTestPod(provisioningModeTestPod, 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC(provisioningModeTestCcc), withSpotToleration())
				infra.Fakes.K8s.AddPod(testPod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods(testPod.Namespace).Get(ctx, testPod.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				napNodePools := infra.Fakes.GkeService.GetAutoprovisionedNodePools()
				if tc.expectedNodePoolSpot == nil {
					assert.Empty(t, napNodePools, "Expected no NAP node pool to be created")
					assert.Empty(t, updatedPod.Spec.NodeName, "Expected %s to stay pending", testPod.Name)
				} else if assert.Len(t, napNodePools, 1, "Expected exactly one NAP node pool to be created") {
					assert.Equal(t, "n2-standard-4", napNodePools[0].Config.MachineType)
					assert.Equal(t, *tc.expectedNodePoolSpot, napNodePools[0].Config.Spot, "Unexpected Spot setting of the NAP node pool")
					assert.Contains(t, updatedPod.Spec.NodeName, napNodePools[0].Name, "Expected %s to be scheduled on the NAP node pool", testPod.Name)
				}

				// Advance virtual time to let the background status aggregator flush the ComputeClass status.
				time.Sleep(ccc.ConditionsFlushInterval)

				got, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, provisioningModeTestCcc, metav1.GetOptions{})
				assert.NoError(t, err)
				var expectedCapacityConditions []metav1.Condition
				if tc.expectedCondition != nil {
					expectedCapacityConditions = []metav1.Condition{*tc.expectedCondition}
				} else {
					ccc.AssertPriorityCondition(t, got, 0, "NodeProvisioningInProgress", "PodPending")
				}
				assert.Equal(t, expectedCapacityConditions, capacityConditionsOfPriority(got, 0), "Unexpected capacity conditions of priority 0")
			})
		})
	}
}

// TestFlexAdvisorAutopilotPayPerPodScopesFollowGuidanceOfBothProvisioningModes verifies that the synthetic
// flexibility scopes of Autopilot pay-per-pod workloads (Predefined Compute Classes and podFamily scopes),
// which are shared by On-Demand and Spot pods, follow the Flex Advisor guidance of both provisioning modes.
//
// Flex Advisor scores the 4-vCPU standard shape highest among On-Demand configurations and the 8-vCPU
// standard shape highest among Spot configurations, so with the fleet-efficiency allocation strategy:
//   - an On-Demand pod is provisioned on an On-Demand node pool of the 4-vCPU shape,
//   - a Spot pod is provisioned on a Spot node pool of the 8-vCPU shape (lowest-cost would pick the
//     4-vCPU shape, which is what a scope without Spot guidance would fall back to).
//
// The behavior is the same with the FlexAdvisorSkipUnneededSpotConfigs experiment enabled and disabled.
func TestFlexAdvisorAutopilotPayPerPodScopesFollowGuidanceOfBothProvisioningModes(t *testing.T) {
	scopes := map[string]struct {
		podOptions []func(*apiv1.Pod)
		// onDemandMachineType and spotMachineType are the machine types with the highest preference score
		// among On-Demand and Spot configurations respectively; otherMachineTypes are the remaining cheapest
		// candidates of the scope, scored low in both modes.
		onDemandMachineType string
		spotMachineType     string
		otherMachineTypes   []string
	}{
		"Balanced compute class": {
			podOptions:          []func(*apiv1.Pod){pod.WithCCC("Balanced")},
			onDemandMachineType: "n2-standard-4",
			spotMachineType:     "n2-standard-8",
			otherMachineTypes:   []string{"n2d-standard-4"},
		},
		"general-purpose pod family": {
			onDemandMachineType: "e2-standard-4",
			spotMachineType:     "e2-standard-8",
		},
	}
	workloads := map[string]instanceavailability.ProvisioningMode{
		"On-Demand pod": instanceavailability.Standard,
		"Spot pod":      instanceavailability.Spot,
	}
	experimentModes := map[string]bool{
		"experiment enabled":  true,
		"experiment disabled": false,
	}

	for scopeName, scope := range scopes {
		for workloadName, podMode := range workloads {
			for experimentName, experimentEnabled := range experimentModes {
				t.Run(scopeName+", "+workloadName+", "+experimentName, func(t *testing.T) {
					boolFlags, stringFlags := payPerPodExperimentFlags(payPerPodExperimentEnabled)
					boolFlags[experiments.FlexAdvisorSkipUnneededSpotConfigsEnabledFlag] = experimentEnabled
					testConfig := integration.NewTestConfig().
						WithExperimentOverrides(boolFlags, stringFlags).
						WithOverrides(
							integration.WithMaxMemoryTotal(140*1024*1024*1024),
							integration.WithFlexAdvisorEnabled(),
							integration.WithAutoProvisioningEnabled(),
							withClassicMachineFamilies(),
							withAutopilotCluster(),
						).
						WithClusterOverrides(
							integration.WithClusterAutoProvisioningEnabled(),
							integration.WithAutoprovisioningLocations(ZoneB),
						)

					guidances := []fake.CapacityGuidance{
						fake.NewGuidance(scope.onDemandMachineType).WithProvisioningMode(instanceavailability.Standard).WithScore(highPreferenceScore),
						fake.NewGuidance(scope.onDemandMachineType).WithProvisioningMode(instanceavailability.Spot).WithScore(lowPreferenceScore),
						fake.NewGuidance(scope.spotMachineType).WithProvisioningMode(instanceavailability.Standard).WithScore(lowPreferenceScore),
						fake.NewGuidance(scope.spotMachineType).WithProvisioningMode(instanceavailability.Spot).WithScore(highPreferenceScore),
					}
					for _, machineType := range scope.otherMachineTypes {
						guidances = append(guidances, fake.NewGuidance(machineType).WithScore(lowPreferenceScore))
					}

					expectedMachineType := scope.onDemandMachineType
					podOptions := scope.podOptions
					if podMode == instanceavailability.Spot {
						expectedMachineType = scope.spotMachineType
						podOptions = append(podOptions, pod.WithNodeSelectorEntry(gkelabels.SpotLabel, gkelabels.PreemptionValue), withSpotToleration())
					}

					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(t.Context())
						infra := integration.SetupInfrastructure(ctx, t)
						infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(guidances...)

						autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)
						defer integration_synctest.TearDown(cancel)

						PrimeFlexAdvisorCacheForPod(ctx, t, autoscaler, infra, scope.podOptions...)

						machineType := runPayPerPodScaleUp(ctx, t, autoscaler, infra, buildPayPerPodTestPod(podOptions...))
						assert.Equal(t, expectedMachineType, machineType, "Expected the pod to be provisioned on the machine type with the highest %s preference score", podMode)
						nodePool := infra.Fakes.GkeService.MustGetSingleAutoprovisionedNodePool(t)
						assert.Equal(t, podMode == instanceavailability.Spot, nodePool.Config.Spot, "Unexpected Spot setting of the NAP node pool")
					})
				})
			}
		}
	}
}
