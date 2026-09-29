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

package vizlogs

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_flexadvisor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/flexadvisor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestNoScaleUpVisibilityLogs_EmittedReasons(t *testing.T) {
	testCases := []struct {
		name                              string
		ccc                               *v1.ComputeClass
		nodePools                         []*gke_api_beta.NodePool
		guidances                         []fake.CapacityGuidance
		disableClusterNap                 bool
		podCpu                            int64
		podCount                          int
		expectedScaleUpNodes              int
		expectedNapFailureReason          *vispb.ParametrizedMessage
		expectedPodGroupNapFailureReasons []*vispb.ParametrizedMessage
		expectedRejectedMigs              map[string]*vispb.ParametrizedMessage
		expectedSkippedMigs               map[string]*vispb.ParametrizedMessage
	}{
		{
			name: "NAP: one option too small and another has no capacity in FA",
			ccc: ccc.NewComputeClassBuilder("my-ccc").WithNapEnabled().
				WithPriorities(
					v1.Priority{
						MachineType: new("n1-standard-8"),
					},
					v1.Priority{
						MachineType: new("n1-standard-2"),
					},
				).Build(),
			nodePools: []*gke_api_beta.NodePool{},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-8").WithCapacity(0),
			},
			podCpu: 3000,
			expectedNapFailureReason: &vispb.ParametrizedMessage{
				MessageId:  "no.scale.up.nap.capacity.constraints",
				Parameters: []string{"my-ccc"},
			},
			expectedPodGroupNapFailureReasons: []*vispb.ParametrizedMessage{
				{
					MessageId:  "no.scale.up.nap.pod.zonal.capacity.constraints",
					Parameters: []string{"us-central1-b"},
				},
			},
		},
		{
			name: "NAP: single option too small, FA not involved",
			ccc: ccc.NewComputeClassBuilder("my-ccc").WithNapEnabled().
				WithPriorities(
					v1.Priority{
						MachineType: new("n1-standard-2"),
					},
				).Build(),
			nodePools: []*gke_api_beta.NodePool{},
			guidances: []fake.CapacityGuidance{},
			podCpu:    3000,
			expectedPodGroupNapFailureReasons: []*vispb.ParametrizedMessage{
				{
					MessageId:  "no.scale.up.nap.pod.zonal.resources.exceeded",
					Parameters: []string{"us-central1-b"},
				},
			},
		},
		{
			name: "NAP: single option both too small and has no capacity in FA -> emits pod zonal resources exceeded",
			ccc: ccc.NewComputeClassBuilder("my-ccc").WithNapEnabled().
				WithPriorities(
					v1.Priority{
						MachineType: new("n1-standard-2"),
					},
				).Build(),
			nodePools: []*gke_api_beta.NodePool{},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-2").WithCapacity(0),
			},
			podCpu: 3000,
			expectedPodGroupNapFailureReasons: []*vispb.ParametrizedMessage{
				{
					MessageId:  "no.scale.up.nap.pod.zonal.resources.exceeded",
					Parameters: []string{"us-central1-b"},
				},
			},
		},
		{
			name: "NAP: one option too small and another missing reservation -> emits pod zonal failing predicates and no scale-up options available",
			ccc: ccc.NewComputeClassBuilder("my-ccc").WithNapEnabled().
				WithPriorities(
					v1.Priority{
						MachineType: new("n1-standard-2"),
					},
					v1.Priority{
						MachineFamily: new("n1"),
						Reservations: &v1.Reservations{
							Affinity: v1.AnyThenFail,
						},
					},
				).Build(),
			nodePools: []*gke_api_beta.NodePool{},
			guidances: []fake.CapacityGuidance{},
			podCpu:    3000,
			// TODO(b/562409581): Currently this emits generic "no.scale.up.nap.pod.zonal.failing.predicates" along the "no scale-up options available"; it should be fixed to emit that the reservation was not matched.
			expectedPodGroupNapFailureReasons: []*vispb.ParametrizedMessage{
				{
					MessageId:  "no.scale.up.nap.pod.zonal.failing.predicates",
					Parameters: []string{"us-central1-b", "no scale-up options available"},
				},
			},
		},
		{
			name: "NAP: some options too small, some with no capacity in FA, another matches -> scale up succeeds and emits no NoScaleUp event",
			ccc: ccc.NewComputeClassBuilder("my-ccc").WithNapEnabled().
				WithPriorities(
					v1.Priority{
						MachineType: new("n1-standard-2"),
					},
					v1.Priority{
						MachineType: new("n1-standard-8"),
					},
					v1.Priority{
						MachineType: new("n1-standard-4"),
					},
				).Build(),
			nodePools: []*gke_api_beta.NodePool{},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-8").WithCapacity(0),
				fake.NewGuidance("n1-standard-4").WithCapacity(10),
			},
			podCpu:               3000,
			expectedScaleUpNodes: 1,
		},
		{
			name: "Node pools: one too small and one at max size -> emits mig failing predicate and mig skipped max size",
			ccc:  ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-2", "pool-4").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-2").WithMachineType("n1-standard-2").WithCCCLabel("my-ccc").Build(),
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").WithMax(0).Build(),
			},
			guidances: []fake.CapacityGuidance{},
			podCpu:    3000,
			expectedRejectedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-2": {
					MessageId:  "no.scale.up.mig.failing.predicate",
					Parameters: []string{"NodeResourcesFit", "Insufficient cpu"},
				},
			},
			expectedSkippedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-4": {
					MessageId:  "no.scale.up.mig.skipped",
					Parameters: []string{"max node group size reached"},
				},
			},
		},
		{
			name: "Node pools: one too small and one at max size, both with no capacity in FA -> emits mig failing predicate and mig skipped max size",
			ccc:  ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-2", "pool-4").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-2").WithMachineType("n1-standard-2").WithCCCLabel("my-ccc").Build(),
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").WithMax(0).Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-2").WithCapacity(0),
				fake.NewGuidance("n1-standard-4").WithCapacity(0),
			},
			podCpu: 3000,
			expectedRejectedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-2": {
					MessageId:  "no.scale.up.mig.failing.predicate",
					Parameters: []string{"NodeResourcesFit", "Insufficient cpu"},
				},
			},
			expectedSkippedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-4": {
					MessageId:  "no.scale.up.mig.skipped",
					Parameters: []string{"max node group size reached"},
				},
			},
		},
		{
			name: "Node pools: one too small, one at max size, and one with no capacity in FA",
			ccc:  ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-2", "pool-4", "pool-8").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-2").WithMachineType("n1-standard-2").WithCCCLabel("my-ccc").Build(),
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").WithMax(0).Build(),
				integration.EmptyNodePool("pool-8").WithMachineType("n1-standard-8").WithCCCLabel("my-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-8").WithCapacity(0),
			},
			podCpu: 3000,
			expectedNapFailureReason: &vispb.ParametrizedMessage{
				MessageId:  "no.scale.up.nap.capacity.constraints",
				Parameters: []string{"my-ccc"},
			},
			expectedRejectedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-8": {
					MessageId: "no.scale.up.mig.capacity.constraints",
				},
				"pool-2": {
					MessageId:  "no.scale.up.mig.failing.predicate",
					Parameters: []string{"NodeResourcesFit", "Insufficient cpu"},
				},
			},
			expectedSkippedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-4": {
					MessageId:  "no.scale.up.mig.skipped",
					Parameters: []string{"max node group size reached"},
				},
			},
		},
		{
			name: "Node pools: all matching pod have no capacity in FA -> emits nap capacity constraints and all migs rejected due to capacity constraints",
			ccc:  ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-4", "pool-8").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").Build(),
				integration.EmptyNodePool("pool-8").WithMachineType("n1-standard-8").WithCCCLabel("my-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-4").WithCapacity(0),
				fake.NewGuidance("n1-standard-8").WithCapacity(0),
			},
			podCpu: 3000,
			expectedNapFailureReason: &vispb.ParametrizedMessage{
				MessageId:  "no.scale.up.nap.capacity.constraints",
				Parameters: []string{"my-ccc"},
			},
			expectedRejectedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-4": {
					MessageId: "no.scale.up.mig.capacity.constraints",
				},
				"pool-8": {
					MessageId: "no.scale.up.mig.capacity.constraints",
				},
			},
		},
		{
			name: "Node pools: only pool requires unfulfilled reservation -> mig rejected with unknown reason",
			ccc:  ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-4").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").
					WithLabel("cloud.google.com/reservation-affinity", "any-reservation-then-fail").
					Build(),
			},
			guidances: []fake.CapacityGuidance{},
			podCpu:    3000,
			expectedRejectedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-4": {
					MessageId: "no.scale.up.mig.unknown.reason",
				},
			},
		},
		{
			name: "Node pools: one too small, one removed by FA, another matches -> scale up succeeds and emits no NoScaleUp event",
			ccc:  ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-2", "pool-8", "pool-4").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-2").WithMachineType("n1-standard-2").WithCCCLabel("my-ccc").Build(),
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").Build(),
				integration.EmptyNodePool("pool-8").WithMachineType("n1-standard-8").WithCCCLabel("my-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-4").WithCapacity(10),
				fake.NewGuidance("n1-standard-8").WithCapacity(0),
			},
			podCpu:               3000,
			expectedScaleUpNodes: 1,
		},
		{
			name: "Node pools: 10 nodes needed but FA limits capacity to 5 -> partial scale up of 5 nodes succeeds and emits no NoScaleUp event",
			ccc:  ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-4").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-4").WithCapacity(5),
			},
			podCpu:               3000,
			podCount:             10,
			expectedScaleUpNodes: 5,
		},
		{
			name: "NAP: all options removed by FA - emits nap capacity constraints and pod zonal capacity constraints",
			ccc: ccc.NewComputeClassBuilder("my-ccc").WithNapEnabled().
				WithPriorities(
					v1.Priority{
						MachineType: new("n1-standard-8"),
					},
				).Build(),
			nodePools: []*gke_api_beta.NodePool{},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-8").WithCapacity(0),
			},
			podCpu: 1000,
			expectedNapFailureReason: &vispb.ParametrizedMessage{
				MessageId:  "no.scale.up.nap.capacity.constraints",
				Parameters: []string{"my-ccc"},
			},
			expectedPodGroupNapFailureReasons: []*vispb.ParametrizedMessage{
				{
					MessageId:  "no.scale.up.nap.pod.zonal.capacity.constraints",
					Parameters: []string{"us-central1-b"},
				},
			},
		},
		{
			name: "NAP: option removed by FA and option too small - emits nap and pod zonal capacity constraints",
			ccc: ccc.NewComputeClassBuilder("my-ccc").WithNapEnabled().
				WithPriorities(
					v1.Priority{
						MachineType: new("n1-standard-8"),
					},
					v1.Priority{
						MachineType: new("n1-standard-2"),
					},
				).Build(),
			nodePools: []*gke_api_beta.NodePool{},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-8").WithCapacity(0),
				fake.NewGuidance("n1-standard-2").WithCapacity(0),
			},
			podCpu: 3000,
			expectedNapFailureReason: &vispb.ParametrizedMessage{
				MessageId:  "no.scale.up.nap.capacity.constraints",
				Parameters: []string{"my-ccc"},
			},
			expectedPodGroupNapFailureReasons: []*vispb.ParametrizedMessage{
				{
					MessageId:  "no.scale.up.nap.pod.zonal.capacity.constraints",
					Parameters: []string{"us-central1-b"},
				},
			},
		},
		{
			name: "Node pools: max size 0, insufficient cpu and removed by FA migs - reports all reasons",
			ccc:  ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-2", "pool-4", "pool-8").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-2").WithMachineType("n1-standard-2").WithCCCLabel("my-ccc").Build(),
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").WithMax(0).Build(),
				integration.EmptyNodePool("pool-8").WithMachineType("n1-standard-8").WithCCCLabel("my-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-2").WithCapacity(0),
				fake.NewGuidance("n1-standard-4").WithCapacity(0),
				fake.NewGuidance("n1-standard-8").WithCapacity(0),
			},
			podCpu: 3000,
			expectedNapFailureReason: &vispb.ParametrizedMessage{
				MessageId:  "no.scale.up.nap.capacity.constraints",
				Parameters: []string{"my-ccc"},
			},
			expectedRejectedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-2": {
					MessageId:  "no.scale.up.mig.failing.predicate",
					Parameters: []string{"NodeResourcesFit", "Insufficient cpu"},
				},
				"pool-8": {
					MessageId: "no.scale.up.mig.capacity.constraints",
				},
			},
			expectedSkippedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-4": {
					MessageId:  "no.scale.up.mig.skipped",
					Parameters: []string{"max node group size reached"},
				},
			},
		},
		{
			name: "Node pools: NAP disabled, migs removed by FA - FA reasons are still reported",
			ccc:  ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-4", "pool-8").Build(),
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").Build(),
				integration.EmptyNodePool("pool-8").WithMachineType("n1-standard-8").WithCCCLabel("my-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-4").WithCapacity(0),
				fake.NewGuidance("n1-standard-8").WithCapacity(0),
			},
			disableClusterNap: true,
			podCpu:            3000,
			expectedNapFailureReason: &vispb.ParametrizedMessage{
				MessageId: "no.scale.up.nap.disabled",
			},
			expectedRejectedMigs: map[string]*vispb.ParametrizedMessage{
				"pool-4": {
					MessageId: "no.scale.up.mig.capacity.constraints",
				},
				"pool-8": {
					MessageId: "no.scale.up.mig.capacity.constraints",
				},
			},
		},
	}

	for _, sim := range []struct {
		name    string
		enabled bool
	}{
		{name: "scale_up_simulation_disabled", enabled: false},
		{name: "scale_up_simulation_enabled", enabled: true},
	} {
		for _, tc := range testCases {
			t.Run(sim.name+": "+tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer integration_synctest.TearDown(cancel)
					infra := integration.SetupInfrastructure(ctx, t)

					testConfig := integration.NewTestConfig().
						WithNodePools(tc.nodePools...).
						WithCccCrds(tc.ccc).
						WithClusterOverrides(
							integration.WithAutoprovisioningLocations("us-central1-b"),
						).
						WithOverrides(
							integration.WithMaxMemoryTotal(140*1024*1024*1024),
							integration.WithFlexAdvisorEnabled(),
							integration.WithAutoProvisioningEnabled(),
							integration.WithAutoscalerVisibility(true),
							integration.WithEmitNoScaleUpCAVizEvents(true),
							integration.WithScaleUpSimulationForSkippedNodeGroups(sim.enabled),
						)

					if !tc.disableClusterNap {
						testConfig = testConfig.WithClusterOverrides(
							integration.WithClusterAutoProvisioningEnabled(),
						)
					}

					if sim.enabled {
						testConfig = testConfig.WithExperiments(
							experiments.ScaleUpSimulationForSkippedNodeGroupsEnabledFlag,
							experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag,
						)
					}

					for _, g := range tc.guidances {
						infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(g)
					}

					autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
					assert.NoError(t, err)

					integration_flexadvisor.PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "my-ccc")
					// PrimeflexAdvisorCache tries to schedule a pod and emits ScaleUp/NoScaleUp events. We need to clean them here
					infra.Fakes.EventLogger.Clear()

					podCount := tc.podCount
					if podCount == 0 {
						podCount = 1
					}
					for i := 0; i < podCount; i++ {
						podName := "unsched-pod"
						if podCount > 1 {
							podName = fmt.Sprintf("unsched-pod-%d", i)
						}
						p := tu.BuildTestPod(podName, tc.podCpu, 8000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"))
						infra.Fakes.K8s.AddPod(p)
					}

					// Advance past NegativeEventsStalenessThreshold (5m) so reasons recorded during cache priming expire,
					// while staying within FlexAdvisor's keepAliveInterval (10m) so the primed FA cache remains active.
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 6*time.Minute)

					noScaleUpEvents := infra.Fakes.EventLogger.NoScaleUpEvents()
					if tc.expectedScaleUpNodes > 0 {
						assert.Empty(t, noScaleUpEvents, "Did not expect NoScaleUp visibility event when scale-up succeeded")
						scaleUpEvents := infra.Fakes.EventLogger.ScaleUpEvents()
						if assert.Len(t, scaleUpEvents, 1, "Expected 1 ScaleUp visibility event") &&
							assert.Len(t, scaleUpEvents[0].IncreasedMigs, 1, "Expected 1 IncreasedMig in ScaleUp event") {
							assert.Equal(t, int32(tc.expectedScaleUpNodes), scaleUpEvents[0].IncreasedMigs[0].RequestedNodes)
						}
						return
					}
					if !assert.NotEmpty(t, noScaleUpEvents, "Expected at least one NoScaleUp visibility event") {
						return
					}
					lastEvent := noScaleUpEvents[len(noScaleUpEvents)-1]

					// Verify top-level NapFailureReason
					assert.Equal(t, lastEvent.NapFailureReason, tc.expectedNapFailureReason)

					// Enabling ScaleUpSimulationForSkippedNodeGroups only changes where skippedMigs are reported:
					// they move from global SkippedMigs to pod group SkippedMigs.
					var expectedGlobalSkippedMigs, expectedPodGroupSkippedMigs map[string]*vispb.ParametrizedMessage
					if sim.enabled {
						expectedPodGroupSkippedMigs = tc.expectedSkippedMigs
					} else {
						expectedGlobalSkippedMigs = tc.expectedSkippedMigs
					}

					// Verify UnhandledPodGroups
					if assert.Len(t, lastEvent.UnhandledPodGroups, 1, "Expected 1 UnhandledPodGroup") {
						pg := lastEvent.UnhandledPodGroups[0]

						// Verify PodGroup NapFailureReasons
						if len(tc.expectedPodGroupNapFailureReasons) == 0 {
							assert.Empty(t, pg.NapFailureReasons, "Unexpected PodGroup NapFailureReasons")
						} else if assert.Len(t, pg.NapFailureReasons, len(tc.expectedPodGroupNapFailureReasons)) {
							for i, expected := range tc.expectedPodGroupNapFailureReasons {
								assert.Equal(t, expected.MessageId, pg.NapFailureReasons[i].MessageId)
								assert.Equal(t, expected.Parameters, pg.NapFailureReasons[i].Parameters)
							}
						}

						assertMigExplanationsByNodePool(t, pg.RejectedMigs, tc.expectedRejectedMigs, "PodGroup RejectedMigs")
						assertMigExplanationsByNodePool(t, pg.SkippedMigs, expectedPodGroupSkippedMigs, "PodGroup SkippedMigs")
					}

					assertMigExplanationsByNodePool(t, lastEvent.SkippedMigs, expectedGlobalSkippedMigs, "global SkippedMigs")
				})
			})
		}
	}
}

func TestNoScaleUpVisibilityLogs_MultiplePodGroups(t *testing.T) {
	type podGroupExpectation struct {
		rejectedMigs map[string]*vispb.ParametrizedMessage
		skippedMigs  map[string]*vispb.ParametrizedMessage
	}

	testCases := []struct {
		name                              string
		simulationEnabled                 bool
		cccs                              []*v1.ComputeClass
		nodePools                         []*gke_api_beta.NodePool
		guidances                         []fake.CapacityGuidance
		pods                              []*apiv1.Pod
		expectedScheduledPodNodePool      map[string]string
		expectedNapFailureReason          *vispb.ParametrizedMessage
		expectedGlobalSkippedMigs         map[string]*vispb.ParametrizedMessage
		expectedPodGroupsByControllerName map[string]podGroupExpectation
	}{
		{
			name:              "unrelated pod failing predicates in same CCC - does not get polluted with FA rejected MIG",
			simulationEnabled: true,
			cccs: []*v1.ComputeClass{
				ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-4").Build(),
			},
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-4").WithCapacity(0),
			},
			pods: []*apiv1.Pod{
				tu.BuildTestPod("pod-small", 1000, 8000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"), pod.WithOwnerReplicaSet("rs-small")),
				tu.BuildTestPod("pod-large", 6000, 8000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"), pod.WithOwnerReplicaSet("rs-large")),
			},
			expectedNapFailureReason: &vispb.ParametrizedMessage{
				MessageId:  "no.scale.up.nap.capacity.constraints",
				Parameters: []string{"my-ccc"},
			},
			expectedPodGroupsByControllerName: map[string]podGroupExpectation{
				"rs-small": {
					rejectedMigs: map[string]*vispb.ParametrizedMessage{
						"pool-4": {
							MessageId: "no.scale.up.mig.capacity.constraints",
						},
					},
				},
				"rs-large": {
					rejectedMigs: map[string]*vispb.ParametrizedMessage{
						"pool-4": {
							MessageId:  "no.scale.up.mig.failing.predicate",
							Parameters: []string{"NodeResourcesFit", "Insufficient cpu"},
						},
					},
				},
			},
		},
		{
			name:              "fallback scale-up on one option - does not contaminate NoScaleUp event of unschedulable pod in same CCC",
			simulationEnabled: false,
			cccs: []*v1.ComputeClass{
				ccc.NewComputeClassBuilder("my-ccc").WithNodePoolsRules("pool-4", "pool-8").Build(),
			},
			nodePools: []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-4").WithMachineType("n1-standard-4").WithCCCLabel("my-ccc").Build(),
				integration.EmptyNodePool("pool-8").WithMachineType("n1-standard-8").WithCCCLabel("my-ccc").Build(),
			},
			guidances: []fake.CapacityGuidance{
				fake.NewGuidance("n1-standard-4").WithCapacity(0),
				fake.NewGuidance("n1-standard-8").WithCapacity(10),
			},
			pods: []*apiv1.Pod{
				// pod-a (3 CPUs) fits on pool-4 (cut by FA) and falls back to pool-8 (scales up successfully)
				tu.BuildTestPod("pod-a", 3000, 8000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"), pod.WithOwnerReplicaSet("rs-a")),
				// pod-b (16 CPUs) is too big for both pool-4 and pool-8 (Insufficient cpu)
				tu.BuildTestPod("pod-b", 16000, 8000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"), pod.WithOwnerReplicaSet("rs-b")),
			},
			expectedScheduledPodNodePool: map[string]string{
				"pod-a": "pool-8",
			},
			expectedNapFailureReason:  nil,
			expectedGlobalSkippedMigs: nil,
			expectedPodGroupsByControllerName: map[string]podGroupExpectation{
				"rs-b": {
					rejectedMigs: map[string]*vispb.ParametrizedMessage{
						"pool-4": {
							MessageId:  "no.scale.up.mig.failing.predicate",
							Parameters: []string{"NodeResourcesFit", "Insufficient cpu"},
						},
						"pool-8": {
							MessageId:  "no.scale.up.mig.failing.predicate",
							Parameters: []string{"NodeResourcesFit", "Insufficient cpu"},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)

				testConfig := integration.NewTestConfig().
					WithNodePools(tc.nodePools...).
					WithCccCrds(tc.cccs...).
					WithClusterOverrides(
						integration.WithAutoprovisioningLocations("us-central1-b"),
						integration.WithClusterAutoProvisioningEnabled(),
					).
					WithOverrides(
						integration.WithMaxMemoryTotal(140*1024*1024*1024),
						integration.WithFlexAdvisorEnabled(),
						integration.WithAutoProvisioningEnabled(),
						integration.WithAutoscalerVisibility(true),
						integration.WithEmitNoScaleUpCAVizEvents(true),
						integration.WithScaleUpSimulationForSkippedNodeGroups(tc.simulationEnabled),
					)

				if tc.simulationEnabled {
					testConfig = testConfig.WithExperiments(
						experiments.ScaleUpSimulationForSkippedNodeGroupsEnabledFlag,
						experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag,
					)
				}

				for _, g := range tc.guidances {
					infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(g)
				}

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)

				integration_flexadvisor.PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "my-ccc")

				for _, p := range tc.pods {
					infra.Fakes.K8s.AddPod(p)
				}

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 6*time.Minute)
				infra.Fakes.RunScheduler(ctx, t)

				for podName, expectedPool := range tc.expectedScheduledPodNodePool {
					updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
					assert.NoError(t, err)
					assert.Contains(t, updatedPod.Spec.NodeName, expectedPool, "Expected pod %q to be scheduled on %q", podName, expectedPool)
				}

				noScaleUpEvents := infra.Fakes.EventLogger.NoScaleUpEvents()
				if !assert.NotEmpty(t, noScaleUpEvents, "Expected at least one NoScaleUp visibility event") {
					return
				}
				lastEvent := noScaleUpEvents[len(noScaleUpEvents)-1]

				assert.Equal(t, lastEvent.NapFailureReason, tc.expectedNapFailureReason)
				assertMigExplanationsByNodePool(t, lastEvent.SkippedMigs, tc.expectedGlobalSkippedMigs, "global SkippedMigs")

				if assert.Len(t, lastEvent.UnhandledPodGroups, len(tc.expectedPodGroupsByControllerName)) {
					for _, pg := range lastEvent.UnhandledPodGroups {
						controllerName := pg.PodGroup.SamplePod.Controller.Name
						expectedPg, ok := tc.expectedPodGroupsByControllerName[controllerName]
						if !assert.True(t, ok, "Unexpected pod group controller %q", controllerName) {
							continue
						}
						assertMigExplanationsByNodePool(t, pg.RejectedMigs, expectedPg.rejectedMigs, fmt.Sprintf("RejectedMigs for %q", controllerName))
						assertMigExplanationsByNodePool(t, pg.SkippedMigs, expectedPg.skippedMigs, fmt.Sprintf("SkippedMigs for %q", controllerName))
					}
				}
			})
		})
	}
}

func assertMigExplanationsByNodePool(t *testing.T, actual []*vispb.MigExplanation, expected map[string]*vispb.ParametrizedMessage, fieldName string) {
	t.Helper()
	if len(expected) == 0 {
		assert.Empty(t, actual, "Unexpected %s", fieldName)
		return
	}
	if !assert.Len(t, actual, len(expected), "Unexpected count for %s", fieldName) {
		return
	}
	for _, migExp := range actual {
		if assert.NotNil(t, migExp.Mig) && assert.NotNil(t, migExp.Reason) {
			expectedReason, ok := expected[migExp.Mig.Nodepool]
			if assert.True(t, ok, "Unexpected nodepool %q in %s", migExp.Mig.Nodepool, fieldName) {
				assert.Equal(t, expectedReason.MessageId, migExp.Reason.MessageId)
				assert.Equal(t, expectedReason.Parameters, migExp.Reason.Parameters)
			}
		}
	}
}

// TestNoScaleUpVisibilityLogs_HtnapTriggerDoesNotIncludeFlexAdvisorData verifies that when HTNAP
// asynchronously initializes a created node pool and invokes ScaleUpStatusProcessor on a background
// goroutine, the HTNAP status processor trigger does not include FlexAdvisor NoScaleUp data even when
// scaleUpLimiterTracker currently holds FA-removed options, and subsequent blocked scale-ups still
// accurately emit their own FA capacity constraint reasons.
func TestNoScaleUpVisibilityLogs_HtnapTriggerDoesNotIncludeFlexAdvisorData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		// nap-ccc has Priority 1 (n1-standard-8) blocked by FlexAdvisor (Capacity=0, which populates
		// scaleUpLimiterTracker during bin-packing) and Priority 2 (n1-standard-4) allowed (Capacity=10),
		// which triggers async HTNAP node pool creation.
		napCcc := ccc.NewComputeClassBuilder("nap-ccc").WithNapEnabled().
			WithPriorities(
				v1.Priority{
					MachineType: new("n1-standard-8"),
				},
				v1.Priority{
					MachineType: new("n1-standard-4"),
				},
			).Build()

		// blocked-ccc has its only priority (n1-standard-8) blocked by FlexAdvisor (Capacity=0).
		blockedCcc := ccc.NewComputeClassBuilder("blocked-ccc").WithNodePoolsRules("pool-blocked").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-blocked").WithMachineType("n1-standard-8").WithCCCLabel("blocked-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(napCcc, blockedCcc).
			WithClusterOverrides(
				integration.WithAutoprovisioningLocations("us-central1-b"),
				integration.WithClusterAutoProvisioningEnabled(),
			).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
				integration.WithAutoProvisioningEnabled(),
				integration.WithHighThroughputNAPEnabled(10, 100),
				integration.WithAutoscalerVisibility(true),
				integration.WithEmitNoScaleUpCAVizEvents(true),
			)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("n1-standard-8").WithCapacity(0),
			fake.NewGuidance("n1-standard-4").WithCapacity(10),
		)
		// Delay CreateNodePool so HTNAP's AsyncNodeGroupInitializer.InitializeNodeGroup runs
		// asynchronously after the main RunOnce loop has completed (while scaleUpLimiterTracker
		// still holds the removed n1-standard-8 option for "nap-ccc").
		infra.Fakes.GkeService.DisableOperationDelay(true)
		infra.Fakes.GkeService.SetCreateNodePoolDelay(5 * time.Second)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		integration_flexadvisor.PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "nap-ccc")
		integration_flexadvisor.PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "blocked-ccc")
		infra.Fakes.EventLogger.Clear()

		// Step 1: Add pod-nap requesting nap-ccc and run one autoscaler loop.
		// During bin-packing, n1-standard-8 is rejected by FA (populating scaleUpLimiterTracker with "nap-ccc")
		// and n1-standard-4 is chosen for async HTNAP creation.
		podNap := tu.BuildTestPod("pod-nap", 3000, 8000, tu.MarkUnschedulable(), pod.WithCCC("nap-ccc"), pod.WithOwnerReplicaSet("rs-nap"))
		infra.Fakes.K8s.AddPod(podNap)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		// Clear events emitted during the synchronous RunOnce pass so we isolate the events emitted
		// by HTNAP's background InitializeNodeGroup -> ScaleUpStatusProcessor.Process call.
		infra.Fakes.EventLogger.Clear()

		// Advance virtual time so the 5s CreateNodePool delay expires and HTNAP's background goroutine
		// executes InitializeNodeGroup -> ScaleUpStatusProcessor.Process.
		time.Sleep(10 * time.Second)
		synctest.Wait()

		// Confirm that HTNAP's background InitializeNodeGroup DID trigger ScaleUpStatusProcessor
		// (producing the ScaleUp visibility event for the async node pool), and that it did NOT emit
		// any NoScaleUp event or FlexAdvisor constraint data despite scaleUpLimiterTracker holding "nap-ccc".
		assert.NotEmpty(t, infra.Fakes.EventLogger.ScaleUpEvents(), "Expected HTNAP InitializeNodeGroup to emit a ScaleUp event via ScaleUpStatusProcessor")
		assert.Empty(t, infra.Fakes.EventLogger.NoScaleUpEvents(), "Expected HTNAP ScaleUpStatusProcessor trigger not to emit any NoScaleUp / FA visibility event")

		infra.Fakes.RunScheduler(ctx, t)
		updatedPodNap, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-nap", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPodNap.Spec.NodeName, "Expected pod-nap to be scheduled on the HTNAP-created node pool")

		// Step 2: Add pod-blocked requesting blocked-ccc (where pool-blocked is constrained by FA Capacity=0)
		// and run the next loop. Verify that InitBinpacking resets scaleUpLimiterTracker (clearing "nap-ccc")
		// and that the NoScaleUp event reports only "blocked-ccc".
		infra.Fakes.EventLogger.Clear()
		podBlocked := tu.BuildTestPod("pod-blocked", 3000, 8000, tu.MarkUnschedulable(), pod.WithCCC("blocked-ccc"), pod.WithOwnerReplicaSet("rs-blocked"))
		infra.Fakes.K8s.AddPod(podBlocked)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 6*time.Minute)

		noScaleUpEvents := infra.Fakes.EventLogger.NoScaleUpEvents()
		if assert.NotEmpty(t, noScaleUpEvents, "Expected NoScaleUp visibility event for blocked-ccc") {
			lastEvent := noScaleUpEvents[len(noScaleUpEvents)-1]
			assert.Equal(t, lastEvent.NapFailureReason, &vispb.ParametrizedMessage{
				MessageId:  "no.scale.up.nap.capacity.constraints",
				Parameters: []string{"blocked-ccc"},
			})
			if assert.Len(t, lastEvent.UnhandledPodGroups, 1) {
				assertMigExplanationsByNodePool(t, lastEvent.UnhandledPodGroups[0].RejectedMigs, map[string]*vispb.ParametrizedMessage{
					"pool-blocked": {
						MessageId: "no.scale.up.mig.capacity.constraints",
					},
				}, "RejectedMigs for rs-blocked")
			}
		}
	})
}
