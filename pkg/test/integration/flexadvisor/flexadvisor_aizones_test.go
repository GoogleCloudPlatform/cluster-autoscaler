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
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	options "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	pod "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
)

func WithZoneTypesEnabled() integration.Option[*options.AutoscalingOptions] {
	return func(o *options.AutoscalingOptions) *options.AutoscalingOptions {
		o.ZoneTypesEnabled = true
		return o
	}
}

func setupAIZonesTest(t *testing.T) (testConfig *integration.TestConfig, standardZone, aiZone string) {
	aiZone = "us-central1-ai1"
	standardZone = "us-central1-a"
	region := "us-central1"

	cccObj := ccc.NewComputeClassBuilder("test-ccc").
		WithNapEnabled().
		WithPriorities(
			v1.Priority{
				MachineType: ptr.To(AvailableMachineType),
				Location: &v1.Location{
					ZoneTypes: []v1.ZoneType{v1.ZoneType("AI")},
				},
			},
		).
		Build()

	testConfig = integration.NewTestConfig().
		WithCccCrds(cccObj).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
			WithZoneTypesEnabled(),
			integration.WithAutoProvisioningEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations(standardZone, aiZone),
		).
		WithRegionToZones(map[string][]string{region: {standardZone}}).
		WithRegionToAiZones(map[string][]string{region: {aiZone}})

	return testConfig, standardZone, aiZone
}

// TestAIZonesFlexAdvisorStockout verifies that when a ComputeClass rule specifies zoneTypes: ["AI"],
// and the only configured AI zone is stocked out (0 capacity in Flex Advisor),
// the autoscaler skips autoprovisioning in that AI zone and does not scale up.
func TestAIZonesFlexAdvisorStockout(t *testing.T) {
	testConfig, standardZone, aiZone := setupAIZonesTest(t)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Mock Flex Advisor to return 0 capacity for the AI zone.
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewFakeCapacityGuidanceForMachineTypeAndZone(AvailableMachineType, aiZone, 0, 0.5),
			fake.NewFakeCapacityGuidanceForMachineTypeAndZone(AvailableMachineType, standardZone, 10, 0.5),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Request scale up for a pod matching the CCC.
		testPod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(testPod)

		// Run autoscaler loop.
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify that the pod remains unschedulable because the AI zone was stocked out
		// and the standard zone was not allowed by the CCC.
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "Expected pod to remain unschedulable when AI zone is stocked out")
		assert.Equal(t, 0, len(infra.Fakes.K8s.Nodes().Items), "Expected 0 nodes to be created")
		assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected Flex Advisor to be queried")
	})
}

// TestAIZonesFlexAdvisorWithCapacity verifies that when a ComputeClass rule specifies zoneTypes: ["AI"],
// and the AI zone has capacity in Flex Advisor, the autoscaler successfully scales up.
func TestAIZonesFlexAdvisorWithCapacity(t *testing.T) {
	testConfig, standardZone, aiZone := setupAIZonesTest(t)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Mock Flex Advisor to return 10 capacity for the AI zone.
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewFakeCapacityGuidanceForMachineTypeAndZone(AvailableMachineType, aiZone, 10, 0.5),
			fake.NewFakeCapacityGuidanceForMachineTypeAndZone(AvailableMachineType, standardZone, 10, 1),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Request scale up.
		testPod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(testPod)

		// Run autoscaler loop.
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify that the pod IS scheduled on the AI zone.
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to be scheduled")
		node, err := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, updatedPod.Spec.NodeName, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, aiZone, node.Labels[apiv1.LabelTopologyZone], "Expected pod to be scheduled in AI zone %s, but node zone label was %s", aiZone, node.Labels[apiv1.LabelTopologyZone])
		assert.Equal(t, 1, len(infra.Fakes.K8s.Nodes().Items), "Expected 1 node to be created")
		assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected Flex Advisor to be queried")
	})
}
