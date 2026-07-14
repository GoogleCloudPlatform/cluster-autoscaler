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
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestFlexAdvisorFailedToRespond verifies that if FlexAdvisor fails to respond (returns an error),
// the autoscaler falls back to standard balancing logic and still succeeds in scaling up.
// Covers b/514268582 (in part, general FA failure handling).
func TestFlexAdvisorFailedToRespond(t *testing.T) {
	ccc := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build()
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-1").WithMachineType(StockOutMachineType).WithCCCLabel(ccc.Name).Build(),
		integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel(ccc.Name).Build(),
	}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(fake.CapacityGuidance{Error: errors.New("FA internal error")})

		// Mock GCE stockout error on pool-1, so if we try to scale it, it fails.
		infra.Fakes.GceService.SetCreateInstanceForMigError("pool-1", stockOutError())

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod)

		// Loop 1: Should try pool-1 (due to FA fallback to standard priority) and fail because of GCE stockout
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod is NOT scheduled yet
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "Expected standard-pod to remain unschedulable after first loop")

		// Loop 2: Should try pool-2 and succeed
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod IS scheduled on pool-2
		updatedPod, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected standard-pod to be scheduled after second loop")
		assert.Contains(t, updatedPod.Spec.NodeName, "pool-2", "Expected pod to be scheduled on pool-2")
	})
}

// TestFlexAdvisorSkippedResponseForZone verifies that if FlexAdvisor skips the response
// for a specific zone, the autoscaler falls back to standard balancing for that zone
// instead of treating it as 0 capacity.
// Covers b/512426121 (missing zone from response is not treated as 0 capacity).
func TestFlexAdvisorSkippedResponseForZone(t *testing.T) {
	ccc := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build()
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-1").WithMachineType(OneInstanceAvailableMachineType).WithCCCLabel(ccc.Name).Build(),
		integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel(ccc.Name).Build(),
	}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(OneInstanceAvailableMachineType).WithZone(ZoneB).WithOmit(true),
			fake.NewGuidance(OneInstanceAvailableMachineType).WithCapacity(1),
			fake.NewGuidance(AvailableMachineType).WithCapacity(10),
		)

		// Mock GCE stockout error on pool-1
		infra.Fakes.GceService.SetCreateInstanceForMigError("pool-1", stockOutError())

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod)

		// Loop 1: Should try pool-1 (due to FA fallback) and fail because of GCE stockout
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod is NOT scheduled yet
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "Expected standard-pod to remain unschedulable after first loop")

		// Loop 2: Should try pool-2 and succeed
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod IS scheduled on pool-2
		updatedPod, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected standard-pod to be scheduled after second loop")
		assert.Contains(t, updatedPod.Spec.NodeName, "pool-2", "Expected pod to be scheduled on pool-2")
	})
}

func TestFlexAdvisorTimeout(t *testing.T) {
	ccc := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build()
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-1").WithMachineType(StockOutMachineType).WithCCCLabel(ccc.Name).Build(),
		integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel(ccc.Name).Build(),
	}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(fake.NewGuidance(StockOutMachineType).WithCapacity(0))

		// Inject a delay longer than the default 15s timeout
		infra.Fakes.FlexAdvisorClient.SetDelay(60 * time.Second)

		// Mock GCE stockout error on pool-1
		infra.Fakes.GceService.SetCreateInstanceForMigError("pool-1", stockOutError())

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod)

		// Loop 1: Should try pool-1 (due to FA timeout and fallback to standard priority)
		// and fail because of GCE stockout. Virtual time will automatically advance.
		// Without timeout it would jump directly to the 2nd priority.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod is NOT scheduled yet
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName)

		// Loop 2: Should try pool-2 and succeed
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod IS scheduled on pool-2
		updatedPod, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, updatedPod.Spec.NodeName, "pool-2")
	})
}

func TestFlexAdvisorMissingRecommendationForMachineType(t *testing.T) {
	ccc := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build()
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-1").WithMachineType(UnknownAvailabilityMachineType).WithCCCLabel(ccc.Name).Build(),
		integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel(ccc.Name).Build(),
	}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(UnknownAvailabilityMachineType).WithOmit(true),
			fake.NewGuidance(AvailableMachineType).WithCapacity(10),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod)

		// Loop 1: Should try pool-1 (due to FA fallback) and succeed because no GCE stockout is simulated
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify standard-pod IS scheduled on pool-1
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected standard-pod to be scheduled after first loop")
		assert.Contains(t, updatedPod.Spec.NodeName, "pool-1", "Expected pod to be scheduled on pool-1")
	})
}

// TestFlexAdvisorSpotVsOnDemand verifies that the autoscaler correctly falls back
// from Spot to On-Demand node pools when Flex Advisor reports Spot capacity is exhausted.
// Covers b/517014046.
func TestFlexAdvisorSpotVsOnDemand(t *testing.T) {
	spotPoolName := "pool-spot"
	standardPoolName := "pool-standard"
	machineType := AvailableMachineType // n1-standard-4

	ccc := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules(spotPoolName, standardPoolName).Build()
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool(spotPoolName).WithMachineType(machineType).WithSpot().WithCCCLabel(ccc.Name).Build(),
		integration.EmptyNodePool(standardPoolName).WithMachineType(machineType).WithCCCLabel(ccc.Name).Build(),
	}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		spotMode := instanceavailability.Spot
		standardMode := instanceavailability.Standard

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.CapacityGuidance{
				MachineType:      &machineType,
				ProvisioningMode: &spotMode,
				InstanceCount:    0,
			},
			fake.CapacityGuidance{
				MachineType:      &machineType,
				ProvisioningMode: &standardMode,
				InstanceCount:    10,
			},
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Build pod that tolerates spot
		pod := tu.BuildTestPod("spot-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, apiv1.Toleration{
			Key:      gkelabels.SpotLabel,
			Operator: apiv1.TolerationOpEqual,
			Value:    gkelabels.PreemptionValue,
			Effect:   apiv1.TaintEffectNoSchedule,
		})
		infra.Fakes.K8s.AddPod(pod)

		// Loop 1: Should try pool-spot, see 0 capacity from FA, and fallback to pool-standard immediately
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Verify pod is scheduled on pool-standard
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "spot-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected spot-pod to be scheduled")
		assert.Contains(t, updatedPod.Spec.NodeName, standardPoolName, "Expected pod to be scheduled on standard pool, but got %s", updatedPod.Spec.NodeName)

		assert.Greater(t, infra.Fakes.FlexAdvisorClient.GetFetchCapacityCalls(), 0, "Expected Flex Advisor to be queried")
	})
}
