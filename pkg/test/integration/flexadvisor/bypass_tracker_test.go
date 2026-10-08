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
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func getFlexAdvisorBypassAttemptsMetric(t *testing.T) float64 {
	t.Helper()
	val, err := metrics.GetFlexAdvisorRecommendationsBypassCountForTest()
	assert.NoError(t, err)
	return val
}

// TestFlexAdvisorRecommendationsBypass_ScalesUpAfterThreeConsecutiveBlockedLoops verifies that when FlexAdvisor
// blocks all scale-up options for a ComputeClass 3 times within the 60-minute measurement window, the 4th loop
// pauses recommendations enforcement for that loop, allowing the pod to scale up. It also verifies that enforcement
// is re-enabled on the 5th loop (intermittent single-loop bypass).
func TestFlexAdvisorRecommendationsBypass_ScalesUpAfterThreeConsecutiveBlockedLoops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-1").WithMachineType(ZeroCapacityRecommendationMachineType).WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
			)

		// FlexAdvisor reports 0 capacity for ZeroCapacityRecommendationMachineType, whereas GCE actually has capacity.
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")
		initialMetricVal := getFlexAdvisorBypassAttemptsMetric(t)

		pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod1)

		// Loops 1, 2, 3: FA blocks scale-up (ScaleUpNoOptionsAvailable). Pod remains unschedulable.
		for loop := 1; loop <= 3; loop++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
			assert.NoError(t, err)
			assert.Empty(t, updatedPod.Spec.NodeName, "loop %d: expected pod-1 to remain unschedulable", loop)
			assert.Equal(t, initialMetricVal, getFlexAdvisorBypassAttemptsMetric(t), "loop %d: expected bypass metric not to increment before threshold", loop)
		}

		// Loop 4: Threshold (3 blocks) reached -> recommendations enforcement paused for this loop -> scales up pool-1.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, updatedPod1.Spec.NodeName, "pool-1", "loop 4: expected pod-1 to be scheduled on pool-1 via bypass")
		assert.Equal(t, initialMetricVal+1, getFlexAdvisorBypassAttemptsMetric(t), "loop 4: expected bypass attempt metric to increment by 1")

		// Loop 5: Add a second pod requiring another node while FA still reports 0 capacity.
		// Verify that RemoveBypasses() cleared the bypass from Loop 4 and enforcement is active again.
		pod2 := tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod2)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod2, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-2", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod2.Spec.NodeName, "loop 5: expected pod-2 to be blocked again because bypass is single-loop")
		assert.Equal(t, initialMetricVal+1, getFlexAdvisorBypassAttemptsMetric(t), "loop 5: expected bypass attempt metric not to increment again")
	})
}

// TestFlexAdvisorRecommendationsBypass_SlidingWindowExpirationDoesNotTriggerBypass verifies that blocked scale-ups
// older than the 60-minute sliding window are evicted and do not trigger a bypass until 3 blocks occur within 60 minutes.
func TestFlexAdvisorRecommendationsBypass_SlidingWindowExpirationDoesNotTriggerBypass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-1").WithMachineType(ZeroCapacityRecommendationMachineType).WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
			)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod1)

		// Block 1 at t = +1m
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		// Block 2 at t = +20m
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 19*time.Minute)
		// Block 3 at t = +62m (Block 1 at t=1m is now 61m old and outside the 60m window; valid blocks = [20m, 62m])
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 42*time.Minute)

		// Loop 4 at t = +63m: Only 2 blocks (20m, 62m) are in the 60m window, so bypass should NOT trigger.
		// Loop 4 itself will be blocked and record a 3rd valid block at t = +63m (valid blocks = [20m, 62m, 63m]).
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "loop 4 (t=63m): expected pod-1 to remain unschedulable because 1st block expired")

		// Loop 5 at t = +64m: Now 3 blocks (20m, 62m, 63m) are within the 60m window -> bypass triggers!
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, updatedPod.Spec.NodeName, "pool-1", "loop 5 (t=64m): expected pod-1 to schedule on pool-1 once 3 blocks are in window")
	})
}

// TestFlexAdvisorRecommendationsBypass_MigsBlockedNotByFa verifies that when MIGs are blocked by prefilters
// (e.g. insufficient CPU) or node group backoffs (e.g. GCE stockout), those ScaleUpNoOptionsAvailable loops do NOT
// contribute to the FlexAdvisor bypass counter unless FlexAdvisor itself constrained at least one viable MIG.
func TestFlexAdvisorRecommendationsBypass_MigsBlockedNotByFa(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		// pool-small: e2-medium (2 vCPU) - too small for a 3 vCPU pod (blocked by prefilter/predicate)
		// pool-stockout: n2-standard-4 (4 vCPU) - FA reports capacity=10, but GCE returns stockout (enters backoff)
		// pool-fa: n1-standard-4 (4 vCPU) - GCE has capacity, controlled by FA guidance
		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-small", "pool-stockout", "pool-fa").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-small").WithMachineType("e2-medium").WithCCCLabel("test-ccc").Build(),
			integration.EmptyNodePool("pool-stockout").WithMachineType(StockOutMachineType).WithCCCLabel("test-ccc").Build(),
			integration.EmptyNodePool("pool-fa").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
			)

		// Initially, only pool-small and pool-stockout exist in the cluster, or pool-fa is at max size / not yet blocked by FA.
		// Let's set GCE stockout on both pool-stockout and pool-fa for Phase 1 while FA reports Capacity: 10 for both!
		infra.Fakes.GceService.SetCreateInstanceForMigError("pool-stockout", stockOutError())
		infra.Fakes.GceService.SetCreateInstanceForMigError("pool-fa", stockOutError())
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("e2-medium").WithCapacity(10),
			fake.NewGuidance(StockOutMachineType).WithCapacity(10),
			fake.NewGuidance(AvailableMachineType).WithCapacity(10),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod1)

		// Phase 1:
		// Loop 1 tries pool-stockout and fails with GCE stockout (pool-stockout enters backoff).
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		// Loop 2 tries pool-fa and fails with GCE stockout (pool-fa enters backoff).
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Loops 3, 4, 5: Both pool-stockout and pool-fa are in backoff, and pool-small fails CPU predicate.
		// All 3 loops result in ScaleUpNoOptionsAvailable, but FA did NOT block any MIG!
		for loop := 3; loop <= 5; loop++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
			infra.Fakes.RunScheduler(ctx, t)
			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
			assert.NoError(t, err)
			assert.Empty(t, updatedPod.Spec.NodeName, "loop %d: expected pod-1 to remain unschedulable", loop)
		}

		// Phase 2: Advance time by 35 minutes so GCE backoffs expire, clear GCE stockouts,
		// and update FA to block pool-fa (Capacity: 0) and pool-stockout (Capacity: 0).
		infra.Fakes.GceService.ClearCreateInstanceForMigError("pool-stockout")
		infra.Fakes.GceService.ClearCreateInstanceForMigError("pool-fa")
		infra.Fakes.FlexAdvisorClient.ClearCapacityGuidances()
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance("e2-medium").WithCapacity(10),
			fake.NewGuidance(StockOutMachineType).WithCapacity(0),
			fake.NewGuidance(AvailableMachineType).WithCapacity(0),
		)

		// Wait 35 minutes so backoffs expire and FA polls the new Capacity: 0 guidance.
		// If the 3 non-FA ScaleUpNoOptionsAvailable loops in Phase 1 had counted toward the bypass tracker,
		// this very next loop would immediately trigger bypass and schedule pod-1!
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 35*time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "expected non-FA blocked loops not to trigger bypass on first FA-blocked loop")

		// That loop was FA-blocked #1. Two more FA-blocked loops (#2 and #3) should still not schedule pod-1:
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute) // FA-blocked #2
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute) // FA-blocked #3
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "expected pod-1 to remain unschedulable until 3 FA-blocked loops complete")

		// Now on the 4th loop after FA started blocking, bypass triggers and schedules pod-1 on pool-stockout or pool-fa!
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "expected pod-1 to be scheduled after 3 genuine FA-blocked loops")
	})
}

// TestFlexAdvisorRecommendationsBypass_PartialFaBlockWithAvailableOptionResetsCounters verifies that when FlexAdvisor
// blocks some MIGs (e.g. pool-1) while another MIG in the same ComputeClass (pool-2) has non-zero FA capacity and scales up,
// the scale-up does not contribute to bypass counters and resets previously accumulated blocked counters.
func TestFlexAdvisorRecommendationsBypass_PartialFaBlockWithAvailableOptionResetsCounters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-1").WithMachineType(ZeroCapacityRecommendationMachineType).WithCCCLabel("test-ccc").Build(),
			integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
			)

		// Initially, FA blocks BOTH pool-1 and pool-2 (Capacity: 0).
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
			fake.NewGuidance(AvailableMachineType).WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod1)

		// Loops 1 & 2: Both pools blocked by FA -> 2 blocked scale-ups recorded.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)

		// Before Loop 3: FA still blocks pool-1 (ZeroCapacityRecommendationMachineType = 0), but allows pool-2 (AvailableMachineType = 1).
		infra.Fakes.FlexAdvisorClient.ClearCapacityGuidances()
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
			fake.NewGuidance(AvailableMachineType).WithCapacity(1),
		)

		// Wait >1m for FA background worker to fetch updated guidance and run Loop 3.
		// Loop 3 scales up pool-2 (not NoOptionsAvailable) and resets the bypass counter for test-ccc!
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, updatedPod1.Spec.NodeName, "pool-2", "loop 3: expected pod-1 to schedule on pool-2")

		// Now FA blocks both pools again (Capacity: 0), and pod-2 arrives.
		infra.Fakes.FlexAdvisorClient.ClearCapacityGuidances()
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
			fake.NewGuidance(AvailableMachineType).WithCapacity(0),
		)
		// Let FA worker refresh guidance before adding pod-2
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)

		pod2 := tu.BuildTestPod("pod-2", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod2)

		// Loops 4 & 5: 1st and 2nd blocked loops for pod-2.
		// Because Loop 3 reset the 2 earlier blocks, the counter is only at 2 (not 4), so neither Loop 4 nor Loop 5 bypasses FA!
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod2, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-2", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod2.Spec.NodeName, "expected pod-2 to remain unschedulable because Loop 3 reset the bypass counter")
	})
}

// TestFlexAdvisorRecommendationsBypass_DisabledByExperimentFlag verifies that when
// FlexAdvisorRecommendationsBypassEnabledFlag is false, recommendations enforcement is never bypassed.
func TestFlexAdvisorRecommendationsBypass_DisabledByExperimentFlag(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-1").WithMachineType(ZeroCapacityRecommendationMachineType).WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithExperimentOverrides(map[string]bool{
				experiments.FlexAdvisorRecommendationsBypassEnabledFlag: false,
			}, nil).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
			)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

		pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod1)

		for loop := 1; loop <= 5; loop++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
			assert.NoError(t, err)
			assert.Empty(t, updatedPod.Spec.NodeName, "loop %d: expected pod-1 to remain unschedulable when bypass experiment is disabled", loop)
		}
	})
}

// TestFlexAdvisorRecommendationsBypass_VisibilityFlagsDisabled tests whether bypass tracking still works
// when UseAutoscalerVisibility (--use-ca-viz) and/or EmitNoScaleUpCAVizEvents (--ca-viz-no-scale-up) are false.
func TestFlexAdvisorRecommendationsBypass_VisibilityFlagsDisabled(t *testing.T) {
	testCases := []struct {
		name                     string
		useAutoscalerVisibility  bool
		emitNoScaleUpCAVizEvents bool
	}{
		{
			name:                     "use-ca-viz disabled but ca-viz-no-scale-up enabled",
			useAutoscalerVisibility:  false,
			emitNoScaleUpCAVizEvents: true,
		},
		{
			name:                     "both use-ca-viz and ca-viz-no-scale-up disabled (default cluster flags)",
			useAutoscalerVisibility:  false,
			emitNoScaleUpCAVizEvents: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)

				cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
				nodePools := []*gke_api_beta.NodePool{
					integration.EmptyNodePool("pool-1").WithMachineType(ZeroCapacityRecommendationMachineType).WithCCCLabel("test-ccc").Build(),
				}

				testConfig := integration.NewTestConfig().
					WithNodePools(nodePools...).
					WithCccCrds(cccCrd).
					WithOverrides(
						integration.WithMaxMemoryTotal(140*1024*1024*1024),
						integration.WithFlexAdvisorEnabled(),
						integration.WithAutoscalerVisibility(tc.useAutoscalerVisibility),
						integration.WithEmitNoScaleUpCAVizEvents(tc.emitNoScaleUpCAVizEvents),
					)

				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
					fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
				)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)

				PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")

				pod1 := tu.BuildTestPod("pod-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
				infra.Fakes.K8s.AddPod(pod1)

				for loop := 1; loop <= 3; loop++ {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
					infra.Fakes.RunScheduler(ctx, t)
				}

				// Loop 4: Expect bypass to trigger even with visibility flags disabled
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
				infra.Fakes.RunScheduler(ctx, t)

				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-1", metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Contains(t, updatedPod.Spec.NodeName, "pool-1", "expected pod-1 to schedule on loop 4")
			})
		})
	}
}

// TestFlexAdvisorRecommendationsBypass_WithHtnapAsyncNodePoolCreation verifies that when HTNAP
// (AsyncGkeManager) asynchronously creates a node pool for another ComputeClass and triggers
// ScaleUpStatusProcessor.Process on a background goroutine, it does not interfere with tracking
// blocked scale-up runs or triggering the bypass on the 4th blocked loop for a FlexAdvisor-blocked CCC.
func TestFlexAdvisorRecommendationsBypass_WithHtnapAsyncNodePoolCreation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		napCcc := ccc.NewComputeClassBuilder("nap-ccc").WithNapEnabled().WithPriorities(
			v1.Priority{
				MachineType: new("e2-standard-8"),
			},
		).Build()
		blockedCcc := ccc.NewComputeClassBuilder("blocked-ccc").WithNodePoolsRules("pool-blocked").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-blocked").WithMachineType(ZeroCapacityRecommendationMachineType).WithCCCLabel("blocked-ccc").Build(),
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
			)

		// ZeroCapacityRecommendationMachineType is blocked by FA (0 capacity) while "e2-standard-8" has capacity for nap-ccc.
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
			fake.NewGuidance("e2-standard-8").WithCapacity(10),
		)
		infra.Fakes.GkeService.DisableOperationDelay(true)
		infra.Fakes.GkeService.SetCreateNodePoolDelay(30 * time.Second)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "nap-ccc")
		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "blocked-ccc")
		initialMetricVal := getFlexAdvisorBypassAttemptsMetric(t)

		// Trigger async HTNAP node pool creation for pod-nap (6 CPUs -> requires e2-standard-8).
		podNap := tu.BuildTestPod("pod-nap", 6000, 12000, tu.MarkUnschedulable(), pod.WithCCC("nap-ccc"))
		infra.Fakes.K8s.AddPod(podNap)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)

		// Add pod-blocked requesting blocked-ccc (ZeroCapacityRecommendationMachineType on pool-blocked, blocked by FA).
		// During the next loops, HTNAP's 30s CreateNodePool delay finishes in the background and calls
		// InitializeNodeGroup -> ScaleUpStatusProcessor.Process.
		podBlocked := tu.BuildTestPod("pod-blocked", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("blocked-ccc"))
		infra.Fakes.K8s.AddPod(podBlocked)

		for loop := 1; loop <= 3; loop++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
			infra.Fakes.RunScheduler(ctx, t)

			updatedBlocked, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-blocked", metav1.GetOptions{})
			assert.NoError(t, err)
			assert.Empty(t, updatedBlocked.Spec.NodeName, "loop %d: expected pod-blocked to remain unschedulable", loop)
		}

		// Loop 4 for pod-blocked: 3 blocked runs reached -> bypass pauses FA recommendations -> scales up pool-blocked.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedBlocked, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-blocked", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, updatedBlocked.Spec.NodeName, "pool-blocked", "loop 4: expected pod-blocked to schedule on pool-blocked via bypass")
		assert.Equal(t, initialMetricVal+1, getFlexAdvisorBypassAttemptsMetric(t))
	})
}

// TestFlexAdvisorRecommendationsBypass_RecoversNormalEnforcementAfterMultipleBypassRounds verifies that after
// multiple consecutive rounds of blocking and bypassing (3 full cycles = 12 loops, triggering bypass 3 times),
// once FlexAdvisor starts returning non-zero recommendations again, FlexAdvisor normal enforcement resumes
// immediately and bypass is not triggered at all across Y subsequent loops.
func TestFlexAdvisorRecommendationsBypass_RecoversNormalEnforcementAfterMultipleBypassRounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		// pool-1 (ZeroCapacityRecommendationMachineType) is higher priority than pool-2 (AvailableMachineType).
		// When bypass is active, CA ignores FA and picks higher-priority pool-1; when FA recommendations are enforced
		// with ZeroCapacityRecommendationMachineType=0 and AvailableMachineType=10, CA must pick pool-2.
		cccCrd := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1", "pool-2").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-1").WithMachineType(ZeroCapacityRecommendationMachineType).WithCCCLabel("test-ccc").Build(),
			integration.EmptyNodePool("pool-2").WithMachineType(AvailableMachineType).WithCCCLabel("test-ccc").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(cccCrd).
			WithOverrides(
				integration.WithMaxMemoryTotal(500*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
			)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
			fake.NewGuidance(AvailableMachineType).WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "test-ccc")
		initialMetricVal := getFlexAdvisorBypassAttemptsMetric(t)

		// Phase 1: 3 full rounds of blocking & bypassing (12 loops total).
		// In each round, loops 1..3 are blocked by FA (0 capacity on all pools), and loop 4 triggers bypass.
		const bypassRounds = 3
		for round := 1; round <= bypassRounds; round++ {
			podName := fmt.Sprintf("pod-bypass-round-%d", round)
			p := tu.BuildTestPod(podName, 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
			infra.Fakes.K8s.AddPod(p)

			for blockedLoop := 1; blockedLoop <= 3; blockedLoop++ {
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
				infra.Fakes.RunScheduler(ctx, t)

				updatedPod, getErr := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
				assert.NoError(t, getErr)
				assert.Empty(t, updatedPod.Spec.NodeName, "round %d blockedLoop %d: expected %s to remain unschedulable", round, blockedLoop, podName)
				assert.Equal(t, initialMetricVal+float64(round-1), getFlexAdvisorBypassAttemptsMetric(t), "round %d blockedLoop %d: bypass metric should not increment yet", round, blockedLoop)
			}

			// 4th loop of the round: bypass triggers and scales up higher-priority pool-1.
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPod, getErr := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
			assert.NoError(t, getErr)
			assert.Contains(t, updatedPod.Spec.NodeName, "pool-1", "round %d loop 4: expected %s to schedule via bypass", round, podName)
			assert.Equal(t, initialMetricVal+float64(round), getFlexAdvisorBypassAttemptsMetric(t), "round %d loop 4: expected bypass metric to increment", round)
		}

		// Phase 2: FlexAdvisor starts returning non-zero recommendations for AvailableMachineType (pool-2),
		// while still blocking ZeroCapacityRecommendationMachineType (pool-1 = 0).
		infra.Fakes.FlexAdvisorClient.ClearCapacityGuidances()
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
			fake.NewGuidance(AvailableMachineType).WithCapacity(10),
		)
		// Allow FA background puller (>1m) to refresh guidance in cache.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)

		// Run Y = 5 consecutive loops with new pods requiring scale-up.
		// Verify that in every loop:
		// 1. Bypass is NOT triggered at all (metric stays at initialMetricVal + 3).
		// 2. FA recommendations are strictly enforced (all pods scale up on pool-2, never on FA-blocked pool-1).
		const recoveryLoops = 5
		for loop := 1; loop <= recoveryLoops; loop++ {
			podName := fmt.Sprintf("pod-recovery-%d", loop)
			p := tu.BuildTestPod(podName, 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
			infra.Fakes.K8s.AddPod(p)

			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPod, getErr := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
			assert.NoError(t, getErr)
			assert.Contains(t, updatedPod.Spec.NodeName, "pool-2", "recovery loop %d: expected %s to schedule on FA-recommended pool-2 (not pool-1)", loop, podName)
			assert.Equal(t, initialMetricVal+float64(bypassRounds), getFlexAdvisorBypassAttemptsMetric(t), "recovery loop %d: expected no additional bypasses to be triggered", loop)
		}
	})
}

// TestFlexAdvisorRecommendationsBypass_TrackedSeparatelyPerComputeClass verifies that when scale-up processes
// multiple ComputeClasses across consecutive loops, blocked scale-up counters and bypasses are tracked
// independently for each ComputeClass:
//   - Loops 1-3: ccc-1 has no scale-up options (reaches bypass threshold for ccc-1).
//   - Loops 4-5: ccc-2 has no scale-up options (2 blocked loops for ccc-2; bypass not triggered).
//   - Loop 6: ccc-1 triggers bypass and scales up pool-ccc-1.
//   - Loop 7: ccc-2 has no scale-up options (3rd blocked loop -> reaches bypass threshold for ccc-2).
//   - Loop 8: ccc-2 triggers bypass and scales up pool-ccc-2.
func TestFlexAdvisorRecommendationsBypass_TrackedSeparatelyPerComputeClass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		ccc1 := ccc.NewComputeClassBuilder("ccc-1").WithNodePoolsRules("pool-ccc-1").Build()
		ccc2 := ccc.NewComputeClassBuilder("ccc-2").WithNodePoolsRules("pool-ccc-2").Build()
		nodePools := []*gke_api_beta.NodePool{
			integration.EmptyNodePool("pool-ccc-1").WithMachineType(ZeroCapacityRecommendationMachineType).WithCCCLabel("ccc-1").Build(),
			integration.EmptyNodePool("pool-ccc-2").WithMachineType(ZeroCapacityRecommendationMachineType).WithCCCLabel("ccc-2").Build(),
		}

		testConfig := integration.NewTestConfig().
			WithNodePools(nodePools...).
			WithCccCrds(ccc1, ccc2).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithFlexAdvisorEnabled(),
			)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fake.NewGuidance(ZeroCapacityRecommendationMachineType).WithCapacity(0),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "ccc-1")
		PrimeFlexAdvisorCache(ctx, t, autoscaler, infra, "ccc-2")
		initialMetricVal := getFlexAdvisorBypassAttemptsMetric(t)

		podCcc1 := tu.BuildTestPod("pod-ccc-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("ccc-1"))
		podCcc2 := tu.BuildTestPod("pod-ccc-2", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("ccc-2"))

		// Loops 1-3: ccc-1 has no scale-up options (3 blocked loops -> reaches bypass threshold for ccc-1).
		infra.Fakes.K8s.AddPod(podCcc1)
		for loop := 1; loop <= 3; loop++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPod1, getErr := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-ccc-1", metav1.GetOptions{})
			assert.NoError(t, getErr)
			assert.Empty(t, updatedPod1.Spec.NodeName, "loop %d: expected pod-ccc-1 to remain unschedulable", loop)
			assert.Equal(t, initialMetricVal, getFlexAdvisorBypassAttemptsMetric(t), "loop %d: expected bypass metric not to increment", loop)
		}

		// Loops 4-5: Process ccc-2 instead of ccc-1. ccc-2 gets 2 blocked loops and must NOT trigger bypass
		// even though ccc-1 already reached its bypass threshold.
		infra.Fakes.K8s.DeletePod(podCcc1.Namespace, podCcc1.Name)
		infra.Fakes.K8s.AddPod(podCcc2)
		for loop := 4; loop <= 5; loop++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPod2, getErr := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-ccc-2", metav1.GetOptions{})
			assert.NoError(t, getErr)
			assert.Empty(t, updatedPod2.Spec.NodeName, "loop %d: expected pod-ccc-2 to remain unschedulable", loop)
			assert.Equal(t, initialMetricVal, getFlexAdvisorBypassAttemptsMetric(t), "loop %d: expected bypass metric not to increment for ccc-2", loop)
		}

		// Loop 6: Process ccc-1 again -> bypass triggers for ccc-1 and schedules pod-ccc-1 on pool-ccc-1.
		infra.Fakes.K8s.DeletePod(podCcc2.Namespace, podCcc2.Name)
		infra.Fakes.K8s.AddPod(podCcc1)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod1, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-ccc-1", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, updatedPod1.Spec.NodeName, "pool-ccc-1", "loop 6: expected pod-ccc-1 to schedule on pool-ccc-1 via bypass")
		assert.Equal(t, initialMetricVal+1, getFlexAdvisorBypassAttemptsMetric(t), "loop 6: expected bypass metric to increment by 1 for ccc-1")

		// Loop 7: Process ccc-2 again -> 3rd blocked loop for ccc-2 (no scale-up, reaches bypass threshold for ccc-2).
		infra.Fakes.K8s.AddPod(podCcc2)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod2, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-ccc-2", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod2.Spec.NodeName, "loop 7: expected pod-ccc-2 to remain unschedulable on its 3rd blocked loop")
		assert.Equal(t, initialMetricVal+1, getFlexAdvisorBypassAttemptsMetric(t), "loop 7: expected bypass metric not to increment yet for ccc-2")

		// Loop 8: Process ccc-2 -> bypass triggers for ccc-2 and schedules pod-ccc-2 on pool-ccc-2.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Minute)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod2, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-ccc-2", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, updatedPod2.Spec.NodeName, "pool-ccc-2", "loop 8: expected pod-ccc-2 to schedule on pool-ccc-2 via bypass")
		assert.Equal(t, initialMetricVal+2, getFlexAdvisorBypassAttemptsMetric(t), "loop 8: expected bypass metric to increment for ccc-2")
	})
}
