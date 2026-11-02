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

package recommendations

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	fakegce "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	fakeflexadvisor "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	testMachineType = "n1-standard-4"
	zoneA           = "us-central1-a"
	zoneB           = "us-central1-b"
)

func gceRefForTest(name, zone string) gce.GceRef {
	return gce.GceRef{
		Project: integration.DefaultProject(),
		Zone:    zone,
		Name:    name,
	}
}

// TestFlexAdvisorRecommendationsTracking verifies end-to-end recommendations tracking
// when scale-up is driven by Flex Advisor (FA).
func TestFlexAdvisorRecommendationsTracking(t *testing.T) {
	for name, tc := range map[string]struct {
		trackingEnabled          bool
		expectedInjectedCount    float64
		expectedExtractedCount   float64
		expectedRecordedRecToken string
	}{
		"Impact tracking enabled attaches recommendation token to CreateInstances payload": {
			trackingEnabled:          true,
			expectedInjectedCount:    1,
			expectedExtractedCount:   1,
			expectedRecordedRecToken: "1/test-fa-rec-123//1",
		},
		"Impact tracking disabled does not attach recommendation token": {
			trackingEnabled:          false,
			expectedInjectedCount:    0,
			expectedExtractedCount:   0,
			expectedRecordedRecToken: "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cccObj := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-fa").Build()
			nodePool := integration.DefaultNodePool(
				integration.WithNodePoolName("pool-fa"),
				integration.WithNodePoolMachineType(testMachineType),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations(zoneA, zoneB),
				integration.WithNodePoolLocationPolicy("BALANCED"),
				integration.WithNodePoolCCCLabel(cccObj.Name),
			)

			overrides := []integration.Option[*options.AutoscalingOptions]{
				integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
				integration.WithBalanceSimilarNodeGroups(),
				integration.WithFlexAdvisorEnabled(),
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePool).
				WithCccCrds(cccObj).
				WithOverrides(overrides...)

			if tc.trackingEnabled {
				testConfig.WithExperiments(experiments.DemandFungibilityImpactTrackingMinCAVersionFlag)
			} else {
				testConfig.WithExperimentOverrides(map[string]bool{experiments.DemandFungibilityImpactTrackingEnabledFlag: false}, nil)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				// Zone A has capacity with guidance ID and spec key, Zone B is unavailable
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
					fakeflexadvisor.NewGuidance(testMachineType).
						WithZone(zoneA).
						WithCapacity(10).
						WithScore(0.8).
						WithGuidanceId("test-fa-rec-123").
						WithSpecKey("1"),
					fakeflexadvisor.NewGuidance(testMachineType).
						WithZone(zoneB).
						WithCapacity(0).
						WithScore(0.8),
				)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// When: A pod requiring 1 node is created
				testPod := tu.BuildTestPod("pod-fa", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
				infra.Fakes.K8s.AddPod(testPod)

				// Autoscaler loop runs
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				// Then: Pod is scheduled on Zone A
				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-fa", metav1.GetOptions{})
				assert.NoError(t, err)
				assert.NotEmpty(t, updatedPod.Spec.NodeName, "Pod should be scheduled")

				migRefA := gceRefForTest("pool-fa", zoneA)
				recordedRecs := infra.Fakes.GceService.GetRecordedRecommendations(migRefA)
				if tc.expectedRecordedRecToken != "" {
					assert.Equal(t, []string{tc.expectedRecordedRecToken}, recordedRecs, "Expected recommendation token to match")
				} else {
					assert.Empty(t, recordedRecs, "Expected no recommendation token when tracking is disabled")
				}

				// Verify observability metrics
				injectedCount, err := internalmetrics.GetDemandFungibilityInjectedCountForTest(internalmetrics.FA)
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedInjectedCount, injectedCount, "Injected FA metrics count mismatch")

				extractedCount, err := internalmetrics.GetDemandFungibilityExtractedCountForTest(internalmetrics.FA)
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedExtractedCount, extractedCount, "Extracted FA metrics count mismatch")
			})
		})
	}
}

// TestRecommendLocationsRecommendationsTracking verifies end-to-end recommendations tracking
// when scale-up is driven by Recommend Locations Advisor (RLA) for LocationPolicy ANY pools.
func TestRecommendLocationsRecommendationsTracking(t *testing.T) {
	for name, tc := range map[string]struct {
		flexAdvisorEnabled       bool
		trackingEnabled          bool
		expectedInjectedCount    float64
		expectedExtractedCount   float64
		expectedRecordedRecToken string
	}{
		"RLA standalone with tracking enabled attaches token": {
			flexAdvisorEnabled:       false,
			trackingEnabled:          true,
			expectedInjectedCount:    1,
			expectedExtractedCount:   1,
			expectedRecordedRecToken: "1/test-rla-rec-456//recommend-locations-nodes",
		},
		"RLA nested in FA with tracking enabled propagates RLA token": {
			flexAdvisorEnabled:       true,
			trackingEnabled:          true,
			expectedInjectedCount:    1,
			expectedExtractedCount:   1,
			expectedRecordedRecToken: "1/test-rla-rec-456//recommend-locations-nodes",
		},
		"RLA with tracking disabled does not attach token": {
			flexAdvisorEnabled:       false,
			trackingEnabled:          false,
			expectedInjectedCount:    0,
			expectedExtractedCount:   0,
			expectedRecordedRecToken: "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			nodePool := integration.DefaultNodePool(
				integration.WithNodePoolName("pool-any"),
				integration.WithNodePoolMachineType(testMachineType),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations(zoneA, zoneB),
				integration.WithNodePoolLocationPolicy("ANY"),
			)

			overrides := []integration.Option[*options.AutoscalingOptions]{
				integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
				integration.WithBalanceSimilarNodeGroups(),
			}
			if tc.flexAdvisorEnabled {
				overrides = append(overrides, integration.WithFlexAdvisorEnabled())
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePool).
				WithOverrides(overrides...)

			if tc.trackingEnabled {
				testConfig.WithExperiments(experiments.DemandFungibilityImpactTrackingMinCAVersionFlag)
			} else {
				testConfig.WithExperimentOverrides(map[string]bool{experiments.DemandFungibilityImpactTrackingEnabledFlag: false}, nil)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				// Configure Fake RecommendLocationsClient
				infra.Fakes.RecommendLocationsClient.SetRecommendationID("test-rla-rec-456")
				infra.Fakes.RecommendLocationsClient.SetSpecKey("recommend-locations-nodes")

				if tc.flexAdvisorEnabled {
					// FA provides guidance for both zones so it doesn't filter them out during binpacking
					infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
						fakeflexadvisor.NewGuidance(testMachineType).WithZone(zoneA).WithCapacity(10).WithScore(0.5),
						fakeflexadvisor.NewGuidance(testMachineType).WithZone(zoneB).WithCapacity(10).WithScore(0.5),
					)
				}

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// When: A pod requiring 1 node is created
				testPod := tu.BuildTestPod("pod-any", 3000, 12000, tu.MarkUnschedulable())
				infra.Fakes.K8s.AddPod(testPod)

				// Autoscaler loop runs
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				// Then: Pod is scheduled
				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-any", metav1.GetOptions{})
				assert.NoError(t, err)
				assert.NotEmpty(t, updatedPod.Spec.NodeName, "Pod should be scheduled")

				// Check recorded recommendations across both zones
				migRefA := gceRefForTest("pool-any", zoneA)
				migRefB := gceRefForTest("pool-any", zoneB)
				recordedRecsA := infra.Fakes.GceService.GetRecordedRecommendations(migRefA)
				recordedRecsB := infra.Fakes.GceService.GetRecordedRecommendations(migRefB)
				allRecorded := append(recordedRecsA, recordedRecsB...)

				if tc.expectedRecordedRecToken != "" {
					assert.Contains(t, allRecorded, tc.expectedRecordedRecToken, "Expected RLA recommendation token to be recorded")
				} else {
					assert.Empty(t, allRecorded, "Expected no recommendation token when tracking is disabled")
				}

				// Verify observability metrics
				injectedCount, err := internalmetrics.GetDemandFungibilityInjectedCountForTest(internalmetrics.RLA)
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedInjectedCount, injectedCount, "Injected RLA metrics count mismatch")

				extractedCount, err := internalmetrics.GetDemandFungibilityExtractedCountForTest(internalmetrics.RLA)
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedExtractedCount, extractedCount, "Extracted RLA metrics count mismatch")
			})
		})
	}
}

// TestRecommendationsTracking_Safeguard_LoopStartClear verifies Safeguard 3 (Layer 2 Top-of-Loop Safety Net):
// Unpopped recommendations left in GkeManager are cleared at the start of each CA loop (in Refresh()).
func TestRecommendationsTracking_Safeguard_LoopStartClear(t *testing.T) {
	nodePool := integration.DefaultNodePool(
		integration.WithNodePoolName("pool-safeguard"),
		integration.WithNodePoolMachineType(testMachineType),
		integration.WithNodePoolSize(1),
		integration.WithNodePoolLocations(zoneA),
	)

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePool).
		WithExperiments(experiments.DemandFungibilityImpactTrackingMinCAVersionFlag)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Simulate an unpopped recommendation left in provider (e.g. from an aborted loop)
		staleRec := gke.ScaleUpRecommendation{
			RecommendationId: "stale-rec-id",
			SpecKey:          "stale-spec-key",
			Source:           internalmetrics.FA,
		}

		groups := autoscaler.CloudProvider.NodeGroups(context.Background())
		assert.NotEmpty(t, groups)
		gkeGroup, ok := groups[0].(gke.NodeGroup)
		assert.True(t, ok)
		gkeMig := gkeGroup.GetMig()

		// Staging recommendation on the MIG
		gkeMig.SetRecommendation(staleRec)

		// Verify the recommendation is staged
		popped, ok := gkeMig.PopRecommendation()
		assert.True(t, ok)
		assert.Equal(t, "stale-rec-id", popped.RecommendationId)

		// Stage it again before running an autoscaler loop
		gkeMig.SetRecommendation(staleRec)

		// Run an autoscaler loop. The loop begins with CloudProvider.Refresh(), which calls ClearRecommendations()
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Verify that the stale recommendation was scrubbed at loop start and is no longer present
		_, ok = gkeMig.PopRecommendation()
		assert.False(t, ok, "Stale recommendation should have been cleared by Refresh() at loop start")
	})
}

// TestRecommendLocationsRecommendationsTracking_FallbackOnError verifies that when RLA errors out,
// CA falls back cleanly to standard balancing without leaking any recommendation tokens.
func TestRecommendLocationsRecommendationsTracking_FallbackOnError(t *testing.T) {
	nodePool := integration.DefaultNodePool(
		integration.WithNodePoolName("pool-fallback"),
		integration.WithNodePoolMachineType(testMachineType),
		integration.WithNodePoolSize(0),
		integration.WithNodePoolLocations(zoneA, zoneB),
		integration.WithNodePoolLocationPolicy("ANY"),
	)

	overrides := []integration.Option[*options.AutoscalingOptions]{
		integration.WithMaxMemoryTotal(140 * 1024 * 1024 * 1024),
		integration.WithBalanceSimilarNodeGroups(),
	}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePool).
		WithExperiments(experiments.DemandFungibilityImpactTrackingMinCAVersionFlag).
		WithOverrides(overrides...)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Simulate RLA API error
		infra.Fakes.RecommendLocationsClient.WithError(assert.AnError)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		testPod := tu.BuildTestPod("pod-fallback", 3000, 12000, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(testPod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Pod is scheduled via standard balancing fallback
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-fallback", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Pod should be scheduled via fallback")

		// Verify no recommendation tokens were attached
		migRefA := gceRefForTest("pool-fallback", zoneA)
		migRefB := gceRefForTest("pool-fallback", zoneB)
		assert.Empty(t, infra.Fakes.GceService.GetRecordedRecommendations(migRefA))
		assert.Empty(t, infra.Fakes.GceService.GetRecordedRecommendations(migRefB))

		// Injected count should be 0
		injectedCount, err := internalmetrics.GetDemandFungibilityInjectedCountForTest(internalmetrics.RLA)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), injectedCount)
	})
}

// TestFlexAdvisorRecommendationsTracking_AsyncNAP verifies that FA
// recommendations are preserved across async NAP node pool creation (surviving
// intermediate Refresh() loops) and attached to
// CreateInstancesWithRecommendation on the newly created MIG.
func TestFlexAdvisorRecommendationsTracking_AsyncNAP(t *testing.T) {
	cccObj := ccc.NewComputeClassBuilder("nap-fa-ccc").
		WithNapEnabled().
		WithPriorities(v1.Priority{
			MachineType: new(testMachineType),
		}).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cccObj).
		WithExperiments(experiments.DemandFungibilityImpactTrackingMinCAVersionFlag).
		WithClusterOverrides(
			integration.WithAutoprovisioningLocations(zoneA, zoneB),
			integration.WithClusterAutoProvisioningEnabled(),
		).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithBalanceSimilarNodeGroups(),
			integration.WithFlexAdvisorEnabled(),
			integration.WithAutoProvisioningEnabled(),
			integration.WithHighThroughputNAPEnabled(10, 100),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fakeflexadvisor.NewGuidance(testMachineType).
				WithZone(zoneA).
				WithCapacity(10).
				WithScore(0.8).
				WithGuidanceId("test-fa-nap-rec-789").
				WithSpecKey("nap-spec-1"),
			fakeflexadvisor.NewGuidance(testMachineType).
				WithZone(zoneB).
				WithCapacity(10).
				WithScore(0.2),
		)
		infra.Fakes.GkeService.DisableOperationDelay(true)
		infra.Fakes.GkeService.SetCreateNodePoolDelay(30 * time.Second)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Loop 1: Pod triggers async NAP node pool creation.
		testPod := tu.BuildTestPod("pod-nap-fa", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("nap-fa-ccc"))
		infra.Fakes.K8s.AddPod(testPod)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Loop 2 (at +10s, while the 30s GKE CreateNodePool op is in progress):
		// CloudProvider.Refresh() runs ClearRecommendations(), which must not
		// wipe the upcoming node pool's recommendation.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		// Loop 3 (at +30s, after GKE CreateNodePool completes and
		// InitializeNodeGroup runs CreateInstances on the real MIG):
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		// Loop 4: CloudProvider.Refresh() drops the processed upcoming node
		// pool and exposes the real created MIGs.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-nap-fa", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Pod should be scheduled on the newly created NAP node pool")

		var allRecorded []string
		for _, ng := range autoscaler.CloudProvider.NodeGroups(ctx) {
			if gkeGroup, ok := ng.(gke.NodeGroup); ok {
				allRecorded = append(allRecorded, infra.Fakes.GceService.GetRecordedRecommendations(gkeGroup.GetMig().GceRef())...)
			}
		}
		assert.Equal(t, []string{"1/test-fa-nap-rec-789//nap-spec-1"}, allRecorded, "Expected FA recommendation token to be recorded on async-created NAP MIG")

		injectedCount, err := internalmetrics.GetDemandFungibilityInjectedCountForTest(internalmetrics.FA)
		assert.NoError(t, err)
		assert.Equal(t, float64(1), injectedCount, "Injected FA metrics count mismatch")

		// FA's background availability cache refreshes poll FetchCapacity
		// multiple times across the 42s synctest window.
		extractedCount, err := internalmetrics.GetDemandFungibilityExtractedCountForTest(internalmetrics.FA)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, extractedCount, float64(1), "Extracted FA metrics count should be at least 1")
	})
}

// TestRecommendLocationsRecommendationsTracking_AsyncNAP verifies that RLA
// recommendations are preserved across async NAP node pool creation (surviving
// intermediate Refresh() loops) and attached to
// CreateInstancesWithRecommendation on the newly created MIG.
func TestRecommendLocationsRecommendationsTracking_AsyncNAP(t *testing.T) {
	cccObj := ccc.NewComputeClassBuilder("nap-rla-ccc").
		WithNapEnabled().
		WithPriorities(v1.Priority{
			MachineType: new(testMachineType),
			Spot:        new(true),
		}).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cccObj).
		WithExperiments(experiments.DemandFungibilityImpactTrackingMinCAVersionFlag).
		WithClusterOverrides(
			integration.WithAutoprovisioningLocations(zoneA, zoneB),
			integration.WithClusterAutoProvisioningEnabled(),
		).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithBalanceSimilarNodeGroups(),
			integration.WithAutoProvisioningEnabled(),
			integration.WithHighThroughputNAPEnabled(10, 100),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.RecommendLocationsClient.SetRecommendationID("test-rla-nap-rec-999")
		infra.Fakes.RecommendLocationsClient.SetSpecKey("rla-nap-spec")
		infra.Fakes.GkeService.DisableOperationDelay(true)
		infra.Fakes.GkeService.SetCreateNodePoolDelay(30 * time.Second)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Loop 1: Pod triggers async NAP Spot node pool creation
		// (LocationPolicy ANY -> balanced by RLA).
		testPod := tu.BuildTestPod("pod-nap-rla", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("nap-rla-ccc"))
		infra.Fakes.K8s.AddPod(testPod)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Loop 2 (at +10s, while the 30s GKE CreateNodePool op is in progress):
		// CloudProvider.Refresh() runs ClearRecommendations(), which must not
		// wipe the upcoming node pool's recommendation.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		// Loop 3 (at +30s, after GKE CreateNodePool completes and
		// InitializeNodeGroup runs CreateInstances on the real MIG):
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		// Loop 4: CloudProvider.Refresh() drops the processed upcoming node
		// pool and exposes the real created MIGs.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-nap-rla", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Pod should be scheduled on the newly created NAP node pool")

		var allRecorded []string
		for _, ng := range autoscaler.CloudProvider.NodeGroups(ctx) {
			if gkeGroup, ok := ng.(gke.NodeGroup); ok {
				allRecorded = append(allRecorded, infra.Fakes.GceService.GetRecordedRecommendations(gkeGroup.GetMig().GceRef())...)
			}
		}
		assert.Equal(t, []string{"1/test-rla-nap-rec-999//rla-nap-spec"}, allRecorded, "Expected RLA recommendation token to be recorded on async-created NAP MIG")

		injectedCount, err := internalmetrics.GetDemandFungibilityInjectedCountForTest(internalmetrics.RLA)
		assert.NoError(t, err)
		assert.Equal(t, float64(1), injectedCount, "Injected RLA metrics count mismatch")

		extractedCount, err := internalmetrics.GetDemandFungibilityExtractedCountForTest(internalmetrics.RLA)
		assert.NoError(t, err)
		assert.Equal(t, float64(1), extractedCount, "Extracted RLA metrics count mismatch")
	})
}

// TestFlexAdvisorRecommendationsTracking_AsyncNAP_Salvo verifies that multiple
// async NAP scale-ups within a single Salvo loop each preserve their own FA
// recommendation across inter-pass clearing and intermediate Refresh() loops.
func TestFlexAdvisorRecommendationsTracking_AsyncNAP_Salvo(t *testing.T) {
	const secondMachineType = "n2-standard-4"

	cccObj := ccc.NewComputeClassBuilder("nap-salvo-ccc").
		WithNapEnabled().
		WithPriorities(
			v1.Priority{MachineType: new(testMachineType)},
			v1.Priority{MachineType: new(secondMachineType)},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cccObj).
		WithExperiments(experiments.DemandFungibilityImpactTrackingMinCAVersionFlag).
		WithClusterOverrides(
			integration.WithAutoprovisioningLocations(zoneA, zoneB),
			integration.WithClusterAutoProvisioningEnabled(),
		).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithBalanceSimilarNodeGroups(),
			integration.WithFlexAdvisorEnabled(),
			integration.WithAutoProvisioningEnabled(),
			integration.WithHighThroughputNAPEnabled(10, 100),
			func(o *options.AutoscalingOptions) *options.AutoscalingOptions {
				o.SalvoScaleUp = true
				o.SalvoScaleUpBudget = time.Minute
				return o
			},
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Priority 1 (n1-standard-4) has capacity for 1 node per zone (2
		// total), so Salvo pass 1 creates an n1-standard-4 async NAP pool
		// across both zones for 2 pods and pass 2 falls through to Priority 2
		// (n2-standard-4) for the third pod. Both zones are given non-zero
		// capacity so temporary-MIG zone selection stays deterministic.
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fakeflexadvisor.NewGuidance(testMachineType).
				WithZone(zoneA).
				WithCapacity(1).
				WithScore(0.8).
				WithGuidanceId("test-fa-salvo-rec-1").
				WithSpecKey("salvo-spec-1"),
			fakeflexadvisor.NewGuidance(testMachineType).
				WithZone(zoneB).
				WithCapacity(1).
				WithScore(0.2).
				WithGuidanceId("test-fa-salvo-rec-1").
				WithSpecKey("salvo-spec-1"),
			fakeflexadvisor.NewGuidance(secondMachineType).
				WithZone(zoneA).
				WithCapacity(10).
				WithScore(0.8).
				WithGuidanceId("test-fa-salvo-rec-2").
				WithSpecKey("salvo-spec-2"),
			fakeflexadvisor.NewGuidance(secondMachineType).
				WithZone(zoneB).
				WithCapacity(10).
				WithScore(0.2).
				WithGuidanceId("test-fa-salvo-rec-2").
				WithSpecKey("salvo-spec-2"),
		)
		infra.Fakes.GkeService.DisableOperationDelay(true)
		infra.Fakes.GkeService.SetCreateNodePoolDelay(30 * time.Second)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Loop 1: Three pods in the same CCC shard trigger two Salvo passes
		// and initiate two async NAP node pools within a single CA loop.
		for _, podName := range []string{"pod-salvo-1", "pod-salvo-2", "pod-salvo-3"} {
			p := tu.BuildTestPod(podName, 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("nap-salvo-ccc"))
			p.UID = types.UID(podName)
			infra.Fakes.K8s.AddPod(p)
		}
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Confirm both async NAP pools were initiated in Loop 1 by Salvo.
		upcomingPools := make(map[string]bool)
		for _, ng := range autoscaler.CloudProvider.NodeGroups(ctx) {
			if gkeGroup, ok := ng.(gke.NodeGroup); ok {
				upcomingPools[gkeGroup.GetMig().NodePoolName()] = true
			}
		}
		assert.Len(t, upcomingPools, 2, "Expected Salvo to initiate 2 async NAP pools in a single loop")

		// Loop 2 (at +10s, while both 30s GKE CreateNodePool ops are running):
		// Refresh() clears GkeManager recommendations without wiping either
		// upcoming node pool's recommendation.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		// Loop 3 (at +30s, after both GKE CreateNodePool ops complete and
		// InitializeNodeGroup runs CreateInstances on both real MIGs):
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		// Loop 4: Refresh() exposes the real created MIGs.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		for _, podName := range []string{"pod-salvo-1", "pod-salvo-2", "pod-salvo-3"} {
			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected %s to be scheduled", podName)
		}

		cluster := integration.GetTestCluster(t, infra, testConfig)
		assert.Len(t, cluster.NodePools, 2, "Expected 2 created NAP node pools")

		recordedByMachineType := make(map[string][]string)
		for _, np := range cluster.NodePools {
			for _, zone := range []string{zoneA, zoneB} {
				recs := infra.Fakes.GceService.GetRecordedRecommendations(gceRefForTest(np.Name, zone))
				recordedByMachineType[np.Config.MachineType] = append(recordedByMachineType[np.Config.MachineType], recs...)
			}
		}
		assert.Equal(t, []string{
			"1/test-fa-salvo-rec-1//salvo-spec-1",
			"1/test-fa-salvo-rec-1//salvo-spec-1",
		}, recordedByMachineType[testMachineType])
		assert.Equal(t, []string{"1/test-fa-salvo-rec-2//salvo-spec-2"}, recordedByMachineType[secondMachineType])

		injectedCount, err := internalmetrics.GetDemandFungibilityInjectedCountForTest(internalmetrics.FA)
		assert.NoError(t, err)
		assert.Equal(t, float64(3), injectedCount, "Injected FA metrics count mismatch")
	})
}

type failFirstCreateInstancesGceClient struct {
	*fakegce.GceClient
	mu          sync.Mutex
	failedFirst bool
	failedZone  string
}

func (c *failFirstCreateInstancesGceClient) CreateInstancesWithRecommendation(ref gce.GceRef, templateName string, count int64, names []string, recommendation string) ([]string, error) {
	c.mu.Lock()
	if !c.failedFirst {
		c.failedFirst = true
		c.failedZone = ref.Zone
		c.mu.Unlock()
		return nil, assert.AnError
	}
	c.mu.Unlock()
	return c.GceClient.CreateInstancesWithRecommendation(ref, templateName, count, names, recommendation)
}

// TestRecommendationsTracking_Safeguard_PerPassClear verifies that unpopped
// recommendations left by a ScaleUp pass are cleared at the end of that pass
// (via ScaleUpEndProcessor) so subsequent scaling in the same loop (e.g.
// scaleUpMinNodes without an intervening Refresh()) cannot attach stale tokens.
func TestRecommendationsTracking_Safeguard_PerPassClear(t *testing.T) {
	cccObj := ccc.NewComputeClassBuilder("per-pass-ccc").
		WithNodePoolsRules("pool-per-pass").
		Build()
	nodePool := integration.DefaultNodePool(
		integration.WithNodePoolName("pool-per-pass"),
		integration.WithNodePoolMachineType(testMachineType),
		integration.WithNodePoolSize(0),
		integration.WithNodePoolMin(1),
		integration.WithNodePoolMax(5),
		integration.WithNodePoolLocations(zoneA, zoneB),
		integration.WithNodePoolLocationPolicy("BALANCED"),
		integration.WithNodePoolCCCLabel(cccObj.Name),
	)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		wrappedGce := &failFirstCreateInstancesGceClient{GceClient: infra.Fakes.GceService}
		testConfig := integration.NewTestConfig().
			WithNodePools(nodePool).
			WithCccCrds(cccObj).
			WithExperiments(experiments.DemandFungibilityImpactTrackingMinCAVersionFlag).
			WithGceClient(wrappedGce).
			WithOverrides(
				integration.WithMaxMemoryTotal(140*1024*1024*1024),
				integration.WithBalanceSimilarNodeGroups(),
				integration.WithFlexAdvisorEnabled(),
			)

		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(
			fakeflexadvisor.NewGuidance(testMachineType).
				WithZone(zoneA).
				WithCapacity(10).
				WithScore(0.8).
				WithGuidanceId("test-fa-per-pass-rec").
				WithSpecKey("per-pass-spec"),
			fakeflexadvisor.NewGuidance(testMachineType).
				WithZone(zoneB).
				WithCapacity(10).
				WithScore(0.2),
		)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Add two pods so FA balances across both zones and stages a
		// recommendation on both MIGs. When the first zone fails during
		// synchronous execution, the sibling zone's recommendation stays
		// unpopped until OnScaleUpEnd() clears it before scaleUpMinNodes runs.
		pod1 := tu.BuildTestPod("pod-per-pass-1", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("per-pass-ccc"))
		pod1.UID = types.UID("pod-per-pass-1")
		pod2 := tu.BuildTestPod("pod-per-pass-2", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("per-pass-ccc"))
		pod2.UID = types.UID("pod-per-pass-2")
		infra.Fakes.K8s.AddPod(pod1)
		infra.Fakes.K8s.AddPod(pod2)

		runErr := integration_synctest.RunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Error(t, runErr, "Expected RunOnce to report the simulated first-zone scale-up error")

		wrappedGce.mu.Lock()
		failedZone := wrappedGce.failedZone
		wrappedGce.mu.Unlock()
		assert.NotEmpty(t, failedZone, "Expected first zone CreateInstancesWithRecommendation to be attempted")

		otherZone := zoneB
		if failedZone == zoneB {
			otherZone = zoneA
		}
		otherMigRef := gceRefForTest("pool-per-pass", otherZone)

		// Confirm scaleUpMinNodes actually scaled the healthy sibling zone in
		// the same loop without attaching the stale FA recommendation.
		targetSize, err := infra.Fakes.GceService.FetchMigTargetSize(ctx, otherMigRef)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), targetSize, "Expected scaleUpMinNodes to scale healthy sibling zone %s to min size", otherZone)
		assert.Empty(t, infra.Fakes.GceService.GetRecordedRecommendations(otherMigRef),
			"Expected no stale FA recommendation on sibling zone %s scaled by scaleUpMinNodes", otherZone)
	})
}
