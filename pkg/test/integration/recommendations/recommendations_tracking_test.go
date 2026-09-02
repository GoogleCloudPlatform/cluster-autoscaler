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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
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
