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
	"maps"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	ccc_crd "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	ccc_rules "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

var registerOnce sync.Once

func TestFlexAdvisorResponseAnomalies(t *testing.T) {
	registerOnce.Do(metrics.RegisterAll)

	ccc := createCCCWithNodePoolsRules([]string{"pool-1"})
	nodePools := annotateNodePoolWithCCCLabel(ccc.Name(), []*gke_api_beta.NodePool{
		createEmptyNodePool("pool-1", AvailableMachineType),
	})

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithNpcCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	for name, tc := range map[string]struct {
		fakeGuidances  []fake.FakeCapacityGuidance
		modifier       func(results map[string]*api.InstanceAvailability, err error) (map[string]*api.InstanceAvailability, error)
		expectedMetric metrics.FAResponseErrorReason
	}{
		"missing_instance_config": {
			fakeGuidances: []fake.FakeCapacityGuidance{
				fake.NewFakeCapacityGuidanceForMachineType(AvailableMachineType, 10, 0.5),
			},
			modifier: func(results map[string]*api.InstanceAvailability, err error) (map[string]*api.InstanceAvailability, error) {
				newResults := maps.Clone(results)
				// remove first instance config
				for k := range newResults {
					delete(newResults, k)
					break
				}
				return newResults, err
			},
			expectedMetric: metrics.ResponseMissingInstanceConfig,
		},
		"missing_zone": {
			fakeGuidances: []fake.FakeCapacityGuidance{
				fake.NewFakeCapacityGuidanceForMachineType(AvailableMachineType, 10, 0.5),
			},
			modifier: func(results map[string]*api.InstanceAvailability, err error) (map[string]*api.InstanceAvailability, error) {
				newResults := maps.Clone(results)
				for k, ia := range newResults {
					newResults[k] = api.NewInstanceAvailability(
						ia.FlexibilityScopeKey(),
						ia.InstanceConfigKey(),
						ia.GuidanceId(),
						map[string]int{}, // missing zones
						map[string]float64{},
					)
					break // change first instance config only
				}
				return newResults, err
			},
			expectedMetric: metrics.ResponseMissingZone,
		},
		"invalid_instance_count": {
			fakeGuidances: []fake.FakeCapacityGuidance{
				fake.NewFakeCapacityGuidanceForMachineType(AvailableMachineType, -5, 0.5),
			},
			expectedMetric: metrics.InvalidInstanceCount,
		},
		"invalid_preference_score": {
			fakeGuidances: []fake.FakeCapacityGuidance{
				fake.NewFakeCapacityGuidanceForMachineType(AvailableMachineType, 10, 1.5),
			},
			expectedMetric: metrics.InvalidPreferenceScore,
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(tc.fakeGuidances...)

				// Inject targeted anomaly into the fake GCE FlexAdvisor backend via custom modifier
				if tc.modifier != nil {
					infra.Fakes.FlexAdvisorClient.SetCapacityGuidanceResponseModifier(tc.modifier)
				}

				stopCh := make(chan struct{})
				autoscaler, err := integration.SetupAutoscaler(t, ctx, testConfig, infra, stopCh)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel, stopCh)

				// Trigger an autoscaling loop
				pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), withTestCCC)
				infra.Fakes.K8s.AddPod(pod)

				// Run one autoscaler loop
				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				// Assert that the corresponding anomaly metric was incremented
				val, err := metrics.GetFlexAdvisorResponseErrorsCountForTest(tc.expectedMetric)
				assert.NoError(t, err)
				assert.Equal(t, float64(1), val, "Expected metric %q to be incremented to 1", tc.expectedMetric)
			})
		})
	}
}

func TestFlexAdvisorMultipleMissingZones(t *testing.T) {
	registerOnce.Do(metrics.RegisterAll)

	ccc := createCCCWithNodePoolsRules([]string{"pool-1"})
	nodePools := annotateNodePoolWithCCCLabel(ccc.Name(), []*gke_api_beta.NodePool{
		createEmptyNodePool("pool-1", AvailableMachineType),
	})

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithNpcCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(testCapacityGuidance()...)

		// Set a modifier that clears all zones to simulate multiple missing zones
		infra.Fakes.FlexAdvisorClient.SetCapacityGuidanceResponseModifier(func(results map[string]*api.InstanceAvailability, err error) (map[string]*api.InstanceAvailability, error) {
			newResults := make(map[string]*api.InstanceAvailability)
			for k, ia := range results {
				newResults[k] = api.NewInstanceAvailability(
					ia.FlexibilityScopeKey(),
					ia.InstanceConfigKey(),
					ia.GuidanceId(),
					map[string]int{}, // remove zones from result
					map[string]float64{},
				)
				break // update first result only
			}
			return newResults, err
		})

		beforeVal, err := metrics.GetFlexAdvisorResponseErrorsCountForTest(metrics.ResponseMissingZone)
		assert.NoError(t, err)

		stopCh := make(chan struct{})
		autoscaler, err := integration.SetupAutoscaler(t, ctx, testConfig, infra, stopCh)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel, stopCh)

		// Trigger an autoscaling loop
		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), withTestCCC)
		infra.Fakes.K8s.AddPod(pod)

		// Run one autoscaler loop
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		afterVal, err := metrics.GetFlexAdvisorResponseErrorsCountForTest(metrics.ResponseMissingZone)
		assert.NoError(t, err)

		assert.Equal(t, beforeVal+1, afterVal, "Expected missing zones to be reported exactly once across the entire response")
	})
}

func TestFlexAdvisorGenerationAnomalies(t *testing.T) {
	registerOnce.Do(metrics.RegisterAll)

	// Create a CCC with a priority rule specifying an unknown machine type
	ccc := ccc_crd.NewTestCrd(
		ccc_crd.WithName("test-ccc"),
		ccc_crd.WithLabel(gkelabels.ComputeClassLabel),
		ccc_crd.WithRules([]ccc_rules.Rule{
			ccc_rules.NewRule(ccc_rules.WithMachineTypeRule(new("invalid-machine-type"))),
		}),
	)

	// Annotate empty node pool so it matches
	nodePools := annotateNodePoolWithCCCLabel(ccc.Name(), []*gke_api_beta.NodePool{
		createEmptyNodePool("pool-1", AvailableMachineType),
	})

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithNpcCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Get initial value of ZeroConfigsGeneratedForRule metric
		beforeVal, err := metrics.GetFlexAdvisorGenerationErrorsCountForTest(metrics.ZeroConfigsGeneratedForRule)
		assert.NoError(t, err)

		stopCh := make(chan struct{})
		autoscaler, err := integration.SetupAutoscaler(t, ctx, testConfig, infra, stopCh)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel, stopCh)

		// Trigger an autoscaling loop
		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), withTestCCC)
		infra.Fakes.K8s.AddPod(pod)

		// Run one autoscaler loop
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Assert that the ZeroConfigsGeneratedForRule metric was incremented
		afterVal, err := metrics.GetFlexAdvisorGenerationErrorsCountForTest(metrics.ZeroConfigsGeneratedForRule)
		assert.NoError(t, err)
		assert.Equal(t, beforeVal+1, afterVal, "Expected ZeroConfigsGeneratedForRule to be incremented by 1")
	})
}
