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
	gke_api_beta "google.golang.org/api/container/v1beta1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
)

func TestFlexAdvisorResponseAnomalies(t *testing.T) {

	ccc := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-1").
			WithMachineType(AvailableMachineType).
			WithLocations(ZoneB, ZoneC, ZoneF).
			WithCCCLabel(ccc.Name).
			Build(),
	}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(ccc).
		WithOverrides(
			integration.WithMaxMemoryTotal(140*1024*1024*1024),
			integration.WithFlexAdvisorEnabled(),
		)

	for name, tc := range map[string]struct {
		fakeGuidances  []fake.CapacityGuidance
		expectedMetric metrics.FAResponseErrorReason
	}{
		"missing_instance_config": {
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance(AvailableMachineType).WithOmit(true),
			},
			expectedMetric: metrics.ResponseMissingInstanceConfig,
		},
		"missing_zone": {
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance(AvailableMachineType).WithZone(ZoneB).WithOmit(true),
				fake.NewGuidance(AvailableMachineType).WithCapacity(10),
			},
			expectedMetric: metrics.ResponseMissingZone,
		},
		"invalid_instance_count": {
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance(AvailableMachineType).WithCapacity(-5),
			},
			expectedMetric: metrics.InvalidInstanceCount,
		},
		"invalid_preference_score": {
			fakeGuidances: []fake.CapacityGuidance{
				fake.NewGuidance(AvailableMachineType).WithCapacity(10).WithScore(1.5),
			},
			expectedMetric: metrics.InvalidPreferenceScore,
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.FlexAdvisorClient.AddCapacityGuidances(tc.fakeGuidances...)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Trigger an autoscaling loop
				pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
				infra.Fakes.K8s.AddPod(pod)

				// Run one autoscaler loop
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
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

	ccc := ccc.NewComputeClassBuilder("test-ccc").WithNodePoolsRules("pool-1").Build()
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-1").
			WithMachineType(AvailableMachineType).
			WithLocations(ZoneB, ZoneC, ZoneF).
			WithCCCLabel(ccc.Name).
			Build(),
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
			fake.NewGuidance(AvailableMachineType).WithZone(ZoneB).WithOmit(true),
			fake.NewGuidance(AvailableMachineType).WithZone(ZoneC).WithOmit(true),
			fake.NewGuidance(AvailableMachineType).WithCapacity(10),
		)

		beforeVal, err := metrics.GetFlexAdvisorResponseErrorsCountForTest(metrics.ResponseMissingZone)
		assert.NoError(t, err)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Trigger an autoscaling loop
		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod)

		// Run one autoscaler loop
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		afterVal, err := metrics.GetFlexAdvisorResponseErrorsCountForTest(metrics.ResponseMissingZone)
		assert.NoError(t, err)

		assert.Equal(t, beforeVal+1, afterVal, "Expected missing zones to be reported exactly once across the entire response")
	})
}

func TestFlexAdvisorGenerationAnomalies(t *testing.T) {

	cccObj := ccc.NewComputeClassBuilder("test-ccc").
		WithPriorities(v1.Priority{MachineType: ptr.To("invalid-machine-type")}).
		Build()

	// Annotate empty node pool so it matches
	nodePools := []*gke_api_beta.NodePool{
		integration.EmptyNodePool("pool-1").
			WithMachineType(AvailableMachineType).
			WithCCCLabel(cccObj.Name).
			Build(),
	}

	testConfig := integration.NewTestConfig().
		WithNodePools(nodePools...).
		WithCccCrds(cccObj).
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

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Trigger an autoscaling loop
		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable(), pod.WithCCC("test-ccc"))
		infra.Fakes.K8s.AddPod(pod)

		// Run one autoscaler loop
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		// Assert that the ZeroConfigsGeneratedForRule metric was incremented
		afterVal, err := metrics.GetFlexAdvisorGenerationErrorsCountForTest(metrics.ZeroConfigsGeneratedForRule)
		assert.NoError(t, err)
		assert.Equal(t, beforeVal+1, afterVal, "Expected ZeroConfigsGeneratedForRule to be incremented by 1")
	})
}
