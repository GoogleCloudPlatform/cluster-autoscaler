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

package dws

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	config "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	provreqtest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/provreq"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

func TestFlexStartTPU_NonQueued_NonNAP(t *testing.T) {
	testCases := []struct {
		name             string
		machineType      string
		topology         string
		acceleratorCount string
		maxNodeCount     int64
	}{
		{
			name:             "SingleHost",
			machineType:      "ct5lp-hightpu-1t",
			topology:         "1x1",
			acceleratorCount: "1",
			maxNodeCount:     1,
		},
		{
			name:             "MultiHost",
			machineType:      "ct5lp-hightpu-8t",
			topology:         "2x4",
			acceleratorCount: "8",
			maxNodeCount:     2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			np := integration.DefaultNodePool(
				integration.WithNodePoolName("flex-start-np"),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations(integration.DefaultZones[0]),
				integration.WithFlexStartNodePool,
				integration.WithTPUConfig(tc.machineType, "tpu-v5-lite-podslice", tc.topology, tc.acceleratorCount, tc.maxNodeCount),
			)
			testConfig := integration.NewTestConfig().
				WithExperiments(experiments.FlexStartNonQueuedEnabledFlag).
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
						{ResourceType: "cpu", Maximum: 1000},
						{ResourceType: "memory", Maximum: 10000000},
						{ResourceType: "tpu-v5-lite-podslice", Maximum: 100},
					}),
				).
				WithNodePools(np)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				p := tu.BuildTestPod("test-pod", 1000, 1000, tu.MarkUnschedulable(),
					pod.WithCreationTimestamp(metav1.Time{Time: time.Now().Add(-1 * time.Hour)}),
					pod.WithFlexStart(),
					pod.WithTPU("tpu-v5-lite-podslice", tc.topology, tc.acceleratorCount),
				)
				infra.Fakes.K8s.AddPod(p)

				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

				np = infra.Fakes.GkeService.MustGetNodePool(t, "flex-start-np")
				assert.Equal(t, int64(1), infra.Fakes.GkeService.MustGetTargetSize(t, np))
			})
		})
	}
}

func TestFlexStartTPU_NonQueued_NAP(t *testing.T) {
	testCases := []struct {
		name             string
		topology         string
		acceleratorCount string
	}{
		{
			name:             "SingleHost",
			topology:         "1x1",
			acceleratorCount: "1",
		},
		{
			name:             "MultiHost",
			topology:         "2x4",
			acceleratorCount: "8",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithExperiments(experiments.FlexStartNonQueuedEnabledFlag, experiments.FlexStartNonQueuedNAPEnabledFlag).
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
						o.InternalOptions.ProvisioningLabelEnabled = true
						return o
					},
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
						{ResourceType: "cpu", Maximum: 1000},
						{ResourceType: "memory", Maximum: 10000000},
						{ResourceType: "tpu-v5-lite-podslice", Maximum: 100},
					}),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				p := tu.BuildTestPod("test-pod", 1000, 1000, tu.MarkUnschedulable(),
					pod.WithCreationTimestamp(metav1.Time{Time: time.Now().Add(-1 * time.Hour)}),
					pod.WithFlexStart(),
					pod.WithTPU("tpu-v5-lite-podslice", tc.topology, tc.acceleratorCount),
				)
				infra.Fakes.K8s.AddPod(p)

				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

				np := infra.Fakes.GkeService.MustGetSingleAutoprovisionedNodePool(t)

				assert.NotNil(t, np.Autoscaling)
				assert.Equal(t, "", np.Autoscaling.LocationPolicy)

				assert.NotNil(t, np.Config)
				assert.NotNil(t, np.Config.ReservationAffinity)
				assert.Equal(t, "NO_RESERVATION", np.Config.ReservationAffinity.ConsumeReservationType)

				foundTaint := false
				for _, taint := range np.Config.Taints {
					if taint.Key == "cloud.google.com/gke-flex-start" && taint.Value == "true" && taint.Effect == "NO_SCHEDULE" {
						foundTaint = true
					}
					assert.NotEqual(t, "cloud.google.com/gke-spot", taint.Key, "Should not have spot taint")
					assert.NotEqual(t, "cloud.google.com/gke-preemptible", taint.Key, "Should not have preemptible taint")
				}
				assert.True(t, foundTaint, "Expected cloud.google.com/gke-flex-start taint")

				assert.Equal(t, int64(1), infra.Fakes.GkeService.MustGetTargetSize(t, np))
			})
		})
	}
}

func TestFlexStartTPU_Queued_NAP(t *testing.T) {
	testCases := []struct {
		name             string
		machineType      string
		topology         string
		acceleratorCount string
	}{
		{
			name:             "SingleHost",
			machineType:      "ct5lp-hightpu-1t",
			topology:         "1x1",
			acceleratorCount: "1",
		},
		{
			name:             "MultiHost",
			machineType:      "ct5lp-hightpu-8t",
			topology:         "2x4",
			acceleratorCount: "8",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dummyPod := tu.BuildTestPod("dummy", 0, 0,
				pod.WithFlexStart(),
				pod.WithTPU("tpu-v5-lite-podslice", tc.topology, tc.acceleratorCount),
			)

			prw := provreqtest.BuildTestProvisioningRequest("default", "test-pr", map[string]string{
				"cloud.google.com/gke-capacity-check-wait-time-seconds": "0",
			}, dummyPod.Spec)

			testConfig := integration.NewTestConfig().
				WithExperiments(experiments.FlexStartNonQueuedEnabledFlag, experiments.FlexStartNonQueuedNAPEnabledFlag).
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
						o.InternalOptions.ProvisioningLabelEnabled = true
						return o
					},
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
						{ResourceType: "cpu", Maximum: 1000},
						{ResourceType: "memory", Maximum: 10000000},
						{ResourceType: "tpu-v5-lite-podslice", Maximum: 100},
					}),
				).
				WithProvisioningRequests(prw)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				for _, zone := range integration.DefaultZones {
					infra.Fakes.GceService.SetBackendMachineCount(zone, tc.machineType, 0)
				}

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				p := tu.BuildTestPod("test-pod", 1000, 1000, tu.MarkUnschedulable(),
					pod.WithCreationTimestamp(metav1.Time{Time: time.Now().Add(-1 * time.Hour)}),
					pod.WithFlexStart(),
					pod.WithTPU("tpu-v5-lite-podslice", tc.topology, tc.acceleratorCount),
					pod.WithAnnotations(map[string]string{
						"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "test-pr",
					}),
				)
				infra.Fakes.K8s.AddPod(p)

				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

				// The second loop advances the virtual clock by the scale-up failure timeout,
				// allowing the ProvisioningRequest Reconciler to mark the PR as Failed.
				// We use RunOnceAfter and ignore the error because the synchronous deletion from
				// fakeGCE causes a brief "not found" error in the CA node registry during the same loop.
				// This does not happen when using real Autoscaler, as the deletion there is asynchronous
				// and takes a few minutes, hence it never happens that a node pool is deleted in the same loop
				// as the CA is trying to read it.
				_ = integration_synctest.RunOnceAfter(t, autoscaler, reconciler.MaxObtainabilityStrategyUnreconciledPeriod)

				nps := infra.Fakes.GkeService.MustGetAutoprovisionedNodePools(t)
				np := nps[0]

				assert.NotNil(t, np.Autoscaling)
				assert.Equal(t, "ANY", np.Autoscaling.LocationPolicy)

				assert.NotNil(t, np.Config)
				assert.NotNil(t, np.Config.ReservationAffinity)
				assert.Equal(t, "NO_RESERVATION", np.Config.ReservationAffinity.ConsumeReservationType)

				foundTaint := false
				for _, taint := range np.Config.Taints {
					if taint.Key == "cloud.google.com/gke-flex-start" && taint.Value == "true" && taint.Effect == "NO_SCHEDULE" {
						foundTaint = true
					}
					assert.NotEqual(t, "cloud.google.com/gke-spot", taint.Key, "Should not have spot taint")
					assert.NotEqual(t, "cloud.google.com/gke-preemptible", taint.Key, "Should not have preemptible taint")
				}
				assert.True(t, foundTaint, "Expected cloud.google.com/gke-flex-start taint")

				assert.Equal(t, int64(0), infra.Fakes.GkeService.MustGetTargetSize(t, np))
			})
		})
	}
}
