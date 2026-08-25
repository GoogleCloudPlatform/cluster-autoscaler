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

package inmemory

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestSafeguard(t *testing.T) {
	testCases := []struct {
		name               string
		nodePools          []*gke_api_beta.NodePool
		podBuilder         func() *apiv1.Pod
		loopDuration       time.Duration
		expectedDefaultFOS float64
	}{
		{
			name: "Default FOS Triggered",
			nodePools: []*gke_api_beta.NodePool{
				integration.DefaultNodePool(
					integration.WithNodePoolName("node-1"),
					integration.WithNodePoolSize(1),
				),
			},
			podBuilder: func() *apiv1.Pod {
				pod := tu.BuildTestPod("my-pod", 500, 500, tu.MarkUnschedulable())
				pod.CreationTimestamp = metav1.Time{Time: time.Now().Add(-20 * time.Minute)}
				return pod
			},
			loopDuration:       11 * time.Minute,
			expectedDefaultFOS: 1,
		},
		{
			name: "Default FOS Young Pod Not Triggered",
			nodePools: []*gke_api_beta.NodePool{
				integration.DefaultNodePool(
					integration.WithNodePoolName("node-1"),
					integration.WithNodePoolSize(1),
				),
			},
			podBuilder: func() *apiv1.Pod {
				pod := tu.BuildTestPod("my-pod-young", 500, 500, tu.MarkUnschedulable())
				pod.CreationTimestamp = metav1.Time{Time: time.Now().Add(-5 * time.Minute)}
				return pod
			},
			loopDuration:       5 * time.Minute,
			expectedDefaultFOS: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().WithNodePools(tc.nodePools...)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				pod := tc.podBuilder()
				infra.Fakes.K8s.AddPod(pod)

				// Run CA loop once (populates falsePositiveFirstSeen)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Run CA loop again (triggers the safeguard if loopDuration is enough)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, tc.loopDuration)

				// Check default FOS metric
				defaultFOSVal, err := metrics.GetFilteredFalsePositiveSchedulablePodsEventsForTest(metrics.FilterOutSchedulable)
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedDefaultFOS, defaultFOSVal)

				// Check that PTS FOS metric remains 0
				ptsVal, err := metrics.GetFilteredFalsePositiveSchedulablePodsEventsForTest(metrics.TopologySpreadMutation)
				assert.NoError(t, err)
				assert.Equal(t, float64(0), ptsVal)
			})
		})
	}
}

// TestSafeguardJitterAndCleanup tests safeguard behavior across transient absences and long absence cleanups.
// An initial CA loop runs to register the pod in tracking state, then the pod is absent for absenceDuration,
// re-added to the cluster, and evaluated after reappearDelay.
func TestSafeguardJitterAndCleanup(t *testing.T) {
	testCases := []struct {
		name               string
		absenceDuration    time.Duration
		reappearDelay      time.Duration
		expectedDefaultFOS float64
	}{
		{
			name:               "Transient absence retains tracking - metric fires",
			absenceDuration:    5 * time.Minute,
			reappearDelay:      6 * time.Minute,
			expectedDefaultFOS: 1,
		},
		{
			name:               "Transient absence retains tracking - metric does not fire yet (below threshold)",
			absenceDuration:    5 * time.Minute,
			reappearDelay:      3 * time.Minute,
			expectedDefaultFOS: 0,
		},
		{
			name:            "Near-cleanup-boundary absence retains tracking - metric fires immediately",
			absenceDuration: 25 * time.Minute,
			reappearDelay:   1 * time.Minute,
			// The metric is a counter and increments on every CA loop where a pod exceeds the 10-minute threshold.
			// It fires once during the immediate registration loop (at ~25m), and again after reappearDelay (at ~26m).
			expectedDefaultFOS: 2,
		},
		{
			name:               "Long absence purges tracking - metric does not fire immediately",
			absenceDuration:    31 * time.Minute,
			reappearDelay:      1 * time.Minute,
			expectedDefaultFOS: 0,
		},
		{
			name:               "Long absence purges tracking - metric fires given time enough to re-fire",
			absenceDuration:    31 * time.Minute,
			reappearDelay:      11 * time.Minute,
			expectedDefaultFOS: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				metrics.ResetAllForTest()
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				testConfig := integration.NewTestConfig().WithNodePools(
					integration.DefaultNodePool(
						integration.WithNodePoolName("node-1"),
						integration.WithNodePoolSize(1),
					),
				)
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				pod := tu.BuildTestPod("my-pod-cleanup", 500, 500, tu.MarkUnschedulable())
				infra.Fakes.K8s.AddPod(pod)

				// 1. Initial loop: Pod is first observed and registered in the safeguard's internal tracking.
				// This simulates the CA discovering the unschedulable pod and starting its internal 10-minute clock.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// 2. Pod deleted for absenceDuration.
				// If absenceDuration > 30m, the CA will purge the pod from its tracking memory during this loop.
				infra.Fakes.K8s.DeletePod(pod.Namespace, pod.Name)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, tc.absenceDuration)

				// 3. Pod re-added.
				// We run an immediate loop to ensure the CA "sees" the pod as soon as it reappears.
				// If the pod was purged during a long absence, this loop registers it as a "new" pod, restarting the clock.
				infra.Fakes.K8s.AddPod(pod)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// 4. Advance time by reappearDelay and run the CA loop.
				// This verifies if the pod has been tracked long enough to trigger the safeguard metric.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, tc.reappearDelay)

				// 5. Verify expected default FOS metric
				defaultFOSVal, err := metrics.GetFilteredFalsePositiveSchedulablePodsEventsForTest(metrics.FilterOutSchedulable)
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedDefaultFOS, defaultFOSVal)
			})
		})
	}
}
