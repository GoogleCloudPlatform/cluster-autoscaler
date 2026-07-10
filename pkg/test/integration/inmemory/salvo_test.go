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
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"

	gke_api_beta "google.golang.org/api/container/v1beta1"
	config "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

const (
	tpuTypeV4 = "tpu-v4-podslice"
)

// TestSalvoMultiScaleUpTPUMultihost verifies that multiple TPU node pools across different zones can be created in a single autoscaler loop when Salvo is enabled.
func TestSalvoMultiScaleUpTPUMultihost(t *testing.T) {
	tests := []struct {
		name          string
		salvoEnabled  bool
		pods          []*apiv1.Pod
		wantScheduled int
		expectedPools int
		expectedNodes int
	}{
		{
			name:          "Salvo Enabled - multiple Scale Ups",
			salvoEnabled:  true,
			pods:          append(newTpuPods("us-central1-a", 2), newTpuPods("us-central1-b", 2)...),
			wantScheduled: 4,
			expectedPools: 2,
			expectedNodes: 4,
		},
		{
			name:          "Salvo Disabled - single Scale Up",
			salvoEnabled:  false,
			pods:          append(newTpuPods("us-central1-a", 2), newTpuPods("us-central1-b", 2)...),
			wantScheduled: 2,
			expectedPools: 1,
			expectedNodes: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					withSalvoMultiScaleUp(tc.salvoEnabled),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
						{ResourceType: "cpu", Maximum: integration.DefaultMaxCoresResourceLimit},
						{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
						{ResourceType: "tpu-v4-podslice", Maximum: 100},
					}),
				).
				WithClusterWideLimits(100, 4000, 4000*1024*1024*1024)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				addPods(infra, tc.pods...)

				// Run the autoscaler loop once
				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

				// Verify that node pools were created
				cluster := integration.GetTestCluster(t, infra, testConfig)
				assert.Equal(t, tc.expectedPools, len(cluster.NodePools))

				// Verify that nodes were created in total
				nodes, err := infra.Fakes.K8s.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNodes, len(nodes.Items))

				// Run the scheduler
				infra.Fakes.RunScheduler(ctx, t)

				// Verify pod scheduling
				assertScheduledPodsCount(t, ctx, infra, tc.pods, tc.wantScheduled)
			})
		})
	}
}

// TestSalvoMultiScaleUpLimitEnforcement verifies that multiple scale ups in a single loop respect cluster-wide node limits by capping total created nodes across zones.
func TestSalvoMultiScaleUpLimitEnforcement(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(
			integration.EmptyNodePool("pool-a").
				WithMachineType("e2-standard-4").
				WithLocations("us-central1-a").
				Build(),
			integration.EmptyNodePool("pool-b").
				WithMachineType("e2-standard-4").
				WithLocations("us-central1-b").
				Build(),
		).
		WithOverrides(
			withSalvoMultiScaleUp(true),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: 12},
				{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
			}),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		addPods(infra,
			buildCpuPod("p1", "us-central1-a"),
			buildCpuPod("p2", "us-central1-a"),
			buildCpuPod("p3", "us-central1-b"),
			buildCpuPod("p4", "us-central1-b"),
		)

		// Run autoscaler once.
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

		// Verify that total nodes in cluster is capped at 3.
		nodes, err := infra.Fakes.K8s.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(nodes.Items), "Expected total nodes to be capped at 3")

		// Verify that one zone has 2 nodes and the other has 1.
		zoneCounts := make(map[string]int)
		for _, node := range nodes.Items {
			zone := node.Labels["topology.kubernetes.io/zone"]
			zoneCounts[zone]++
		}
		countA := zoneCounts["us-central1-a"]
		countB := zoneCounts["us-central1-b"]
		assert.True(t, (countA == 2 && countB == 1) || (countA == 1 && countB == 2), "Expected one zone to have 2 nodes and the other 1 node, got A: %d, B: %d", countA, countB)
	})
}

// TestSalvoMultiScaleUpPartialFailure verifies that if one node pool fails during multiple scale ups in a single loop, successful node pools still scale up and schedule pods.
func TestSalvoMultiScaleUpPartialFailure(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			withSalvoMultiScaleUp(true),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: integration.DefaultMaxCoresResourceLimit},
				{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
				{ResourceType: "tpu-v4-podslice", Maximum: 100},
			}),
		).
		WithClusterWideLimits(100, 4000, 4000*1024*1024*1024)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		podsZoneA := newTpuPods("us-central1-a", 2)
		podsZoneB := newTpuPods("us-central1-b", 2)
		addPods(infra, append(podsZoneA, podsZoneB...)...)

		// Simulate cluster quota / limit state allowing only 1 node pool.
		infra.Fakes.GkeService.SetMaxNodePoolCount(1)

		// Run the autoscaler loop once
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

		// Verify that only one node pool was created (the one that succeeded).
		cluster := integration.GetTestCluster(t, infra, testConfig)
		assert.Equal(t, 1, len(cluster.NodePools))

		createdZone := cluster.NodePools[0].Locations[0]
		assert.True(t, createdZone == "us-central1-a" || createdZone == "us-central1-b")

		// Verify that 2 nodes were created
		nodes, err := infra.Fakes.K8s.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(nodes.Items))

		// Run the scheduler
		infra.Fakes.RunScheduler(ctx, t)

		// Verify that only 2 of 4 pods are scheduled
		allPods := append(podsZoneA, podsZoneB...)
		assertScheduledPodsCount(t, ctx, infra, allPods, 2)
	})
}

// TestSalvoMultiScaleUpTimeout verifies that multiple scale ups in a single loop respect the Salvo time budget by halting further node pool creations once the budget expires.
func TestSalvoMultiScaleUpTimeout(t *testing.T) {
	// Budget is 500ms, and we configure the fake GKE client to take 600ms
	// for node pool creation (independent of the default 1s polling interval).
	// So Salvo should timeout after the first node pool creation.
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			withSalvoMultiScaleUp(true, 500*time.Millisecond),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: integration.DefaultMaxCoresResourceLimit},
				{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
				{ResourceType: "tpu-v4-podslice", Maximum: 100},
			}),
		).
		WithClusterWideLimits(100, 4000, 4000*1024*1024*1024)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		// Configure fake GKE client for controlled delay, independent of GKE polling defaults.
		infra.Fakes.GkeService.DisableOperationDelay(true)
		infra.Fakes.GkeService.SetCreateNodePoolDelay(600 * time.Millisecond)

		podsZoneA := newTpuPods("us-central1-a", 2)
		podsZoneB := newTpuPods("us-central1-b", 2)
		addPods(infra, append(podsZoneA, podsZoneB...)...)

		// Run the autoscaler loop once
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

		// Verify that only one node pool was created because the second one timed out.
		cluster := integration.GetTestCluster(t, infra, testConfig)
		assert.Equal(t, 1, len(cluster.NodePools), "Expected only 1 node pool due to timeout")

		createdZone := cluster.NodePools[0].Locations[0]
		assert.True(t, createdZone == "us-central1-a" || createdZone == "us-central1-b")
	})
}

// TestSalvoMultiScaleUpSharded verifies that pods belonging to different compute shards are scaled up in consecutive loops rather than in the same loop.
func TestSalvoMultiScaleUpSharded(t *testing.T) {
	// TPU and CPU pods are in different shards.
	// They should be scaled up in consecutive loops, not together, even if Salvo is enabled.
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			withSalvoMultiScaleUp(true),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: integration.DefaultMaxCoresResourceLimit},
				{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
				{ResourceType: "tpu-v4-podslice", Maximum: 100},
			}),
		).
		WithClusterWideLimits(100, 4000, 4000*1024*1024*1024)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		// Create 1 TPU pod (needs TPU node pool)
		tpuPod := buildTpuPod("tpu-pod-2x2x2", "us-central1-a")
		infra.Fakes.K8s.AddPod(tpuPod)

		// Create 1 CPU pod (needs CPU node pool)
		cpuPod := buildCpuPod("cpu-pod", "us-central1-b")
		infra.Fakes.K8s.AddPod(cpuPod)

		// Run loop 1.
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

		// Verify that only one node pool was created (either TPU or CPU).
		cluster := integration.GetTestCluster(t, infra, testConfig)
		assert.Equal(t, 1, len(cluster.NodePools), "Expected only 1 node pool in first loop")

		// Run loop 2.
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

		// Verify that both node pools are now created.
		cluster = integration.GetTestCluster(t, infra, testConfig)
		assert.Equal(t, 2, len(cluster.NodePools), "Expected both node pools to be created after second loop")

		hasTPU := false
		hasCPU := false
		for _, np := range cluster.NodePools {
			if np.Config.MachineType == "ct4p-hightpu-4t" {
				hasTPU = true
			} else {
				hasCPU = true
			}
		}
		assert.True(t, hasTPU && hasCPU, "Expected both TPU and CPU node pools to be created")

		// Run scheduler
		infra.Fakes.RunScheduler(ctx, t)

		// Verify both pods are scheduled
		assertScheduledPodsCount(t, ctx, infra, []*apiv1.Pod{tpuPod, cpuPod}, 2)
	})
}

func withSalvoMultiScaleUp(enabled bool, budget ...time.Duration) integration.Option[*config.AutoscalingOptions] {
	return func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
		o.SalvoScaleUp = enabled
		if enabled {
			if len(budget) > 0 {
				o.SalvoScaleUpBudget = budget[0]
			} else {
				o.SalvoScaleUpBudget = 1 * time.Minute
			}
		}
		return o
	}
}

func buildPod(name string, cpu, memory int64, zone string, opts ...func(*apiv1.Pod)) *apiv1.Pod {
	allOpts := append([]func(*apiv1.Pod){tu.MarkUnschedulable()}, opts...)
	if zone != "" {
		allOpts = append(allOpts, pod.WithNodeSelectorEntry("topology.kubernetes.io/zone", zone))
	}
	p := tu.BuildTestPod(name, cpu, memory, allOpts...)
	p.UID = types.UID(name)
	return p
}

func buildTpuPod(name, zone string) *apiv1.Pod {
	return buildPod(name, 100, 100, zone, pod.WithTPU(tpuTypeV4, "2x2x2", "4"))
}

func buildCpuPod(name, zone string) *apiv1.Pod {
	return buildPod(name, 3000, 1000, zone)
}

func newTpuPods(zone string, count int) []*apiv1.Pod {
	var pods []*apiv1.Pod
	for i := 0; i < count; i++ {
		pods = append(pods, buildTpuPod(fmt.Sprintf("tpu-pod-2x2x2-%s-%d", zone, i), zone))
	}
	return pods
}

func addPods(infra *integration.TestInfrastructure, pods ...*apiv1.Pod) {
	for _, p := range pods {
		infra.Fakes.K8s.AddPod(p)
	}
}

func assertScheduledPodsCount(t *testing.T, ctx context.Context, infra *integration.TestInfrastructure, pods []*apiv1.Pod, wantScheduled int) {
	t.Helper()
	scheduledCount := 0
	for _, p := range pods {
		pod, err := infra.Fakes.K8s.Client.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		if pod.Spec.NodeName != "" {
			scheduledCount++
		}
	}
	assert.Equal(t, wantScheduled, scheduledCount)
}
