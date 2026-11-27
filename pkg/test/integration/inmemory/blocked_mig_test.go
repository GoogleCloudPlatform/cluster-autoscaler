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
	"github.com/stretchr/testify/require"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestBlockedMigCachingBypass verifies that when a MIG is blocked (e.g. irretrievable),
// CA does not bypass bulk caching to issue synchronous single-MIG GET requests (FetchMig/FetchMigTemplate)
// for each node in the blocked MIG during the autoscaler loop, and does not attempt to scale up
// the degraded node group.
func TestBlockedMigCachingBypass(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolMachineType("n1-standard-4"),
			integration.WithNodePoolSize(2),
			integration.WithNodePoolLocations("us-central1-b"),
		))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Initial run to populate cluster state and caches with healthy nodes.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		nodes := infra.Fakes.K8s.Nodes().Items
		assert.Equal(t, 2, len(nodes), "Expected 2 initial nodes in the node pool")

		migs := infra.GkeCache.GetMigs()
		assert.NotEmpty(t, migs, "Expected at least 1 MIG registered in GkeCache")
		migRef := migs[0].GceRef()
		assert.Equal(t, "us-central1-b", migRef.Zone)

		// Given: The MIG becoming blocked (e.g. marked irretrievable after failures).
		infra.GkeCache.MarkIrretrievableMig(migRef, 1, gke.IrretrievableMigReasonNotFound)
		assert.True(t, infra.GkeCache.IsMigBlocked(migRef), "MIG should be marked as blocked")

		// Fill existing nodes with 3500m CPU (out of 4000m on n1-standard-4) so any new pod requires a scale-up.
		for i, node := range nodes {
			fillerPod := tu.BuildTestPod(fmt.Sprintf("filler-pod-%d", i), 3500, 0)
			fillerPod.Spec.NodeName = node.Name
			infra.Fakes.K8s.AddPod(fillerPod)
		}
		// Inject a 3000m CPU pod that cannot fit on the remaining 500m CPU of existing nodes,
		// but would otherwise trigger scale-up on an n1-standard-4 node pool.
		unschedulablePod := tu.BuildTestPod("unschedulable-pod", 3000, 0, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(unschedulablePod)

		// Reset call counts before the second loop.
		infra.Fakes.GceService.ResetCallCounts()

		// When: Advancing time past ClusterRefreshInterval (1m) so full resource refresh and template eviction occur.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)

		for _, node := range nodes {
			ng, err := autoscaler.CloudProvider.NodeGroupForNode(ctx, &node)
			assert.NoError(t, err)
			assert.Nil(t, ng, "NodeGroupForNode should return (nil, nil) for nodes belonging to a blocked MIG")
		}

		fetchMigCalls := infra.Fakes.GceService.FetchMigCalls(migRef)
		t.Logf("FetchMig calls for blocked MIG %s: %d", migRef.Name, fetchMigCalls)

		// Then: Expect 0 single-MIG FetchMig calls occur.
		// (The bug which caused synchronous single-MIG FetchMig calls for each node in the blocked zone.)
		assert.Equal(t, 0, fetchMigCalls, "Expected 0 FetchMig calls for blocked MIG, but got %d", fetchMigCalls)

		// Expect CA does not scale up or add nodes to the blocked node group while GCE is under the weather.
		nodesAfter := infra.Fakes.K8s.Nodes().Items
		assert.Equal(t, 2, len(nodesAfter), "Expected node count to remain 2 (no scale up for blocked MIG)")

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "unschedulable-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, "Expected unschedulable-pod to remain unscheduled")
	})
}

// TestBlockedMigInMultiZoneNodePoolWithTotalMaxSize verifies that when one zonal
// MIG in a multi-zone node pool with total size limits is deleted and becomes
// blocked mid-loop (before ClusterRefreshInterval), healthy sibling MIGs stop
// issuing per-node FetchMig 404 calls for it and continue scaling normally.
func TestBlockedMigInMultiZoneNodePoolWithTotalMaxSize(t *testing.T) {
	const (
		deletedZone   = "us-central1-a"
		nodesPerZone  = 5
		totalMinNodes = 1
		totalMaxNodes = 100
	)
	zones := []string{deletedZone, "us-central1-b", "us-central1-c"}

	withTotalSizeLimits := func(np *gke_api_beta.NodePool) *gke_api_beta.NodePool {
		np.Autoscaling.MinNodeCount = 0
		np.Autoscaling.MaxNodeCount = 0
		np.Autoscaling.TotalMinNodeCount = totalMinNodes
		np.Autoscaling.TotalMaxNodeCount = totalMaxNodes
		return np
	}

	testConfig := integration.NewTestConfig().
		WithOverrides(integration.WithBalanceSimilarNodeGroups()).
		WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolMachineType("n1-standard-4"),
			integration.WithNodePoolSize(nodesPerZone),
			integration.WithNodePoolLocations(zones...),
			withTotalSizeLimits,
		))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		require.NoError(t, err)

		// Initial run populates cluster state, caches, and nodePool.migs for all 3 zones.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		initialNodes := infra.Fakes.K8s.Nodes().Items
		require.Len(t, initialNodes, nodesPerZone*len(zones))

		var deletedMigRef gce.GceRef
		for _, mig := range infra.GkeCache.GetMigs() {
			if mig.GceRef().Zone == deletedZone {
				deletedMigRef = mig.GceRef()
				break
			}
		}
		require.NotEmpty(t, deletedMigRef.Name, "expected a registered MIG in %s", deletedZone)

		// Delete the zonal MIG in GCE while GKE still lists it in InstanceGroupUrls
		// and before ClusterRefreshInterval (1m) triggers refreshNodePools.
		require.NoError(t, infra.Fakes.GceService.DeleteInstanceGroupManager(deletedMigRef.Zone, deletedMigRef.Name))

		// Fill all existing nodes so scale-down checks run against every healthy node
		// without deleting them, and so a subsequent pod triggers scale-up.
		for i, node := range initialNodes {
			fillerPod := tu.BuildTestPod(fmt.Sprintf("filler-pod-%d", i), 3500, 0)
			fillerPod.Spec.NodeName = node.Name
			infra.Fakes.K8s.AddPod(fillerPod)
		}

		// Loop 1 (+10s < ClusterRefreshInterval): deleted MIG hits 404s until blocked.
		// Healthy siblings must stop querying it once blocked instead of firing one
		// FetchMig 404 per healthy node (10 healthy nodes).
		infra.Fakes.GceService.ResetCallCounts()
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		assert.True(t, infra.GkeCache.IsMigBlocked(deletedMigRef), "deleted MIG should be blocked after 404 errors")
		assert.LessOrEqual(t, infra.Fakes.GceService.FetchMigCalls(deletedMigRef), gke.MaxIrretrievableErrsBeforeBlocked,
			"expected at most %d FetchMig calls to block the deleted MIG, not per-node 404s from healthy siblings",
			gke.MaxIrretrievableErrsBeforeBlocked)

		// Loop 2 (+10s, still < ClusterRefreshInterval so nodePool.migs was not rebuilt):
		// Add an unschedulable pod and verify 0 FetchMig calls for the blocked MIG
		// and that healthy siblings can still scale up.
		unschedulablePod := tu.BuildTestPod("unschedulable-pod", 3000, 0, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(unschedulablePod)

		infra.Fakes.GceService.ResetCallCounts()
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		assert.Equal(t, 0, infra.Fakes.GceService.FetchMigCalls(deletedMigRef),
			"expected 0 FetchMig calls for already-blocked MIG before ClusterRefreshInterval")
		assert.Greater(t, len(infra.Fakes.K8s.Nodes().Items), len(initialNodes),
			"healthy sibling MIGs should scale up despite a blocked sibling in the same node pool")
	})
}
