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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
