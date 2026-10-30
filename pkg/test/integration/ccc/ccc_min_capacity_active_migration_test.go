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

package ccc_test

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status/history"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

func stockOutError() cloudprovider.InstanceErrorInfo {
	return cloudprovider.InstanceErrorInfo{
		ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
		ErrorCode:    "ZONE_RESOURCE_POOL_EXHAUSTED",
		ErrorMessage: "GCE API error: stock out",
	}
}

func TestCCCMinCapacityActiveMigration(t *testing.T) {
	cccName := "active-migration-ccc"

	// CC has n2 as optimal (P1, index 0) and e2 as non-optimal (P2, index 1).
	cc := ccc.NewComputeClassBuilder(cccName).
		WithTargetNodeCount(ptr.To(1)).
		AddPriority(v1.Priority{MachineType: ptr.To("n2-standard-2")}).
		AddPriority(v1.Priority{MachineType: ptr.To("e2-standard-2")}).
		WithActiveMigration(true).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithEnhancedCrdStatusReportingEnabled(),
			integration.WithDefragEnabled("high-priority-migration"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
		).
		WithNodePools(
			// P0 (Optimal): n2, empty
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-n2"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// P1 (Fallback): e2, empty
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-e2"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "e2",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
		).
		WithCccCrds(cc)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)

		// 1. Mock GCE stockout error on ng-n2
		infra.Fakes.GceService.SetCreateInstanceForMigError("ng-n2", stockOutError())

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		for i := 0; i < 6; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		}

		// Wait for ng-e2 node to be created.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		if !assert.Len(t, nodes.Items, 1, "Expected 1 node after fallback scale-up") {
			t.FailNow()
		}
		assert.Equal(t, "e2", nodes.Items[0].Labels[labels.MachineFamilyLabel], "Node should be from fallback e2 pool")

		// 3. Remove stockout on optimal pool
		infra.Fakes.GceService.ClearCreateInstanceForMigError("ng-n2")
		// we want the backoffs to expire, so we wait 5 minutes.
		time.Sleep(5 * time.Minute)

		// Defrag needs 4 iterations to fully happen - a new node has to be created
		// for the scale down to finish. The condition is checked after the first
		// one, while the migration is still in progress.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)

		// Let the background status aggregator flush the queued condition.
		time.Sleep(ccc.ConditionsFlushInterval)

		updatedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
		assert.NoError(t, err)

		// The scale-up was triggered by pods defrag wants to migrate, not by real pending pods.
		ccc.AssertPriorityCondition(t, updatedCCC, 0, history.ConditionTypeNodeProvisioningInProgress, history.ConditionReasonActiveMigration)

		for i := 0; i < 3; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		}

		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)

		if !assert.Len(t, nodes.Items, 1, "Expected exactly 1 node after active migration") {
			t.FailNow()
		}
		assert.Equal(t, "n2", nodes.Items[0].Labels[labels.MachineFamilyLabel], "Node should be actively migrated to optimal n2 pool")
	})
}

// TestCCCMinCapacityActiveMigration_FailedReplacementKeepsMinCapacity checks
// that while defrag's replacement of a min capacity node fails, CA neither adds
// nodes nor taints a second node, and that defrag retries once the stockout
// clears.
func TestCCCMinCapacityActiveMigration_FailedReplacementKeepsMinCapacity(t *testing.T) {
	const minCapacity = 2
	cccName := "failed-replacement-ccc"
	cc := ccc.NewComputeClassBuilder(cccName).
		WithTargetNodeCount(ptr.To(minCapacity)).
		AddPriority(v1.Priority{MachineType: ptr.To("n2-standard-2")}).
		AddPriority(v1.Priority{MachineType: ptr.To("e2-standard-2")}).
		WithActiveMigration(true).
		Build()
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithDefragEnabled("high-priority-migration"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
		).
		WithNodePools(
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-n2"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-e2"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "e2",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
		).
		WithCccCrds(cc)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)
		listNodes := func(loop string) []apiv1.Node {
			t.Helper()
			nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("%s: Nodes().List() failed: %v", loop, err)
			}
			return nodes.Items
		}
		// The min capacity nodes land in the e2 fallback pool while n2 is
		// stocked out.
		infra.Fakes.GceService.SetCreateInstanceForMigError("ng-n2", stockOutError())
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if err != nil {
			t.Fatalf("SetupAutoscaler() failed: %v", err)
		}
		for i := 0; i < 6; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		}
		if got := countNodesOfFamily(listNodes("Fallback scale-up"), "e2"); got != minCapacity {
			t.Fatalf("Got %d e2 nodes after the fallback scale-up, want %d", got, minCapacity)
		}
		// Let the n2 backoff expire, so that defrag picks an e2 node and asks
		// for an n2 replacement, which fails on the stockout.
		time.Sleep(5 * time.Minute)

		// Fewer loops than the defrag scale-up timeout, while n2 stays stocked
		// out.
		sawCandidate := false
		for i := 0; i < 12; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)

			loop := fmt.Sprintf("Stocked out loop %d", i)
			nodes := listNodes(loop)
			if len(nodes) != minCapacity {
				t.Fatalf("%s: got %d nodes, want the %d min capacity nodes", loop, len(nodes), minCapacity)
			}
			// The candidate keeps its min capacity fake pod, so defrag keeps
			// asking for its replacement instead of moving on to the other
			// node. Defrag drops the candidate while n2 is backed off.
			candidates := countNodesWithTaint(nodes, defrag.HardTaint)
			if candidates > 1 {
				t.Fatalf("%s: got %d nodes with the defrag HardTaint, want at most 1", loop, candidates)
			}
			sawCandidate = sawCandidate || candidates == 1
		}
		if !sawCandidate {
			t.Fatalf("Got no node with the defrag HardTaint while n2 was stocked out, want defrag to pick an e2 node")
		}

		// Defrag must migrate before its 10m scale-up timeout (20 loops); a
		// migration takes 2 loops (scale-up, then drain). n2 being preferred,
		// both nodes may migrate.
		infra.Fakes.GceService.ClearCreateInstanceForMigError("ng-n2")
		time.Sleep(5 * time.Minute)
		const maxMigrationLoops = 6
		migrated := false
		for i := 0; i < maxMigrationLoops && !migrated; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)

			loop := fmt.Sprintf("Migration loop %d", i)
			nodes := listNodes(loop)
			if got := len(nodes) - countNodesWithTaint(nodes, taints.ToBeDeletedTaint); got < minCapacity {
				t.Fatalf("%s: got %d nodes not being deleted, want at least the min capacity %d", loop, got, minCapacity)
			}
			migrated = countNodesOfFamily(nodes, "n2") >= 1
		}
		if !migrated {
			t.Fatalf("Got nodes %v after %d loops without the stockout, want an e2 node migrated to n2", nodeFamilies(listNodes("After migration")), maxMigrationLoops)
		}
	})
}

func countNodesOfFamily(nodes []apiv1.Node, family string) int {
	count := 0
	for _, node := range nodes {
		if node.Labels[labels.MachineFamilyLabel] == family {
			count++
		}
	}
	return count
}

func countNodesWithTaint(nodes []apiv1.Node, taintKey string) int {
	count := 0
	for i := range nodes {
		if taints.HasTaint(&nodes[i], taintKey) {
			count++
		}
	}
	return count
}

func nodeFamilies(nodes []apiv1.Node) map[string]string {
	families := make(map[string]string)
	for _, node := range nodes {
		families[node.Name] = node.Labels[labels.MachineFamilyLabel]
	}
	return families
}
