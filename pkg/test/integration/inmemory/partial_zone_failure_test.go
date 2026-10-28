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
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gceinternal "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	pod "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestPartialZoneFailure_SiblingZoneOutageWithRealNodeInHealthyZone verifies that in a
// multi-zone NodePool where one zone has a running real node and the sibling zone has 0 nodes
// and fails GCE API calls with 503 during template refresh, RunOnce() skips the unreachable
// zone without panicking and scales up the healthy sibling zone.
func TestPartialZoneFailure_SiblingZoneOutageWithRealNodeInHealthyZone(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolName("default-pool"),
			integration.WithNodePoolMachineType("n1-standard-4"),
			integration.WithNodePoolSize(0),
			integration.WithNodePoolMin(0),
			integration.WithNodePoolMax(5),
			integration.WithNodePoolLocations("us-central1-a", "us-central1-b"),
		))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		migRefA := gceinternal.GceRef{
			Project: integration.DefaultProject(),
			Zone:    "us-central1-a",
			Name:    "default-pool",
		}
		migRefB := gceinternal.GceRef{
			Project: integration.DefaultProject(),
			Zone:    "us-central1-b",
			Name:    "default-pool",
		}
		require.NoError(t, infra.Fakes.GceService.ResizeMig(ctx, migRefA, 1))

		nodes := infra.Fakes.K8s.Nodes().Items
		require.Len(t, nodes, 1, "Expected 1 running node in us-central1-a")
		healthyNode := nodes[0]
		assert.Equal(t, "us-central1-a", healthyNode.Labels[apiv1.LabelTopologyZone])

		// Inject a zonal GCE API failure in us-central1-b. Because Refresh() invalidates
		// MIG instance template names every loop, fetchExistingMigTemplateNode ->
		// GetMigInstanceTemplate -> GetMigInstanceTemplateName queries GCE for us-central1-b
		// and fails, leaving us-central1-b absent from nodeInfos during UpdateNodeInfosWithinNodePools.
		infra.Fakes.GceService.SetZoneError("us-central1-b", &googleapi.Error{
			Code:    http.StatusServiceUnavailable,
			Message: "GCE API error in us-central1-b",
		})
		infra.Fakes.GceService.ResetCallCounts()

		// Fill the existing node in us-central1-a and add an unschedulable pod.
		fillerPod := tu.BuildTestPod("filler-pod", 3500, 1000)
		fillerPod.Spec.NodeName = healthyNode.Name
		infra.Fakes.K8s.AddPod(fillerPod)

		pendingPod := tu.BuildTestPod("pending-pod", 3000, 1000, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pendingPod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)

		assert.Greater(t, infra.Fakes.GceService.TotalCallsForZone("us-central1-b"), 0, "Expected GCE calls to failing zone us-central1-b during RunOnce")
		infra.Fakes.GceService.ClearZoneError("us-central1-b")
		targetSizeB, err := infra.Fakes.GceService.FetchMigTargetSize(ctx, migRefB)
		require.NoError(t, err)
		assert.Equal(t, int64(0), targetSizeB, "Unreachable zone us-central1-b should remain at target size 0")

		infra.Fakes.RunScheduler(ctx, t)

		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pending-pod", metav1.GetOptions{})
		require.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pending-pod to be scheduled in the healthy zone")

		allNodes := infra.Fakes.K8s.Nodes().Items
		assert.Len(t, allNodes, 2, "Expected healthy zone us-central1-a to scale up to 2 nodes")
		for _, n := range allNodes {
			assert.Equal(t, "us-central1-a", n.Labels[apiv1.LabelTopologyZone], "All running nodes should be in healthy zone us-central1-a")
		}
	})
}

// TestPartialZoneFailure_CreateInstancesErrorInOneZone verifies that during a multi-zone scale-up,
// if instance creation fails in one zone, Cluster Autoscaler handles the error cleanly, backs off
// the failing zone, and scales up the healthy sibling zone to schedule pending pods.
func TestPartialZoneFailure_CreateInstancesErrorInOneZone(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(integration.WithBalanceSimilarNodeGroups()).
		WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolName("default-pool"),
			integration.WithNodePoolMachineType("n1-standard-4"),
			integration.WithNodePoolSize(0),
			integration.WithNodePoolMin(0),
			integration.WithNodePoolMax(5),
			integration.WithNodePoolLocations("us-central1-a", "us-central1-b"),
		))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		migRefB := gceinternal.GceRef{
			Project: integration.DefaultProject(),
			Zone:    "us-central1-b",
			Name:    "default-pool",
		}

		// Inject an instance creation failure only in us-central1-b.
		infra.Fakes.GceService.SetCreateInstanceForZoneError("us-central1-b", cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
			ErrorCode:    "RESOURCE_POOL_EXHAUSTED",
			ErrorMessage: "Simulated stockout in us-central1-b",
		})

		// Cycle 1: Add a pod targeting us-central1-b to deterministically trigger scale-up in us-central1-b.
		podB := tu.BuildTestPod("pod-b", 3000, 1000,
			tu.MarkUnschedulable(),
			pod.WithNodeSelectorEntry(apiv1.LabelTopologyZone, "us-central1-b"),
		)
		infra.Fakes.K8s.AddPod(podB)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Empty(t, infra.Fakes.K8s.Nodes().Items, "No K8s nodes should be registered when instance creation fails in us-central1-b")
		targetSizeB, err := infra.Fakes.GceService.FetchMigTargetSize(ctx, migRefB)
		require.NoError(t, err)
		assert.Equal(t, int64(1), targetSizeB, "Cycle 1 should attempt to scale up us-central1-b to target size 1")

		// Cycle 2: Add 2 zone-agnostic pods that each require a dedicated n1-standard-4 node (3000m CPU each).
		// On this loop, CA cleans up the failed creating instance in us-central1-b, backs off us-central1-b,
		// and scales up healthy sibling zone us-central1-a to size 2 even with BalanceSimilarNodeGroups enabled.
		pod1 := tu.BuildTestPod("pod-1", 3000, 1000, tu.MarkUnschedulable())
		pod2 := tu.BuildTestPod("pod-2", 3000, 1000, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod1)
		infra.Fakes.K8s.AddPod(pod2)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		targetSizeB, err = infra.Fakes.GceService.FetchMigTargetSize(ctx, migRefB)
		require.NoError(t, err)
		assert.Equal(t, int64(0), targetSizeB, "Failed creating instance in us-central1-b should be cleaned up and MIG TargetSize reset to 0")

		infra.Fakes.RunScheduler(ctx, t)

		for _, podName := range []string{"pod-1", "pod-2"} {
			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
			require.NoError(t, err)
			assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected %s to be scheduled on healthy zone us-central1-a", podName)
		}

		updatedPodB, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-b", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Empty(t, updatedPodB.Spec.NodeName, "Expected pod-b targeting failed zone us-central1-b to remain unscheduled")

		allNodes := infra.Fakes.K8s.Nodes().Items
		assert.Len(t, allNodes, 2, "Expected 2 total nodes in healthy zone us-central1-a")
		for _, n := range allNodes {
			assert.Equal(t, "us-central1-a", n.Labels[apiv1.LabelTopologyZone], "All scheduled nodes should be in healthy zone us-central1-a")
		}
	})
}
