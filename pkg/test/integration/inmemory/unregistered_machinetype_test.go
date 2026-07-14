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
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gcev1 "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	gke "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestUnregisteredMachineType verifies that Cluster Autoscaler can scale up
// a node pool with an unregistered machine type by falling back to GCE API data,
// and also scale it down when the node is no longer needed.
func TestUnregisteredMachineType(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolMachineType("mock1-standard-2"),
			integration.WithNodePoolSize(0),
			integration.WithNodePoolLocations("us-central1-a"),
		))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Add the unregistered machine type to the fake GCE service.
		infra.Fakes.GceService.AddCustomMachineType(&gcev1.MachineType{
			Name:      "mock1-standard-2",
			GuestCpus: 2,
			MemoryMb:  7680, // 7.5 GB
			Zone:      "us-central1-a",
		})

		// Create an unschedulable pod that fits on mock1-standard-2 (2 CPU, 7.5GB).
		// Requesting 1.5 CPU and 4GB memory.
		pod := tu.BuildTestPod("unschedulable-pod", 1500, 4000, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod)

		// Run autoscaler loop.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Run scheduler to update pod status if node was added.
		infra.Fakes.RunScheduler(ctx, t)

		// Verify if the pod got scheduled.
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "unschedulable-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to be scheduled")

		// Verify that a node was added.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nodes.Items), "Expected 1 node to be added, but got %d", len(nodes.Items))

		// Verify the node has the correct machine type label.
		node := nodes.Items[0]
		assert.Equal(t, "mock1-standard-2", node.Labels[apiv1.LabelInstanceTypeStable])

		// --- Scale down ---
		// Delete the pod to make the node unneeded.
		err = infra.Fakes.KubeClient.CoreV1().Pods("default").Delete(ctx, "unschedulable-pod", metav1.DeleteOptions{})
		assert.NoError(t, err)

		// Run autoscaler loop to detect node is unneeded.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Check node taints after detection.
		nodesDetect, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nodesDetect.Items))

		// Run autoscaler loop again to scale down.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second*2)

		// Verify that the node was removed.
		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(nodes.Items), "Expected node to be scaled down")
	})
}

// TestUnregisteredMachineTypeErrorCaching verifies that GCE API errors encountered when fetching
// machine types during NAP scale-up are cached to prevent GCE API spam across multiple pod groups.
// It also verifies that the autoscaler successfully recovers and scales up once the cache TTL expires.
func TestUnregisteredMachineTypeErrorCaching(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(integration.WithAutoProvisioningEnabled()).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations("us-central1-a"),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Register a GCE handler that fails with 404 on the very first call, and returns success
		// on all subsequent calls. This simulates a transient GCE outage that recovers mid-loop.
		//
		// If the autoscaler's error caching is working, it will cache the first failure and block
		// subsequent scale-up attempts within the same loop without querying GCE again (which would
		// otherwise succeed since GCE recovered immediately).
		failedOnce := false
		infra.Fakes.GceService.SetFetchMachineTypeHandler("us-central1-a", "n1-standard-96", func() error {
			if !failedOnce {
				failedOnce = true
				return &googleapi.Error{
					Code:    http.StatusNotFound,
					Message: "machine type n1-standard-96 in zone us-central1-a not found (temporary testing error)",
				}
			}
			return nil
		})

		// Create 5 unschedulable pods strictly requesting n1-standard-96.
		// We use slightly different CPU requests to ensure they are not equivalent,
		// forcing the autoscaler to process them as 5 separate groups in the same loop.
		for i := 0; i < 5; i++ {
			podName := fmt.Sprintf("pod-%d", i)
			cpuRequest := int64(70000 + i) // 70000, 70001, etc. -> forces 1 pod per node on 96-core machine
			pod := tu.BuildTestPod(podName, cpuRequest, 2000, tu.MarkUnschedulable())
			pod.Spec.NodeSelector = map[string]string{
				apiv1.LabelInstanceTypeStable: "n1-standard-96",
			}
			infra.Fakes.K8s.AddPod(pod)
		}

		// Loop 1: The first pod group triggers a GCE query, which fails and gets cached.
		// If caching is working, the other 4 groups will hit the cache and fail immediately
		// without calling GCE (which would otherwise succeed).
		// Assert that no nodes are added, confirming the scale-up was blocked.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Assert that no scale-up happened, confirming the cached error blocked all pod groups.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Empty(t, nodes.Items, "Expected no nodes to be added because the error should be cached for all pods in the loop")

		// Loop 2: Sleep past the cache TTL. Since the cache has expired and GCE has recovered,
		// the autoscaler should now successfully query GCE, validate the machine config,
		// and scale up the cluster to accommodate all 5 pods.
		time.Sleep(gke.MachineTypeErrorCacheValidity + time.Minute)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Run scheduler to update pod status.
		infra.Fakes.RunScheduler(ctx, t)

		// Verify all 5 pods got scheduled.
		for i := 0; i < 5; i++ {
			podName := fmt.Sprintf("pod-%d", i)
			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod %s to be scheduled after cache expiration", podName)
		}

		// Verify that nodes were added.
		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 5, "Expected 5 nodes to be added after cache expiration")
	})
}
