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
	"github.com/stretchr/testify/require"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	unmanagedGceNodeName  = "unmanaged-gce-node"
	emptyProviderIDNode   = "empty-provider-id-node"
	invalidProviderIDNode = "malformed-provider-id-node"
	unmanagedTaintKey     = "test.gke.io/unmanaged"
	unmanagedTaintValue   = "true"
)

// TestUnmanagedAndMalformedProviderIDNodes verifies that Cluster Autoscaler handles:
//  1. Unmanaged Kubernetes Nodes whose valid GCE ProviderID does not belong to any
//     registered autoscaled MIG (NodeGroupForNode returns (nil, nil)).
//  2. Nodes belonging to non-autoscaled GKE node pools (NodeGroupForNode returns (nil, nil)).
//  3. Kubernetes Nodes with empty or malformed Spec.ProviderID (NodeGroupForNode returns an error).
//
// Both scale-up and scale-down cycles are exercised alongside a ComputeClass CRD with
// enhanced CRD status reporting, scale-down status, config drift, and min-capacity enabled
// so that all core and custom GKE processors inspect the unmanaged and malformed nodes.
func TestUnmanagedAndMalformedProviderIDNodes(t *testing.T) {
	const (
		cccName                = "unmanaged-test-ccc"
		managedPoolName        = "managed-pool"
		nonAutoscaledPoolName  = "non-autoscaled-pool"
		scaleDownUnneededTimer = 2 * time.Second
	)

	testConfig := unmanagedNodesTestConfig(cccName, managedPoolName, nonAutoscaledPoolName, scaleDownUnneededTimer)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		require.NoError(t, err)

		addUnmanagedAndMalformedNodes(infra, cccName)
		assertTotalNodeCount(ctx, t, infra, 5)

		initialManagedNodeName := findNodeInPool(ctx, t, infra, managedPoolName)
		nonAutoscaledNodeName := findNodeInPool(ctx, t, infra, nonAutoscaledPoolName)

		scaleUpPod := addManagedPoolScaleUpPods(infra, initialManagedNodeName, cccName, managedPoolName)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		infra.Fakes.RunScheduler(ctx, t)

		assertTotalNodeCount(ctx, t, infra, 6)
		assertPodScheduled(ctx, t, infra, scaleUpPod.Name)

		infra.Fakes.K8s.DeletePod(apiv1.NamespaceDefault, scaleUpPod.Name)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

		assertTotalNodeCount(ctx, t, infra, 5)
		assertNodesUntouchedAndUntainted(ctx, t, infra,
			unmanagedGceNodeName,
			nonAutoscaledNodeName,
			emptyProviderIDNode,
			invalidProviderIDNode,
		)

		processedCCC, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, cccName, processedCCC.Name)
	})
}

func unmanagedNodesTestConfig(cccName, managedPoolName, nonAutoscaledPoolName string, scaleDownUnneededTimer time.Duration) *integration.TestConfig {
	cc := ccc.NewComputeClassBuilder(cccName).
		WithNodePoolAutoCreation(false).
		WithNodePoolsRules(managedPoolName).
		WithTargetNodeCount(ptr.To(1)).
		Build()

	managedPool := integration.EmptyNodePool(managedPoolName).
		WithMachineType("e2-standard-4").
		WithSize(1).
		WithMin(1).
		WithMax(4).
		WithCCCLabel(cccName).
		Build()

	nonAutoscaledPool := integration.DefaultNodePool(
		integration.WithNodePoolName(nonAutoscaledPoolName),
		integration.WithNodePoolMachineType("e2-standard-4"),
		integration.WithNodePoolSize(1),
		integration.WithNodePoolLocations("us-central1-b"),
		integration.WithNodePoolAutoscalingEnabled(false),
		integration.WithNodePoolTaints(&gke_api_beta.NodeTaint{
			Key:    unmanagedTaintKey,
			Value:  unmanagedTaintValue,
			Effect: "NO_SCHEDULE",
		}),
	)

	return integration.NewTestConfig().
		WithNodePools(managedPool, nonAutoscaledPool).
		WithCccCrds(cc).
		WithOverrides(
			integration.WithScaleDownUnneededTime(scaleDownUnneededTimer),
			integration.WithEnhancedCrdStatusReporting(true),
			integration.WithComputeClassScaleDownStatusEnabled(),
			integration.WithComputeClassConfigDriftReportingEnabled(),
			integration.WithDefragEnabled("nodeconfigdrift"),
			integration.WithComputeClassMinCapacityEnabled(),
		)
}

func addUnmanagedAndMalformedNodes(infra *integration.TestInfrastructure, cccName string) {
	unmanagedTaint := apiv1.Taint{
		Key:    unmanagedTaintKey,
		Value:  unmanagedTaintValue,
		Effect: apiv1.TaintEffectNoSchedule,
	}

	infra.Fakes.K8s.AddNode(buildUnmanagedTestNode(
		unmanagedGceNodeName,
		"gce://test-project/us-central1-b/unmanaged-gce-instance-1",
		cccName,
		unmanagedTaint,
	))
	infra.Fakes.K8s.AddNode(buildUnmanagedTestNode(
		emptyProviderIDNode,
		"",
		cccName,
		unmanagedTaint,
	))
	infra.Fakes.K8s.AddNode(buildUnmanagedTestNode(
		invalidProviderIDNode,
		"malformed://invalid-provider-id-format",
		cccName,
		unmanagedTaint,
	))
}

func buildUnmanagedTestNode(name, providerID, cccName string, taint apiv1.Taint) *apiv1.Node {
	n := tu.BuildTestNode(name, 4000, 16*1024*1024*1024,
		tu.IsReady(true),
		tu.WithNodeLabels(map[string]string{
			labels.ComputeClassLabel: cccName,
		}),
	)
	n.Spec.ProviderID = providerID
	n.Spec.Taints = []apiv1.Taint{taint}
	return n
}

func findNodeInPool(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, poolName string) string {
	t.Helper()
	nodes := listNodesInNodePool(ctx, t, infra, poolName)
	require.NotEmpty(t, nodes, "expected an initial node in %s", poolName)
	return nodes[0]
}

func listNodesInNodePool(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, poolName string) []string {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	var names []string
	for _, n := range nodes.Items {
		if n.Labels[labels.GkeNodePoolLabel] == poolName {
			names = append(names, n.Name)
		}
	}
	return names
}

func addManagedPoolScaleUpPods(infra *integration.TestInfrastructure, initialManagedNodeName, cccName, managedPoolName string) *apiv1.Pod {
	pinnedPod := tu.BuildTestPod("managed-pinned-pod", 2500, 100*1024*1024,
		tu.WithNodeName(initialManagedNodeName),
		pod.WithCCC(cccName),
		pod.WithNodeSelectorEntry(labels.GkeNodePoolLabel, managedPoolName),
	)
	scaleUpPod := tu.BuildTestPod("managed-scaleup-pod", 2500, 100*1024*1024,
		tu.MarkUnschedulable(),
		pod.WithCCC(cccName),
		pod.WithNodeSelectorEntry(labels.GkeNodePoolLabel, managedPoolName),
	)
	infra.Fakes.K8s.AddPod(pinnedPod)
	infra.Fakes.K8s.AddPod(scaleUpPod)
	return scaleUpPod
}

func assertTotalNodeCount(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, wantCount int) {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, nodes.Items, wantCount)
}

func assertPodScheduled(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, podName string) {
	t.Helper()
	scheduledPod, err := infra.Fakes.KubeClient.CoreV1().Pods(apiv1.NamespaceDefault).Get(ctx, podName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, scheduledPod.Spec.NodeName, "expected pod %q to be scheduled", podName)
}

func assertNodesUntouchedAndUntainted(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, nodeNames ...string) {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)

	remainingByName := make(map[string]apiv1.Node, len(nodes.Items))
	for _, n := range nodes.Items {
		remainingByName[n.Name] = n
	}

	for _, name := range nodeNames {
		n, exists := remainingByName[name]
		if assert.True(t, exists, "expected unmanaged/malformed node %q to remain untouched", name) {
			assert.False(t, taints.HasToBeDeletedTaint(&n),
				"unmanaged/malformed node %q must not be tainted for deletion", name)
		}
	}
}

// TestZeroOrMaxNodeScalingAtomicScaleDown verifies scale-down behavior for a node pool
// configured with ZeroOrMaxNodeScaling (atomic scaling):
//  1. When one node in the atomic slice hosts a non-evictable pod and all other nodes in
//     the atomic slice are empty, partial scale-down must not occur and RunOnce must not panic.
//  2. Once the blocking pod is removed so all nodes in the atomic slice are unneeded,
//     all nodes in the atomic node pool scale down to zero atomically.
func TestZeroOrMaxNodeScalingAtomicScaleDown(t *testing.T) {
	const (
		atomicPoolName = "atomic-tpu-pool"
		defaultPool    = "default-pool"
		sliceNodeCount = 16
	)

	atomicTPUPool := atomicTPUNodePool(atomicPoolName, sliceNodeCount)
	regularPool := integration.EmptyNodePool(defaultPool).
		WithMachineType("e2-standard-4").
		WithSize(1).
		WithMin(1).
		WithMax(2).
		Build()

	testConfig := integration.NewTestConfig().
		WithNodePools(regularPool, atomicTPUPool).
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Second),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		require.NoError(t, err)

		assertTotalNodeCount(ctx, t, infra, sliceNodeCount+1)
		atomicNodeNames := listNodesInNodePool(ctx, t, infra, atomicPoolName)
		require.Len(t, atomicNodeNames, sliceNodeCount, "expected all 16 nodes in %s", atomicPoolName)

		blockingPod := verifyAtomicPoolBlocksPartialScaleDown(ctx, t, autoscaler, infra, atomicNodeNames[0], sliceNodeCount+1)
		verifyAtomicPoolScalesDownToZero(ctx, t, autoscaler, infra, blockingPod.Name, defaultPool)
	})
}

func atomicTPUNodePool(name string, sliceNodeCount int64) *gke_api_beta.NodePool {
	return &gke_api_beta.NodePool{
		Name: name,
		Config: &gke_api_beta.NodeConfig{
			MachineType: "tpu7x-standard-4t",
			ImageType:   "cos_containerd",
		},
		InitialNodeCount: sliceNodeCount,
		Autoscaling: &gke_api_beta.NodePoolAutoscaling{
			Enabled:      true,
			MinNodeCount: 0,
			MaxNodeCount: sliceNodeCount,
		},
		Locations: []string{"us-central1-b"},
		PlacementPolicy: &gke_api_beta.PlacementPolicy{
			TpuTopology: "4x4x4",
		},
	}
}

func verifyAtomicPoolBlocksPartialScaleDown(
	ctx context.Context,
	t *testing.T,
	autoscaler core.Autoscaler,
	infra *integration.TestInfrastructure,
	blockingNodeName string,
	expectedTotalNodes int,
) *apiv1.Pod {
	t.Helper()
	blockingPod := tu.BuildTestPod("unremovable-atomic-pod", 100, 100*1024*1024,
		tu.WithNodeName(blockingNodeName),
		pod.WithAnnotation("cluster-autoscaler.kubernetes.io/safe-to-evict", "false"),
	)
	infra.Fakes.K8s.AddPod(blockingPod)

	for range 3 {
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
	}

	nodesAfterPartialCheck, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, nodesAfterPartialCheck.Items, expectedTotalNodes,
		"partial scale-down must not occur when one node in a ZeroOrMaxNodeScaling pool is needed")
	return blockingPod
}

func verifyAtomicPoolScalesDownToZero(
	ctx context.Context,
	t *testing.T,
	autoscaler core.Autoscaler,
	infra *integration.TestInfrastructure,
	blockingPodName string,
	expectedRemainingPool string,
) {
	t.Helper()
	infra.Fakes.K8s.DeletePod(apiv1.NamespaceDefault, blockingPodName)

	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

	nodesAfterAtomicScaleDown, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	if assert.Len(t, nodesAfterAtomicScaleDown.Items, 1,
		"expected all nodes in ZeroOrMaxNodeScaling pool to scale down atomically to 0, leaving only default-pool node") {
		assert.Equal(t, expectedRemainingPool, nodesAfterAtomicScaleDown.Items[0].Labels[labels.GkeNodePoolLabel])
	}
}

// TestTerminatingAutoprovisionedNodePool verifies that an autoprovisioned (NAP) node pool
// marked as deleting/terminating (Status: "STOPPING" or undergoing asynchronous deletion)
// when unschedulable pods arrive does not cause RunOnce() to panic or attempt invalid
// resize operations on the terminating node pool, and that CA cleanly provisions capacity
// to schedule the arriving pods.
func TestTerminatingAutoprovisionedNodePool(t *testing.T) {
	t.Run("stopping_nap_node_pool_with_lingering_node_during_scale_up", func(t *testing.T) {
		const (
			defaultPoolName  = "default-pool"
			stoppingPoolName = "nap-e2-standard-8-stopping"
		)

		defaultPool := integration.EmptyNodePool(defaultPoolName).
			WithMachineType("e2-standard-4").
			WithSize(1).
			WithMin(1).
			WithMax(1).
			Build()

		stoppingNapPool := autoprovisionedNodePool(stoppingPoolName, 1)

		testConfig := integration.NewTestConfig().
			WithNodePools(defaultPool, stoppingNapPool).
			WithOverrides(
				integration.WithAutoProvisioningEnabled(),
			).
			WithClusterOverrides(
				integration.WithClusterAutoProvisioningEnabled(),
			)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer integration_synctest.TearDown(cancel)

			infra := integration.SetupInfrastructure(ctx, t)
			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			require.NoError(t, err)

			markNapPoolStoppingWithUnschedulableNodes(ctx, t, infra, stoppingPoolName)

			largePod := tu.BuildTestPod("nap-scaleup-pod", 6000, 8*1024*1024*1024, tu.MarkUnschedulable())
			infra.Fakes.K8s.AddPod(largePod)

			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)
			infra.Fakes.RunScheduler(ctx, t)

			assertPodScheduled(ctx, t, infra, largePod.Name)
			assertStoppingPoolNotResized(t, infra, stoppingPoolName, 1)
		})
	})

	t.Run("async_deleting_empty_nap_node_pool_when_unschedulable_pods_arrive", func(t *testing.T) {
		const (
			defaultPoolName = "default-pool"
			emptyNapPool    = "nap-e2-standard-8-empty"
		)

		defaultPool := integration.EmptyNodePool(defaultPoolName).
			WithMachineType("e2-standard-4").
			WithSize(1).
			WithMin(1).
			WithMax(1).
			Build()

		emptyAutoprovisionedPool := autoprovisionedNodePool(emptyNapPool, 0)

		testConfig := integration.NewTestConfig().
			WithNodePools(defaultPool, emptyAutoprovisionedPool).
			WithOverrides(
				integration.WithScaleDownUnneededTime(time.Second),
				integration.WithAutoProvisioningEnabled(),
				integration.WithHighThroughputNAPEnabled(10, 100),
			).
			WithClusterOverrides(
				integration.WithClusterAutoProvisioningEnabled(),
			)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer integration_synctest.TearDown(cancel)

			infra := integration.SetupInfrastructure(ctx, t)
			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			require.NoError(t, err)

			triggerEmptyNapPoolDeletion(ctx, t, autoscaler)

			arrivingPod := tu.BuildTestPod("arriving-nap-pod", 6000, 8*1024*1024*1024, tu.MarkUnschedulable())
			infra.Fakes.K8s.AddPod(arrivingPod)

			provisionAndScheduleArrivingNapPod(ctx, t, autoscaler, infra)
			assertPodScheduled(ctx, t, infra, arrivingPod.Name)
		})
	})
}

func autoprovisionedNodePool(name string, initialNodeCount int64) *gke_api_beta.NodePool {
	return &gke_api_beta.NodePool{
		Name:     name,
		Status:   "RUNNING",
		SelfLink: "projects/test-project/locations/us-central1/clusters/test-cluster/nodePools/" + name,
		Config: &gke_api_beta.NodeConfig{
			MachineType: "e2-standard-8",
			ImageType:   "cos_containerd",
		},
		InitialNodeCount: initialNodeCount,
		Autoscaling: &gke_api_beta.NodePoolAutoscaling{
			Enabled:         true,
			Autoprovisioned: true,
			MinNodeCount:    0,
			MaxNodeCount:    5,
		},
		Locations: []string{"us-central1-b"},
	}
}

func markNapPoolStoppingWithUnschedulableNodes(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, poolName string) {
	t.Helper()
	stoppingPool := infra.Fakes.GkeService.MustGetNodePool(t, poolName)
	stoppingPool.Status = "STOPPING"

	initialNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	for i := range initialNodes.Items {
		n := &initialNodes.Items[i]
		if n.Labels[labels.GkeNodePoolLabel] == poolName {
			n.Spec.Unschedulable = true
			infra.Fakes.K8s.UpdateNode(n)
		}
	}
}

func assertStoppingPoolNotResized(t *testing.T, infra *integration.TestInfrastructure, poolName string, expectedSize int64) {
	t.Helper()
	stoppingPool := infra.Fakes.GkeService.MustGetNodePool(t, poolName)
	assert.Equal(t, "STOPPING", stoppingPool.Status)
	assert.Equal(t, expectedSize, infra.Fakes.GkeService.MustGetTargetSize(t, stoppingPool))
}

func triggerEmptyNapPoolDeletion(ctx context.Context, t *testing.T, autoscaler core.Autoscaler) {
	t.Helper()
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Second)
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)
}

func provisionAndScheduleArrivingNapPod(ctx context.Context, t *testing.T, autoscaler core.Autoscaler, infra *integration.TestInfrastructure) {
	t.Helper()
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 200*time.Millisecond)
	for range 4 {
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
	}
	infra.Fakes.RunScheduler(ctx, t)
}
