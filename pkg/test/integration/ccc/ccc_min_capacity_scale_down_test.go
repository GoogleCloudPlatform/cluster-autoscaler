/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ccc_test

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestCCCScaleDown(t *testing.T) {
	testCases := []struct {
		name              string
		experimentEnabled bool
		// specMinCapacity and priorityMinCapacity set the ComputeClass
		// spec-level and priority-level min capacity, if not nil.
		specMinCapacity            *int
		priorityMinCapacity        *int
		nodesTaintedForDeletion    int
		nodesWithDeletionTimestamp int
		expectedNodeCount          int
		failureMessage             string
	}{
		{
			name:              "prevents scale down below targetNodeCount",
			experimentEnabled: true,
			specMinCapacity:   ptr.To(5),
			expectedNodeCount: 5,
			failureMessage:    "Expected exactly 5 nodes to remain in the cluster due to the min capacity",
		},
		{
			// Nodes in deletion don't count toward the min capacity, so 5 other
			// nodes are kept.
			name:                       "scale down limits account for nodes in deletion",
			experimentEnabled:          true,
			specMinCapacity:            ptr.To(5),
			nodesTaintedForDeletion:    2,
			nodesWithDeletionTimestamp: 1,
			expectedNodeCount:          5,
			failureMessage:             "Expected exactly 5 nodes not in deletion to remain because capacity should subtract nodes in deletion",
		},
		{
			name:              "scale down not prevented when disabled by experiment",
			experimentEnabled: false,
			specMinCapacity:   ptr.To(5),
			expectedNodeCount: 1,
			failureMessage:    "Expected node count to drop to 1 because the minimum capacity feature is disabled by the experiment flag",
		},
		{
			// No spec-level min capacity fake pods hold these nodes: only
			// ComputeClassMinCapacityProcessor keeps the priority-level floor.
			name:                "prevents scale down below a priority-level targetNodeCount",
			experimentEnabled:   true,
			priorityMinCapacity: ptr.To(5),
			expectedNodeCount:   5,
			failureMessage:      "Expected exactly 5 nodes to remain in the cluster due to the priority-level min capacity",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ccBuilder := ccc.NewComputeClassBuilder("my-ccc")
			if tc.specMinCapacity != nil {
				ccBuilder.WithTargetNodeCount(tc.specMinCapacity)
			}
			if tc.priorityMinCapacity != nil {
				ccBuilder.AddPriority(v1.Priority{
					MachineFamily:   ptr.To("n1"),
					MinimumCapacity: &v1.MinimumCapacity{TargetNodeCount: tc.priorityMinCapacity},
				})
			}
			cc := ccBuilder.Build()

			nodePool := integration.DefaultNodePool(
				integration.WithNodePoolName("default-pool"),
				integration.WithNodePoolMachineType("n1-standard-1"),
				integration.WithNodePoolSize(10),
				integration.WithNodePoolLocations("us-central1-b"),
			)
			nodePool.Autoscaling.MinNodeCount = 1

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePool).
				WithCccCrds(cc).
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Second),
					integration.WithComputeClassMinCapacityEnabled(),
				)

			if !tc.experimentEnabled {
				testConfig.WithExperimentOverrides(
					map[string]bool{"ComputeClassMinCapacity::Enabled": false},
					map[string]string{},
				)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Label all auto-generated nodes with our ComputeClass label so the
				// min-capacity processor applies to them.
				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, 10, len(nodes.Items))

				for _, node := range nodes.Items {
					if node.Labels == nil {
						node.Labels = make(map[string]string)
					}
					node.Labels["cloud.google.com/compute-class"] = "my-ccc"
					if tc.priorityMinCapacity != nil {
						metav1.SetMetaDataAnnotation(&node.ObjectMeta, gke_labels.CCCPriorityIndexAnnotationKey, "0")
					}
					infra.Fakes.K8s.UpdateNode(&node)
				}

				// Run the autoscaler loop once to identify unneeded nodes.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 0)

				// Sleep inside the synctest bubble to advance virtual clock past the ScaleDownUnneededTime.
				time.Sleep(5 * time.Second)

				if tc.nodesTaintedForDeletion > 0 || tc.nodesWithDeletionTimestamp > 0 {
					freshNodes, listErr := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
					assert.NoError(t, listErr)

					for i := 0; i < tc.nodesTaintedForDeletion; i++ {
						if _, err := taints.MarkToBeDeleted(ctx, &freshNodes.Items[i], infra.Fakes.KubeClient, false /* cordonNode */); err != nil {
							t.Fatalf("MarkToBeDeleted(%s) failed: %v", freshNodes.Items[i].Name, err)
						}
					}
					for i := 0; i < tc.nodesWithDeletionTimestamp; i++ {
						infra.Fakes.K8s.UpdateNode(withDeletionTimestamp(freshNodes.Items[tc.nodesTaintedForDeletion+i]))
					}
				}

				// Run the autoscaler loop again to trigger scale down.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 0)

				// Wait for the node count to drop to the expected level.
				var remaining int
				for i := 0; i < 20; i++ {
					time.Sleep(5 * time.Second)
					remaining = countNodesNotInDeletion(ctx, t, infra)
					if remaining == tc.expectedNodeCount {
						break
					}
				}

				assert.Equal(t, tc.expectedNodeCount, remaining, tc.failureMessage)
			})
		})
	}
}

// countNodesNotInDeletion returns the number of nodes that are neither tainted
// for deletion nor have a deletion timestamp.
func countNodesNotInDeletion(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure) int {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Nodes().List() failed: %v", err)
	}
	count := 0
	for _, node := range nodes.Items {
		if node.DeletionTimestamp == nil && !taints.HasToBeDeletedTaint(&node) {
			count++
		}
	}
	return count
}

// withDeletionTimestamp returns a copy of the node as a pointer, with DeletionTimestamp set.
func withDeletionTimestamp(node apiv1.Node) *apiv1.Node {
	now := metav1.Now()
	node.DeletionTimestamp = &now
	return &node
}

// TestCCCScaleDownAtomicGroups reproduces b/551773344: reducing a ComputeClass
// minimumCapacity.targetNodeCount for atomic (multi-host TPU) node pools must
// scale the CC down to the target by removing whole slices rather than getting
// stuck. The bug was that the per-node min capacity quota split the allowance
// non-deterministically across the slices, so AtomicResizeFilteringProcessor
// rejected every slice and nothing scaled down.
//
// Setup: two 16-node multi-host TPU slices (32 nodes) under one CC with
// targetNodeCount=16. All nodes are idle. With the fix, exactly one whole slice
// is removed (16 nodes remain). With the experiment disabled there is no
// minimum-capacity floor, so both idle slices are removed.
func TestCCCScaleDownAtomicGroups(t *testing.T) {
	testCases := []struct {
		name              string
		experimentEnabled bool
		expectedNodeCount int
		// convergenceCycles is the exact number of autoscaler cycles needed to
		// reach expectedNodeCount. Scale-down is deterministic under synctest:
		// the first cycle marks the idle nodes unneeded (they must stay unneeded
		// for ScaleDownUnneededTime=1s), then one whole atomic slice is removed
		// per subsequent cycle.
		convergenceCycles int
		failureMessage    string
	}{
		{
			name:              "removes one whole slice down to targetNodeCount",
			experimentEnabled: true,
			expectedNodeCount: 16,
			// cycle 1: mark unneeded; cycle 2: remove one slice (32->16). The
			// min-capacity floor then blocks removing the last slice.
			convergenceCycles: 2,
			failureMessage:    "Expected exactly 16 nodes (one 16-node slice) to remain due to the atomic minimum-capacity floor",
		},
		{
			name:              "scale down not prevented when disabled by experiment",
			experimentEnabled: false,
			expectedNodeCount: 0,
			// cycle 1: mark unneeded; cycle 2: remove first slice (32->16);
			// cycle 3: remove second slice (16->0). No floor when disabled.
			convergenceCycles: 3,
			failureMessage:    "Expected all idle atomic nodes to be removed when the minimum-capacity feature is disabled",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder("my-ccc").
				WithTargetNodeCount(ptr.To(16)).
				Build()

			testConfig := integration.NewTestConfig().
				WithNodePools(
					atomicTPUNodePool("tpu-slice-1", "my-ccc"),
					atomicTPUNodePool("tpu-slice-2", "my-ccc"),
				).
				WithCccCrds(cc).
				WithOverrides(
					integration.WithScaleDownUnneededTime(time.Second),
					integration.WithComputeClassMinCapacityEnabled(),
				)

			if !tc.experimentEnabled {
				testConfig.WithExperimentOverrides(
					map[string]bool{"ComputeClassMinCapacity::Enabled": false},
					map[string]string{},
				)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Two 16-node slices should be present up-front.
				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, 32, len(nodes.Items), "expected 32 pre-existing atomic TPU nodes")

				// Ensure every node carries the ComputeClass label so the
				// min-capacity processor applies to them.
				for _, node := range nodes.Items {
					if node.Labels == nil {
						node.Labels = make(map[string]string)
					}
					node.Labels["cloud.google.com/compute-class"] = "my-ccc"
					infra.Fakes.K8s.UpdateNode(&node)
				}

				for i := 0; i < tc.convergenceCycles; i++ {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
				}

				remaining, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNodeCount, len(remaining.Items), tc.failureMessage)

				// One further cycle must not change the result.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
				remaining, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedNodeCount, len(remaining.Items), tc.failureMessage)
			})
		})
	}
}

// TestCCCScaleDownAtomicGroupsKeepsSliceWithRunningPods verifies that, with a
// ComputeClass min capacity of one slice and two atomic slices, scale-down
// removes the idle slice and keeps the slice running workload pods, whichever
// slice comes first by name and however many of its nodes run pods. With
// ComputeClassMinCapacityDefragOnlyFakePods disabled, the min capacity fake pods
// are in the snapshot during scale-down, so this also checks where they go.
func TestCCCScaleDownAtomicGroupsKeepsSliceWithRunningPods(t *testing.T) {
	const (
		cccName     = "my-ccc"
		sliceNodes  = 16
		firstSlice  = "tpu-slice-1"
		secondSlice = "tpu-slice-2"
	)
	testCases := []struct {
		name         string
		occupiedPool string
		idlePool     string
		// busyNodes is the number of nodes of occupiedPool running a workload pod.
		busyNodes int
	}{
		{
			name:         "workload on every node of the first slice by name",
			occupiedPool: firstSlice,
			idlePool:     secondSlice,
			busyNodes:    sliceNodes,
		},
		{
			name:         "workload on every node of the last slice by name",
			occupiedPool: secondSlice,
			idlePool:     firstSlice,
			busyNodes:    sliceNodes,
		},
		{
			name:         "workload on some nodes of the last slice by name",
			occupiedPool: secondSlice,
			idlePool:     firstSlice,
			busyNodes:    4,
		},
	}
	for _, tc := range testCases {
		for _, defragOnlyFakePods := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s, defrag-only fake pods %t", tc.name, defragOnlyFakePods), func(t *testing.T) {
				cc := ccc.NewComputeClassBuilder(cccName).
					WithTargetNodeCount(ptr.To(sliceNodes)).
					Build()
				testConfig := integration.NewTestConfig().
					WithNodePools(
						atomicTPUNodePool(firstSlice, cccName),
						atomicTPUNodePool(secondSlice, cccName),
					).
					WithCccCrds(cc).
					WithOverrides(
						integration.WithScaleDownUnneededTime(time.Second),
						integration.WithComputeClassMinCapacityEnabled(),
					).
					WithExperimentOverrides(
						map[string]bool{experiments.ComputeClassMinCapacityDefragOnlyFakePodsEnabledFlag: defragOnlyFakePods},
						map[string]string{},
					)

				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer integration_synctest.TearDown(cancel)
					infra := integration.SetupInfrastructure(ctx, t)
					autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
					if err != nil {
						t.Fatalf("SetupAutoscaler() failed: %v", err)
					}

					nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
					if err != nil {
						t.Fatalf("Nodes().List() failed: %v", err)
					}
					nodesByPool := map[string][]string{}
					for _, node := range nodes.Items {
						pool := node.Labels[gke_labels.GkeNodePoolLabel]
						nodesByPool[pool] = append(nodesByPool[pool], node.Name)
					}
					if len(nodesByPool[tc.occupiedPool]) != sliceNodes || len(nodesByPool[tc.idlePool]) != sliceNodes {
						t.Fatalf("got %d nodes in %s and %d in %s, want %d in each", len(nodesByPool[tc.occupiedPool]), tc.occupiedPool, len(nodesByPool[tc.idlePool]), tc.idlePool, sliceNodes)
					}
					occupiedNodes := slices.Sorted(slices.Values(nodesByPool[tc.occupiedPool]))
					// The node pool selector keeps the pods from moving to the idle slice,
					// so the occupied slice can't be removed.
					for i, nodeName := range occupiedNodes[:tc.busyNodes] {
						infra.Fakes.K8s.AddPod(tu.BuildTestPod(fmt.Sprintf("real-pod-%d", i), 100, 100*1024*1024,
							tu.WithNodeName(nodeName),
							pod.WithCCC(cccName),
							pod.WithNodeSelectorEntry(gke_labels.GkeNodePoolLabel, tc.occupiedPool),
						))
					}

					// The first loop marks the idle slice unneeded, the second removes it.
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
					// A further loop must not remove the occupied slice.
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

					remaining, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
					if err != nil {
						t.Fatalf("Nodes().List() failed: %v", err)
					}
					var remainingNames []string
					for _, n := range remaining.Items {
						remainingNames = append(remainingNames, n.Name)
					}
					assert.ElementsMatch(t, occupiedNodes, remainingNames, "remaining nodes; want exactly the nodes of %s, with %s removed", tc.occupiedPool, tc.idlePool)
				})
			})
		}
	}
}

// TestCCCMinCapacityDefragCreateBeforeDelete recycles, with create-before-delete
// defrag, one node (or one NAP-provisioned TPU slice) of a ComputeClass sitting
// on its min capacity, while the replacement is not registered with k8s.
// Neither defrag nor scale-down may count the upcoming replacement towards the
// min capacity: all initial nodes must stay until the replacement registers,
// if ever. Once it does, the recycled nodes go.
func TestCCCMinCapacityDefragCreateBeforeDelete(t *testing.T) {
	const (
		cccName       = "my-ccc"
		tpuPolicyName = "test-policy"
	)
	testCases := []struct {
		name string
		// configure adds the case-specific setup to the shared test config.
		configure func(*integration.TestConfig) *integration.TestConfig
		// resourcePolicy, if set, is registered in the fake GCE before CA starts.
		resourcePolicy *gceclient.GceResourcePolicy
		minCapacity    int
		// recycled selects the nodes annotated for defrag; last is the last node by name.
		recycled func(node, last *apiv1.Node) bool
	}{
		{
			name: "non-atomic",
			configure: func(c *integration.TestConfig) *integration.TestConfig {
				return c.
					WithNodePools(integration.DefaultNodePool(
						integration.WithNodePoolName("pool"),
						integration.WithNodePoolMachineType("n1-standard-1"),
						integration.WithNodePoolSize(3),
						integration.WithNodePoolMin(0),
						integration.WithNodePoolMax(4),
						integration.WithNodePoolLocations("us-central1-b"),
						integration.WithNodePoolLabels(map[string]string{gke_labels.ComputeClassLabel: cccName}),
						integration.WithNodePoolAutoscalingEnabled(true),
					)).
					WithCccCrds(ccc.NewComputeClassBuilder(cccName).
						WithTargetNodeCount(ptr.To(3)).
						AddPriority(v1.Priority{MachineType: ptr.To("n1-standard-1")}).
						Build())
			},
			minCapacity: 3,
			recycled: func(node, last *apiv1.Node) bool {
				return node.Name == last.Name
			},
		},
		{
			// NAP provisions two 16-node PROVISION_ONLY tpu7x slices for the
			// min capacity through the ComputeClass placement (workload) policy.
			name: "atomic TPU slices provisioned by NAP",
			configure: func(c *integration.TestConfig) *integration.TestConfig {
				return c.
					WithExperiments(experiments.ResourcePolicyPullerFlag).
					WithOverrides(
						integration.WithAutoProvisioningEnabled(),
						integration.WithCompactPlacementEnabled(true),
						// Let a candidate cover a whole 16-node slice.
						integration.WithDefragCandidateNodeLimit(16),
					).
					WithClusterOverrides(
						integration.WithClusterAutoProvisioningEnabled(),
						integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
							{ResourceType: "cpu", Maximum: integration.DefaultMaxCoresResourceLimit},
							{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
							{ResourceType: "tpu7x", Maximum: 1000000},
						}),
					).
					WithCccCrds(ccc.NewComputeClassBuilder(cccName).
						WithNapEnabled().
						WithTargetNodeCount(ptr.To(32)).
						WithPriorities(v1.Priority{
							MachineFamily: ptr.To("tpu7x"),
							Tpu:           &v1.TPU{Type: "tpu7x", Count: 4, Topology: "4x4x4"},
							Placement:     &v1.Placement{PolicyName: tpuPolicyName},
						}).
						Build())
			},
			resourcePolicy: &gceclient.GceResourcePolicy{
				Name:   tpuPolicyName,
				Status: "READY",
				WorkloadPolicy: gceclient.WorkloadPolicy{
					AcceleratorTopology:     "4x4x4",
					AcceleratorTopologyMode: gceclient.AcceleratorTopologyModeProvisionOnly,
				},
			},
			minCapacity: 32,
			recycled: func(node, last *apiv1.Node) bool {
				return node.Labels[gke_labels.GkeNodePoolLabel] == last.Labels[gke_labels.GkeNodePoolLabel]
			},
		},
	}
	registrations := []struct {
		name string
		// registersAfter is the number of loops after which the replacement
		// registers with k8s; 0 means never.
		registersAfter int
	}{
		{name: "replacement never registers"},
		{name: "replacement registers late", registersAfter: 4},
	}
	for _, tc := range testCases {
		for _, registration := range registrations {
			t.Run(tc.name+"/"+registration.name, func(t *testing.T) {
				testConfig := tc.configure(integration.NewTestConfig().
					WithOverrides(
						integration.WithComputeClassMinCapacityEnabled(),
						integration.WithDefragEnabled("annotation"),
						integration.WithDefragCandidateLimit(16),
						integration.WithMaxDrainParallelism(16),
						integration.WithMaxScaleDownParallelism(16),
						integration.WithScaleDownUnneededTime(time.Second),
						integration.WithMaxNodeProvisionTime(15*time.Minute),
					))

				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					infra := integration.SetupInfrastructure(ctx, t)
					if tc.resourcePolicy != nil {
						infra.Fakes.GceService.WithResourcePolicies(tc.resourcePolicy)
					}
					autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
					assert.NoError(t, err)
					defer integration_synctest.TearDown(cancel)

					// Wait for the min capacity to be in place (NAP provisions it over a few loops).
					listNodes := func() []apiv1.Node {
						nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
						assert.NoError(t, err)
						return nodes.Items
					}
					for i := 0; i < 10 && len(listNodes()) < tc.minCapacity; i++ {
						integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
					}
					nodes := listNodes()
					if !assert.Len(t, nodes, tc.minCapacity, "expected the ComputeClass to sit exactly on its min capacity") {
						return
					}

					sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
					last := nodes[len(nodes)-1].DeepCopy()
					initial := map[string]bool{}
					recycled := map[string]bool{}
					for _, node := range nodes {
						if tc.recycled(&node, last) {
							metav1.SetMetaDataAnnotation(&node.ObjectMeta, "defrag.cluster-autoscaler.kubernetes.io", "create-before-delete")
							infra.Fakes.K8s.UpdateNode(&node)
							recycled[node.Name] = true
						}
						initial[node.Name] = true
					}

					// Defrag picks the candidate and provisions a replacement, which the
					// fake registers right away: drop its Node objects so that it stays
					// upcoming until it registers.
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
					nodes = listNodes()
					assert.Greater(t, len(nodes), len(initial), "expected the defrag replacement to be created")
					var replacement []apiv1.Node
					for _, node := range nodes {
						if !initial[node.Name] {
							replacement = append(replacement, node)
							infra.Fakes.K8s.DeleteNode(node.Name)
						}
					}

					unregisteredLoops := 8
					if registration.registersAfter > 0 {
						unregisteredLoops = registration.registersAfter
					}
					for i := 0; i < unregisteredLoops; i++ {
						integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
						present := map[string]bool{}
						for _, node := range listNodes() {
							present[node.Name] = true
						}
						for name := range initial {
							assert.True(t, present[name], "node %s removed in loop %d while the replacement is not registered", name, i)
						}
					}
					if registration.registersAfter == 0 {
						return
					}

					// Once the replacement registers, the recycled nodes can go.
					for i := range replacement {
						infra.Fakes.K8s.AddNode(&replacement[i])
					}
					for i := 0; i < 10; i++ {
						// Defrag and the main scale-down can both release the recycled
						// node in the loop the replacement registers: defrag starts its
						// deletion first, and the main scale-down, which filters ongoing
						// deletions with the actuation status taken at the start of the
						// loop, picks the same node and fails to taint it. CA logs the
						// error and retries on the next loop, so the test tolerates it
						// and checks the outcome instead. See b/573569100.
						if err := integration_synctest.RunOnceAfter(ctx, t, autoscaler, 30*time.Second); err != nil {
							assert.ErrorContains(t, err, "couldn't taint", "unexpected error in loop %d after the replacement registered", i)
						}
						nodes = listNodes()
						assert.GreaterOrEqual(t, len(nodes), tc.minCapacity, "ComputeClass below its min capacity in loop %d after the replacement registered", i)
						if !slices.ContainsFunc(nodes, func(node apiv1.Node) bool { return recycled[node.Name] }) {
							break
						}
					}
					for _, node := range nodes {
						assert.False(t, recycled[node.Name], "recycled node %s not removed after the replacement registered", node.Name)
					}
					assert.Len(t, nodes, tc.minCapacity, "expected the ComputeClass to end on its min capacity")
				})
			})
		}
	}
}

// TestCCCMinCapacityLoweringMinCapacityRemovesIdleNodes verifies that lowering
// the min capacity of an idle ComputeClass removes the surplus nodes in the
// first scale-down loop, or after ScaleDownUnneededTime when
// ComputeClassMinCapacityDefragOnlyFakePods is disabled.
func TestCCCMinCapacityLoweringMinCapacityRemovesIdleNodes(t *testing.T) {
	const (
		cccName            = "my-ccc"
		initialMinCapacity = 5
		loweredMinCapacity = 3
		unneededTime       = 10 * time.Minute
	)
	testCases := []struct {
		name                    string
		defragOnlyFakePods      bool
		wantNodesAfterFirstLoop int
	}{
		{
			name:                    "min capacity enforced by counting: the surplus goes in the first loop",
			defragOnlyFakePods:      true,
			wantNodesAfterFirstLoop: loweredMinCapacity,
		},
		{
			name:                    "min capacity held by fake pods: the surplus waits for ScaleDownUnneededTime",
			defragOnlyFakePods:      false,
			wantNodesAfterFirstLoop: initialMinCapacity,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder(cccName).
				WithTargetNodeCount(ptr.To(initialMinCapacity)).
				AddPriority(v1.Priority{MachineType: ptr.To("n1-standard-1")}).
				Build()
			testConfig := integration.NewTestConfig().
				WithNodePools(integration.DefaultNodePool(
					integration.WithNodePoolName("pool"),
					integration.WithNodePoolMachineType("n1-standard-1"),
					integration.WithNodePoolSize(initialMinCapacity),
					integration.WithNodePoolMin(0),
					integration.WithNodePoolMax(initialMinCapacity),
					integration.WithNodePoolLocations("us-central1-b"),
					integration.WithNodePoolLabels(map[string]string{gke_labels.ComputeClassLabel: cccName}),
					integration.WithNodePoolAutoscalingEnabled(true),
				)).
				WithCccCrds(cc).
				WithOverrides(
					integration.WithComputeClassMinCapacityEnabled(),
					integration.WithScaleDownUnneededTime(unneededTime),
				).
				WithExperimentOverrides(
					map[string]bool{experiments.ComputeClassMinCapacityDefragOnlyFakePodsEnabledFlag: tc.defragOnlyFakePods},
					map[string]string{},
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				if err != nil {
					t.Fatalf("SetupAutoscaler() failed: %v", err)
				}

				assert.Equal(t, initialMinCapacity, countNodes(ctx, t, infra), "expected the ComputeClass to start on its min capacity")

				// The idle min capacity nodes sit well past ScaleDownUnneededTime
				// without being touched.
				for range 3 {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unneededTime)
				}
				assert.Equal(t, initialMinCapacity, countNodes(ctx, t, infra), "the min capacity must keep every node")

				// Lower the min capacity the way a user would. The spec is taken from the
				// object the test created rather than from the fake API server: the
				// fake applies status updates to the whole object, and the CCC
				// conditions are written with an empty spec.
				lowered := cc.DeepCopy()
				lowered.Spec.MinimumCapacity.TargetNodeCount = ptr.To(loweredMinCapacity)
				integration.UpdateComputeClass(ctx, t, infra, lowered)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
				time.Sleep(5 * time.Second)
				assert.Equal(t, tc.wantNodesAfterFirstLoop, countNodes(ctx, t, infra), "unexpected node count in the first loop after lowering the min capacity")

				// Either way the ComputeClass converges to the new min capacity once
				// the surplus nodes have been unneeded for ScaleDownUnneededTime.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unneededTime+time.Minute)
				time.Sleep(5 * time.Second)
				assert.Equal(t, loweredMinCapacity, countNodes(ctx, t, infra), "expected the ComputeClass to converge to the lowered min capacity")

				// A further loop must not go below the new min capacity.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unneededTime+time.Minute)
				assert.Equal(t, loweredMinCapacity, countNodes(ctx, t, infra), "the lowered min capacity must keep the remaining nodes")
			})
		})
	}
}

// atomicTPUNodePool builds a pre-existing multi-host TPU node pool (one 16-node
// 4x4x4 tpu7x slice) that the GKE client converts into an atomic
// (ZeroOrMaxNodeScaling) MIG. The 4x4x4 topology with 4 chips per node yields
// 64/4 = 16 nodes per slice.
func atomicTPUNodePool(name, cccName string) *gke_api_beta.NodePool {
	return &gke_api_beta.NodePool{
		Name: name,
		Config: &gke_api_beta.NodeConfig{
			MachineType: "tpu7x-standard-4t",
			ImageType:   "cos_containerd",
			Labels: map[string]string{
				"cloud.google.com/compute-class": cccName,
			},
		},
		InitialNodeCount: 16,
		Autoscaling: &gke_api_beta.NodePoolAutoscaling{
			Enabled:      true,
			MinNodeCount: 0,
			MaxNodeCount: 16,
		},
		Locations: []string{"us-central1-b"},
		PlacementPolicy: &gke_api_beta.PlacementPolicy{
			TpuTopology: "4x4x4",
		},
	}
}
