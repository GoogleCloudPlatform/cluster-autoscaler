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
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	cataints "sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestCCCNodeConfigDriftActiveMigration tests that the nodeconfigdrift defrag plugin
// detects nodes that diverge from ComputeClass configuration and priority rules,
// and migrates them to a conforming node pool.
func TestCCCNodeConfigDriftActiveMigration(t *testing.T) {
	cccName := "drift-active-migration-ccc"

	// Define CCC with ConfigDrift enabled, required node label "tier: frontend", and n2 priority rule.
	cc := ccc.NewComputeClassBuilder(cccName).
		WithNodePoolConfig(&v1.NodePoolConfig{
			NodeLabels: map[string]string{
				"tier": "frontend",
			},
		}).
		AddPriority(v1.Priority{
			MachineFamily: ptr.To("n2"),
		}).
		WithConfigDrift(true).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cc).
		WithNodePools(
			// Target compliant node pool: size 1, n2-standard-2, tier=frontend.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-compliant"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(1),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
					"tier":                    "frontend",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// Source drifted node pool: size 1, e2-standard-2, missing "tier" label.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-drifted"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(1),
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
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
			integration.WithDefragEnabled("nodeconfigdrift"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		// Mock eviction controller: delete the evicted pod from fake client.
		infra.Fakes.KubeClient.PrependReactor("create", "pods", func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
			if action.GetSubresource() != "eviction" {
				return false, nil, nil
			}
			createAction, ok := action.(clientgotesting.CreateAction)
			if !ok {
				return false, nil, nil
			}
			eviction, ok := createAction.GetObject().(*policyv1beta1.Eviction)
			if !ok {
				return false, nil, nil
			}
			podName := eviction.Name
			namespace := eviction.Namespace

			go func() {
				err := infra.Fakes.KubeClient.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
				if err != nil {
					t.Logf("Eviction mock: failed to delete evicted pod %s/%s: %v", namespace, podName, err)
				}
			}()

			return true, eviction, nil
		})

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		if err != nil {
			return
		}

		// Verify initial nodes exist.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 2)

		var driftedNode *apiv1.Node
		for i := range nodes.Items {
			node := &nodes.Items[i]
			if node.Labels[labels.MachineFamilyLabel] == "e2" {
				driftedNode = node
			}
		}
		assert.NotNil(t, driftedNode, "Drifted node must be found")

		// Create a workload pod on the drifted node (60% CPU to prevent regular unneeded scale-down).
		pod := tu.BuildTestPod("workload-pod", 1200, 100)
		tu.SetRSPodSpec(pod, "rs")
		pod.Spec.NodeName = driftedNode.Name
		_, err = infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Run autoscaler iteration to trigger defrag nodeconfigdrift migration.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)

		// Poll for drifted node to be deleted.
		for i := 0; i < 20; i++ {
			time.Sleep(5 * time.Second)
			currentNodes, listErr := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			assert.NoError(t, listErr)
			if len(currentNodes.Items) == 1 {
				break
			}
		}

		// Verify that exactly 1 node remains and it belongs to the compliant node pool.
		remainingNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, remainingNodes.Items, 1, "Expected exactly 1 node remaining after drifted node is scaled down")
		assert.Equal(t, "n2", remainingNodes.Items[0].Labels[labels.MachineFamilyLabel], "Remaining node must be the compliant n2 node")
		assert.Equal(t, "frontend", remainingNodes.Items[0].Labels["tier"], "Remaining node must have the required tier label")
	})
}

// TestCCCNodeConfigDriftDisabled tests that when ConfigDrift is disabled,
// drifted nodes are not migrated.
func TestCCCNodeConfigDriftDisabled(t *testing.T) {
	cccName := "drift-disabled-ccc"

	// Define CCC with ConfigDrift explicitly disabled.
	cc := ccc.NewComputeClassBuilder(cccName).
		WithNodePoolConfig(&v1.NodePoolConfig{
			NodeLabels: map[string]string{
				"tier": "frontend",
			},
		}).
		AddPriority(v1.Priority{
			MachineFamily: ptr.To("n2"),
		}).
		WithConfigDrift(false).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cc).
		WithNodePools(
			// Compliant node pool: size 1.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-compliant"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(1),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
					"tier":                    "frontend",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// Drifted node pool: size 1, e2-standard-2, missing "tier" label.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-drifted"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(1),
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
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
			integration.WithDefragEnabled("nodeconfigdrift"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		if err != nil {
			return
		}

		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 2)

		var driftedNode *apiv1.Node
		for i := range nodes.Items {
			node := &nodes.Items[i]
			if node.Labels[labels.MachineFamilyLabel] == "e2" {
				driftedNode = node
			}
		}
		assert.NotNil(t, driftedNode)

		// Create workload pod on the drifted node.
		pod := tu.BuildTestPod("workload-pod", 1200, 100)
		tu.SetRSPodSpec(pod, "rs")
		pod.Spec.NodeName = driftedNode.Name
		_, err = infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Run autoscaler loop.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)

		// Verify that both nodes still remain because config drift active migration is disabled.
		remainingNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, remainingNodes.Items, 2, "Both nodes should remain when ConfigDrift is false")
	})
}

// TestCCCMinCapacityNodeConfigDriftActiveMigration tests that when a ComputeClass has
// minimumCapacity.targetNodeCount configured and a node is initially present in a
// drifted node pool (e.g. missing required ComputeClass labels), the nodeconfigdrift
// plugin actively migrates the node to the compliant node pool while preserving targetNodeCount.
func TestCCCMinCapacityNodeConfigDriftActiveMigration(t *testing.T) {
	cccName := "min-capacity-config-drift-ccc"

	// Define CCC with TargetNodeCount=1, ConfigDrift=true, required label "tier: frontend", and n2 priority.
	cc := ccc.NewComputeClassBuilder(cccName).
		WithTargetNodeCount(ptr.To(1)).
		WithNodePoolConfig(&v1.NodePoolConfig{
			NodeLabels: map[string]string{
				"tier": "frontend",
			},
		}).
		AddPriority(v1.Priority{MachineFamily: ptr.To("n2")}).
		WithConfigDrift(true).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithDefragEnabled("nodeconfigdrift"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
		).
		WithNodePools(
			// Compliant pool: n2, tier=frontend, initially empty (size 0)
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-compliant"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
					"tier":                    "frontend",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// Drifted pool: n2, missing "tier" label, initially has 1 node (size 1)
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-drifted"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(1),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
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

		// Mock eviction controller: delete the evicted pod from fake client.
		infra.Fakes.KubeClient.PrependReactor("create", "pods", func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
			if action.GetSubresource() != "eviction" {
				return false, nil, nil
			}
			createAction, ok := action.(clientgotesting.CreateAction)
			if !ok {
				return false, nil, nil
			}
			eviction, ok := createAction.GetObject().(*policyv1beta1.Eviction)
			if !ok {
				return false, nil, nil
			}
			podName := eviction.Name
			namespace := eviction.Namespace

			go func() {
				err := infra.Fakes.KubeClient.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
				if err != nil {
					t.Logf("Eviction mock: failed to delete evicted pod %s/%s: %v", namespace, podName, err)
				}
			}()

			return true, eviction, nil
		})

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		if err != nil {
			return
		}

		// Initial state: 1 node in drifted pool.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 1)
		assert.Empty(t, nodes.Items[0].Labels["tier"], "Initial node should be from drifted pool without tier label")

		// Create a workload pod on the drifted node (60% CPU).
		pod := tu.BuildTestPod("workload-pod", 1200, 100)
		tu.SetRSPodSpec(pod, "rs")
		pod.Spec.NodeName = nodes.Items[0].Name
		_, err = infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Run autoscaler loop to trigger nodeconfigdrift defrag migration.
		for i := 0; i < 4; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		}

		// Poll for drifted node to be deleted.
		for i := 0; i < 20; i++ {
			time.Sleep(5 * time.Second)
			currentNodes, listErr := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			assert.NoError(t, listErr)
			if len(currentNodes.Items) == 1 && currentNodes.Items[0].Labels["tier"] == "frontend" {
				break
			}
		}

		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)

		if !assert.Len(t, nodes.Items, 1, "Expected exactly 1 node after config drift active migration") {
			t.FailNow()
		}
		assert.Equal(t, "n2", nodes.Items[0].Labels[labels.MachineFamilyLabel], "Node should be in n2 pool")
		assert.Equal(t, "frontend", nodes.Items[0].Labels["tier"], "Node should have required tier label")
	})
}

// TestCCCNodeConfigDriftMaxNodeDisruption tests that when a ComputeClass has
// maxNodeDisruption configured, the nodeconfigdrift defrag plugin respects this limit
// by not scaling down more nodes than allowed concurrently.
func TestCCCNodeConfigDriftMaxNodeDisruption(t *testing.T) {
	cccName := "drift-max-disruption-ccc"

	// Define CCC with ConfigDrift enabled, required node label "tier: frontend", n2 priority rule, and MaxNodeDisruption=1.
	cc := ccc.NewComputeClassBuilder(cccName).
		WithNodePoolConfig(&v1.NodePoolConfig{
			NodeLabels: map[string]string{
				"tier": "frontend",
			},
		}).
		AddPriority(v1.Priority{
			MachineFamily: ptr.To("n2"),
		}).
		WithConfigDrift(true).
		WithMaxNodeDisruption(1).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cc).
		WithNodePools(
			// Target compliant node pool: size 1, n2-standard-2, tier=frontend.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-compliant"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(1),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
					"tier":                    "frontend",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// Source drifted node pool: size 2, e2-standard-2, missing "tier" label.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-drifted"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(2),
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
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
			integration.WithDefragEnabled("nodeconfigdrift"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		if err != nil {
			return
		}

		// Verify initial nodes exist. (1 compliant + 2 drifted = 3 nodes total)
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 3)

		var driftedNodes []*apiv1.Node
		var compliantNodes []*apiv1.Node
		for i := range nodes.Items {
			node := &nodes.Items[i]
			if node.Labels[labels.MachineFamilyLabel] == "e2" {
				driftedNodes = append(driftedNodes, node)
			} else {
				compliantNodes = append(compliantNodes, node)
			}
		}
		assert.Len(t, driftedNodes, 2, "2 drifted nodes must be found")
		assert.Len(t, compliantNodes, 1, "1 compliant node must be found")

		// Create a workload pod on ALL 3 nodes (60% CPU to prevent regular unneeded scale-down).
		// This ensures regular scale-down does not delete any nodes and consume the MaxNodeDisruption budget.
		for i, n := range nodes.Items {
			pod := tu.BuildTestPod(fmt.Sprintf("workload-pod-%d", i), 1200, 100)
			tu.SetRSPodSpec(pod, fmt.Sprintf("rs-%d", i))
			pod.Spec.NodeName = n.Name
			_, err = infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			assert.NoError(t, err)
		}

		// Run autoscaler loop 2 times:
		// Loop 1: scales up the replacement compliant node.
		// Loop 2: selects and taints exactly 1 drifted node for deletion while respecting MaxNodeDisruption=1.
		for i := 0; i < 2; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		}

		// Verify that exactly 1 drifted node is tainted for deletion.
		currentNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)

		taintedE2Count := 0
		untaintedE2Count := 0
		n2Count := 0

		for _, n := range currentNodes.Items {
			if n.Labels[labels.MachineFamilyLabel] == "n2" {
				n2Count++
			}
			if n.Labels[labels.MachineFamilyLabel] == "e2" {
				if cataints.HasToBeDeletedTaint(&n) {
					taintedE2Count++
				} else {
					untaintedE2Count++
				}
			}
		}

		assert.Equal(t, 1, taintedE2Count, "Expected exactly 1 drifted (e2) node to be tainted for deletion")
		assert.Equal(t, 1, untaintedE2Count, "Expected exactly 1 drifted (e2) node to remain untainted due to MaxNodeDisruption limit")
		assert.GreaterOrEqual(t, n2Count, 2, "Expected at least 1 replacement (n2) node to be scaled up, plus the 1 original compliant node")
	})
}

// TestCCCMinCapacityNodeConfigDriftMaxNodeDisruption tests that when a ComputeClass has
// minimumCapacity.targetNodeCount configured AND maxNodeDisruption is set, the nodeconfigdrift
// plugin actively migrates the nodes to the compliant node pool but respects the MaxNodeDisruption
// limit, scaling down only the allowed number of nodes concurrently while preserving targetNodeCount.
func TestCCCMinCapacityNodeConfigDriftMaxNodeDisruption(t *testing.T) {
	cccName := "min-capacity-drift-max-disruption-ccc"

	// Define CCC with TargetNodeCount=2, ConfigDrift=true, required label "tier: frontend", n2 priority, and MaxNodeDisruption=1.
	cc := ccc.NewComputeClassBuilder(cccName).
		WithTargetNodeCount(ptr.To(2)).
		WithNodePoolConfig(&v1.NodePoolConfig{
			NodeLabels: map[string]string{
				"tier": "frontend",
			},
		}).
		AddPriority(v1.Priority{MachineFamily: ptr.To("n2")}).
		WithConfigDrift(true).
		WithMaxNodeDisruption(1).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithDefragEnabled("nodeconfigdrift"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
		).
		WithNodePools(
			// Compliant pool: n2, tier=frontend, initially empty (size 0)
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-compliant"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
					"tier":                    "frontend",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// Drifted pool: n2, missing "tier" label, initially has 2 nodes (size 2)
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-drifted"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(2),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
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

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		if err != nil {
			return
		}

		// Initial state: 2 nodes in drifted pool.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 2)
		assert.Empty(t, nodes.Items[0].Labels["tier"], "Initial node should be from drifted pool without tier label")
		assert.Empty(t, nodes.Items[1].Labels["tier"], "Initial node should be from drifted pool without tier label")

		// Place a workload pod on BOTH drifted nodes (60% CPU).
		// Since the pods are not deleted (no eviction mock), whichever node is picked for deletion
		// will remain in DeletionsInProgress. The MaxNodeDisruption=1 limit should prevent
		// the second drifted node from being picked concurrently.
		for i, n := range nodes.Items {
			pod := tu.BuildTestPod(fmt.Sprintf("workload-pod-%d", i), 1200, 100)
			tu.SetRSPodSpec(pod, fmt.Sprintf("rs-%d", i))
			pod.Spec.NodeName = n.Name
			_, err = infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			assert.NoError(t, err)
		}

		// Run autoscaler loop 3 times:
		// Loop 1: scales up the replacement compliant nodes for minimum capacity.
		// Loop 2: compliant nodes become ready.
		// Loop 3: selects and taints exactly 1 drifted node for deletion while respecting MaxNodeDisruption=1.
		for i := 0; i < 3; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		}

		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)

		taintedDriftedCount := 0
		untaintedDriftedCount := 0
		compliantCount := 0
		for _, n := range nodes.Items {
			if n.Labels["tier"] == "frontend" {
				compliantCount++
			} else {
				if cataints.HasToBeDeletedTaint(&n) {
					taintedDriftedCount++
				} else {
					untaintedDriftedCount++
				}
			}
		}

		assert.GreaterOrEqual(t, compliantCount, 1, "Expected at least 1 compliant node to be scaled up as replacement")
		assert.Equal(t, 1, taintedDriftedCount, "Expected exactly 1 drifted node to be tainted for deletion")
		assert.Equal(t, 1, untaintedDriftedCount, "Expected exactly 1 drifted node to remain untainted due to MaxNodeDisruption limit")
	})
}

// TestCCCNodeConfigDriftAtomicGroupLabels tests that when AtomicGroupLabels is configured,
// drifted nodes are partitioned into atomic groups by their label values, and all nodes in
// a selected atomic group are migrated together as a unit without being truncated by candidate limit.
func TestCCCNodeConfigDriftAtomicGroupLabels(t *testing.T) {
	cccName := "drift-atomic-groups-ccc"

	// Define CCC with ConfigDrift enabled, required node label "tier: frontend", n2 priority rule,
	// and AtomicGroupLabels set to ["partition"].
	cc := ccc.NewComputeClassBuilder(cccName).
		WithNodePoolConfig(&v1.NodePoolConfig{
			NodeLabels: map[string]string{
				"tier": "frontend",
			},
		}).
		AddPriority(v1.Priority{
			MachineFamily: ptr.To("n2"),
		}).
		WithConfigDrift(true).
		WithAtomicGroupLabels("partition").
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cc).
		WithNodePools(
			// Target compliant node pool: size 1, n2-standard-2, tier=frontend.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-compliant"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(1),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
					"tier":                    "frontend",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// Source drifted node pool: size 4, e2-standard-2, missing "tier" label.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-drifted"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(4),
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
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
			integration.WithDefragEnabled("nodeconfigdrift"),
			// Set DefragCandidateLimit to 1 to verify that atomic groups retain all nodes (2 nodes)
			// rather than being capped by candidate node limit.
			integration.WithDefragCandidateLimit(1),
			integration.WithMaxDrainParallelism(10),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		if err != nil {
			return
		}

		// Initial nodes: 1 compliant + 4 drifted = 5 nodes total.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 5)

		var driftedNodes []*apiv1.Node
		for i := range nodes.Items {
			node := &nodes.Items[i]
			if node.Labels[labels.MachineFamilyLabel] == "e2" {
				driftedNodes = append(driftedNodes, node)
			}
		}
		assert.Len(t, driftedNodes, 4, "4 drifted nodes must be found")

		// Partition the 4 drifted nodes into two atomic groups:
		// 2 nodes with partition=part-a, and 2 nodes with partition=part-b.
		for i, n := range driftedNodes {
			if i < 2 {
				n.Labels["partition"] = "part-a"
			} else {
				n.Labels["partition"] = "part-b"
			}
			_, err = infra.Fakes.KubeClient.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{})
			assert.NoError(t, err)
		}
		synctest.Wait()

		// Create workload pods on all 5 nodes (60% CPU) to prevent unneeded regular scale-down.
		currentNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		for i, n := range currentNodes.Items {
			pod := tu.BuildTestPod(fmt.Sprintf("workload-pod-%d", i), 1200, 100)
			tu.SetRSPodSpec(pod, fmt.Sprintf("rs-%d", i))
			pod.Spec.NodeName = n.Name
			_, err = infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			assert.NoError(t, err)
		}

		// Run autoscaler loop 2 times:
		// Loop 1: defrag selects one atomic group (2 nodes with same partition label),
		//         retaining both nodes despite DefragCandidateLimit=1, and triggers scale-up of 2 replacement nodes.
		// Loop 2: replacement nodes become ready, and the 2 drifted nodes in that atomic group are tainted for deletion.
		for i := 0; i < 2; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		}

		afterNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)

		taintedDriftedPartitions := make(map[string]int)
		untaintedDriftedPartitions := make(map[string]int)
		compliantCount := 0

		for _, n := range afterNodes.Items {
			if n.Labels[labels.MachineFamilyLabel] == "n2" {
				compliantCount++
			} else if n.Labels[labels.MachineFamilyLabel] == "e2" {
				part := n.Labels["partition"]
				if cataints.HasToBeDeletedTaint(&n) {
					taintedDriftedPartitions[part]++
				} else {
					untaintedDriftedPartitions[part]++
				}
			}
		}

		// Exactly 2 replacement compliant nodes should be scaled up (1 original + 2 replacement = 3 total n2 nodes).
		assert.GreaterOrEqual(t, compliantCount, 3, "Expected at least 2 replacement compliant nodes scaled up for the atomic group")

		// Exactly one partition should be tainted, with exactly 2 nodes tainted.
		assert.Len(t, taintedDriftedPartitions, 1, "Exactly 1 atomic partition must be selected for migration")
		var selectedPartition string
		for part, count := range taintedDriftedPartitions {
			selectedPartition = part
			assert.Equal(t, 2, count, "Both nodes in the selected atomic partition must be tainted together")
		}

		// The other partition must remain completely untainted (2 nodes).
		assert.Len(t, untaintedDriftedPartitions, 1, "The non-selected atomic partition must remain untainted")
		for part, count := range untaintedDriftedPartitions {
			assert.NotEqual(t, selectedPartition, part, "Untainted nodes must belong to the other partition")
			assert.Equal(t, 2, count, "Both nodes of the other partition must remain untainted")
		}
	})
}

// TestCCCNodeConfigDriftAtomicGroupExceedsMaxNodeDisruption tests that when an atomic group's
// size exceeds the MaxNodeDisruption limit, the candidate is rejected as an all-or-nothing unit,
// and config drift migration does not occur.
func TestCCCNodeConfigDriftAtomicGroupExceedsMaxNodeDisruption(t *testing.T) {
	cccName := "drift-atomic-exceeds-disruption-ccc"

	// Define CCC with ConfigDrift enabled, required node label "tier: frontend", n2 priority rule,
	// AtomicGroupLabels set to ["partition"], and MaxNodeDisruption set to 1.
	cc := ccc.NewComputeClassBuilder(cccName).
		WithNodePoolConfig(&v1.NodePoolConfig{
			NodeLabels: map[string]string{
				"tier": "frontend",
			},
		}).
		AddPriority(v1.Priority{
			MachineFamily: ptr.To("n2"),
		}).
		WithConfigDrift(true).
		WithAtomicGroupLabels("partition").
		WithMaxNodeDisruption(1).
		Build()

	testConfig := integration.NewTestConfig().
		WithCccCrds(cc).
		WithNodePools(
			// Target compliant node pool: size 1, n2-standard-2, tier=frontend.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-compliant"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(1),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
					"tier":                    "frontend",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// Source drifted node pool: size 2, e2-standard-2, missing "tier" label.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-drifted"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(2),
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
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
			integration.WithDefragEnabled("nodeconfigdrift"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		if err != nil {
			return
		}

		// Initial nodes: 1 compliant + 2 drifted = 3 nodes total.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 3)

		var driftedNodes []*apiv1.Node
		for i := range nodes.Items {
			node := &nodes.Items[i]
			if node.Labels[labels.MachineFamilyLabel] == "e2" {
				driftedNodes = append(driftedNodes, node)
			}
		}
		assert.Len(t, driftedNodes, 2, "2 drifted nodes must be found")

		// Both drifted nodes share partition=part-a, forming an atomic group of size 2.
		for _, n := range driftedNodes {
			n.Labels["partition"] = "part-a"
			_, err = infra.Fakes.KubeClient.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{})
			assert.NoError(t, err)
		}
		synctest.Wait()

		// Create workload pods on all 3 nodes (60% CPU) to prevent unneeded regular scale-down.
		currentNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		for i, n := range currentNodes.Items {
			pod := tu.BuildTestPod(fmt.Sprintf("workload-pod-%d", i), 1200, 100)
			tu.SetRSPodSpec(pod, fmt.Sprintf("rs-%d", i))
			pod.Spec.NodeName = n.Name
			_, err = infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			assert.NoError(t, err)
		}

		// Run autoscaler loop.
		// Since the atomic group requires 2 nodes to be disrupted, but MaxNodeDisruption=1,
		// the atomic candidate must be rejected by the disruption tracker and no migration should occur.
		for i := 0; i < 2; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		}

		afterNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, afterNodes.Items, 3, "No new nodes should be scaled up and no nodes should be deleted")

		for _, n := range afterNodes.Items {
			assert.False(t, cataints.HasToBeDeletedTaint(&n), "No node should be tainted for deletion: %s", n.Name)
		}
	})
}
