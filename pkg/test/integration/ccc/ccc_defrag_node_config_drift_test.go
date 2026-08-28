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
