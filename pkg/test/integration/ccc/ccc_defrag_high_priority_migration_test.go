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

package ccc

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
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestCCCMinCapacityDefragPrevention tests that Defrag's High Priority Migration
// plugin respects the TargetNodeCount (Minimum Capacity) limits of lower priority rules,
// preventing Defrag from scaling down a node group below its minimum capacity limit (TargetNodeCount=5).
func TestCCCMinCapacityDefragPrevention(t *testing.T) {
	// cc has n2 as optimal (P1, index 0, min 10) and e2 as non-optimal (P2, index 1, min 5).
	cc := NewComputeClassBuilder("my-ccc").
		AddPriority(v1.Priority{
			MachineFamily: ptr.To("n2"),
			MinimumCapacity: &v1.MinimumCapacity{
				TargetNodeCount: ptr.To(10),
			},
		}).
		AddPriority(v1.Priority{
			MachineFamily: ptr.To("e2"),
			MinimumCapacity: &v1.MinimumCapacity{
				TargetNodeCount: ptr.To(5),
			},
		}).
		Build()

	// Explicitly enable OptimizeRulePriority for Defrag high-priority-migration to run.
	cc.Spec.ActiveMigration = &v1.ActiveMigration{
		OptimizeRulePriority: true,
	}

	testConfig := integration.NewTestConfig().
		WithCccCrds(cc).
		WithNodePools(
			// P1 (Optimal): Size 12. We have plenty of empty nodes to accept migrations.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-n2"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(12),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  "my-ccc",
					labels.MachineFamilyLabel: "n2",
				}),
				integration.WithNodePoolMin(1),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// P2 (Non-Optimal): Size 10. We will try to migrate nodes from here.
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-e2"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(10),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  "my-ccc",
					labels.MachineFamilyLabel: "e2",
				}),
				integration.WithNodePoolMin(1),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
		).
		WithOverrides(
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
			integration.WithDefragEnabled("high-priority-migration"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		// Intercept eviction requests and delete the pods from the fake client,
		// simulating the behavior of a real API server eviction controller.
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

			// Delete the pod asynchronously to avoid blocking the reactor.
			go func() {
				err := infra.Fakes.KubeClient.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
				if err != nil {
					t.Logf("Eviction controller mock: failed to delete evicted pod %s/%s: %v", namespace, podName, err)
				} else {
					t.Logf("Eviction controller mock: successfully deleted pod %s/%s", namespace, podName)
				}
			}()

			return true, eviction, nil
		})

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		if err != nil {
			return
		}

		// Wait for all 22 nodes to be created.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 22)

		// Manually annotate the nodes with their priority index.
		// n2 nodes -> index 0 (P1)
		// e2 nodes -> index 1 (P2)
		var e2Nodes []apiv1.Node
		for _, node := range nodes.Items {
			if node.Annotations == nil {
				node.Annotations = make(map[string]string)
			}
			if node.Labels[labels.MachineFamilyLabel] == "n2" {
				node.Annotations[labels.CCCPriorityIndexAnnotationKey] = "0"
			} else if node.Labels[labels.MachineFamilyLabel] == "e2" {
				node.Annotations[labels.CCCPriorityIndexAnnotationKey] = "1"
				e2Nodes = append(e2Nodes, node)
			}
			infra.Fakes.K8s.UpdateNode(&node)
		}
		assert.Len(t, e2Nodes, 10)

		// Create a pod on 6 of the ng-e2 nodes taking 60% CPU (1200m).
		// This protects these 6 nodes from standard ScaleDown.
		// Defrag should be able to migrate 5 of them (bringing e2 count to 5),
		// but the 6th migration should be blocked by the quota tracker.
		for i := 0; i < 6; i++ {
			node := e2Nodes[i]
			pod := tu.BuildTestPod(fmt.Sprintf("pod-e2-%d", i), 1200, 100)
			tu.SetRSPodSpec(pod, "rs")
			pod.Spec.NodeName = node.Name
			_, err = infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
			assert.NoError(t, err)
		}

		// Run autoscaler loop to trigger Defrag.
		// Since we have 6 pods to migrate and they all fit on the 12 empty n2 nodes,
		// only the min capacity quota (min 5 for e2) should limit the migration to 5 nodes.
		integration_synctest.MustRunOnceAfter(t, autoscaler, 2*time.Minute)

		// Wait for the node count to stabilize.
		// We expect 5 e2 nodes to be deleted, leaving exactly 5 e2 nodes.
		// The 12 n2 nodes should remain (none deleted, none added, they just host the migrated pods).
		var remainingNodes *apiv1.NodeList
		for i := 0; i < 20; i++ {
			time.Sleep(5 * time.Second)
			remainingNodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)

			e2Count := 0
			for _, node := range remainingNodes.Items {
				if node.Labels[labels.MachineFamilyLabel] == "e2" {
					e2Count++
				}
			}
			if e2Count == 5 {
				break
			}
		}

		// Final counts verification.
		e2Count := 0
		n2Count := 0
		for _, node := range remainingNodes.Items {
			if node.Labels[labels.MachineFamilyLabel] == "e2" {
				e2Count++
			}
			if node.Labels[labels.MachineFamilyLabel] == "n2" {
				n2Count++
			}
		}

		// Print events for debugging if assertion fails.
		if e2Count != 5 {
			events, listErr := infra.Fakes.KubeClient.CoreV1().Events("").List(ctx, metav1.ListOptions{})
			if listErr == nil {
				for _, event := range events.Items {
					t.Logf("Event: %s - %s (Object: %s, Reason: %s)", event.Type, event.Message, event.InvolvedObject.Name, event.Reason)
				}
			}
		}

		assert.Equal(t, 5, e2Count, "Expected exactly 5 e2 nodes to remain due to P2 min capacity defrag prevention (TargetNodeCount=5)")
		assert.Equal(t, 12, n2Count, "Expected all 12 n2 nodes to remain")
	})
}
