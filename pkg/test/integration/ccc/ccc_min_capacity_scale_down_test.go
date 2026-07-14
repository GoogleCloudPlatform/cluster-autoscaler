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
	"flag"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

func init() {
	klog.InitFlags(nil)
	_ = flag.Set("v", "5")
}

func TestCCCScaleDown(t *testing.T) {
	testCases := []struct {
		name                       string
		experimentEnabled          bool
		nodesTaintedForDeletion    int
		nodesWithDeletionTimestamp int
		expectedNodeCount          int
		failureMessage             string
	}{
		{
			name:              "prevents scale down below targetNodeCount",
			experimentEnabled: true,
			expectedNodeCount: 5,
			failureMessage:    "Expected exactly 5 nodes to remain in the cluster due to TargetNodeCountQuota minimum limit",
		},
		{
			name:                       "scale down limits account for nodes in deletion",
			experimentEnabled:          true,
			nodesTaintedForDeletion:    2,
			nodesWithDeletionTimestamp: 1,
			expectedNodeCount:          8,
			failureMessage:             "Expected exactly 8 nodes to remain (3 in deletion + 5 alive) because capacity should subtract nodes in deletion",
		},
		{
			name:              "scale down not prevented when disabled by experiment",
			experimentEnabled: false,
			expectedNodeCount: 1,
			failureMessage:    "Expected node count to drop to 1 because the minimum capacity feature is disabled by the experiment flag",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder("my-ccc").
				WithTargetNodeCount(ptr.To(5)).
				Build()

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

				// Label all auto-generated nodes with our ComputeClass label so the TargetNodeCountQuota applies to them.
				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Equal(t, 10, len(nodes.Items))

				for _, node := range nodes.Items {
					if node.Labels == nil {
						node.Labels = make(map[string]string)
					}
					node.Labels["cloud.google.com/compute-class"] = "my-ccc"
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
						infra.Fakes.K8s.UpdateNode(withDeletionTaint(freshNodes.Items[i]))
					}
					for i := 0; i < tc.nodesWithDeletionTimestamp; i++ {
						infra.Fakes.K8s.UpdateNode(withDeletionTimestamp(freshNodes.Items[tc.nodesTaintedForDeletion+i]))
					}
				}

				// Run the autoscaler loop again to trigger scale down.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 0)

				// Wait for the node count to drop to the expected level.
				var remainingNodes *apiv1.NodeList
				for i := 0; i < 20; i++ {
					time.Sleep(5 * time.Second)
					remainingNodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
					assert.NoError(t, err)
					if len(remainingNodes.Items) == tc.expectedNodeCount {
						break
					}
				}

				assert.Equal(t, tc.expectedNodeCount, len(remainingNodes.Items), tc.failureMessage)
			})
		})
	}
}

// withDeletionTaint returns a copy of the node as a pointer, with ToBeDeletedTaint added.
func withDeletionTaint(node apiv1.Node) *apiv1.Node {
	node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
		Key:    taints.ToBeDeletedTaint,
		Value:  "true",
		Effect: apiv1.TaintEffectNoSchedule,
	})
	return &node
}

// withDeletionTimestamp returns a copy of the node as a pointer, with DeletionTimestamp set.
func withDeletionTimestamp(node apiv1.Node) *apiv1.Node {
	now := metav1.Now()
	node.DeletionTimestamp = &now
	return &node
}
