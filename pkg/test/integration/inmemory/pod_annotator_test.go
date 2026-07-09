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
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestPodAnnotator_SkipsScheduledPods(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-e2"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(2),
				integration.WithNodePoolMaxNodeCount(2),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
		).
		WithOverrides(
			integration.WithScaleDownUnneededTime(time.Second),
			integration.WithScaleDownDelayAfterAdd(time.Second),
			integration.WithScaleDownUtilizationThreshold(0.5),
			integration.WithDefragEnabled("annotation"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
			integration.WithMaxNodesTotal(2), // Block scale-up globally
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if err != nil {
			t.Fatalf("Failed to setup autoscaler: %v", err)
		}

		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("Failed to list nodes: %v", err)
		}

		var targetNodes []metav1.ObjectMeta
		for i := range nodes.Items {
			if nodes.Items[i].Labels["cloud.google.com/gke-nodepool"] == "ng-e2" {
				targetNodes = append(targetNodes, nodes.Items[i].ObjectMeta)
			}
		}

		if len(targetNodes) < 2 {
			t.Fatalf("Expected at least 2 ng-e2 nodes, got %d", len(targetNodes))
		}

		var nodeA, nodeB *apiv1.Node
		for i := range nodes.Items {
			if nodes.Items[i].Name == targetNodes[0].Name {
				nodeA = &nodes.Items[i]
			}
			if nodes.Items[i].Name == targetNodes[1].Name {
				nodeB = &nodes.Items[i]
			}
		}

		// Add defrag annotation to trigger it on Node B
		if nodeB.Annotations == nil {
			nodeB.Annotations = make(map[string]string)
		}
		nodeB.Annotations["defrag.cluster-autoscaler.kubernetes.io"] = ""
		infra.Fakes.K8s.UpdateNode(nodeB)

		// Place podA on NodeA
		podA := tu.BuildTestPod("pod-a", 1800, 100)
		tu.SetRSPodSpec(podA, "rs-a")
		podA.Spec.NodeName = nodeA.Name
		_, err = infra.Fakes.KubeClient.CoreV1().Pods(podA.Namespace).Create(ctx, podA, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create pod A: %v", err)
		}

		// Place podB on NodeB (low util -> candidate for defrag)
		podB := tu.BuildTestPod("pod-b", 500, 100)
		tu.SetRSPodSpec(podB, "rs-b")
		podB.Spec.NodeName = nodeB.Name
		_, err = infra.Fakes.KubeClient.CoreV1().Pods(podB.Namespace).Create(ctx, podB, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create pod B: %v", err)
		}

		// Run autoscaler loop to trigger Defrag.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Second)

		updatedPodB, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "pod-b", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get updated pod B: %v", err)
		}
		_, hasAnnotation := updatedPodB.Annotations["cloud.google.com/cluster_autoscaler_unhelpable_since"]
		assert.False(t, hasAnnotation, "Scheduled pods should not be annotated with unhelpable")
	})
}
