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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestResilienceToNonGcpNodes verifies that Cluster Autoscaler can gracefully handle
// and ignore non-GCE nodes (such as Virtual Kubelet) without crashing or failing to scale
// the rest of the cluster. It injects a malformed node alongside a healthy node group,
// adds an unschedulable pod, and asserts that CA scales up the healthy node group.
func TestResilienceToNonGcpNodes(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolMachineType("n1-standard-4"),
			integration.WithNodePoolSize(1),
			integration.WithNodePoolLocations("us-central1-b"),
		),
		).
		WithOverrides(
			integration.WithMaxMemoryTotal(30 * 1024 * 1024 * 1024),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		infra.Fakes.K8s.AddNode(newRogueNode("vk-node"))

		var healthyNodeName string
		for _, node := range infra.Fakes.K8s.Nodes().Items {
			if node.Name != "vk-node" {
				healthyNodeName = node.Name
				break
			}
		}
		assert.NotEmpty(t, healthyNodeName, "Should find the auto-generated healthy node attached to pool")

		fillerPod := tu.BuildTestPod("filler-pod", 3500, 14000)
		fillerPod.Spec.NodeName = healthyNodeName
		infra.Fakes.K8s.AddPod(fillerPod)

		// Now ask for 3000m. Existing node only has 500m left. It will trigger scale up.
		pod := tu.BuildTestPod("standard-pod", 3000, 12000, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod)

		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

		infra.Fakes.RunScheduler(ctx, t)

		// Verify if the standard pod got scheduled.
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "standard-pod", metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected standard-pod to be scheduled by RunScheduler")
		assert.NotEqual(t, "vk-node", updatedPod.Spec.NodeName, "Pod should not schedule on virtual node")

		assert.Equal(t, 3, len(infra.Fakes.K8s.Nodes().Items), "Expected CA to scale up, resulting in 3 total nodes (healthy, rogue, and new scaled up node), but got %d", len(infra.Fakes.K8s.Nodes().Items))
	})
}

// newRogueNode returns a realistic rogue node (e.g. Virtual Kubelet) with values
// captured from a production bug where CA failed to scale up alongside such nodes.
func newRogueNode(name string) *apiv1.Node {
	return &apiv1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Node",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(asTime("2026-02-02T14:58:44Z")),
			Labels: map[string]string{
				"kubernetes.io/hostname":           name,
				"kubernetes.io/role":               "agent",
				"node.kubernetes.io/instance-type": "n2-standard-4",
				"topology.kubernetes.io/zone":      "us-central1-b",
				"type":                             "virtual-kubelet",
			},
			Name: name,
		},
		Spec: apiv1.NodeSpec{
			ProviderID: fmt.Sprintf("vnode:%s", name),
			Taints: []apiv1.Taint{{
				Key:    "virtual-kubelet.io/provider",
				Value:  "present",
				Effect: apiv1.TaintEffectNoSchedule,
			}},
		},
		Status: apiv1.NodeStatus{
			Allocatable: apiv1.ResourceList{
				"cpu":    resource.MustParse("4700m"),
				"memory": resource.MustParse("17494Mi"),
				"pods":   resource.MustParse("100k"),
			},
			Capacity: apiv1.ResourceList{
				"cpu":    resource.MustParse("20k"),
				"memory": resource.MustParse("80000Gi"),
				"pods":   resource.MustParse("500M"),
			},
			Conditions: []apiv1.NodeCondition{{
				LastHeartbeatTime:  metav1.NewTime(asTime("2026-02-04T09:41:46Z")),
				LastTransitionTime: metav1.NewTime(asTime("2026-02-04T08:20:35Z")),
				Message:            "kubelet is ready",
				Reason:             "KubeletReady",
				Status:             "True",
				Type:               "Ready",
			}},
			DaemonEndpoints: apiv1.NodeDaemonEndpoints{KubeletEndpoint: apiv1.DaemonEndpoint{Port: 6906}},
		},
	}
}

func asTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}
