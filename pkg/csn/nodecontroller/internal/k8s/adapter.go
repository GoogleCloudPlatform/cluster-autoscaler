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

package k8s

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	"k8s.io/klog/v2"
)

const maxTaintCount = 16

// ClientAdapter supplies k8s methods required to support
// the functionality of the CSN node controller.
type ClientAdapter struct {
	clientSet clientset.Interface
}

func NewClientAdapter(c clientset.Interface) *ClientAdapter {
	return &ClientAdapter{clientSet: c}
}

func (c *ClientAdapter) IsSuspensionBlocked(ctx context.Context, nodeName string) (bool, error) {
	pods, err := c.clientSet.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	})
	if err != nil {
		return false, fmt.Errorf("listing pods failed for node %q: %w", nodeName, err)
	}

	for _, pod := range pods.Items {
		if csn.IsPodBlockingSuspension(&pod) {
			return true, nil
		}
	}
	return false, nil
}

// ApplyNodePatch patches the node to reflect the desired state.
// K8s patch request is not sent if it's not necessary.
func (c *ClientAdapter) ApplyNodePatch(ctx context.Context, node *v1.Node, desiredState csn.NodeState) error {
	if csn.ClassifyNode(node) == desiredState {
		return nil
	}
	patcher := func(n *v1.Node) (bool, error) {
		updatedNode, err := csn.SetNodeAs(n, desiredState)
		if err != nil {
			return false, fmt.Errorf("applying state %v to node %q returned error: %w", desiredState, n.Name, err)
		}
		*n = *updatedNode
		return true, nil
	}
	if _, err := utils.PatchNode(ctx, c.clientSet, node, patcher, true); err != nil {
		return fmt.Errorf("k8s patch request for node %s returned error: %w", node.Name, err)
	}
	return nil
}

// ApplyNodeToBufferAssignmentPatch patches the node to indicate the CSN buffer to which a node is assigned.
func (c *ClientAdapter) ApplyNodeToBufferAssignmentPatch(ctx context.Context, node *v1.Node, buffer *v1beta1.CapacityBuffer) error {
	patcher := func(n *v1.Node) (bool, error) {
		updatedNode, err := csn.AssignNodeToBufferId(n, fmt.Sprintf("%s/%s", buffer.Namespace, buffer.Name))
		if err != nil {
			return false, fmt.Errorf("assigning node %q to buffer %s/%s returned error: %w", n.Name, buffer.Namespace, buffer.Name, err)
		}
		*n = *updatedNode
		return true, nil
	}
	_, err := utils.PatchNode(ctx, c.clientSet, node, patcher, false)
	if err != nil {
		return fmt.Errorf("k8s patch request error: %w", err)
	}
	return nil
}

// ApplyAdditionalSoftTaintsPatch patches the node with additional soft taints.
func (c *ClientAdapter) ApplyAdditionalSoftTaintsPatch(ctx context.Context, node *v1.Node, taintCount int) error {
	if node == nil {
		return fmt.Errorf("cannot add %d soft taints to nil node", taintCount)
	}
	// Adding more taints is irresponsible.
	// 65 000 is the max number of nodes at the time of writing this doc.
	// https://docs.cloud.google.com/kubernetes-engine/quotas#limits_per_cluster
	// log2(65000) ~ 16
	if taintCount > maxTaintCount {
		klog.Errorf("CSN K8s client: cannot add %d soft taints to node %q as it is more than a max of %d", taintCount, node.Name, maxTaintCount)
		taintCount = maxTaintCount
	}
	patcher := func(n *v1.Node) (bool, error) {
		if err := csn.ApplySoftTaints(n, taintCount); err != nil {
			return false, err
		}
		return true, nil
	}
	_, err := utils.PatchNode(ctx, c.clientSet, node, patcher, false)
	if err != nil {
		return fmt.Errorf("k8s patch request error: %w", err)
	}
	return nil
}
