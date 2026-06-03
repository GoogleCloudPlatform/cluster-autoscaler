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

package operationtracker

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/taints"
)

type balloonPodController interface {
	// Init initialize the event handlers.
	Init() error
	// CreateBalloonPod creates a Balloon Pod with given size for a given Node.
	CreateBalloonPod(node *v1.Node, cpu, mem resource.Quantity) error
	// DeleteAllBalloonPods deletes all Balloon Pod for a given Node.
	DeleteAllBalloonPods(node *v1.Node) error
	// List returns list of all Balloon Pods.
	List() []*v1.Pod
	// GetPodsForNode return Balloon Pod for a given Node.
	GetPodsForNode(node *v1.Node) []*v1.Pod
}

type defaultBalloonPodResizer struct {
	bPController balloonPodController
	clientSet    clientset.Interface
}

func (b *defaultBalloonPodResizer) init() error {
	return b.bPController.Init()
}

func (b *defaultBalloonPodResizer) resizeBalloonPod(node *v1.Node, desiredSize size.Allocatable) error {
	if node.Status.Allocatable == nil {
		return fmt.Errorf("cannot resize balloon pod for node %q with no allocatable set", node.Name)
	}

	if err := b.bPController.DeleteAllBalloonPods(node); err != nil {
		return fmt.Errorf("balloon pod removal error for node %q: %v", node.Name, err)
	}

	bPodCpu, bPodMem := getBalloonPodSize(node, desiredSize)
	if err := b.bPController.CreateBalloonPod(node, bPodCpu, bPodMem); err != nil {
		return fmt.Errorf("balloon pod creation error for node %q: %v", node.Name, err)
	}

	return nil
}

func (b *defaultBalloonPodResizer) listAllBalloonPods(node *v1.Node) []*v1.Pod {
	return b.bPController.GetPodsForNode(node)
}

func (b *defaultBalloonPodResizer) addTaint(node *v1.Node, timeAdded time.Time) (*v1.Node, error) {
	currentTime := time.Now()
	bpResizeTaintWithTimeAdded := ekvmtypes.BPResizeTaint.DeepCopy()
	bpResizeTaintWithTimeAdded.TimeAdded = &metav1.Time{Time: timeAdded}
	nodePatcher := func(n *v1.Node) (bool, error) {
		updatedNode, updated, err := taints.AddOrUpdateTaint(n, bpResizeTaintWithTimeAdded)
		n.Spec.Taints = updatedNode.Spec.Taints
		return updated, err
	}
	node, err := b.patchNode(node, nodePatcher)
	elapsedTime := time.Since(currentTime)
	if err == nil && elapsedTime > patchLoggingThreshold {
		klog.Warningf("Adding taint %q to node %q took more than %v: %v", ekvmtypes.BPResizeTaint.Key, node.Name, patchLoggingThreshold, elapsedTime)
	}
	return node, err
}

func (b *defaultBalloonPodResizer) removeTaint(node *v1.Node) (*v1.Node, error) {
	currentTime := time.Now()
	nodePatcher := func(n *v1.Node) (bool, error) {
		updatedNode, updated, err := taints.RemoveTaint(n, ekvmtypes.BPResizeTaint)
		n.Spec.Taints = updatedNode.Spec.Taints
		return updated, err
	}
	node, err := b.patchNode(node, nodePatcher)
	elapsedTime := time.Since(currentTime)
	if err == nil && elapsedTime > patchLoggingThreshold {
		klog.Warningf("Removing taint %q to node %q took more than %v: %v", ekvmtypes.BPResizeTaint.Key, node.Name, patchLoggingThreshold, elapsedTime)
	}
	return node, err
}

func (b *defaultBalloonPodResizer) hasTaint(node *v1.Node) bool {
	return taints.TaintExists(node.Spec.Taints, ekvmtypes.BPResizeTaint)
}

func (b *defaultBalloonPodResizer) patchNode(oldNode *v1.Node, nodePatcher utils.NodePatcher) (*v1.Node, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), patchTimeout)
	defer cancelFunc()
	newNode, err := utils.PatchNode(ctx, b.clientSet, oldNode, nodePatcher, true)
	if err != nil {
		return nil, fmt.Errorf("failed to patch node %q: %v", oldNode.Name, err)
	}
	// utils.PatchNode returns nil if there is no update. In this case we want to return the old node.
	if newNode == nil {
		return oldNode, nil
	}
	return newNode, nil
}

func (b *defaultBalloonPodResizer) getPodForNode(node *v1.Node) *v1.Pod {
	pods := b.bPController.GetPodsForNode(node)
	switch len(pods) {
	case 0:
		return nil
	case 1:
		return pods[0]
	default:
		klog.Warningf("More than one balloon pod is found: %v", pods)
		return pods[0]
	}
}
