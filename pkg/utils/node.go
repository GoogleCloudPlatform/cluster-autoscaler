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

package utils

import (
	"context"
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// NodePatcher is a function that patches a node. Returns if the node was patched and an error.
type NodePatcher func(node *v1.Node) (bool, error)

// AnnotateNode adds annotations to the node
func AnnotateNode(ctx context.Context, kubeClient clientset.Interface, node *v1.Node, annotations map[string]string) error {
	_, err := PatchNodeNoConflict(ctx, kubeClient, node, func(node *v1.Node) (bool, error) {
		var updated bool
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}
		for key, value := range annotations {
			if v := node.Annotations[key]; v != value {
				node.Annotations[key] = value
				updated = true
			}
		}
		return updated, nil
	})
	return err
}

// RemoveAnnotations removes annotations from the node
func RemoveAnnotations(ctx context.Context, kubeClient clientset.Interface, node *v1.Node, annotationKeys []string) error {
	if node.Annotations == nil {
		return nil
	}
	_, err := PatchNodeNoConflict(ctx, kubeClient, node, func(node *v1.Node) (bool, error) {
		var updated bool
		for _, key := range annotationKeys {
			if _, ok := node.Annotations[key]; ok {
				delete(node.Annotations, key)
				updated = true
			}
		}
		return updated, nil
	})
	return err
}

// PatchNodeNoConflict patches a copy of the node with patcher function and sends
// strategic merge patch to the API server. This function does not check
// conflicts, so use it iff:
// 1. You are fine with possible overwrite of an intermediate update
// 2. You write to a map field (e.g. labels, annotations), or to an array field
// that supports strategic merge (e.g. pod CIDRs).
// WARNING: taints **do not** support strategic merge.
// Returns an updated copy of the node or nil if no update was needed and error.
//
// Docs on strategic merge: https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/
// API reference: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.33/#node-v1-core
func PatchNodeNoConflict(ctx context.Context, kubeClient clientset.Interface, node *v1.Node, patcher NodePatcher) (*v1.Node, error) {
	oldData, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}
	newNode := node.DeepCopy()
	patched, err := patcher(newNode)
	if err != nil {
		return nil, err
	}
	if !patched || equality.Semantic.DeepEqual(node, newNode) {
		return nil, nil
	}
	newData, err := json.Marshal(newNode)
	if err != nil {
		return nil, err
	}
	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Node{})
	if err != nil {
		return nil, err
	}
	return kubeClient.CoreV1().Nodes().Patch(ctx, node.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
}

// PatchNode patches a copy of the node with patcher function and sends strategic
// merge patch to the API server. Retries on conflict. Updates either node or its status or both if needed.
// Returns an updated copy of the node or nil if no update was needed and error.
func PatchNode(ctx context.Context, kubeClient clientset.Interface, node *v1.Node, patcher NodePatcher, initialNodeRefresh bool) (*v1.Node, error) {
	var newNode *v1.Node
	refresh := initialNodeRefresh
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if refresh {
			var err error
			node, err = kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
		}
		newNode = nil // clean up state from previous try
		refresh = true

		// create a patched node
		nodeToPatch := node.DeepCopy()
		updated, err := patcher(nodeToPatch)
		if err != nil {
			return err
		}
		if !updated {
			return nil
		}

		// detect changes to node
		statusChanged := !equality.Semantic.DeepEqual(node.Status, nodeToPatch.Status)
		metaChanged := !equality.Semantic.DeepEqual(node.ObjectMeta, nodeToPatch.ObjectMeta)
		specChanged := !equality.Semantic.DeepEqual(node.Spec, nodeToPatch.Spec)
		if !statusChanged && !metaChanged && !specChanged {
			return nil
		}

		// some node fields (e.g. taints) do not support strategic merge,
		// clean up resource version to ensure that we don't overwrite intermediate updates.
		oldNodeNoRV := node.DeepCopy()
		oldNodeNoRV.ResourceVersion = ""
		if metaChanged || specChanged {
			newNode, err = patchNodeMainFields(ctx, kubeClient, oldNodeNoRV, nodeToPatch)
			if err != nil {
				return err
			}
		}
		if statusChanged {
			newNode, err = patchNodeStatus(ctx, kubeClient, oldNodeNoRV, nodeToPatch)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return newNode, nil
}

func patchNodeMainFields(ctx context.Context, kubeClient clientset.Interface, oldNode *v1.Node, newNode *v1.Node) (*v1.Node, error) {
	oldNodeMetaSpec := v1.Node{ObjectMeta: oldNode.ObjectMeta, Spec: oldNode.Spec}
	newNodeMetaSpec := v1.Node{ObjectMeta: newNode.ObjectMeta, Spec: newNode.Spec}
	patchBytes, err := getTwoWayMergePatchBytes(&oldNodeMetaSpec, &newNodeMetaSpec)
	if err != nil {
		return nil, err
	}
	return kubeClient.CoreV1().Nodes().Patch(ctx, oldNode.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
}

func patchNodeStatus(ctx context.Context, kubeClient clientset.Interface, oldNode *v1.Node, newNode *v1.Node) (*v1.Node, error) {
	oldNodeStatus := v1.Node{Status: oldNode.Status}
	newNodeStatus := v1.Node{Status: newNode.Status}
	patchBytes, err := getTwoWayMergePatchBytes(&oldNodeStatus, &newNodeStatus)
	if err != nil {
		return nil, err
	}
	return kubeClient.CoreV1().Nodes().Patch(ctx, oldNode.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")
}

func getTwoWayMergePatchBytes(oldNode *v1.Node, newNode *v1.Node) ([]byte, error) {
	oldData, err := json.Marshal(oldNode)
	if err != nil {
		return nil, err
	}
	newData, err := json.Marshal(newNode)
	if err != nil {
		return nil, err
	}
	return strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Node{})
}

// TerminatingNodeFilter excludes nodes that are undergoing deletion.
type TerminatingNodeFilter struct{}

// ExcludeFromTracking returns true if the node has a non-nil deletion timestamp or has ToBeDeleted taint.
func (f TerminatingNodeFilter) ExcludeFromTracking(node *v1.Node) bool {
	return node.DeletionTimestamp != nil || taints.HasToBeDeletedTaint(node)
}
