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

package processor

import (
	"context"
	"errors"
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	cacontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	cataints "k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
)

const defaultParallelism = 100

// nodeReconciler manages taints and annotations on nodes.
type nodeReconciler struct {
	parallelism int
}

type operation interface {
	Node() *apiv1.Node
	Patch(node *apiv1.Node) (needsUpdate bool, err error)
	ProcessError(err error) error
}

type nodeReconcilerOptions struct {
	// Parallelism is the number of concurrent workers for node updates.
	Parallelism int
}

func newNodeReconciler(opts nodeReconcilerOptions) *nodeReconciler {
	parallelism := opts.Parallelism
	if parallelism <= 0 {
		parallelism = defaultParallelism
	}
	return &nodeReconciler{
		parallelism: parallelism,
	}
}

// Reconcile ensures that taints and annotations are correctly set on candidate nodes
// and cleaned up from non-candidate nodes.
func (nr *nodeReconciler) Reconcile(ctx *cacontext.AutoscalingContext, candidateInfos []*candidateInfo) error {
	if err := nr.applyToCandidates(ctx, candidateInfos); err != nil {
		return fmt.Errorf("failed to reconcile candidates: %w", err)
	}
	if err := nr.cleanUpNonCandidates(ctx, candidateInfos); err != nil {
		return fmt.Errorf("failed to clean up non-candidate nodes: %w", err)
	}
	return nil
}

// ReconcileCandidate ensures that taints and annotations are correctly set on a single candidate.
func (nr *nodeReconciler) ReconcileCandidate(ctx *cacontext.AutoscalingContext, info *candidateInfo) error {
	if err := nr.applyToCandidates(ctx, []*candidateInfo{info}); err != nil {
		return fmt.Errorf("failed to reconcile candidate %s: %w", info.String(), err)
	}
	return nil
}

func (nr *nodeReconciler) applyToCandidates(ctx *cacontext.AutoscalingContext, infos []*candidateInfo) error {
	var operations []operation
	for _, info := range infos {
		taintValue := fmt.Sprint(info.creationTime.Unix())

		for _, nodeName := range info.candidate.Nodes {
			nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
			if err != nil {
				return err
			}
			operations = append(operations, &applyOperation{
				node:       nodeInfo.Node(),
				taintValue: taintValue,
			})
		}
	}

	if len(operations) == 0 {
		return nil
	}
	return nr.processNodeUpdates(ctx, operations)
}

func (nr *nodeReconciler) cleanUpNonCandidates(ctx *cacontext.AutoscalingContext, candidateInfos []*candidateInfo) error {
	allCandidateNodeNames := sets.New[string]()
	for _, info := range candidateInfos {
		for _, nodeName := range info.candidate.Nodes {
			allCandidateNodeNames.Insert(nodeName)
		}
	}

	allNodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return err
	}

	var operations []operation
	for _, nodeInfo := range allNodeInfos {
		node := nodeInfo.Node()
		if allCandidateNodeNames.Has(node.Name) {
			continue
		}
		if _, ok := node.Annotations[annotations.NodeUpcomingAnnotation]; ok {
			continue
		}
		// node is about to be deleted, it doesn't make sense to clean its taints and annotations
		if cataints.HasToBeDeletedTaint(node) {
			continue
		}

		cleanupNeeded := cataints.HasTaint(node, defrag.HardTaint)
		cleanupNeeded = cleanupNeeded || cataints.HasTaint(node, defrag.SoftTaint)

		if cleanupNeeded {
			operations = append(operations, &cleanupOperation{node: node})
		}
	}

	if len(operations) == 0 {
		return nil
	}
	return nr.processNodeUpdates(ctx, operations)
}

// processNodeUpdates performs batch updates on nodes in parallel.
func (nr *nodeReconciler) processNodeUpdates(asCtx *cacontext.AutoscalingContext, operations []operation) error {
	if len(operations) == 0 {
		return nil
	}

	errsChan := make(chan error, len(operations))
	patchedNodesChan := make(chan *apiv1.Node, len(operations))

	// TODO: consider setting some timeout here to ensure that main loop is not blocked for too long
	ctx := context.TODO()
	kubeClient := asCtx.AutoscalingKubeClients.ClientSet
	workqueue.ParallelizeUntil(ctx, nr.parallelism, len(operations), func(i int) {
		op := operations[i]
		nodeToPatch := op.Node()

		patchedNode, err := utils.PatchNode(ctx, kubeClient, nodeToPatch, op.Patch, false)

		err = op.ProcessError(err)
		if err != nil {
			errsChan <- fmt.Errorf("node %s patch failed: %w", nodeToPatch.Name, err)
			return
		}

		// if patchedNode is non-nil, an update occurred
		if patchedNode != nil {
			patchedNodesChan <- patchedNode
		}
	})

	close(patchedNodesChan)
	close(errsChan)

	var multiErr []error

	// Update cluster snapshot with the successfully patched nodes.
	for updatedNode := range patchedNodesChan {
		nodeInfo, err := asCtx.ClusterSnapshot.GetNodeInfo(updatedNode.Name)
		if err != nil {
			multiErr = append(multiErr, err)
			continue
		}
		nodeInfo.SetNode(updatedNode)
	}

	// Collect any patching errors.
	for err := range errsChan {
		multiErr = append(multiErr, err)
	}

	return errors.Join(multiErr...)
}

type applyOperation struct {
	node       *apiv1.Node
	taintValue string
}

func (op *applyOperation) Node() *apiv1.Node {
	return op.node
}

// Patch applies defrag taints and annotation to the node if needed.
func (op *applyOperation) Patch(node *apiv1.Node) (bool, error) {
	var updated bool

	hardTaint := apiv1.Taint{Key: defrag.HardTaint, Value: op.taintValue, Effect: apiv1.TaintEffectNoSchedule}
	softTaint := apiv1.Taint{Key: defrag.SoftTaint, Value: op.taintValue, Effect: apiv1.TaintEffectPreferNoSchedule}

	for _, taintToAdd := range []apiv1.Taint{hardTaint, softTaint} {
		if !cataints.HasTaint(node, taintToAdd.Key) {
			node.Spec.Taints = append(node.Spec.Taints, taintToAdd)
			updated = true
		}
	}

	return updated, nil
}

func (op *applyOperation) ProcessError(err error) error {
	return err
}

type cleanupOperation struct {
	node *apiv1.Node
}

func (op *cleanupOperation) Node() *apiv1.Node {
	return op.node
}

// Patch removes defrag taints and annotation from the node if present.
func (op *cleanupOperation) Patch(node *apiv1.Node) (bool, error) {
	var updated bool
	var taintsModified bool

	var newTaints []apiv1.Taint
	for _, taint := range node.Spec.Taints {
		if taint.Key == defrag.HardTaint || taint.Key == defrag.SoftTaint {
			taintsModified = true
			continue
		}
		newTaints = append(newTaints, taint)
	}

	if taintsModified {
		node.Spec.Taints = newTaints
		updated = true
	}

	return updated, nil
}

// ProcessError ignores NotFound errors.
func (op *cleanupOperation) ProcessError(err error) error {
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
