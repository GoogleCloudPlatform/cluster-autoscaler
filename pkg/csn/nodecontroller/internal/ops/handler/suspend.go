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

package handler

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
)

const (
	suspendHandlerLogPrefix = "CSN Suspend Handler:"
)

type SuspendHandler struct {
	stateManager  StateManager
	k8sClient     K8sClient
	enqueue       Enqueue
	beforeSuspend time.Duration
	// suspend drives the GCE side of the operation: it tells which instances still
	// need to be suspended, suspends them and polls until they are down.
	suspend instanceTransitioner
}

func NewSuspendHandler(
	sm StateManager,
	cp CloudProvider,
	c K8sClient,
	e Enqueue,
	beforeSuspend time.Duration,
) *SuspendHandler {
	return &SuspendHandler{
		stateManager:  sm,
		k8sClient:     c,
		enqueue:       e,
		beforeSuspend: beforeSuspend,
		suspend:       newSuspendTransitioner(sm, cp),
	}
}

func (h *SuspendHandler) Handle(ctx context.Context, op ops.Operation) (ops.Result, error) {
	result := ops.NewResult()
	if op.Type != ops.SuspendOp {
		return result, fmt.Errorf("got operation type %s, expected %s", op.Type, ops.SuspendOp)
	}
	// Patch nodes to Suspended (Initial intent)
	// We need the node object to patch it.
	nodesToPatch := make(map[string]*v1.Node)
	for nodeName := range op.NodeNames {
		tn, ok := h.stateManager.Get(nodeName)
		if !ok {
			result.Success.Insert(nodeName)
			continue
		}
		nodesToPatch[nodeName] = tn.Node
	}

	for nodeName, node := range nodesToPatch {
		if err := h.k8sClient.ApplyNodePatch(ctx, node, csn.NodeStateSuspended); err != nil {
			result.Errs[nodeName] = fmt.Errorf("failed to patch node %q to be suspended: %w", nodeName, err)
			delete(nodesToPatch, nodeName)
		}
	}

	if len(nodesToPatch) == 0 {
		return result, nil
	}

	// Wait to verify that no pods are scheduled.
	select {
	case <-time.After(h.beforeSuspend):
	case <-ctx.Done():
		for nodeName := range nodesToPatch {
			result.Errs[nodeName] = ctx.Err()
		}
		return result, nil
	}

	// Check for pods that block suspension.
	nodes, errs := h.categorizeNodeNames(ctx, set.KeySet(nodesToPatch))
	for nodeName, err := range errs {
		result.Errs[nodeName] = err
	}

	// Consume nodes for which suspension is blocked.
	if names := nodes.ToConsume; names.Len() > 0 {
		klog.Infof("%s found %d nodes to consume (e.g. because they have pods scheduled): %v", suspendHandlerLogPrefix, names.Len(), names.UnsortedList())
		// Enqueue consumption for these nodes
		err := h.enqueue(ops.Operation{
			MIG:       op.MIG,
			Type:      ops.ConsumeOp,
			NodeNames: names,
		})
		if err != nil {
			result.AddErrForNodeSet(fmt.Errorf("failed to enqueue consume op for reverted nodes: %w", err), names)
		} else {
			result.AddSuccessForNodeSet(names)
		}
	}

	// Nothing to suspend, return early.
	if nodes.ToSuspend.Len() == 0 {
		return result, nil
	}

	instances := h.suspend.categorizeInstances(op.MIG, nodes.ToSuspend, &result)

	// Instances that are already suspended need nothing more.
	for _, ref := range instances.completed {
		result.Success.Insert(ref.Name)
	}

	toPoll := h.suspend.start(op.MIG, instances, &result)
	if len(toPoll) == 0 {
		return result, nil
	}

	for ref, nonBlockingErr := range h.suspend.pollUntilDone(ctx, op.MIG, toPoll, &result) {
		if nonBlockingErr != nil {
			// GCE keeps retrying the suspension, and there is no suspension backoff to
			// feed, so just leave a trace and keep waiting.
			klog.V(4).Infof("%s suspending instance %q reported a non-blocking error: code %q, message: %q, instance status: %q",
				suspendHandlerLogPrefix, ref.Name, nonBlockingErr.Code, nonBlockingErr.Message, nonBlockingErr.InstanceStatus)
			continue
		}
		result.Success.Insert(ref.Name)
	}

	return result, nil
}

type categorizedNodeNames struct {
	ToSuspend set.Set[string]
	ToConsume set.Set[string]
}

func (h *SuspendHandler) categorizeNodeNames(ctx context.Context, nodeNames set.Set[string]) (categorizedNodeNames, map[string]error) {
	errs := make(map[string]error)
	result := categorizedNodeNames{ToSuspend: set.New[string](), ToConsume: set.New[string]()}
	for nodeName := range nodeNames {
		blocked, err := h.k8sClient.IsSuspensionBlocked(ctx, nodeName)
		if err != nil {
			errs[nodeName] = fmt.Errorf("failed to check whether suspension is blocked for node %q: %w", nodeName, err)
			continue
		}
		if blocked {
			result.ToConsume.Insert(nodeName)
			continue
		}
		result.ToSuspend.Insert(nodeName)

	}
	return result, errs
}
