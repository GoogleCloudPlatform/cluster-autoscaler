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

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
)

type AssignBufferHandler struct {
	stateManager StateManager
	k8sClient    K8sClient
}

func NewAssignBufferHandler(sm StateManager, c K8sClient) *AssignBufferHandler {
	return &AssignBufferHandler{
		stateManager: sm,
		k8sClient:    c,
	}
}

func (h *AssignBufferHandler) Handle(ctx context.Context, op ops.Operation) (ops.Result, error) {
	result := ops.NewResult()
	if op.Type != ops.AssignBufferOp {
		return result, fmt.Errorf("got operation type %s, expected %s", op.Type, ops.AssignBufferOp)
	}

	nodeToBuffer := h.stateManager.GetAssignedBuffers(op.NodeNames.UnsortedList()...)

	for nodeFromOp := range op.NodeNames {
		if _, ok := nodeToBuffer[nodeFromOp]; !ok {
			// node could have been consumed if it doesn't have a buffer.
			result.Success.Insert(nodeFromOp)
		}
	}

	for nodeName, buffer := range nodeToBuffer {
		tn, ok := h.stateManager.Get(nodeName)
		if !ok {
			// Looks like the node was deleted. Omitting it.
			result.Success.Insert(nodeName)
			continue
		}
		if err := h.k8sClient.ApplyNodeToBufferAssignmentPatch(ctx, tn.Node, buffer); err != nil {
			result.Errs[nodeName] = fmt.Errorf("failed to patch node %q to indicate assignment to buffer \"%s/%s\": %w", nodeName, buffer.Namespace, buffer.Name, err)
		} else {
			result.Success.Insert(nodeName)
		}
	}
	return result, nil
}
