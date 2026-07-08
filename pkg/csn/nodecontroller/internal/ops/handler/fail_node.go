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

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
)

// FailNodeHandler handles applying the permanent failure state.
type FailNodeHandler struct {
	stateManager StateManager
	k8sClient    K8sClient
}

// NewFailNodeHandler returns a new instance of FailNodeHandler.
func NewFailNodeHandler(sm StateManager, c K8sClient) *FailNodeHandler {
	return &FailNodeHandler{
		stateManager: sm,
		k8sClient:    c,
	}
}

// Handle executes the FailNodeOp logic.
func (h *FailNodeHandler) Handle(ctx context.Context, op ops.Operation) (ops.Result, error) {
	result := ops.NewResult()
	if op.Type != ops.FailNodeOp {
		return result, fmt.Errorf("got operation type %s, expected %s", op.Type, ops.FailNodeOp)
	}

	for nodeName := range op.NodeNames {
		tn, ok := h.stateManager.Get(nodeName)
		if !ok {
			result.Success.Insert(nodeName)
			continue
		}

		// Check if there are blocking pods before applying the failed taint/state.
		isBlocked, err := h.k8sClient.IsWorkloadPresent(ctx, nodeName)
		if err != nil {
			result.Errs[nodeName] = fmt.Errorf("failed to check if workload is present: %w", err)
			continue
		}

		if isBlocked {
			// Fallback: If blocking user pods exist, we consume the node to drop CSN tracking
			// and return it to normal user control so the blocking pods can run.
			err = h.k8sClient.ApplyNodePatch(ctx, tn.Node, csn.NodeStateConsumed)
			if err != nil {
				result.Errs[nodeName] = fmt.Errorf("failed to fallback patch node %q to CONSUMED: %w", nodeName, err)
			} else {
				result.Success.Insert(nodeName)
			}
			continue
		}

		// Standard path: Patch node to FAILED.
		err = h.k8sClient.ApplyNodePatch(ctx, tn.Node, csn.NodeStateFailed)
		if err != nil {
			result.Errs[nodeName] = fmt.Errorf("failed to patch node %q to FAILED: %w", nodeName, err)
		} else {
			result.Success.Insert(nodeName)
		}
	}

	return result, nil
}
