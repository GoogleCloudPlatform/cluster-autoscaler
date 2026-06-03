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

// TaintTracker is responsible for tracking and computing soft taint count for
// CSN nodes.
type TaintTracker interface {
	GetTaintCountToAssign(nodeName string) (int, bool)
}

// AssignSoftTaintHandler is responsible for
// applying additional soft taints to CSN nodes.
// This limits pod disruption when chilling nodes are consumed by avoiding
// default scheduler from spreading pods on all nodes which will prevent
// suspension and cause scale-down of the underutilized nodes.
type AssignSoftTaintHandler struct {
	stateManager StateManager
	k8sClient    K8sClient
	tracker      TaintTracker
}

func NewAssignSoftTaintHandler(sm StateManager, c K8sClient, t TaintTracker) *AssignSoftTaintHandler {
	return &AssignSoftTaintHandler{
		stateManager: sm,
		k8sClient:    c,
		tracker:      t,
	}
}

// Handle will do the following for nodes in the operation:
// * get the number of taints to assign from the taint tracker,
// * call k8sClient to apply the taints to nodes from the operation if
// they are tracked by all dependencies.
func (h *AssignSoftTaintHandler) Handle(ctx context.Context, op ops.Operation) (ops.Result, error) {
	result := ops.NewResult()
	if op.Type != ops.AssignSoftTaintOp {
		return result, fmt.Errorf("got operation type %s, expected %s", op.Type, ops.AssignSoftTaintOp)
	}

	for name := range op.NodeNames {
		count, tracked := h.tracker.GetTaintCountToAssign(name)
		if !tracked {
			result.Success.Insert(name)
			continue
		}
		tn, ok := h.stateManager.Get(name)
		if !ok || tn.Node == nil {
			result.Success.Insert(name)
			continue
		}
		if err := h.k8sClient.ApplyAdditionalSoftTaintsPatch(ctx, tn.Node, count); err != nil {
			result.Errs[name] = fmt.Errorf("failed to apply taints to node %q: %w", name, err)
		} else {
			result.Success.Insert(name)
		}
	}
	return result, nil
}
