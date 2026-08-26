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
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
)

const (
	suspendHandlerLogPrefix = "CSN Suspend Handler:"
)

type SuspendHandler struct {
	stateManager  StateManager
	cloudProvider CloudProvider
	k8sClient     K8sClient
	enqueue       Enqueue
	beforeSuspend time.Duration
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
		cloudProvider: cp,
		k8sClient:     c,
		enqueue:       e,
		beforeSuspend: beforeSuspend,
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
	categorized, errs := h.categorizeNodeNames(ctx, set.KeySet(nodesToPatch))
	for nodeName, err := range errs {
		result.Errs[nodeName] = err
	}

	// Consume nodes for which suspension is blocked.
	if names := categorized.ToConsume; names.Len() > 0 {
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
	if categorized.ToSuspend.Len() == 0 {
		return result, nil
	}

	// Suspend instances via GCE call.
	instancesToSuspend := make([]instance, 0, categorized.ToSuspend.Len())
	for nodeName := range categorized.ToSuspend {
		tn, ok := h.stateManager.Get(nodeName)
		if !ok {
			result.Success.Insert(nodeName)
			continue
		}
		ref, err := gce.GceRefFromProviderId(tn.Node.Spec.ProviderID)
		if err != nil {
			result.Errs[nodeName] = fmt.Errorf("failed to get GceRef for node %q: %v", nodeName, err)
			continue
		}
		inst := h.cloudProvider.InstanceByRef(ref)
		if inst == nil || inst.GCEStatus == "" {
			result.Errs[nodeName] = fmt.Errorf("could not find instance status for node %q", nodeName)
			continue
		}
		if internal.IsSuspended(inst.GCEStatus) {
			result.Success.Insert(nodeName)
			continue
		}
		instancesToSuspend = append(instancesToSuspend, instance{Ref: ref, Status: inst.GCEStatus})
	}

	h.suspendInstancesInBatches(op, instancesToSuspend, &result)
	return result, nil
}

func (h *SuspendHandler) suspendInstancesInBatches(op ops.Operation, instances []instance, result *ops.Result) {
	if len(instances) == 0 {
		return
	}
	refs := make([]gce.GceRef, 0, len(instances))
	for _, inst := range instances {
		refs = append(refs, inst.Ref)
	}
	for i := 0; i < len(refs); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(refs) {
			end = len(refs)
		}
		batchRefs := refs[i:end]
		status := gceSuccess
		if err := h.cloudProvider.SuspendInstances(op.MIG, batchRefs, false); err != nil {
			status = gceFailure
			result.AddErrForRefSlice(fmt.Errorf("failed to suspend instances: %w, instances in batch: %v", err, instances[i:end]), batchRefs)
		} else {
			result.AddSuccessForRefSlice(batchRefs)
		}
		opGceBatchSize.WithLabelValues(suspendCall, status).Observe(float64(len(batchRefs)))
	}
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
