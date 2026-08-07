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

	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/gce"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/klog/v2"
	"k8s.io/utils/set"
)

// Limit for GCE is 1000 instances per call.
// Source: https://docs.cloud.google.com/compute/docs/reference/rest/v1/instanceGroupManagers/resumeInstances
// Date of access: 2026.03.27
const maxBatchSize = 1000

// CSNCompositeBackoff handles backoff logic specifically for CSN resumption errors.
type CSNCompositeBackoff interface {
	base_backoff.Backoff
	ReportResumptionError(nodeGroup cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorCode, errorMessage, instanceStatus string)
}

type instance struct {
	Ref    gce.GceRef
	Status string
}

type ConsumeHandler struct {
	stateManager  StateManager
	cloudProvider CloudProvider
	k8sClient     K8sClient
	backoff       CSNCompositeBackoff
}

func NewConsumeHandler(sm StateManager, cp CloudProvider, c K8sClient, b CSNCompositeBackoff) *ConsumeHandler {
	return &ConsumeHandler{
		stateManager:  sm,
		cloudProvider: cp,
		k8sClient:     c,
		backoff:       b,
	}
}

func (h *ConsumeHandler) Handle(ctx context.Context, op ops.Operation) (ops.Result, error) {
	result := ops.NewResult()
	if op.Type != ops.ConsumeOp {
		return result, fmt.Errorf("got operation type %s, expected %s", op.Type, ops.ConsumeOp)
	}
	instancesToResume := h.getInstancesToResume(op, &result)
	h.resumeInstancesInBatches(op, instancesToResume, &result)
	h.patchNodesToConsumed(ctx, op, &result)
	return result, nil
}

func (h *ConsumeHandler) resumeInstancesInBatches(op ops.Operation, instancesToResume []instance, result *ops.Result) {
	if len(instancesToResume) == 0 {
		return
	}

	refs := make([]gce.GceRef, 0, len(instancesToResume))
	for _, inst := range instancesToResume {
		refs = append(refs, inst.Ref)
	}

	nonBlockingErrorsHandler := h.getNonBlockingErrorsHandler()
	for i := 0; i < len(refs); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(refs) {
			end = len(refs)
		}
		batch := refs[i:end]

		err := h.cloudProvider.ResumeInstances(op.MIG, batch, nonBlockingErrorsHandler)
		status := gceSuccess
		if err != nil {
			status = gceFailure
			result.AddErrForRefSlice(fmt.Errorf("failed to resume instances: %w, instances in batch: %v", err, instancesToResume[i:end]), batch)
		}
		opGceBatchSize.WithLabelValues(resumeCall, status).Observe(float64(len(batch)))
	}
}

// getNonBlockingErrorsHandler returns a non blocking errors handler (needed by ResumeInstances method) that reports node-level GCE errors to the global backoff.
func (h *ConsumeHandler) getNonBlockingErrorsHandler() gceclient.NonBlockingErrorsHandler {
	if h.backoff == nil || !h.stateManager.IsBackoffEnabled() {
		return nil
	}
	return func(ref gce.GceRef, code, msg, instanceStatus string) {
		tn, ok := h.stateManager.Get(ref.Name)
		if !ok || tn.Node == nil {
			return
		}
		nodeGroup, _ := h.cloudProvider.GkeMigForNode(tn.Node)
		if nodeGroup == nil {
			return
		}
		nodeInfo := framework.NewNodeInfo(tn.Node, nil)
		h.backoff.ReportResumptionError(nodeGroup, nodeInfo, code, msg, instanceStatus)
		klog.Errorf("Resuming node %q encountered an error (will continue polling for resumption but will trigger global backoffs): code %q, message: %q, instance status: %q", tn.Node.Name, code, msg, instanceStatus)
		// Note that GCE will continue resuming the instance forever until the resumption succeed or the node is deleted. That's why we don't stop polling until timeout.
	}
}

func (h *ConsumeHandler) patchNodesToConsumed(ctx context.Context, op ops.Operation, result *ops.Result) {
	nodesToPatch := op.NodeNames.Difference(set.KeySet(result.Errs)).Difference(result.Success)
	for nodeName := range nodesToPatch {
		tn, ok := h.stateManager.Get(nodeName)
		if !ok {
			result.Success.Insert(nodeName)
			continue
		}
		err := h.k8sClient.ApplyNodePatch(ctx, tn.Node, csn.NodeStateConsumed)
		if err != nil {
			result.Errs[nodeName] = fmt.Errorf("failed to patch node %q to be consumed: %w", nodeName, err)
			continue
		}
		result.Success.Insert(nodeName)
	}
}

// returns a nodeName->instance mapping.
func (h *ConsumeHandler) getInstancesToResume(op ops.Operation, res *ops.Result) []instance {
	instances := make([]instance, 0, op.NodeNames.Len())
	for nodeName := range op.NodeNames {
		tn, ok := h.stateManager.Get(nodeName)
		if !ok {
			res.Success.Insert(nodeName)
			continue
		}
		ref, err := gce.GceRefFromProviderId(tn.Node.Spec.ProviderID)
		if err != nil {
			res.Errs[nodeName] = fmt.Errorf("invalid provider ID for node %q: %w", nodeName, err)
			continue
		}
		inst := h.cloudProvider.InstanceByRef(ref)
		if inst == nil || inst.GCEStatus == "" {
			res.Errs[nodeName] = fmt.Errorf("could not find instance status for node %q", nodeName)
			continue
		}
		if !internal.IsSuspended(inst.GCEStatus) {
			// No need to resume an instance that is already running.
			// Patching might still be required.
			continue
		}
		instances = append(instances, instance{Ref: ref, Status: inst.GCEStatus})
	}
	return instances
}
