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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

// ResizableVmDefragScaleDownNodeProcessor filters out resizable nodes with ongoing or pending
// resize operations for defrag. Unlike ScaleDownNodeProcessor, it does not apply downsize logic
// or downsize-related scale down restrictions, allowing utilized nodes to be drained during defrag.
type ResizableVmDefragScaleDownNodeProcessor struct {
	mcp                *machinetypes.MachineConfigProvider
	resizableVmManager operationtracker.Manager
}

// NewResizableVmDefragScaleDownNodeProcessor returns a new ResizableVmDefragScaleDownNodeProcessor.
func NewResizableVmDefragScaleDownNodeProcessor(mcp *machinetypes.MachineConfigProvider, resizableVmManager operationtracker.Manager) *ResizableVmDefragScaleDownNodeProcessor {
	return &ResizableVmDefragScaleDownNodeProcessor{
		mcp:                mcp,
		resizableVmManager: resizableVmManager,
	}
}

// GetScaleDownCandidates returns all nodes except resizable ones with an ongoing or pending resize.
func (p *ResizableVmDefragScaleDownNodeProcessor) GetScaleDownCandidates(_ context.Context, _ *ca_context.AutoscalingContext, allNodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	if !isAnyResizingEnabled(p.resizableVmManager, p.mcp.AllResizableMachineFamilies()) {
		return allNodes, nil
	}

	candidates := make([]*apiv1.Node, 0, len(allNodes))
	for _, node := range allNodes {
		if isResizableNode(node, p.mcp) && p.resizableVmManager.IsNodeResizingOrPending(node.Name) {
			klog.V(4).Infof("Defrag: resizable node %q is in process of resize, not eligible for defrag", node.Name)
			continue
		}
		candidates = append(candidates, node)
	}
	return candidates, nil
}

// GetPodDestinationCandidates returns all nodes: an ongoing resize does not block scheduling.
func (p *ResizableVmDefragScaleDownNodeProcessor) GetPodDestinationCandidates(_ *ca_context.AutoscalingContext, allNodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return allNodes, nil
}

// CleanUp is a no-op.
func (p *ResizableVmDefragScaleDownNodeProcessor) CleanUp() {}
