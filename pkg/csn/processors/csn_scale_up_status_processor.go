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

package processors

import (
	"context"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/capacitybuffers"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
)

// csnScaleUpFailedReason is the reason of the event emitted on a CapacityBuffer
// whose CSN scale-up is blocked by constraints specific to CSNs.
const csnScaleUpFailedReason = "StandbyBufferScaleUpFailed"

// CSNScaleUpStatusProcessor surfaces standby buffer scale-up failures caused by GCE VM
// Suspend/Resume constraints (such as the memory limit or Local SSD restriction) as events on the
// owning CapacityBuffer.
//
// Standby buffer fake pods carry node affinities restricting them to nodes that can be suspended
// (see csn.MakePodCSN). When those affinities are what block the scale-up, the upstream eventing
// processor drops the reason, because it only reports rejections for node groups that already
// exist, and node autoprovisioning candidates do not. The buffer would then be left with no
// capacity and no explanation if it wasn't for this processor.
type CSNScaleUpStatusProcessor struct {
	buffersRegistry    *fakepods.Registry
	experimentsManager experiments.Manager
}

// NewCSNScaleUpStatusProcessor creates a CSNScaleUpStatusProcessor.
func NewCSNScaleUpStatusProcessor(buffersRegistry *fakepods.Registry, experimentsManager experiments.Manager) *CSNScaleUpStatusProcessor {
	return &CSNScaleUpStatusProcessor{
		buffersRegistry:    buffersRegistry,
		experimentsManager: experimentsManager,
	}
}

type blockedBuffer struct {
	buffer  *v1beta1.CapacityBuffer
	message string
}

// Process implements status.ScaleUpStatusProcessor.
func (p *CSNScaleUpStatusProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	// Evaluated per loop so the launch can be turned off without restarting the autoscaler.
	if !p.experimentsManager.DirectLaunchBoolFlag(experiments.ColdStandbyNodesScaleUpStatusProcessorFlag) {
		return
	}
	// Deliberately no check on scaleUpStatus.Result: the signal is per pod, so an unrelated
	// workload succeeding in the same loop must not silence a blocked standby buffer.
	// ConsideredNodeGroups is nil on the early error paths, which just leaves the node-level
	// check with nothing to inspect.
	consideredNodeGroups := cloudprovider.NodeGroupListToMapById(scaleUpStatus.ConsideredNodeGroups)
	constraints := csn.NewSuspensionConstraints(p.experimentsManager)

	blockedBuffers := map[types.UID]blockedBuffer{}
	for _, info := range scaleUpStatus.PodsRemainUnschedulable {
		if !csn.IsCSNPod(info.Pod) {
			continue
		}
		buffer := p.buffersRegistry.GetCapacityBuffer(info.Pod.UID)
		if buffer == nil {
			continue
		}
		// A buffer with several replicas produces several unschedulable pods, but only one event.
		if _, seen := blockedBuffers[buffer.UID]; seen {
			continue
		}
		strat := buffer.Status.ProvisioningStrategy
		if strat == nil {
			strat = buffer.Spec.ProvisioningStrategy
		}
		if strat == nil || *strat != capacitybuffers.ColdProvisioningStrategy {
			continue
		}
		if message, blocked := blockedBySuspensionConstraints(ctx, info, consideredNodeGroups, constraints); blocked {
			blockedBuffers[buffer.UID] = blockedBuffer{buffer: buffer, message: message}
		}
	}

	for _, b := range blockedBuffers {
		autoscalingCtx.Recorder.Eventf(b.buffer, apiv1.EventTypeWarning, csnScaleUpFailedReason, "%s", b.message)
	}
}

// CleanUp implements status.ScaleUpStatusProcessor.
func (p *CSNScaleUpStatusProcessor) CleanUp() {
}

// blockedBySuspensionConstraints checks whether info.Pod's spec directly violates a CSN suspension
// constraint or whether any node group in info.RejectedNodeGroups was rejected by a CSN suspension
// constraint despite matching the pod's remaining required node affinity, returning the
// corresponding warning message.
func blockedBySuspensionConstraints(ctx context.Context, info status.NoScaleUpInfo, consideredNodeGroups map[string]cloudprovider.NodeGroup, constraints csn.SuspensionConstraints) (string, bool) {
	// The pod's own requests/selectors directly violate a suspension constraint, whatever node shapes are available.
	if message, blocked := constraints.BlocksPodRequest(info.Pod); blocked {
		return message, true
	}
	// Or it would have fit on a node that was rejected only because of a suspension constraint.
	return constraints.BlocksPodOnAnyNode(info.Pod, rejectedNodes(ctx, info, consideredNodeGroups)...)
}

// rejectedNodes returns the node group templates that rejected the pod.
func rejectedNodes(ctx context.Context, info status.NoScaleUpInfo, consideredNodeGroups map[string]cloudprovider.NodeGroup) []*apiv1.Node {
	nodes := make([]*apiv1.Node, 0, len(info.RejectedNodeGroups))
	for nodeGroupID := range info.RejectedNodeGroups {
		nodeGroup, found := consideredNodeGroups[nodeGroupID]
		if !found {
			continue
		}
		nodeInfo, err := nodeGroup.TemplateNodeInfo(ctx)
		if err != nil || nodeInfo == nil {
			continue
		}
		nodes = append(nodes, nodeInfo.Node())
	}
	return nodes
}
