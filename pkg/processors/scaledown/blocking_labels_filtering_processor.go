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

package scaledown

import (
	"context"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
)

// blockedNodeUnremovableReason is reported for nodes kept out of scale-down
// because they carry a scale-down-blocking label (e.g. a node bound to a TPU
// dynamic-slicing Slice via cloud.google.com/gke-tpu-slice).
//
// The vendored simulator.UnremovableReason enum cannot be extended from here, so
// we reuse ScaleDownDisabledAnnotation (semantically: scale-down intentionally
// disabled for this node) and carry the real reason + blocking label in the klog
// message below.
//
// TODO(b/558187147): surface a dedicated "bound to TPU slice" reason in the
// NoScaleDown visibility logs. This does NOT require extending the vendored enum:
// the GKE-owned visibility mapper (pkg/visibility/noscaledown) can emit a
// slice-specific message by detecting the blocking label on nodes reported with
// ScaleDownDisabledAnnotation. Tracked as a follow-up per
// go/ca-dynamic-slicing-early-scoping (observability via visibility logs).
const blockedNodeUnremovableReason = simulator.ScaleDownDisabledAnnotation

// BlockingLabelsFilteringProcessor is a scale-down processor (implements
// nodes.ScaleDownSetProcessor). It prevents scale-down of any node that carries
// one of the configured scale-down-blocking labels. For example, this includes the
// cloud.google.com/gke-tpu-slice label added by the Slice Controller to nodes
// bound to a TPU dynamic-slicing Slice.
//
// It must run BEFORE AtomicResizeFilteringProcessor. For atomically-scaled
// (ZeroOrMaxNodeScaling) node groups, removing a single blocked node from the
// candidate set is enough to protect the whole cube: AtomicResizeFilteringProcessor
// then sees fewer candidates than the group size and marks the remaining cube
// nodes unremovable (AtomicScaleDownFailed). Draining a partial cube would orphan
// the Slice (see go/ca-dynamic-slicing-early-scoping and the T2/T7 live-cluster
// tests). Because atomicity is restored downstream, this processor does not need
// to group nodes itself.
type BlockingLabelsFilteringProcessor struct {
	// blockingLabels are node label keys that make a node ineligible for
	// scale-down when present with a non-empty value.
	blockingLabels []string
}

// NewBlockingLabelsFilteringProcessor creates a new BlockingLabelsFilteringProcessor that
// blocks scale-down of nodes carrying any of blockingLabels (with a non-empty
// value).
func NewBlockingLabelsFilteringProcessor(blockingLabels []string) *BlockingLabelsFilteringProcessor {
	return &BlockingLabelsFilteringProcessor{blockingLabels: blockingLabels}
}

// FilterUnremovableNodes keeps nodes carrying a scale-down-blocking label out of
// scale-down. Every other node passes through unchanged; for atomic node groups,
// AtomicResizeFilteringProcessor (which must run after this processor) keeps the
// remaining cube nodes together.
func (p *BlockingLabelsFilteringProcessor) FilterUnremovableNodes(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleDownCtx *nodes.ScaleDownContext, candidates []simulator.NodeToBeRemoved) ([]simulator.NodeToBeRemoved, []simulator.UnremovableNode) {
	nodesToBeRemoved := make([]simulator.NodeToBeRemoved, 0, len(candidates))
	unremovableNodes := []simulator.UnremovableNode{}

	for _, c := range candidates {
		if label, found := p.blockingLabel(c.Node); found {
			klog.Infof("BlockingLabelsFilteringProcessor: not scaling down node %s; carries scale-down-blocking label %q=%q", c.Node.Name, label, c.Node.Labels[label])
			unremovableNodes = append(unremovableNodes, simulator.UnremovableNode{Node: c.Node, Reason: blockedNodeUnremovableReason})
			continue
		}
		nodesToBeRemoved = append(nodesToBeRemoved, c)
	}

	return nodesToBeRemoved, unremovableNodes
}

// CleanUp is called at CA termination.
func (p *BlockingLabelsFilteringProcessor) CleanUp() {}

// blockingLabel returns the first configured scale-down-blocking label that the
// node carries with a non-empty value, and whether one was found.
func (p *BlockingLabelsFilteringProcessor) blockingLabel(node *apiv1.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	for _, l := range p.blockingLabels {
		if node.Labels[l] != "" {
			return l, true
		}
	}
	return "", false
}
