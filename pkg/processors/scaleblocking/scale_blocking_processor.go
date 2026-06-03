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

package scaleblocking

import (
	"fmt"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/callbacks"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/klog/v2"
)

const (
	// BlockedMigsContextKey is the key under which this processor reports which MIGs were
	// blocked, and for what reason.
	BlockedMigsContextKey = "blocked-migs-status.processors.gke-autoscaler"
)

// CloudProvider is the subset of GkeCloudProvider needed for scaleblocking.Processor.
type CloudProvider interface {
	GkeMigForNode(node *apiv1.Node) (*gke.GkeMig, error)
}

// BlockedMigReason denotes a reason why a particular MIG was blocked from scaling. If you're adding a new reason,
// make sure to add a corresponding visibility message as well.
type BlockedMigReason string

// BlockedMigReasonSet is a set of BlockedMigReasons, defined explicitly for readability.
type BlockedMigReasonSet map[BlockedMigReason]bool

func (s BlockedMigReasonSet) String() string {
	var reasons []string
	for reason := range s {
		reasons = append(reasons, string(reason))
	}
	return strings.Join(reasons, ", ")
}

// BlockedMigs contains ids of MIGs which should have scaling blocked, with a set of reasons explaining why.
type BlockedMigs struct {
	NoScaleUpMigs   map[string]BlockedMigReasonSet
	NoScaleDownMigs map[string]BlockedMigReasonSet
}

func (bm BlockedMigs) String() string {
	var noScaleUpMigs []string
	for migId, reasons := range bm.NoScaleUpMigs {
		noScaleUpMigs = append(noScaleUpMigs, fmt.Sprintf("<%s: %s>", migId, reasons.String()))
	}
	var noScaleDownMigs []string
	for migId, reasons := range bm.NoScaleDownMigs {
		noScaleDownMigs = append(noScaleDownMigs, fmt.Sprintf("<%s: %s>", migId, reasons.String()))
	}
	return fmt.Sprintf("<NoScaleUpMigs: [%s], NoScaleDownMigs: [%s]>", strings.Join(noScaleUpMigs, ", "), strings.Join(noScaleDownMigs, ", "))
}

// Union returns BlockedMigs which contain MIGs from both bm and otherBlockedMigs, with reasons for each MIG combined.
func (bm BlockedMigs) Union(otherBlockedMigs BlockedMigs) BlockedMigs {
	return BlockedMigs{
		NoScaleUpMigs:   unionNoScaleMigs(bm.NoScaleUpMigs, otherBlockedMigs.NoScaleUpMigs),
		NoScaleDownMigs: unionNoScaleMigs(bm.NoScaleDownMigs, otherBlockedMigs.NoScaleDownMigs),
	}
}

// BlockedMigsSource provides BlockedMigs.
type BlockedMigsSource interface {
	BlockedMigs() BlockedMigs
	CleanUp()
}

// Processor can be used to temporarily block scaling specific MIGs.
type Processor struct {
	cloudProvider CloudProvider
	sources       []BlockedMigsSource
	// blockedMigs shouldn't be accessed directly, use p.getBlockedMigs() instead.
	blockedMigs BlockedMigs
}

// NewProcessor returns a processor blocking scaling of MIGs provided by the sources.
func NewProcessor(cloudProvider CloudProvider, sources []BlockedMigsSource) *Processor {
	return &Processor{cloudProvider: cloudProvider, sources: sources}
}

// FilterNoScaleUpNodeGroups removes blocked MIGs from being considered as candidates in scale-up.
func (p *Processor) FilterNoScaleUpNodeGroups(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup) []cloudprovider.NodeGroup {
	blockedMigs := p.getBlockedMigs(ctx.ProcessorCallbacks)
	return p.filterNodeGroups(nodeGroups, blockedMigs.NoScaleUpMigs)
}

// FilterNoScaleDownNodeGroups removes blocked MIGs from being considered as candidates for node-pool deletion.
func (p *Processor) FilterNoScaleDownNodeGroups(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup) []cloudprovider.NodeGroup {
	blockedMigs := p.getBlockedMigs(ctx.ProcessorCallbacks)
	return p.filterNodeGroups(nodeGroups, blockedMigs.NoScaleDownMigs)
}

// GetPodDestinationCandidates is a no-op, this processor needs to define it to implement ScaleDownNodeProcessor.
func (p *Processor) GetPodDestinationCandidates(_ *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return nodes, nil
}

// GetScaleDownCandidates removes nodes belonging to blocked MIGs from being considered as candidates for scale-down.
func (p *Processor) GetScaleDownCandidates(ctx *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	blockedMigs := p.getBlockedMigs(ctx.ProcessorCallbacks)
	var filteredNodes []*apiv1.Node
	for _, node := range nodes {
		mig, err := p.cloudProvider.GkeMigForNode(node)
		if err != nil {
			klog.Errorf("Failed to retrieve MIG for node %s: %v", node.Name, err)
			filteredNodes = append(filteredNodes, node)
			continue
		}
		if mig == nil {
			// This processor should be run after
			// PreFilteringProcessor, so we expect to only see
			// nodes with node group config here. However, there
			// are race conditions possible if GCE cache gets
			// updated and it turns out that the underlying VM is
			// no longer there. We need to log this, but this is
			// generally not an error.
			// In principle, we could filter out such node here (as
			// it is most likely not possible to scale it down
			// anyway), but keeping it is actually ok too and it
			// helps to maintain single responsibility principle
			// for this processor: it only filters out nodes that
			// it knows are blocked.
			klog.V(4).Infof("Node %s from non-autoscaled MIG found in scaleblocking.Processor, unable to check blocked status.", node.Name)
			filteredNodes = append(filteredNodes, node)
			continue
		}
		if _, blocked := blockedMigs.NoScaleDownMigs[mig.Id()]; !blocked {
			filteredNodes = append(filteredNodes, node)
		}
	}
	return filteredNodes, nil
}

// getBlockedMigs returns a snapshot of blocked MIGs provided by the sources. The MIGs are fetched from each source only
// on the first call in each autoscaling loop, and are reported in ProcessorCallbacks.
func (p *Processor) getBlockedMigs(processorCallbacks callbacks.ProcessorCallbacks) BlockedMigs {
	p.fetchAndReportBlockedMigsIfNeeded(processorCallbacks)
	return p.blockedMigs
}

// fetchAndReportBlockedMigsIfNeeded fetches blocked MIGs from sources and reports them in ProcessorCallbacks if they
// haven't already been reported in this autoscaling loop. Otherwise, it is a no-op.
//
// This processor needs to access blocked MIGs at various points in the autoscaling loop. We want to fetch them from
// sources and report to CA Viz only once per loop, to have a consistent state. ProcessorCallbacks are reset at
// the beginning of the autoscaling loop, so we can just check if we've already reported the MIGs, and only do it once.
func (p *Processor) fetchAndReportBlockedMigsIfNeeded(processorCallbacks callbacks.ProcessorCallbacks) {
	if _, blockedMigsAlreadyReported := processorCallbacks.GetExtraValue(BlockedMigsContextKey); blockedMigsAlreadyReported {
		// We've already fetched and reported the blocked MIGs in this autoscaling loop.
		return
	}
	// This is a first call to this function in this autoscaling loop, fetch and report the blocked MIGs.
	p.blockedMigs = p.fetchBlockedMigs()
	processorCallbacks.SetExtraValue(BlockedMigsContextKey, p.blockedMigs)
	klog.Infof("MIGs blocked from scaling: %s", p.blockedMigs.String())
}

func (p *Processor) fetchBlockedMigs() BlockedMigs {
	result := BlockedMigs{}
	for _, source := range p.sources {
		result = result.Union(source.BlockedMigs())
	}
	return result
}

// filterNodeGroups returns a copy of nodeGroups with MIGs present in blockedMigs filtered out.
func (p *Processor) filterNodeGroups(nodeGroups []cloudprovider.NodeGroup, blockedMigs map[string]BlockedMigReasonSet) []cloudprovider.NodeGroup {
	var filteredNodeGroups []cloudprovider.NodeGroup
	for _, nodeGroup := range nodeGroups {
		mig := nodeGroup.(*gke.GkeMig)
		if _, blocked := blockedMigs[mig.Id()]; !blocked {
			filteredNodeGroups = append(filteredNodeGroups, nodeGroup)
		}
	}
	return filteredNodeGroups
}

// CleanUp executes clean-up routines for all provided blocking sources.
func (p *Processor) CleanUp() {
	for _, source := range p.sources {
		source.CleanUp()
	}
}

func unionNoScaleMigs(noScaleMigsA, noScaleMigsB map[string]BlockedMigReasonSet) map[string]BlockedMigReasonSet {
	result := copyNoScaleMigs(noScaleMigsA)
	for migId, reasonSetB := range noScaleMigsB {
		reasonSet, found := result[migId]
		if !found {
			reasonSet = BlockedMigReasonSet{}
			result[migId] = reasonSet
		}
		for reasonB := range reasonSetB {
			reasonSet[reasonB] = true
		}
	}
	return result
}

func copyNoScaleMigs(noScaleMigs map[string]BlockedMigReasonSet) map[string]BlockedMigReasonSet {
	result := make(map[string]BlockedMigReasonSet, len(noScaleMigs))
	for migId, reasonSet := range noScaleMigs {
		reasonSetCopy := BlockedMigReasonSet{}
		for reason := range reasonSet {
			reasonSetCopy[reason] = true
		}
		result[migId] = reasonSetCopy
	}
	return result
}
