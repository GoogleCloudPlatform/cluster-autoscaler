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

package history

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	cc_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
	cloudprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	scaledownstatus "sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/drain"
	pod_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/pod"
)

// priorityKey identifies a single CCC priority.
type priorityKey struct {
	crdID status.CRDId
	index string
}

// reasonRank is the position of each customer-facing reason in the order CA checks it, so that
// nodes under a later reason are known to have passed the earlier checks.
//
// CA defines no such order in a single place, it follows from where each check runs:
// BlockingLabelsFilteringProcessor, then eligibility.Checker (annotation, readiness,
// utilization), the drain simulation, unneeded.Nodes.RemovableAt (unneeded time, resource
// limits, node group minimum size) and finally the scale-down set processor (atomic groups).
// Nothing enforces it here, it has to be kept in sync with CA by hand.
var reasonRank = map[string]int{
	crd.ConsolidationReasonUsedByFormedSlice:          0,
	crd.ConsolidationReasonNodeConsolidationDisabled:  1,
	crd.ConsolidationReasonNodeNotReady:               2,
	crd.ConsolidationReasonAboveUtilizationThreshold:  3,
	crd.ConsolidationReasonBlockingPods:               4,
	crd.ConsolidationReasonPodDisruptionBudget:        5,
	crd.ConsolidationReasonNoPlaceToMovePods:          6,
	crd.ConsolidationReasonNotUnneededLongEnough:      7,
	crd.ConsolidationReasonMinCapacityReached:         8,
	crd.ConsolidationReasonAtomicGroupBlocked:         9,
	crd.ConsolidationReasonRecentConsolidationFailure: 10,
	crd.ConsolidationReasonConsolidationBlocked:       11,
}

// ScaleDownBlockedStatusProcessor reports, per CCC priority, how many nodes are being removed
// and how many can't be removed, broken down by reason.
type ScaleDownBlockedStatusProcessor struct {
	lister             lister.Lister
	matcher            computeclass.Matcher
	updatesCh          chan<- status.UpdateMessage
	experimentsManager experiments.Manager
	now                func() time.Time
	// blockingLabels are the --scale-down-blocking-node-labels keys.
	blockingLabels []string
	// lastReported holds the priorities with a non-empty status, which have to be cleared
	// explicitly once they have nothing to report.
	lastReported sets.Set[priorityKey]
	// lastReason holds the reason reported for each node in the previous pass.
	lastReason map[string]string
	// loggedUnresolved holds the node groups already logged as matching no CCC priority.
	loggedUnresolved sets.Set[string]
}

// NewScaleDownBlockedStatusProcessor creates a new ScaleDownBlockedStatusProcessor.
func NewScaleDownBlockedStatusProcessor(lister lister.Lister, provider machineConfigProvider, updatesCh chan<- status.UpdateMessage, experimentsManager experiments.Manager, blockingLabels []string) *ScaleDownBlockedStatusProcessor {
	return &ScaleDownBlockedStatusProcessor{
		lister:             lister,
		matcher:            computeclass.NewMatcher(lister, provider),
		updatesCh:          updatesCh,
		experimentsManager: experimentsManager,
		now:                time.Now,
		blockingLabels:     blockingLabels,
		lastReported:       sets.New[priorityKey](),
		lastReason:         make(map[string]string),
		loggedUnresolved:   sets.New[string](),
	}
}

// Process reports the Consolidation status of every CCC priority.
func (p *ScaleDownBlockedStatusProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleDownStatus *scaledownstatus.ScaleDownStatus) {
	if scaleDownStatus == nil || p.updatesCh == nil {
		return
	}
	if !computeclass.IsComputeClassScaleDownStatusEnabled(p.experimentsManager) {
		return
	}

	// The scale-down status only lists the nodes CA inspected, the rest of the picture comes
	// from the nodes in the cluster snapshot.
	var nodeInfos []*framework.NodeInfo
	var cloudProvider cloudprovider.CloudProvider
	if autoscalingCtx != nil && autoscalingCtx.ClusterSnapshot != nil {
		cloudProvider = autoscalingCtx.CloudProvider
		var err error
		if nodeInfos, err = autoscalingCtx.ClusterSnapshot.ListNodeInfos(); err != nil {
			klog.Errorf("Failed to list nodes, the consolidation status will miss the nodes CA did not inspect: %v", err)
		}
	}
	snapshot := p.getSnapshotInfo(ctx, cloudProvider, nodeInfos)

	blockedCounts, deletionsInProgress := p.groupByReason(scaleDownStatus, snapshot)
	p.lastReported = p.reportConsolidation(blockedCounts, deletionsInProgress, snapshot.nodeCounts)
}

// CleanUp implements status.ScaleDownStatusProcessor.
func (p *ScaleDownBlockedStatusProcessor) CleanUp() {
}

// groupByReason returns, per CCC priority, the number of blocked nodes per reason and the
// number of nodes being deleted. Nodes with no scale-down blocked reason are left out, unless
// their node group is at its minimum size, in which case they are reported under
// MinCapacityReached, or scale-down is in cooldown, in which case they keep the reason
// reported in the previous pass.
func (p *ScaleDownBlockedStatusProcessor) groupByReason(scaleDownStatus *scaledownstatus.ScaleDownStatus, snapshot snapshotInfo) (map[priorityKey]map[string]int, map[priorityKey]int) {
	blockedCounts := make(map[priorityKey]map[string]int)
	addBlocked := func(key priorityKey, reason string) {
		if blockedCounts[key] == nil {
			blockedCounts[key] = make(map[string]int)
		}
		blockedCounts[key][reason]++
	}
	deletionsInProgress := make(map[priorityKey]int)
	// counted holds the nodes already reported, either as being deleted or as blocked, so that
	// none is reported twice: a node being deleted shows up in ScaledDownNodes and, on later
	// loops, as CurrentlyBeingDeleted, and a blocked node in a node group at its minimum size
	// must keep the reason CA gave for it.
	counted := sets.New[string]()
	reasons := make(map[string]string, len(p.lastReason))

	for _, scaledDown := range scaleDownStatus.ScaledDownNodes {
		if scaledDown == nil || scaledDown.Node == nil || scaledDown.NodeGroup == nil {
			continue
		}
		key, ok := p.resolvePriority(scaledDown.NodeGroup)
		if !ok {
			continue
		}
		counted.Insert(scaledDown.Node.Name)
		deletionsInProgress[key]++
	}
	for _, unremovable := range scaleDownStatus.UnremovableNodes {
		if unremovable == nil || unremovable.Node == nil || unremovable.NodeGroup == nil {
			continue
		}
		key, ok := p.resolvePriority(unremovable.NodeGroup)
		if !ok {
			continue
		}
		if unremovable.Reason == simulator.CurrentlyBeingDeleted {
			if !counted.Has(unremovable.Node.Name) {
				counted.Insert(unremovable.Node.Name)
				deletionsInProgress[key]++
			}
			continue
		}
		reason := p.blockedReason(unremovable, snapshot.nodeInfos[unremovable.Node.Name])
		if reason == "" {
			continue
		}
		reasons[unremovable.Node.Name] = reason
		counted.Insert(unremovable.Node.Name)
		addBlocked(key, reason)
	}

	// The nodes of a node group at its minimum size are never inspected by CA (see
	// PreFilteringScaleDownNodeProcessor.GetScaleDownCandidates), so they have no reason in the
	// scale-down status, and may be kept by their utilization or pods as well. Nodes that do
	// have one were counted above and keep it.
	for nodeName, key := range snapshot.minSizeNodesToPriority {
		if counted.Has(nodeName) {
			continue
		}
		counted.Insert(nodeName)
		addBlocked(key, crd.ConsolidationReasonMinCapacityReached)
	}

	// In cooldown CA skips NodesToDelete, so the reasons it gives there (unneeded time, atomic
	// groups, minimum sizes) are missing. Keep the previous reason until CA gives a new one.
	if scaleDownStatus.Result == scaledownstatus.ScaleDownInCooldown {
		for nodeName, reason := range p.lastReason {
			key, ok := snapshot.nodePriority[nodeName]
			if !ok || counted.Has(nodeName) {
				continue
			}
			reasons[nodeName] = reason
			counted.Insert(nodeName)
			addBlocked(key, reason)
		}
	}
	p.lastReason = reasons
	return blockedCounts, deletionsInProgress
}

// blockedReason returns the reason to report for an unremovable node. CA reports
// RecentlyUnremovable while it skips re-running the drain simulation of a node that failed it
// recently, so the node keeps the reason reported last time.
func (p *ScaleDownBlockedStatusProcessor) blockedReason(un *scaledownstatus.UnremovableNode, nodeInfo *framework.NodeInfo) string {
	if un.Reason != simulator.RecentlyUnremovable {
		return p.mapReason(un, nodeInfo)
	}
	if reason, ok := p.lastReason[un.Node.Name]; ok {
		return reason
	}
	return crd.ConsolidationReasonConsolidationBlocked
}

type snapshotInfo struct {
	// nodeCounts is the number of nodes per CCC priority.
	nodeCounts map[priorityKey]int
	// nodePriority is the CCC priority of every node.
	nodePriority map[string]priorityKey
	// minSizeNodesToPriority maps the nodes of node groups at their minimum size to their
	// CCC priority.
	minSizeNodesToPriority map[string]priorityKey
	// nodeInfos holds the snapshot NodeInfo of every node.
	nodeInfos map[string]*framework.NodeInfo
}

// getSnapshotInfo collects, for the nodes in the cluster that belong to a CCC priority, their
// priority and whether their node group is at its minimum size.
func (p *ScaleDownBlockedStatusProcessor) getSnapshotInfo(ctx context.Context, cloudProvider cloudprovider.CloudProvider, nodeInfos []*framework.NodeInfo) snapshotInfo {
	info := snapshotInfo{
		nodeCounts:             make(map[priorityKey]int),
		nodePriority:           make(map[string]priorityKey),
		minSizeNodesToPriority: make(map[string]priorityKey),
		nodeInfos:              make(map[string]*framework.NodeInfo),
	}
	if cloudProvider == nil {
		return info
	}
	atMinByNodeGroup := make(map[string]bool)
	for _, nodeInfo := range nodeInfos {
		node := nodeInfo.Node()
		if node == nil || isGeneratedNode(node) {
			continue
		}
		nodeGroup, err := cloudProvider.NodeGroupForNode(ctx, node)
		if err != nil || nodeGroup == nil {
			continue
		}
		key, ok := p.resolvePriority(nodeGroup)
		if !ok {
			continue
		}
		info.nodeCounts[key]++
		info.nodePriority[node.Name] = key
		atMin, ok := atMinByNodeGroup[nodeGroup.Id()]
		if !ok {
			atMin = isAtMinSize(ctx, nodeGroup)
			atMinByNodeGroup[nodeGroup.Id()] = atMin
		}
		if atMin {
			info.minSizeNodesToPriority[node.Name] = key
		}
		info.nodeInfos[node.Name] = nodeInfo
	}
	return info
}

// isGeneratedNode reports whether CA generated the node from a node group template.
func isGeneratedNode(node *apiv1.Node) bool {
	_, generated := node.Annotations[gke_labels.NodeGeneratedFromTemplateAnnotation]
	return generated
}

// TODO(b/570549559): Share this check with PreFilteringScaleDownNodeProcessor once it is a
// method of the node group in the OSS cloudprovider.
func isAtMinSize(ctx context.Context, nodeGroup cloudprovider.NodeGroup) bool {
	size, err := nodeGroup.TargetSize(ctx)
	if err != nil {
		klog.Warningf("Failed to get the target size of node group %v: %v", nodeGroup.Id(), err)
		return false
	}
	return size <= nodeGroup.MinSize(ctx)
}

// heldOnlyByMinCapacity reports whether the only pods on the node that would need moving are
// compute class minimum capacity fake pods, which fill their node so that the drain simulation
// finds nowhere to move them. With any other pod on the node, that pod may be what can't move.
//
// Only reached while the fake pods stay in the scale-down snapshot. With
// ComputeClassMinCapacityPodsRemoval (gkecl/2319094) they are removed before scale-down and the
// floor arrives as MinimalResourceLimitExceeded or NodeGroupMinSizeReached instead.
// TODO(b/570549559): Remove together with the ComputeClassMinCapacityPodsRemoval flag.
func heldOnlyByMinCapacity(nodeInfo *framework.NodeInfo) bool {
	if nodeInfo == nil {
		return false
	}
	hasFakePod := false
	for _, podInfo := range nodeInfo.Pods() {
		switch {
		case cc_processors.IsMinCapacityFakePod(podInfo.Pod):
			hasFakePod = true
		case pod_util.IsDaemonSetPod(podInfo.Pod), pod_util.IsMirrorPod(podInfo.Pod), pod_util.IsStaticPod(podInfo.Pod):
			// Not moved by a drain.
		default:
			return false
		}
	}
	return hasFakePod
}

// reportConsolidation sends the Consolidation status of every priority that needs one and
// returns the priorities left with a non-empty status.
func (p *ScaleDownBlockedStatusProcessor) reportConsolidation(blockedCounts map[priorityKey]map[string]int, deletionsInProgress, nodeCounts map[priorityKey]int) sets.Set[priorityKey] {
	// Priorities reported last pass are included to clear a status that no longer holds.
	toReport := sets.KeySet(blockedCounts).
		Union(sets.KeySet(deletionsInProgress)).
		Union(sets.KeySet(nodeCounts)).
		Union(p.lastReported)

	reported := sets.New[priorityKey]()
	for key := range toReport {
		consolidation := p.consolidationStatus(blockedCounts[key], deletionsInProgress[key], nodeCounts[key])
		sent := status.TrySendRuleUpdate(p.updatesCh, status.UpdateMessage{
			Id: key.crdID,
			Mutate: func(s crd.CRDStatus) {
				s.UpdateRuleConsolidationStatus(key.index, consolidation)
			},
		}, key.index)
		// An empty status is written once, not on every loop. A dropped update is retried.
		if !sent || !isEmptyConsolidation(consolidation) {
			reported.Insert(key)
		}
	}
	return reported
}

// consolidationStatus builds the Consolidation status of a single priority.
func (p *ScaleDownBlockedStatusProcessor) consolidationStatus(blockedByReason map[string]int, deletionsInProgress, totalNodes int) crd.ConsolidationStatus {
	blocked := 0
	for _, n := range blockedByReason {
		blocked += n
	}
	return crd.ConsolidationStatus{
		ActuationInProgress: deletionsInProgress,
		// The node counts and the scale-down status are sampled at slightly different
		// times, so this can briefly go negative.
		NotProcessed: max(0, totalNodes-deletionsInProgress-blocked),
		BlockedNodes: sortByEvaluationOrder(blockedByReason),
		MeasuredAt:   metav1.NewTime(p.now()),
	}
}

func isEmptyConsolidation(cs crd.ConsolidationStatus) bool {
	return cs.ActuationInProgress == 0 && cs.NotProcessed == 0 && len(cs.BlockedNodes) == 0
}

// sortByEvaluationOrder sorts the reasons in the order CA checks them, see reasonRank.
func sortByEvaluationOrder(blockedByReason map[string]int) []crd.BlockedNodesByReason {
	blocked := make([]crd.BlockedNodesByReason, 0, len(blockedByReason))
	for reason, count := range blockedByReason {
		blocked = append(blocked, crd.BlockedNodesByReason{Reason: reason, Count: count})
	}
	rank := func(reason string) int {
		if r, ok := reasonRank[reason]; ok {
			return r
		}
		return len(reasonRank)
	}
	slices.SortFunc(blocked, func(a, b crd.BlockedNodesByReason) int {
		return cmp.Or(cmp.Compare(rank(a.Reason), rank(b.Reason)), cmp.Compare(a.Reason, b.Reason))
	})
	return blocked
}

// resolvePriority returns the CCC priority of the node group. Node groups matching no priority
// are logged once.
func (p *ScaleDownBlockedStatusProcessor) resolvePriority(nodeGroup cloudprovider.NodeGroup) (priorityKey, bool) {
	index, c, err := getRuleIndex(nodeGroup, p.lister, p.matcher)
	if err != nil {
		if p.loggedUnresolved.Has(nodeGroup.Id()) {
			klog.V(4).Infof("Node group %v is still not attributed to any ComputeClass priority: %v", nodeGroup.Id(), err)
		} else {
			klog.Warningf("Node group %v is not attributed to any ComputeClass priority, its nodes are left out of the consolidation status: %v", nodeGroup.Id(), err)
			p.loggedUnresolved.Insert(nodeGroup.Id())
		}
		return priorityKey{}, false
	}
	p.loggedUnresolved.Delete(nodeGroup.Id())
	if c == nil {
		return priorityKey{}, false
	}
	return priorityKey{
		crdID: status.CRDId{CRDName: c.Name(), CRDLabel: c.Label()},
		index: strconv.Itoa(index),
	}, true
}

// mapReason maps a CA unremovable reason to the reason reported on the CCC status, or "" when
// CA did not inspect the node. Every reason returned must be in crd.ConsolidationReasons, the API
// server rejects the whole status otherwise.
//
// TODO(b/570549559): RecentConsolidationFailure is never produced, as no per-node reason maps
// to it. Derive it from the scale-down status instead (a non-OK Result, or the cooldown after
// a failed deletion).
func (p *ScaleDownBlockedStatusProcessor) mapReason(un *scaledownstatus.UnremovableNode, nodeInfo *framework.NodeInfo) string {
	switch un.Reason {
	case simulator.NotUnderutilized:
		return crd.ConsolidationReasonAboveUtilizationThreshold
	case simulator.NotUnneededLongEnough:
		return crd.ConsolidationReasonNotUnneededLongEnough
	case simulator.ScaleDownUnreadyDisabled, simulator.NotUnreadyLongEnough:
		return crd.ConsolidationReasonNodeNotReady
	case simulator.NoPlaceToMovePods:
		if heldOnlyByMinCapacity(nodeInfo) {
			return crd.ConsolidationReasonMinCapacityReached
		}
		return crd.ConsolidationReasonNoPlaceToMovePods
	case simulator.BlockedByOnCompletionPod:
		return crd.ConsolidationReasonBlockingPods
	case simulator.BlockedByPod:
		if un.BlockingPod != nil && un.BlockingPod.Reason == drain.NotEnoughPdb {
			return crd.ConsolidationReasonPodDisruptionBudget
		}
		return crd.ConsolidationReasonBlockingPods
	case simulator.AtomicScaleDownFailed:
		return crd.ConsolidationReasonAtomicGroupBlocked
	case simulator.NodeGroupMinSizeReached, simulator.MinimalResourceLimitExceeded:
		return crd.ConsolidationReasonMinCapacityReached
	case simulator.ScaleDownDisabledAnnotation:
		// BlockingLabelsFilteringProcessor reports blocking labels under this reason too.
		if p.hasBlockingLabel(un.Node) {
			return crd.ConsolidationReasonUsedByFormedSlice
		}
		return crd.ConsolidationReasonNodeConsolidationDisabled
	case simulator.NodeGroupMaxDeletionCountReached, simulator.NotUnneededOtherReason:
		// The per-loop removal cap was hit, or the node was never inspected.
		return ""
	default:
		return crd.ConsolidationReasonConsolidationBlocked
	}
}

// hasBlockingLabel reports whether the node has a non-empty --scale-down-blocking-node-labels label.
func (p *ScaleDownBlockedStatusProcessor) hasBlockingLabel(node *apiv1.Node) bool {
	for _, label := range p.blockingLabels {
		if node.Labels[label] != "" {
			return true
		}
	}
	return false
}
