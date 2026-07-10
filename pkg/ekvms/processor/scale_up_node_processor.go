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
	gocontext "context"
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/fake"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	ca_taints "k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/component-helpers/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	ekvms_customthresholds "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/backoff/customthresholds"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/daemon"
	"k8s.io/kubernetes/pkg/util/taints"
)

const (
	processResizableVmUpsizes = "scaleUp:processResizableVmUpsizes"
	scheduleLookaheadPods     = "scaleUp:scheduleLookaheadPods"
)

type podId = types.UID

type scaleUpMetrics interface {
	RegisterResizableVmPodsSchedulableOnUpsizes(machineFamily string, pods int)
	UpdateResizableVmUnschedulableLookaheadPodsCount(machineFamily string, pods int)
	UpdateResizableVmTotalNodesLookaheadSpace(machineFamily string, val size.Allocatable)
}
type NodesState int

const (
	allNodes NodesState = iota
	idleNodes
	processingNodes
	resizableNodes
	nonResizableNodes
)

type CCCRuleBackoff interface {
	RuleBackoffStatus(crd.CRD, int, time.Time) backoff.Status
}

type cloudProvider interface {
	GkeMigForNode(node *v1.Node) (*gke.GkeMig, error)
	IsAutopilotEnabled() bool
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

type resizableNodesSnapshotsByRule map[int]operationtracker.ResizableNodesSnapshot
type resizableNodesSnapshotsByCCC map[string]resizableNodesSnapshotsByRule

// ScaleUpNodeProcessor is responsible for resizable VM computation in scale-up algorithm.
type ScaleUpNodeProcessor struct {
	resizableVmManager         operationtracker.Manager
	simulator                  *scheduling.HintingSimulator
	sizeCalculator             calculator.Calculator
	metrics                    scaleUpMetrics
	cccRuleBackoff             CCCRuleBackoff
	cccCrdLister               lister.Lister
	cloudProvider              cloudProvider
	mcp                        *machinetypes.MachineConfigProvider
	matcher                    computeclass.Matcher
	customThresholdsProvider   ekvms_customthresholds.CustomThresholdsProvider
	attemptsToScheduleOnUpsize map[podId]int
}

// NewScaleUpNodeProcessor returns a new instance of the ScaleUpNodeProcessor.
func NewScaleUpNodeProcessor(cloudProvider cloudProvider, resizableVmManager operationtracker.Manager, sizeCalculator calculator.Calculator, metrics scaleUpMetrics, cccRuleBackoff CCCRuleBackoff, cccCrdLister lister.Lister, customThresholdsProvider ekvms_customthresholds.CustomThresholdsProvider) *ScaleUpNodeProcessor {
	return &ScaleUpNodeProcessor{
		resizableVmManager:         resizableVmManager,
		simulator:                  scheduling.NewHintingSimulator(),
		sizeCalculator:             sizeCalculator,
		metrics:                    metrics,
		cccRuleBackoff:             cccRuleBackoff,
		cccCrdLister:               cccCrdLister,
		cloudProvider:              cloudProvider,
		mcp:                        cloudProvider.MachineConfigProvider(),
		matcher:                    computeclass.NewMatcher(cccCrdLister, cloudProvider),
		customThresholdsProvider:   customThresholdsProvider,
		attemptsToScheduleOnUpsize: make(map[podId]int),
	}
}

// Process filters out pods that can be scheduled by performing resizable VM resizes.
// It readjusts the balloon pod size for simulation purposes and requests upsizes in Manager.
func (p *ScaleUpNodeProcessor) Process(ctx *context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	resizableFamilies := p.mcp.AllResizableMachineFamilies()
	return p.process(ctx, unschedulablePods, true, resizableFamilies)
}

func (p *ScaleUpNodeProcessor) process(ctx *context.AutoscalingContext, unschedulablePods []*v1.Pod, emitMetrics bool, resizableFamilies []machinetypes.MachineFamily) ([]*v1.Pod, error) {
	if !isAnyResizingEnabled(p.resizableVmManager, resizableFamilies) {
		return unschedulablePods, nil
	}
	var podsForcingScaleUp []*v1.Pod
	if p.isForceScaleUpFeatureEnabled() {
		// Filter out all pods that faces a lot of failed upsizes. Remove them from the list now and inject back right before return.
		unschedulablePods, podsForcingScaleUp = filterPodsForcingScaleUp(unschedulablePods, p.attemptsToScheduleOnUpsize, p.customThresholdsProvider.GetUpsizeTriesThreshold())

	}

	if emitMetrics {
		defer metrics.UpdateDurationFromStart(processResizableVmUpsizes, time.Now())
	}

	resizableNodesSnapshot := p.resizableVmManager.FilteredNodesSnapshot(true, operationtracker.ResizableOnly)
	// Omit nodes that have ongoing eviction.
	// Needed until we switch Balloon Pods to in-place pod updates. Context: b/433668832.
	for nodeName := range resizableNodesSnapshot {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Warningf("Retrieving node info for node %q failed: %v", nodeName, err)
			continue
		}
		if nodeInfo.GetRequested().GetMilliCPU() > nodeInfo.GetAllocatable().GetMilliCPU() ||
			nodeInfo.GetRequested().GetMemory() > nodeInfo.GetAllocatable().GetMemory() {
			if hasResizingdPod(nodeInfo) {
				klog.V(4).Infof("Node %q has to-be-resized pod; allowing upsizes", nodeName)
			} else {
				klog.V(4).Infof("Node %q has onging pod preemption; treating the node as non-resizable", nodeName)
				delete(resizableNodesSnapshot, nodeName)
			}
		}
	}

	if len(resizableNodesSnapshot) == 0 {
		return unschedulablePods, nil
	}
	desiredSizes := make(map[string]size.Allocatable, len(resizableNodesSnapshot))
	maxSizes := make(map[string]size.Allocatable, len(resizableNodesSnapshot))
	for nodeName, resizableNode := range resizableNodesSnapshot {
		desiredSizes[nodeName] = resizableNode.DesiredSize
		maxSizes[nodeName] = size.Max(resizableNode.UpsizableMaxSize, resizableNode.DesiredSize)
	}

	clusterSnapshot := ctx.ClusterSnapshot
	clusterSnapshot.Fork()

	if err := AdjustBalloonPodsSize(ctx.ClusterSnapshot, maxSizes, nil); err != nil {
		clusterSnapshot.Revert()
		return unschedulablePods, err
	}

	// We keep unschedulablePods unchaged so it can be returned in case of an error.
	var unschedulablePodsWithDS []*v1.Pod
	if dsPods, err := generateMissingDaemonSetPods(ctx, resizableNodesSnapshot); err != nil {
		klog.Warningf("Generating DaemonSet Pods error: %v", err)
		unschedulablePodsWithDS = unschedulablePods
	} else {
		klog.V(4).Infof("Injecting %d DaemonSet Pods into upsize logic", len(dsPods))
		unschedulablePodsWithDS = slices.Concat(dsPods, unschedulablePods)
	}

	nodeInfos, err := clusterSnapshot.ListNodeInfos()
	if err != nil {
		clusterSnapshot.Revert()
		return unschedulablePods, fmt.Errorf("error during listing nodeInfos: %v", err)
	}

	// Move lookahead pods to the back of the scheduling queue to prioritize other unschedulable pods.
	removedScheduledLAPods := removeScheduledLookaheadPods(resizableNodesSnapshot, nodeInfos)
	unschedulablePodsWithLAPods := slices.Concat(unschedulablePodsWithDS, removedScheduledLAPods)
	removedScheduledLAPodsCount := len(removedScheduledLAPods)
	if removedScheduledLAPodsCount > 0 {
		klog.V(4).Infof("ScaleUpNodeProcessor: moved %d lookahead pods to the back of the scheduling queue: %s", removedScheduledLAPodsCount, strings.Join(podDetails(removedScheduledLAPods), ", "))
	}

	schedulableStatuses, unschedulableStatuses, err := p.schedulePods(clusterSnapshot, resizableNodesSnapshot, unschedulablePodsWithLAPods)
	if err != nil {
		clusterSnapshot.Revert()
		return unschedulablePods, fmt.Errorf("error during scheduling pods: %v", err)
	}

	requestedResources := map[string]size.Allocatable{}
	laResources := map[string]size.Allocatable{}
	totalUpsizablePerFamily := map[string]size.Allocatable{}
	for _, family := range resizableFamilies {
		if p.resizableVmManager.IsResizingEnabled(family.Name()) {
			totalUpsizablePerFamily[family.Name()] = size.Allocatable{}
		}
	}
	totalUpsizablePerFamily["unknown"] = size.Allocatable{}

	for nodeName, resizableNode := range resizableNodesSnapshot {
		nodeInfo, err := clusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Warningf("Retrieving node info for node %q failed: %v", nodeName, err)
			continue
		}

		// Omit balloon pod for correct request calculations.
		if nodeInfo, err = removeBalloonPod(clusterSnapshot, nodeInfo); err != nil {
			clusterSnapshot.Revert()
			return unschedulablePods, err
		}

		requestedResources[nodeName] = getRequestedResources(nodeInfo)
		laResources[nodeName] = allLookaheadPodsRequests(nodeInfo)
		requestedResourcesWithoutLAPods := size.Subtract(requestedResources[nodeName], laResources[nodeName])

		// updating the total upsizable size
		nodeUpsizeable := size.Subtract(maxSizes[nodeName], requestedResourcesWithoutLAPods)
		totalUpsizablePerFamily[resizableNode.MachineFamily] = size.Add(totalUpsizablePerFamily[resizableNode.MachineFamily], nodeUpsizeable)

		maxResources := size.Max(desiredSizes[nodeName], requestedResourcesWithoutLAPods)
		newDesiredSize, err := p.sizeCalculator.RoundUp(nodeInfo.Node(), maxResources)
		if err != nil {
			klog.Warningf("Rounding up new desired size for node %q failed: %v", nodeName, err)
			continue
		}
		if !newDesiredSize.IsUpsizeFrom(desiredSizes[nodeName]) {
			continue
		}
		err = p.resizableVmManager.Upsize(nodeInfo.Node(), newDesiredSize)
		if err != nil {
			// Log only. Pods will be handled during next scale-up.
			// Backoff mechanism will take care of failing upsizes.
			klog.Warningf("[%s resize] Upsize of node %q failed: %v", resizableNode.MachineFamily, nodeName, err)
			continue
		}
		desiredSizes[nodeName] = newDesiredSize
	}

	for family, val := range totalUpsizablePerFamily {
		p.metrics.UpdateResizableVmTotalNodesLookaheadSpace(family, val)
	}

	// Lookahead pods space is not upsized, so it is not considered as part of desiredSize. We need to adjust BP pods to not exceed total node capacity.
	// We want to make sure that LA pod occupies the upsizable space first, keeping existing headroom on the Node.
	desiredSizesWithLAPods := map[string]size.Allocatable{}
	for nodeName := range desiredSizes {
		desiredSizesWithLAPods[nodeName] = size.Min(size.Add(desiredSizes[nodeName], laResources[nodeName]), maxSizes[nodeName])
	}
	err = AdjustBalloonPodsSize(ctx.ClusterSnapshot, desiredSizesWithLAPods, nil)
	if err != nil {
		clusterSnapshot.Revert()
		return unschedulablePods, err
	}

	klog.V(4).Infof("Filtering out %d pods moved to resized VMs.", len(schedulableStatuses))
	if emitMetrics {
		schedulableForMetrics := filterOutLookaheadPods(filterDaemonSetStatuses(schedulableStatuses))
		schedulableForMetricsPerFamily := map[string]int{}
		for _, status := range schedulableForMetrics {
			nodeName := status.NodeName
			resizableNode, ok := resizableNodesSnapshot[nodeName]
			if !ok {
				klog.Warningf("Node %q not found in snapshot for scheduled pod %q", nodeName, status.Pod.Name)
				continue
			}
			schedulableForMetricsPerFamily[resizableNode.MachineFamily]++
		}

		for family, count := range schedulableForMetricsPerFamily {
			p.metrics.RegisterResizableVmPodsSchedulableOnUpsizes(family, count)
		}
	}
	err = clusterSnapshot.Commit()

	unschedulablePods = []*v1.Pod{}
	filteredUnschedulableStatuses := filterDaemonSetStatuses(unschedulableStatuses)
	for _, status := range filteredUnschedulableStatuses {
		unschedulablePods = append(unschedulablePods, status.Pod)
	}

	if len(podsForcingScaleUp) > 0 {
		// Inject filtered pods back so they will trigger scale up
		unschedulablePods = append(unschedulablePods, podsForcingScaleUp...)
	}
	return unschedulablePods, err
}

// ScheduleLookaheadPods schedules lookahead pods considering UAS signal, without doing any upsizing for them.
func (p *ScaleUpNodeProcessor) ScheduleLookaheadPods(ctx *context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	defer metrics.UpdateDurationFromStart(scheduleLookaheadPods, time.Now())

	var laPods []*v1.Pod
	var remainingPods []*v1.Pod
	for _, pod := range unschedulablePods {
		if lookaheadbuffer.IsLookaheadPod(pod) {
			laPods = append(laPods, pod)
		} else {
			remainingPods = append(remainingPods, pod)
		}
	}

	var unscheduledLAPods []*v1.Pod
	var err error
	resizableFamilies := p.mcp.AllResizableMachineFamilies()
	if len(laPods) != 0 {
		unscheduledLAPods, err = p.process(ctx, laPods, false, resizableFamilies)
		if err != nil {
			return unschedulablePods, fmt.Errorf("failed to schedule LA pods: %v", err)
		}
	}

	p.updateUnschedulableLAPodsMetrics(unscheduledLAPods, resizableFamilies)
	return append(remainingPods, unscheduledLAPods...), nil
}

func (p *ScaleUpNodeProcessor) updateUnschedulableLAPodsMetrics(unscheduledLAPods []*v1.Pod, resizableFamilies []machinetypes.MachineFamily) {
	unscheduledLAPodsPerFamily := make(map[string]int)
	for _, family := range resizableFamilies {
		if p.resizableVmManager.IsResizingEnabled(family.Name()) {
			unscheduledLAPodsPerFamily[family.Name()] = 0
		}
	}
	unscheduledLAPodsPerFamily["unknown"] = 0

	for _, pod := range unscheduledLAPods {
		family := pod.Spec.NodeSelector[gkelabels.MachineFamilyLabel]
		if _, ok := unscheduledLAPodsPerFamily[family]; ok {
			unscheduledLAPodsPerFamily[family]++
		} else {
			unscheduledLAPodsPerFamily["unknown"]++
		}
	}

	for family, count := range unscheduledLAPodsPerFamily {
		p.metrics.UpdateResizableVmUnschedulableLookaheadPodsCount(family, count)
	}
}

// Preprocess updates clustersnapshot. It sets the balloon pod size based on the resizable VM Nodes allocatable and desired sizes.
func (p *ScaleUpNodeProcessor) Preprocess(ctx *context.AutoscalingContext) error {
	resizableFamilies := p.mcp.AllResizableMachineFamilies()
	if !isAnyResizingEnabled(p.resizableVmManager, resizableFamilies) {
		return nil
	}

	resizableSnapshot := p.resizableVmManager.FilteredNodesSnapshot(true, operationtracker.AllNodes)
	sizeMap := make(map[string]size.Allocatable, len(resizableSnapshot))
	for nodeName, resizableNode := range resizableSnapshot {
		sizeMap[nodeName] = resizableNode.DesiredSize
	}

	if len(sizeMap) == 0 {
		return nil
	}

	ctx.ClusterSnapshot.Fork()
	if err := AdjustBalloonPodsSize(ctx.ClusterSnapshot, sizeMap, p.sizeCalculator); err != nil {
		ctx.ClusterSnapshot.Revert()
		return err
	}
	if err := p.injectDefaultBalloonPods(ctx, sizeMap); err != nil {
		ctx.ClusterSnapshot.Revert()
		return err
	}

	for nodeName := range resizableSnapshot {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Warningf("Retrieving node info for node %q failed: %v", nodeName, err)
			continue
		}
		updatedNode, updated, err := taints.RemoveTaint(nodeInfo.Node(), ekvmtypes.BPResizeTaint)
		if err != nil {
			// Should never happen.
			ctx.ClusterSnapshot.Revert()
			return fmt.Errorf("taint removal failed: %v", err)
		}
		if updated {
			nodeInfo.SetNode(updatedNode)
		}
	}
	return ctx.ClusterSnapshot.Commit()
}

func (p *ScaleUpNodeProcessor) injectDefaultBalloonPods(ctx *context.AutoscalingContext, sizeMap map[string]size.Allocatable) error {
	nodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return fmt.Errorf("failed to inject default balloon pods: %v", err)
	}
	for _, nodeInfo := range nodeInfos {
		if !isResizableNode(nodeInfo.Node(), p.mcp) {
			continue
		}
		if _, ok := sizeMap[nodeInfo.Node().Name]; ok {
			// These nodes already have a balloon pod.
			continue
		}
		// This will prevent the insertion of a default balloon pod for the nodes under deletion.
		if ca_taints.HasTaint(nodeInfo.Node(), ca_taints.ToBeDeletedTaint) {
			continue
		}
		if err := operationtracker.InjectDefaultBalloonPod(nodeInfo, p.sizeCalculator); err != nil {
			// Injecting default balloon pods is done on a best-effort basis. Failure is tolerable.
			continue
		}
	}
	return nil
}

// groupPodsStatusesByCCC categorizes pod statuses based on their CCC, separating lookahead pods.
func (p *ScaleUpNodeProcessor) groupPodsStatusesByCCC(unschedulablePodStatuses []scheduling.Status, resizableSnapshotsPerCCCPerRule resizableNodesSnapshotsByCCC) (
	podStatusesPerCCC map[string][]scheduling.Status,
	laPodStatusesPerCCC map[string][]scheduling.Status,
	cccCrdPerCCC map[string]crd.CRD,
	skippedPodStatuses []scheduling.Status,
) {
	podStatusesPerCCC = make(map[string][]scheduling.Status)
	laPodStatusesPerCCC = make(map[string][]scheduling.Status)
	cccCrdPerCCC = make(map[string]crd.CRD)

	for _, status := range unschedulablePodStatuses {
		pod := status.Pod
		cccCrd, cccName, err := p.cccCrdLister.PodCrd(pod)
		if err != nil {
			klog.Warningf("Retrieving crd for pod %q failed: %v", pod.Name, err)
			skippedPodStatuses = append(skippedPodStatuses, status)
			continue
		}

		// If no nodes exist for this CCC, the pods remain unschedulable
		if _, found := resizableSnapshotsPerCCCPerRule[cccName]; !found {
			skippedPodStatuses = append(skippedPodStatuses, status)
			continue
		}
		if cccCrd != nil {
			cccCrdPerCCC[cccName] = cccCrd
		}
		if lookaheadbuffer.IsLookaheadPod(pod) {
			laPodStatusesPerCCC[cccName] = append(laPodStatusesPerCCC[cccName], status)
		} else {
			podStatusesPerCCC[cccName] = append(podStatusesPerCCC[cccName], status)
		}
	}
	return podStatusesPerCCC, laPodStatusesPerCCC, cccCrdPerCCC, skippedPodStatuses
}

// schedulePodsInBatches attempts to schedule pods grouped by CCC, respecting priorities.
func (p *ScaleUpNodeProcessor) schedulePodsInBatches(
	snapshot clustersnapshot.ClusterSnapshot,
	statusesPerCCC map[string][]scheduling.Status,
	isLA bool,
	resizableSnapshotsPerCCCPerRule resizableNodesSnapshotsByCCC,
	cccCrdPerCCC map[string]crd.CRD,
) ([]scheduling.Status, []scheduling.Status, error) {
	var batchSchedulable []scheduling.Status
	var batchUnschedulable []scheduling.Status

	// Sort CCC names to ensure deterministic scheduling order across runs
	cccNames := make([]string, 0, len(statusesPerCCC))
	for name := range statusesPerCCC {
		cccNames = append(cccNames, name)
	}
	slices.Sort(cccNames)

	for _, cccName := range cccNames {
		statuses := statusesPerCCC[cccName]
		var sched, unsched []scheduling.Status
		var err error

		if cccName == "" {
			// Default non-CCC behavior (Rule 0)
			sched, unsched, err = p.trySchedulePods(snapshot, resizableSnapshotsPerCCCPerRule[cccName][0], statuses, isLA)
		} else {
			// Respect CCC priority rules
			sched, unsched, err = p.tryScheduleCCCPods(snapshot, resizableSnapshotsPerCCCPerRule[cccName], statuses, cccCrdPerCCC[cccName], isLA)
		}

		if err != nil {
			return nil, nil, err
		}
		batchSchedulable = append(batchSchedulable, sched...)
		batchUnschedulable = append(batchUnschedulable, unsched...)
	}
	return batchSchedulable, batchUnschedulable, nil
}

func (p *ScaleUpNodeProcessor) schedulePods(snapshot clustersnapshot.ClusterSnapshot, resizableSnapshot operationtracker.ResizableNodesSnapshot, unschedulablePods []*v1.Pod) ([]scheduling.Status, []scheduling.Status, error) {
	klog.V(4).Infof("ScaleUpNodeProcessor: Attempting to schedule pods on nodes")
	resizableSnapshotsPerCCCPerRule := p.organizeByCCCByRule(resizableSnapshot)

	var unschedulableStatuses []scheduling.Status
	for _, pod := range unschedulablePods {
		unschedulableStatuses = append(unschedulableStatuses, scheduling.Status{Pod: pod, NodeName: ""})
	}

	// Pods without ccc will have empty ccc -> ""
	podStatusesPerCCC, laPodStatusesPerCCC, cccCrdPerCCC, skippedPodStatuses := p.groupPodsStatusesByCCC(unschedulableStatuses, resizableSnapshotsPerCCCPerRule)

	var allSchedulable []scheduling.Status
	var allUnschedulable []scheduling.Status
	allUnschedulable = append(allUnschedulable, skippedPodStatuses...)

	// Schedule non-Lookahead pods
	sched, unsched, err := p.schedulePodsInBatches(snapshot, podStatusesPerCCC, false, resizableSnapshotsPerCCCPerRule, cccCrdPerCCC)
	if err != nil {
		return nil, nil, err
	}
	allSchedulable = append(allSchedulable, sched...)
	allUnschedulable = append(allUnschedulable, unsched...)

	// Schedule Lookahead pods
	// Note: Pods that failed to schedule in the previous step are not retried here.
	// We only process the initial laPodStatusesPerCCC.
	laSched, laUnsched, err := p.schedulePodsInBatches(snapshot, laPodStatusesPerCCC, true, resizableSnapshotsPerCCCPerRule, cccCrdPerCCC)
	if err != nil {
		return nil, nil, err
	}
	allSchedulable = append(allSchedulable, laSched...)
	allUnschedulable = append(allUnschedulable, laUnsched...)

	return allSchedulable, allUnschedulable, nil
}

// organizeByCCCByRule organized resizable nodes into a nested map for each CCC and CCC rule,
// as a resizable node can only belong to at most one CCC and one rule in that CCC
// (terms rule and priority are used interchangeably).
// E.g. ResizableNodesSnapshot consists of the nodes:
// non_ccc_node, ccc1_rule0_node, ccc1_rule2_node1, ccc1_rule2_node2, ccc2_scale_up_anyway_node
// (assuming ccc2 has 1 rule overall and ScaleUpAnyway option enabled),
// so the output structure would be the following:
//
//	ccc1: {
//	    0: {ccc1_rule0_node: {}},
//	    2: {ccc1_rule2_node1: {}, ccc1_rule2_node2: {}},
//	},
//
//	ccc2: {
//	    1: {ccc2_scale_up_anyway_node: {}},
//	},
//
//	"": {
//	    0: {non_ccc_node: {}},
//	}
func (p *ScaleUpNodeProcessor) organizeByCCCByRule(resizableSnapshot operationtracker.ResizableNodesSnapshot) resizableNodesSnapshotsByCCC {
	split := make(resizableNodesSnapshotsByCCC)
	for nodeName, resizableNode := range resizableSnapshot {
		// nodes withouth ccc are treated as nodes having an empty "" ccc
		cccCrd, cccName, err := p.cccCrdLister.NodeCrd(resizableNode.Node)
		// NodeCrd method returns error in case of a node
		// having a crd that couldn't be fetched,
		// so we cannot determine which priority the node
		// belongs to
		if err != nil {
			klog.Warningf("Retrieving crd for node %q failed: %v", resizableNode.Node.Name, err)
			continue
		}

		// All non-CCC nodes have the same resizing priority,
		// which is the same as if all non-CCC nodes belonged to a
		// CCC with a single priority
		priorityIndex := 0
		if cccCrd != nil {
			priorityIndex, err = p.calculateNodeRuleIndex(resizableNode.Node, cccCrd)
			if err != nil {
				klog.Warningf("Retrieving priority index for node %q failed: %v", resizableNode.Node.Name, err)
				continue
			}
		}

		if _, found := split[cccName]; !found {
			split[cccName] = make(resizableNodesSnapshotsByRule)
		}
		if _, found := split[cccName][priorityIndex]; !found {
			split[cccName][priorityIndex] = operationtracker.ResizableNodesSnapshot{}
		}
		split[cccName][priorityIndex][nodeName] = resizableNode
	}
	return split
}

func (p *ScaleUpNodeProcessor) calculateNodeRuleIndex(node *v1.Node, cccCrd crd.CRD) (int, error) {
	mig, err := p.cloudProvider.GkeMigForNode(node)
	if err != nil {
		return 0, err
	}

	found, ruleIdx, _ := p.matcher.FirstMatchedRule(mig, cccCrd)
	if !found {
		if cccCrd.ScaleUpAnyway() {
			return len(cccCrd.Rules()), nil
		}
		return 0, fmt.Errorf("node %q doesn't match any rule in CCC %q", node.Name, cccCrd.Name())
	}

	return ruleIdx, nil
}

func removeScheduledLookaheadPods(resizableSnapshot operationtracker.ResizableNodesSnapshot, nodeInfos []*framework.NodeInfo) []*v1.Pod {
	var laPods []*v1.Pod
	for _, nodeInfo := range nodeInfos {
		// We skip nodes that are not part of the resizable snapshot as they are likely upcoming nodes
		// and won't be able to be re-schedule lookahead pods if lookahead pods are removed from the snapshot.
		if _, inSnapshot := resizableSnapshot[nodeInfo.Node().Name]; !inSnapshot {
			continue
		}
		pods := nodeInfo.Pods()
		for _, podInfo := range pods {
			pod := podInfo.Pod
			if lookaheadbuffer.IsLookaheadPod(pod) {
				laPods = append(laPods, pod)
				nodeInfo.RemovePod(klog.Background(), pod)
			}
		}
	}
	return laPods
}

func (p *ScaleUpNodeProcessor) tryScheduleCCCPods(snapshot clustersnapshot.ClusterSnapshot, resizableSnapshots resizableNodesSnapshotsByRule, podStatuses []scheduling.Status, ccc crd.CRD, isLookaheadPods bool) ([]scheduling.Status, []scheduling.Status, error) {
	if len(podStatuses) == 0 {
		return nil, nil, nil
	}

	var schedulable []scheduling.Status
	unsched := podStatuses
	var sched []scheduling.Status
	var err error

	for index := range ccc.Rules() {
		if resizableSnapshot, found := resizableSnapshots[index]; found {
			sched, unsched, err = p.trySchedulePods(snapshot, resizableSnapshot, unsched, isLookaheadPods)
			if err != nil {
				return nil, nil, err
			}
			schedulable = append(schedulable, sched...)

			if len(unsched) == 0 {
				return schedulable, unsched, nil
			}
		}

		if !p.cccRuleBackoff.RuleBackoffStatus(ccc, index, time.Now()).IsBackedOff {
			return schedulable, unsched, nil
		}
	}

	// ScaleUpAnyway option nodes
	if resizableSnapshot, found := resizableSnapshots[len(ccc.Rules())]; ccc.ScaleUpAnyway() && found {
		sched, unsched, err = p.trySchedulePods(snapshot, resizableSnapshot, unsched, false)
		if err != nil {
			return nil, nil, err
		}
		schedulable = append(schedulable, sched...)
		return schedulable, unsched, nil
	}

	return schedulable, unsched, nil
}

func (p *ScaleUpNodeProcessor) trySchedulePods(snapshot clustersnapshot.ClusterSnapshot, resizableSnapshot operationtracker.ResizableNodesSnapshot, statuses []scheduling.Status, isLookaheadPods bool) ([]scheduling.Status, []scheduling.Status, error) {
	if len(statuses) == 0 {
		return nil, nil, nil
	}

	var filters []func(*framework.NodeInfo) bool

	if isLookaheadPods {
		filters = []func(*framework.NodeInfo) bool{
			p.createNodeFilterForPodNodeState(resizableSnapshot, resizableNodes),
			p.createNodeFilterForPodNodeState(resizableSnapshot, nonResizableNodes),
			p.createUpcomingNodeFilter(),
		}
	} else {
		filters = []func(*framework.NodeInfo) bool{
			p.createNodeFilterForPodNodeState(resizableSnapshot, idleNodes),
			p.createNodeFilterForPodNodeState(resizableSnapshot, processingNodes),
		}
	}

	var schedulable []scheduling.Status
	unschedulableStatuses := statuses
	var unschedPerFilter []scheduling.Status
	var err error

	for _, filter := range filters {
		var schedPerFilter []scheduling.Status
		schedPerFilter, unschedPerFilter, err = p.trySchedulePodsOnSpecifiedNodes(snapshot, unschedulableStatuses, filter)
		schedulable = append(schedulable, schedPerFilter...)

		if err != nil || len(unschedPerFilter) == 0 {
			return schedulable, unschedPerFilter, err
		}

		unschedulableStatuses = unschedPerFilter
	}
	return schedulable, unschedPerFilter, nil
}

func (p *ScaleUpNodeProcessor) createNodeFilterForPodNodeState(resizableSnapshot operationtracker.ResizableNodesSnapshot, nodesState NodesState) func(*framework.NodeInfo) bool {
	return func(nodeInfo *framework.NodeInfo) bool {
		nodeName := nodeInfo.Node().Name
		resizableNode, inSnapshot := resizableSnapshot[nodeName]
		if !inSnapshot {
			// Skip nodes not in snapshot
			return false
		}

		isNodeInProcess := p.resizableVmManager.IsNodeInProcess(nodeName)
		isResizableNode := resizableNode.IsResizable() && resizableNode.IsSafelyUpsizable()
		switch nodesState {
		case allNodes:
			return true
		case idleNodes:
			return !isNodeInProcess
		case processingNodes:
			return isNodeInProcess
		case resizableNodes:
			return isResizableNode
		case nonResizableNodes:
			return !isResizableNode
		default:
			klog.Errorf("Unknown nodesState while scheduling pods, using default condition for acceptable nodes (all resizable nodes)")
			return true
		}
	}
}

func (p *ScaleUpNodeProcessor) createUpcomingNodeFilter() func(*framework.NodeInfo) bool {
	return func(nodeInfo *framework.NodeInfo) bool {
		if !isResizableNode(nodeInfo.Node(), p.mcp) {
			return false
		}
		return utils.IsNodeInfoUpcoming(nodeInfo)
	}
}

func (p *ScaleUpNodeProcessor) trySchedulePodsOnSpecifiedNodes(snapshot clustersnapshot.ClusterSnapshot, statuses []scheduling.Status, isNodeAcceptable func(nodeInfo *framework.NodeInfo) bool) ([]scheduling.Status, []scheduling.Status, error) {
	var simulatorPods []*v1.Pod
	for _, status := range statuses {
		simulatorPods = append(simulatorPods, status.Pod)
	}

	schedulable, _, err := p.simulator.TrySchedulePods(snapshot, simulatorPods, false, clustersnapshot.SchedulingOptions{
		IsNodeAcceptable: isNodeAcceptable,
	})
	if err != nil {
		return nil, nil, err
	}

	podsWithStatus := make(map[types.UID]bool)
	for _, status := range schedulable {
		podsWithStatus[status.Pod.UID] = true
	}

	var unschedulable []scheduling.Status
	for _, status := range statuses {
		pod := status.Pod
		if !podsWithStatus[pod.UID] {
			unschedulable = append(unschedulable, status)
		}
	}
	return schedulable, unschedulable, nil
}

// isForceScaleUpFeatureEnabled returns true if force scale up feature is enabled.
func (p *ScaleUpNodeProcessor) isForceScaleUpFeatureEnabled() bool {
	return p.customThresholdsProvider != nil && p.customThresholdsProvider.IsForceScaleUpFeatureEnabled()
}

// TrackUnschedulablePods tracks pods that were unsuccessfully tried to be scheduled on the resizable VM upsizes.
//
// - unschedulablePodsBeforeFiltering - pods that are unschedulable before filtering processors.
//
// - unschedulableBeforeScaleUpNodeProcessor - pods that are unschedulable before ScaleUpNodeProcessor.
//
// - unschedulableAfterScaleUpNodeProcessor - pods that are unschedulable after ScaleUpNodeProcessor.
func (p *ScaleUpNodeProcessor) TrackUnschedulablePods(unschedulablePodsBeforeFiltering, unschedulableBeforeScaleUpNodeProcessor, unschedulableAfterScaleUpNodeProcessor []*v1.Pod) {
	if !p.isForceScaleUpFeatureEnabled() {
		return
	}
	// Step 1 : clean up the pods map. If a pod is not found in unschedulableInTheBeginning it means that it was successfully scheduled.
	removeScheduledPodsFromMap(p.attemptsToScheduleOnUpsize, unschedulablePodsBeforeFiltering)
	// Step 2 : determine which pods will be "tried-to-be-scheduled" on the resizable VM upsizes. These pods are the diff between two pods lists, unschedulable before ScaleUpNodeProcessor and after.
	podsAdmittedForUpsizes := diffBetweenPodLists(unschedulableAfterScaleUpNodeProcessor, unschedulableBeforeScaleUpNodeProcessor)
	// Step 3 : add podsAdmittedForUpsizes to the map or increment their counter
	addPodsToMap(p.attemptsToScheduleOnUpsize, podsAdmittedForUpsizes)
}

// CleanUp is called at CA termination
func (p *ScaleUpNodeProcessor) CleanUp() {
}

func generateMissingDaemonSetPods(ctx *context.AutoscalingContext, resizableSnapshot operationtracker.ResizableNodesSnapshot) ([]*v1.Pod, error) {
	logger := klog.FromContext(gocontext.Background())
	daemonSets, err := ctx.ListerRegistry.DaemonSetLister().List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("listing DaemonSets error: %v", err)
	}

	missingDsPods := []*v1.Pod{}
	for nodeName := range resizableSnapshot {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Warningf("Retrieving node info for node %q failed: %v", nodeName, err)
			continue
		}

		if nodeInfo.Node() == nil {
			klog.Warningf("Nil Node object for node info %q", nodeName)
			continue
		}

		runningDS := make(map[types.UID]bool)
		for _, podInfo := range nodeInfo.Pods() {
			controllerRef := metav1.GetControllerOf(podInfo.Pod)
			if controllerRef != nil && controllerRef.Kind == "DaemonSet" {
				runningDS[controllerRef.UID] = true
			}
		}

		for _, ds := range daemonSets {
			if runningDS[ds.UID] {
				continue
			}
			if shouldRun, _ := daemon.NodeShouldRunDaemonPod(logger, nodeInfo.Node(), ds); !shouldRun {
				continue
			}
			dsPod := daemon.NewPod(ds, nodeInfo.Node().Name)
			dsPod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(ds, appsv1.SchemeGroupVersion.WithKind("DaemonSet"))}
			missingDsPods = append(missingDsPods, dsPod)
		}
	}

	return missingDsPods, nil
}

func filterDaemonSetStatuses(statuses []scheduling.Status) []scheduling.Status {
	filteredStatuses := []scheduling.Status{}
	for _, status := range statuses {
		if controllerRef := metav1.GetControllerOf(status.Pod); controllerRef != nil && controllerRef.Kind == "DaemonSet" {
			continue
		}
		filteredStatuses = append(filteredStatuses, status)
	}
	return filteredStatuses
}

func filterOutLookaheadPods(statuses []scheduling.Status) []scheduling.Status {
	filteredStatuses := []scheduling.Status{}
	for _, status := range statuses {
		if lookaheadbuffer.IsLookaheadPod(status.Pod) {
			continue
		}
		filteredStatuses = append(filteredStatuses, status)
	}
	return filteredStatuses
}

func podDetails(pods []*v1.Pod) []string {
	details := make([]string, 0, len(pods))
	for _, pod := range pods {
		requests := lookaheadbuffer.CpuMemRequests(pod)
		cpu := requests[v1.ResourceCPU]
		mem := requests[v1.ResourceMemory]
		workloadInfo := lookaheadbuffer.LookaheadWorkloadSeparationInfo(pod)
		details = append(details, fmt.Sprintf("{name: %s, cpu: %s, memory: %s, workload separation info: %s}", pod.Name, cpu.String(), mem.String(), strings.Join(workloadInfo, ",")))
	}
	return details
}

// hasResizingdPod returns true if there is in-place resizing pod on the node.
func hasResizingdPod(nodeInfo *framework.NodeInfo) bool {
	for _, podInfo := range nodeInfo.Pods() {
		if podInfo.Pod != nil && resource.IsPodResizeDeferred(podInfo.Pod) {
			return true
		}
	}
	return false
}

// diffBetweenPodLists return the difference between two lists (pods that are in secondList and not in firstList)
// Note: listBefore is a superset of the listAfter
func diffBetweenPodLists(listAfter, listBefore []*v1.Pod) []*v1.Pod {
	diffList := []*v1.Pod{}
	setAfter := make(map[podId]struct{}, len(listAfter))
	for _, pod := range listAfter {
		setAfter[pod.UID] = struct{}{}
	}
	for _, pod := range listBefore {
		if _, found := setAfter[pod.UID]; !found {
			diffList = append(diffList, pod)
		}
	}
	return diffList
}

// removeScheduledPodsFromMap removes scheduled pods from the map (i.e. ones that are NOT present in unscheduledPods)
func removeScheduledPodsFromMap(mapToClean map[podId]int, unscheduledPods []*v1.Pod) {
	if len(mapToClean) == 0 {
		return
	}
	setOfUnscheduledPods := make(map[podId]struct{}, len(unscheduledPods))
	for _, pod := range unscheduledPods {
		setOfUnscheduledPods[pod.UID] = struct{}{}
	}
	for key := range mapToClean {
		if _, found := setOfUnscheduledPods[key]; !found {
			delete(mapToClean, key)
		}
	}
}

// addPodsToMap increments the counters of pods that were admitted for upsizes (except for virtual/fake pods)
func addPodsToMap(mapToPopulate map[podId]int, podsAdmittedForUpsizes []*v1.Pod) {
	for _, pod := range podsAdmittedForUpsizes {
		if !fake.IsFake(pod) {
			mapToPopulate[pod.UID]++
		}
	}
}

// filterPodsForcingScaleUp splits the list of unschedulable pods into two:
// - ones that faced a lot of failed upsizes (the ones with counter >= thresholds)
// - all others
func filterPodsForcingScaleUp(unschedulablePods []*v1.Pod, attemptsToScheduleOnUpsize map[podId]int, threshold int) ([]*v1.Pod, []*v1.Pod) {
	unschedulablePodsAfterFiltering := []*v1.Pod{}
	podsForcingScaleUp := []*v1.Pod{}
	for _, pod := range unschedulablePods {
		if attemptsToScheduleOnUpsize[pod.UID] <= threshold {
			unschedulablePodsAfterFiltering = append(unschedulablePodsAfterFiltering, pod)
			continue
		}
		podsForcingScaleUp = append(podsForcingScaleUp, pod)
	}
	if len(podsForcingScaleUp) > 0 {
		klog.V(4).Infof("Pods %v faced more than %d failed upsizes; they will force scale up", strings.Join(getPodNames(podsForcingScaleUp), ", "), threshold)
	}
	return unschedulablePodsAfterFiltering, podsForcingScaleUp
}

func getPodNames(pods []*v1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}
