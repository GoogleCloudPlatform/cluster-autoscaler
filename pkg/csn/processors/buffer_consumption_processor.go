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
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/annotator"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/metrics"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/klogx"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

const (
	bufferConsumptionMetricLabel          = "scaleUp:CSNbufferConsumptionProcessor"
	schedulingThroughInformersMetricLabel = "scaleUp:CSNschedulingThroughInformers"
	bufferConsumptionLogPrefix            = "CSN Buffer Consumption processor:"
	defaultPodAgeFallbackThreshold        = 10 * time.Minute
)

type csnMetrics interface {
	SetCSNInvalidCondition(condition internalmetrics.CSNInvalidCondition)
	UpdateCSNConsideredPods(counts map[internalmetrics.CSNConsideredPodsKey]int)
}

// BufferConsumptionProcessor is a processor that consumes CSN nodes to schedule pods (and filter out scheduled pods). go/csn-in-ca
type BufferConsumptionProcessor struct {
	nodeController     csnNodeController
	simulator          *scheduling.HintingSimulator
	experimentsManager experiments.Manager
	metrics            csnMetrics
	clock              clock.PassiveClock
}

func NewBufferConsumptionProcessor(nodeController csnNodeController, experimentsManager experiments.Manager) *BufferConsumptionProcessor {
	return &BufferConsumptionProcessor{
		nodeController:     nodeController,
		simulator:          scheduling.NewHintingSimulator(),
		experimentsManager: experimentsManager,
		metrics:            internalmetrics.Metrics,
		clock:              clock.RealClock{},
	}
}

func (p *BufferConsumptionProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	defer metrics.UpdateDurationFromStart(ctx, bufferConsumptionMetricLabel, p.clock.Now())

	snapshot := autoscalingCtx.ClusterSnapshot
	snapshot.Fork()

	// We shoudn't attempt to schedule unhelpable pods to improve performance as otherwise they will keep wasting a lot of processing slowing down CA loop in every loop,
	// but they must remain in the final returned list in their original relative order.
	// This should be fine bec if it is unhelpable it means suspended nodes won't help it bec:
	// - If the CSN node is suspended, it means the pod became unhelpable after attempting to schedule it on the suspended nodes.
	// - If the CSN node is chilling, there is a chance that it might help the pod if the node is created after the pod is marked unhelpable.
	//   But if this is true, then NAP is likely able to create a nodepool for it before it is unhelpable.
	//   In the rare case this isn't true, scheduler will schedule it on the CSN node anyway blocking suspension.
	helpablePods := slices.DeleteFunc(slices.Clone(unschedulablePods), annotator.IsUnhelpablePod)

	if unhelpablePodsNum := len(unschedulablePods) - len(helpablePods); unhelpablePodsNum > 0 {
		klog.Infof("%s filtered out %d unhelpablePods", bufferConsumptionLogPrefix, unhelpablePodsNum)
	}

	now := p.clock.Now()
	threshold := p.podAgeFallbackThreshold()
	podAgeFallbackEnabled := p.isPodAgeFallbackEnabled()
	nodesOfScheduledPods, err := p.consumeCSNBuffers(autoscalingCtx, helpablePods, now, threshold, podAgeFallbackEnabled)
	if err != nil {
		snapshot.Revert()
		klog.Errorf("%s error while consuming CSN buffers: %v", bufferConsumptionLogPrefix, err)
		p.metrics.SetCSNInvalidCondition(internalmetrics.CSNBufferConsumptionProcessorError)
		return unschedulablePods, nil // Snapshot is reverted, so we should return the original unschedulable pods.
	}

	// We reuse now, threshold and podAgeFallbackEnabled values here to make them consistent with consumeCSNBuffers().
	// So even if giraffe flag changes, or a new pod becomes old, they will have consistent results.
	// If they become out of sync, it won't break anything though. It is just nicer to have the metric correspond to the what is being considered at a given time.
	if podAgeFallbackEnabled {
		p.emitConsideredPodsMetrics(unschedulablePods, nodesOfScheduledPods, now, threshold)
	}

	// Remove scheduled pods.
	unschedulablePods = slices.DeleteFunc(unschedulablePods, func(pod *apiv1.Pod) bool {
		return nodesOfScheduledPods[pod] != ""
	})

	if err := snapshot.Commit(); err != nil {
		klog.Errorf("%s error while commiting the snapshot: %v", bufferConsumptionLogPrefix, err)
		p.metrics.SetCSNInvalidCondition(internalmetrics.CommitSnapshotError)
		return nil, fmt.Errorf("error while commiting the snapshot: %v", err)
	}

	return unschedulablePods, nil
}

// CleanUp cleans up the processor.
func (p *BufferConsumptionProcessor) CleanUp() {}

// Note: consumeCSNBuffers assumes that it is already under a forked snapshot.
func (p *BufferConsumptionProcessor) consumeCSNBuffers(ctx *ca_context.AutoscalingContext, unschedulablePods []*apiv1.Pod, now time.Time, threshold time.Duration, podAgeFallbackEnabled bool) (nodesOfScheduledPods map[*apiv1.Pod]string, err error) {
	snapshot := ctx.ClusterSnapshot

	classifiedNodes, err := classifyNodes(snapshot)
	if err != nil {
		return nil, fmt.Errorf("error classifying nodes: %v", err)
	}
	if !classifiedNodes.hasNonConsumedNodes() {
		return nil, nil
	}

	alreadyConsumedNodes, err := p.consumedNodes(classifiedNodes, ctx.AutoscalingKubeClients)
	if err != nil {
		return nil, fmt.Errorf("error getting already consumed nodes: %v", err)
	}

	csnNodes, filteredCounts, err := p.nodeController.List(nodecontroller.WithoutPendingOperationsFilter, nodecontroller.WithoutBackedOffSuspendedFilter)
	if err != nil {
		return nil, fmt.Errorf("error listing CSN nodes: %v", err)
	}
	if len(filteredCounts) > 0 {
		klog.V(4).Infof("%s Filtered out CSN nodes during buffer consumption: %v", bufferConsumptionLogPrefix, filteredCounts)
	}

	nodeControllerNodes := map[string]bool{}
	for _, csnNode := range csnNodes {
		nodeControllerNodes[csnNode.Name] = true
	}

	nodeControllerFilter := func(ni *framework.NodeInfo) bool {
		return nodeControllerNodes[ni.Node().Name]
	}

	alreadyConsumedFilter := func(ni *framework.NodeInfo) bool {
		return alreadyConsumedNodes[ni.Node().Name]
	}

	var normalPods, oldPods []*apiv1.Pod
	if podAgeFallbackEnabled {
		for _, pod := range unschedulablePods {
			if isPodTooOld(pod, now, threshold) {
				oldPods = append(oldPods, pod)
			} else {
				normalPods = append(normalPods, pod)
			}
		}
		if len(oldPods) > 0 {
			klog.Infof("%s found %d pods that are older than %v, suspended nodes will not be considered for scheduling those pods", bufferConsumptionLogPrefix, len(oldPods), threshold)
		}
	} else {
		normalPods = unschedulablePods
	}

	commonSchedulingPriorities := []priorityFilter{
		alreadyConsumedFilter,
		// Upcoming nodes are not returned from the controller.List call.
		// Otherwise, the node is either already chilling or being chilling, both are okay.
		// Even if such a node is in backoff, it still doesn't block scheduling from Kubernetes Scheduler.
		isChillingFilter,
	}

	nodesOfScheduledPods, err = schedulePodGroupsOnCSNNodes(snapshot, p.simulator,
		schedulePodsOnCSNNodesOptions{ignoreBufferAssignment: true},
		podGroup{
			pods:       oldPods,
			priorities: commonSchedulingPriorities,
		},
		podGroup{
			pods: normalPods,
			priorities: append(commonSchedulingPriorities,
				// If the node isn't returned from the controller.List call, then it is likely either backed-off, deleted or being suspended.
				// In all those cases we want to skip scheduling on it.
				// Note that old pods can't be scheduled on suspended nodes (go/csn-backoff-strategy).
				allOfPriorityFilters(nodeControllerFilter, isSuspendedFilter),
			),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error scheduling pods: %v", err)
	}

	nodesToConsume := map[string]bool{}
	for _, nodeName := range nodesOfScheduledPods {
		nodesToConsume[nodeName] = true
	}
	for nodeName := range alreadyConsumedNodes {
		nodesToConsume[nodeName] = true
	}

	enqueuedNodes := p.nodeController.Consume(filterNodeControllerNodes(nodesToConsume, nodeControllerNodes))

	for nodeName := range nodesToConsume {
		// If the node is not enqueued but it is not part of nodeControllerNodes, we still wants to consume them bec they may not be registered yet (e.g. upcoming nodes).
		if nodeControllerNodes[nodeName] && !enqueuedNodes.Has(nodeName) {
			continue
		}
		ni, err := snapshot.GetNodeInfo(nodeName)
		if err != nil {
			return nil, fmt.Errorf("error getting node info for node %q: %v", nodeName, err) // This should never happen (as we just scheduled pod on that node in ClusterSnapshot earlier).
		}
		node := ni.Node()
		node, err = setNodeAsForProcessors(node, csn.NodeStateConsumed)
		if err != nil {
			return nil, fmt.Errorf("error marking node %q as consumed: %v", nodeName, err) // This should never happen (this is just in-memory change of that node).
		}
		ni.SetNode(node)
	}

	// Remove pods that were scheduled on nodes that weren't enqueued to the node controller.
	for pod, nodeName := range nodesOfScheduledPods {
		// If the node is not enqueued but it is not part of nodeControllerNodes, we don't want to remove the scheduled pods on it bec they may not be registered yet (e.g. upcoming nodes).
		// We don't undo pods that are scheduled on non-suspended nodes, because they are still available for scheduling by Kubernetes Scheduler regardless whether or not it is successfully enqueued in node controller.
		if nodeControllerNodes[nodeName] && !enqueuedNodes.Has(nodeName) && classifiedNodes.suspended[nodeName] != nil {
			if err := snapshot.RemovePodInfo(pod.Namespace, pod.Name, nodeName); err != nil {
				klog.Warningf("%s failed to remove pod %q from node %q: %v", bufferConsumptionLogPrefix, pod.Name, nodeName, err)
			}
			delete(nodesOfScheduledPods, pod)
		}
	}

	return nodesOfScheduledPods, nil
}

func (p *BufferConsumptionProcessor) emitConsideredPodsMetrics(unschedulablePods []*apiv1.Pod, nodesOfScheduledPods map[*apiv1.Pod]string, now time.Time, threshold time.Duration) {
	consideredPods := map[internalmetrics.CSNConsideredPodsKey]int{}
	for _, pod := range unschedulablePods {
		isOld := isPodTooOld(pod, now, threshold)
		var status internalmetrics.CSNPodStatus
		if annotator.IsUnhelpablePod(pod) {
			status = internalmetrics.CSNPodUnhelpable
		} else if nodesOfScheduledPods[pod] != "" {
			status = internalmetrics.CSNPodScheduled
		} else {
			status = internalmetrics.CSNPodUnschedulable
		}
		consideredPods[internalmetrics.CSNConsideredPodsKey{Status: status, IsOld: isOld}]++
	}
	p.metrics.UpdateCSNConsideredPods(consideredPods)
}

func (p *BufferConsumptionProcessor) isPodAgeFallbackEnabled() bool {
	if p.experimentsManager == nil {
		return false
	}
	return p.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ColdStandbyNodesBackoffMinCAVersionFlag, false)
}

func (p *BufferConsumptionProcessor) podAgeFallbackThreshold() time.Duration {
	if !p.isPodAgeFallbackEnabled() {
		return math.MaxInt64
	}
	if p.experimentsManager == nil {
		return defaultPodAgeFallbackThreshold
	}
	threshold := p.experimentsManager.EvaluateDurationSecondsFlagOrFailsafe(experiments.ColdStandbyNodesPodAgeFallbackThresholdSecondsFlag, defaultPodAgeFallbackThreshold)
	if threshold <= 0 {
		return defaultPodAgeFallbackThreshold
	}
	return threshold
}

func isPodTooOld(pod *apiv1.Pod, now time.Time, threshold time.Duration) bool {
	if pod.CreationTimestamp.IsZero() {
		return false
	}
	return now.Sub(pod.CreationTimestamp.Time) > threshold
}

func filterNodeControllerNodes(nodesToConsume map[string]bool, nodeControllerNodes map[string]bool) []string {
	var filteredNodes []string
	for nodeName := range nodesToConsume {
		if nodeControllerNodes[nodeName] {
			filteredNodes = append(filteredNodes, nodeName)
		}
	}
	return filteredNodes
}

type classifiedNodes struct {
	chilling  map[string]*framework.NodeInfo
	suspended map[string]*framework.NodeInfo
}

func (c *classifiedNodes) hasNonConsumedNodes() bool {
	return len(c.chilling) > 0 || len(c.suspended) > 0
}

func (c *classifiedNodes) csnNodes() map[string]*framework.NodeInfo {
	res := maps.Clone(c.chilling)
	maps.Copy(res, c.suspended)
	return res
}

func classifyNodes(snapshot clustersnapshot.ClusterSnapshot) (*classifiedNodes, error) {
	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		return nil, fmt.Errorf("failed to list node infos: %v", err)
	}

	cn := &classifiedNodes{
		chilling:  map[string]*framework.NodeInfo{},
		suspended: map[string]*framework.NodeInfo{},
	}
	for _, ni := range nodeInfos {
		node := ni.Node()

		switch csn.ClassifyNode(node) {
		case csn.NodeStateChilling:
			cn.chilling[node.Name] = ni
		case csn.NodeStateSuspended:
			cn.suspended[node.Name] = ni
		}
	}

	return cn, nil
}

// consumedNodes returns all nodes that are already consumed by checking if any of them has pods that block suspension.
func (p *BufferConsumptionProcessor) consumedNodes(nodeInfos *classifiedNodes, kubeClients ca_context.AutoscalingKubeClients) (map[string]bool, error) {
	alreadyConsumedNodes, err := p.consumedNodesThroughSnapshot(nodeInfos.chilling)
	if err != nil {
		return nil, fmt.Errorf("error getting consumed nodes through snapshot: %v", err)
	}
	cons, err := p.consumedNodesThroughInformers(nodeInfos, kubeClients)
	if err != nil {
		return nil, fmt.Errorf("error getting consumed nodes through informers: %v", err)
	}
	maps.Copy(alreadyConsumedNodes, cons)

	return alreadyConsumedNodes, nil
}

// consumedNodesThroughSnapshot returns nodes through clustersnapshot to consume if any of them has pods that block suspension.
// This can capture any decisions made by CA through the loop.
func (p *BufferConsumptionProcessor) consumedNodesThroughSnapshot(chillingNodeInfos map[string]*framework.NodeInfo) (map[string]bool, error) {
	nodesToConsume := map[string]bool{}
	blockingPods := map[string]string{} // From node name to pod identifier.

	for _, ni := range chillingNodeInfos {
		p := blockingPod(ni)
		if p == "" {
			continue
		}
		blockingPods[ni.Node().Name] = p
		nodesToConsume[ni.Node().Name] = true
	}
	logAlreadyConsumedNodes(blockingPods, "snapshot")
	return nodesToConsume, nil
}

// consumedNodesThroughInformers returns nodes that should be consumed based on the current state of pods in the cluster.
// This is used to mitigate race conditions between CA loop and scheduler (since scheduler can schedule pods on them).
func (p *BufferConsumptionProcessor) consumedNodesThroughInformers(nodeInfos *classifiedNodes, kubeClients ca_context.AutoscalingKubeClients) (map[string]bool, error) {
	defer metrics.UpdateDurationFromStart(context.TODO(), schedulingThroughInformersMetricLabel, p.clock.Now())

	loggingQuota := logging.CSNPodLoggingQuota()

	nodeInfosMap := nodeInfos.chilling
	if p.experimentsManager.DirectLaunchBoolFlag(experiments.ColdStandbyNodesCheckPodsOnSuspendedNodes) {
		nodeInfosMap = nodeInfos.csnNodes()
	}

	nodesToConsume := map[string]bool{}
	blockingPods := map[string]string{} // From node name to pod identifier.
	unexpectedSuspendedNodesWithPods := map[string]bool{}

	nodeLister := kubeClients.AllNodeLister()
	pods, err := kubeClients.AllPodLister().List()
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %v", err)
	}
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if nodeName == "" || nodeInfosMap[nodeName] == nil {
			continue
		}
		if !csn.IsPodBlockingSuspension(pod) {
			continue
		}
		if nodeInfos.suspended[nodeName] != nil && hasNodeStartedSuspension(nodeName, nodeLister) {
			unexpectedSuspendedNodesWithPods[nodeName] = true
		}
		blockingPods[nodeName] = podId(pod)
		nodesToConsume[nodeName] = true
	}

	for nodeName := range unexpectedSuspendedNodesWithPods {
		podId := blockingPods[nodeName]
		klogx.V(4).UpTo(loggingQuota).Infof("%s pod %q should block suspension but scheduled on suspended or being suspended node %q", bufferConsumptionLogPrefix, podId, nodeName)
	}
	klogx.V(4).Over(loggingQuota).Infof("%s there were also %v more suspended or being suspended nodes scheduled with pods that should block suspension", bufferConsumptionLogPrefix, -loggingQuota.Left())

	if len(unexpectedSuspendedNodesWithPods) > 0 {
		p.metrics.SetCSNInvalidCondition(internalmetrics.SuspendedNodeWithBlockingPods)
	}

	logAlreadyConsumedNodes(blockingPods, "informers")
	return nodesToConsume, nil
}

func hasNodeStartedSuspension(nodeName string, nodeLister kubernetes.NodeLister) bool {
	// We are getting a node from informer to ensure the node is actually suspended or started suspension (in contrast to being set to suspended during the CA loop simulation).
	// Even if the node is being consumed/deleted, it should still have the suspended taint and condition and cordoning and shouldn't schedule pods there.
	node, err := nodeLister.Get(nodeName)
	if err != nil {
		klog.Errorf("%s failed to get node %q: %v", bufferConsumptionLogPrefix, nodeName, err)
		return false
	}
	return csn.IsSuspendedNode(node)
}

func logAlreadyConsumedNodes(suspensionBlockingPods map[string]string, source string) {
	if len(suspensionBlockingPods) == 0 {
		return
	}
	for nodeName, podId := range suspensionBlockingPods {
		klog.V(4).Infof("%s node %q will be consumed bec it has pod %q that blocks suspension (verified through %s)", bufferConsumptionLogPrefix, nodeName, podId, source)
	}
}

func blockingPod(nodeInfo *framework.NodeInfo) string {
	for _, pod := range nodeInfo.Pods() {
		if csn.IsPodBlockingSuspension(pod.Pod) {
			return podId(pod.Pod)
		}
	}
	return ""
}

func podId(pod *apiv1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}
