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
	"fmt"
	"maps"
	"slices"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/processors/pods"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/autoscaler/cluster-autoscaler/utils/klogx"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/logging"
	cbmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/capacitybuffer"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/utils/ptr"
)

const (
	csnPodsLifecycleMetricLabel          = "scaleUp:CSNPodsLifecycleProcessor"
	csnPodsLifecycleLogPrefix            = "CSN Pods Lifecycle processor:"
	nodeRefreshFrequencyBufferAnnotation = "buffer.gke.io/standby-capacity-refresh-frequency"
	neverRefreshAnnotationValue          = "never"
)

var (
	// outdatedTaint is a taint that is added when the node refresh cycle has been passed and it should be recreated.
	// Used to avoid scheduling pods on them in this processor and in scale-down.
	// We don't have to apply this taint in k8s, in memory is enough. However it can be applied in k8s too (this is a terminal state).
	outdatedTaint = apiv1.Taint{
		Key:    "buffer.gke.io/standby-capacity-node-outdated",
		Value:  "true",
		Effect: apiv1.TaintEffectNoSchedule,
	}
)

// CSNPodsLifecycleProcessor is a processor that manages the lifecycle of CSN pods.
// It creates CSN pods, schedules them on CSN nodes, and marks the nodes as suspended.
// CSN pods that didn't get scheduled will be returned to trigger scale-up. go/csn-in-ca
type CSNPodsLifecycleProcessor struct {
	nodeController           csnNodeController
	csnPodInjectionProcessor pods.PodListProcessor
	simulator                *scheduling.HintingSimulator
	bufferRegistry           *fakepods.Registry
	defaultRefreshFrequency  time.Duration
	cbFakePodStateObserver   *cbmetrics.FakePodStateObserver
}

func NewCSNPodsLifecycleProcessor(nodeController csnNodeController, csnPodInjectionProcessor pods.PodListProcessor, cbFakePodStateObserver *cbmetrics.FakePodStateObserver, bufferRegistry *fakepods.Registry, csnDefaultRefreshFrequency time.Duration) *CSNPodsLifecycleProcessor {
	return &CSNPodsLifecycleProcessor{
		nodeController:           nodeController,
		csnPodInjectionProcessor: csnPodInjectionProcessor,
		simulator:                scheduling.NewHintingSimulator(),
		bufferRegistry:           bufferRegistry,
		defaultRefreshFrequency:  csnDefaultRefreshFrequency,
		cbFakePodStateObserver:   cbFakePodStateObserver,
	}
}

func (p *CSNPodsLifecycleProcessor) Process(ctx *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	defer metrics.UpdateDurationFromStart(csnPodsLifecycleMetricLabel, time.Now())

	nodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		return nil, fmt.Errorf("error getting node infos: %v", err)
	}
	for _, ni := range nodeInfos {
		node := ni.Node()
		if csn.GetBufferIdFromNode(node) != bufferAssignmentUnknown {
			continue
		}
		removeBufferAssignmentForProcessors(node)
		ni.SetNode(node)
	}

	// TODO(b/474324313): Investigate of a better way of using this processor (e.g. do refactoring there), note that it is OSS code.
	csnPods, err := p.csnPodInjectionProcessor.Process(ctx, nil)
	if err != nil {
		klog.Errorf("%s error creating CSN pods: %v", csnPodsLifecycleLogPrefix, err)
		return nil, nil
	}
	klog.V(4).Infof("%s created %v CSN pods", csnPodsLifecycleLogPrefix, len(csnPods))

	if len(csnPods) == 0 {
		return unschedulablePods, nil
	}

	var newCSNPods []*apiv1.Pod
	bufferIdToBuffer := map[string]*v1beta1.CapacityBuffer{}
	for _, pod := range csnPods {
		buffer := p.bufferRegistry.GetCapacityBuffer(pod.UID)
		if buffer == nil {
			klog.Errorf("%s buffer not found for pod %s/%s, this should never happen", csnPodsLifecycleLogPrefix, pod.Namespace, pod.Name)
			continue
		}
		bufferId := fmt.Sprintf("%s/%s", buffer.Namespace, buffer.Name)
		bufferIdToBuffer[bufferId] = buffer
		csn.MakePodCSN(pod, bufferId)
		addOwnerReference(pod, buffer)
		newCSNPods = append(newCSNPods, pod)
	}
	csnPods = newCSNPods

	if p.cbFakePodStateObserver != nil {
		p.cbFakePodStateObserver.ObserveInjectedPods(csnPods)
	}

	snapshot := ctx.ClusterSnapshot
	snapshot.Fork()

	p.preventSchedulingOnOutdatedNodes(snapshot, bufferIdToBuffer)

	unschedulablePods, err = p.scheduleAndMarkSuspendableNodes(snapshot, unschedulablePods, csnPods)
	if err != nil {
		snapshot.Revert()
		return nil, err
	}
	if err := snapshot.Commit(); err != nil {
		return nil, fmt.Errorf("error while commiting the snapshot")
	}

	return unschedulablePods, nil
}

// TODO(b/495847540): Remove this when OSS change is submitted and merged in internal source.
// This will speed up CSN scheduling due to built-in optimizations in HintingSimulator.
func addOwnerReference(pod *apiv1.Pod, buffer *v1beta1.CapacityBuffer) {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == capacitybuffer.CapacityBufferKind {
			return
		}
	}
	pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{
		APIVersion: capacitybuffer.CapacityBufferApiVersion,
		Kind:       capacitybuffer.CapacityBufferKind,
		Name:       buffer.Name,
		UID:        buffer.UID,
		Controller: ptr.To(true),
	})
}

func (p *CSNPodsLifecycleProcessor) preventSchedulingOnOutdatedNodes(snapshot clustersnapshot.ClusterSnapshot, bufferIdToBuffer map[string]*v1beta1.CapacityBuffer) {
	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		klog.Errorf("%s error getting node infos: %v", csnPodsLifecycleLogPrefix, err)
		return
	}
	var outdatedNodes []string
	for _, ni := range nodeInfos {
		node := ni.Node()
		if !csn.IsCSNNode(node) {
			continue
		}

		bufferId := csn.GetBufferIdFromNode(node)
		if bufferId == "" {
			continue
		}
		buffer := bufferIdToBuffer[bufferId]
		if buffer == nil {
			klog.V(4).Infof("%s buffer %q not found for node %q (the buffer might be deleted)", csnPodsLifecycleLogPrefix, bufferId, node.Name)
			continue
		}

		if !p.isNodeOutdated(node, buffer) {
			continue
		}

		// This is needed bec nodes being unready (due to suspension) can slow down the removal of the node.
		node, err := setNodeAsForProcessors(node, csn.NodeStateConsumed)
		if err != nil {
			klog.Errorf("%s error marking node %q as consumed: %v", csnPodsLifecycleLogPrefix, node.Name, err)
			continue
		}
		csn.AddTaint(node, &outdatedTaint)
		ni.SetNode(node)
		outdatedNodes = append(outdatedNodes, node.Name)
	}

	if len(outdatedNodes) > 0 {
		klog.Infof("%s marked CSN nodes to be refreshed: %v", csnPodsLifecycleLogPrefix, outdatedNodes)
	}
}

// isNodeOutdated checks if the node's suspended time exceeds the buffer's refresh frequency.
func (p *CSNPodsLifecycleProcessor) isNodeOutdated(node *apiv1.Node, buffer *v1beta1.CapacityBuffer) bool {
	refreshFrequency, refresh := getRefreshFrequency(buffer, p.defaultRefreshFrequency)
	if !refresh {
		return false
	}

	var suspendedTime time.Time
	for _, taint := range node.Spec.Taints {
		if !taint.MatchTaint(&csn.SuspendedTaint) {
			continue
		}
		if taint.TimeAdded == nil {
			klog.Warningf("%s node %q has suspended taint but no time added", csnPodsLifecycleLogPrefix, node.Name)
			return false
		}
		suspendedTime = taint.TimeAdded.Time
		break
	}
	if suspendedTime.IsZero() {
		return false
	}

	return time.Since(suspendedTime) > refreshFrequency
}

// getRefreshFrequency returns the frequency at which CSN nodes should be refreshed for a given buffer and a boolean indicating if refreshing is enabled.
// It returns (_, false) if refreshing is disabled ("never"), the default frequency and true if not specified,
// or the parsed duration and true from the buffer's annotation. If parsing fails or the duration is invalid, it returns the default frequency and true.
func getRefreshFrequency(buffer *v1beta1.CapacityBuffer, defaultRefreshFrequency time.Duration) (time.Duration, bool) {
	refFreqStr := buffer.Annotations[nodeRefreshFrequencyBufferAnnotation]
	if refFreqStr == neverRefreshAnnotationValue {
		return 0, false
	}
	if refFreqStr == "" {
		return defaultRefreshFrequency, true
	}
	refreshFrequency, err := time.ParseDuration(refFreqStr)
	if err != nil {
		klog.Errorf("%s error parsing refresh frequency %q for buffer %q: %v", csnPodsLifecycleLogPrefix, refFreqStr, buffer.Name, err)
		return defaultRefreshFrequency, true
	}
	if refreshFrequency <= 0 {
		klog.Errorf("%s invalid refresh frequency %q for buffer %q", csnPodsLifecycleLogPrefix, refFreqStr, buffer.Name)
		return defaultRefreshFrequency, true
	}
	return refreshFrequency, true
}

func addTaint(node *apiv1.Node, taint apiv1.Taint) {
	if taints.TaintExists(node.Spec.Taints, &taint) {
		return
	}
	node.Spec.Taints = append(node.Spec.Taints, taint)
}

// Note: scheduleAndMarkSuspendableNodes assumes that it is already under a forked snapshot.
func (p *CSNPodsLifecycleProcessor) scheduleAndMarkSuspendableNodes(snapshot clustersnapshot.ClusterSnapshot, unschedulablePods []*apiv1.Pod, csnPods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	nodesOfScheduledPods, err := schedulePodsOnCSNNodes(snapshot, p.simulator, csnPods,
		schedulePodsOnCSNNodesOptions{ignoreBufferAssignment: false},
		isChillingFilter,
		isSuspendedFilter,
	)
	if err != nil {
		return nil, fmt.Errorf("error scheduling pods: %v", err)
	}

	nodeNameToBuffer := map[string]*v1beta1.CapacityBuffer{}
	for pod, nodeName := range nodesOfScheduledPods {
		buffer := p.bufferRegistry.GetCapacityBuffer(pod.UID)
		if buffer == nil {
			return nil, fmt.Errorf("buffer not found for pod %s/%s, this should never happen", pod.Namespace, pod.Name)
		}
		nodeNameToBuffer[nodeName] = buffer
	}

	p.scheduledCSNPodsOnUnassignedNodes(csnPods, snapshot, nodesOfScheduledPods, nodeNameToBuffer)

	nodesToPotentiallySuspend := map[string]*framework.NodeInfo{}
	for nodeName := range nodeNameToBuffer {
		ni, err := snapshot.GetNodeInfo(nodeName)
		if err != nil {
			return nil, fmt.Errorf("error getting node info for node %q: %v", nodeName, err) // This should never happen (as we just scheduled pod on that node in ClusterSnapshot earlier).
		}
		nodesToPotentiallySuspend[ni.Node().Name] = ni
	}

	p.nodeController.ProcessBufferAssignment(nodeNameToBuffer)

	suspendedNodes, err := p.nodeController.MarkAsSuspendable(slices.Collect(maps.Values(nodesToPotentiallySuspend)))
	if err != nil {
		return nil, fmt.Errorf("error suspending CSN nodes: %v", err)
	}

	for _, nodeName := range suspendedNodes {
		ni := nodesToPotentiallySuspend[nodeName]
		node := ni.Node()
		node, err = setNodeAsForProcessors(node, csn.NodeStateSuspended)
		if err != nil {
			return nil, fmt.Errorf("error marking node %q as suspended: %v", nodeName, err) // This should never happen (this is just in-memory change of that node).
		}
		ni.SetNode(node)
	}

	if p.cbFakePodStateObserver != nil {
		p.cbFakePodStateObserver.ObserveSchedulablePods(slices.Collect(maps.Keys(nodesOfScheduledPods)))
	}

	var unschedulableCSNPods []*apiv1.Pod
	for _, pod := range csnPods {
		if nodesOfScheduledPods[pod] == "" {
			unschedulableCSNPods = append(unschedulableCSNPods, pod)
		}
	}

	// We need to remove buffer assignment for unschedulable pods. Otherwise, they will trigger creation of new nodepools with workload separation and we don't want that as it affects nodepool upgrade negatively.
	for _, pod := range unschedulableCSNPods {
		csn.RemoveBufferAssignmentWorkloadSeparation(pod)
	}

	unschedulablePods = append(unschedulablePods, unschedulableCSNPods...)
	return unschedulablePods, nil
}

func (p *CSNPodsLifecycleProcessor) scheduledCSNPodsOnUnassignedNodes(pods []*apiv1.Pod, snapshot clustersnapshot.ClusterSnapshot, nodesOfScheduledPods map[*apiv1.Pod]string, nodeNameToBuffer map[string]*v1beta1.CapacityBuffer) {
	loggingQuota := logging.CSNPodLoggingQuota()

	// We use NewLastIndexOrderMapping(0) to ensure that we start searching for a node
	// from the same node we scheduled the previous pod on. This allows us to pack
	// multiple pods belonging to the same buffer on the same node if there's enough capacity,
	// instead of spreading them across multiple available nodes (the default behavior).
	// Note that NewLastIndexOrderMapping is efficient.
	nodeOrdering := clustersnapshot.NewLastIndexOrderMapping(0)

	for _, pod := range pods {
		if nodesOfScheduledPods[pod] != "" {
			continue
		}

		// Since the node is unassigned (i.e. doesn't have csn.BufferAssignmentKey equivalent to the pod), pods won't be scheduled there.
		// So, we temporarily remove the nodeSelector of the pod and revert it after we do the scheduling.
		prev := pod.Spec.NodeSelector[csn.BufferAssignmentKey]
		delete(pod.Spec.NodeSelector, csn.BufferAssignmentKey)
		defer func() {
			if prev != "" {
				pod.Spec.NodeSelector[csn.BufferAssignmentKey] = prev
			}
		}()

		podAssignedBuffer := p.bufferRegistry.GetCapacityBuffer(pod.UID)

		nodeFilter := func(ni *framework.NodeInfo) bool {
			node := ni.Node()
			nodeAssignedBuffer := nodeNameToBuffer[node.Name]
			matchAssignedSoFar := true
			if nodeAssignedBuffer != nil {
				matchAssignedSoFar = getBufferId(nodeAssignedBuffer) == getBufferId(podAssignedBuffer)
			}
			return csn.IsCSNNode(node) && matchAssignedSoFar
		}

		matchingNode, schedulingErr := snapshot.SchedulePodOnAnyNodeMatching(pod, clustersnapshot.SchedulingOptions{
			IsNodeAcceptable: nodeFilter,
			NodeOrdering:     nodeOrdering,
		})
		if schedulingErr != nil {
			klogx.V(4).UpTo(loggingQuota).Infof("%s couldn't schedule CSN pod %s/%s on any node: %v", csnPodsLifecycleLogPrefix, pod.Namespace, pod.Name, schedulingErr)
			continue
		}
		if matchingNode == "" {
			continue
		}

		nodesOfScheduledPods[pod] = matchingNode
		nodeNameToBuffer[matchingNode] = podAssignedBuffer

		ni, err := snapshot.GetNodeInfo(matchingNode)
		if err != nil {
			klog.Errorf("%s error getting node info for node %q (this should never happen): %v", csnPodsLifecycleLogPrefix, matchingNode, err)
			continue
		}
		node := ni.Node()
		node, err = assignNodeToBufferForProcessors(node, getBufferId(podAssignedBuffer))
		if err != nil {
			klog.Errorf("%s error assigning node %q to buffer %q: %v", csnPodsLifecycleLogPrefix, node.Name, getBufferId(podAssignedBuffer), err)
			continue
		}
		ni.SetNode(node)
	}
	klogx.V(4).Over(loggingQuota).Infof("%s there were also %v more pods that couldn't be scheduled on any node", csnPodsLifecycleLogPrefix, -loggingQuota.Left())
}

func getBufferId(buffer *v1beta1.CapacityBuffer) string {
	if buffer == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s", buffer.Namespace, buffer.Name)
}
