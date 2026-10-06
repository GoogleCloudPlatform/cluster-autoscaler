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
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apilabels "k8s.io/apimachinery/pkg/labels"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	lookaheadbuffer_strategy "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer/strategy"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/daemon"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	taintutils "sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

const (
	sampleNodeName = "ca-sample-default-biggest-node"

	milliCpuMetricIncrement  = 1000
	memoryKiBMetricIncrement = 4 * giBToKiB

	giBToKiB = size.GiB / size.KiB
)

var biggestMachineTypeForMachineFamily = map[string]string{
	machinetypes.EK.Name():  "ek-standard-32",
	machinetypes.E4A.Name(): "e4a-standard-32",
	machinetypes.E4.Name():  "e4-standard-32",
}

type limiter interface {
	Limit() int
}

type metrics interface {
	UpdateLookaheadPodsCount(laPodsCount map[size.Allocatable]int)
}

type strategyProvider interface {
	Strategy(machineFamily string) (lookaheadbuffer_strategy.LookaheadPodStrategy, error)
}

type workloadIDRequestsPair struct {
	workloadID string
	resources  apiv1.ResourceList
}

// LookaheadPodInjectionProcessor injects lookahead pods to unschedulable pods.
type LookaheadPodInjectionProcessor struct {
	laPodProvider        lookaheadbuffer.PodProvider
	strategyProvider     strategyProvider
	mcp                  *machinetypes.MachineConfigProvider
	limiter              limiter
	systemPodsClassifier systempods.Classifier
	cccLister            lister.Lister
	metrics              metrics
	// We use this node to simulate which daemonSets can be scheduled on this node (e.g. match nodeSelector and taints criteria).
	// It won't be perfect (in fact it is impossible to make it perfect), but it doesn't need to be perfect since it is an optimization.
	// Since the node creation is idempotent, it is only ran once at the beginning and cached.
	sampleNodesPerMachineFamily map[string]*apiv1.Node
}

// NewLookaheadPodInjectionProcessor return an instance of LookaheadPodInjectionProcessor.
func NewLookaheadPodInjectionProcessor(laPodProvider lookaheadbuffer.PodProvider, strategyProvider strategyProvider, limiter limiter, mcp *machinetypes.MachineConfigProvider, systemPodsClassifier systempods.Classifier, cccLister lister.Lister, calc calculator.Calculator, metrics metrics) *LookaheadPodInjectionProcessor {
	sampleNodesPerMachineFamily := map[string]*apiv1.Node{}
	for _, family := range mcp.ResizableFamilyNames() {
		sampleNode, err := getSampleDefaultBiggestNode(calc, family)
		if err != nil {
			klog.Errorf("Failed to get sample node in LookaheadPodInjectionProcessor pod list processor: %v", err)
			continue
		}
		sampleNodesPerMachineFamily[family] = sampleNode
	}

	return &LookaheadPodInjectionProcessor{
		laPodProvider:               laPodProvider,
		strategyProvider:            strategyProvider,
		mcp:                         mcp,
		limiter:                     limiter,
		systemPodsClassifier:        systemPodsClassifier,
		cccLister:                   cccLister,
		metrics:                     metrics,
		sampleNodesPerMachineFamily: sampleNodesPerMachineFamily,
	}
}

// Process updates unschedulablePods by injecting lookahead pods.
func (p *LookaheadPodInjectionProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	machineFamiliesWithLAEnabled := map[string]bool{}
	for _, family := range p.mcp.ResizableFamilyNames() {
		status := p.launchStatus(family)
		if status != lookaheadbuffer_strategy.Enabled {
			klog.V(4).Infof("Skipping lookahead buffer for %s machine family. Status: %q", family, status)
			continue
		}
		machineFamiliesWithLAEnabled[family] = true

		if p.sampleNodesPerMachineFamily[family] == nil {
			p.emitLookaheadPodsCountMetric(nil)
			return unschedulablePods, errors.New("sample node is nil in LookaheadPodInjectionProcessor pod list processor, it should be initialized correctly during the initialization of the processor")
		}
	}
	if len(machineFamiliesWithLAEnabled) == 0 {
		p.emitLookaheadPodsCountMetric(nil)
		return unschedulablePods, nil
	}

	nodeInfos, err := autoscalingCtx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		p.emitLookaheadPodsCountMetric(nil)
		return unschedulablePods, fmt.Errorf("failed to list nodeInfos from cluster snapshot: %v", err)
	}

	taintConfig := taintutils.NewTaintConfig(autoscalingCtx.AutoscalingOptions)
	requests := p.podRequestsPerMachineFamilyPerWorkloadID(nodeInfos, &taintConfig, machineFamiliesWithLAEnabled)
	topRequests := p.limitMaxWorkloadSeparations(requests)
	lookaheadPods := p.createLookaheadPods(autoscalingCtx, topRequests)

	p.emitLookaheadPodsCountMetric(lookaheadPods)

	if len(lookaheadPods) == 0 {
		return unschedulablePods, nil
	}

	return slices.Concat(lookaheadPods, unschedulablePods), nil
}

func (p *LookaheadPodInjectionProcessor) launchStatus(machineFamily string) lookaheadbuffer_strategy.Status {
	strategy, err := p.strategyProvider.Strategy(machineFamily)
	if err != nil {
		klog.Errorf("Error while fetching lookahead buffer strategy: %v", err)
		return lookaheadbuffer_strategy.Unspecified
	}
	return strategy.Status
}

func (p *LookaheadPodInjectionProcessor) podRequestsPerMachineFamilyPerWorkloadID(nodeInfos []*framework.NodeInfo, taintConfig *taintutils.TaintConfig, machineFamiliesWithLAEnabled map[string]bool) map[string]map[string]apiv1.ResourceList {
	requests := map[string]map[string]apiv1.ResourceList{}
	for _, ni := range nodeInfos {
		machineFamily, err := utils.GetMachineFamilyName(ni.Node())
		if err != nil || !machineFamiliesWithLAEnabled[machineFamily] {
			continue
		}

		if !p.isNodeEligibleForLookahead(ni.Node(), taintConfig) {
			continue
		}
		podRequests := sumNonSystemPodRequests(ni, p.systemPodsClassifier)
		if len(podRequests) > 0 {
			id := podrequirements.ExtractWorkloadID(ni.Node())
			if requests[machineFamily] == nil {
				requests[machineFamily] = map[string]apiv1.ResourceList{}
			}
			requests[machineFamily][id] = quota.Add(requests[machineFamily][id], podRequests)
		}
	}
	return requests
}

func (p *LookaheadPodInjectionProcessor) limitMaxWorkloadSeparations(requestsPerMachineFamily map[string]map[string]apiv1.ResourceList) map[string]map[string]apiv1.ResourceList {
	// Default workload ID should always have lookahead enabled.
	// This is in case default workload ID isn't in the top `maxWorkloadSeparations` by pod requests.
	limitedRequests := map[string]map[string]apiv1.ResourceList{}
	for machineFamily, requests := range requestsPerMachineFamily {
		defaultWID, defaultExists := requests[""]
		delete(requests, "")

		// TODO(b/421106616): Set of workload IDs with lookahead is recomputed every loop. A cluster
		// with more workload IDs than `maxWorkloadSeparations` might have some groups moving between having lookahead and not having it.
		// This could lead to extra node churn. This is an edge-case and probably not worth handling right now.
		limitedRequests[machineFamily] = selectLargestRequests(requests, p.limiter.Limit())
		if defaultExists {
			// Add default workload ID back, if it existed in the first place.
			limitedRequests[machineFamily][""] = defaultWID
		}
	}
	return limitedRequests
}

func (p *LookaheadPodInjectionProcessor) createLookaheadPods(ctx *ca_context.AutoscalingContext, requestsPerMachineFamilyPerWorkloadID map[string]map[string]apiv1.ResourceList) []*apiv1.Pod {
	lookaheadPods := []*apiv1.Pod{}
	for family, requestsPerWorkloadID := range requestsPerMachineFamilyPerWorkloadID {
		for id, requests := range requestsPerWorkloadID {
			pods, err := p.createLookaheadPodsForWorkloadID(id, requests, family, ctx)
			if err != nil {
				klog.Warningf("Couldn't create lookahead pods for workload ID %q and machine family %q: %v", id, family, err)
				continue
			}

			logLookaheadPods(pods, id, family)
			lookaheadPods = append(lookaheadPods, pods...)
		}
	}
	return lookaheadPods
}

// createLookaheadPodsForWorkloadID creates lookahead pods for single workload ID.
func (p *LookaheadPodInjectionProcessor) createLookaheadPodsForWorkloadID(workloadID string, requests apiv1.ResourceList, machineFamily string, ctx *ca_context.AutoscalingContext) ([]*apiv1.Pod, error) {
	pods := p.laPodProvider.GetLookaheadPods(int(requests.Cpu().Value()), workloadID, machineFamily)
	pods, err := p.subtractDaemonSet(ctx, pods, workloadID, machineFamily)
	return pods, err
}

// subtractDaemonSet subtracts DaemonSet resource usage from lookahead pods to avoid overprovisioning and avoid having unschedulable lookahead pod indefinitely.
func (p *LookaheadPodInjectionProcessor) subtractDaemonSet(ctx *ca_context.AutoscalingContext, pods []*apiv1.Pod, workloadID, machineFamily string) ([]*apiv1.Pod, error) {
	dsSize, err := p.getTargetDaemonSetSize(ctx, podrequirements.WorkloadIDToTolerations(workloadID), machineFamily)
	if err != nil {
		return nil, err
	}
	klog.V(4).Infof("Estimated daemonset size subtracted from lookahead pods for workload ID %q: %v", workloadID, stringifyResourceList(dsSize))

	var newPods []*apiv1.Pod
	for _, pod := range pods {
		laRequests := lookaheadbuffer.CpuMemRequests(pod)
		laRequests = quota.Subtract(laRequests, dsSize)
		if len(quota.IsNegative(laRequests)) > 0 {
			klog.Warningf("Skipping lookahead pod %q: daemonset requests (%+v) is greater than lookahead pod requests (%+v)", pod.Name, dsSize, laRequests)
			continue
		}
		pod.Spec.Containers[0].Resources.Requests[apiv1.ResourceCPU] = *laRequests.Cpu()
		pod.Spec.Containers[0].Resources.Requests[apiv1.ResourceMemory] = *laRequests.Memory()
		newPods = append(newPods, pod)
	}
	return newPods, nil
}

// getTargetDaemonSetSize estimates the aggregate resource requests of DaemonSets schedulable on a default, biggest node.
func (p *LookaheadPodInjectionProcessor) getTargetDaemonSetSize(ctx *ca_context.AutoscalingContext, tolerations []apiv1.Toleration, machineFamily string) (apiv1.ResourceList, error) {
	logger := klog.FromContext(context.Background())
	requests := apiv1.ResourceList{}
	sampleNode := p.sampleNodesPerMachineFamily[machineFamily]
	if sampleNode == nil {
		return nil, fmt.Errorf("sample node for machine family %s is nil", machineFamily)
	}
	node := updateNodeWithWorkloadID(sampleNode.DeepCopy(), tolerations)
	daemonSets, err := ctx.ListerRegistry.DaemonSetLister().List(apilabels.Everything())
	if err != nil {
		return nil, fmt.Errorf("failed to list daemon sets: %v", err)
	}
	for _, ds := range daemonSets {
		if shouldRun, _ := daemon.NodeShouldRunDaemonPod(logger, node, ds); shouldRun {
			requests = quota.Add(requests, daemonSetRequests(ds))
		}
	}
	return requests, nil
}

func daemonSetRequests(ds *appsv1.DaemonSet) apiv1.ResourceList {
	pod := daemon.NewPod(ds, sampleNodeName)
	return lookaheadbuffer.CpuMemRequests(pod)
}

func (p *LookaheadPodInjectionProcessor) emitLookaheadPodsCountMetric(pods []*apiv1.Pod) {
	laPodsCount := map[size.Allocatable]int{}

	for _, pod := range pods {
		requests := utils.PodRequestsAsSize(pod)
		requests.MilliCpus = size.RoundUpToIncrement(requests.MilliCpus, milliCpuMetricIncrement)
		requests.KBytes = size.RoundUpToIncrement(requests.KBytes, memoryKiBMetricIncrement)
		laPodsCount[requests]++
	}

	p.metrics.UpdateLookaheadPodsCount(laPodsCount)
}

// logLookaheadPods logs important information about lookahead pods.
func logLookaheadPods(pods []*apiv1.Pod, workloadID string, machineFamily string) {
	var podsLogs []string
	for _, pod := range pods {
		requests := utils.PodRequestsAsSize(pod)
		podsLogs = append(podsLogs, fmt.Sprintf("(pod name: %q, pod requests: %v)", pod.Name, requests))
	}
	klog.V(4).Infof("Injected %d lookahead pods for workload ID %q and machine family %q: %s", len(pods), workloadID, machineFamily, strings.Join(podsLogs, ", "))
}

func getSampleDefaultBiggestNode(calc calculator.Calculator, machineFamily string) (*apiv1.Node, error) {
	vmSize, err := calc.GetMaxResizableVmSizeByMachineType(biggestMachineTypeForMachineFamily[machineFamily])
	if err != nil {
		return nil, err
	}
	node := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:     sampleNodeName,
			SelfLink: fmt.Sprintf("/api/v1/nodes/%s", sampleNodeName),
			Labels: map[string]string{
				apiv1.LabelInstanceTypeStable: biggestMachineTypeForMachineFamily[machineFamily],
				gkelabels.MachineFamilyLabel:  machineFamily,
				// TODO(b/510661032): Specify gce.SystemArchitecture for E4A machine family
				apiv1.LabelArchStable:            string(gce.DefaultArch),
				apiv1.LabelOSStable:              string(gce.OperatingSystemDefault),
				gkelabels.GkeOsDistributionLabel: string(gce.OperatingSystemDistributionDefault),
				gkelabels.MaxPodsPerNodeLabel:    strconv.FormatInt(gkelabels.DefaultMaxPodsPerNode, 10),
			},
		},
		Spec: apiv1.NodeSpec{
			ProviderID: sampleNodeName,
		},
		Status: apiv1.NodeStatus{
			Capacity: apiv1.ResourceList{
				apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
				apiv1.ResourceCPU:    *resource.NewMilliQuantity(vmSize.MilliCpus, resource.DecimalSI),
				apiv1.ResourceMemory: *resource.NewQuantity(vmSize.KBytes*size.KiB, resource.BinarySI),
			},
		},
	}
	allocatable := calc.ToAllocatable(node, vmSize)
	node.Status.Allocatable = apiv1.ResourceList{
		apiv1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
		apiv1.ResourceCPU:    *resource.NewMilliQuantity(allocatable.MilliCpus, resource.DecimalSI),
		apiv1.ResourceMemory: *resource.NewQuantity(allocatable.KBytes*size.KiB, resource.BinarySI),
	}
	return node, nil
}

func stringifyResourceList(resourceList apiv1.ResourceList) string {
	// We convert resourceList to a map whose value is pointer to quantity since pointer to quantity has proper String() method.
	resources := map[apiv1.ResourceName]*resource.Quantity{}
	for resourceName, quantity := range resourceList {
		resources[resourceName] = &quantity
	}
	return fmt.Sprint(resources)
}

func sumNonSystemPodRequests(nodeInfo *framework.NodeInfo, classifier systempods.Classifier) apiv1.ResourceList {
	resources := apiv1.ResourceList{}
	for _, podInfo := range nodeInfo.Pods() {
		if classifier.IsSystemPod(podInfo.Pod) {
			continue
		}
		podResources := podutils.PodRequests(podInfo.Pod)
		resources = quota.Add(resources, podResources)
	}
	return resources
}

// isNodeEligibleForLookahead checks if pods running on this node should be included in the calculation for lookahead buffer.
func (p *LookaheadPodInjectionProcessor) isNodeEligibleForLookahead(n *apiv1.Node, taintConfig *taintutils.TaintConfig) bool {
	isResizable, err := utils.IsResizableNode(n, p.mcp)
	if err != nil || !isResizable {
		return false
	}

	if _, usesCC := n.Labels[gkelabels.ComputeClassLabel]; usesCC && !hasEligibleComputeClass(n, p.cccLister) {
		return false
	}

	// Lookahead buffer is only supported on on-demand nodes.
	// NAP creates a workload separation for spot VMs without compute class,
	// which would pass hasSupportedTaints check
	if utils.IsPreemptible(n) {
		return false
	}

	if !hasSupportedTaints(n, taintConfig) {
		return false
	}
	return true
}

// hasEligibleComputeClass checks if the compute class associated with this node
// supports lookahead buffer. No compute class results in `true`.
//
// Currently, resizable VM nodes are only consumable through following compute classes:
// `autopilot`, `autopilot-spot`, `autopilot-arm`, `autopilot-arm-spot`.
// Since lookahead buffer is a latency optimization,
// we decided to only support lookahead on pod-billed compute classes where on-demand
// resizable VM nodes are the highest priority.
func hasEligibleComputeClass(node *apiv1.Node, lister lister.Lister) bool {
	crd, _, err := lister.NodeCrd(node)
	if err != nil {
		klog.Errorf("Unexpected error while listing CCC CRD for node %q: %v", node.Name, err)
		return false
	}
	if crd == nil {
		return true
	}
	if !crd.AutopilotManaged() {
		return false
	}
	if len(crd.Rules()) == 0 {
		return false
	}
	/*
		We only support lookahead if the highest priority is a non-spot resizable node.
		* It makes little sense to support lookahead for any but the highest priority.
		* Spot VMs don't warrant the same latency requirements.
	*/
	if topRule := crd.Rules()[0]; !rules.IsPodFamily(topRule.PodFamilyName()) || topRule.Spot() {
		return false
	}
	return true
}

// hasSupportedTaints checks if the node only has taints either ignored by the taint config or those belonging to its
// workload separation or custom compute class.
func hasSupportedTaints(node *apiv1.Node, taintConfig *taintutils.TaintConfig) bool {
	if node == nil {
		return false
	}
	if taintConfig == nil {
		klog.Error("taintConfig is nil, this should not happen")
		return false
	}

	workloadIDTaints := podrequirements.ExtractWorkloadIDTaints(node)
	sanitizedTaints := taintutils.SanitizeTaints(context.TODO(), node.Spec.Taints, *taintConfig)

	if len(workloadIDTaints) != len(sanitizedTaints) {
		return false
	}
	// Set equality check.
	slices.SortFunc(workloadIDTaints, func(a, b apiv1.Taint) int {
		return strings.Compare(a.Key, b.Key)
	})
	slices.SortFunc(sanitizedTaints, func(a, b apiv1.Taint) int {
		return strings.Compare(a.Key, b.Key)
	})
	return slices.Equal(workloadIDTaints, sanitizedTaints)
}

func updateNodeWithWorkloadID(node *apiv1.Node, wsTolerations []apiv1.Toleration) *apiv1.Node {
	for _, t := range wsTolerations {
		node.Spec.Taints = append(node.Spec.Taints, apiv1.Taint{
			Key:    t.Key,
			Value:  t.Value,
			Effect: t.Effect,
		})
		node.Labels[t.Key] = t.Value
	}
	return node
}

func selectLargestRequests(requests map[string]apiv1.ResourceList, limit int) map[string]apiv1.ResourceList {
	if len(requests) <= limit {
		return requests
	}

	pairs := make([]workloadIDRequestsPair, 0, len(requests))
	for k, v := range requests {
		pairs = append(pairs, workloadIDRequestsPair{k, v})
	}
	slices.SortFunc(pairs, func(a, b workloadIDRequestsPair) int {
		aCpu := a.resources.Cpu().MilliValue()
		bCpu := b.resources.Cpu().MilliValue()
		if aCpu != bCpu {
			return int(aCpu - bCpu)
		}
		return strings.Compare(b.workloadID, a.workloadID)

	})
	slices.Reverse(pairs)
	topRequests := pairs[:min(limit, len(pairs))]
	requests = map[string]apiv1.ResourceList{}
	for _, pair := range topRequests {
		requests[pair.workloadID] = pair.resources
	}

	return requests
}
