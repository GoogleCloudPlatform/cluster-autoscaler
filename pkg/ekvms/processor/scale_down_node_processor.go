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
	"fmt"
	"log"
	"math"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	processor_proto "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor/proto"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	kube "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	gke_metrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	processResizableDownsizes = "scaleDown:ekProcessDownsizes"
)

type nodeInfoLister interface {
	GetNodeInfo(nodeName string) (*framework.NodeInfo, error)
}

type scaleDownMetrics interface {
	UpdateNodesWithLookaheadPodsShape([]gke_metrics.LAPodNodeShape)
}

type ScaleDownNodeProcessor struct {
	resizableVmManager           operationtracker.Manager
	experimentsManager           experiments.Manager
	fetcher                      kube.UpdateInfoFetcher
	downsizeConfigProvider       config.Provider[map[string]*processor_proto.DownsizeConfig]
	requestedResourcesMaxWindows map[string]utils.MaxWindow
	downsizePossibleSince        map[string]time.Time
	sizeCalculator               calculator.Calculator
	metrics                      scaleDownMetrics
	clock                        clock.PassiveClock
	mcp                          *machinetypes.MachineConfigProvider
}

func NewScaleDownNodeProcessor(mcp *machinetypes.MachineConfigProvider, resizableVmManager operationtracker.Manager, experimentsManager experiments.Manager, fetcher kube.UpdateInfoFetcher, downsizeConfigProvider config.Provider[map[string]*processor_proto.DownsizeConfig], sizeCalculator calculator.Calculator, metrics scaleDownMetrics, clock clock.PassiveClock) *ScaleDownNodeProcessor {
	if downsizeConfigProvider == nil {
		log.Fatal("NewScaleDownNodeProcessor: downsizeConfigProvider is not allowed to be nil.")
	}
	return &ScaleDownNodeProcessor{
		resizableVmManager:           resizableVmManager,
		experimentsManager:           experimentsManager,
		fetcher:                      fetcher,
		downsizeConfigProvider:       downsizeConfigProvider,
		requestedResourcesMaxWindows: make(map[string]utils.MaxWindow),
		downsizePossibleSince:        make(map[string]time.Time),
		sizeCalculator:               sizeCalculator,
		metrics:                      metrics,
		clock:                        clock,
		mcp:                          mcp,
	}
}

func (p *ScaleDownNodeProcessor) GetScaleDownCandidates(ctx *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	sourceCandidates, _, _ := p.process(ctx, nodes, false)
	return sourceCandidates, nil
}

func (p *ScaleDownNodeProcessor) GetPodDestinationCandidates(ctx *context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	_, targetCandidates, _ := p.process(ctx, nodes, true)
	return targetCandidates, nil
}

func (p *ScaleDownNodeProcessor) CleanUp() {}

// process executes the scaledown logic: determines for each node if it should be a source/candidate
// for scaledown, or if it should be downsized.
func (p *ScaleDownNodeProcessor) process(ctx *context.AutoscalingContext, nodes []*apiv1.Node, emitLookaheadMetrics bool) (sourceCandidates, targetCandidates []*apiv1.Node, newDesiredSizes map[string]size.Allocatable) {
	resizableFamilies := p.mcp.AllResizableMachineFamilies()
	if !isAnyResizingEnabled(p.resizableVmManager, resizableFamilies) {
		return nodes, nodes, nil
	}

	defer metrics.UpdateDurationFromStart(processResizableDownsizes, time.Now())

	var filterMode operationtracker.SnapshotFilterMode
	if p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.EkDownsizeNonResizableFlag, false) {
		filterMode = operationtracker.DownsizableOnly
	} else {
		filterMode = operationtracker.ResizableOnly
	}
	resizableSnapshot := p.resizableVmManager.FilteredNodesSnapshot(false, filterMode)
	downsizeConfigs := p.downsizeConfigProvider.Provide()
	p.updateRequestedResources(ctx, downsizeConfigs, nodes, resizableSnapshot)

	sourceCandidates, targetCandidates, newDesiredSizes = p.classifyNodes(ctx.ClusterSnapshot, downsizeConfigs, nodes, resizableSnapshot)
	p.adjustBalloonPods(ctx, newDesiredSizes)

	if emitLookaheadMetrics {
		p.emitLookaheadMetrics(ctx)
	}

	return
}

func (p *ScaleDownNodeProcessor) emitLookaheadMetrics(ctx *context.AutoscalingContext) {
	nodeInfos, err := ctx.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		klog.Infof("Getting node infos from cluster snapshot failed (some metrics won't be emitted): %v", err)
		return
	}
	resizableSnapshot := p.resizableVmManager.FilteredNodesSnapshot(false, operationtracker.AllNodes)

	var laNodesShape []gke_metrics.LAPodNodeShape
	for _, nodeInfo := range nodeInfos {
		laRequests := allLookaheadPodsRequests(nodeInfo)
		if laRequests.KBytes == 0 && laRequests.MilliCpus == 0 {
			continue
		}

		_, isUpcoming := nodeInfo.Node().Annotations[annotations.NodeUpcomingAnnotation]
		if isUpcoming {
			continue
		}

		name := nodeInfo.Node().Name
		resizableNode, exists := resizableSnapshot[name]
		if !exists {
			klog.Warningf("resizable node %q not found in resizable node snapshot", name)
			continue
		}

		userWorkloadPodsNum := 0
		for _, podInfo := range nodeInfo.Pods() {
			if IsUserWorkloadPod(podInfo.Pod) {
				userWorkloadPodsNum++
			}
		}

		laNodesShape = append(laNodesShape, gke_metrics.LAPodNodeShape{
			NodeSizeAllocatable:   resizableNode.DesiredSize,
			UserWorkloadPodsCount: userWorkloadPodsNum,
		})
	}

	p.metrics.UpdateNodesWithLookaheadPodsShape(laNodesShape)
}

// updateRequestedResources updates requestedResourcesMaxWindows with current values for resizable nodes.
// Creates new max windows for new resizable nodes, and deletes max windows for nodes that are not tracked
// by resizable VM manager.
func (p *ScaleDownNodeProcessor) updateRequestedResources(ctx *context.AutoscalingContext, downsizeConfigs map[string]*processor_proto.DownsizeConfig, nodes []*apiv1.Node, resizableSnapshot operationtracker.ResizableNodesSnapshot) {
	for name := range p.requestedResourcesMaxWindows {
		if _, exists := resizableSnapshot[name]; !exists {
			// Node becoming unresizable resets its downsizing state.
			delete(p.requestedResourcesMaxWindows, name)
			delete(p.downsizePossibleSince, name)
		}
	}
	for _, node := range nodes {
		name := node.Name
		resizableNode, exists := resizableSnapshot[name]
		if !exists {
			continue
		}
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(name)
		if err != nil {
			continue
		}
		downsizeConfig := downsizeConfigs[resizableNode.MachineFamily]
		if downsizeConfig == nil {
			continue
		}

		var requestedResources size.Allocatable
		if resizableNode.IsResizable() && resizableNode.IsSafelyUpsizable() {
			bPodAndLAPodsRequests := size.Add(allLookaheadPodsRequests(nodeInfo), allBalloonPodsRequests(nodeInfo))
			requestedResources = size.Subtract(getRequestedResources(nodeInfo), bPodAndLAPodsRequests)
		} else {
			requestedResources = size.Subtract(getRequestedResources(nodeInfo), allBalloonPodsRequests(nodeInfo))
		}

		if _, exists := p.requestedResourcesMaxWindows[name]; exists {
			p.requestedResourcesMaxWindows[name].UpdateTtl(downsizeConfig.SmoothingWindowLength.AsDuration())
		} else {
			p.requestedResourcesMaxWindows[name] = utils.NewTtlMaxWindow(p.clock, downsizeConfig.SmoothingWindowLength.AsDuration())
		}
		p.requestedResourcesMaxWindows[name].Add(requestedResources)
	}
}

// classifyNodes decides if a resizable node is a suitable (source/target) scaledown candidate, and if the node should be downsized.
// For nodes with decision to downsize it creates and enqueues a resize operation.
func (p *ScaleDownNodeProcessor) classifyNodes(nodeInfos nodeInfoLister, downsizeConfigs map[string]*processor_proto.DownsizeConfig, nodes []*apiv1.Node, resizableSnapshot operationtracker.ResizableNodesSnapshot) (sourceCandidates, targetCandidates []*apiv1.Node, newDesiredSizes map[string]size.Allocatable) {
	nodesWithSurge, err := p.nodesWithSurge()
	if err != nil {
		klog.Infof("Getting resizable nodes with surge failed : %v", err)
		return nodes, nodes, nil
	}

	nodesScaleDownAllowed := p.getNodesScaleDownAllowedFromCache(nodes)
	sourceCandidates = []*apiv1.Node{}
	targetCandidates = []*apiv1.Node{}
	newDesiredSizes = map[string]size.Allocatable{}

	for _, node := range nodes {
		var allowedForScaledown bool
		var newDesiredSize *size.Allocatable

		// Check if the node's scale-down status is already present in the cache.
		if allowed, found := nodesScaleDownAllowed[node.Name]; found {
			allowedForScaledown = allowed
		} else {
			// If not found in the cache, determine the node's scale-down eligibility and desired size.
			allowedForScaledown, newDesiredSize = p.classifyNode(resizableSnapshot, nodeInfos, nodesWithSurge, downsizeConfigs, node)
			// Cache the calculated scale-down status for future use.
			nodesScaleDownAllowed[node.Name] = allowedForScaledown
		}

		if allowedForScaledown {
			sourceCandidates = append(sourceCandidates, node)
			targetCandidates = append(targetCandidates, node)
		}
		if newDesiredSize != nil {
			newDesiredSizes[node.Name] = *newDesiredSize
		}
	}

	p.resizableVmManager.UpdateNodesScaleDownAllowedCache(nodesScaleDownAllowed)
	return
}

func (p *ScaleDownNodeProcessor) adjustBalloonPods(ctx *context.AutoscalingContext, newDesiredSizes map[string]size.Allocatable) {
	ctx.ClusterSnapshot.Fork()
	err := AdjustBalloonPodsSize(ctx.ClusterSnapshot, newDesiredSizes, nil)
	if err != nil {
		klog.Infof("Adjusting balloon pod sizes failed: %v", err)
		ctx.ClusterSnapshot.Revert()
		return
	}
	err = ctx.ClusterSnapshot.Commit()
	if err != nil {
		klog.Infof("Committing cluster snapshot failed: %v", err)
	}
}

func (p *ScaleDownNodeProcessor) nodesWithSurge() (map[string]bool, error) {
	if p.fetcher == nil {
		return nil, fmt.Errorf("fetcher is nil")
	}
	updateInfos, err := p.fetcher.GetUpdateInfos()
	if err != nil {
		return nil, fmt.Errorf("getting update infos failed: %v", err)
	}
	nodesWithSurge := make(map[string]bool)
	for _, updateInfo := range updateInfos {
		nodesWithSurge[updateInfo.Spec.TargetNode] = true
		if updateInfo.Spec.SurgeNode != "" {
			nodesWithSurge[updateInfo.Spec.SurgeNode] = true
		}
	}
	return nodesWithSurge, nil
}

// classifyNode decides if a node should be downsized. If yes, then a downsize operation
// is enqueued in resizable manager. classifyNode also decides if a node should be a scaledown candidate,
// based on a downsize config selected according to the node desired size, and the result of
// downsize operation enqueuing.
func (p *ScaleDownNodeProcessor) classifyNode(resizableSnapshot operationtracker.ResizableNodesSnapshot, nodeInfos nodeInfoLister, nodesWithSurge map[string]bool, downsizeConfigs map[string]*processor_proto.DownsizeConfig, node *apiv1.Node) (allowedForScaledown bool, newDesiredSize *size.Allocatable) {
	allowedForScaledown = true
	newDesiredSize = nil

	if !isResizableNode(node, p.mcp) {
		return
	}

	if p.resizableVmManager.IsNodeResizingOrPending(node.Name) {
		klog.V(4).Infof("Node %q is in process of resize, not eligible for scaledown.", node.Name)
		return false, nil
	}

	resizableNode, exists := resizableSnapshot[node.Name]
	if !exists {
		return
	}

	downsizeConfig := downsizeConfigs[resizableNode.MachineFamily]
	downsizeBehavior, found := GetBehavior(downsizeConfig, resizableNode)
	if !found {
		klog.Infof("Downsize config not found for node %q.", node.Name)
		return
	}

	allowedForScaledown = downsizeBehavior.AllowedForScaledown

	downsizePossibleSince := p.downsizePossibleSince[node.Name]
	// Reset downsizePossibleSince if an operation happened in the meantime.
	if resizableNode.LastOperationTime.After(downsizePossibleSince) {
		downsizePossibleSince = time.Time{}
		delete(p.downsizePossibleSince, node.Name)
	}

	requestedResourcesMaxWindow := p.requestedResourcesMaxWindows[node.Name]
	if requestedResourcesMaxWindow == nil {
		klog.Errorf("No requestedResourcesMaxWindow for node %q.", node.Name)
		return
	}

	if nodesWithSurge[node.Name] {
		klog.V(4).Infof("Node %q has a surge upgrade in progress.", node.Name)
		delete(p.downsizePossibleSince, node.Name)
		return
	}

	targetMilliCpus, err := requestedResourcesMaxWindow.MaxMilliCpus()
	if err != nil {
		klog.Warningf("Getting maxMilliCpus for node %q failed: %v", node.Name, err)
		return
	}
	minDownsizeMillicores := int64(math.Round(downsizeBehavior.MinDownsizeFraction * float64(resizableNode.PhysicalMaxSize.MilliCpus)))
	if targetMilliCpus < minDownsizeMillicores && minDownsizeMillicores <= resizableNode.DesiredSize.MilliCpus {
		klog.V(4).Infof("Applying min downsize of %v (instead of %v) MilliCpus for node %q", minDownsizeMillicores, targetMilliCpus, node.Name)
		targetMilliCpus = minDownsizeMillicores
	}
	targetKBytes, err := requestedResourcesMaxWindow.MaxKBytes()
	if err != nil {
		klog.Warningf("Getting maxKBytes for node %q failed: %v", node.Name, err)
		return
	}
	minDownsizeKBytes := int64(math.Round(downsizeBehavior.MinDownsizeFraction * float64(resizableNode.PhysicalMaxSize.KBytes)))
	if targetKBytes < minDownsizeKBytes && minDownsizeKBytes <= resizableNode.DesiredSize.KBytes {
		klog.V(4).Infof("Applying min downsize of %v (instead of %v) KBytes for node %q", minDownsizeKBytes, targetKBytes, node.Name)
		targetKBytes = minDownsizeKBytes
	}

	rawTargetDesiredSize := size.Allocatable{MilliCpus: targetMilliCpus, KBytes: targetKBytes}
	nodeInfo, err := nodeInfos.GetNodeInfo(node.Name)
	if err != nil {
		klog.Warningf("Getting node info for node %q failed: %v", node.Name, err)
		return
	}
	targetDesiredSize, err := p.sizeCalculator.RoundUp(nodeInfo.Node(), rawTargetDesiredSize)
	if err != nil {
		klog.Warningf("Rounding up target desired size for node %q failed: %v", node.Name, err)
		return
	}

	if !targetDesiredSize.IsDownsizeFrom(resizableNode.DesiredSize) {
		klog.V(4).Infof("Not a downsize for node %q: targetDesiredSize=%+v, rawTargetDesiredSize=%+v, resizableNode.DesiredSize=%+v", node.Name, targetDesiredSize, rawTargetDesiredSize, resizableNode.DesiredSize)
		delete(p.downsizePossibleSince, node.Name)
		return
	}

	if downsizePossibleSince.IsZero() {
		downsizePossibleSince = p.clock.Now()
		p.downsizePossibleSince[node.Name] = downsizePossibleSince
	}

	delayEnd := downsizePossibleSince.Add(downsizeBehavior.DownsizeDelay.AsDuration())
	if p.clock.Now().Before(delayEnd) {
		klog.V(4).Infof("Node %q under downsize delay until %v", node.Name, delayEnd)
		return
	}

	err = p.resizableVmManager.Downsize(nodeInfo.Node(), targetDesiredSize)
	if err != nil {
		klog.Infof("[%s resize] Enqueueing downsize for node %q failed: %v", resizableNode.MachineFamily, node.Name, err)
		return
	}

	klog.Infof("[%s resize] Downsize enqueued for node %q, removing from scaledown candidates.", resizableNode.MachineFamily, node.Name)

	if resizableNode.IsResizable() && resizableNode.IsSafelyUpsizable() {
		// Lookahead pods space is downsized, so it is not considered as part of desiredSize. We need to adjust BP pods to not exceed total node capacity.
		// We want to make sure that LA pod occupies the upsizable space first, keeping existing headroom on the Node.
		laRequest := allLookaheadPodsRequests(nodeInfo)
		targetDesiredSizeWithLAPods := size.Min(size.Add(targetDesiredSize, laRequest), resizableNode.UpsizableMaxSize)
		newDesiredSize = &targetDesiredSizeWithLAPods
	} else {
		// Downsizing a non-resizbale node, the node will already inlcude the LA pod size in its requestedResourcesMaxWindow and targetDesiredSize
		newDesiredSize = &targetDesiredSize
	}

	allowedForScaledown = false
	return
}

func (p *ScaleDownNodeProcessor) getNodesScaleDownAllowedFromCache(nodes []*apiv1.Node) map[string]bool {
	nodeNames := make([]string, len(nodes))
	for i, node := range nodes {
		nodeNames[i] = node.Name
	}
	return p.resizableVmManager.GetNodesScaleDownAllowedFromCache(nodeNames)
}
