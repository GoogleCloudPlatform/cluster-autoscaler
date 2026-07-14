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

package operationtracker

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	ca_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// Manager tracks and resizes nodes.
type Manager interface {
	// Run initializes Manager, and starts OperationTracker.
	Run(ctx context.Context)
	// Upsize requests an upsize of a given resizable node, or returns an error
	// if the upsize operation cannot be queued up.
	Upsize(*v1.Node, size.Allocatable) error
	// Downsize requests a downsize of a given resizable node, or returns an error
	// if the downsize operation cannot be queued up.
	Downsize(*v1.Node, size.Allocatable) error
	// FilteredNodesSnapshot return a filtered copy of the latest resizable nodes state
	FilteredNodesSnapshot(forceRefresh bool, mode SnapshotFilterMode) ResizableNodesSnapshot
	// UnhealthyNodesWithStatus returns the nodes that are unresizable (with unknown resize state), and either unfixable or have exhausted all fix attempts and should be replaced by defrag.
	UnhealthyNodesWithStatus(status UnhealthyResizableNodeStatus) []string
	// IsResizingEnabled returns true if VM resizing is enabled.
	IsResizingEnabled(machineFamily string) bool
	// IsNodeInProcess returns if the node is in process of running an operation or not
	IsNodeInProcess(nodeName string) bool
	// IsNodeResizingOrPending returns if the node is enqueued for a resize operation or not
	IsNodeResizingOrPending(nodeName string) bool
	// GetNodesScaleDownAllowedFromCache retrieves the scale-down information for nodes from the cache.
	GetNodesScaleDownAllowedFromCache([]string) map[string]bool
	// UpdateNodesScaleDownAllowedCache updates the scale-down information for nodes in the cache.
	UpdateNodesScaleDownAllowedCache(map[string]bool)
	// InvalidateNodesScaleDownAllowed invalidates the cache storing information about whether nodes are allowed to be scaled down.
	InvalidateNodesScaleDownAllowedCache()
}

// CloudProvider is the subset of GkeCloudProvider needed for resizable VM manager.
type CloudProvider interface {
	NodeGroupForNode(node *v1.Node) (cloudprovider.NodeGroup, error)
	ResizingEnabled(machineFamily string) bool
	GetNodesScaleDownAllowedFromCache([]string) map[string]bool
	UpdateNodesScaleDownAllowedCache(map[string]bool)
	InvalidateNodesScaleDownAllowedCache()
}

type ManagerImpl struct {
	tracker          OperationTracker
	sizeCalculator   calculator.Calculator
	metricsExporter  metricsExporter
	cloudProvider    CloudProvider
	nodeStateManager nodeStateManager

	clock clock.PassiveClock
}

// NewManager creates a new instance of resizable VM manager with an empty snapshot.
func NewManager(cloudProvider CloudProvider, tracker OperationTracker, sizeCalculator calculator.Calculator,
	nodeSizeRecommender nodesizerecommender.NodeSizeRecommender, metrics periodicMetrics,
	nodeStateManager nodeStateManager) *ManagerImpl {
	managerClock := clock.RealClock{}
	return &ManagerImpl{
		tracker:          tracker,
		sizeCalculator:   sizeCalculator,
		cloudProvider:    cloudProvider,
		nodeStateManager: nodeStateManager,
		clock:            managerClock,
		metricsExporter:  newMetricsExporter(nodeStateManager, nodeSizeRecommender, metrics, managerClock),
	}
}

// Run initializes the resizable VM manager, populates snapshot, and starts OperationTracker.
// It blocks until ctx.Done() is closed.
func (m *ManagerImpl) Run(ctx context.Context) {
	go m.tracker.Run(ctx)
	go m.metricsExporter.run(ctx)
	<-ctx.Done()
}

// FilteredNodesSnapshot returns a copy of the snapshot filtered by resizability mode, excluding unhealthy or backed-off nodes.
func (m *ManagerImpl) FilteredNodesSnapshot(forceRefresh bool, mode SnapshotFilterMode) ResizableNodesSnapshot {
	return m.nodeStateManager.filteredNodesSnapshot(forceRefresh, mode)
}

// UnhealthyNodesWithStatus returns the nodes that are unresizable (with unknown resize state), and either unfixable or have exhausted all fix attempts and should be replaced by defrag.
func (m *ManagerImpl) UnhealthyNodesWithStatus(status UnhealthyResizableNodeStatus) []string {
	if m == nil || m.nodeStateManager == nil {
		return nil
	}
	return m.nodeStateManager.getUnhealthyNodesWithStatus(status)
}

// Upsize validates node upsize request and sends it to OperationTracker for execution.
// It returns an error if snapshot does not contain given node,
// or if downsize was requested using Upsize function.
func (m *ManagerImpl) Upsize(node *v1.Node, proposedSize size.Allocatable) error {
	resizableNode, exists := m.nodeStateManager.getNode(node.Name)
	if !exists {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "node %q is not present in resizable node snapshot", node.Name)
	}
	if !resizableNode.IsResizable() {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "node %q is non-resizable", node.Name)
	}

	klog.V(4).Infof("[%s resize] Upsize: received request for node %q, proposed size %+v", resizableNode.MachineFamily, node.Name, proposedSize)

	currentSize := resizableNode.DesiredSize
	if !proposedSize.IsUpsizeFrom(currentSize) {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "proposed size %+v for node %q is not an upsize from current size %+v", proposedSize, node.Name, currentSize)
	}

	if proposedSize.MilliCpus > resizableNode.UpsizableMaxSize.MilliCpus || proposedSize.KBytes > resizableNode.UpsizableMaxSize.KBytes {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "node %q proposed size %+v exceeds maximum possible size %+v", node.Name, proposedSize, resizableNode.UpsizableMaxSize)
	}
	if proposedSize == currentSize {
		klog.V(1).Infof("[%s resize] Upsize skipped: node %q proposed size and current size are the same %+v, no need to resize", resizableNode.MachineFamily, node.Name, currentSize)
		return nil
	}

	startingSize, err := m.sizeCalculator.ToVmSize(node, currentSize)
	if err != nil {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "failed to get starting VM size for node %q, error: %v", node.Name, err)
	}
	desiredSize, err := m.sizeCalculator.ToVmSize(node, proposedSize)
	if err != nil {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "failed to get desired VM size for node %q, error: %v", node.Name, err)
	}
	actuatedSize := m.sizeCalculator.ToAllocatable(node, desiredSize)

	op := ResizeOperation{
		NodeName:     node.Name,
		StartingSize: startingSize,
		DesiredSize:  desiredSize,
	}

	m.nodeStateManager.setNodeSize(node, actuatedSize)
	m.tracker.Resize(op)

	klog.V(4).Infof("[%s resize] Upsize: requested for node %q from %+v to %+v", resizableNode.MachineFamily, node.Name, startingSize, desiredSize)

	return nil
}

// Downsize validates node downsize request, and sends it to OperationTracker for execution.
// It updates the request to comply with VM size requirements before invoking OperationTracker.
// It returns an error if snapshot does not contain given node,
// or if an upsize was requested using Downsize function.
func (m *ManagerImpl) Downsize(node *v1.Node, proposedSize size.Allocatable) error {
	resizableNode, exists := m.nodeStateManager.getNode(node.Name)
	if !exists {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "node %q is not present in resizable node snapshot", node.Name)
	}

	klog.V(4).Infof("[%s resize] Downsize request received for node %q to size %+v", resizableNode.MachineFamily, node.Name, proposedSize)

	currentSize := resizableNode.DesiredSize
	startingSize, err := m.sizeCalculator.ToVmSize(node, currentSize)
	if err != nil {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "failed to get starting VM size for node %q, error: %v", node.Name, err)
	}
	desiredSize, err := m.sizeCalculator.ToVmSize(node, proposedSize)
	if err != nil {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "failed to get desired VM size for node %q, error: %v", node.Name, err)
	}
	actuatedSize := m.sizeCalculator.ToAllocatable(node, desiredSize)

	// Valid resize request must be in the interval [minSize <= size <= desiredSize] in both dimensions.
	// minSize is derived from valid VM sizes, it is greater than 0.
	if !proposedSize.IsDownsizeFrom(currentSize) {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "proposed size %+v for node %q is not a downsize from current size %+v", actuatedSize, node.Name, currentSize)
	}

	if actuatedSize == currentSize {
		klog.V(1).Infof("[%s resize] Downsize skipped: node %q proposed size and current size are the same %+v, no need to resize", resizableNode.MachineFamily, node.Name, currentSize)
		return nil
	}

	// TODO: What should happen when currentSize < MinAllocatable(node)?
	minAllocatable, err := m.sizeCalculator.MinAllocatable(node)
	if err != nil {
		return ca_errors.NewAutoscalerErrorf(ca_errors.InternalError, "failed to get min allocatable VM size for node %q, error: %v", node.Name, err)
	}
	if currentSize == minAllocatable {
		klog.V(1).Infof("[%s resize] Downsize skipped: node %q is at min possible size %+v", resizableNode.MachineFamily, node.Name, currentSize)
		return nil
	}

	op := ResizeOperation{
		NodeName:     node.Name,
		StartingSize: startingSize,
		DesiredSize:  desiredSize,
	}

	m.nodeStateManager.setNodeSize(node, actuatedSize)
	m.tracker.Resize(op)

	klog.V(4).Infof("[%s resize] Downsize requested for node %q from %+v to %+v", resizableNode.MachineFamily, node.Name, currentSize, actuatedSize)

	return nil
}

func (m *ManagerImpl) IsResizingEnabled(machineFamily string) bool {
	return m.cloudProvider.ResizingEnabled(machineFamily)
}

func (m *ManagerImpl) IsNodeInProcess(nodeName string) bool {
	return m.tracker.IsNodeInProcess(nodeName)
}

func (m *ManagerImpl) IsNodeResizingOrPending(nodeName string) bool {
	return m.tracker.IsNodeResizingOrPending(nodeName)
}

// GetNodesScaleDownAllowedFromCache retrieves the scale-down information for nodes from the cache.
func (m *ManagerImpl) GetNodesScaleDownAllowedFromCache(nodeNames []string) map[string]bool {
	return m.cloudProvider.GetNodesScaleDownAllowedFromCache(nodeNames)
}

// UpdateNodesScaleDownAllowedCache updates the scale-down information for nodes in the cache.
func (m *ManagerImpl) UpdateNodesScaleDownAllowedCache(nodesScaleDownAllowed map[string]bool) {
	m.cloudProvider.UpdateNodesScaleDownAllowedCache(nodesScaleDownAllowed)
}

// InvalidateNodesScaleDownAllowed invalidates the cache storing information about whether nodes are allowed to be scaled down.
func (m *ManagerImpl) InvalidateNodesScaleDownAllowedCache() {
	m.cloudProvider.InvalidateNodesScaleDownAllowedCache()
}

// NodesCount returns the number of resizable nodes for a given machine family.
func (m *ManagerImpl) NodesCount(machineFamily string) int {
	return m.nodeStateManager.nodesCount(machineFamily)
}
