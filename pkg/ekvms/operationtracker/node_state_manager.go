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
	"fmt"
	"maps"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/backoff"
	resize_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodesizerecommender"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	nsmLogPrefix            = "nodeStateManager:"
	snapshotLogInterval     = 1 * time.Minute
	snapshotTTL             = 5 * time.Second
	maxSizeRecomLogInterval = 15 * time.Second
)

type ResizableNodesSnapshot map[string]ResizableNode

type ResizableNode struct {
	DesiredSize       size.Allocatable
	UpsizableMaxSize  size.Allocatable
	PhysicalMaxSize   size.Allocatable
	LastOperationTime time.Time
	Node              *v1.Node
	MachineFamily     string
}

type SnapshotFilterMode int

const (
	ResizableOnly SnapshotFilterMode = iota
	DownsizableOnly
	AllNodes
)

func (m SnapshotFilterMode) String() string {
	switch m {
	case ResizableOnly:
		return "ResizableOnly"
	case DownsizableOnly:
		return "DownsizableOnly"
	case AllNodes:
		return "AllNodes"
	default:
		return fmt.Sprintf("Unknown(%d)", int(m))
	}
}

// IsResizable determines whether node is resizable.
// When VM is non-resizable, there should be max VM size recommendation {0,0}.
// This translates to less than or equal to zero allocatable.
// See go/gke-ek-handle-spillover.
func (n *ResizableNode) IsResizable() bool {
	return n.UpsizableMaxSize.MilliCpus > 0 || n.UpsizableMaxSize.KBytes > 0
}

// IsSafelyUpsizable ensures a recommended resize is not a downgrade.
// It returns true only if both the UpsizableMaxSize CPU and memory meet or exceed the DesiredSize values.
func (n *ResizableNode) IsSafelyUpsizable() bool {
	return n.UpsizableMaxSize.MilliCpus >= n.DesiredSize.MilliCpus && n.UpsizableMaxSize.KBytes >= n.DesiredSize.KBytes
}

func (n ResizableNode) String() string {
	return fmt.Sprintf("ResizableNode{Name: %s, DesiredSize: %v, UpsizableMaxSize: %v, PhysicalMaxSize: %v, LastOperationTime: %v}", n.Node.Name, n.DesiredSize, n.UpsizableMaxSize, n.PhysicalMaxSize, n.LastOperationTime)
}

type nodeStateManager interface {
	snapshot(forceRefresh bool) ResizableNodesSnapshot
	filteredNodesSnapshot(forceRefresh bool, mode SnapshotFilterMode) ResizableNodesSnapshot
	getNode(string) (ResizableNode, bool)
	setNode(string, ResizableNode)
	setNodeSize(node *v1.Node, newSize size.Allocatable)
	getUnhealthyNodesWithStatus(status UnhealthyResizableNodeStatus) []string
	deleteNode(string)
	nodesCount(machineFamily string) int
	setNodeAsUnhealthy(nodeName string, status UnhealthyResizableNodeStatus)
	setNodeAsHealthy(nodeName string)
	isUnhealthy(nodeName string) bool
	backoff(node *v1.Node, err resize_errors.ResizeError)
	isResizingEnabled(family string) bool
	resizableFamilyNames() []string
}

type UnhealthyResizableNodeStatus string

const (
	// UnknownResizeStatus is for the nodes that have unknown resize state.
	UnknownResizeStatus UnhealthyResizableNodeStatus = "unknown"
	// FailedResizeStatus is for the nodes that are unresizable, and either unfixable or have exhausted all fix attempts and should be replaced by defrag.
	FailedResizeStatus UnhealthyResizableNodeStatus = "failed"
)

// ResizingProvider is an interface that provides information about resizing.
type ResizingProvider interface {
	ResizingEnabled(machineFamily string) bool
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// nodeStateManagerImpl manages the state of resizable nodes
type nodeStateManagerImpl struct {
	nodeSizeRecommender nodesizerecommender.NodeSizeRecommender
	backoffManager      backoff.Manager
	sizeCalculator      calculator.Calculator
	clock               clock.Clock
	lastSnapshotLog     time.Time
	provider            ResizingProvider

	mux                 sync.Mutex
	nodes               ResizableNodesSnapshot
	snapshotLastUpdated time.Time
	unhealthyNodes      map[string]UnhealthyResizableNodeStatus
	nodesWithNoRecom    map[string]time.Time
	nodeCountByFamily   map[string]int
}

func NewNodeStateManager(provider ResizingProvider, nodeSizeRecommender nodesizerecommender.NodeSizeRecommender, backoffManager backoff.Manager, sizeCalculator calculator.Calculator, clock clock.Clock) *nodeStateManagerImpl {
	return &nodeStateManagerImpl{
		nodeSizeRecommender: nodeSizeRecommender,
		backoffManager:      backoffManager,
		sizeCalculator:      sizeCalculator,
		clock:               clock,
		provider:            provider,
		nodes:               make(ResizableNodesSnapshot),
		snapshotLastUpdated: time.Time{},
		unhealthyNodes:      make(map[string]UnhealthyResizableNodeStatus),
		nodesWithNoRecom:    make(map[string]time.Time),
		nodeCountByFamily:   make(map[string]int),
	}
}

func (m *nodeStateManagerImpl) isSnapshotFresh() bool {
	return m.clock.Now().Before(m.snapshotLastUpdated.Add(snapshotTTL))
}

func (m *nodeStateManagerImpl) updateSnapshot() {
	for nodeName, node := range m.nodes {
		node.UpsizableMaxSize = m.maxRecommendedNodeSize(node)
		m.nodes[nodeName] = node
	}
	m.snapshotLastUpdated = m.clock.Now()
}

// snapshot returns a copy of the resizable nodes snapshot.
func (m *nodeStateManagerImpl) snapshot(forceRefresh bool) ResizableNodesSnapshot {
	m.mux.Lock()
	defer m.mux.Unlock()

	// Refresh the snapshot if forced or stale by updating UpsizableMaxSize before cloning.
	if forceRefresh || !m.isSnapshotFresh() {
		m.updateSnapshot()
	}

	clone := make(ResizableNodesSnapshot, len(m.nodes))
	maps.Copy(clone, m.nodes)
	return clone
}

// filteredNodesSnapshot returns a copy of the snapshot filtered by resizability mode, health or backed-off status.
// ResizableOnly includes resizable nodes only.
// DownsizableOnly includes resizable and nonResizable nodes.
// AllNodes includes resizable, nonResizable, unhealthy and backed-off nodes.
func (m *nodeStateManagerImpl) filteredNodesSnapshot(forceRefresh bool, mode SnapshotFilterMode) ResizableNodesSnapshot {
	snapshot := m.snapshot(forceRefresh)
	unknownStateOrFailedNodes := make(map[string][]ResizableNode)
	backedOffNodes := make(map[string][]ResizableNode)
	nonResizableNodes := make(map[string][]ResizableNode)
	resizableNodes := make(map[string][]ResizableNode)
	for nodeName, node := range snapshot {
		if !m.isResizingEnabled(node.MachineFamily) {
			continue
		}
		if m.isUnhealthy(nodeName) {
			unknownStateOrFailedNodes[node.MachineFamily] = append(unknownStateOrFailedNodes[node.MachineFamily], node)
			continue
		}
		if m.backoffManager.IsBackedOff(node.MachineFamily, nodeName) {
			backedOffNodes[node.MachineFamily] = append(backedOffNodes[node.MachineFamily], node)
			continue
		}
		if !node.IsResizable() {
			nonResizableNodes[node.MachineFamily] = append(nonResizableNodes[node.MachineFamily], node)
			continue
		}
		resizableNodes[node.MachineFamily] = append(resizableNodes[node.MachineFamily], node)
	}
	if m.clock.Now().After(m.lastSnapshotLog.Add(snapshotLogInterval)) {
		omittedNodesLogger(unknownStateOrFailedNodes, "Nodes with unknown or failed state")
		omittedNodesLogger(backedOffNodes, "BackedOff nodes")
		omittedNodesLogger(nonResizableNodes, "Non-resizable nodes")
		omittedNodesLogger(resizableNodes, "Resizable nodes")
		klog.V(4).Infof("%s resizable nodes snapshot (mode: %s): %+v", nsmLogPrefix, mode, snapshot)
		m.lastSnapshotLog = m.clock.Now()
	}
	switch mode {
	case ResizableOnly:
		return createSnapshot(resizableNodes)
	case DownsizableOnly:
		return createSnapshot(resizableNodes, nonResizableNodes)
	}
	return snapshot
}

func createSnapshot(listsOfNodes ...map[string][]ResizableNode) ResizableNodesSnapshot {
	snapshot := make(ResizableNodesSnapshot)
	for _, familyMap := range listsOfNodes {
		for _, nodes := range familyMap {
			for _, node := range nodes {
				snapshot[node.Node.Name] = node
			}
		}
	}
	return snapshot
}

func omittedNodesLogger(familyToNodes map[string][]ResizableNode, nodeType string) {
	for family, nodes := range familyToNodes {
		nodeNames := make([]string, 0, len(nodes))
		for _, node := range nodes {
			nodeNames = append(nodeNames, node.Node.Name)
		}
		klog.V(4).Infof("%s [%s] %s: %s", nsmLogPrefix, family, nodeType, nodeNames)
	}
}

// getNode returns the ResizableNode for the given node name.
func (m *nodeStateManagerImpl) getNode(nodeName string) (ResizableNode, bool) {
	m.mux.Lock()
	defer m.mux.Unlock()
	node, exists := m.nodes[nodeName]
	if !exists {
		return node, false
	}
	node.UpsizableMaxSize = m.maxRecommendedNodeSize(node)
	return node, true
}

// setNode sets ResizableNode for the given node name.
func (m *nodeStateManagerImpl) setNode(nodeName string, node ResizableNode) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if _, exists := m.nodes[nodeName]; !exists {
		m.nodeCountByFamily[node.MachineFamily]++
	}
	m.nodes[nodeName] = node
}

// updateSnapshot updates the desired size of a node in the snapshot.
func (m *nodeStateManagerImpl) setNodeSize(node *v1.Node, newSize size.Allocatable) {
	n, exist := m.getNode(node.Name)
	if !exist {
		klog.Errorf("%s unable to update resizable nodes snapshot for unknown node %q", nsmLogPrefix, node.Name)
		return
	}
	n.DesiredSize = newSize
	n.LastOperationTime = m.clock.Now()
	m.setNode(node.Name, n)
	klog.V(4).Infof("%s update completed for node %q. New desired size: %+v", nsmLogPrefix, node.Name, newSize)
}

// deleteNode deletes the ResizableNode for the given node name.
func (m *nodeStateManagerImpl) deleteNode(nodeName string) {
	m.mux.Lock()
	defer m.mux.Unlock()

	if node, ok := m.nodes[nodeName]; ok {
		m.backoffManager.DeleteNode(node.MachineFamily, nodeName)
		m.nodeCountByFamily[node.MachineFamily]--
	}
	delete(m.nodes, nodeName)
	delete(m.unhealthyNodes, nodeName)
	delete(m.nodesWithNoRecom, nodeName)
}

// nodesCount returns the number of resizable nodes for the given machine family.
func (m *nodeStateManagerImpl) nodesCount(machineFamily string) int {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.nodeCountByFamily[machineFamily]
}

func (m *nodeStateManagerImpl) setNodeAsUnhealthy(nodeName string, status UnhealthyResizableNodeStatus) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.unhealthyNodes[nodeName] = status
}

func (m *nodeStateManagerImpl) setNodeAsHealthy(nodeName string) {
	m.mux.Lock()
	defer m.mux.Unlock()
	delete(m.unhealthyNodes, nodeName)
}

func (m *nodeStateManagerImpl) isUnhealthy(nodeName string) bool {
	m.mux.Lock()
	defer m.mux.Unlock()
	_, exists := m.unhealthyNodes[nodeName]
	return exists
}

func (m *nodeStateManagerImpl) getUnhealthyNodesWithStatus(status UnhealthyResizableNodeStatus) []string {
	m.mux.Lock()
	defer m.mux.Unlock()
	var nodes []string
	for nodeName := range m.unhealthyNodes {
		if _, exists := m.nodes[nodeName]; !exists {
			klog.Warningf("Failed to find unhealthy resizable node %q in resizable node snapshot", nodeName)
			continue
		}
		if m.unhealthyNodes[nodeName] != status {
			continue
		}
		nodes = append(nodes, nodeName)
	}
	return nodes
}

func (m *nodeStateManagerImpl) isResizingEnabled(family string) bool {
	return m.provider.ResizingEnabled(family)
}

func (m *nodeStateManagerImpl) resizableFamilyNames() []string {
	return m.provider.MachineConfigProvider().ResizableFamilyNames()
}

func (m *nodeStateManagerImpl) maxRecommendedNodeSize(node ResizableNode) size.Allocatable {
	maxSize := node.UpsizableMaxSize
	if m.nodeSizeRecommender == nil {
		return size.Allocatable{}
	}
	if !m.isResizingEnabled(node.MachineFamily) {
		return maxSize
	}
	recommendedMaxVmSize := m.nodeSizeRecommender.MaxSize(node.Node)
	if recommendedMaxVmSize == nil {
		// Do not report the missing recommendation for a node if the last report happened earlier than maxSizeRecomLogInterval.
		if m.clock.Now().After(m.nodesWithNoRecom[node.Node.Name].Add(maxSizeRecomLogInterval)) {
			m.nodesWithNoRecom[node.Node.Name] = m.clock.Now()
			klog.Warningf("%s max size recommendation missing for node %q", nsmLogPrefix, node.Node.Name)
		}
		return size.Allocatable{}
	}
	delete(m.nodesWithNoRecom, node.Node.Name)
	recommendedMaxAllocatable := m.sizeCalculator.ToAllocatable(node.Node, recommendedMaxVmSize.VmSize)
	maxSize.MilliCpus = min(node.PhysicalMaxSize.MilliCpus, recommendedMaxAllocatable.MilliCpus)
	maxSize.KBytes = min(node.PhysicalMaxSize.KBytes, recommendedMaxAllocatable.KBytes)
	return maxSize
}

func (m *nodeStateManagerImpl) backoff(node *v1.Node, err resize_errors.ResizeError) {
	m.backoffManager.Backoff(node, err)
}
