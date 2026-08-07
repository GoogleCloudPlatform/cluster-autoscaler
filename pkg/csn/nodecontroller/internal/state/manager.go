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

package state

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/set"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
)

const (
	logPrefix = "CSN Node State Manager:"
)

// CloudProvider allows retrieval of the GKE MIG for a node.
type CloudProvider interface {
	GkeMigForNode(node *v1.Node) (*gke.GkeMig, error)
}

// NodeStateManager is responsible for tracking the state of CSN nodes.
// It exports methods which allow to read this state and perform certain updates
// on it.
type NodeStateManager struct {
	clock               clock.WithTicker
	registerNodeHandler RegisterNodeHandler
	nodeMutex           sync.RWMutex
	// nodeName -> TrackedNode
	trackedNodes            map[string]*TrackedNode
	nodeNamesToStopTracking map[string]time.Time

	eventHandlers []EventHandler

	stopTrackingDelay       time.Duration
	metricsSyncInterval     time.Duration
	experimentsSyncInterval time.Duration
	backoffCleanupInterval  time.Duration

	backoffEnabled     bool
	backoff            base_backoff.Backoff
	cloudProvider      CloudProvider
	experimentsManager experiments.Manager
}

// Option configures the NodeStateManager.
type Option func(*NodeStateManager)

func WithEventHandler(eh EventHandler) Option {
	return func(m *NodeStateManager) {
		m.eventHandlers = append(m.eventHandlers, eh)
	}
}

func WithClock(clock clock.WithTicker) Option {
	return func(m *NodeStateManager) {
		m.clock = clock
	}
}

func WithMetricsSyncInterval(interval time.Duration) Option {
	return func(m *NodeStateManager) {
		if interval > 0 {
			m.metricsSyncInterval = interval
		}
	}
}

func WithExperimentsSyncInterval(interval time.Duration) Option {
	return func(m *NodeStateManager) {
		if interval > 0 {
			m.experimentsSyncInterval = interval
		}
	}
}

func WithBackoffCleanupInterval(interval time.Duration) Option {
	return func(m *NodeStateManager) {
		if interval > 0 {
			m.backoffCleanupInterval = interval
		}
	}
}

func WithStopTrackingDelay(delay time.Duration) Option {
	return func(m *NodeStateManager) {
		if delay > 0 {
			m.stopTrackingDelay = delay
		}
	}
}

func WithBackoff(backoff base_backoff.Backoff) Option {
	return func(m *NodeStateManager) {
		m.backoff = backoff
	}
}

func WithCloudProvider(cp CloudProvider) Option {
	return func(m *NodeStateManager) {
		m.cloudProvider = cp
	}
}

func WithExperimentsManager(em experiments.Manager) Option {
	return func(m *NodeStateManager) {
		m.experimentsManager = em
	}
}

// NewNodeStateManager returns a new instance of a node state manager.
func NewNodeStateManager(nodeSource RegisterNodeHandler, opts ...Option) *NodeStateManager {
	m := &NodeStateManager{
		clock:                   clock.RealClock{},
		trackedNodes:            make(map[string]*TrackedNode),
		nodeNamesToStopTracking: make(map[string]time.Time),
		registerNodeHandler:     nodeSource,
		eventHandlers:           []EventHandler{newMetricsEventHandler()},
		// Default values, can be overridden with options.
		stopTrackingDelay:       10 * time.Minute,
		metricsSyncInterval:     5 * time.Second,
		experimentsSyncInterval: 10 * time.Minute, // Giraffe values are refreshed inside experiments package every 15 minutes anyway.
		backoffCleanupInterval:  10 * time.Minute,
	}

	for _, opt := range opts {
		opt(m)
	}

	m.updateBackoffEnabled()
	return m
}

func (m *NodeStateManager) IsBackoffEnabled() bool {
	return m.backoffEnabled
}

// UpdateBackoffStatus updates the IsBackedOff status for all suspended tracked nodes.
func (m *NodeStateManager) UpdateBackoffStatus() {
	m.nodeMutex.Lock()
	defer m.nodeMutex.Unlock()

	for _, tn := range m.trackedNodes {
		if tn == nil {
			continue
		}
		if tn.State == csn.NodeStateSuspended || tn.State == csn.NodeStateChilling {
			tn.IsBackedOff = m.isNodeBackedOff(tn.Node)
		} else {
			tn.IsBackedOff = false
		}
	}
}

func (m *NodeStateManager) isNodeBackedOff(node *v1.Node) bool {
	if m.backoff == nil || m.cloudProvider == nil || !m.backoffEnabled {
		return false
	}
	mig, err := m.cloudProvider.GkeMigForNode(node)
	if err != nil || mig == nil {
		return false
	}
	return m.backoff.BackoffStatus(mig, framework.NewNodeInfo(node, nil), m.clock.Now()).IsBackedOff
}

// WithoutBackedOffSuspendedFilter excludes nodes that are suspended and backed-off.
func (m *NodeStateManager) WithoutBackedOffSuspendedFilter(tn *TrackedNode) bool {
	if tn == nil {
		return true
	}
	return tn.State != csn.NodeStateSuspended || !tn.IsBackedOff
}

func (m *NodeStateManager) Run(ctx context.Context) error {
	if err := m.registerNodeHandler(NodeHandler{
		OnAdd:    m.onAdd,
		OnUpdate: m.onUpdate,
		OnDelete: m.onDelete,
	}); err != nil {
		return fmt.Errorf("failed to register node handler: %w", err)
	}

	m.runPeriodically(ctx, m.stopTrackingDelay, m.stopTrackingExpiredNodes)
	m.runPeriodically(ctx, m.metricsSyncInterval, m.emitNodeCounts)
	m.runPeriodically(ctx, m.experimentsSyncInterval, m.updateBackoffEnabled)
	m.runPeriodically(ctx, m.backoffCleanupInterval, func() {
		if m.backoff != nil {
			m.backoff.RemoveStaleBackoffData(time.Now())
		}
	})
	return nil
}

func (m *NodeStateManager) updateBackoffEnabled() {
	m.nodeMutex.Lock()
	defer m.nodeMutex.Unlock()
	if m.experimentsManager == nil {
		m.backoffEnabled = false
		return
	}
	m.backoffEnabled = m.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ColdStandbyNodesBackoffMinCAVersionFlag, false)
}

func (m *NodeStateManager) runPeriodically(ctx context.Context, interval time.Duration, f func()) {
	ticker := m.clock.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C():
				f()
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (m *NodeStateManager) stopTrackingExpiredNodes() {
	var expiredNodeNames []string

	m.nodeMutex.RLock()
	for nodeName, untrackedTime := range m.nodeNamesToStopTracking {
		if m.clock.Now().After(untrackedTime.Add(m.stopTrackingDelay)) {
			expiredNodeNames = append(expiredNodeNames, nodeName)
		}
	}
	m.nodeMutex.RUnlock()

	if len(expiredNodeNames) > 0 {
		m.stopTrackingNodes(expiredNodeNames...)
	}
}

// Get returns a node if it's currently tracked by the NodeStateManager.
// The second return value is false if no node with a given name is found.
func (m *NodeStateManager) Get(nodeName string) (TrackedNode, bool) {
	m.nodeMutex.RLock()
	defer m.nodeMutex.RUnlock()
	tn, ok := m.trackedNodes[nodeName]
	if !ok || tn == nil {
		return TrackedNode{}, false
	}
	return *tn, ok
}

// List returns references to all nodes currently tracked
// by the NodeStateManager.
func (m *NodeStateManager) List(filters ...NodeFilter) []TrackedNode {
	m.nodeMutex.RLock()
	defer m.nodeMutex.RUnlock()

	var result []TrackedNode
	for _, tn := range m.trackedNodes {
		if tn != nil && m.allFiltersPass(tn, filters...) {
			result = append(result, *tn)
		}
	}
	return result
}

func (m *NodeStateManager) allFiltersPass(n *TrackedNode, filters ...NodeFilter) bool {
	for _, filter := range filters {
		if filter != nil && !filter(n) {
			return false
		}
	}
	return true
}

// SetPendingOperation returns a {nodeName->error} map to indicate
// partial failures for setting the status of pending operations.
// Consider the call fully successful if this map is empty.
func (m *NodeStateManager) SetPendingOperation(op ops.OperationType, pending bool, nodeNames set.Set[string], opts ...PendingOperationOpt) map[string]error {
	m.nodeMutex.Lock()
	defer m.nodeMutex.Unlock()

	errs := make(map[string]error)
	for nodeName := range nodeNames {
		tn, ok := m.trackedNodes[nodeName]
		if !ok {
			errs[nodeName] = fmt.Errorf("node %q not found", nodeName)
			continue
		}
		if pending && slices.Contains(opts, ExclusiveOp) && tn.PendingOperations != ops.NoOp {
			errs[nodeName] = fmt.Errorf("node %q already has an operation and %q opt was passed", nodeName, ExclusiveOp)
			continue
		}
		if pending {
			tn.PendingOperations = tn.PendingOperations.With(op)
			m.setDesiredState(op, tn)
		} else {
			tn.PendingOperations = tn.PendingOperations.Without(op)
		}
	}
	return errs
}

func (m *NodeStateManager) setDesiredState(opType ops.OperationType, tn *TrackedNode) {
	switch opType {
	case ops.SuspendOp:
		tn.DesiredState = csn.NodeStateSuspended
	case ops.ConsumeOp:
		tn.DesiredState = csn.NodeStateConsumed
	}
}

// AssignBuffers updates the internal information of tracked nodes
// with pointers to their buffers.
func (m *NodeStateManager) AssignBuffers(nodeNameToBuffer map[string]*v1beta1.CapacityBuffer) {
	m.nodeMutex.Lock()
	defer m.nodeMutex.Unlock()
	for nodeName, buffer := range nodeNameToBuffer {
		if tn, ok := m.trackedNodes[nodeName]; ok {
			tn.Buffer = buffer
		}
	}
}

func (m *NodeStateManager) GetAssignedBuffers(nodeNames ...string) map[string]*v1beta1.CapacityBuffer {
	m.nodeMutex.RLock()
	defer m.nodeMutex.RUnlock()
	nameToBuffer := make(map[string]*v1beta1.CapacityBuffer)

	for _, name := range nodeNames {
		if tn, ok := m.trackedNodes[name]; ok && tn.Buffer != nil {
			nameToBuffer[name] = tn.Buffer
		}
	}

	return nameToBuffer
}

func (m *NodeStateManager) onAdd(n *v1.Node) {
	state := csn.ClassifyNode(n)
	defer m.emitEvent(NodeAdded{Node: n, State: state})

	// Return if not a CSN node.
	if !csn.IsCSNNode(n) {
		return
	}

	m.nodeMutex.Lock()
	defer m.nodeMutex.Unlock()
	m.trackedNodes[n.Name] = &TrackedNode{
		Node:  n,
		State: state,
	}
}

func (m *NodeStateManager) onUpdate(n *v1.Node) {
	newState := csn.ClassifyNode(n)
	isCSN, status := csn.IsCSNNode(n), m.trackingStatus(n)
	defer m.emitEvent(NodeUpdated{Node: n, OldState: status.State, NewState: newState})
	if status.TrackingToStop {
		// Tracking was supposed to stop, we don't revert it.
		return
	}
	if !isCSN && !status.Tracked {
		// don't care about a non-CSN untracked node.
		return
	}

	m.nodeMutex.Lock()
	defer m.nodeMutex.Unlock()

	nodeName := n.Name
	tn, ok := m.trackedNodes[nodeName]
	if !ok {
		klog.Warningf("%s CSN node %q created %q ago made its first appearance via an update, which is unexpected", logPrefix, nodeName, m.clock.Now().Sub(n.CreationTimestamp.Time).String())
		m.trackedNodes[nodeName] = &TrackedNode{
			Node:  n,
			State: newState,
		}
		return
	}
	tn.State = newState
	tn.Node = n

	if !isCSN {
		// If the node is no longer a CSN node, stop tracking it.
		// Deletion is delayed so the callers of NodeStateManager
		// can verify that the node has been consumed via List or Get for some time.
		m.nodeNamesToStopTracking[nodeName] = m.clock.Now()
	}
}

func (m *NodeStateManager) onDelete(n *v1.Node) {
	defer m.emitEvent(NodeDeleted{Node: n, State: csn.ClassifyNode(n)})
	m.stopTrackingNodes(n.Name)
}

func (m *NodeStateManager) stopTrackingNodes(nodeNames ...string) {
	m.nodeMutex.Lock()
	defer m.nodeMutex.Unlock()
	for _, n := range nodeNames {
		tn, ok := m.trackedNodes[n]
		if !ok {
			continue
		}
		delete(m.trackedNodes, n)
		delete(m.nodeNamesToStopTracking, n)
		m.emitEvent(NodeUntracked{Node: tn.Node})
	}
}

func (m *NodeStateManager) emitNodeCounts() {
	m.nodeMutex.RLock()
	counts := make(map[csn.NodeState]int)
	for _, tn := range m.trackedNodes {
		if tn != nil {
			counts[tn.State]++
		}
	}
	m.nodeMutex.RUnlock()
	m.emitEvent(NodeCounts{Counts: counts})
}

type trackingStatus struct {
	Tracked        bool
	TrackingToStop bool
	State          csn.NodeState
}

func (m *NodeStateManager) trackingStatus(n *v1.Node) trackingStatus {
	m.nodeMutex.RLock()
	defer m.nodeMutex.RUnlock()

	tn, tracked := m.trackedNodes[n.Name]
	_, trackingToStop := m.nodeNamesToStopTracking[n.Name]
	status := trackingStatus{
		Tracked:        tracked,
		TrackingToStop: trackingToStop,
		State:          csn.NodeStateConsumed,
	}
	if tracked {
		status.State = tn.State
	}
	return status
}

func (m *NodeStateManager) emitEvent(e NodeEvent) {
	for _, f := range m.eventHandlers {
		f(e)
	}
}
