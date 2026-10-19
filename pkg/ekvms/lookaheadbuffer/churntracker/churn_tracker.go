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

package churntracker

import (
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	ekvms_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

const (
	logPrefix = "[LA ChurnTracker]"
)

// nodeHistory is the per-resizable node record, keyed by node UID.
type nodeHistory struct {
	// creationTime is the creation timestamp of the node, used to measure the node lifetime.
	// It is set to the time the node is seen by the ChurnTracker for the first time if the node has no creation timestamp.
	creationTime time.Time
	// each resizable machine family has its own lookahead, so we need to store node's machine family as well.
	machineFamily string
	// everHostedLookahead indicates if the node ever had a virtual lookahead pod placed on it by the simulation.
	everHostedLookahead bool
	// everHadRunningCustomerPod indicates if the node ever had running user workload assigned to it.
	everHadRunningCustomerPod bool
	// lookaheadWorkloadID is the workload ID of the node, recorded when a lookahead pod is placed on it by the simulation.
	// It is extracted from the node in the same way as the lookahead pod injection does it for customer pods.
	lookaheadWorkloadID string
}

type workloadKey struct {
	machineFamily string
	workloadID    string
}

// churnEvents are the timestamps of churn events, tracked per workload separation of a machine family.
type churnEvents map[workloadKey][]time.Time

// churnMetrics is the subset of metrics emitted by the ChurnTracker and its BackoffManager.
type churnMetrics interface {
	RegisterLookaheadChurnEvent(machineFamily string)
	UpdateLookaheadBackedOffWorkloadSeparations(machineFamily string, tier, count int)
}

// ChurnTracker tracks node history and lookahead churn events per workload separation.
// It is not thread-safe: it is expected to be called from the main autoscaler loop (RunOnce) only.
type ChurnTracker struct {
	// configProvider provides the churn tracker configuration. The tracker is a no-op if it is nil or the experiment is disabled.
	configProvider ConfigProvider
	// mcp is used to filter out the nodes of non-resizable machine families, which are not tracked.
	mcp     *machinetypes.MachineConfigProvider
	metrics churnMetrics
	clock   clock.PassiveClock
	// backoffManager decides which workload separations have their lookahead backed off, based on the churn events.
	backoffManager *BackoffManager

	nodes  map[types.UID]*nodeHistory
	events churnEvents // churn events happened within the churnWindow
}

// NewChurnTracker returns a new initialized ChurnTracker.
func NewChurnTracker(configProvider ConfigProvider, mcp *machinetypes.MachineConfigProvider, metrics churnMetrics, c clock.PassiveClock) *ChurnTracker {
	if c == nil {
		c = clock.RealClock{}
	}
	return &ChurnTracker{
		configProvider: configProvider,
		mcp:            mcp,
		metrics:        metrics,
		clock:          c,
		backoffManager: NewBackoffManager(c, configProvider, metrics),
		nodes:          make(map[types.UID]*nodeHistory),
		events:         make(churnEvents),
	}
}

// ProcessNodeInfos updates existing nodeHistory entries or creates new ones for newly seen nodes, and drops entries
// for nodes that have left the snapshot. Upcoming nodes and nodes of non-resizable machine families are skipped,
// so the caller can pass all the nodes from the cluster snapshot.
// It is expected to be called once per loop, it also resets the backoffs past ResetWindow.
// It is a no-op if the churn circuit breaker experiment is disabled.
func (t *ChurnTracker) ProcessNodeInfos(nodeInfos []*framework.NodeInfo) {
	if !t.isEnabled() {
		return
	}

	now := t.clock.Now()
	updatedNodes := make(map[types.UID]*nodeHistory, len(nodeInfos))

	for _, ni := range nodeInfos {
		if t.shouldSkipNode(ni) {
			continue
		}
		laWorkloadID, hasLookahead := lookaheadWorkloadID(ni)
		hasRunningCustomerPod := ekvms_utils.HasRunningCustomerPods(ni)

		uid := ni.Node().UID
		history, exists := t.nodes[uid]
		if !exists {
			creationTime := ni.Node().CreationTimestamp.Time
			// The creation timestamp is always set by the API server, fall back to the current time just in case.
			if creationTime.IsZero() {
				creationTime = now
			}
			machineFamily, err := ekvms_utils.GetMachineFamilyName(ni.Node())
			if err != nil {
				klog.Errorf("Cannot retrieve the machine family of node %q: %v", ni.Node().Name, err)
				continue
			}
			history = &nodeHistory{creationTime: creationTime, machineFamily: machineFamily}
		}
		history.everHostedLookahead = history.everHostedLookahead || hasLookahead
		history.everHadRunningCustomerPod = history.everHadRunningCustomerPod || hasRunningCustomerPod
		if hasLookahead {
			history.lookaheadWorkloadID = laWorkloadID
		}
		updatedNodes[uid] = history
	}
	t.nodes = updatedNodes

	t.cleanExpiredEvents(now)
	t.backoffManager.ResetBackoffs(t.events, now)
}

// RecordScaledDownNodes records a churn event for each scaled-down node and checks if it qualifies as churn event.
// It is a no-op if the churn circuit breaker experiment is disabled.
func (t *ChurnTracker) RecordScaledDownNodes(nodes []*apiv1.Node) {
	if !t.isEnabled() {
		return
	}

	now := t.clock.Now()
	t.cleanExpiredEvents(now)
	for _, node := range nodes {
		if node == nil {
			continue
		}
		history, found := t.nodes[node.UID]
		if !found || !t.isLookaheadChurn(history, now) {
			continue
		}
		key := workloadKey{machineFamily: history.machineFamily, workloadID: history.lookaheadWorkloadID}
		t.events[key] = append(t.events[key], now)
		if t.metrics != nil {
			t.metrics.RegisterLookaheadChurnEvent(history.machineFamily)
		}
		klog.V(4).Infof("%s: Recorded lookahead churn event for workload ID %q: node %q of machine family %q without running customer pods was scaled down after %v", logPrefix, history.lookaheadWorkloadID, node.Name, history.machineFamily, now.Sub(history.creationTime))
	}
	// UpdateEvents will reset and apply/update backoffs if needed.
	t.backoffManager.UpdateEvents(t.events, now)
}

// IsBackedOff returns true if the lookahead of the workload separation of the machine family is backed off.
// It always returns false if the churn circuit breaker experiment is disabled.
func (t *ChurnTracker) IsBackedOff(machineFamily, workloadID string) bool {
	return t.backoffManager.IsBackedOff(machineFamily, workloadID)
}

// cleanExpiredEvents removes churn events older than churnWindow before now.
func (t *ChurnTracker) cleanExpiredEvents(now time.Time) {
	windowStart := now.Add(-t.configProvider.ChurnWindow())
	for key, eventsForWorkloadKey := range t.events {
		// Since eventsForWorkloadKey correspond to the time of when we record the churn events, they will always be in ascending order. So it is safe to copy everything after the first non-stale event.
		startIndex := indexOfFirstEventAfterTs(eventsForWorkloadKey, windowStart)
		validTs := eventsForWorkloadKey[startIndex:]
		if len(validTs) == 0 {
			delete(t.events, key)
		} else {
			t.events[key] = validTs
		}
	}
}

// isEnabled indicates if the churn circuit breaker experiment is enabled.
func (t *ChurnTracker) isEnabled() bool {
	return t.configProvider != nil && t.configProvider.IsEnabled()
}

// isLookaheadChurn indicates if the removal of the node should be considered a lookahead churn event.
func (t *ChurnTracker) isLookaheadChurn(history *nodeHistory, now time.Time) bool {
	return history.everHostedLookahead && !history.everHadRunningCustomerPod && now.Sub(history.creationTime) < t.configProvider.ChurnLifetimeThreshold()
}

// shouldSkipNode indicates if the node is not interesting for the churn tracker and if it should be skipped.
// Upcoming nodes and nodes of non-resizable machine families are not tracked.
func (t *ChurnTracker) shouldSkipNode(ni *framework.NodeInfo) bool {
	if ni == nil || ni.Node() == nil || ni.Node().UID == "" || utils.IsNodeInfoUpcoming(ni) || t.mcp == nil {
		return true
	}
	isResizable, err := ekvms_utils.IsResizableNode(ni.Node(), t.mcp)
	return err != nil || !isResizable
}

// lookaheadWorkloadID returns the workload ID of the node and whether the node hosts any lookahead pods.
// The workload ID is extracted from the node taints in the same way as the lookahead pod injection does it,
// so it matches the workload ID the lookahead pods were created for.
func lookaheadWorkloadID(ni *framework.NodeInfo) (string, bool) {
	for _, podInfo := range ni.Pods() {
		if lookaheadbuffer.IsLookaheadPod(podInfo.Pod) {
			return podrequirements.ExtractWorkloadID(ni.Node()), true
		}
	}
	return "", false
}

// indexOfFirstEventAfterTs return that index of the first eventTimestamp that is at or after ts. Otherwise it returns the length of an input array.
func indexOfFirstEventAfterTs(eventTimestamps []time.Time, ts time.Time) int {
	for i, eventTimestamp := range eventTimestamps {
		if !eventTimestamp.Before(ts) {
			return i
		}
	}
	return len(eventTimestamps)
}

// countEventsAfterTs returns the number of the event timestamps that happened at or after ts.
func countEventsAfterTs(eventTimestamps []time.Time, ts time.Time) int {
	return len(eventTimestamps) - indexOfFirstEventAfterTs(eventTimestamps, ts)
}

// countEventsBetween returns the number of the sorted timestamps within [from, to).
// Note: timestamps should be sorted in ascending order and to >= from.
func countEventsBetween(eventTimestamps []time.Time, from, to time.Time) int {
	return indexOfFirstEventAfterTs(eventTimestamps, to) - indexOfFirstEventAfterTs(eventTimestamps, from)
}
