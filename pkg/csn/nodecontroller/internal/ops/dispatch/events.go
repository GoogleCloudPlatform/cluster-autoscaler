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

package dispatch

import (
	"fmt"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/utils/set"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/klogx"
)

const (
	// StandbyBufferNodesSuspended is the event reason emitted on a CapacityBuffer when standby nodes are successfully suspended.
	StandbyBufferNodesSuspended = "StandbyBufferNodesSuspended"
	// StandbyBufferNodesConsumed is the event reason emitted on a CapacityBuffer when standby nodes are successfully consumed.
	StandbyBufferNodesConsumed = "StandbyBufferNodesConsumed"
	// StandbyBufferSuspendFailed is the event reason emitted on a CapacityBuffer when suspending standby nodes permanently fails.
	StandbyBufferSuspendFailed = "StandbyBufferSuspendFailed"
	// StandbyBufferConsumeFailed is the event reason emitted on a CapacityBuffer when consuming standby nodes permanently fails.
	StandbyBufferConsumeFailed = "StandbyBufferConsumeFailed"

	maxEventNodeNames = 5
)

// StateManager provides access to the CapacityBuffer assignments of tracked CSN nodes.
type StateManager interface {
	GetAssignedBuffers(nodeNames ...string) map[string]*v1beta1.CapacityBuffer
}

// EventEmitter emits Kubernetes events on CapacityBuffers when CSN node operations
// succeed or permanently fail.
type EventEmitter struct {
	recorder           kube_record.EventRecorder
	stateManager       StateManager
	experimentsManager experiments.Manager
}

// NewEventEmitter creates a new EventEmitter.
func NewEventEmitter(recorder kube_record.EventRecorder, sm StateManager, em experiments.Manager) *EventEmitter {
	return &EventEmitter{
		recorder:           recorder,
		stateManager:       sm,
		experimentsManager: em,
	}
}

func (e *EventEmitter) isEnabled(nodeNames set.Set[string]) bool {
	return e != nil &&
		len(nodeNames) > 0 &&
		e.recorder != nil &&
		e.stateManager != nil &&
		e.experimentsManager != nil &&
		e.experimentsManager.DirectLaunchBoolFlag(experiments.ColdStandbyNodesEmitNodeControllerEventsFlag)
}

func (e *EventEmitter) emitSuccess(opType ops.OperationType, nodeNames set.Set[string]) {
	if !e.isEnabled(nodeNames) {
		return
	}
	var reason string
	switch opType {
	case ops.SuspendOp:
		reason = StandbyBufferNodesSuspended
	case ops.ConsumeOp:
		reason = StandbyBufferNodesConsumed
	default:
		return
	}
	for _, group := range e.groupNodesByBuffer(nodeNames) {
		e.recorder.Event(group.buffer, v1.EventTypeNormal, reason, formatSuccessMessage(opType, group.nodes))
	}
}

func (e *EventEmitter) emitFailure(opType ops.OperationType, failedNodes set.Set[string], errs map[string]error) {
	if !e.isEnabled(failedNodes) {
		return
	}
	var reason string
	switch opType {
	case ops.SuspendOp:
		reason = StandbyBufferSuspendFailed
	case ops.ConsumeOp:
		reason = StandbyBufferConsumeFailed
	default:
		return
	}
	for _, group := range e.groupNodesByBuffer(failedNodes) {
		msg := formatFailureMessage(opType, group.nodes, errs)
		if msg == "" {
			continue
		}
		e.recorder.Event(group.buffer, v1.EventTypeWarning, reason, msg)
	}
}

func formatSuccessMessage(opType ops.OperationType, nodes []string) string {
	switch opType {
	case ops.SuspendOp:
		return fmt.Sprintf("Successfully suspended %s.", formatNodesSummary(nodes))
	case ops.ConsumeOp:
		return fmt.Sprintf("Successfully consumed %s.", formatNodesSummary(nodes))
	default:
		return ""
	}
}

func formatFailureMessage(opType ops.OperationType, nodes []string, errs map[string]error) string {
	errSummary, count := summarizeErrorsByCode(nodes, errs)
	if count == 0 {
		return ""
	}
	switch opType {
	case ops.SuspendOp:
		return fmt.Sprintf("Failed to suspend %s: %s.", formatNodesCount(count), errSummary)
	case ops.ConsumeOp:
		return fmt.Sprintf("Failed to consume %s: %s.", formatNodesCount(count), errSummary)
	default:
		return ""
	}
}

type bufferNodesGroup struct {
	buffer *v1beta1.CapacityBuffer
	nodes  []string
}

func (e *EventEmitter) groupNodesByBuffer(nodeNames set.Set[string]) map[types.NamespacedName]bufferNodesGroup {
	sortedNodes := nodeNames.SortedList()
	nodeToBuffer := e.stateManager.GetAssignedBuffers(sortedNodes...)
	loggingQuota := klogx.NodesLoggingQuota()
	groups := make(map[types.NamespacedName]bufferNodesGroup)
	for _, nodeName := range sortedNodes {
		buffer := nodeToBuffer[nodeName]
		if buffer == nil {
			klogx.V(4).UpTo(loggingQuota).Infof("%s no assigned CapacityBuffer found for node %q in CSN node state manager cache, skipping event emission", logPrefix, nodeName)
			continue
		}
		key := types.NamespacedName{Namespace: buffer.Namespace, Name: buffer.Name}
		group := groups[key]
		group.buffer = buffer
		group.nodes = append(group.nodes, nodeName)
		groups[key] = group
	}
	klogx.V(4).Over(loggingQuota).Infof("%s no assigned CapacityBuffer found for %d other nodes in CSN node state manager cache, skipping event emission", logPrefix, -loggingQuota.Left())
	return groups
}

func formatNodesSummary(nodes []string) string {
	return fmt.Sprintf("%s (%s)", formatNodesCount(len(nodes)), formatNodeList(nodes))
}

func formatNodesCount(count int) string {
	if count == 1 {
		return "1 standby node"
	}
	return fmt.Sprintf("%d standby nodes", count)
}

func formatNodeList(nodes []string) string {
	if len(nodes) <= maxEventNodeNames {
		return strings.Join(nodes, ", ")
	}
	return fmt.Sprintf("%s, and %d more", strings.Join(nodes[:maxEventNodeNames], ", "), len(nodes)-maxEventNodeNames)
}

func summarizeErrorsByCode(nodes []string, errs map[string]error) (string, int) {
	loggingQuota := klogx.NodesLoggingQuota()
	codeToNodes := make(map[string][]string)
	count := 0
	for _, nodeName := range nodes {
		code := ops.ExtractErrorCode(errs[nodeName])
		if code == ops.ErrorCodeNone {
			klogx.V(4).UpTo(loggingQuota).Infof("%s No error code found for node %q and error %v, this should never happen", logPrefix, nodeName, errs[nodeName])
			continue
		}
		codeToNodes[code] = append(codeToNodes[code], nodeName)
		count++
	}
	klogx.V(4).Over(loggingQuota).Infof("%s No error code found for %d other nodes, this should never happen", logPrefix, -loggingQuota.Left())
	if count == 0 {
		return "", 0
	}

	codes := make([]string, 0, len(codeToNodes))
	for code := range codeToNodes {
		codes = append(codes, code)
	}
	sort.Strings(codes)

	parts := make([]string, 0, len(codes))
	for _, code := range codes {
		parts = append(parts, fmt.Sprintf("%s (%s)", code, formatNodeList(codeToNodes[code])))
	}
	return strings.Join(parts, ", "), count
}
