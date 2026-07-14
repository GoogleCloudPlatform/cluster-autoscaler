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

package csn

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/util/taints"
)

// NodeState represents the state of a CSN node from CA perspective (go/csn-in-ca).
type NodeState string

const (
	SoftWorkloadSeparationKey   string = "buffer.gke.io/standby-capacity-node"
	SoftWorkloadSeparationValue string = "true"
	SuspendedTaintKey           string = "buffer.gke.io/standby-capacity-node-suspended"
	SuspendedTaintValue         string = "true"

	BufferAssignmentKey = "buffer.gke.io/standby-capacity-node-buffer"

	// NodeStateChilling is the state where the node is running but has CSN taint and CSN label.
	NodeStateChilling NodeState = "CHILLING"
	// NodeStateConsumed is the state where the node is running and has no CSN taint or CSN label.
	NodeStateConsumed NodeState = "CONSUMED"
	// NodeStateSuspended is the state where the node is suspended and has CSN taint, CSN label and cordoned.
	NodeStateSuspended NodeState = "SUSPENDED"
	// NodeStateUnknown is the state where the node state is unknown.
	NodeStateUnknown NodeState = "UNKNOWN"
	// NodeConditionSuspended is used to have visibility on the K8s layer if a given node is suspended or not
	// it should always be present with a True value when node is in the Suspended state, and False or not present otherwise
	NodeConditionSuspended apiv1.NodeConditionType = "Suspended"

	NodeSuspendedMessage              string = "Node is suspended as part of a capacity buffer."
	NodeResumedMessage                string = "Node is active, can be suspended in a capacity buffer or used for workload scheduling."
	NodeConsumedMessage               string = "Node has been resumed from capacity buffer and is now ready for workload scheduling."
	NodeConditionReason               string = "CapacityBuffer"
	ColdCapacityInitTimeAnnotationKey string = "buffer.gke.io/standby-capacity-init-time"
)

var (
	SuspendedTaint = apiv1.Taint{
		Key:    SuspendedTaintKey,
		Value:  SuspendedTaintValue,
		Effect: apiv1.TaintEffectNoSchedule,
	}

	SoftWorkloadSeparationTaint = apiv1.Taint{
		Key:    SoftWorkloadSeparationKey,
		Value:  SoftWorkloadSeparationValue,
		Effect: apiv1.TaintEffectPreferNoSchedule,
	}
)

// SetNodeAs marks a node as a CSN node with the given state.
// This method works whether the update is queued or being updated.
func SetNodeAs(node *apiv1.Node, desiredState NodeState) (*apiv1.Node, error) {
	var err error
	switch desiredState {
	case NodeStateChilling:
		// We only uncordon if the current state was suspended which is identified by existence of hard taint.
		// Otherwise, the cordon might have came from another entity.
		if taints.TaintExists(node.Spec.Taints, &SuspendedTaint) {
			uncordonNode(node)
		}
		node, _, err = taints.RemoveTaint(node, &SuspendedTaint)
		if err != nil {
			return node, fmt.Errorf("error removing taint %v to node %q: %v", SuspendedTaint, node.Name, err)
		}
		addCSNLabel(node)
		removeSuspendedCondition(node, NodeResumedMessage)
	case NodeStateSuspended:
		AddTaint(node, withTimeAdded(SuspendedTaint))
		cordonNode(node)
		addCSNLabel(node)
		addSuspendedCondition(node)
	case NodeStateConsumed:
		// We only uncordon if the current state was suspended which is identified by existence of hard taint.
		// Otherwise, the cordon might have came from another entity.
		if taints.TaintExists(node.Spec.Taints, &SuspendedTaint) {
			uncordonNode(node)
		}
		node, _, err = taints.RemoveTaint(node, &SuspendedTaint)
		if err != nil {
			return node, fmt.Errorf("error removing taint %v to node %q: %v", SuspendedTaint, node.Name, err)
		}
		node, _, err = taints.RemoveTaint(node, &SoftWorkloadSeparationTaint)
		if err != nil {
			return node, fmt.Errorf("error removing taint %v to node %q: %v", SoftWorkloadSeparationTaint, node.Name, err)
		}
		removeSuspendedCondition(node, NodeConsumedMessage)
		removeCSNLabel(node)
		RemoveBufferAssignment(node)
		if err := ApplySoftTaints(node, 0); err != nil {
			return node, fmt.Errorf("error removing soft taints: %v", err)
		}
	default:
		return node, fmt.Errorf("state %s is not supported in markNodeAs", desiredState)
	}
	return node, nil
}

func AddTaint(node *apiv1.Node, taint *apiv1.Taint) {
	if taint == nil || taints.TaintExists(node.Spec.Taints, taint) {
		return
	}
	node.Spec.Taints = append(node.Spec.Taints, *taint)
}

func withTimeAdded(t apiv1.Taint) *apiv1.Taint {
	now := metav1.Now()
	taint := t.DeepCopy()
	taint.TimeAdded = &now
	return taint
}

func addCSNLabel(node *apiv1.Node) {
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	node.Labels[SoftWorkloadSeparationKey] = SoftWorkloadSeparationValue
}

func removeCSNLabel(node *apiv1.Node) {
	if node.Labels == nil {
		return
	}
	delete(node.Labels, SoftWorkloadSeparationKey)
}

func cordonNode(node *apiv1.Node) {
	node.Spec.Unschedulable = true
}

func uncordonNode(node *apiv1.Node) {
	node.Spec.Unschedulable = false
}

func addSuspendedCondition(node *apiv1.Node) {
	if node.Status.Conditions == nil {
		node.Status.Conditions = []apiv1.NodeCondition{}
	}
	now := metav1.Now()
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == NodeConditionSuspended {
			if node.Status.Conditions[i].Status == apiv1.ConditionTrue {
				return
			}

			node.Status.Conditions[i].Status = apiv1.ConditionTrue
			node.Status.Conditions[i].LastTransitionTime = now
			node.Status.Conditions[i].LastHeartbeatTime = now
			node.Status.Conditions[i].Message = NodeSuspendedMessage
			node.Status.Conditions[i].Reason = NodeConditionReason
			return
		}
	}
	node.Status.Conditions = append(node.Status.Conditions, apiv1.NodeCondition{
		Type:               NodeConditionSuspended,
		Status:             apiv1.ConditionTrue,
		LastHeartbeatTime:  now,
		LastTransitionTime: now,
		Message:            NodeSuspendedMessage,
		Reason:             NodeConditionReason,
	})
}

func removeSuspendedCondition(node *apiv1.Node, message string) {
	if node.Status.Conditions == nil {
		return
	}
	now := metav1.Now()
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == NodeConditionSuspended {
			node.Status.Conditions[i].Status = apiv1.ConditionFalse
			node.Status.Conditions[i].LastTransitionTime = now
			node.Status.Conditions[i].LastHeartbeatTime = now
			node.Status.Conditions[i].Message = message
			node.Status.Conditions[i].Reason = NodeConditionReason
			break
		}
	}
}

func IsCSNNode(node *apiv1.Node) bool {
	if node.Labels == nil {
		return false
	}
	return node.Labels[SoftWorkloadSeparationKey] == SoftWorkloadSeparationValue
}

func ClassifyNode(node *apiv1.Node) NodeState {
	if !IsCSNNode(node) {
		return NodeStateConsumed
	}
	if taints.TaintExists(node.Spec.Taints, &SuspendedTaint) {
		return NodeStateSuspended
	}
	return NodeStateChilling
}

// AssignNodeToBufferId returns a copy of the node with updated annotations
// that indicate the CSN buffer to which a node is assigned.
// Expected bufferId format: "namespace/name"
func AssignNodeToBufferId(node *apiv1.Node, bufferId string) (*apiv1.Node, error) {
	if node == nil || bufferId == "" {
		return nil, fmt.Errorf("cannot assign node to buffer when node or buffer is nil")
	}

	n := node.DeepCopy()
	if n.Annotations == nil {
		n.Annotations = make(map[string]string)
	}
	n.Annotations[BufferAssignmentKey] = bufferId
	return n, nil
}

// IsAssignedToBuffer returns true if the node has been successfully processed
// by AssignNodeToBufferId with the provided bufferId.
func IsAssignedToBuffer(node *apiv1.Node, bufferId string) bool {
	if node == nil || bufferId == "" {
		return false
	}
	if node.Annotations == nil {
		return false
	}
	return node.Annotations[BufferAssignmentKey] == bufferId
}

// RemoveBufferAssignment modifies the node to remove any mention of assigning a buffer.
func RemoveBufferAssignment(node *apiv1.Node) {
	if node == nil {
		return
	}

	delete(node.Annotations, BufferAssignmentKey)
}

// GetBufferIdFromNode returns the buffer ID assigned to the node if exists, and an empty string otherwise.
func GetBufferIdFromNode(node *apiv1.Node) string {
	if node == nil {
		return ""
	}
	return node.Annotations[BufferAssignmentKey]
}

// IsSuspendedNode returns true if the node has the Suspended condition set to True.
// Which means the node is already suspended, or in the middle of suspend or resume operation.
func IsSuspendedNode(node *apiv1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == NodeConditionSuspended {
			return condition.Status == apiv1.ConditionTrue
		}
	}
	return false
}
