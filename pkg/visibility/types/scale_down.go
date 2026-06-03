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

package types

import (
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility"
	vispb "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/proto"
)

// ScaleDownNode contains information about a scaled down node.
type ScaleDownNode struct {
	Node        *Node
	EvictedPods []*Pod
}

// UnremovableNode contains information about a node that couldn't be removed.
type UnremovableNode struct {
	Node        *Node
	Reason      simulator.UnremovableReason
	BlockingPod *BlockingPod
}

// BlockingPod contains information about a pod that is blocking scale down of a node.
type BlockingPod struct {
	Pod    *Pod
	Reason drain.BlockingPodReason
}

// ScaleDownStatus contains information about the status of a scale down.
type ScaleDownStatus struct {
	Result            status.ScaleDownResult
	ScaledDownNodes   []*ScaleDownNode
	RemovedMigs       []*GkeMig
	NodeDeleteResults map[string]status.NodeDeleteResult
	UnremovableNodes  []*UnremovableNode
}

// Proto converts the structure to its proto representation.
func (n *ScaleDownNode) Proto() *vispb.ScaleDownNode {
	var protoPods []*vispb.Pod
	for i, pod := range n.EvictedPods {
		if i >= visibility.MaxPodsInEvent {
			break
		}
		protoPods = append(protoPods, pod.Proto())
	}

	return &vispb.ScaleDownNode{
		Node:                  n.Node.Proto(),
		EvictedPods:           protoPods,
		EvictedPodsTotalCount: int32(len(n.EvictedPods)),
	}
}

// NodePoolDeletedDataProto extracts a proto representation of data concerning deleted node pools.
func (s *ScaleDownStatus) NodePoolDeletedDataProto() *vispb.NodePoolDeletedData {
	nodePoolNamesSet := make(map[string]bool)
	for _, mig := range s.RemovedMigs {
		nodePoolNamesSet[mig.NodePoolName] = true
	}

	nodePoolNames := make([]string, 0, len(nodePoolNamesSet))
	for nodePoolName := range nodePoolNamesSet {
		nodePoolNames = append(nodePoolNames, nodePoolName)
	}

	return &vispb.NodePoolDeletedData{
		NodePoolNames: nodePoolNames,
	}
}

// ScaleDownDataProto extracts a proto representation of data concerning scaled down nodes.
func (s *ScaleDownStatus) ScaleDownDataProto() *vispb.ScaleDownData {
	var nodes []*vispb.ScaleDownNode
	for _, node := range s.ScaledDownNodes {
		nodes = append(nodes, node.Proto())
	}

	return &vispb.ScaleDownData{
		NodesToBeRemoved: nodes,
	}
}

// ConvertScaleDownNode converts a scale down node to its visibility-specific counterpart.
func ConvertScaleDownNode(scaleDownNode *status.ScaleDownNode) (*ScaleDownNode, error) {
	node, err := ConvertNode(scaleDownNode.Node, &scaleDownNode.UtilInfo, scaleDownNode.NodeGroup)
	if err != nil {
		return nil, err
	}

	var evictedPods []*Pod
	for _, pod := range scaleDownNode.EvictedPods {
		evictedPods = append(evictedPods, ConvertPod(pod))
	}

	return &ScaleDownNode{
		Node:        node,
		EvictedPods: evictedPods,
	}, nil
}

// ConvertUnremovableNode converts an unremovable node to its visibility-specific counterpart.
func ConvertUnremovableNode(unremovableNode *status.UnremovableNode) (*UnremovableNode, error) {
	node, err := ConvertNode(unremovableNode.Node, unremovableNode.UtilInfo, unremovableNode.NodeGroup)
	if err != nil {
		return nil, err
	}

	result := &UnremovableNode{
		Node:   node,
		Reason: unremovableNode.Reason,
	}

	if unremovableNode.BlockingPod != nil {
		result.BlockingPod = ConvertBlockingPod(unremovableNode.BlockingPod)
	}

	return result, nil
}

// ConvertBlockingPod convert a blocking pod to its visibility-specific counterpart.
func ConvertBlockingPod(blockingPod *drain.BlockingPod) *BlockingPod {
	return &BlockingPod{
		Pod:    ConvertPod(blockingPod.Pod),
		Reason: blockingPod.Reason,
	}
}

// ConvertScaleDownStatus converts a scale down status to its visibility-specific counterpart,
// basically replacing all pods, migs etc. with visibility-specific ones.
func ConvertScaleDownStatus(originalStatus *status.ScaleDownStatus) (*ScaleDownStatus, error) {
	var removedMigs []*GkeMig
	for _, removedNodeGroup := range originalStatus.RemovedNodeGroups {
		mig, err := ConvertGkeMig(removedNodeGroup)
		if err != nil {
			return nil, err
		}
		removedMigs = append(removedMigs, mig)
	}

	var scaledDownNodes []*ScaleDownNode
	for _, scaledDownNode := range originalStatus.ScaledDownNodes {
		convertedNode, err := ConvertScaleDownNode(scaledDownNode)
		if err != nil {
			return nil, err
		}
		scaledDownNodes = append(scaledDownNodes, convertedNode)
	}

	var unremovableNodes []*UnremovableNode
	for _, unremovableNode := range originalStatus.UnremovableNodes {
		convertedNode, err := ConvertUnremovableNode(unremovableNode)
		if err != nil {
			return nil, err
		}
		unremovableNodes = append(unremovableNodes, convertedNode)
	}

	return &ScaleDownStatus{
		Result:            originalStatus.Result,
		NodeDeleteResults: originalStatus.NodeDeleteResults,
		RemovedMigs:       removedMigs,
		ScaledDownNodes:   scaledDownNodes,
		UnremovableNodes:  unremovableNodes,
	}, nil
}
