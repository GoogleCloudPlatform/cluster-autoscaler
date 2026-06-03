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

package customresources

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
)

const (
	// lastAppliedLabelsKey is a key of an annotation, which gcp-controller-manager
	// writes to a node when reconciling its labels. If it's not present,
	// that means that gcp-controller-manager hasn't applied labels to the node yet.
	lastAppliedLabelsKey = "node.gke.io/last-applied-node-labels"
)

// LabelsProcessor marks as unready nodes that have
// missing labels neccessary for proper scheduling,
// such as missing user labels present in the nodeGroup template node
// or gke node pool label
type LabelsProcessor struct {
}

func (p *LabelsProcessor) FilterOutNodesWithMissingLabels(allNodes, readyNodes []*v1.Node) ([]*v1.Node, []*v1.Node) {
	newAllNodes, newReadyNodes := []*v1.Node{}, []*v1.Node{}
	nodesWithMissingLabels := map[string]bool{}
	for _, node := range readyNodes {
		if node.Annotations[lastAppliedLabelsKey] != "" {
			newReadyNodes = append(newReadyNodes, node)
		} else {
			nodesWithMissingLabels[node.Name] = true
		}
	}

	// Override any node with unready labels with its "unready" copy
	for _, node := range allNodes {
		if nodesWithMissingLabels[node.Name] {
			newAllNodes = append(newAllNodes, kubernetes.GetUnreadyNodeCopy(node, kubernetes.ResourceUnready))
		} else {
			newAllNodes = append(newAllNodes, node)
		}
	}
	return newAllNodes, newReadyNodes
}
