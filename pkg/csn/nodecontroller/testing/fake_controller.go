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

package testing

import (
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
	"k8s.io/utils/set"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// MockCSNNodeController is a mock implementation of csnNodeController for testing.
type MockCSNNodeController struct {
	nodes               []nodecontroller.CSNNode
	hasOperationsNodes  map[string]bool
	backedOffNodes      map[string]bool
	currentStates       map[string]csn.NodeState
	nonSuspendableNodes map[string]bool
	nonConsumableNodes  map[string]bool
	bufferAssignments   map[string]*v1beta1.CapacityBuffer
	listErr             error
	reconcileCalls      int
	reconcileChan       chan struct{}
}

// NewMockCSNNodeController returns a new instance of MockCSNNodeController.
func NewMockCSNNodeController(nodes []nodecontroller.CSNNode) *MockCSNNodeController {
	return &MockCSNNodeController{
		nodes:               nodes,
		hasOperationsNodes:  map[string]bool{},
		backedOffNodes:      map[string]bool{},
		currentStates:       map[string]csn.NodeState{},
		nonSuspendableNodes: map[string]bool{},
		nonConsumableNodes:  map[string]bool{},
		reconcileChan:       make(chan struct{}, 5),
	}
}

func (m *MockCSNNodeController) List(filters ...nodecontroller.CSNFilter) ([]nodecontroller.CSNNode, map[string]int, error) {
	if m.listErr != nil {
		return nil, nil, m.listErr
	}
	var nodes []nodecontroller.CSNNode
	filteredCounts := make(map[string]int)
	for _, node := range m.nodes {
		currentState, ok := m.currentStates[node.Name]
		if !ok {
			currentState = node.DesiredState
		}
		hasPendingOps := m.hasOperationsNodes[node.Name]
		n := node
		n.State = currentState
		n.HasPendingOperations = hasPendingOps
		n.IsBackedOff = m.backedOffNodes[node.Name]

		pass := true
		for _, filter := range filters {
			if filter == nil {
				continue
			}
			ok, reason := filter(n)
			if ok {
				continue
			}
			if reason != "" {
				filteredCounts[reason]++
			}
			pass = false
			break
		}
		if pass {
			nodes = append(nodes, n)
		}
	}
	return nodes, filteredCounts, m.listErr
}

func (m *MockCSNNodeController) Consume(nodes []string) set.Set[string] {
	consumedNodes := set.New[string]()
	for _, node := range nodes {
		if !m.nonConsumableNodes[node] {
			consumedNodes.Insert(node)
		}
	}
	m.adjustNodeStatesTo(consumedNodes.UnsortedList(), csn.NodeStateConsumed)
	return consumedNodes
}

func (m *MockCSNNodeController) MarkAsSuspendable(nodeInfos []*framework.NodeInfo) set.Set[string] {
	suspendedNodes := set.New[string]()
	for _, ni := range nodeInfos {
		if !m.nonSuspendableNodes[ni.Node().Name] {
			suspendedNodes.Insert(ni.Node().Name)
		}
	}
	m.adjustNodeStatesTo(suspendedNodes.UnsortedList(), csn.NodeStateSuspended)
	return suspendedNodes
}

func (m *MockCSNNodeController) Reconcile() {
	m.reconcileCalls += 1
	m.reconcileChan <- struct{}{}
}

func (m *MockCSNNodeController) UpdateBackoffStatus() {
	for i, node := range m.nodes {
		m.nodes[i].IsBackedOff = m.backedOffNodes[node.Name]
	}
}

func (m *MockCSNNodeController) SetNonSuspendableNodes(nodes []string) {
	clear(m.nonSuspendableNodes)
	for _, node := range nodes {
		m.nonSuspendableNodes[node] = true
	}
}

func (m *MockCSNNodeController) SetNonConsumableNodes(nodes []string) {
	clear(m.nonConsumableNodes)
	for _, node := range nodes {
		m.nonConsumableNodes[node] = true
	}
}

func (m *MockCSNNodeController) MarkAsHasPendingOperations(nodes []string) {
	for _, node := range nodes {
		m.hasOperationsNodes[node] = true
	}
}

func (m *MockCSNNodeController) MarkAsBackedOff(nodes []string) {
	for _, node := range nodes {
		m.backedOffNodes[node] = true
	}
}

func (m *MockCSNNodeController) SetCurrentState(nodeName string, state csn.NodeState) {
	m.currentStates[nodeName] = state
}

func (m *MockCSNNodeController) ProcessBufferAssignment(nodeNameToBuffer map[string]*v1beta1.CapacityBuffer) {
	m.bufferAssignments = nodeNameToBuffer
}

func (m *MockCSNNodeController) GetBufferAssignments() map[string]*v1beta1.CapacityBuffer {
	return m.bufferAssignments
}

func (m *MockCSNNodeController) SetListError(err error) {
	m.listErr = err
}

func (m *MockCSNNodeController) NodesWithState(state csn.NodeState) []string {
	var nodes []string
	for _, node := range m.nodes {
		if node.DesiredState == state {
			nodes = append(nodes, node.Name)
		}
	}
	return nodes
}

func (m *MockCSNNodeController) GetReconcileCalls() int {
	return m.reconcileCalls
}

func (m *MockCSNNodeController) WaitForReconcileCall() {
	<-m.reconcileChan
}

func (m *MockCSNNodeController) adjustNodeStatesTo(nodes []string, state csn.NodeState) {
	nodesSet := map[string]bool{}
	for _, node := range nodes {
		nodesSet[node] = true
	}

	for i, node := range m.nodes {
		if nodesSet[node.Name] {
			m.nodes[i].DesiredState = state
		}
	}
}
