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
	"slices"

	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
)

// MockCSNNodeController is a mock implementation of csnNodeController for testing.
type MockCSNNodeController struct {
	nodes               []nodecontroller.CSNNode
	hasOperationsNodes  map[string]bool
	nonSuspendableNodes map[string]bool
	bufferAssignments   map[string]*v1beta1.CapacityBuffer
	listErr             error
	consumeErr          error
	suspendErr          error
	reconcileCalls      int
	reconcileChan       chan struct{}
}

// NewMockCSNNodeController returns a new instance of MockCSNNodeController.
func NewMockCSNNodeController(nodes []nodecontroller.CSNNode) *MockCSNNodeController {
	return &MockCSNNodeController{
		nodes:               nodes,
		hasOperationsNodes:  map[string]bool{},
		nonSuspendableNodes: map[string]bool{},
		reconcileChan:       make(chan struct{}, 5),
	}
}

func (m *MockCSNNodeController) List(filters ...nodecontroller.CSNFilter) ([]nodecontroller.CSNNode, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var nodes []nodecontroller.CSNNode
	for _, node := range m.nodes {
		if slices.Contains(filters, nodecontroller.WithoutPendingOperationsFilter) && m.hasOperationsNodes[node.Name] {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, m.listErr
}

func (m *MockCSNNodeController) Consume(nodes []string) error {
	if m.consumeErr != nil {
		return m.consumeErr
	}
	m.adjustNodeStatesTo(nodes, csn.NodeStateConsumed)
	return nil
}

func (m *MockCSNNodeController) MarkAsSuspendable(nodeInfos []*framework.NodeInfo) ([]string, error) {
	if m.suspendErr != nil {
		return nil, m.suspendErr
	}
	var suspendedNodes []string
	for _, ni := range nodeInfos {
		if !m.nonSuspendableNodes[ni.Node().Name] {
			suspendedNodes = append(suspendedNodes, ni.Node().Name)
		}
	}
	m.adjustNodeStatesTo(suspendedNodes, csn.NodeStateSuspended)
	return suspendedNodes, nil
}

func (m *MockCSNNodeController) Reconcile() {
	m.reconcileCalls += 1
	m.reconcileChan <- struct{}{}
}

func (m *MockCSNNodeController) SetNonSuspendableNodes(nodes []string) {
	clear(m.nonSuspendableNodes)
	for _, node := range nodes {
		m.nonSuspendableNodes[node] = true
	}
}

func (m *MockCSNNodeController) MarkAsHasPendingOperations(nodes []string) {
	for _, node := range nodes {
		m.hasOperationsNodes[node] = true
	}
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

func (m *MockCSNNodeController) SetConsumeError(err error) {
	m.consumeErr = err
}

func (m *MockCSNNodeController) SetSuspendError(err error) {
	m.suspendErr = err
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
