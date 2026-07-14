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

package test

import (
	"sync"

	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state"
	"k8s.io/utils/set"
)

type SetPendingOperationCall struct {
	Op        ops.OperationType
	Pending   bool
	NodeNames set.Set[string]
	Opts      []state.PendingOperationOpt
}

// MockStateManager is a simplified version of the NodeStateManager
// that can be used for testing.
type MockStateManager struct {
	mutex            sync.Mutex
	Nodes            map[string]state.TrackedNode
	pendingOps       []SetPendingOperationCall
	SetPendingErr    map[string]error
	NodeNameToBuffer map[string]*v1beta1.CapacityBuffer
}

func (m *MockStateManager) Get(nodeName string) (state.TrackedNode, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	n, ok := m.Nodes[nodeName]
	return n, ok
}

func (m *MockStateManager) SetPendingOperation(op ops.OperationType, pending bool, nodeNames set.Set[string], opts ...state.PendingOperationOpt) map[string]error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.pendingOps = append(m.pendingOps, SetPendingOperationCall{Op: op, Pending: pending, NodeNames: nodeNames.Clone(), Opts: opts})
	return m.SetPendingErr
}

func (m *MockStateManager) GetPendingOperationUpdateCalls() []SetPendingOperationCall {
	return m.pendingOps
}

func (m *MockStateManager) GetAssignedBuffers(_ ...string) map[string]*v1beta1.CapacityBuffer {
	return m.NodeNameToBuffer
}
