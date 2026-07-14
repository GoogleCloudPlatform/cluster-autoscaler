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
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
)

// Basic integration test, relies on the core logic being tested
// in the NodeStateManager package.
func TestNodeStateManager_InformerAdapter(t *testing.T) {
	client := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	eventChan := make(chan NodeEvent, 10)
	m := NewNodeStateManagerFromInformer(
		informerFactory,
		WithEventHandler(func(e NodeEvent) {
			eventChan <- e
		}),
	)
	mustRunManager(t, m)
	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactory.Start(stopCh)
	informerFactory.WaitForCacheSync(stopCh)

	nodeName := "test-node"
	node := test.CreateNode(nodeName, test.StateOpt(csn.NodeStateChilling))

	// 1. Add CSN node (Chilling)
	_, err := client.CoreV1().Nodes().Create(nil, node, metav1.CreateOptions{})
	assert.NoError(t, err)
	ev := <-eventChan
	if assert.IsType(t, NodeAdded{}, ev) {
		added := ev.(NodeAdded)
		assert.Equal(t, csn.NodeStateChilling, added.State)
	}

	tn, ok := m.Get(nodeName)
	assert.True(t, ok)
	assert.Equal(t, csn.NodeStateChilling, tn.State)

	// 2. Transition Chilling -> Suspended
	suspendedNode, err := csn.SetNodeAs(node.DeepCopy(), csn.NodeStateSuspended)
	assert.NoError(t, err)
	_, err = client.CoreV1().Nodes().Update(nil, suspendedNode, metav1.UpdateOptions{})
	assert.NoError(t, err)
	ev = <-eventChan
	if assert.IsType(t, NodeUpdated{}, ev) {
		updated := ev.(NodeUpdated)
		assert.Equal(t, csn.NodeStateSuspended, updated.NewState)
	}

	tn, ok = m.Get(nodeName)
	assert.True(t, ok)
	assert.Equal(t, csn.NodeStateSuspended, tn.State)

	// 3. Delete node
	err = client.CoreV1().Nodes().Delete(nil, nodeName, metav1.DeleteOptions{})
	assert.NoError(t, err)
	assert.IsType(t, NodeUntracked{}, <-eventChan)
	assert.IsType(t, NodeDeleted{}, <-eventChan)

	_, ok = m.Get(nodeName)
	assert.False(t, ok)
}
