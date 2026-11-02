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

package operationtracker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestEnqueue(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024)

	newOperationForResize := func(from, to size.VmSize) operation {
		resizeOperation := ResizeOperation{
			NodeName:     node.Name,
			StartingSize: from,
			DesiredSize:  to,
		}
		return operation{resize: &resizeOperation}
	}

	newOperationsForResize := func(sizeSteps ...size.VmSize) []operation {
		var ops []operation
		if len(sizeSteps) < 2 {
			return ops
		}
		for i := 0; i < len(sizeSteps)-1; i++ {
			ops = append(ops, newOperationForResize(sizeSteps[i], sizeSteps[i+1]))
		}
		return ops
	}

	fixOp := newFixOp(node.Name)

	testCases := []struct {
		desc           string
		operations     []operation
		wantOperations []operation
	}{
		{
			desc:           "empty operations",
			operations:     []operation{},
			wantOperations: nil,
		},
		{
			desc:           "fix operations only",
			operations:     []operation{fixOp, fixOp, fixOp},
			wantOperations: []operation{fixOp},
		},
		{
			desc:           "upsizes only",
			operations:     newOperationsForResize(newSize(100, 100), newSize(200, 200), newSize(400, 800)),
			wantOperations: newOperationsForResize(newSize(100, 100), newSize(400, 800)),
		},
		{
			desc:           "downsizes only",
			operations:     []operation{fixOp, newOperationForResize(newSize(200, 200), newSize(100, 100))},
			wantOperations: newOperationsForResize(newSize(200, 200), newSize(100, 100)),
		},
		{
			desc:           "mixed operations",
			operations:     newOperationsForResize(newSize(100, 100), newSize(200, 400), newSize(150, 50)),
			wantOperations: newOperationsForResize(newSize(100, 100), newSize(150, 100), newSize(150, 50)),
		},
		{
			desc:           "upsize+downsize -> downsize",
			operations:     newOperationsForResize(newSize(200, 200), newSize(200, 400), newSize(100, 100)),
			wantOperations: newOperationsForResize(newSize(200, 200), newSize(100, 100)),
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Enqueue: %s", tc.desc), func(t *testing.T) {
			opQueue := newOperationQueue("test")
			defer opQueue.ShutDown()

			for _, op := range tc.operations {
				opQueue.Enqueue(op)
			}

			gotOperations := popOperations(t, opQueue, len(tc.wantOperations))
			assert.Equal(t, tc.wantOperations, gotOperations)
		})
		t.Run(fmt.Sprintf("EnqueueAfter: %s", tc.desc), func(t *testing.T) {
			opQueue := newOperationQueue("test")
			defer opQueue.ShutDown()

			for _, op := range tc.operations {
				opQueue.EnqueueAfter(op, 10*time.Millisecond)
			}

			gotOperations := popOperations(t, opQueue, len(tc.wantOperations))
			assert.Equal(t, tc.wantOperations, gotOperations)
		})
	}
}

func TestMixOfEnqueueAndEnqueueAfter(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024)

	fixOp := newFixOp(node.Name)

	opQueue := newOperationQueue("test")
	defer opQueue.ShutDown()
	opQueue.EnqueueAfter(fixOp, 1*time.Hour)
	opQueue.Enqueue(fixOp)

	gotNodeName, quit := opQueue.nodeQueue.Get()

	assert.False(t, quit)
	assert.Equal(t, node.Name, gotNodeName)
}

func TestEnqueueFront(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024)

	fixOp := operation{fix: &fixOperation{NodeName: node.Name, MachineFamily: "ek"}}
	resizeOp := newResizeOp(node.Name, 100, 200)

	opQueue := newOperationQueue("test")
	defer opQueue.ShutDown()

	opQueue.Enqueue(fixOp)
	opQueue.EnqueueFront(resizeOp)

	op, quit := opQueue.Get()
	assert.False(t, quit)
	assert.Equal(t, resizeOp, op)
}

func TestGet(t *testing.T) {
	node1 := test.BuildTestNode("node1", 1000, 1024)
	node2 := test.BuildTestNode("node2", 1000, 1024)

	testCases := []struct {
		desc             string
		enqueueOperation []operation
		wantOperation    []operation
	}{
		{
			desc:             "single node",
			enqueueOperation: []operation{{resize: &ResizeOperation{NodeName: node1.Name}}},
			wantOperation:    []operation{{resize: &ResizeOperation{NodeName: node1.Name}}},
		},
		{
			desc:             "node separation",
			enqueueOperation: []operation{{resize: &ResizeOperation{NodeName: node1.Name}}, {resize: &ResizeOperation{NodeName: node2.Name}}},
			wantOperation:    []operation{{resize: &ResizeOperation{NodeName: node1.Name}}, {resize: &ResizeOperation{NodeName: node2.Name}}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			opQueue := newOperationQueue("test")
			defer opQueue.ShutDown()

			for _, op := range tc.enqueueOperation {
				opQueue.Enqueue(op)
			}

			for _, wantOp := range tc.wantOperation {
				op, quit := opQueue.Get()
				assert.False(t, quit)
				assert.Equal(t, wantOp, op)
			}
		})
	}
}

func TestGetUpsizesFirst(t *testing.T) {
	node := func(i int) *v1.Node {
		return test.BuildTestNode(fmt.Sprintf("node%d", i), 1000, 1024)
	}

	upsize := func(node *v1.Node) operation {
		return newResizeOp(node.Name, 100, 200)
	}

	downsize := func(node *v1.Node) operation {
		return newResizeOp(node.Name, 200, 100)
	}

	fix := func(node *v1.Node) operation {
		return newFixOp(node.Name)
	}

	ops := []operation{
		upsize(node(1)),
		fix(node(2)),
		downsize(node(3)),
		downsize(node(4)),
		upsize(node(5)),
		downsize(node(6)),
		downsize(node(7)),
		downsize(node(8)),
		upsize(node(9)),
	}
	upsizesCount := 3

	operationCounter := 0
	opQueue := newOperationQueue("test")
	for _, op := range ops {
		opQueue.Enqueue(op)
	}

	for range len(ops) {
		operation, quit := opQueue.Get()
		if quit {
			break
		}

		if operationCounter < upsizesCount {
			assert.True(t, isUpsize(operation))
		} else {
			assert.False(t, isUpsize(operation))
		}

		operationCounter++
		opQueue.Done(operation)
	}

	opQueue.ShutDown()
	assert.Equal(t, len(ops), operationCounter)
}

func TestEnqueueAfterWithUpsizes(t *testing.T) {
	t.Run("delayed_upsize_does_not_block_ready_non_upsizes", func(t *testing.T) {
		opQueue := newOperationQueue("test")
		defer opQueue.ShutDown()

		opQueue.EnqueueAfter(newResizeOp("node1", 100, 200), 1*time.Hour)
		opQueue.Enqueue(newResizeOp("node2", 200, 100))
		opQueue.Enqueue(newFixOp("node3"))

		wantOperations := []operation{newResizeOp("node2", 200, 100), newFixOp("node3")}
		gotOperations := popOperations(t, opQueue, len(wantOperations))
		assert.Equal(t, wantOperations, gotOperations)
	})

	t.Run("delayed_upsize_preempts_non_upsize_once_ready", func(t *testing.T) {
		opQueue := newOperationQueue("test")
		defer opQueue.ShutDown()

		opQueue.Enqueue(newResizeOp("node2", 200, 100))
		opQueue.EnqueueAfter(newResizeOp("node1", 100, 200), 20*time.Millisecond)
		time.Sleep(40 * time.Millisecond)

		wantOperations := []operation{newResizeOp("node1", 100, 200), newResizeOp("node2", 200, 100)}
		gotOperations := popOperations(t, opQueue, len(wantOperations))
		assert.Equal(t, wantOperations, gotOperations)
	})
}

func TestGetPriorityDowngrade(t *testing.T) {
	testCases := []struct {
		name              string
		enqueueOperations []operation
		clearNodeName     string
		postClearEnqueue  []operation
		wantOperations    []operation
	}{
		{
			name: "upsize_squashed_into_net_downsize",
			enqueueOperations: []operation{
				newResizeOp("node1", 100, 200),
				newResizeOp("node2", 200, 100),
				newResizeOp("node1", 200, 50),
				newResizeOp("node3", 100, 200),
			},
			wantOperations: []operation{
				newResizeOp("node3", 100, 200),
				newResizeOp("node2", 200, 100),
				newResizeOp("node1", 100, 50),
			},
		},
		{
			name: "upsize_overridden_by_reconcile_node_state",
			enqueueOperations: []operation{
				newResizeOp("node1", 100, 200),
				newReconcileOp("node1"),
				newResizeOp("node2", 100, 200),
			},
			wantOperations: []operation{
				newResizeOp("node2", 100, 200),
				newReconcileOp("node1"),
			},
		},
		{
			name: "upsize_cleared_and_replaced_by_fix",
			enqueueOperations: []operation{
				newResizeOp("node1", 100, 200),
			},
			clearNodeName: "node1",
			postClearEnqueue: []operation{
				newFixOp("node1"),
				newResizeOp("node2", 100, 200),
			},
			wantOperations: []operation{
				newResizeOp("node2", 100, 200),
				newFixOp("node1"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opQueue := newOperationQueue("test")
			defer opQueue.ShutDown()

			for _, op := range tc.enqueueOperations {
				opQueue.Enqueue(op)
			}
			if tc.clearNodeName != "" {
				opQueue.ClearResizeOperations(tc.clearNodeName)
			}
			for _, op := range tc.postClearEnqueue {
				opQueue.Enqueue(op)
			}

			gotOperations := popOperations(t, opQueue, len(tc.wantOperations))
			assert.Equal(t, tc.wantOperations, gotOperations)
		})
	}
}

func TestMultipleWorkerProcessSingleNodeOperationsSequentially(t *testing.T) {
	const (
		operationCount = 5
		workerCount    = 10
	)
	node := test.BuildTestNode("node1", 1000, 1024)
	newResizeOperation := func() operation {
		return operation{
			resize: &ResizeOperation{
				NodeName:     node.Name,
				StartingSize: newSize(0, 0),
				DesiredSize:  newSize(1, 1),
			},
		}
	}

	workerWG := sync.WaitGroup{}
	opQueue := newOperationQueue("test")

	ops := make([]struct {
		op        operation
		waitCh    chan (struct{})
		processCh chan (struct{})
	}, operationCount)
	for i := range operationCount {
		ops[i].op = newResizeOperation()
		ops[i].waitCh = make(chan struct{})
		ops[i].processCh = make(chan struct{})
	}

	currentOp := int32(0)
	concurrentOps := int32(0)
	for range workerCount {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for {
				operation, quit := opQueue.Get()
				if quit {
					return
				}
				atomic.AddInt32(&concurrentOps, 1)
				// Ensure that concurrency equals exactly 1.
				assert.Equal(t, int32(1), atomic.LoadInt32(&concurrentOps))
				current := atomic.LoadInt32(&currentOp)
				close(ops[current].waitCh)
				<-ops[current].processCh

				atomic.AddInt32(&currentOp, 1)

				atomic.AddInt32(&concurrentOps, -1)
				// Ensure that no other worker is processing an item.
				assert.Equal(t, int32(0), atomic.LoadInt32(&concurrentOps))

				opQueue.Done(operation)
			}
		}()
	}

	// Enqueue the first item
	opQueue.Enqueue(ops[0].op)
	for i := range len(ops) - 1 {
		<-ops[i].waitCh
		opQueue.Enqueue(ops[i+1].op) // Overlap Next enqueue while Current is busy
		close(ops[i].processCh)      // Release Current
	}
	// Handle the last item
	<-ops[len(ops)-1].waitCh
	close(ops[len(ops)-1].processCh)

	opQueue.ShutDown()
	workerWG.Wait()
}

func TestClearNodeQueue(t *testing.T) {
	opQueue := newOperationQueue("test")
	defer opQueue.ShutDown()

	node := test.BuildTestNode("node1", 1000, 1024)
	op1 := operation{resize: &ResizeOperation{NodeName: node.Name}}
	wantOp := newFixOp(node.Name)

	opQueue.Enqueue(op1)
	opQueue.ClearResizeOperations(node.Name)
	opQueue.Enqueue(wantOp)

	op, quit := opQueue.Get()
	assert.False(t, quit)
	assert.Equal(t, wantOp, op)
}

func TestShutdown(t *testing.T) {
	opQueue := newOperationQueue("test")
	opQueue.ShutDown()
	_, quit := opQueue.Get()
	assert.True(t, quit)
}

func TestSquashOperations(t *testing.T) {
	newOperationForResize := func(from, to size.VmSize) operation {
		resizeOperation := ResizeOperation{
			NodeName:     "",
			StartingSize: from,
			DesiredSize:  to,
		}
		return operation{resize: &resizeOperation}
	}

	newOperationsForResize := func(sizeSteps ...size.VmSize) []operation {
		var ops []operation
		if len(sizeSteps) < 2 {
			return ops
		}
		for i := 0; i < len(sizeSteps)-1; i++ {
			ops = append(ops, newOperationForResize(sizeSteps[i], sizeSteps[i+1]))
		}
		return ops
	}

	fixOp := operation{fix: &fixOperation{}}
	reconcileNodeStateOp1 := operation{reconcileNodeState: &reconcileNodeStateOperation{attempts: 1}}
	reconcileNodeStateOp2 := operation{reconcileNodeState: &reconcileNodeStateOperation{attempts: 2}}

	testCases := []struct {
		desc               string
		operations         []operation
		expectedOperations []operation
	}{
		{
			desc:               "empty operations",
			operations:         []operation{},
			expectedOperations: []operation{},
		},
		{
			desc:               "only generic fix operations",
			operations:         []operation{fixOp, fixOp, fixOp},
			expectedOperations: []operation{fixOp},
		},
		{
			desc:               "mix between resize and generic fix operations -> resize operations",
			operations:         []operation{fixOp, newOperationForResize(newSize(100, 100), newSize(200, 400)), fixOp, newOperationForResize(newSize(200, 400), newSize(150, 50)), fixOp},
			expectedOperations: newOperationsForResize(newSize(100, 100), newSize(150, 100), newSize(150, 50)),
		},
		{
			desc:               "only fix unknown state operations",
			operations:         []operation{reconcileNodeStateOp1, reconcileNodeStateOp2},
			expectedOperations: []operation{reconcileNodeStateOp2},
		},
		{
			desc:               "mix between resize, generic fix and fix unknown state operations ->  fix unknown state operation",
			operations:         []operation{fixOp, newOperationForResize(newSize(100, 100), newSize(200, 400)), fixOp, reconcileNodeStateOp1, fixOp, newOperationForResize(newSize(200, 400), newSize(150, 50)), fixOp, reconcileNodeStateOp2},
			expectedOperations: []operation{reconcileNodeStateOp2},
		},
		{
			desc:               "only upsizes",
			operations:         newOperationsForResize(newSize(100, 100), newSize(200, 400), newSize(400, 800)),
			expectedOperations: newOperationsForResize(newSize(100, 100), newSize(400, 800)),
		},
		{
			desc:               "only downsizes",
			operations:         newOperationsForResize(newSize(400, 800), newSize(200, 400), newSize(100, 100)),
			expectedOperations: newOperationsForResize(newSize(400, 800), newSize(100, 100)),
		},
		{
			desc:               "upsize+downsize -> upsize",
			operations:         newOperationsForResize(newSize(100, 100), newSize(200, 400), newSize(150, 300)),
			expectedOperations: newOperationsForResize(newSize(100, 100), newSize(150, 300)),
		},
		{
			desc:               "downsize+upsize -> downsize",
			operations:         newOperationsForResize(newSize(200, 400), newSize(100, 100), newSize(150, 300)),
			expectedOperations: newOperationsForResize(newSize(200, 400), newSize(150, 300)),
		},
		{
			desc:               "mixed resize operations",
			operations:         newOperationsForResize(newSize(100, 100), newSize(200, 400), newSize(150, 50)),
			expectedOperations: newOperationsForResize(newSize(100, 100), newSize(150, 100), newSize(150, 50)),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			squashed := squashOperations(tc.operations)
			assert.Equal(t, tc.expectedOperations, squashed)
		})
	}
}

func TestIsNodeInProcess(t *testing.T) {
	opQueue := newOperationQueue("test-queue")
	defer opQueue.ShutDown()
	nodeName := "node1"
	op1 := operation{resize: &ResizeOperation{NodeName: nodeName}}

	// Node not in process by default
	assert.Equal(t, opQueue.IsNodeInProcess(nodeName), false)

	opQueue.Enqueue(op1)

	// Node not in process yet even after enqueue
	assert.Equal(t, opQueue.IsNodeInProcess(nodeName), false)

	op, _ := opQueue.Get()
	// Node should be in-process after Getting the operation
	assert.Equal(t, opQueue.IsNodeInProcess(nodeName), true)

	// Node should be back idle after calling Done
	opQueue.Done(op)
	assert.Equal(t, opQueue.IsNodeInProcess(nodeName), false)
}

func TestIsNodeResizingOrPending(t *testing.T) {
	opQueue := newOperationQueue("test-queue")
	defer opQueue.ShutDown()
	nodeName := "node1"

	// No operations in the queue
	assert.Equal(t, opQueue.IsNodeResizingOrPending(nodeName), false)

	// Fix operation is in progress
	opQueue.Enqueue(newFixOp(nodeName))
	fixOp, _ := opQueue.Get()
	assert.Equal(t, opQueue.IsNodeResizingOrPending(nodeName), false)
	opQueue.Done(fixOp)

	// Resize operation is enqueued
	opQueue.Enqueue(operation{resize: &ResizeOperation{NodeName: nodeName}})
	assert.Equal(t, opQueue.IsNodeResizingOrPending(nodeName), true)

	// Resize operation is in progress
	resizeOp, _ := opQueue.Get()
	assert.Equal(t, opQueue.IsNodeResizingOrPending(nodeName), true)

	// Resize operation is finished
	opQueue.Done(resizeOp)
	assert.Equal(t, opQueue.IsNodeInProcess(nodeName), false)
}

func newResizeOp(nodeName string, from, to int64) operation {
	return operation{
		resize: &ResizeOperation{
			NodeName:     nodeName,
			StartingSize: newSize(from, from),
			DesiredSize:  newSize(to, to),
		},
	}
}

func newFixOp(nodeName string) operation {
	return operation{
		fix: &fixOperation{
			NodeName: nodeName,
		},
	}
}

func newReconcileOp(nodeName string) operation {
	return operation{
		reconcileNodeState: &reconcileNodeStateOperation{
			nodeName:   nodeName,
			targetSize: newSize(100, 100),
		},
	}
}

func popOperations(t *testing.T, opQueue *operationQueue, count int) []operation {
	t.Helper()
	var gotOperations []operation
	for range count {
		op, quit := opQueue.Get()
		assert.False(t, quit)
		gotOperations = append(gotOperations, op)
		opQueue.Done(op)
	}
	return gotOperations
}
