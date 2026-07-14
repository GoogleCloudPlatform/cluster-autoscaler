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
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/klog/v2"
)

// operationQueue is a thread-safe operations queue.
// All operations are group by the nodes.
type operationQueue struct {
	nodeQueue       workqueue.TypedDelayingInterface[string]
	isNodeInProcess sync.Map

	mux               sync.Mutex
	operationsPerNode map[string][]operation
	upsizeCounter     int
}

// newOperationQueue returns a new instance of operation queue.
func newOperationQueue(name string) *operationQueue {
	return &operationQueue{
		nodeQueue:         workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Name: name}),
		operationsPerNode: map[string][]operation{},
	}
}

func (q *operationQueue) Enqueue(op operation) {
	q.enqueue(op, 0)
}

// Don't use it for upsizes with high priority (currently upsizes) until b/423865823 is fixed.
// TODO(b/423865823): Remove the above comment after the bug is fixed.
func (q *operationQueue) EnqueueAfter(op operation, delay time.Duration) {
	q.enqueue(op, delay)
}

// enqueue places operation in the queue based on the operation node.
func (q *operationQueue) enqueue(op operation, delay time.Duration) {
	nodeName := op.nodeName()
	if nodeName == "" {
		klog.Warningf("Operation Queue: Node name is empty in operation: %+v", op)
		return
	}

	q.mux.Lock()
	defer q.mux.Unlock()

	upsizesBefore := countUpsizes(q.operationsPerNode[nodeName])
	q.operationsPerNode[nodeName] = squashOperations(append(q.operationsPerNode[nodeName], op))
	upsizesAfter := countUpsizes(q.operationsPerNode[nodeName])
	q.upsizeCounter = q.upsizeCounter + upsizesAfter - upsizesBefore

	q.nodeQueue.AddAfter(nodeName, delay)
}

// Get returns either an operation to be processed or a shutdown notification.
// If gets returns operation, Done should be called for this operation.
func (q *operationQueue) Get() (operation, bool) {
	for {
		nodeName, quit := q.nodeQueue.Get()
		if quit {
			return operation{}, true
		}

		operation, hasNext := q.popNextPreferredOperation(nodeName)
		if hasNext {
			q.isNodeInProcess.Store(nodeName, operation)
			return operation, false
		}
	}
}

// Done marks item as done processing.
// Moreover, it will re-add the node to the queue for further processing.
func (q *operationQueue) Done(op operation) {
	nodeName := op.nodeName()
	if nodeName == "" {
		klog.Warningf("Operation Queue: Node name is empty in operation: %+v", op)
		return
	}

	// Re-Add node to queue. Node will be only removed from queue when there is no operation for the given node.
	q.nodeQueue.Add(nodeName)
	q.isNodeInProcess.Delete(nodeName)
	q.nodeQueue.Done(nodeName)
}

// ShutDown will cause queue to ignore all new items added to it and immediately instruct the worker goroutines to exit.
func (q *operationQueue) ShutDown() {
	q.nodeQueue.ShutDown()
}

// ClearResizeOperations removes all pending resize operations for the given Node.
func (q *operationQueue) ClearResizeOperations(nodeName string) {
	q.mux.Lock()
	defer q.mux.Unlock()

	q.upsizeCounter -= countUpsizes(q.operationsPerNode[nodeName])
	nonResizeOperations := []operation{}
	for _, operation := range q.operationsPerNode[nodeName] {
		if operation.resize == nil {
			nonResizeOperations = append(nonResizeOperations, operation)
		}
	}
	if len(nonResizeOperations) == 0 {
		delete(q.operationsPerNode, nodeName)
	} else {
		q.operationsPerNode[nodeName] = nonResizeOperations
	}
}

// popNextPreferredOperation pops a next preferred operation from the queue,
// if such an operation exists for a node.
// If there are upsizes in the queue, preferred operation is an upsize,
// otherwise any.
// This method is thread-safe.
func (q *operationQueue) popNextPreferredOperation(nodeName string) (operation, bool) {
	q.mux.Lock()
	defer q.mux.Unlock()

	op, hasNext := q.peekNextOperation(nodeName)
	if !hasNext {
		// Nothing to do - mark node processed and move on.
		q.nodeQueue.Done(nodeName)
		return operation{}, false
	}

	// In the valid state if an upsize is present in node operations,
	// it will be the first operation in the list
	if q.upsizeCounter > 0 && !isUpsize(op) {
		q.nodeQueue.Add(nodeName)
		q.nodeQueue.Done(nodeName)
		return operation{}, false
	}

	if isUpsize(op) {
		q.upsizeCounter--
	}

	q.dropNextOperation(nodeName)
	return op, true
}

// peekNextOperation peeks a top operation from the queue,
// if the operation exists for a node.
func (q *operationQueue) peekNextOperation(nodeName string) (operation, bool) {
	operations := q.operationsPerNode[nodeName]
	if len(operations) == 0 {
		delete(q.operationsPerNode, nodeName)
		return operation{}, false
	}

	return operations[0], true
}

// dropNextOperation drops a top operation from the queue.
func (q *operationQueue) dropNextOperation(nodeName string) {
	operations := q.operationsPerNode[nodeName]
	if len(operations) == 0 {
		delete(q.operationsPerNode, nodeName)
		return
	}

	q.operationsPerNode[nodeName] = operations[1:]
}

func squashOperations(operations []operation) []operation {
	if len(operations) == 0 {
		// Nothing to do.
		return operations
	}

	var resizeOperations []operation
	var reconcileNodeStateOp *operation
	for _, op := range operations {
		// If there are multiple reconcile node state operations (this should never happen), we want the last one.
		if op.reconcileNodeState != nil {
			reconcileNodeStateOp = &op
			continue
		}
		if op.resize != nil {
			resizeOperations = append(resizeOperations, op)
		}
	}
	// Reconcile node state operations should always take priority over everything else.
	if reconcileNodeStateOp != nil {
		return []operation{*reconcileNodeStateOp}
	}
	// If we have any resize operations, we don't handle fix operations.
	if len(resizeOperations) != 0 {
		return squashResizeOperations(resizeOperations)
	}

	// At this point, operations contains only fix operations.
	return []operation{operations[0]}
}

func squashResizeOperations(operations []operation) []operation {
	// If we have any resize operations, we remove fix operations.
	if len(operations) == 0 {
		// Nothing to do.
		return operations
	}
	startingSize := operations[0].resize.StartingSize
	desiredSize := operations[len(operations)-1].resize.DesiredSize
	if desiredSize.IsUpsizeFrom(startingSize) || desiredSize.IsDownsizeFrom(startingSize) {
		// Pure operation.
		op := ResizeOperation{
			NodeName:     operations[0].resize.NodeName,
			StartingSize: startingSize,
			DesiredSize:  desiredSize,
		}
		return []operation{{
			resize: &op,
		}}
	}
	// Mixed operation. Break into upsize+downsize.
	// Middle state is always valid:
	// - Since we're taking CPU and memory values from valid changes, they're
	//   individually valid values (between min and max values, using proper increment).
	// - Proportion between memory and CPU is preserved, see go/ek-operations-transient-state-validity.
	middleState := size.MaxSize(startingSize, desiredSize)
	upsizeOperation := ResizeOperation{
		NodeName:     operations[0].resize.NodeName,
		StartingSize: startingSize,
		DesiredSize:  middleState,
	}
	downsizeOperation := ResizeOperation{
		NodeName:     operations[0].resize.NodeName,
		StartingSize: middleState,
		DesiredSize:  desiredSize,
	}
	return []operation{{resize: &upsizeOperation}, {resize: &downsizeOperation}}
}

func countUpsizes(operations []operation) int {
	upsizes := 0
	for _, op := range operations {
		if isUpsize(op) {
			upsizes++
		}
	}

	return upsizes
}

func isUpsize(op operation) bool {
	return op.resize != nil && op.resize.DesiredSize.IsUpsizeFrom(op.resize.StartingSize)
}

func (q *operationQueue) IsNodeInProcess(nodeName string) bool {
	_, ok := q.isNodeInProcess.Load(nodeName)
	return ok
}

func (q *operationQueue) IsNodeResizingOrPending(nodeName string) bool {
	q.mux.Lock()
	defer q.mux.Unlock()

	// check if node is resizing right now
	if q.isNodeResizing(nodeName) {
		return true
	}
	// check if node is in resize queue
	return countResizes(q.operationsPerNode[nodeName]) > 0
}

func (q *operationQueue) isNodeResizing(nodeName string) bool {
	v, ok := q.isNodeInProcess.Load(nodeName)
	if !ok {
		return false
	}
	return v.(operation).resize != nil
}

func countResizes(operations []operation) int {
	resizes := 0
	for _, op := range operations {
		if op.resize != nil {
			resizes++
		}
	}
	return resizes
}
