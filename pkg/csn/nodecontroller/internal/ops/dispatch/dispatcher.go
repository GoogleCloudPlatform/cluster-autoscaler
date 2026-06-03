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
	"context"
	"sync"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops/retry"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/set"
)

const logPrefix = "CSN Operation Dispatcher:"

type Queue interface {
	Dequeue(ctx context.Context) (ops.Operation, bool)
	Enqueue(o ops.Operation) error
}
type ClearPendingOperationF func(op ops.OperationType, nodeNames set.Set[string])

// Dispatcher is responsible for starting up and coordinating worker
// goroutines which perform operations related to Cold Standby Nodes.
type Dispatcher struct {
	queue                 Queue
	clearPendingOperation ClearPendingOperationF
	handlers              map[ops.OperationType]ops.OperationHandler
	workerCount           int
	backoffManager        *retry.BackoffManager
}

// NewDispatcher returns a concrete Dispatcher struct.
// It uses the queue to dequeue and enqueue operations.
func NewDispatcher(workerCount int, retryCfg retry.Config, queue Queue, clearOpF ClearPendingOperationF) *Dispatcher {
	return &Dispatcher{
		queue:                 queue,
		clearPendingOperation: clearOpF,
		handlers:              make(map[ops.OperationType]ops.OperationHandler),
		workerCount:           workerCount,
		backoffManager: retry.NewBackoffManager(
			clock.RealClock{},
			retryCfg,
			queue.Enqueue,
		),
	}
}

func (d *Dispatcher) RegisterHandler(opType ops.OperationType, handler ops.OperationHandler) {
	d.handlers[opType] = handler
}

func (d *Dispatcher) Run(stopCh <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stopCh
		cancel()
	}()
	go d.backoffManager.Run(ctx)

	var wg sync.WaitGroup
	for i := 0; i < d.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.workerLoop(ctx)
		}()
	}
	wg.Wait()
}

func (d *Dispatcher) workerLoop(ctx context.Context) {
	for {
		op, ok := d.queue.Dequeue(ctx)
		if !ok {
			return
		}
		if len(op.NodeNames) == 0 {
			// No point in processing an operation without nodes.
			continue
		}
		klog.V(4).Infof("%s starting op %q for nodes: %v", logPrefix, op.Type.String(), op.NodeNames)

		handler, ok := d.handlers[op.Type]
		if !ok {
			klog.Errorf("%s no handler for operation type %v", logPrefix, op.Type)
			continue
		}

		beforeOp := time.Now()
		res, err := handler(ctx, op)
		opLatencySeconds.WithLabelValues(op.Type.String()).Observe(time.Since(beforeOp).Seconds())
		if err != nil {
			klog.Errorf("%s error handling operation %v: %v", logPrefix, op, err)
			d.clearPendingOperation(op.Type, op.NodeNames)
			continue
		}
		if len(res.Success) > 0 {
			klog.V(4).Infof("%s op %q returned successfully for nodes: %v", logPrefix, op.Type.String(), res.Success)
			d.clearPendingOperation(op.Type, res.Success)
			opResultsTotal.WithLabelValues(op.Type.String(), opSuccess).Add(float64(len(res.Success)))
		}
		d.handleBackoff(op, res)
	}
}

// NodesAwaitingRetry returns the number of node-opType pairs that
// are still waiting to be retried.
func (d *Dispatcher) NodesAwaitingRetry() int {
	return len(d.backoffManager.RetryCounts())
}

func (d *Dispatcher) handleBackoff(op ops.Operation, res ops.Result) {
	if len(res.Success) > 0 {
		d.backoffManager.ClearRetryCount(op.Type, res.Success)
	}
	if len(res.Errs) == 0 {
		return
	}
	klog.V(4).Infof("%s nodes to enter backoff for op %q: %v", logPrefix, op.Type.String(), res.Errs)
	backoffResult := d.backoffManager.AddFailedNodes(op, set.KeySet(res.Errs))

	// Track retryable failures (nodes processed for backoff)
	retryableCount := len(res.Errs) - len(backoffResult.FailedNodes)
	if retryableCount > 0 {
		opResultsTotal.WithLabelValues(op.Type.String(), opRetryFailure).Add(float64(retryableCount))
	}

	if len(backoffResult.FailedNodes) == 0 {
		return
	}

	// Track permanent failures
	opResultsTotal.WithLabelValues(op.Type.String(), opFailure).Add(float64(len(backoffResult.FailedNodes)))

	// If the operation is considered a permanent failure,
	// then pending operation should be cleared.
	d.clearPendingOperation(op.Type, backoffResult.FailedNodes)
}
