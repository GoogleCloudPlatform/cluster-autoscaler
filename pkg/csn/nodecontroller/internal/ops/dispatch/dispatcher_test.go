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
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops/retry"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/utils/set"
)

type fakeQueue struct {
	opsCh chan ops.Operation
}

func (m *fakeQueue) Dequeue(ctx context.Context) (ops.Operation, bool) {
	select {
	case op, ok := <-m.opsCh:
		return op, ok
	case <-ctx.Done():
		return ops.Operation{}, false
	}
}

func (m *fakeQueue) Enqueue(o ops.Operation) error {
	m.opsCh <- o
	return nil
}

type clearPendingOpCall struct {
	Op        ops.OperationType
	NodeNames set.Set[string]
}

func toUpdatePendingOpCalls(calls []clearPendingOpCall) []statetest.SetPendingOperationCall {
	setCalls := make([]statetest.SetPendingOperationCall, 0, len(calls))
	for _, call := range calls {
		setCalls = append(setCalls, statetest.SetPendingOperationCall{Pending: false, NodeNames: call.NodeNames, Op: call.Op})
	}
	return setCalls
}

type fakeHandler struct {
	mutex          sync.Mutex
	HandleChan     chan ops.OperationType
	Err            error
	NodeErrs       map[string]error
	ErrsHappenOnce bool
}

func (f *fakeHandler) Handle(_ context.Context, op ops.Operation) (ops.Result, error) {
	if f.Err != nil {
		return ops.NewResult(), f.Err
	}
	f.HandleChan <- op.Type
	f.mutex.Lock()
	errs := maps.Clone(f.NodeErrs)
	// errors only work for one call
	if f.ErrsHappenOnce {
		maps.DeleteFunc(f.NodeErrs, func(k string, _ error) bool {
			return op.NodeNames.Has(k)
		})
	}
	f.mutex.Unlock()
	maps.DeleteFunc(errs, func(k string, _ error) bool {
		return !op.NodeNames.Has(k)
	})
	return ops.Result{
		Success: op.NodeNames.Difference(set.KeySet(errs)),
		Errs:    errs,
	}, nil
}

func TestDispatcher(t *testing.T) {
	mig1 := gce.GceRef{Name: "mig1"}

	tests := []struct {
		name                  string
		ops                   []ops.Operation
		workerCount           int
		errs                  map[ops.OperationType]error
		nodeErrs              map[string]error
		handlerErrsHappenOnce bool
		// map of op type to expected count of processed operations
		expectedCounts              map[ops.OperationType]int
		expectedClearPendingOpCalls []clearPendingOpCall
	}{
		{
			name: "process_mixed_ops",
			ops: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1")},
				{MIG: mig1, Type: ops.ConsumeOp, NodeNames: set.New("n2", "n4", "n5")},
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n3")},
			},
			workerCount: 2,
			nodeErrs: map[string]error{
				"n4": errors.New("some-error"),
			},
			handlerErrsHappenOnce: true,
			expectedCounts: map[ops.OperationType]int{
				ops.SuspendOp: 2,
				// Even though only one consume operation is processed,
				// it is expected for node `n4` to be retried because of
				// the per-node error.
				ops.ConsumeOp: 2,
			},
			expectedClearPendingOpCalls: []clearPendingOpCall{
				{Op: ops.SuspendOp, NodeNames: set.New("n1")},
				{Op: ops.ConsumeOp, NodeNames: set.New("n2", "n5")},
				{Op: ops.SuspendOp, NodeNames: set.New("n3")},
				// Separate call for `n4` because of retry.
				{Op: ops.ConsumeOp, NodeNames: set.New("n4")},
			},
		},
		{
			name:           "empty_queue",
			ops:            []ops.Operation{},
			workerCount:    1,
			expectedCounts: map[ops.OperationType]int{},
		},
		{
			name:           "operation_without_nodes_is_no_op",
			ops:            []ops.Operation{{MIG: mig1, Type: ops.SuspendOp}},
			workerCount:    1,
			expectedCounts: map[ops.OperationType]int{},
		},
		{
			name: "skips_unknown_operation",
			ops: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1")},
				{MIG: mig1, Type: ops.OperationType(3), NodeNames: set.New("n2")},
				{MIG: mig1, Type: ops.ConsumeOp, NodeNames: set.New("n3")},
			},
			workerCount: 1,
			expectedCounts: map[ops.OperationType]int{
				ops.SuspendOp: 1,
				ops.ConsumeOp: 1,
			},
			expectedClearPendingOpCalls: []clearPendingOpCall{
				{Op: ops.SuspendOp, NodeNames: set.New("n1")},
				{Op: ops.ConsumeOp, NodeNames: set.New("n3")},
			},
		},
		{
			name: "skips_error",
			ops: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1")},
				{MIG: mig1, Type: ops.ConsumeOp, NodeNames: set.New("n2")},
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n3")},
			},
			workerCount: 1,
			errs: map[ops.OperationType]error{
				ops.ConsumeOp: errors.New("some-error"),
			},
			expectedCounts: map[ops.OperationType]int{
				ops.SuspendOp: 2,
			},
			expectedClearPendingOpCalls: []clearPendingOpCall{
				{Op: ops.SuspendOp, NodeNames: set.New("n1")},
				{Op: ops.ConsumeOp, NodeNames: set.New("n2")},
				{Op: ops.SuspendOp, NodeNames: set.New("n3")},
			},
		},
		{
			name: "permanent_failure_should_eventually_be_cleared",
			ops: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1")},
			},
			workerCount: 1,
			nodeErrs: map[string]error{
				"n1": errors.New("some-error"),
			},
			expectedCounts: map[ops.OperationType]int{
				// first call + 6 retry attempts
				ops.SuspendOp: 7,
			},
			expectedClearPendingOpCalls: []clearPendingOpCall{
				{Op: ops.SuspendOp, NodeNames: set.New("n1")},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opsCh := make(chan ops.Operation, len(tc.ops))
			for _, op := range tc.ops {
				opsCh <- op
			}

			q := &fakeQueue{opsCh: opsCh}
			sm := &statetest.MockStateManager{}
			d := NewDispatcher(
				tc.workerCount,
				retry.Config{
					MaxRetries:   6,
					InitialDelay: 5 * time.Nanosecond,
					MaxDelay:     10 * time.Nanosecond,
				},
				q,
				func(op ops.OperationType, nodeNames set.Set[string]) {
					sm.SetPendingOperation(op, false, nodeNames)
				},
			)

			handlers := make(map[ops.OperationType]*fakeHandler)
			handleChan := make(chan ops.OperationType, len(tc.ops))
			for _, opType := range []ops.OperationType{ops.SuspendOp, ops.ConsumeOp} {
				handler := &fakeHandler{
					HandleChan:     handleChan,
					Err:            tc.errs[opType],
					NodeErrs:       maps.Clone(tc.nodeErrs),
					ErrsHappenOnce: tc.handlerErrsHappenOnce,
				}
				handlers[opType] = handler
				d.RegisterHandler(opType, handler.Handle)
			}
			ctx, cancel := context.WithCancel(context.Background())
			// Ensure cleanup of the cancellation goroutine in Run
			defer cancel()

			// Run blocks until workers exit (which happens when opsCh is drained)
			dispatcherDone := make(chan bool)
			go func() {
				d.Run(ctx)
				dispatcherDone <- true
			}()

			opCounter := make(map[ops.OperationType]int)
			// TODO(b/493892863): Consider refactoring this part to make errors
			// clearer and avoid timeouts.
			for {
				if maps.Equal(opCounter, tc.expectedCounts) {
					break
				}
				opCounter[<-handleChan]++
			}
			close(opsCh)
			<-dispatcherDone
			assert.ElementsMatch(t, toUpdatePendingOpCalls(tc.expectedClearPendingOpCalls), sm.GetPendingOperationUpdateCalls())
			assert.Zero(t, d.NodesAwaitingRetry())
		})
	}
}
