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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/ops/retry"
	statetest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/state/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller/internal/test"
	"k8s.io/utils/set"
)

func TestDispatcherMetrics(t *testing.T) {
	mig1 := gce.GceRef{Name: "mig1"}

	tests := []struct {
		name              string
		ops               []ops.Operation
		nodeErrs          map[string]error
		retryConfig       retry.Config
		expectedCallCount int
		expectedDeltas    []*test.MetricDelta
	}{
		{
			name: "success_metrics",
			ops: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1", "n2", "n3")},
			},
			retryConfig:       retry.Config{},
			expectedCallCount: 1,
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.Positive(), opLatencySeconds, []string{ops.SuspendOp.String(), "1"}),
				test.NewMetricDelta(test.ExpectedValue(3), opResultsTotal, []string{ops.SuspendOp.String(), opSuccess}),
			},
		},
		{
			name: "retryable_failure_metrics",
			ops: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1", "n2")},
			},
			nodeErrs: map[string]error{
				"n1": errors.New("some-error"),
			},
			retryConfig:       retry.Config{MaxRetries: 1},
			expectedCallCount: 2,
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.Positive(), opLatencySeconds, []string{ops.SuspendOp.String(), "1"}),
				test.NewMetricDelta(test.ExpectedValue(1), opResultsTotal, []string{ops.SuspendOp.String(), opRetryFailure}),
				test.NewMetricDelta(test.ExpectedValue(1), opResultsTotal, []string{ops.SuspendOp.String(), opSuccess}),
			},
		},
		{
			name: "permanent_failure_metrics_immediate",
			ops: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New("n1", "n2")},
			},
			nodeErrs: map[string]error{
				"n1": errors.New("some-error"),
			},
			retryConfig: retry.Config{
				MaxRetries: 0, // Exceeds max retries immediately if it fails!
			},
			expectedCallCount: 1,
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.Positive(), opLatencySeconds, []string{ops.SuspendOp.String(), "1"}),
				test.NewMetricDelta(test.ExpectedValue(1), opResultsTotal, []string{ops.SuspendOp.String(), opFailure}),
				test.NewMetricDelta(test.ExpectedValue(1), opResultsTotal, []string{ops.SuspendOp.String(), opSuccess}),
			},
		},
		{
			name: "metric_with_no_nodes",
			ops: []ops.Operation{
				{MIG: mig1, Type: ops.SuspendOp, NodeNames: set.New[string]()},
			},
			retryConfig:       retry.Config{},
			expectedCallCount: 0,
			expectedDeltas: []*test.MetricDelta{
				test.NewMetricDelta(test.ExpectedValue(0), opLatencySeconds, []string{ops.SuspendOp.String(), "0"}),
				test.NewMetricDelta(test.ExpectedValue(0), opResultsTotal, []string{ops.SuspendOp.String(), opSuccess}),
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
				1,
				tc.retryConfig,
				q,
				func(op ops.OperationType, nodeNames set.Set[string]) {
					sm.SetPendingOperation(op, false, nodeNames)
				},
			)

			handler := &fakeHandler{
				HandleChan: make(chan ops.OperationType, len(tc.ops)*5),
				NodeErrs:   tc.nodeErrs,
			}
			d.RegisterHandler(ops.SuspendOp, handler.Handle)

			for _, ed := range tc.expectedDeltas {
				ed.Init(t)
			}

			ctx, cancel := context.WithCancel(context.Background())
			dispatcherDone := make(chan bool)
			go func() {
				d.Run(ctx)
				dispatcherDone <- true
			}()

			// Wait for expected calls to handler to ensure metrics are recorded
			for range tc.expectedCallCount {
				<-handler.HandleChan
			}

			cancel()
			<-dispatcherDone

			// Verify after
			for _, ed := range tc.expectedDeltas {
				assert.NoError(t, ed.Verify(t))
			}
		})
	}
}
