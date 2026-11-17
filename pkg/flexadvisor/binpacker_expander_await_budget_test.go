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

package flexadvisor

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestBinpackerExpanderAwaitBudget(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		wantCanConsume     bool
		wantFirstTimeout   time.Duration
		wantFirstConsumed  bool
		wantUsedAfterFirst bool
	}{
		{
			name:               "nil experiments manager - returns (0, false) and leaves budget unused",
			experimentsManager: nil,
			wantCanConsume:     false,
			wantFirstTimeout:   0,
			wantFirstConsumed:  false,
			wantUsedAfterFirst: false,
		},
		{
			name:               "experiment disabled - returns (0, false) and leaves budget unused",
			experimentsManager: binpackerBlockTestManager(false, nil),
			wantCanConsume:     false,
			wantFirstTimeout:   0,
			wantFirstConsumed:  false,
			wantUsedAfterFirst: false,
		},
		{
			name: "general FlexAdvisorProcessing disabled - returns (0, false) and leaves budget unused",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:          false,
				experiments.FlexAdvisorBinpackerBlockEnabledFlag:      true,
				experiments.FlexAdvisorBinpackerBlockMinCAVersionFlag: true,
			}, nil),
			wantCanConsume:     false,
			wantFirstTimeout:   0,
			wantFirstConsumed:  false,
			wantUsedAfterFirst: false,
		},
		{
			name:               "experiment enabled with default budget - returns (3s, true) and marks budget used",
			experimentsManager: binpackerBlockTestManager(true, nil),
			wantCanConsume:     true,
			wantFirstTimeout:   3 * time.Second,
			wantFirstConsumed:  true,
			wantUsedAfterFirst: true,
		},
		{
			name: "experiment enabled with custom 5s budget - returns (5s, true) and marks budget used",
			experimentsManager: binpackerBlockTestManager(true, map[string]string{
				experiments.FlexAdvisorBinpackerBlockTimeBudgetFlag: "5",
			}),
			wantCanConsume:     true,
			wantFirstTimeout:   5 * time.Second,
			wantFirstConsumed:  true,
			wantUsedAfterFirst: true,
		},
		{
			name: "experiment enabled with 0s budget - returns (0s, true) and marks budget used",
			experimentsManager: binpackerBlockTestManager(true, map[string]string{
				experiments.FlexAdvisorBinpackerBlockTimeBudgetFlag: "0",
			}),
			wantCanConsume:     true,
			wantFirstTimeout:   0,
			wantFirstConsumed:  true,
			wantUsedAfterFirst: true,
		},
		{
			name: "experiment enabled with negative budget (-5s) - falls back to (3s, true) and marks budget used",
			experimentsManager: binpackerBlockTestManager(true, map[string]string{
				experiments.FlexAdvisorBinpackerBlockTimeBudgetFlag: "-5",
			}),
			wantCanConsume:     true,
			wantFirstTimeout:   3 * time.Second,
			wantFirstConsumed:  true,
			wantUsedAfterFirst: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			budget := NewBinpackerExpanderAwaitBudget(tc.experimentsManager)
			assert.False(t, budget.IsUsed())
			assert.Equal(t, tc.wantCanConsume, budget.CanConsume())

			gotFirst, gotConsumed := budget.Consume()
			assert.Equal(t, tc.wantFirstTimeout, gotFirst)
			assert.Equal(t, tc.wantFirstConsumed, gotConsumed)
			assert.Equal(t, tc.wantUsedAfterFirst, budget.IsUsed())
			assert.False(t, budget.CanConsume())

			gotSecond, gotSecondConsumed := budget.Consume()
			assert.Equal(t, time.Duration(0), gotSecond)
			assert.False(t, gotSecondConsumed)

			budget.OnScaleUpEnd()
			assert.False(t, budget.IsUsed())
			assert.Equal(t, tc.wantCanConsume, budget.CanConsume())

			gotAfterReset, gotAfterResetConsumed := budget.Consume()
			assert.Equal(t, tc.wantFirstTimeout, gotAfterReset)
			assert.Equal(t, tc.wantFirstConsumed, gotAfterResetConsumed)
		})
	}
}

func TestBinpackerExpanderAwaitBudget_ConcurrentConsumeAndReset(t *testing.T) {
	budget := NewBinpackerExpanderAwaitBudget(binpackerBlockTestManager(true, nil))

	for range 2 {
		const workers = 16
		start := make(chan struct{})
		type result struct {
			timeout  time.Duration
			consumed bool
		}
		results := make([]result, workers)
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := range workers {
			go func(idx int) {
				defer wg.Done()
				<-start
				timeout, ok := budget.Consume()
				results[idx] = result{timeout: timeout, consumed: ok}
			}(i)
		}
		close(start)
		wg.Wait()

		winners := 0
		for _, r := range results {
			if r.consumed {
				winners++
				assert.Equal(t, 3*time.Second, r.timeout)
			} else {
				assert.Equal(t, time.Duration(0), r.timeout)
			}
		}
		assert.Equal(t, 1, winners)
		assert.True(t, budget.IsUsed())

		budget.Reset()
		assert.False(t, budget.IsUsed())
	}
}
