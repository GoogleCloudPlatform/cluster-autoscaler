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
	"sync/atomic"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

const (
	defaultBinpackerBlockTimeBudget = 3 * time.Second
)

// BinpackerExpanderAwaitBudget controls how long bin packing and the Fleet
// Efficiency expander are allowed to block waiting for a scope's first fetch
// during a single scale-up pass.
type BinpackerExpanderAwaitBudget struct {
	experimentsManager experiments.Manager
	used               atomic.Bool
}

// NewBinpackerExpanderAwaitBudget initializes and returns a thread-safe
// BinpackerExpanderAwaitBudget.
func NewBinpackerExpanderAwaitBudget(experimentsManager experiments.Manager) *BinpackerExpanderAwaitBudget {
	return &BinpackerExpanderAwaitBudget{
		experimentsManager: experimentsManager,
	}
}

// CanConsume reports whether the await budget is enabled and has not yet been
// consumed during the current scale-up pass.
func (b *BinpackerExpanderAwaitBudget) CanConsume() bool {
	return b != nil && !b.used.Load() && IsFlexAdvisorBinpackerBlockEnabled(b.experimentsManager)
}

// Consume claims the budget for the current scale-up pass and returns the
// configured blocking timeout duration alongside true if claimed, or (0, false)
// if nil, disabled, or already consumed.
func (b *BinpackerExpanderAwaitBudget) Consume() (time.Duration, bool) {
	if !b.CanConsume() {
		return 0, false
	}
	if !b.used.CompareAndSwap(false, true) {
		return 0, false
	}
	timeout := b.experimentsManager.EvaluateDurationSecondsFlagOrFailsafe(
		experiments.FlexAdvisorBinpackerBlockTimeBudgetFlag,
		defaultBinpackerBlockTimeBudget,
	)
	if timeout < 0 {
		return defaultBinpackerBlockTimeBudget, true
	}
	return timeout, true
}

// IsUsed reports whether the await budget has already been consumed during the
// current scale-up pass.
func (b *BinpackerExpanderAwaitBudget) IsUsed() bool {
	return b != nil && b.used.Load()
}

// Reset clears the used state so the budget can be consumed in the next
// scale-up pass.
func (b *BinpackerExpanderAwaitBudget) Reset() {
	if b != nil {
		b.used.Store(false)
	}
}

// OnScaleUpEnd resets the per-scale-up-pass await budget.
func (b *BinpackerExpanderAwaitBudget) OnScaleUpEnd() {
	b.Reset()
}
