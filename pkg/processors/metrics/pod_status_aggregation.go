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

package metrics_processors

import (
	"sync"

	apiv1 "k8s.io/api/core/v1"
)

// PodStatusAggregator keeps information about state (pending / schedulable / etc) of
// all pods collected at various points of autoscaler logic.
//
// The aggregator is shared between the main autoscaler loop (which updates it via
// the pod list processor) and the scale-up status processors (which read it).
// With asynchronous node group provisioning (e.g. NAP), the scale-up status
// processors can run on a separate goroutine concurrently with the main loop, so
// access to the aggregated state must be synchronized.
type PodStatusAggregator struct {
	mu sync.RWMutex
	// unschedulable is a list of unschedulable pods considered in this loop.
	unschedulable []*apiv1.Pod
}

// NewPodStatusAggregator creates and returns a new PodStatusAggregator
func NewPodStatusAggregator() *PodStatusAggregator {
	return &PodStatusAggregator{}
}

// SetUnschedulable stores a copy of the unschedulable pods considered in the
// current loop. It is safe to call concurrently with GetUnschedulable.
func (a *PodStatusAggregator) SetUnschedulable(pods []*apiv1.Pod) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.unschedulable = append([]*apiv1.Pod{}, pods...)
}

// GetUnschedulable returns the unschedulable pods considered in the current
// loop. It is safe to call concurrently with SetUnschedulable. The returned
// slice must not be mutated by callers.
func (a *PodStatusAggregator) GetUnschedulable() []*apiv1.Pod {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.unschedulable
}
