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

package fairness

import (
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

type FairnessEnforcer interface {
	// Admit reports whether the processor may run in this loop. Each
	// processor calls it exactly once per loop. wantsToRun tells whether the
	// processor has work to do: a processor that has nothing to do passes
	// false and is not admitted, and its turn goes to the next processor
	// instead of being wasted.
	Admit(unschedulablePods []*apiv1.Pod, wantsToRun bool) bool
}

// unhelpablePodTracker reports whether a pod is currently considered unhelpable by CA.
type unhelpablePodTracker interface {
	IsPodUnhelpable(pod *apiv1.Pod) bool
}

type enforcer struct {
	processorName      string
	previouslyAdmitted bool
	lastAdmitted       time.Time
	maxDelay           time.Duration
	clock              clock.PassiveClock
	unhelpableTracker  unhelpablePodTracker
}

// NewFairnessEnforcer creates a standalone fairness tracker for a single processor.
func NewFairnessEnforcer(processorName string, maxDelay time.Duration, unhelpableTracker unhelpablePodTracker) FairnessEnforcer {
	return newEnforcerWithClock(processorName, maxDelay, clock.RealClock{}, unhelpableTracker)
}

func newEnforcerWithClock(processorName string, maxDelay time.Duration, c clock.PassiveClock, unhelpableTracker unhelpablePodTracker) *enforcer {
	return &enforcer{
		processorName:     processorName,
		maxDelay:          maxDelay,
		clock:             c,
		lastAdmitted:      c.Now(),
		unhelpableTracker: unhelpableTracker,
	}
}

// Admit reports whether the processor may run in this loop. When wantsToRun
// is false, Admit returns false and leaves the enforcer's state unchanged, as
// if it had not been called: a standalone enforcer has no other processor to
// pass the turn to.
func (f *enforcer) Admit(unschedulablePods []*apiv1.Pod, wantsToRun bool) bool {
	if !wantsToRun {
		return false
	}
	admitted := f.admitInternal(unschedulablePods)
	if admitted {
		f.lastAdmitted = f.clock.Now()
		klog.Infof("Fairness Enforcer: Admitting %s processor to run", f.processorName)
	}
	f.previouslyAdmitted = admitted
	return admitted
}

func (f *enforcer) admitInternal(unschedulablePods []*apiv1.Pod) bool {
	if len(unschedulablePods) == 0 {
		return true
	}
	if f.previouslyAdmitted {
		return false
	}
	if allUnhelpable(unschedulablePods, f.unhelpableTracker) {
		return true
	}
	return f.clock.Since(f.lastAdmitted) > f.maxDelay
}

func isUnhelpable(p *apiv1.Pod, unhelpableTracker unhelpablePodTracker) bool {
	return unhelpableTracker != nil && unhelpableTracker.IsPodUnhelpable(p)
}

func allUnhelpable(ps []*apiv1.Pod, unhelpableTracker unhelpablePodTracker) bool {
	for _, p := range ps {
		if !isUnhelpable(p, unhelpableTracker) {
			return false
		}
	}
	return true
}
