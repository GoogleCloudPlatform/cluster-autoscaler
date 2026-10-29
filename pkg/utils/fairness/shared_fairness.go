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

// IMPORTANT: The owner of the shared enforcer manager calls `StartLoop` once at the
// start of every fairness loop, before any processor runs. Every registered processor
// then calls the `Admit` method of its `sharedEnforcer` instance exactly once per loop,
// passing wantsToRun=false when it has nothing to do. Failure to do so (e.g., by
// conditionally skipping the call) can lead to starvation for other processors, as the
// round-robin turn will not advance.
package fairness

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// sharedEnforcerManager orchestrates multiple processors sharing a global loop-counting throttle.
// It keeps track of registered processors, their state, and enforces strict round-robin
// turns to ensure fairness across different processors.
// It is the central state holder for the shared fairness logic.
type sharedEnforcerManager struct {
	// processors maps processor names to their corresponding sharedEnforcer instances.
	processors map[string]*sharedEnforcer
	// currentTurn is the index of the processor whose turn it is to be admitted.
	currentTurn int
	// admittedInThisLoop tracks if any processor was admitted in the current fairness loop.
	admittedInThisLoop bool
	// currentLoop is the current loop index
	currentLoop int
	// lastAdmittedLoop is the loop index when a processor was last admitted
	lastAdmittedLoop int
	// maximum number of loops to wait before admitting a processor
	maxLoopsBeforeAdmission int
	unhelpableTracker       unhelpablePodTracker
}

func NewSharedEnforcerManager(maxLoopsBeforeAdmission int, unhelpableTracker unhelpablePodTracker) *sharedEnforcerManager {
	return &sharedEnforcerManager{
		processors:              make(map[string]*sharedEnforcer),
		lastAdmittedLoop:        0,
		maxLoopsBeforeAdmission: maxLoopsBeforeAdmission,
		unhelpableTracker:       unhelpableTracker,
	}
}

func (m *sharedEnforcerManager) CreateEnforcer(processorName string) FairnessEnforcer {
	if enforcer, exists := m.processors[processorName]; exists {
		return enforcer
	}
	index := len(m.processors)
	enforcer := &sharedEnforcer{
		manager:       m,
		processorName: processorName,
		index:         index,
	}
	m.processors[processorName] = enforcer
	return enforcer
}

// StartLoop starts a new fairness loop. Call it once per loop, before any
// processor calls Admit, so that every processor sees the same loop
// regardless of the order the processors run in.
func (m *sharedEnforcerManager) StartLoop() {
	m.admittedInThisLoop = false
	m.currentLoop++
}

// sharedEnforcer is a lightweight wrapper around sharedEnforcerManager that implements
// the FairnessEnforcer interface. This indirection allows individual processors to use
// the standard FairnessEnforcer API without needing to know about the manager or pass
// their specific processor name on every call, as the name is bound to this instance.
type sharedEnforcer struct {
	manager       *sharedEnforcerManager
	processorName string
	index         int
}

func (e *sharedEnforcer) Admit(unschedulablePods []*apiv1.Pod, wantsToRun bool) bool {
	m := e.manager

	// Another processor was already admitted in this loop.
	if m.admittedInThisLoop {
		return false
	}

	// Check if it is this processor's turn.
	if e.index != m.currentTurn {
		return false
	}

	// A processor with nothing to do passes its turn, so that the next
	// processor can be admitted in this loop.
	if !wantsToRun {
		m.passTurn()
		klog.V(4).Infof("Shared Fairness Enforcer: %s processor passed its turn", e.processorName)
		return false
	}

	// Run fairness logic.
	admitted := m.admitInternal(unschedulablePods)

	if admitted {
		m.lastAdmittedLoop = m.currentLoop
		m.admittedInThisLoop = true
		m.passTurn()
		klog.Infof("Shared Fairness Enforcer: Admitting %s processor to run", e.processorName)
	}
	return admitted
}

// passTurn gives the turn to the next registered processor.
func (m *sharedEnforcerManager) passTurn() {
	m.currentTurn = (m.currentTurn + 1) % len(m.processors)
}

func (m *sharedEnforcerManager) admitInternal(unschedulablePods []*apiv1.Pod) bool {
	if len(unschedulablePods) == 0 {
		return true
	}
	loopsBetweenAdmissions := m.maxLoopsBeforeAdmission
	if allUnhelpable(unschedulablePods, m.unhelpableTracker) {
		loopsBetweenAdmissions /= 2
	}
	return m.currentLoop-m.lastAdmittedLoop >= loopsBetweenAdmissions
}
