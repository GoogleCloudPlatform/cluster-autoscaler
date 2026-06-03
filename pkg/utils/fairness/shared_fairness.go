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

// IMPORTANT: All registered processors are expected to call the `Admit` method of their
// respective `sharedEnforcer` instance in every iteration of the fairness loop. Failure
// to do so (e.g., by conditionally skipping the call) can lead to starvation for other
// processors, as the round-robin turn will not advance.
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
}

func NewSharedEnforcerManager(maxLoopsBeforeAdmission int) *sharedEnforcerManager {
	return &sharedEnforcerManager{
		processors:              make(map[string]*sharedEnforcer),
		lastAdmittedLoop:        -1,
		maxLoopsBeforeAdmission: maxLoopsBeforeAdmission,
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

// sharedEnforcer is a lightweight wrapper around sharedEnforcerManager that implements
// the FairnessEnforcer interface. This indirection allows individual processors to use
// the standard FairnessEnforcer API without needing to know about the manager or pass
// their specific processor name on every call, as the name is bound to this instance.
type sharedEnforcer struct {
	manager       *sharedEnforcerManager
	processorName string
	index         int
}

func (e *sharedEnforcer) Admit(unschedulablePods []*apiv1.Pod) bool {
	m := e.manager

	// Reset loop state when the first registered processor is called.
	// We assume processors are called in the order they were created (by index).
	if e.index == 0 {
		m.admittedInThisLoop = false
		m.currentLoop++
	}

	// Another processor was already admitted in this loop.
	if m.admittedInThisLoop {
		return false
	}

	// Check if it is this processor's turn.
	if e.index != m.currentTurn {
		return false
	}

	// Run fairness logic.
	admitted := m.admitInternal(unschedulablePods)

	if admitted {
		m.lastAdmittedLoop = m.currentLoop
		m.admittedInThisLoop = true
		m.currentTurn = (m.currentTurn + 1) % len(m.processors)
		klog.Infof("Shared Fairness Enforcer: Admitting %s processor to run", e.processorName)
	}
	return admitted
}

func (m *sharedEnforcerManager) admitInternal(unschedulablePods []*apiv1.Pod) bool {
	if len(unschedulablePods) == 0 {
		return true
	}
	if m.lastAdmittedLoop == -1 {
		return true
	}
	loopsBetweenAdmissions := m.maxLoopsBeforeAdmission
	if allUnhelpable(unschedulablePods) {
		loopsBetweenAdmissions /= 2
	}
	return m.currentLoop-m.lastAdmittedLoop >= loopsBetweenAdmissions
}
