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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/annotator"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

type FairnessEnforcer interface {
	Admit(unschedulablePods []*apiv1.Pod) bool
}

type enforcer struct {
	processorName      string
	previouslyAdmitted bool
	lastAdmitted       time.Time
	maxDelay           time.Duration
	clock              clock.PassiveClock
}

// NewFairnessEnforcer creates a standalone fairness tracker for a single processor.
func NewFairnessEnforcer(processorName string, maxDelay time.Duration) FairnessEnforcer {
	return newEnforcerWithClock(processorName, maxDelay, clock.RealClock{})
}

func newEnforcerWithClock(processorName string, maxDelay time.Duration, c clock.PassiveClock) *enforcer {
	return &enforcer{
		processorName: processorName,
		maxDelay:      maxDelay,
		clock:         c,
		lastAdmitted:  c.Now(),
	}
}

func (f *enforcer) Admit(unschedulablePods []*apiv1.Pod) bool {
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
	if allUnhelpable(unschedulablePods) {
		return true
	}
	return f.clock.Since(f.lastAdmitted) > f.maxDelay
}

func isUnhelpable(p *apiv1.Pod) bool {
	return p.Annotations != nil && p.Annotations[annotator.UnhelpableUntilAnnotation] == annotator.UnhelpableForever
}

func allUnhelpable(ps []*apiv1.Pod) bool {
	for _, p := range ps {
		if !isUnhelpable(p) {
			return false
		}
	}
	return true
}
