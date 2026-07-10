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

package fake

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// Evaluator is a fake implementation of experiments.Evaluator for testing.
type Evaluator struct {
	boolFlags   map[string]bool
	stringFlags map[string]string
}

// NewEvaluator creates a new fake Evaluator.
func NewEvaluator(boolFlags map[string]bool, stringFlags map[string]string) *Evaluator {
	return &Evaluator{
		boolFlags:   boolFlags,
		stringFlags: stringFlags,
	}
}

// UpdateReleaseChannel implements experiments.Evaluator.
func (f *Evaluator) UpdateReleaseChannel(_ string) {}

// EvaluateStringFlagOrFailsafe implements experiments.Evaluator.
func (f *Evaluator) EvaluateStringFlagOrFailsafe(flag, failsafe string) string {
	if val, ok := f.stringFlags[flag]; ok {
		return val
	}
	return failsafe
}

// EvaluateBoolFlagOrFailsafe implements experiments.Evaluator.
func (f *Evaluator) EvaluateBoolFlagOrFailsafe(flag string, failsafe bool) bool {
	if val, ok := f.boolFlags[flag]; ok {
		return val
	}
	return failsafe
}

// DirectLaunchBoolFlag implements experiments.Evaluator.
func (f *Evaluator) DirectLaunchBoolFlag(flag string) bool {
	if val, ok := f.boolFlags[flag]; ok && !val {
		return false
	}
	return true
}

// SubscribeToUpdate implements experiments.Evaluator.
func (f *Evaluator) SubscribeToUpdate(subscriber experiments.Subscriber) {}
