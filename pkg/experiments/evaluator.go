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

package experiments

// Evaluator implements interface for evaluating experiment flags.
// There should only be one created and used for the lifetime of GCW.
type Evaluator interface {
	EvaluateStringFlagOrFailsafe(flag, fallback string) string
	EvaluateBoolFlagOrFailsafe(flag string, failsafe bool) bool
	DirectLaunchBoolFlag(flag string) bool
	SubscribeToUpdate(s Subscriber)
	UpdateReleaseChannel(releaseChannel string)
}

type Subscriber func()

// noopEvaluator is used when dedicated experiment evaluation is disabled.
// Always returns failsafe/fallback values for checks.
type noopEvaluator struct{}

func (n *noopEvaluator) EvaluateStringFlagOrFailsafe(_, fallback string) string {
	return fallback
}

func (n *noopEvaluator) EvaluateBoolFlagOrFailsafe(_ string, failsafe bool) bool {
	return failsafe
}

func (n *noopEvaluator) DirectLaunchBoolFlag(_ string) bool {
	return true
}

func (n *noopEvaluator) SubscribeToUpdate(subscriber Subscriber) {}

func (n *noopEvaluator) UpdateReleaseChannel(_ string) {}

// NewNoopEvaluator returns an instance of noopEvaluator.
func NewNoopEvaluator() *noopEvaluator {
	return &noopEvaluator{}
}
