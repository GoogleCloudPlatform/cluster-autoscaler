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

// IsFlexAdvisorSendRankEnabled returns whether sending Rank in FlexAdvisor recommendation requests is dynamically enabled.
func IsFlexAdvisorSendRankEnabled(manager Manager) bool {
	if manager == nil {
		return true
	}

	return manager.EvaluateMinimumVersionFlagOrFailsafe(FlexAdvisorSendRankMinCAVersionFlag, true) &&
		manager.EvaluateBoolFlagOrFailsafe(FlexAdvisorSendRankEnabledFlag, true)
}

// IsFlexAdvisorSkipUnneededSpotConfigsEnabled returns whether skipping unneeded Spot instance configs
// in FlexAdvisor when rule.Spot() is false is dynamically enabled (kill-switch, default on).
func IsFlexAdvisorSkipUnneededSpotConfigsEnabled(manager Manager) bool {
	if manager == nil {
		return true
	}

	return manager.EvaluateMinimumVersionFlagOrFailsafe(FlexAdvisorSkipUnneededSpotConfigsMinCAVersionFlag, true) &&
		manager.EvaluateBoolFlagOrFailsafe(FlexAdvisorSkipUnneededSpotConfigsEnabledFlag, true)
}
