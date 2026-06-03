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

package tracking

import (
	"fmt"

	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

var asyncNodeGroupsEnabledField = trackedField{
	name: "AsyncNodeGroupsEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.AsyncNodeGroupsEnabled == optsB.AsyncNodeGroupsEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.AsyncNodeGroupsEnabled)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		// HTNAP::Enabled and HTNAP::MinCAVersion are not defined by default and only used in emergency management
		// see go/htnap-rollout
		// This leaves us with `flagValue` in the usual path.
		flagValue := optsFromFlags.AsyncNodeGroupsEnabled
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.HtnapEnabledFlag, flagValue)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.HtnapMinCAVersionFlag, flagValue)

		optsToModify.AsyncNodeGroupsEnabled = flagValue && enabled && currentVersionSupported
		return nil
	},
}
