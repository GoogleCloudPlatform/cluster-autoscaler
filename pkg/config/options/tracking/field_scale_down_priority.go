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
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

var scaleDownPriorityTaintKeyField = trackedField{
	name: "ScaleDownPriorityTaintKey",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.ScaleDownPriorityTaintKey == optsB.ScaleDownPriorityTaintKey
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return opts.ScaleDownPriorityTaintKey
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		// The flag enables the feature; ScaleDownPriority::Enabled is a
		// direct-launch kill switch that defaults to true.
		// ScaleDownPriority::MinCAVersion limits the feature to CA versions at
		// or above the given one, so it can be re-enabled from a fixed version
		// on; it fails safe to true so that leaving it unset keeps the feature.
		enabled := experimentsManager.DirectLaunchBoolFlag(experiments.ScaleDownPriorityEnabledFlag)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ScaleDownPriorityMinCAVersionFlag, true)
		if enabled && currentVersionSupported {
			optsToModify.ScaleDownPriorityTaintKey = optsFromFlags.ScaleDownPriorityTaintKey
		} else {
			optsToModify.ScaleDownPriorityTaintKey = ""
		}
		return nil
	},
	caRestartNeededOnValueChange: true,
}
