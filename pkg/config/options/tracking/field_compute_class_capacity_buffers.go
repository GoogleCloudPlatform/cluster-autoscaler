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

// The ComputeClass capacity buffers controller is only set up at CA startup, so both fields restart CA when they change.
var computeClassCapacityBuffersEnabledField = trackedField{
	name: "ComputeClassCapacityBuffersEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.ComputeClassCapacityBuffersEnabled == optsB.ComputeClassCapacityBuffersEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.ComputeClassCapacityBuffersEnabled)
	},
	setValue: func(_ internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		optsToModify.ComputeClassCapacityBuffersEnabled = experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.ComputeClassCapacityBuffersFlag, true) &&
			experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ComputeClassCapacityBuffersMinCAVersionFlag, false)
		return nil
	},
	caRestartNeededOnValueChange: true,
}

var computeClassCapacityBuffersCleanupField = trackedField{
	name: "ComputeClassCapacityBuffersCleanup",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.ComputeClassCapacityBuffersCleanup == optsB.ComputeClassCapacityBuffersCleanup
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.ComputeClassCapacityBuffersCleanup)
	},
	setValue: func(_ internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		// Failsafe false: an evaluation failure must never delete the derived buffers.
		optsToModify.ComputeClassCapacityBuffersCleanup = experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.ComputeClassCapacityBuffersCleanupFlag, false)
		return nil
	},
	caRestartNeededOnValueChange: true,
}
