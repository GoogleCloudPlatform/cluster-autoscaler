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

var scaleUpSimulationForSkippedNodeGroupsEnabledField = trackedField{
	name: "ScaleUpSimulationForSkippedNodeGroupsEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.ScaleUpSimulationForSkippedNodeGroupsEnabled == optsB.ScaleUpSimulationForSkippedNodeGroupsEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.ScaleUpSimulationForSkippedNodeGroupsEnabled)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		// Plan for the rollout:
		//   - do not introduce the flag in manifest,
		//   - have a direct launch flag set to true by default (modify it in case of mitigation experiments),
		// 	 - have a version based experiment to rollout the feature region by region.
		directLaunchEnabled := experimentsManager.DirectLaunchBoolFlag(experiments.ScaleUpSimulationForSkippedNodeGroupsEnabledFlag)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag, false)

		optsToModify.ScaleUpSimulationForSkippedNodeGroupsEnabled = directLaunchEnabled && currentVersionSupported
		return nil
	},
}
