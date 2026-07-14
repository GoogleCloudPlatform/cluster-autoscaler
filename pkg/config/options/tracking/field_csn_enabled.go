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

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

var csnEnabledField = trackedField{
	name: "CSNEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.CSNEnabled == optsB.CSNEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.CSNEnabled)
	},
	setValue: func(optsFromFlag internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		optsToModify.CSNEnabled = isCSNEnabled(optsFromFlag.AutopilotEnabled, optsFromFlag.CSNCAFlag, experimentsManager)
		return nil
	},
}

func isCSNEnabled(autopilotEnabled bool, csnCAFlag options.CSNStatus, gm experiments.Manager) bool {
	if autopilotEnabled && !gm.EvaluateMinimumVersionFlagOrFailsafe(experiments.ColdStandbyNodesAutopilotSoHWFlag, false) {
		return false
	}
	if csnCAFlag == "" { // Mainly for tests.
		csnCAFlag = options.CSNUnspecified
	}
	if csnCAFlag != options.CSNUnspecified && gm.EvaluateMinimumVersionFlagOrFailsafe(experiments.ColdStandbyNodesMinCAVersionGuardForCAFlag, true) {
		return csnCAFlag == options.CSNEnabled
	}
	return gm.EvaluateMinimumVersionFlagOrFailsafe(experiments.ColdStandbyNodesInternalMinCAVersionFlag, false) ||
		gm.EvaluateMinimumVersionFlagOrFailsafe(experiments.ColdStandbyNodesMinCAVersionFlag, false)
}
