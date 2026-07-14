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

var salvoScaleUpField = trackedField{
	name: "SalvoScaleUp",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.SalvoScaleUp == optsB.SalvoScaleUp
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.SalvoScaleUp)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		flagValue := optsFromFlags.SalvoScaleUp
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.SalvoScaleUpEnabledFlag, flagValue)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.SalvoScaleUpMinCAVersionFlag, flagValue)

		optsToModify.SalvoScaleUp = enabled && currentVersionSupported
		return nil
	},
}

var salvoScaleUpBudgetField = trackedField{
	name: "SalvoScaleUpBudget",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.SalvoScaleUpBudget == optsB.SalvoScaleUpBudget
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.SalvoScaleUpBudget)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		optsToModify.SalvoScaleUpBudget = experimentsManager.EvaluateDurationSecondsFlagOrFailsafe(experiments.SalvoScaleUpBudgetSecondsFlag, optsFromFlags.SalvoScaleUpBudget)
		return nil
	},
}
