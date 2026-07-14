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

// DecreasedMaxNodesPerScaleUp represents the last safe value used on production
const DecreasedMaxNodesPerScaleUp = 500

var maxNodePerScaleUpField = trackedField{
	name: "MaxNodesPerScaleUp",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.MaxNodesPerScaleUp == optsB.MaxNodesPerScaleUp
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.MaxNodesPerScaleUp)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		if optsFromFlags.MaxNodesPerScaleUp <= DecreasedMaxNodesPerScaleUp {
			optsToModify.MaxNodesPerScaleUp = optsFromFlags.MaxNodesPerScaleUp
			return nil
		}
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.IncreasedMaxNodesPerScaleUpEnabledFlag, true)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.IncreasedMaxNodesPerScaleUpMinCAVersionFlag, true)
		if enabled && currentVersionSupported {
			optsToModify.MaxNodesPerScaleUp = optsFromFlags.MaxNodesPerScaleUp
		} else {
			optsToModify.MaxNodesPerScaleUp = DecreasedMaxNodesPerScaleUp
		}
		return nil
	},
}
