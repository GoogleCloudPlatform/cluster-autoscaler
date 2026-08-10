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

var defaultReservedResourcesV2EnabledField = trackedField{
	name: "DefaultReservedResourcesV2Enabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.DefaultReservedResourcesV2Enabled == optsB.DefaultReservedResourcesV2Enabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.DefaultReservedResourcesV2Enabled)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		directLaunchEnabled := experimentsManager.DirectLaunchBoolFlag(experiments.DefaultReservedResourcesEnabledFlag)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.DefaultReservedResourcesMinCAVersionFlag, false)

		optsToModify.DefaultReservedResourcesV2Enabled = directLaunchEnabled && currentVersionSupported
		return nil
	},
}
