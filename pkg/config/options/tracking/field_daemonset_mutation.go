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
	"strconv"

	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

var daemonSetMutationEnabledField = trackedField{
	name: "DaemonSetMutationEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.DaemonSetMutationEnabled == optsB.DaemonSetMutationEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return strconv.FormatBool(opts.DaemonSetMutationEnabled)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		flagValue := optsFromFlags.DaemonSetMutationEnabled
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.DaemonSetMutationEnabledFlag, flagValue)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.DaemonSetMutationMinCAVersionFlag, flagValue)

		optsToModify.DaemonSetMutationEnabled = flagValue && enabled && currentVersionSupported
		return nil
	},
}
