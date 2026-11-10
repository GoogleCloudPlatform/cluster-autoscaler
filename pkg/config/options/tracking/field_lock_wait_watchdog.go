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

const enableLockWaitWatchdogFlag = "enable-lock-wait-watchdog"

// LockWaitWatchdog is a mitigation experiment: the lock wait watchdog is
// enabled by default (--enable-lock-wait-watchdog=true), and setting
// LockWaitWatchdog::Enabled=false or LockWaitWatchdog::MinCAVersion above the
// running CA version disables it at runtime without restarting Cluster
// Autoscaler. An explicit --override_enable-lock-wait-watchdog flag takes
// precedence over the experiment.
var lockWaitWatchdogEnabledField = trackedField{
	name: "LockWaitWatchdogEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.LockWaitWatchdogEnabled == optsB.LockWaitWatchdogEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return strconv.FormatBool(opts.LockWaitWatchdogEnabled)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		if !optsFromFlags.LockWaitWatchdogEnabled || optsFromFlags.FlagOverrideResult.HasFlagOverride(enableLockWaitWatchdogFlag) {
			optsToModify.LockWaitWatchdogEnabled = optsFromFlags.LockWaitWatchdogEnabled
			return nil
		}
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.LockWaitWatchdogEnabledFlag, true)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.LockWaitWatchdogMinCAVersionFlag, true)
		optsToModify.LockWaitWatchdogEnabled = enabled && currentVersionSupported
		return nil
	},
	caRestartNeededOnValueChange: false,
}
