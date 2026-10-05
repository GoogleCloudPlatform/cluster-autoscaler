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

// The reservation tracked fields below allow experiment flags to override static CLI flags
// in either direction (true -> false or false -> true) while preserving safe fallback behavior:
//  1. EvaluateBoolFlagOrFailsafe takes flagValue as the fallback so that if no experiment is
//     defined, the original CLI flag value is preserved.
//  2. When MinCAVersion is not satisfied (!currentVersionSupported), we explicitly fall back to
//     flagValue instead of evaluating to false. This ensures that when rolling out an experiment
//     with a bumped MinCAVersion (e.g. after a bug fix in a newer CA release), older/unsupported
//     CA versions safely retain their original CLI flag behavior without regression.
var specificTypeReservationMatchEnabledField = trackedField{
	name: "SpecificTypeReservationMatchEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.SpecificTypeReservationMatchEnabled == optsB.SpecificTypeReservationMatchEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return strconv.FormatBool(opts.SpecificTypeReservationMatchEnabled)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		flagValue := optsFromFlags.SpecificTypeReservationMatchEnabled
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.SpecificTypeReservationMatchEnabledFlag, flagValue)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.SpecificTypeReservationMatchMinCAVersionFlag, true)

		if currentVersionSupported {
			optsToModify.SpecificTypeReservationMatchEnabled = enabled
		} else {
			optsToModify.SpecificTypeReservationMatchEnabled = flagValue
		}
		return nil
	},
}

var specificTypeReservationWithoutMatchEnabledField = trackedField{
	name: "SpecificTypeReservationWithoutMatchEnabled",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.SpecificTypeReservationWithoutMatchEnabled == optsB.SpecificTypeReservationWithoutMatchEnabled
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return strconv.FormatBool(opts.SpecificTypeReservationWithoutMatchEnabled)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		flagValue := optsFromFlags.SpecificTypeReservationWithoutMatchEnabled
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.SpecificTypeReservationWithoutMatchEnabledFlag, flagValue)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.SpecificTypeReservationWithoutMatchMinCAVersionFlag, true)

		if currentVersionSupported {
			optsToModify.SpecificTypeReservationWithoutMatchEnabled = enabled
		} else {
			optsToModify.SpecificTypeReservationWithoutMatchEnabled = flagValue
		}
		return nil
	},
}

var reservationsAnyLocationPolicyOverrideField = trackedField{
	name: "ReservationsAnyLocationPolicyOverride",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.ReservationsAnyLocationPolicyOverride == optsB.ReservationsAnyLocationPolicyOverride
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return strconv.FormatBool(opts.ReservationsAnyLocationPolicyOverride)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		flagValue := optsFromFlags.ReservationsAnyLocationPolicyOverride
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.ReservationsAnyLocationPolicyOverrideEnabledFlag, flagValue)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ReservationsAnyLocationPolicyOverrideMinCAVersionFlag, true)

		if currentVersionSupported {
			optsToModify.ReservationsAnyLocationPolicyOverride = enabled
		} else {
			optsToModify.ReservationsAnyLocationPolicyOverride = flagValue
		}
		return nil
	},
}
