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
	"k8s.io/klog/v2"
)

const DecreasedNapMaxNodesCount = 1000

var napMaxNodesField = trackedField{
	name: "NapMaxNodes",
	valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
		return optsA.NapMaxNodes == optsB.NapMaxNodes
	},
	getValueStr: func(opts internalopts.AutoscalingOptions) string {
		return fmt.Sprintf("%v", opts.NapMaxNodes)
	},
	setValue: func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
		if optsFromFlags.NapMaxNodes <= DecreasedNapMaxNodesCount {
			optsToModify.NapMaxNodes = optsFromFlags.NapMaxNodes
			return nil
		}
		// IncreasedNapMaxNodes::Enabled and IncreasedNapMaxNodes::MinCAVersion are not defined by default and only used in emergency management
		// As described in go/gke-ca-perf-2k-migs
		// This leaves us with `flagValue` in the usual path.
		enabled := experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.IncreasedNapMaxNodesEnabledFlag, true)
		currentVersionSupported := experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.IncreasedNapMaxNodesMinCAVersionFlag, true)
		if enabled && currentVersionSupported {
			optsToModify.NapMaxNodes = optsFromFlags.NapMaxNodes
		} else {
			optsToModify.NapMaxNodes = DecreasedNapMaxNodesCount
			klog.Infof("Enforcing lower NAP max nodes per zone limit (rollback/mitigation). New limit: %d, limit from CA flag: %d", optsToModify.NapMaxNodes, optsFromFlags.NapMaxNodes)
		}
		return nil
	},
}
