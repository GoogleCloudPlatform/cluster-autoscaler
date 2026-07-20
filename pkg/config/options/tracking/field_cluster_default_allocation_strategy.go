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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

var clusterDefaultAllocationStrategyField = trackedField{
	name: "ClusterDefaultAllocationStrategy",
	valueEqual: func(optsA, optsB options.AutoscalingOptions) bool {
		return optsA.ClusterDefaultAllocationStrategy == optsB.ClusterDefaultAllocationStrategy
	},
	getValueStr: func(opts options.AutoscalingOptions) string {
		return string(opts.ClusterDefaultAllocationStrategy)
	},
	setValue: func(optsFromFlags options.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *options.AutoscalingOptions) error {
		optsToModify.ClusterDefaultAllocationStrategy = getClusterDefaultAllocationStrategy(optsFromFlags.ClusterDefaultAllocationStrategy, experimentsManager)
		return nil
	},
}

func getClusterDefaultAllocationStrategy(flagValue options.ClusterDefaultAllocationStrategy, em experiments.Manager) options.ClusterDefaultAllocationStrategy {
	if flagValue != "" {
		return flagValue
	}
	expValue := em.EvaluateStringFlagOrFailsafe(experiments.ClusterDefaultAllocationStrategyFlag, "")
	return options.ClusterDefaultAllocationStrategy(expValue)
}
