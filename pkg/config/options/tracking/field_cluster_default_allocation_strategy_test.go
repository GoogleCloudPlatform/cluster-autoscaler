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
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestClusterDefaultAllocationStrategyFieldSetValue(t *testing.T) {
	for _, tc := range []struct {
		testName string

		flagValue              options.ClusterDefaultAllocationStrategy
		stringExperimentValues map[string]string

		wantValue options.ClusterDefaultAllocationStrategy
	}{
		{
			testName:               "flag_set_to_lowest-cost_experiment_fleet-efficiency",
			flagValue:              options.ClusterDefaultAllocationStrategyLowestCost,
			stringExperimentValues: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "fleet-efficiency"},
			wantValue:              options.ClusterDefaultAllocationStrategyLowestCost,
		},
		{
			testName:               "flag_set_to_fleet-efficiency_experiment_lowest-cost",
			flagValue:              options.ClusterDefaultAllocationStrategyFleetEfficiency,
			stringExperimentValues: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "lowest-cost"},
			wantValue:              options.ClusterDefaultAllocationStrategyFleetEfficiency,
		},
		{
			testName:               "flag_empty_experiment_fleet-efficiency",
			flagValue:              "",
			stringExperimentValues: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "fleet-efficiency"},
			wantValue:              options.ClusterDefaultAllocationStrategyFleetEfficiency,
		},
		{
			testName:               "flag_empty_experiment_lowest-cost",
			flagValue:              "",
			stringExperimentValues: map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "lowest-cost"},
			wantValue:              options.ClusterDefaultAllocationStrategyLowestCost,
		},
		{
			testName:               "flag_empty_experiment_undefined",
			flagValue:              "",
			stringExperimentValues: nil,
			wantValue:              "",
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			optsFromFlags := options.AutoscalingOptions{
				InternalOptions: options.InternalOptions{
					ClusterDefaultAllocationStrategy: tc.flagValue,
				},
			}

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, nil, tc.stringExperimentValues)

			optsToModify := options.AutoscalingOptions{}
			err := clusterDefaultAllocationStrategyField.setValue(optsFromFlags, experimentsManager, &optsToModify)
			assert.NoError(t, err)

			assert.Equal(t, tc.wantValue, optsToModify.ClusterDefaultAllocationStrategy)
			assert.Equal(t, string(tc.wantValue), clusterDefaultAllocationStrategyField.getValueStr(optsToModify))
		})
	}
}

func TestClusterDefaultAllocationStrategyFieldValueEqual(t *testing.T) {
	for _, tc := range []struct {
		testName string

		optsA options.AutoscalingOptions
		optsB options.AutoscalingOptions

		wantEqual bool
	}{
		{
			testName:  "both_lowest-cost",
			optsA:     options.AutoscalingOptions{InternalOptions: options.InternalOptions{ClusterDefaultAllocationStrategy: options.ClusterDefaultAllocationStrategyLowestCost}},
			optsB:     options.AutoscalingOptions{InternalOptions: options.InternalOptions{ClusterDefaultAllocationStrategy: options.ClusterDefaultAllocationStrategyLowestCost}},
			wantEqual: true,
		},
		{
			testName:  "both_empty",
			optsA:     options.AutoscalingOptions{},
			optsB:     options.AutoscalingOptions{},
			wantEqual: true,
		},
		{
			testName:  "different_values",
			optsA:     options.AutoscalingOptions{InternalOptions: options.InternalOptions{ClusterDefaultAllocationStrategy: options.ClusterDefaultAllocationStrategyLowestCost}},
			optsB:     options.AutoscalingOptions{InternalOptions: options.InternalOptions{ClusterDefaultAllocationStrategy: options.ClusterDefaultAllocationStrategyFleetEfficiency}},
			wantEqual: false,
		},
		{
			testName:  "different_values_one_empty",
			optsA:     options.AutoscalingOptions{},
			optsB:     options.AutoscalingOptions{InternalOptions: options.InternalOptions{ClusterDefaultAllocationStrategy: options.ClusterDefaultAllocationStrategyFleetEfficiency}},
			wantEqual: false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			gotEqual := clusterDefaultAllocationStrategyField.valueEqual(tc.optsA, tc.optsB)
			assert.Equal(t, tc.wantEqual, gotEqual)
		})
	}
}
