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
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/cluster-autoscaler/config"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestOptionsTrackerFieldsIntegration(t *testing.T) {
	// This is an integration test validating that each tracked field is properly plumbed into OptionsTracker:
	// - The field value is correctly reflected in Options() after calling RecomputeOptions().
	// - The field value changing correctly affects OptionChangesRequireRestart().
	//
	// The full logic for field values is tested in field-specific tests.
	for _, tc := range []struct {
		testName string

		flagValues             internalopts.AutoscalingOptions
		experimentValues       map[string]bool
		stringExperimentValues map[string]string

		wantOptionsAfterExperiments internalopts.AutoscalingOptions
		wantRestart                 bool
	}{
		{
			testName:                    "untracked_fields_are_properly_initialized_with_CLI_Values",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{EstimatorName: "xyz"}}, // Arbitrary field that isn't tracked by OptionsTracker, and is unlikely to be in the future.
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{EstimatorName: "xyz"}}, // Assert that fields that aren't tracked by OptionsTracker are correctly initialized to their CLI-based values.
			wantRestart:                 false,
		},
		{
			testName:                    "DynamicResourceAllocationEnabled_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: true}},
			experimentValues:            map[string]bool{"DRA::Enabled": false},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{DynamicResourceAllocationEnabled: false}}, // Assert that the field value is modified.
			wantRestart:                 true,
		},
		{
			testName:                    "AsyncNodeGroupsEnabled_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{AsyncNodeGroupsEnabled: true}},
			experimentValues:            map[string]bool{experiments.HtnapEnabledFlag: false},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{AsyncNodeGroupsEnabled: false}}, // Assert that the field value is modified.
			wantRestart:                 true,
		},
		{
			testName:                    "CapacityBuffersControllerPrivatePreviewEnabled_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{CapacitybufferControllerEnabled: false}},
			experimentValues:            map[string]bool{experiments.CapacityBuffersPrivatePreviewMinCAVersion: true},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{CapacitybufferPodInjectionEnabled: true, CapacitybufferControllerEnabled: true}}, // Assert that the field value is modified.
			wantRestart:                 true,
		},
		{
			testName:                    "CapacityBuffersPodInjectionPrivatePreviewEnabled_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{CapacitybufferPodInjectionEnabled: false}},
			experimentValues:            map[string]bool{experiments.CapacityBuffersPrivatePreviewMinCAVersion: true},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{CapacitybufferPodInjectionEnabled: true, CapacitybufferControllerEnabled: true}}, // Assert that the field value is modified.
			wantRestart:                 true,
		},
		{
			testName:                    "FastpathBinpackingEnabled_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{FastpathBinpackingEnabled: true}},
			experimentValues:            map[string]bool{experiments.FastpathBinpackingEnabledFlag: false},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{FastpathBinpackingEnabled: false}}, // Assert that the field value is modified.
			wantRestart:                 true,
		},
		{
			testName:                    "MaxNodesPerScaleUp_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{MaxNodesPerScaleUp: 1000}},
			experimentValues:            map[string]bool{experiments.IncreasedMaxNodesPerScaleUpEnabledFlag: false},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{MaxNodesPerScaleUp: 500}},
			wantRestart:                 true,
		},
		{
			testName:                    "NapMaxNodes_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{NapMaxNodes: 2000}},
			experimentValues:            map[string]bool{experiments.IncreasedNapMaxNodesEnabledFlag: false},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{NapMaxNodes: 1000}},
			wantRestart:                 true,
		},
		{
			testName:   "CSNEnabled_field_is_tracked",
			flagValues: internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{AutopilotEnabled: false, CSNCAFlag: internalopts.CSNUnspecified}},
			experimentValues: map[string]bool{
				experiments.ColdStandbyNodesMinCAVersionFlag: true,
			},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{AutopilotEnabled: false, CSNEnabled: true, CSNCAFlag: internalopts.CSNUnspecified}},
			wantRestart:                 true,
		},
		{
			testName:                    "SalvoScaleUp_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{SalvoScaleUp: true}},
			experimentValues:            map[string]bool{experiments.SalvoScaleUpEnabledFlag: false},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{SalvoScaleUp: false}},
			wantRestart:                 true,
		},
		{
			testName:                    "SalvoScaleUpBudget_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{SalvoScaleUpBudget: 5 * time.Minute}},
			stringExperimentValues:      map[string]string{experiments.SalvoScaleUpBudgetSecondsFlag: "600"},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{SalvoScaleUpBudget: 10 * time.Minute}},
			wantRestart:                 true,
		},
		{
			testName:                    "ScaleUpSimulationForSkippedNodeGroupsEnabled_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{ScaleUpSimulationForSkippedNodeGroupsEnabled: false}},
			experimentValues:            map[string]bool{experiments.ScaleUpSimulationForSkippedNodeGroupsMinCAVersionFlag: true},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{ScaleUpSimulationForSkippedNodeGroupsEnabled: true}},
			wantRestart:                 true,
		},
		{
			testName:                    "ClusterDefaultAllocationStrategyFlag_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{ClusterDefaultAllocationStrategy: ""}},
			stringExperimentValues:      map[string]string{experiments.ClusterDefaultAllocationStrategyFlag: "fleet-efficiency"},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{ClusterDefaultAllocationStrategy: internalopts.ClusterDefaultAllocationStrategyFleetEfficiency}},
			wantRestart:                 true,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			noExperiments := experiments.NewMockManager()
			withExperiments := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, tc.stringExperimentValues)
			// Make sure the same init logic used by the public NewOptionsTracker constructor gets tested here - it's crucial for OptionsTracker correctly
			// handling the fields that aren't tracked.
			tracker := NewOptionsTracker(tc.flagValues, noExperiments)

			// Compute the options for the first time with no experiments defined - all field values should stay the same as the flag ones.
			// Since this is the first call to RecomputeOptions(), the resulting values should be saved as the startup options.
			tracker.RecomputeOptions(gkeclient.Cluster{})
			assert.Equal(t, tc.flagValues, tracker.Options())
			// Last computed options are trivially the same as startup options, so no need for restart.
			assert.False(t, tracker.OptionChangesRequireRestart())

			// Simulate experiments being defined over time by swapping the experiment manager to one which has them defined. If the tested field should
			// change value based on the experiments, the new value should be reflected after the next RecomputeOptions() call.
			tracker.experimentsManager = withExperiments
			tracker.RecomputeOptions(gkeclient.Cluster{})
			assert.Equal(t, tc.wantOptionsAfterExperiments, tracker.Options())
			assert.Equal(t, tc.wantRestart, tracker.OptionChangesRequireRestart())
		})
	}
}
