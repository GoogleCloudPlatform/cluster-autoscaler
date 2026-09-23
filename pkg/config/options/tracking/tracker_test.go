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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/cluster-autoscaler/pkg/config"

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
			testName:                    "GracefulDegradationEnabled_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{GracefulDegradationEnabled: true}},
			experimentValues:            map[string]bool{experiments.GracefulDegradationEnabledFlag: false},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{AutoscalingOptions: config.AutoscalingOptions{GracefulDegradationEnabled: false}}, // Assert that the field value is modified.
			wantRestart:                 true,
		},
		{
			testName:                    "DefaultReservedResourcesV2Enabled_field_is_tracked",
			flagValues:                  internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{DefaultReservedResourcesV2Enabled: false}},
			experimentValues:            map[string]bool{experiments.DefaultReservedResourcesMinCAVersionFlag: true},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{DefaultReservedResourcesV2Enabled: true}},
			wantRestart:                 true,
		},
		{
			testName:                    "BalloonPodIpprResizeEnabled_defaults_to_enabled_when_no_experiment_is_defined",
			flagValues:                  internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{BalloonPodIpprResizeEnabled: false}},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{BalloonPodIpprResizeEnabled: true}},
			wantRestart:                 false,
		},
		{
			testName:                    "BalloonPodIpprResizeEnabled_is_tracked_when_experiment_disables_it",
			flagValues:                  internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{BalloonPodIpprResizeEnabled: true}},
			experimentValues:            map[string]bool{experiments.BalloonPodIpprResizeFlag: false},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{BalloonPodIpprResizeEnabled: false}},
			wantRestart:                 true,
		},
		{
			testName: "EkvmsIncrementStep_field_is_tracked",
			flagValues: internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{
				EkvmsIncrementStep: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("2"),
					apiv1.ResourceMemory: resource.MustParse("1Mi"),
				},
			}},
			experimentValues:       map[string]bool{experiments.EkFineGrainedResizeMinCAVersionFlag: true},
			stringExperimentValues: map[string]string{experiments.EkFineGrainedResizeIncrementStepFlag: "1"},
			wantOptionsAfterExperiments: internalopts.AutoscalingOptions{InternalOptions: internalopts.InternalOptions{
				EkvmsIncrementStep: apiv1.ResourceList{
					apiv1.ResourceCPU:    resource.MustParse("1"),
					apiv1.ResourceMemory: resource.MustParse("1Mi"),
				},
			}},
			wantRestart: true,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			noExperiments := experiments.NewMockManager()
			withExperiments := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, tc.stringExperimentValues)
			// Make sure the same init logic used by the public NewOptionsTracker constructor gets tested here - it's crucial for OptionsTracker correctly
			// handling the fields that aren't tracked.
			tracker := NewOptionsTracker(tc.flagValues, noExperiments)
			tracker.StoreCluster(gkeclient.Cluster{})

			// Compute the options for the first time with no experiments defined - all field values should stay the same as the flag ones.
			// Since this is the first call to RecomputeOptions(), the resulting values should be saved as the startup options.
			err := tracker.RecomputeOptions()
			assert.NoError(t, err)
			assert.Equal(t, withDirectLaunchDefaults(tc.flagValues), tracker.Options())
			// Last computed options are trivially the same as startup options, so no need for restart.
			assert.False(t, tracker.OptionChangesRequireRestart())

			// Simulate experiments being defined over time by swapping the experiment manager to one which has them defined. If the tested field should
			// change value based on the experiments, the new value should be reflected after the next RecomputeOptions() call.
			tracker.experimentsManager = withExperiments
			err = tracker.RecomputeOptions()
			assert.NoError(t, err)

			wantOpts := tc.wantOptionsAfterExperiments
			if _, isDirectLaunchDisable := tc.experimentValues[experiments.BalloonPodIpprResizeFlag]; !isDirectLaunchDisable {
				wantOpts = withDirectLaunchDefaults(wantOpts)
			}
			assert.Equal(t, wantOpts, tracker.Options())
			assert.Equal(t, tc.wantRestart, tracker.OptionChangesRequireRestart())
		})
	}
}

// withDirectLaunchDefaults returns opts with the fields backed by direct-launch experiment flags set to their default value. Direct-launch flags are
// enabled unless an experiment explicitly disables them, so RecomputeOptions() always applies them regardless of the CLI flag values. Applying them to
// the expected options here lets the assertions keep comparing the whole options struct, which is what guarantees that a tracked field doesn't
// accidentally mutate any other field.
func withDirectLaunchDefaults(opts internalopts.AutoscalingOptions) internalopts.AutoscalingOptions {
	opts.BalloonPodIpprResizeEnabled = true
	return opts
}

func TestOptionsTrackerRequireRestart(t *testing.T) {
	const (
		nonRestartingExp = "NonRestartingExp"
		restartingExp    = "RestartingExp"
	)

	nonRestartingField := trackedField{
		name:        "NonRestartingField",
		valueEqual:  func(optsA, optsB internalopts.AutoscalingOptions) bool { return optsA.Profile == optsB.Profile },
		getValueStr: func(opts internalopts.AutoscalingOptions) string { return opts.Profile },
		setValue: func(optsFromFlags internalopts.AutoscalingOptions, em experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
			if em.EvaluateBoolFlagOrFailsafe(nonRestartingExp, false) {
				optsToModify.Profile = "modified"
			} else {
				optsToModify.Profile = "initial"
			}
			return nil
		},
		caRestartNeededOnValueChange: false,
	}

	restartingField := trackedField{
		name:        "RestartingField",
		valueEqual:  func(optsA, optsB internalopts.AutoscalingOptions) bool { return optsA.Location == optsB.Location },
		getValueStr: func(opts internalopts.AutoscalingOptions) string { return opts.Location },
		setValue: func(optsFromFlags internalopts.AutoscalingOptions, em experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error {
			if em.EvaluateBoolFlagOrFailsafe(restartingExp, false) {
				optsToModify.Location = "modified"
			} else {
				optsToModify.Location = "initial"
			}
			return nil
		},
		caRestartNeededOnValueChange: true,
	}

	for _, tc := range []struct {
		testName         string
		experimentValues map[string]bool
		wantRestart      bool
	}{
		{
			testName:         "no_fields_changed",
			experimentValues: map[string]bool{},
			wantRestart:      false,
		},
		{
			testName:         "only_non_restarting_field_changed",
			experimentValues: map[string]bool{nonRestartingExp: true},
			wantRestart:      false,
		},
		{
			testName:         "only_restarting_field_changed",
			experimentValues: map[string]bool{restartingExp: true},
			wantRestart:      true,
		},
		{
			testName:         "both_fields_changed",
			experimentValues: map[string]bool{nonRestartingExp: true, restartingExp: true},
			wantRestart:      true,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			tracker := NewOptionsTracker(internalopts.AutoscalingOptions{}, experiments.NewMockManager())
			tracker.trackedFields = []trackedField{nonRestartingField, restartingField}
			tracker.StoreCluster(gkeclient.Cluster{})
			err := tracker.RecomputeOptions()
			assert.NoError(t, err)

			tracker.experimentsManager = experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)
			err = tracker.RecomputeOptions()
			assert.NoError(t, err)

			assert.Equal(t, tc.wantRestart, tracker.OptionChangesRequireRestart())
		})
	}
}

func TestValidateTrackedFields(t *testing.T) {
	for _, field := range allTrackedFields {
		t.Run(field.name, func(t *testing.T) {
			assert.NotNil(t, field.getValueStr, "getValueStr must not be nil")
			assert.NotNil(t, field.valueEqual, "valueEqual must not be nil")

			hasSetValue := field.setValue != nil
			hasSetValueFromClusterProto := field.setValueFromClusterProto != nil
			assert.True(t, hasSetValue != hasSetValueFromClusterProto, "setValue or setValueFromClusterProto must be set, never both")

			_, isOSS := reflect.TypeFor[config.AutoscalingOptions]().FieldByName(field.name)
			_, isInternal := reflect.TypeFor[internalopts.InternalOptions]().FieldByName(field.name)

			assert.True(t, isOSS != isInternal, "field name must match option in config.AutoscalingOptions or internalopts.InternalOptions, never both")

			if isInternal {
				assert.Nil(t, field.propagateChangesToAutoscalingContext,
					"internal field must not define propagateChangesToAutoscalingContext")
			} else if field.caRestartNeededOnValueChange {
				assert.Nil(t, field.propagateChangesToAutoscalingContext,
					"restarting OSS field must not define propagateChangesToAutoscalingContext")
			} else {
				assert.NotNil(t, field.propagateChangesToAutoscalingContext,
					"dynamic OSS field must define propagateChangesToAutoscalingContext")
			}
		})
	}
}

func TestOptionsTrackerStoreClusterAndRecompute(t *testing.T) {
	clusterField := trackedField{
		name: "ClusterField",
		valueEqual: func(optsA, optsB internalopts.AutoscalingOptions) bool {
			return optsA.Profile == optsB.Profile
		},
		getValueStr: func(opts internalopts.AutoscalingOptions) string {
			return opts.Profile
		},
		setValueFromClusterProto: func(optsFromFlags internalopts.AutoscalingOptions, em experiments.Manager, cluster gkeclient.Cluster, optsToModify *internalopts.AutoscalingOptions) error {
			optsToModify.Profile = cluster.ClusterVersion
			return nil
		},
		caRestartNeededOnValueChange: true,
	}

	t.Run("Recompute before StoreCluster returns error", func(t *testing.T) {
		manager := experiments.NewMockManager()
		tracker := NewOptionsTracker(internalopts.AutoscalingOptions{}, manager)
		tracker.trackedFields = []trackedField{clusterField}
		err := tracker.RecomputeOptions()
		assert.Error(t, err)
		assert.False(t, tracker.startupOptsFinalized)
		assert.False(t, tracker.OptionChangesRequireRestart())
	})

	t.Run("Lifecycle: store cluster, recompute, and detect restart requirement", func(t *testing.T) {
		manager := experiments.NewMockManager()
		tracker := NewOptionsTracker(internalopts.AutoscalingOptions{}, manager)
		tracker.trackedFields = []trackedField{clusterField}

		// Storing a cluster enables RecomputeOptions() to initialize startup options.
		tracker.StoreCluster(gkeclient.Cluster{ClusterVersion: "1.30.0"})
		err := tracker.RecomputeOptions()
		assert.NoError(t, err)
		assert.Equal(t, "1.30.0", tracker.Options().Profile)
		assert.False(t, tracker.OptionChangesRequireRestart())

		// Options should not change until RecomputeOptions() is explicitly called.
		tracker.StoreCluster(gkeclient.Cluster{ClusterVersion: "1.31.0"})
		assert.Equal(t, "1.30.0", tracker.Options().Profile)

		// Calling StoreCluster again before recomputing overwrites the previous cluster.
		// RecomputeOptions() should apply only the latest stored cluster proto.
		tracker.StoreCluster(gkeclient.Cluster{ClusterVersion: "1.32.0"})
		err = tracker.RecomputeOptions()
		assert.NoError(t, err)
		assert.Equal(t, "1.32.0", tracker.Options().Profile)
		assert.True(t, tracker.OptionChangesRequireRestart())
	})
}

func TestOptionsTrackerThreadSafety(t *testing.T) {
	manager := experiments.NewMockManager()
	tracker := NewOptionsTracker(internalopts.AutoscalingOptions{}, manager)
	tracker.StoreCluster(gkeclient.Cluster{})
	if err := tracker.RecomputeOptions(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start concurrent readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					_ = tracker.Options()
					_ = tracker.OptionChangesRequireRestart()
				}
			}
		}()
	}

	// Start concurrent writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			tracker.StoreCluster(gkeclient.Cluster{})
			_ = tracker.RecomputeOptions()
		}
		close(stopCh)
	}()

	wg.Wait()
}

func TestPropagateDynamicOptions(t *testing.T) {
	tracker := &OptionsTracker{
		lastOpts: internalopts.AutoscalingOptions{
			AutoscalingOptions: config.AutoscalingOptions{
				MaxNodesTotal: 100,
				ScanInterval:  10 * time.Second,
			},
		},
		trackedFields: []trackedField{
			{
				name: "MaxNodesTotal",
				propagateChangesToAutoscalingContext: func(src config.AutoscalingOptions, dst *config.AutoscalingOptions) {
					dst.MaxNodesTotal = src.MaxNodesTotal
				},
			},
			{
				name: "ScanInterval",
				// propagateChangesToAutoscalingContext is nil
			},
		},
	}
	dst := config.AutoscalingOptions{
		MaxNodesTotal: 50,
		ScanInterval:  5 * time.Second,
	}
	tracker.PropagateDynamicOptions(&dst)
	// Dynamically propagated field was updated
	assert.Equal(t, 100, dst.MaxNodesTotal)
	// Field without propagation was left untouched
	assert.Equal(t, 5*time.Second, dst.ScanInterval)
}
