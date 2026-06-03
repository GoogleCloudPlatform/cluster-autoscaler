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
	"k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// trackedField represents an AutoscalingOptions field for which the value can change dynamically during Cluster Autoscaler runtime.
type trackedField struct {
	// name should match the field name in AutoscalingOptions. OptionsTracker uses this for logging.
	name string
	// setValue should compute the value of the tracked field based on the provided CLI flags and experiments, and set the computed value in the provided optsToModify.
	// OptionsTracker uses this to recompute the value of this field in the AutoscalingOptions it tracks.
	setValue func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, optsToModify *internalopts.AutoscalingOptions) error
	// setValueFromClusterProto is equivalent to setValue, but for fields that depend on the Cluster proto in addition to CLI flags and experiments.
	// Mutually exclusive with setValue, a given field should implement one or the other.
	setValueFromClusterProto func(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager, cluster gkeclient.Cluster, optsToModify *internalopts.AutoscalingOptions) error
	// getValueStr should return a string representation of the value of the tracked field in the provided AutoscalingOptions. OptionsTracker uses this for logging.
	getValueStr func(opts internalopts.AutoscalingOptions) string
	// valueEqual should return true iff the provided AutoscalingOptions objects have the same value of the tracked field. OptionsTracker uses this to determine
	// if the value of the tracked field changed since Cluster Autoscaler first started.
	valueEqual func(optsA, optsB internalopts.AutoscalingOptions) bool
}

var allTrackedFields = []trackedField{asyncNodeGroupsEnabledField, dynamicResourceAllocationEnabledField, capacityBuffersControllerEnabledField, capacityBuffersPodInjectionEnabledField, zoneTypesEnabledField, fastpathBinpackingEnabledField, maxNodePerScaleUpField, csnEnabledField, napMaxNodesField, salvoScaleUpField, salvoScaleUpBudgetField}

// OptionsTracker computes AutoscalingOptions based on <CLI flags, experiments, Cluster proto> and tracks changes to them during Cluster Autoscaler runtime.
//
// Note: OptionsTracker currently only tracks AutoscalingOptions fields that are plumbed into OSS logic during CA startup, so CA needs to be restarted if their
// values change. The tracker could be easily extended to handle non-OSS AutoscalingOptions fields that don't necessitate CA restart - we'd just need a mutex for
// lastOpts read/write, and a requiresCaRestart bool added to trackedField. Then internal CA components could take OptionsTracker as a dependency, add new non-restarting
// tracked fields, and get their latest value via OptionsTracker.Options(). Right now, these components instead depend on OptionsTracker.ExperimentsManager() and the startup
// snapshot of OptionsTracker.Options() - they treat the field value from Options() as the CLI-flag-only value, and implement additional logic based on experiments
// on top of it internally.
type OptionsTracker struct {
	// trackedFields contains an entry for each AutoscalingOptions field for which OptionsTracker is supposed to track changes to. This allows OptionsTracker to
	// reason about the tracked fields without having to understand them individually, delegating field-specific logic to trackedFields.
	trackedFields []trackedField
	// experimentsManager allows evaluating experiments.
	experimentsManager experiments.Manager

	// Snapshot of AutoscalingOptions computed directly from CLI flags, without taking experiments into account.
	optsFromFlags internalopts.AutoscalingOptions
	// Snapshot of AutoscalingOptions combined from CLI flags and experiments - computed based on the most recent state.
	lastOpts internalopts.AutoscalingOptions
	// Snapshot of AutoscalingOptions combined from CLI flags and experiments - computed at Cluster Autoscaler startup time.
	startupOpts internalopts.AutoscalingOptions
	// startupOpts should be set once, after AutoscalingOptions are fully initialized for the first time during Cluster Autoscaler startup.
	// This bool tracks whether this has happened yet.
	startupOptsFinalized bool
}

// NewOptionsTracker creates and initializes an instance of OptionsTracker.
func NewOptionsTracker(optsFromFlags internalopts.AutoscalingOptions, experimentsManager experiments.Manager) *OptionsTracker {
	tracker := &OptionsTracker{
		trackedFields:      allTrackedFields,
		experimentsManager: experimentsManager,
		optsFromFlags:      optsFromFlags, // Snapshot the options computed based on just CLI flags forever here.
		// Start lastOpts with the options computed based on CLI flags - this is crucial as only the values for the tracked fields get recomputed later, the rest of the fields will keep these CLI-based values.
		lastOpts:             optsFromFlags,
		startupOptsFinalized: false, // startupOpts won't be fully computed until the first call to RecomputeOptions().
	}
	// OptionsTracker is created at the very beginning of CA init logic, before the Cluster proto is obtained from the API.
	// This is necessary, as some AutoscalingOptions fields are needed to configure the API access in the first place.
	// Because of this, we need to compute the options without the Cluster proto here. Fields that depend on the Cluster
	// proto for their value are not properly initialized, and shouldn't be referenced until the first call to
	// RecomputeOptions() happens as part of creating GkeManager.
	tracker.recomputeOptionsWithoutClusterProto()

	return tracker
}

// ExperimentsManager returns the internal experiments.Manager used for evaluating experiments, to be used in other Cluster Autoscaler components.
func (t *OptionsTracker) ExperimentsManager() experiments.Manager {
	return t.experimentsManager
}

// Options returns a snapshot of AutoscalingOptions computed based on the most recent state. AutoscalingOptions fields are computed at multiple stages of
// CA startup, and each field should only be referenced after it's first computed:
//   - The vast majority of fields depend only on their corresponding CLI flag (they don't need to be tracked, so they don't have an entry in allTrackedFields).
//     Such fields are only computed once when creating OptionsTracker via NewOptionsTracker(), and can be referenced from the result of Options() immediately after.
//   - Some fields depend on a combination of CLI flags and experiments (they have an entry in allTrackedFields, with the setValue function configured).
//     Such fields are first computed when creating OptionsTracker via NewOptionsTracker(), and can be referenced from the result of Options() immediately after.
//   - Some fields depend on a combination of CLI flags, experiments, and the Cluster proto (they have an entry in allTrackedFields, with
//     the setValueFromClusterProto function configured). Such fields aren't computed until the first RecomputeOptions() call, which happens as part of creating
//     GkeManager - they shouldn't be referenced from the result of Options() before that.
//
// All result fields can be safely referenced if CA startup is already completed and Options() is called from the main CA loop. The values of the tracked
// AutoscalingOptions fields are recomputed every Cluster Autoscaler loop.
// Not thread-safe, has to be called from the main CA goroutine.
func (t *OptionsTracker) Options() internalopts.AutoscalingOptions {
	return t.lastOpts
}

// RecomputeOptions recomputes the values of the tracked AutoscalingOptions fields based on the current values of experiments and the provided Cluster proto. The result
// can be obtained via Options().
// Not thread-safe, has to be called from the main CA goroutine.
func (t *OptionsTracker) RecomputeOptions(cluster gkeclient.Cluster) {
	t.recomputeOptionsWithoutClusterProto()
	t.recomputeOptionsWithClusterProto(cluster)

	// OptionsTracker needs to snapshot the initial AutoscalingOptions computed during Cluster Autoscaler startup, so that it can detect if an option changes
	// later on (which might require a CA restart). The startup AutoscalingOptions are only fully initialized after RecomputeOptions() is called for the first time
	// as part of creating GkeManager.
	if !t.startupOptsFinalized {
		// RecomputeOptions() called for the first time, snapshot the startup options.
		t.startupOpts = t.lastOpts
		t.startupOptsFinalized = true
	}
}

// OptionChangesRequireRestart returns whether OptionsTracker has detected that Cluster Autoscaler should be restarted in order to correctly handle the value
// of a tracked field changing.
// Not thread-safe, has to be called from the main CA goroutine.
func (t *OptionsTracker) OptionChangesRequireRestart() bool {
	for _, field := range t.trackedFields {
		// The values for some fields are used in Cluster Autoscaler startup logic, and if the value changes CA needs to be fully restarted so that the
		// startup logic can run again using the new value. Right now OptionsTracker only tracks fields that work this way - check if the value has changed
		// since startup, the restart is needed if so.
		if !field.valueEqual(t.startupOpts, t.lastOpts) {
			klog.Warningf("AutoscalingOptions.%s value switched by a Cluster proto/experiment change, new value: %v", field.name, field.getValueStr(t.lastOpts))
			return true
		}
	}
	return false
}

func (t *OptionsTracker) recomputeOptionsWithoutClusterProto() {
	for _, field := range t.trackedFields {
		if field.setValue != nil {
			err := field.setValue(t.optsFromFlags, t.experimentsManager, &t.lastOpts)
			if err != nil {
				// Log the error and continue so that errors in one field don't block other fields from working.
				klog.Errorf("Error when computing AutoscalingOptions.%s: %v", field.name, err)
				continue
			}
		}
	}
}

func (t *OptionsTracker) recomputeOptionsWithClusterProto(cluster gkeclient.Cluster) {
	for _, field := range t.trackedFields {
		if field.setValueFromClusterProto != nil {
			err := field.setValueFromClusterProto(t.optsFromFlags, t.experimentsManager, cluster, &t.lastOpts)
			if err != nil {
				// Log the error and continue so that errors in one field don't block other fields from working.
				klog.Errorf("Error when computing AutoscalingOptions.%s: %v", field.name, err)
				continue
			}
		}
	}
}
