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
	"sync"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

// trackedField represents an AutoscalingOptions field for which the value can change dynamically during Cluster Autoscaler runtime.
type trackedField struct {
	// name must match the field name in config.AutoscalingOptions (OSS) or internalopts.InternalOptions (GKE).
	// OptionsTracker uses this for logging, and tests use it to distinguish OSS fields from internal ones.
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
	// caRestartNeededOnValueChange indicates if CA restart is necessary after value of the option is changed.
	// Mutually exclusive with propagateChangesToAutoscalingContext.
	caRestartNeededOnValueChange bool
	// propagateChangesToAutoscalingContext copies the tracked field value from OptionsTracker to the OSS
	// AutoscalingOptions embedded in AutoscalingContext. If nil, changes are not propagated dynamically.
	//
	// This should ONLY be configured if all accesses to the field in OSS logic read directly from
	// AutoscalingContext on each loop iteration. It must NOT be used for fields that are:
	// 1. Used in initialization logic (which requires a CA restart to re-run init).
	// 2. Plumbed into constructors and cached by subcomponents (which won't notice dynamic changes).
	// Updating such fields dynamically would lead to inconsistencies between components.
	// 3. Internal field (belonging to InternalOptions)
	//
	// Fields that have this configured should be accompanied by a Big Unit Test with the value changing dynamically
	// to prevent regressions if new accesses to the field are added in OSS logic in the future.
	propagateChangesToAutoscalingContext func(src config.AutoscalingOptions, dst *config.AutoscalingOptions)
}

var allTrackedFields = []trackedField{asyncNodeGroupsEnabledField, dynamicResourceAllocationEnabledField, capacityBuffersControllerEnabledField, capacityBuffersPodInjectionEnabledField, zoneTypesEnabledField, fastpathBinpackingEnabledField, maxNodePerScaleUpField, csnEnabledField, napMaxNodesField, salvoScaleUpField, salvoScaleUpBudgetField, scaleUpSimulationForSkippedNodeGroupsEnabledField, daemonSetMutationEnabledField, gracefulDegradationEnabledField, defaultReservedResourcesV2EnabledField, provisioningErrorDetailsEnabledField, balloonPodIpprResizeEnabledField, ekvmsIncrementStepField}

// OptionsTracker computes AutoscalingOptions based on <CLI flags, experiments, Cluster proto> and tracks changes to them during Cluster Autoscaler runtime.
// Thread-safe.
type OptionsTracker struct {
	// trackedFields contains an entry for each AutoscalingOptions field for which OptionsTracker is supposed to track changes to. This allows OptionsTracker to
	// reason about the tracked fields without having to understand them individually, delegating field-specific logic to trackedFields.
	trackedFields []trackedField
	// experimentsManager allows evaluating experiments.
	experimentsManager experiments.Manager

	// Snapshot of AutoscalingOptions computed directly from CLI flags, without taking experiments into account.
	optsFromFlags internalopts.AutoscalingOptions

	mu sync.RWMutex
	// Snapshot of AutoscalingOptions combined from CLI flags and experiments - computed based on the most recent state.
	lastOpts internalopts.AutoscalingOptions
	// Snapshot of AutoscalingOptions combined from CLI flags and experiments - computed at Cluster Autoscaler startup time.
	startupOpts internalopts.AutoscalingOptions
	// startupOpts should be set once, after AutoscalingOptions are fully initialized for the first time during Cluster Autoscaler startup.
	// This bool tracks whether this has happened yet.
	startupOptsFinalized bool
	// Snapshot of the Cluster proto stored on each cluster refresh.
	clusterProto gkeclient.Cluster
	// clusterStored needs to be called before RecomputeOptions is called.
	// Tracks whether StoreCluster was updated at least once.
	clusterStored bool
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

// StoreCluster stores the latest Cluster proto in OptionsTracker.
// Thread-safe.
func (t *OptionsTracker) StoreCluster(cluster gkeclient.Cluster) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clusterProto = cluster
	t.clusterStored = true
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
// Thread-safe.
func (t *OptionsTracker) Options() internalopts.AutoscalingOptions {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastOpts
}

// RecomputeOptions recomputes the values of the tracked AutoscalingOptions fields based on the current values of experiments and the stored Cluster proto. The result
// can be obtained via Options(). Returns an error if called before a Cluster proto was stored.
// Thread-safe.
func (t *OptionsTracker) RecomputeOptions() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.clusterStored {
		return fmt.Errorf("RecomputeOptions called before a cluster proto was stored via StoreCluster")
	}

	t.recomputeOptionsWithoutClusterProto()
	t.recomputeOptionsWithClusterProto(t.clusterProto)

	// OptionsTracker needs to snapshot the initial AutoscalingOptions computed during Cluster Autoscaler startup, so that it can detect if an option changes
	// later on (which might require a CA restart). The startup AutoscalingOptions are only fully initialized after RecomputeOptions() is called for the first time
	// as part of creating GkeManager.
	if !t.startupOptsFinalized {
		// RecomputeOptions() called for the first time, snapshot the startup options.
		t.startupOpts = t.lastOpts
		t.startupOptsFinalized = true
	}
	return nil
}

// OptionChangesRequireRestart returns whether OptionsTracker has detected that Cluster Autoscaler should be restarted in order to correctly handle the value
// of a tracked field changing.
// Thread-safe.
func (t *OptionsTracker) OptionChangesRequireRestart() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, field := range t.trackedFields {
		// The values for some fields are used in Cluster Autoscaler startup logic, and if the value changes CA needs to be fully restarted so that the
		// startup logic can run again using the new value. Check if the value has changed since startup for fields that require restart.
		if field.caRestartNeededOnValueChange && !field.valueEqual(t.startupOpts, t.lastOpts) {
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

// PropagateDynamicOptions syncs tracked fields that are configured to dynamically propagate changes
// (via propagateChangesToAutoscalingContext) from the latest computed options to the provided OSS
// AutoscalingOptions (typically embedded in AutoscalingContext).
// Thread-safe.
func (t *OptionsTracker) PropagateDynamicOptions(dst *config.AutoscalingOptions) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, field := range t.trackedFields {
		if field.propagateChangesToAutoscalingContext != nil {
			field.propagateChangesToAutoscalingContext(t.lastOpts.AutoscalingOptions, dst)
		}
	}
}
