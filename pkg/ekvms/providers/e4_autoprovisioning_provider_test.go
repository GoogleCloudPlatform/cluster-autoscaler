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

package providers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	resizable_vm_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestParseE4AutoprovisioningMode(t *testing.T) {
	tests := []struct {
		input        string
		expectedMode resizable_vm_types.E4AutoprovisioningMode
		wantErr      bool
	}{
		{"", resizable_vm_types.E4AutoprovisioningUnspecified, false},
		{string(resizable_vm_types.E4AutoprovisioningUnspecified), resizable_vm_types.E4AutoprovisioningUnspecified, false},
		{string(resizable_vm_types.E4AutoprovisioningDisabled), resizable_vm_types.E4AutoprovisioningDisabled, false},
		{string(resizable_vm_types.E4AutoprovisioningEnabledNoResize), resizable_vm_types.E4AutoprovisioningEnabledNoResize, false},
		{string(resizable_vm_types.E4AutoprovisioningEnabledCoarseGrainedResize), resizable_vm_types.E4AutoprovisioningEnabledCoarseGrainedResize, false},
		{"INVALID_VALUE", "", true},
	}

	for _, tc := range tests {
		gotMode, err := parseE4AutoprovisioningMode(tc.input)
		assert.Equal(t, tc.wantErr, err != nil)
		assert.Equal(t, tc.expectedMode, gotMode)
	}
}

func TestRefreshE4LaunchStatus(t *testing.T) {
	tests := []struct {
		name                  string
		mode                  resizable_vm_types.E4AutoprovisioningMode
		isBalloonPodCreatable bool
		experimentFlags       map[string]bool
		expectedPhase         launchPhase
		expectedSource        launchSource
	}{
		{
			name:                  "balloon pod error - flag coarse resize disabled",
			mode:                  resizable_vm_types.E4AutoprovisioningEnabledCoarseGrainedResize,
			isBalloonPodCreatable: false,
			experimentFlags:       map[string]bool{},
			expectedPhase:         launchDisabledBalloonPodError,
			expectedSource:        launchUndefined,
		},
		{
			name:                  "balloon pod error - flag no resize still enabled",
			mode:                  resizable_vm_types.E4AutoprovisioningEnabledNoResize,
			isBalloonPodCreatable: false,
			experimentFlags:       map[string]bool{},
			expectedPhase:         launchEnabledNoResize,
			expectedSource:        launchClusterProto,
		},
		{
			name:                  "balloon pod error - experiment coarse resize disabled",
			isBalloonPodCreatable: false,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: true,
				experiments.AutopilotE4WithResizeEnabledFlag:    true,
			},
			expectedPhase:  launchDisabledBalloonPodError,
			expectedSource: launchUndefined,
		},
		{
			name:                  "balloon pod error - no resize enabled",
			isBalloonPodCreatable: false,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4MinVersionFlag:      true,
				experiments.AutopilotE4NoResizeEnabledFlag: true,
			},
			expectedPhase:  launchEnabledNoResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "flag disabled overrides experiment enabled",
			mode:                  resizable_vm_types.E4AutoprovisioningDisabled,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: true,
				experiments.AutopilotE4WithResizeEnabledFlag:    true,
			},
			expectedPhase:  launchDisabled,
			expectedSource: launchUndefined,
		},
		{
			name:                  "flag coarse resize overrides experiment disabled",
			mode:                  resizable_vm_types.E4AutoprovisioningEnabledCoarseGrainedResize,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: false,
				experiments.AutopilotE4WithResizeEnabledFlag:    true,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchClusterProto,
		},
		{
			name:                  "flag no resize overrides experiment coarse resize",
			mode:                  resizable_vm_types.E4AutoprovisioningEnabledNoResize,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: true,
				experiments.AutopilotE4WithResizeEnabledFlag:    true,
			},
			expectedPhase:  launchEnabledNoResize,
			expectedSource: launchClusterProto,
		},
		{
			name:                  "flag coarse resize overrides experiment no resize",
			mode:                  resizable_vm_types.E4AutoprovisioningEnabledCoarseGrainedResize,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: false,
				experiments.AutopilotE4WithResizeEnabledFlag:    true,
				experiments.AutopilotE4MinVersionFlag:           true,
				experiments.AutopilotE4NoResizeEnabledFlag:      true,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchClusterProto,
		},
		{
			name:                  "experiment enables coarse resize",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: true,
				experiments.AutopilotE4WithResizeEnabledFlag:    true,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "coarse resize enabled flag missing - enabled (failsafe is true)",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: true,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "coarse resize min version not met - not enabled",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: false,
				experiments.AutopilotE4WithResizeEnabledFlag:    true,
			},
			expectedPhase:  launchNotEnabled,
			expectedSource: launchUndefined,
		},
		{
			name:                  "coarse resize enabled flag false - not enabled",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: true,
				experiments.AutopilotE4WithResizeEnabledFlag:    false,
			},
			expectedPhase:  launchNotEnabled,
			expectedSource: launchUndefined,
		},
		{
			name:                  "experiment enables E4 without resize",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4MinVersionFlag:      true,
				experiments.AutopilotE4NoResizeEnabledFlag: true,
			},
			expectedPhase:  launchEnabledNoResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "no resize enabled flag missing - enabled (failsafe is true)",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4MinVersionFlag: true,
			},
			expectedPhase:  launchEnabledNoResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "no resize enabled flag false - not enabled",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4MinVersionFlag:      true,
				experiments.AutopilotE4NoResizeEnabledFlag: false,
			},
			expectedPhase:  launchNotEnabled,
			expectedSource: launchUndefined,
		},
		{
			name:                  "both coarse resize and no resize enabled - coarse resize takes precedence",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: true,
				experiments.AutopilotE4WithResizeEnabledFlag:    true,
				experiments.AutopilotE4MinVersionFlag:           true,
				experiments.AutopilotE4NoResizeEnabledFlag:      true,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "coarse resize disabled by flag, no resize enabled",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4WithResizeMinVersionFlag: true,
				experiments.AutopilotE4WithResizeEnabledFlag:    false,
				experiments.AutopilotE4MinVersionFlag:           true,
				experiments.AutopilotE4NoResizeEnabledFlag:      true,
			},
			expectedPhase:  launchEnabledNoResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "no experiment - not enabled",
			isBalloonPodCreatable: true,
			experimentFlags:       map[string]bool{},
			expectedPhase:         launchNotEnabled,
			expectedSource:        launchUndefined,
		},
		{
			name:                  "experiment mitigated - not enabled",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4MinVersionFlag: false,
			},
			expectedPhase:  launchNotEnabled,
			expectedSource: launchUndefined,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			experimentsManager := experiments.NewMockManagerWithOptions(
				version.Version{},
				tc.experimentFlags,
				map[string]string{},
			)

			mode, err := parseE4AutoprovisioningMode(string(tc.mode))
			assert.NoError(t, err)

			p := &e4AutoprovisioningProvider{
				mode:               mode,
				experimentsManager: experimentsManager,
				status: LaunchStatus{
					phase:  launchNotEnabled,
					source: launchUndefined,
				},
				bpChecker: &balloonPodChecker{
					isBalloonPodCreatable: tc.isBalloonPodCreatable,
				},
				metrics: new(mockResizableVmMetrics),
			}

			p.refreshLaunchStatus()
			assert.Equal(t, tc.expectedPhase, p.status.phase)
			assert.Equal(t, tc.expectedSource, p.status.source)
		})
	}
}

func TestRefreshE4ManagedNodesStatus(t *testing.T) {
	tests := []struct {
		name                        string
		enabledOnManagedNodesCAFlag bool
		nilExperimentsManager       bool
		experimentFlags             map[string]bool
		want                        bool
	}{
		{
			name:                        "enabled via CA flag",
			enabledOnManagedNodesCAFlag: true,
			want:                        true,
		},
		{
			name:                        "disabled when CA flag is false and no experiment",
			enabledOnManagedNodesCAFlag: false,
			want:                        false,
		},
		{
			name:                        "disabled when CA flag is false and experimentsManager is nil",
			enabledOnManagedNodesCAFlag: false,
			nilExperimentsManager:       true,
			want:                        false,
		},
		{
			name:                        "enabled via experiment (default enabled flag)",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags: map[string]bool{
				experiments.E4OnManagedNodesMinCAVersionFlag: true,
			},
			want: true,
		},
		{
			name:                        "enabled via experiment (explicit enabled flag)",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags: map[string]bool{
				experiments.E4OnManagedNodesMinCAVersionFlag: true,
				experiments.E4OnManagedNodesEnabledFlag:      true,
			},
			want: true,
		},
		{
			name:                        "disabled via experiment (explicit disabled flag)",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags: map[string]bool{
				experiments.E4OnManagedNodesMinCAVersionFlag: true,
				experiments.E4OnManagedNodesEnabledFlag:      false,
			},
			want: false,
		},
		{
			name:                        "disabled via experiment (min version not met)",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags: map[string]bool{
				experiments.E4OnManagedNodesMinCAVersionFlag: false,
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var experimentsManager experiments.Manager
			if !tc.nilExperimentsManager {
				experimentsManager = experiments.NewMockManagerWithOptions(
					version.Version{},
					tc.experimentFlags,
					map[string]string{},
				)
			}

			p := &e4AutoprovisioningProvider{
				enabledOnManagedNodesCAFlag: tc.enabledOnManagedNodesCAFlag,
				experimentsManager:          experimentsManager,
				metrics:                     new(mockResizableVmMetrics),
			}

			p.refreshManagedNodesStatus()
			assert.Equal(t, tc.want, p.enabledOnManagedNodes)
		})
	}
}

func TestIsE4EnabledInAutopilot(t *testing.T) {
	tests := []struct {
		name             string
		launchPhase      launchPhase
		autopilotEnabled bool
		want             bool
	}{
		{
			name:             "disabled when autopilot is disabled",
			launchPhase:      launchCoarseGrainedResize,
			autopilotEnabled: false,
			want:             false,
		},
		{
			name:             "disabled when launch phase is disabled",
			launchPhase:      launchDisabled,
			autopilotEnabled: true,
			want:             false,
		},
		{
			name:             "disabled when launch phase is not enabled",
			launchPhase:      launchNotEnabled,
			autopilotEnabled: true,
			want:             false,
		},
		{
			name:             "disabled on balloon pod error",
			launchPhase:      launchDisabledBalloonPodError,
			autopilotEnabled: true,
			want:             false,
		},
		{
			name:             "enabled when phase is coarse grained resize",
			launchPhase:      launchCoarseGrainedResize,
			autopilotEnabled: true,
			want:             true,
		},
		{
			name:             "enabled when phase is init downsizing",
			launchPhase:      launchEnabledNoResize,
			autopilotEnabled: true,
			want:             true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &e4AutoprovisioningProvider{
				status: LaunchStatus{
					phase: tc.launchPhase,
				},
				autopilotEnabled: tc.autopilotEnabled,
			}

			assert.Equal(t, tc.want, p.isEnabledInAutopilot())
		})
	}
}

func TestE4ManagedNodesEnabled(t *testing.T) {
	tests := []struct {
		name                  string
		launchPhase           launchPhase
		enabledOnManagedNodes bool
		want                  bool
	}{
		{
			name:                  "disabled when managed nodes flag is false",
			launchPhase:           launchCoarseGrainedResize,
			enabledOnManagedNodes: false,
			want:                  false,
		},
		{
			name:                  "disabled when launch phase is disabled",
			launchPhase:           launchDisabled,
			enabledOnManagedNodes: true,
			want:                  false,
		},
		{
			name:                  "enabled when managed nodes flag is true and phase is coarse grained resize",
			launchPhase:           launchCoarseGrainedResize,
			enabledOnManagedNodes: true,
			want:                  true,
		},
		{
			name:                  "enabled when managed nodes flag is true and phase is init downsizing",
			launchPhase:           launchEnabledNoResize,
			enabledOnManagedNodes: true,
			want:                  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &e4AutoprovisioningProvider{
				status: LaunchStatus{
					phase: tc.launchPhase,
				},
				enabledOnManagedNodes: tc.enabledOnManagedNodes,
			}

			assert.Equal(t, tc.want, p.managedNodesEnabled())
		})
	}
}
