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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	resizable_vm_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestParseE4aAutoprovisioningMode(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedMode resizable_vm_types.E4aAutoprovisioningMode
		expectedErr  error
	}{
		{
			name:         "valid E4aAutoprovisioningUnspecified",
			input:        string(resizable_vm_types.E4aAutoprovisioningUnspecified),
			expectedMode: resizable_vm_types.E4aAutoprovisioningUnspecified,
		},
		{
			name:         "valid E4aAutoprovisioningDisabled",
			input:        string(resizable_vm_types.E4aAutoprovisioningDisabled),
			expectedMode: resizable_vm_types.E4aAutoprovisioningDisabled,
		},
		{
			name:         "valid E4aAutoprovisioningEnabledNoResize",
			input:        string(resizable_vm_types.E4aAutoprovisioningEnabledNoResize),
			expectedMode: resizable_vm_types.E4aAutoprovisioningEnabledNoResize,
		},
		{
			name:         "valid E4aAutoprovisioningEnabledCoarseGrainedResize",
			input:        string(resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize),
			expectedMode: resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize,
		},
		{
			name:         "invalid E4aAutoprovisioning value",
			input:        "INVALID_VALUE",
			expectedMode: "",
			expectedErr:  fmt.Errorf("unrecognized flag for E4aAutoprovisioning: %q", "INVALID_VALUE"),
		},
		{
			name:         "empty E4aAutoprovisioning value",
			input:        "",
			expectedMode: resizable_vm_types.E4aAutoprovisioningUnspecified,
			expectedErr:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotMode, gotErr := parseE4aAutoprovisioningMode(tc.input)
			if tc.expectedErr == nil {
				assert.Equal(t, tc.expectedMode, gotMode)
				assert.NoError(t, gotErr)
			} else {
				assert.Error(t, gotErr)
			}
		})
	}
}

func TestRefreshE4aLaunchStatus(t *testing.T) {
	tests := []struct {
		name                  string
		mode                  resizable_vm_types.E4aAutoprovisioningMode
		isBalloonPodCreatable bool
		experimentFlags       map[string]bool
		expectedPhase         launchPhase
		expectedSource        launchSource
	}{
		{
			name:                  "balloon pod error - disabled",
			mode:                  resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize,
			isBalloonPodCreatable: false,
			experimentFlags:       map[string]bool{},
			expectedPhase:         launchDisabledBalloonPodError,
			expectedSource:        launchUndefined,
		},
		{
			name:                  "flag disabled overrides experiment enabled",
			mode:                  resizable_vm_types.E4aAutoprovisioningDisabled,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4aWithResizeEnabledFlag:    true,
				experiments.AutopilotE4aWithResizeMinVersionFlag: true,
			},
			expectedPhase:  launchDisabled,
			expectedSource: launchUndefined,
		},
		{
			name:                  "flag coarse resize overrides experiment disabled",
			mode:                  resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4aWithResizeEnabledFlag:    true,
				experiments.AutopilotE4aWithResizeMinVersionFlag: false,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchClusterProto,
		},
		{
			name:                  "flag no resize overrides experiment coarse resize",
			mode:                  resizable_vm_types.E4aAutoprovisioningEnabledNoResize,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4aWithResizeEnabledFlag:    true,
				experiments.AutopilotE4aWithResizeMinVersionFlag: true,
			},
			expectedPhase:  launchEnabledNoResize,
			expectedSource: launchClusterProto,
		},
		{
			name:                  "flag coarse resize overrides experiment no resize",
			mode:                  resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4aWithResizeEnabledFlag:    true,
				experiments.AutopilotE4aWithResizeMinVersionFlag: false,
				experiments.AutopilotE4aNoResizeMinVersionFlag:   true,
				experiments.AutopilotE4aNoResizeEnabledFlag:      true,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchClusterProto,
		},
		{
			name:                  "flag unspecified - experiment enables coarse resize",
			mode:                  resizable_vm_types.E4aAutoprovisioningUnspecified,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4aWithResizeEnabledFlag:    true,
				experiments.AutopilotE4aWithResizeMinVersionFlag: true,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "flag unspecified - experiment enables coarse resize (default enabled flag)",
			mode:                  resizable_vm_types.E4aAutoprovisioningUnspecified,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4aWithResizeMinVersionFlag: true,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "flag unspecified - experiment enables no resize",
			mode:                  resizable_vm_types.E4aAutoprovisioningUnspecified,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4aWithResizeEnabledFlag:    true,
				experiments.AutopilotE4aWithResizeMinVersionFlag: false,
				experiments.AutopilotE4aNoResizeMinVersionFlag:   true,
				experiments.AutopilotE4aNoResizeEnabledFlag:      true,
			},
			expectedPhase:  launchEnabledNoResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "flag unspecified - experiment enables no resize (default enabled flag)",
			mode:                  resizable_vm_types.E4aAutoprovisioningUnspecified,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4aWithResizeEnabledFlag:    true,
				experiments.AutopilotE4aWithResizeMinVersionFlag: false,
				experiments.AutopilotE4aNoResizeMinVersionFlag:   true,
			},
			expectedPhase:  launchEnabledNoResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "flag unspecified and no experiment - not enabled",
			mode:                  resizable_vm_types.E4aAutoprovisioningUnspecified,
			isBalloonPodCreatable: true,
			experimentFlags:       map[string]bool{},
			expectedPhase:         launchNotEnabled,
			expectedSource:        launchUndefined,
		},
		{
			name:                  "flag unspecified and experiment mitigated - not enabled",
			mode:                  resizable_vm_types.E4aAutoprovisioningUnspecified,
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4aWithResizeEnabledFlag:    true,
				experiments.AutopilotE4aWithResizeMinVersionFlag: false,
				experiments.AutopilotE4aNoResizeMinVersionFlag:   true,
				experiments.AutopilotE4aNoResizeEnabledFlag:      false,
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

			p := &e4aAutoprovisioningProvider{
				mode:               tc.mode,
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

func TestRefreshE4aManagedNodesStatus(t *testing.T) {
	tests := []struct {
		name                        string
		enabledOnManagedNodesCAFlag bool
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
			name:                        "enabled via experiment (default enabled flag)",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags: map[string]bool{
				experiments.E4AOnManagedNodesMinCAVersionFlag: true,
			},
			want: true,
		},
		{
			name:                        "enabled via experiment (explicit enabled flag)",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags: map[string]bool{
				experiments.E4AOnManagedNodesMinCAVersionFlag: true,
				experiments.E4AOnManagedNodesEnabledFlag:      true,
			},
			want: true,
		},
		{
			name:                        "disabled via experiment (explicit disabled flag)",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags: map[string]bool{
				experiments.E4AOnManagedNodesMinCAVersionFlag: true,
				experiments.E4AOnManagedNodesEnabledFlag:      false,
			},
			want: false,
		},
		{
			name:                        "disabled via experiment (min version not met)",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags: map[string]bool{
				experiments.E4AOnManagedNodesMinCAVersionFlag: false,
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			experimentsManager := experiments.NewMockManagerWithOptions(
				version.Version{},
				tc.experimentFlags,
				map[string]string{},
			)

			p := &e4aAutoprovisioningProvider{
				enabledOnManagedNodesCAFlag: tc.enabledOnManagedNodesCAFlag,
				experimentsManager:          experimentsManager,
				metrics:                     new(mockResizableVmMetrics),
			}

			p.refreshManagedNodesStatus()
			assert.Equal(t, tc.want, p.enabledOnManagedNodes)
		})
	}
}

func TestIsE4aEnabledInAutopilot(t *testing.T) {
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
			p := &e4aAutoprovisioningProvider{
				status: LaunchStatus{
					phase: tc.launchPhase,
				},
				autopilotEnabled: tc.autopilotEnabled,
			}

			assert.Equal(t, tc.want, p.isEnabledInAutopilot())
		})
	}
}

func TestE4aManagedNodesEnabled(t *testing.T) {
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
			p := &e4aAutoprovisioningProvider{
				status: LaunchStatus{
					phase: tc.launchPhase,
				},
				enabledOnManagedNodes: tc.enabledOnManagedNodes,
			}

			assert.Equal(t, tc.want, p.managedNodesEnabled())
		})
	}
}

func TestE4aNodesCount(t *testing.T) {
	for _, tc := range []struct {
		description        string
		nodesCountProvider nodesCountProvider
		expectedNodesCount int
	}{
		{
			description:        "E4A nodesCountProvider is nil, should return 0",
			nodesCountProvider: nil,
			expectedNodesCount: 0,
		},
		{
			description:        "E4A nodesCountProvider is nil 2nd case, should return 0",
			nodesCountProvider: (*mockNodesCountProvider)(nil),
			expectedNodesCount: 0,
		},
		{
			description: "E4A nodesCountProvider returns 5, should return 5",
			nodesCountProvider: func() nodesCountProvider {
				m := &mockNodesCountProvider{}
				m.On("NodesCount", machinetypes.E4A.Name()).Return(5)
				return m
			}(),
			expectedNodesCount: 5,
		},
	} {
		t.Run(tc.description, func(t *testing.T) {

			p := &e4aAutoprovisioningProvider{}

			p.registerNodesCountProvider(tc.nodesCountProvider)
			assert.Equal(t, tc.expectedNodesCount, p.nodesCount())

		})
	}
}
