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
	resizable_vm_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestParseEkAutoprovisioningMode(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedMode resizable_vm_types.EkAutoprovisioningMode
		expectedErr  error
	}{
		{
			name:         "valid EkAutoprovisioningUnspecified",
			input:        string(resizable_vm_types.EkAutoprovisioningUnspecified),
			expectedMode: resizable_vm_types.EkAutoprovisioningUnspecified,
		},
		{
			name:         "valid EkAutoprovisioningDisabled",
			input:        string(resizable_vm_types.EkAutoprovisioningDisabled),
			expectedMode: resizable_vm_types.EkAutoprovisioningDisabled,
		},
		{
			name:         "valid EkAutoprovisioningEnabledCoarseGrainedResize",
			input:        string(resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize),
			expectedMode: resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize,
		},
		{
			name:         "valid EkAutoprovisioningDisabledCgroupv1Detected",
			input:        string(resizable_vm_types.EkAutoprovisioningDisabledCgroupv1Detected),
			expectedMode: resizable_vm_types.EkAutoprovisioningDisabledCgroupv1Detected,
		},
		{
			name:         "invalid EkAutoprovisioning value",
			input:        "INVALID_VALUE",
			expectedMode: "",
			expectedErr:  fmt.Errorf("unrecognized flag for EkAutoprovisioning: %q", "INVALID_VALUE"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotMode, gotErr := parseEkAutoprovisioningMode(tc.input)
			if tc.expectedErr == nil {
				assert.Equal(t, tc.expectedMode, gotMode)
				assert.NoError(t, gotErr)
			} else {
				assert.Error(t, gotErr)
			}
		})
	}
}

func TestRefreshEkLaunchStatus(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		isBalloonPodCreatable bool
		ekAutoprovisioning    resizable_vm_types.EkAutoprovisioningMode
		expectedLaunchPhase   launchPhase
		expectedLaunchFrom    launchSource
	}{
		{
			name:                  "ek_autoprovisioning_disabled",
			isBalloonPodCreatable: true,
			ekAutoprovisioning:    resizable_vm_types.EkAutoprovisioningDisabled,
			expectedLaunchPhase:   launchDisabled,
			expectedLaunchFrom:    launchUndefined,
		},
		{
			name:                  "ek_autoprovisioning_enabled_coarse_grained_resize",
			isBalloonPodCreatable: true,
			ekAutoprovisioning:    resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize,
			expectedLaunchPhase:   launchCoarseGrainedResize,
			expectedLaunchFrom:    launchClusterProto,
		},
		{
			name:                  "ek_autoprovisioning_enabled_coarse_grained_resize - enabled by default",
			isBalloonPodCreatable: true,
			ekAutoprovisioning:    resizable_vm_types.EkAutoprovisioningUnspecified,
			expectedLaunchPhase:   launchCoarseGrainedResize,
			expectedLaunchFrom:    launchUndefined,
		},
		{
			name:                  "ek_autoprovisioning_disabled",
			isBalloonPodCreatable: true,
			ekAutoprovisioning:    resizable_vm_types.EkAutoprovisioningDisabled,
			expectedLaunchPhase:   launchDisabled,
			expectedLaunchFrom:    launchUndefined,
		},
		{
			name:                  "ek_autoprovisioning_disabled_cgroupv1_detected",
			isBalloonPodCreatable: true,
			ekAutoprovisioning:    resizable_vm_types.EkAutoprovisioningDisabledCgroupv1Detected,
			expectedLaunchPhase:   launchDisabledCgroupv1,
			expectedLaunchFrom:    launchUndefined,
		},
		{
			name:                  "ek_autoprovisioning_disabled_balloon_pod_error",
			isBalloonPodCreatable: false,
			ekAutoprovisioning:    resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize,
			expectedLaunchPhase:   launchDisabledBalloonPodError,
			expectedLaunchFrom:    launchUndefined,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &ekAutoprovisioningProvider{
				mode: tc.ekAutoprovisioning,
				status: LaunchStatus{
					phase:  launchNotEnabled,
					source: launchUndefined,
				},
				bpChecker: &balloonPodChecker{
					isBalloonPodCreatable: tc.isBalloonPodCreatable,
				},
			}
			p.refreshLaunchStatus()
			assert.Equal(t, tc.expectedLaunchPhase, p.status.phase)
			assert.Equal(t, tc.expectedLaunchFrom, p.status.source)
		})
	}
}

func TestRefreshEkManagedNodesStatus(t *testing.T) {
	tests := []struct {
		name                        string
		enabledOnManagedNodesCAFlag bool
		experimentFlags             []string
		want                        bool
	}{
		{
			name:                        "enabled via CA flags",
			enabledOnManagedNodesCAFlag: true,
			want:                        true,
		},
		{
			name:                        "not enabled and no experiment",
			enabledOnManagedNodesCAFlag: false,
			want:                        false,
		},
		{
			name:                        "enabled via experiment",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags:             []string{experiments.EKOnManagedNodesMinCAVersionFlag},
			want:                        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ekAutoprovisioningProvider{
				enabledOnManagedNodesCAFlag: tt.enabledOnManagedNodesCAFlag,
				experimentsManager:          experiments.NewMockManager(tt.experimentFlags...),
			}
			p.refreshManagedNodesStatus()
			assert.Equal(t, tt.want, p.enabledOnManagedNodes)
		})
	}
}

func TestIsEkEnabledInAutopilot(t *testing.T) {
	for _, tc := range []struct {
		name             string
		ekLaunchPhase    launchPhase
		autopilotEnabled bool
		want             bool
	}{
		{
			name:          "ek_in_autopilot_should_be_disabled_with_ek_launch_disabled",
			ekLaunchPhase: launchDisabled,
			want:          false,
		},
		{
			name:          "ek_in_autopilot_should_be_disabled_with_ek_launch_disabled_cgroupv1",
			ekLaunchPhase: launchDisabledCgroupv1,
			want:          false,
		},
		{
			name:          "ek_in_autopilot_should_be_disabled_with_ek_launch_init_downsizing",
			ekLaunchPhase: launchEnabledNoResize,
			want:          false,
		},
		{
			name:          "ek_in_autopilot_should_be_disabled_with_ek_launch_not_enabled",
			ekLaunchPhase: launchNotEnabled,
			want:          false,
		},
		{
			name:             "ek_in_autopilot_should_be_disabled_with_standard_mode",
			ekLaunchPhase:    launchCoarseGrainedResize,
			autopilotEnabled: false,
			want:             false,
		},
		{
			name:             "ek_in_autopilot_should_be_enabled_with_autopilot_mode_and_ek_launch_coarse_grained_resize",
			ekLaunchPhase:    launchCoarseGrainedResize,
			autopilotEnabled: true,
			want:             true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &ekAutoprovisioningProvider{
				status: LaunchStatus{
					phase:  tc.ekLaunchPhase,
					source: "",
				},
				autopilotEnabled: tc.autopilotEnabled,
			}

			assert.Equal(t, tc.want, p.isEnabledInAutopilot())
		})
	}
}

func TestEkManagedNodesEnabled(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		ekLaunchPhase           launchPhase
		ekOnManagedNodesEnabled bool
		want                    bool
	}{
		{
			name:                    "ek on managed nodes disabled if ekvms launch disabled and ek on managed nodes disabled",
			ekLaunchPhase:           launchDisabled,
			ekOnManagedNodesEnabled: false,
			want:                    false,
		},
		{
			name:                    "ek on managed nodes disabled if ekvms launch init downsizing and ek on managed nodes enabled",
			ekLaunchPhase:           launchEnabledNoResize,
			ekOnManagedNodesEnabled: true,
			want:                    false,
		},
		{
			name:                    "ek on managed nodes disabled if ekvms enabled and ek on managed nodes disabled",
			ekLaunchPhase:           launchCoarseGrainedResize,
			ekOnManagedNodesEnabled: false,
			want:                    false,
		},
		{
			name:                    "ek on managed nodes enabled if ekvms enabled and ek on managed nodes enabled",
			ekLaunchPhase:           launchCoarseGrainedResize,
			ekOnManagedNodesEnabled: true,
			want:                    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &ekAutoprovisioningProvider{
				status: LaunchStatus{
					phase: tc.ekLaunchPhase,
				},
				enabledOnManagedNodes: tc.ekOnManagedNodesEnabled,
			}

			assert.Equal(t, tc.want, p.resizingEnabled())
		})
	}
}

func TestEkNodesCount(t *testing.T) {
	for _, tc := range []struct {
		description        string
		nodesCountProvider nodesCountProvider
		expectedNodesCount int
	}{
		{
			description:        "EK nodesCountProvider is nil, should return 0",
			nodesCountProvider: nil,
			expectedNodesCount: 0,
		},
		{
			description:        "EK nodesCountProvider is nil 2nd case, should return 0",
			nodesCountProvider: (*mockNodesCountProvider)(nil),
			expectedNodesCount: 0,
		},
		{
			description: "EK nodesCountProvider returns 5, should return 5",
			nodesCountProvider: func() nodesCountProvider {
				m := &mockNodesCountProvider{}
				m.On("NodesCount", machinetypes.EK.Name()).Return(5)
				return m
			}(),
			expectedNodesCount: 5,
		},
	} {
		t.Run(tc.description, func(t *testing.T) {

			p := &ekAutoprovisioningProvider{}

			p.registerNodesCountProvider(tc.nodesCountProvider)
			assert.Equal(t, tc.expectedNodesCount, p.nodesCount())

		})
	}
}
