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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestRefreshE4LaunchStatus(t *testing.T) {
	tests := []struct {
		name                  string
		isBalloonPodCreatable bool
		experimentFlags       map[string]bool
		expectedPhase         launchPhase
		expectedSource        launchSource
	}{
		{
			name:                  "balloon pod error - disabled",
			isBalloonPodCreatable: false,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4MinVersionFlag:      true,
				experiments.AutopilotE4NoResizeEnabledFlag: false,
			},
			expectedPhase:  launchDisabledBalloonPodError,
			expectedSource: launchUndefined,
		},
		{
			name:                  "experiment enables coarse resize",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4MinVersionFlag:      true,
				experiments.AutopilotE4NoResizeEnabledFlag: false,
			},
			expectedPhase:  launchCoarseGrainedResize,
			expectedSource: launchExperiment,
		},
		{
			name:                  "experiment enables E4 without resize (default)",
			isBalloonPodCreatable: true,
			experimentFlags: map[string]bool{
				experiments.AutopilotE4MinVersionFlag: true,
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

			p := &e4AutoprovisioningProvider{
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
			name:                        "enabled via experiment",
			enabledOnManagedNodesCAFlag: false,
			experimentFlags: map[string]bool{
				experiments.E4OnManagedNodesMinCAVersionFlag: true,
			},
			want: true,
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
			experimentsManager := experiments.NewMockManagerWithOptions(
				version.Version{},
				tc.experimentFlags,
				map[string]string{},
			)

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
