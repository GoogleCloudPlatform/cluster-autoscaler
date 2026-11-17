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
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestBalloonPodIpprResizeEnabledFieldSetValue(t *testing.T) {
	testCases := []struct {
		testName         string
		experimentValues map[string]bool
		wantValue        bool
	}{
		{
			testName:         "no experiments, expect default false",
			experimentValues: nil,
			wantValue:        false,
		},
		{
			testName: "MinCAVersion met, Enabled unset, expect true",
			experimentValues: map[string]bool{
				experiments.BalloonPodIpprResizeMinCAVersionFlag: true,
			},
			wantValue: true,
		},
		{
			testName: "MinCAVersion met, Enabled true, expect true",
			experimentValues: map[string]bool{
				experiments.BalloonPodIpprResizeMinCAVersionFlag: true,
				experiments.BalloonPodIpprResizeEnabledFlag:      true,
			},
			wantValue: true,
		},
		{
			testName: "MinCAVersion met, Enabled false, expect false",
			experimentValues: map[string]bool{
				experiments.BalloonPodIpprResizeMinCAVersionFlag: true,
				experiments.BalloonPodIpprResizeEnabledFlag:      false,
			},
			wantValue: false,
		},
		{
			testName: "MinCAVersion not met, Enabled true, expect false",
			experimentValues: map[string]bool{
				experiments.BalloonPodIpprResizeMinCAVersionFlag: false,
				experiments.BalloonPodIpprResizeEnabledFlag:      true,
			},
			wantValue: false,
		},
		{
			testName: "unrelated experiment present, expect default false",
			experimentValues: map[string]bool{
				"some-unrelated-experiment": true,
			},
			wantValue: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			t.Parallel()

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, tc.experimentValues, nil)
			optsToModify := internalopts.AutoscalingOptions{}

			err := balloonPodIpprResizeEnabledField.setValue(internalopts.AutoscalingOptions{}, experimentsManager, &optsToModify)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantValue, optsToModify.BalloonPodIpprResizeEnabled)
		})
	}
}
