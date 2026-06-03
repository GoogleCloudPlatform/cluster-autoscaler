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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

func TestGetNodeNameFromInstance(t *testing.T) {
	testCases := []struct {
		desc          string
		inst          *cloudprovider.Instance
		expectedName  string
		expectedError bool
	}{
		{
			desc: "Default",
			inst: &cloudprovider.Instance{
				Id: "gce://p/z/instance",
			},
			expectedName:  "instance",
			expectedError: false,
		},
		{
			desc: "error expected",
			inst: &cloudprovider.Instance{
				Id: "instance",
			},
			expectedName:  "",
			expectedError: true,
		},
		{
			desc:          "nil",
			inst:          nil,
			expectedName:  "",
			expectedError: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			gotName, gotErr := GetNodeNameFromInstance(tc.inst)
			if tc.expectedError {
				assert.Error(t, gotErr)
			}
			assert.Equal(t, gotName, tc.expectedName)
		})
	}
}

func TestRightShiftTransformResourceLimiter(t *testing.T) {
	testCases := []struct {
		desc                string
		currentMinResources map[string]int64
		currentMaxResources map[string]int64
		additionalResources map[string]int64
		expectedMin         map[string]int64
		expectedMax         map[string]int64
	}{
		{
			desc:                "cpu increase",
			currentMinResources: map[string]int64{"cpu": 1},
			currentMaxResources: map[string]int64{"cpu": 4},
			additionalResources: map[string]int64{"cpu": 1},
			expectedMin:         map[string]int64{"cpu": 2},
			expectedMax:         map[string]int64{"cpu": 5},
		},
		{
			desc:                "different resource increase",
			currentMinResources: map[string]int64{"cpu": 1},
			currentMaxResources: map[string]int64{"cpu": 4},
			additionalResources: map[string]int64{"mem": 1},
			expectedMin:         map[string]int64{"cpu": 1},
			expectedMax:         map[string]int64{"cpu": 4},
		},
		{
			desc:                "multiple resource increase",
			currentMinResources: map[string]int64{"cpu": 1, "mem": 1},
			currentMaxResources: map[string]int64{"cpu": 4, "mem": 2},
			additionalResources: map[string]int64{"cpu": 1, "mem": 1},
			expectedMin:         map[string]int64{"cpu": 2, "mem": 2},
			expectedMax:         map[string]int64{"cpu": 5, "mem": 3},
		},
		{
			desc:                "additional resources has different resource",
			currentMinResources: map[string]int64{"cpu": 1},
			currentMaxResources: map[string]int64{"cpu": 4},
			additionalResources: map[string]int64{"cpu": 1, "mem": 1},
			expectedMin:         map[string]int64{"cpu": 2},
			expectedMax:         map[string]int64{"cpu": 5},
		},
		{
			desc:                "nil additional",
			currentMinResources: map[string]int64{"cpu": 1},
			currentMaxResources: map[string]int64{"cpu": 4},
			additionalResources: nil,
			expectedMin:         map[string]int64{"cpu": 1},
			expectedMax:         map[string]int64{"cpu": 4},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rl := cloudprovider.NewResourceLimiter(tc.currentMinResources, tc.currentMaxResources)
			gotRL := RightShiftTransformResourceLimiter(rl, tc.additionalResources)
			assert.Equal(t, tc.expectedMin, getResourceMap(gotRL, true))
			assert.Equal(t, tc.expectedMax, getResourceMap(gotRL, false))
		})
	}
}

func getResourceMap(rl *cloudprovider.ResourceLimiter, min bool) map[string]int64 {
	rmap := map[string]int64{}
	for _, resource := range rl.GetResources() {
		if min {
			rmap[resource] = rl.GetMin(resource)
		} else {
			rmap[resource] = rl.GetMax(resource)
		}
	}
	return rmap
}

func TestGetRegionFromLocation(t *testing.T) {
	tests := []struct {
		name     string
		location string
		want     string
		wantErr  bool
	}{
		{
			name:     "simple test",
			location: "us-central1-a",
			want:     "us-central1",
			wantErr:  false,
		},
		{
			name:     "simple test eu",
			location: "europe-central2-c",
			want:     "europe-central2",
			wantErr:  false,
		},
		{
			name:     "region",
			location: "us-central1",
			want:     "us-central1",
			wantErr:  false,
		},
		{
			name:     "region eu",
			location: "europe-central2",
			want:     "europe-central2",
			wantErr:  false,
		},
		{
			name:     "tpc region eu",
			location: "u-europe-central2",
			want:     "u-europe-central2",
			wantErr:  false,
		},
		{
			name:     "tpc zone to region eu",
			location: "u-europe-central2-a",
			want:     "u-europe-central2",
			wantErr:  false,
		},
		{
			name:     "random text not a zone",
			location: "europeasdqweqsad",
			want:     "",
			wantErr:  true,
		},
		{
			name:     "not tpc zone",
			location: "us-europe-central2-a",
			want:     "",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetRegionFromLocation(tt.location)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRegionFromLocation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetRegionFromLocation() = %v, want %v", got, tt.want)
			}
		})
	}
}
