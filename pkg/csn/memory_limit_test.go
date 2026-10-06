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

package csn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestNewMemoryLimit(t *testing.T) {
	tests := []struct {
		name      string
		flagValue string
		flags     map[string]string
		want      int64
	}{
		{
			name:  "flag unset",
			flags: map[string]string{},
			want:  defaultMinUnsupportedMemoryGB,
		},
		{
			name:  "flag set",
			flags: flagsWithLimit("150"),
			want:  150,
		},
		{
			name:  "flag unparsable",
			flags: flagsWithLimit("not-a-number"),
			want:  defaultMinUnsupportedMemoryGB,
		},
		{
			name:  "flag zero",
			flags: flagsWithLimit("0"),
			want:  defaultMinUnsupportedMemoryGB,
		},
		{
			name:  "flag negative",
			flags: flagsWithLimit("-5"),
			want:  defaultMinUnsupportedMemoryGB,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{}, tc.flags)
			assert.Equal(t, tc.want, NewMemoryLimit(experimentsManager).GB())
		})
	}
}

func TestMemoryLimitGBZeroValue(t *testing.T) {
	assert.Equal(t, int64(defaultMinUnsupportedMemoryGB), MemoryLimit{}.GB())
}

func TestMemoryLimitExceededByPodRequest(t *testing.T) {
	tests := []struct {
		name   string
		limit  MemoryLimit
		memReq int64
		want   bool
	}{
		{
			name:   "well below the limit",
			memReq: 1 * units.GiB,
			want:   false,
		},
		{
			name: "just below the limit",
			// The limit is in decimal GB, so 208 GB fits and 209 GB does not.
			memReq: 208 * units.GB,
			want:   false,
		},
		{
			name:   "exactly at the limit",
			memReq: 209 * units.GB,
			want:   true,
		},
		{
			name:   "above the limit",
			memReq: 300 * units.GB,
			want:   true,
		},
		{
			name: "binary and decimal units are not interchangeable",
			// 195 GiB is 209.4 GB, which is over the limit even though 195 < 209.
			memReq: 195 * units.GiB,
			want:   true,
		},
		{
			name:   "non-default limit",
			limit:  MemoryLimit{minUnsupportedGB: 129},
			memReq: 130 * units.GB,
			want:   true,
		},
		{
			name:   "non-default limit not reached",
			limit:  MemoryLimit{minUnsupportedGB: 300},
			memReq: 250 * units.GB,
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.limit.exceededByPodRequest(test.BuildTestPod("some-pod", 1000, tc.memReq)))
		})
	}
}

func TestMemoryLimitExceededByPodRequestNilPod(t *testing.T) {
	assert.False(t, MemoryLimit{}.exceededByPodRequest(nil))
}

func flagsWithLimit(val string) map[string]string {
	return map[string]string{experiments.ColdStandbyNodesMinUnsupportedMemoryGBFlag: val}
}
