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

func flagsWithLimit(val string) map[string]string {
	return map[string]string{experiments.ColdStandbyNodesMinUnsupportedMemoryGBFlag: val}
}
