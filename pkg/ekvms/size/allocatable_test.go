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

package size

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestResourcesToSize(t *testing.T) {

	testCases := []struct {
		desc         string
		resources    v1.ResourceList
		expectedSize Allocatable
	}{
		{
			desc: "simple resource",
			resources: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1024Gi"),
			},
			expectedSize: Allocatable{
				MilliCpus: 1000,
				KBytes:    1024 * 1024 * 1024,
			},
		},
		{
			desc: "rounding",
			resources: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("1000"),
			},
			expectedSize: Allocatable{
				MilliCpus: 1000,
				KBytes:    1,
			},
		},
		{
			desc:         "empty resources",
			resources:    nil,
			expectedSize: Allocatable{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			size := ResourcesToSize(tc.resources)
			assert.Equal(t, tc.expectedSize, size)
		})
	}
}

func TestSizeComparisons(t *testing.T) {
	testCases := []struct {
		desc         string
		startingSize Allocatable
		desiredSize  Allocatable
		wantUpsize   bool
		wantDownsize bool
	}{
		{
			desc:         "startingSize CPU < desiredSize CPU && startingSize Memory < desiredSize Memory",
			startingSize: Allocatable{MilliCpus: 1, KBytes: 1 * MiB},
			desiredSize:  Allocatable{MilliCpus: 2, KBytes: 2 * MiB},
			wantUpsize:   true,
			wantDownsize: false,
		},
		{
			desc:         "startingSize CPU < desiredSize CPU && startingSize Memory == desiredSize Memory",
			startingSize: Allocatable{MilliCpus: 1, KBytes: 1 * MiB},
			desiredSize:  Allocatable{MilliCpus: 2, KBytes: 1 * MiB},
			wantUpsize:   true,
			wantDownsize: false,
		},
		{
			desc:         "startingSize CPU < desiredSize CPU && startingSize Memory > desiredSize Memory",
			startingSize: Allocatable{MilliCpus: 1, KBytes: 2 * MiB},
			desiredSize:  Allocatable{MilliCpus: 2, KBytes: 1 * MiB},
			wantUpsize:   false,
			wantDownsize: false,
		},
		{
			desc:         "startingSize CPU == desiredSize CPU && startingSize Memory < desiredSize Memory",
			startingSize: Allocatable{MilliCpus: 1, KBytes: 1 * MiB},
			desiredSize:  Allocatable{MilliCpus: 1, KBytes: 2 * MiB},
			wantUpsize:   true,
			wantDownsize: false,
		},
		{
			desc:         "startingSize CPU == desiredSize CPU && startingSize Memory == desiredSize Memory",
			startingSize: Allocatable{MilliCpus: 1, KBytes: 1 * MiB},
			desiredSize:  Allocatable{MilliCpus: 1, KBytes: 1 * MiB},
			wantUpsize:   false,
			wantDownsize: false,
		},
		{
			desc:         "startingSize CPU == desiredSize CPU && startingSize Memory > desiredSize Memory",
			startingSize: Allocatable{MilliCpus: 1, KBytes: 2 * MiB},
			desiredSize:  Allocatable{MilliCpus: 1, KBytes: 1 * MiB},
			wantUpsize:   false,
			wantDownsize: true,
		},
		{
			desc:         "startingSize CPU > desiredSize CPU && startingSize Memory < desiredSize Memory",
			startingSize: Allocatable{MilliCpus: 2, KBytes: 1 * MiB},
			desiredSize:  Allocatable{MilliCpus: 1, KBytes: 2 * MiB},
			wantUpsize:   false,
			wantDownsize: false,
		},
		{
			desc:         "startingSize CPU > desiredSize CPU && startingSize Memory == desiredSize Memory",
			startingSize: Allocatable{MilliCpus: 2, KBytes: 1 * MiB},
			desiredSize:  Allocatable{MilliCpus: 1, KBytes: 1 * MiB},
			wantUpsize:   false,
			wantDownsize: true,
		},
		{
			desc:         "startingSize CPU > desiredSize CPU && startingSize Memory > desiredSize Memory",
			startingSize: Allocatable{MilliCpus: 2, KBytes: 2 * MiB},
			desiredSize:  Allocatable{MilliCpus: 1, KBytes: 1 * MiB},
			wantUpsize:   false,
			wantDownsize: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			gotUpsize := tc.desiredSize.IsUpsizeFrom(tc.startingSize)
			assert.Equal(t, tc.wantUpsize, gotUpsize)
			gotDownsize := tc.desiredSize.IsDownsizeFrom(tc.startingSize)
			assert.Equal(t, tc.wantDownsize, gotDownsize)
		})
	}
}

func TestAllocatableStructsPrint(t *testing.T) {
	tests := []struct {
		desc        string
		allocatable Allocatable
		want        string
	}{
		{
			desc:        "Struct is printed not empty",
			allocatable: Allocatable{},
			want:        "Allocatable{0 mCPU, 0 KiB}",
		},
		{
			desc:        "Struct with CPU value only is printed not empty",
			allocatable: Allocatable{MilliCpus: 1000},
			want:        "Allocatable{1000 mCPU, 0 KiB}",
		},
		{
			desc:        "Struct with mem value only is printed not empty",
			allocatable: Allocatable{KBytes: 1024},
			want:        "Allocatable{0 mCPU, 1024 KiB}",
		},
		{
			desc:        "Struct with both CPU and mem values is printed not empty",
			allocatable: Allocatable{MilliCpus: 1000, KBytes: 1024},
			want:        "Allocatable{1000 mCPU, 1024 KiB}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			t.Logf("Log: [%v]", tt.allocatable)
			assert.NotEmpty(t, fmt.Sprint(tt.allocatable))
			assert.Equal(t, fmt.Sprint(tt.allocatable), fmt.Sprint(&tt.allocatable))
			assert.Equal(t, tt.want, fmt.Sprint(tt.allocatable))
		})
	}
}

func TestMax(t *testing.T) {
	testCases := []struct {
		desc         string
		size1        Allocatable
		size2        Allocatable
		expectedSize Allocatable
	}{
		{
			desc: "s1 > s2",
			size1: Allocatable{
				MilliCpus: 2001,
				KBytes:    2002,
			},
			size2: Allocatable{
				MilliCpus: 1001,
				KBytes:    1002,
			},
			expectedSize: Allocatable{
				MilliCpus: 2001,
				KBytes:    2002,
			},
		},
		{
			desc: "s1 < s2",
			size1: Allocatable{
				MilliCpus: 1001,
				KBytes:    1002,
			},
			size2: Allocatable{
				MilliCpus: 2001,
				KBytes:    2002,
			},
			expectedSize: Allocatable{
				MilliCpus: 2001,
				KBytes:    2002,
			},
		},
		{
			desc: "s1.MilliCpus < s2.MilliCpus and s1.KBytes > s2.KBytes",
			size1: Allocatable{
				MilliCpus: 1001,
				KBytes:    2002,
			},
			size2: Allocatable{
				MilliCpus: 2001,
				KBytes:    1002,
			},
			expectedSize: Allocatable{
				MilliCpus: 2001,
				KBytes:    2002,
			},
		},
		{
			desc: "s1.MilliCpus > s2.MilliCpus and s1.KBytes < s2.KBytes",
			size1: Allocatable{
				MilliCpus: 2001,
				KBytes:    1002,
			},
			size2: Allocatable{
				MilliCpus: 1001,
				KBytes:    2002,
			},
			expectedSize: Allocatable{
				MilliCpus: 2001,
				KBytes:    2002,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			size := Max(tc.size1, tc.size2)
			assert.Equal(t, tc.expectedSize, size)
		})
	}
}

func TestMin(t *testing.T) {
	testCases := []struct {
		desc         string
		size1        Allocatable
		size2        Allocatable
		expectedSize Allocatable
	}{
		{
			desc: "s1 > s2",
			size1: Allocatable{
				MilliCpus: 2001,
				KBytes:    2002,
			},
			size2: Allocatable{
				MilliCpus: 1001,
				KBytes:    1002,
			},
			expectedSize: Allocatable{
				MilliCpus: 1001,
				KBytes:    1002,
			},
		},
		{
			desc: "s1 < s2",
			size1: Allocatable{
				MilliCpus: 1001,
				KBytes:    1002,
			},
			size2: Allocatable{
				MilliCpus: 2001,
				KBytes:    2002,
			},
			expectedSize: Allocatable{
				MilliCpus: 1001,
				KBytes:    1002,
			},
		},
		{
			desc: "s1.MilliCpus < s2.MilliCpus and s1.KBytes > s2.KBytes",
			size1: Allocatable{
				MilliCpus: 1001,
				KBytes:    2002,
			},
			size2: Allocatable{
				MilliCpus: 2001,
				KBytes:    1002,
			},
			expectedSize: Allocatable{
				MilliCpus: 1001,
				KBytes:    1002,
			},
		},
		{
			desc: "s1.MilliCpus > s2.MilliCpus and s1.KBytes < s2.KBytes",
			size1: Allocatable{
				MilliCpus: 2001,
				KBytes:    1002,
			},
			size2: Allocatable{
				MilliCpus: 1001,
				KBytes:    2002,
			},
			expectedSize: Allocatable{
				MilliCpus: 1001,
				KBytes:    1002,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			size := Min(tc.size1, tc.size2)
			assert.Equal(t, tc.expectedSize, size)
		})
	}
}

func TestSubtract(t *testing.T) {
	testCases := []struct {
		name     string
		s1       Allocatable
		s2       Allocatable
		expected Allocatable
	}{
		{
			name: "Simple subtraction",
			s1: Allocatable{
				MilliCpus: 1000,
				KBytes:    2048,
			},
			s2: Allocatable{
				MilliCpus: 500,
				KBytes:    1024,
			},
			expected: Allocatable{
				MilliCpus: 500,
				KBytes:    1024,
			},
		},
		{
			name: "Subtraction yielding negative result",
			s1: Allocatable{
				MilliCpus: 1000,
				KBytes:    2048,
			},
			s2: Allocatable{
				MilliCpus: 1500,
				KBytes:    3072,
			},
			expected: Allocatable{
				MilliCpus: -500,
				KBytes:    -1024,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Subtract(tc.s1, tc.s2)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestAdd(t *testing.T) {
	testCases := []struct {
		name     string
		s1       Allocatable
		s2       Allocatable
		expected Allocatable
	}{
		{
			name: "Simple addition",
			s1: Allocatable{
				MilliCpus: 1000,
				KBytes:    2048,
			},
			s2: Allocatable{
				MilliCpus: 500,
				KBytes:    1024,
			},
			expected: Allocatable{
				MilliCpus: 1500,
				KBytes:    3072,
			},
		},
		{
			name: "Addition with negative values",
			s1: Allocatable{
				MilliCpus: 1000,
				KBytes:    2048,
			},
			s2: Allocatable{
				MilliCpus: -250,
				KBytes:    -512,
			},
			expected: Allocatable{
				MilliCpus: 750,
				KBytes:    1536,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Add(tc.s1, tc.s2)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestRoundUpToIncrement(t *testing.T) {
	testCases := []struct {
		name      string
		x         int64
		increment int64
		expected  int64
	}{
		{
			name:      "Exact multiple",
			x:         20,
			increment: 10,
			expected:  20,
		},
		{
			name:      "Round up in the middle",
			x:         25,
			increment: 10,
			expected:  30,
		},
		{
			name:      "Round up close to next multiple",
			x:         29,
			increment: 10,
			expected:  30,
		},
		{
			name:      "Round up far from next multiple",
			x:         21,
			increment: 10,
			expected:  30,
		},
		{
			name:      "Round up zero",
			x:         0,
			increment: 10,
			expected:  0,
		},
		{
			name:      "Round up negative value",
			x:         -27,
			increment: 10,
			expected:  -20,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := RoundUpToIncrement(tc.x, tc.increment)
			assert.Equal(t, tc.expected, result)
		})
	}

}
