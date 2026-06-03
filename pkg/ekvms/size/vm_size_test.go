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
)

func TestMaxSize(t *testing.T) {
	testCases := []struct {
		desc         string
		size1        VmSize
		size2        VmSize
		expectedSize VmSize
	}{
		{
			desc: "s1 > s2",
			size1: VmSize{
				MilliCpus: 2001,
				KBytes:    2002,
			},
			size2: VmSize{
				MilliCpus: 1001,
				KBytes:    1002,
			},
			expectedSize: VmSize{
				MilliCpus: 2001,
				KBytes:    2002,
			},
		},
		{
			desc: "s1 < s2",
			size1: VmSize{
				MilliCpus: 1001,
				KBytes:    1002,
			},
			size2: VmSize{
				MilliCpus: 2001,
				KBytes:    2002,
			},
			expectedSize: VmSize{
				MilliCpus: 2001,
				KBytes:    2002,
			},
		},
		{
			desc: "s1.MilliCpus < s2.MilliCpus and s1.KBytes > s2.KBytes",
			size1: VmSize{
				MilliCpus: 1001,
				KBytes:    2002,
			},
			size2: VmSize{
				MilliCpus: 2001,
				KBytes:    1002,
			},
			expectedSize: VmSize{
				MilliCpus: 2001,
				KBytes:    2002,
			},
		},
		{
			desc: "s1.MilliCpus > s2.MilliCpus and s1.KBytes < s2.KBytes",
			size1: VmSize{
				MilliCpus: 2001,
				KBytes:    1002,
			},
			size2: VmSize{
				MilliCpus: 1001,
				KBytes:    2002,
			},
			expectedSize: VmSize{
				MilliCpus: 2001,
				KBytes:    2002,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			size := MaxSize(tc.size1, tc.size2)
			assert.Equal(t, tc.expectedSize, size)
		})
	}
}

func TestIsUpsizeVmSize(t *testing.T) {
	testCases := []struct {
		desc         string
		startingSize VmSize
		desiredSize  VmSize
		wantUpsize   bool
	}{
		{
			desc: "upsize",
			startingSize: VmSize{
				MilliCpus: 1000,
				KBytes:    1000 * 1024 * 1024,
			},
			desiredSize: VmSize{
				MilliCpus: 2000,
				KBytes:    2000 * 1024 * 1024,
			},
			wantUpsize: true,
		},
		{
			desc: "downsize",
			startingSize: VmSize{
				MilliCpus: 2000,
				KBytes:    2000 * 1024 * 1024,
			},
			desiredSize: VmSize{
				MilliCpus: 1000,
				KBytes:    1000 * 1024 * 1024,
			},
			wantUpsize: false,
		},
		{
			desc: "mixed - cpu downsize",
			startingSize: VmSize{
				MilliCpus: 2000,
				KBytes:    1000 * 1024 * 1024,
			},
			desiredSize: VmSize{
				MilliCpus: 1000,
				KBytes:    2000 * 1024 * 1024,
			},
			wantUpsize: false,
		},
		{
			desc: "mixed - mem downsize",
			startingSize: VmSize{
				MilliCpus: 1000,
				KBytes:    2000 * 1024 * 1024,
			},
			desiredSize: VmSize{
				MilliCpus: 2000,
				KBytes:    1000 * 1024 * 1024,
			},
			wantUpsize: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			gotUpsize := tc.desiredSize.IsUpsizeFrom(tc.startingSize)
			assert.Equal(t, tc.wantUpsize, gotUpsize)
		})
	}
}

func TestIsDownsizeVmSize(t *testing.T) {
	testCases := []struct {
		desc         string
		startingSize VmSize
		desiredSize  VmSize
		wantDownsize bool
	}{
		{
			desc: "upsize",
			startingSize: VmSize{
				MilliCpus: 1000,
				KBytes:    1000 * 1024 * 1024,
			},
			desiredSize: VmSize{
				MilliCpus: 2000,
				KBytes:    2000 * 1024 * 1024,
			},
			wantDownsize: false,
		},
		{
			desc: "downsize",
			startingSize: VmSize{
				MilliCpus: 2000,
				KBytes:    2000 * 1024 * 1024,
			},
			desiredSize: VmSize{
				MilliCpus: 1000,
				KBytes:    1000 * 1024 * 1024,
			},
			wantDownsize: true,
		},
		{
			desc: "mixed - mem upsize",
			startingSize: VmSize{
				MilliCpus: 2000,
				KBytes:    1000 * 1024 * 1024,
			},
			desiredSize: VmSize{
				MilliCpus: 1000,
				KBytes:    2000 * 1024 * 1024,
			},
			wantDownsize: false,
		},
		{
			desc: "mixed - cpu upsize",
			startingSize: VmSize{
				MilliCpus: 1000,
				KBytes:    2000 * 1024 * 1024,
			},
			desiredSize: VmSize{
				MilliCpus: 2000,
				KBytes:    1000 * 1024 * 1024,
			},
			wantDownsize: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			gotDownsize := tc.desiredSize.IsDownsizeFrom(tc.startingSize)
			assert.Equal(t, tc.wantDownsize, gotDownsize)
		})
	}
}

func TestVmSizePrint(t *testing.T) {
	tests := []struct {
		desc   string
		vmSize VmSize
		want   string
	}{
		{
			desc:   "Struct is printed not empty",
			vmSize: VmSize{},
			want:   "VmSize{0 mCPU, 0 KiB}",
		},
		{
			desc:   "Struct with CPU value only is printed not empty",
			vmSize: VmSize{MilliCpus: 1000},
			want:   "VmSize{1000 mCPU, 0 KiB}",
		},
		{
			desc:   "Struct with mem value only is printed not empty",
			vmSize: VmSize{KBytes: 1024},
			want:   "VmSize{0 mCPU, 1024 KiB}",
		},
		{
			desc:   "Struct with both CPU and mem values is printed not empty",
			vmSize: VmSize{MilliCpus: 1000, KBytes: 1024},
			want:   "VmSize{1000 mCPU, 1024 KiB}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			t.Logf("Log: [%v]", tt.vmSize)
			assert.NotEmpty(t, fmt.Sprint(tt.vmSize))
			assert.Equal(t, fmt.Sprint(tt.vmSize), fmt.Sprint(&tt.vmSize))
			assert.Equal(t, tt.want, fmt.Sprint(tt.vmSize))
		})
	}
}

func TestMinSize(t *testing.T) {
	tests := []struct {
		name string

		s1   VmSize
		s2   VmSize
		want VmSize
	}{
		{
			name: "s1 > s2",
			s1: VmSize{
				MilliCpus: 2000,
				KBytes:    2000,
			},
			s2: VmSize{
				MilliCpus: 1000,
				KBytes:    1000,
			},
			want: VmSize{
				MilliCpus: 1000,
				KBytes:    1000,
			},
		},
		{
			name: "s1 < s2",
			s1: VmSize{
				MilliCpus: 1000,
				KBytes:    1000,
			},
			s2: VmSize{
				MilliCpus: 2000,
				KBytes:    2000,
			},
			want: VmSize{
				MilliCpus: 1000,
				KBytes:    1000,
			},
		},
		{
			name: "s1.MilliCpus < s2.MilliCpus and s1.KBytes > s2.KBytes",
			s1: VmSize{
				MilliCpus: 1000,
				KBytes:    2000,
			},
			s2: VmSize{
				MilliCpus: 2000,
				KBytes:    1000,
			},
			want: VmSize{
				MilliCpus: 1000,
				KBytes:    1000,
			},
		},
		{
			name: "s1.MilliCpus > s2.MilliCpus and s1.KBytes < s2.KBytes",
			s1: VmSize{
				MilliCpus: 2000,
				KBytes:    1000,
			},
			s2: VmSize{
				MilliCpus: 1000,
				KBytes:    2000,
			},
			want: VmSize{
				MilliCpus: 1000,
				KBytes:    1000,
			},
		},
		{
			name: "s1 == s2",
			s1: VmSize{
				MilliCpus: 1000,
				KBytes:    1000,
			},
			s2: VmSize{
				MilliCpus: 1000,
				KBytes:    1000,
			},
			want: VmSize{
				MilliCpus: 1000,
				KBytes:    1000,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MinSize(tt.s1, tt.s2)
			if got != tt.want {
				t.Errorf("MinSize() = %v, want %v", got, tt.want)
			}
		})
	}
}
