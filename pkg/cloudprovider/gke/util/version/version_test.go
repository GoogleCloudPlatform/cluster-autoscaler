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

package version

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestFromString(t *testing.T) {
	tests := []struct {
		ver  string
		want Version
		err  bool
	}{
		{
			ver:  "1.2.3-gke.4",
			want: Version{1, 2, 3, 4},
		},
		{
			ver:  "1.22.44-gke-rc.1222",
			want: Version{1, 22, 44, 1222},
		},
		{
			ver: "1.22.33-gke12",
			err: true,
		},
		{
			ver:  "1.22.33",
			want: Version{1, 22, 33, 0},
		},
		{
			ver:  "1.22.33-gke.lol",
			want: Version{1, 22, 33, 0},
		},
		{
			ver:  "35.200.2+wow",
			want: Version{35, 200, 2, 0},
		},
		{
			ver:  "35.200.2-gke.2+nice",
			want: Version{35, 200, 2, 2},
		},
		{
			ver:  "35.200.2-gke-rc.2+nice",
			want: Version{35, 200, 2, 2},
		},
	}
	for _, test := range tests {
		got, err := FromString(test.ver)
		if test.err && err == nil {
			t.Errorf("Parsing string %s should have returned error, but returned nil", test.ver)
		}
		if !test.err && err != nil {
			t.Errorf("Parsing string %s returned error %v, but should return nil", test.ver, err)
		}
		if diff := cmp.Diff(test.want, got); diff != "" {
			t.Errorf("Parsing string %s mismatch (-want +got):\n%s", test.ver, diff)
		}
	}
}

func TestLessThan(t *testing.T) {
	tests := []struct {
		nv1 string
		nv2 string
		// nv1 less than nv2
		less bool
	}{
		{
			nv1:  "1.2.3-gke.4",
			nv2:  "1.2.3-gke.5",
			less: true,
		},
		{
			nv1:  "1.5.3-gke.4",
			nv2:  "1.2.3-gke.5",
			less: false,
		},
		{
			nv1:  "1.1.9-gke.6",
			nv2:  "1.2.3-gke.5",
			less: true,
		},
		{
			nv1:  "1.2.5-gke.0",
			nv2:  "1.2.3-gke.5",
			less: false,
		},
	}
	for _, test := range tests {
		v1, _ := FromString(test.nv1)
		v2, _ := FromString(test.nv2)
		result := v1.LessThan(v2)
		if result != test.less {
			t.Errorf("nodeVersionLess(%s, %s) = %v, want %v", test.nv1, test.nv2, result, test.less)
		}
	}
}
