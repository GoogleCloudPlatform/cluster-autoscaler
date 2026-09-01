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

package cli

import (
	"reflect"
	"testing"
)

func TestProcessFlagOverrides(t *testing.T) {
	testCases := []struct {
		name             string
		args             []string
		defined          []string
		want             []string
		wantActive       int
		wantUnrecognized int
	}{
		{
			name:             "no overrides",
			args:             []string{"app", "--foo=bar", "--baz", "qux"},
			defined:          []string{"foo", "baz"},
			want:             []string{"app", "--foo=bar", "--baz", "qux"},
			wantActive:       0,
			wantUnrecognized: 0,
		},
		{
			name:             "override defined flag with value",
			args:             []string{"app", "--foo=old", "--override_foo=new"},
			defined:          []string{"foo"},
			want:             []string{"app", "--foo=new"},
			wantActive:       1,
			wantUnrecognized: 0,
		},
		{
			name:             "override without value is ignored",
			args:             []string{"app", "--foo", "old", "--override_foo"},
			defined:          []string{"foo"},
			want:             []string{"app", "--foo", "old"},
			wantActive:       0,
			wantUnrecognized: 0,
		},
		{
			name:             "override defined flag with empty string",
			args:             []string{"app", "--foo=old", "--override_foo="},
			defined:          []string{"foo"},
			want:             []string{"app", "--foo="},
			wantActive:       1,
			wantUnrecognized: 0,
		},
		{
			name:             "drop undefined override flag",
			args:             []string{"app", "--foo=old", "--override_undef=new"},
			defined:          []string{"foo"},
			want:             []string{"app", "--foo=old"},
			wantActive:       0,
			wantUnrecognized: 1,
		},
		{
			name:             "multiple override sets",
			args:             []string{"app", "--foo=old", "--override_foo=new1", "--override_foo=new2"},
			defined:          []string{"foo"},
			want:             []string{"app", "--foo=new1", "--foo=new2"},
			wantActive:       2,
			wantUnrecognized: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defined := make(map[string]bool)
			for _, flag := range tc.defined {
				defined[flag] = true
			}
			actual := processFlagOverrides(tc.args, defined)
			if !reflect.DeepEqual(actual.args, tc.want) {
				t.Errorf("Want %v, got %v", tc.want, actual.args)
			}
			if actual.activeCount != tc.wantActive {
				t.Errorf("Want active count %d, got %d", tc.wantActive, actual.activeCount)
			}
			if actual.unrecognizedCount != tc.wantUnrecognized {
				t.Errorf("Want unrecognized count %d, got %d", tc.wantUnrecognized, actual.unrecognizedCount)
			}
		})
	}
}
