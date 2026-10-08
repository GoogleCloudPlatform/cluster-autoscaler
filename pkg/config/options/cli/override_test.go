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
	"slices"
	"testing"
)

func TestProcessFlagOverrides(t *testing.T) {
	testCases := []struct {
		name             string
		args             []string
		defined          map[string]string
		want             []string
		wantActive       int
		wantUnrecognized int
		wantRedundant    int
		wantErr          bool
	}{
		{
			name:    "no overrides",
			args:    []string{"app", "--foo=bar", "--baz=qux"},
			defined: map[string]string{"foo": "", "baz": ""},
			want:    []string{"app", "--foo=bar", "--baz=qux"},
		},
		{
			name:       "override defined flag with value",
			args:       []string{"app", "--foo=old", "--override_foo=new"},
			defined:    map[string]string{"foo": "def"},
			want:       []string{"app", "--foo=new"},
			wantActive: 1,
		},
		{
			name:       "override defined flag with negative number",
			args:       []string{"app", "--retry-count=-3", "--override_retry-count=-5"},
			defined:    map[string]string{"retry-count": "0"},
			want:       []string{"app", "--retry-count=-5"},
			wantActive: 1,
		},
		{
			name:       "override defined flag with empty string",
			args:       []string{"app", "--foo=old", "--override_foo="},
			defined:    map[string]string{"foo": ""},
			want:       []string{"app", "--foo="},
			wantActive: 1,
		},
		{
			name:             "drop undefined override flag",
			args:             []string{"app", "--foo=old", "--override_undef=new"},
			defined:          map[string]string{"foo": ""},
			want:             []string{"app", "--foo=old"},
			wantUnrecognized: 1,
		},
		{
			name:             "unrecognized override does not drop undefined cli flag",
			args:             []string{"app", "--foo=old", "--undef=old", "--override_undef=new"},
			defined:          map[string]string{"foo": ""},
			want:             []string{"app", "--foo=old", "--undef=old"},
			wantUnrecognized: 1,
		},
		{
			name:       "multiple override sets",
			args:       []string{"app", "--foo=old", "--override_foo=new1", "--override_foo=new2"},
			defined:    map[string]string{"foo": ""},
			want:       []string{"app", "--foo=new1", "--foo=new2"},
			wantActive: 2,
		},
		{
			name:          "redundant override from cli",
			args:          []string{"app", "--foo=val", "--override_foo=val"},
			defined:       map[string]string{"foo": "def"},
			want:          []string{"app", "--foo=val"},
			wantRedundant: 1,
		},
		{
			name:          "redundant override from default",
			args:          []string{"app", "--override_foo=def"},
			defined:       map[string]string{"foo": "def"},
			want:          []string{"app"},
			wantRedundant: 1,
		},
		{
			name:          "redundant override with multiple values",
			args:          []string{"app", "--foo=val1", "--foo=val2", "--override_foo=val1", "--override_foo=val2"},
			defined:       map[string]string{"foo": "def"},
			want:          []string{"app", "--foo=val1", "--foo=val2"},
			wantRedundant: 2,
		},
		{
			name:       "mismatched length: fewer cli flags than overrides",
			args:       []string{"app", "--foo=val1", "--override_foo=val1", "--override_foo=val2"},
			defined:    map[string]string{"foo": "def"},
			want:       []string{"app", "--foo=val1", "--foo=val2"},
			wantActive: 2,
		},
		{
			name:       "mismatched length: more cli flags than overrides",
			args:       []string{"app", "--foo=val1", "--foo=val2", "--override_foo=val1"},
			defined:    map[string]string{"foo": "def"},
			want:       []string{"app", "--foo=val1"},
			wantActive: 1,
		},
		{
			name:          "redundant override with empty default value",
			args:          []string{"app", "--override_foo="},
			defined:       map[string]string{"foo": ""},
			want:          []string{"app"},
			wantRedundant: 1,
		},
		{
			name:          "redundant override with negative default value",
			args:          []string{"app", "--override_cutoff=-10"},
			defined:       map[string]string{"cutoff": "-10"},
			want:          []string{"app"},
			wantRedundant: 1,
		},
		{
			name:       "override flag with quoted value",
			args:       []string{"app", "--foo='old'", "--override_foo=\"new\""},
			defined:    map[string]string{"foo": ""},
			want:       []string{"app", "--foo=\"new\""},
			wantActive: 1,
		},
		{
			name:       "override flag with value containing equals",
			args:       []string{"app", "--foo=k1=v1", "--override_foo=k2=v2"},
			defined:    map[string]string{"foo": ""},
			want:       []string{"app", "--foo=k2=v2"},
			wantActive: 1,
		},
		{
			name:       "override single dash flag",
			args:       []string{"app", "-v=2", "--override_v=4"},
			defined:    map[string]string{"v": "0"},
			want:       []string{"app", "--v=4"},
			wantActive: 1,
		},
		{
			name:          "redundant single dash flag",
			args:          []string{"app", "-v=4", "-override_v=4"},
			defined:       map[string]string{"v": "0"},
			want:          []string{"app", "-v=4"},
			wantRedundant: 1,
		},
		{
			name:    "bare boolean flag without overrides",
			args:    []string{"app", "--bool-flag", "-h"},
			defined: map[string]string{"bool-flag": "false"},
			want:    []string{"app", "--bool-flag", "-h"},
		},
		{
			name:       "override bare boolean flag",
			args:       []string{"app", "--bool-flag", "--override_bool-flag=false"},
			defined:    map[string]string{"bool-flag": "false"},
			want:       []string{"app", "--bool-flag=false"},
			wantActive: 1,
		},
		{
			name:          "redundant override on bare boolean flag",
			args:          []string{"app", "--bool-flag", "--override_bool-flag=true"},
			defined:       map[string]string{"bool-flag": "false"},
			want:          []string{"app", "--bool-flag"},
			wantRedundant: 1,
		},
		{
			name:    "error on override without value",
			args:    []string{"app", "--foo=old", "--override_foo"},
			defined: map[string]string{"foo": ""},
			wantErr: true,
		},
		{
			name:    "error on empty override flag name",
			args:    []string{"app", "--override_=val"},
			defined: map[string]string{"foo": ""},
			wantErr: true,
		},
		{
			name:    "error on empty arg",
			args:    []string{"app", ""},
			defined: map[string]string{"foo": ""},
			wantErr: true,
		},
		{
			name:    "error on space separated base flag",
			args:    []string{"app", "--foo", "old", "--override_foo=new"},
			defined: map[string]string{"foo": "def"},
			wantErr: true,
		},
		{
			name:    "error on space separated negative number",
			args:    []string{"app", "--retry-count", "-3"},
			defined: map[string]string{"retry-count": "0"},
			wantErr: true,
		},
		{
			name:    "error on space separated override",
			args:    []string{"app", "--foo=old", "--override_foo", "new"},
			defined: map[string]string{"foo": "def"},
			wantErr: true,
		},
		{
			name:    "error on triple dash flag",
			args:    []string{"app", "---foo=bar"},
			defined: map[string]string{"foo": ""},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := mergeFlagOverrides(tc.args, tc.defined)
			if (err != nil) != tc.wantErr {
				t.Fatalf("mergeFlagOverrides() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if !slices.Equal(actual.args, tc.want) {
				t.Errorf("Want %v, got %v", tc.want, actual.args)
			}
			if actual.activeCount != tc.wantActive {
				t.Errorf("Want active count %d, got %d", tc.wantActive, actual.activeCount)
			}
			if actual.unrecognizedCount != tc.wantUnrecognized {
				t.Errorf("Want unrecognized count %d, got %d", tc.wantUnrecognized, actual.unrecognizedCount)
			}
			if actual.redundantCount != tc.wantRedundant {
				t.Errorf("Want redundant count %d, got %d", tc.wantRedundant, actual.redundantCount)
			}
		})
	}
}
