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
	"os"
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
			if !slices.Equal(actual.Args, tc.want) {
				t.Errorf("Want %v, got %v", tc.want, actual.Args)
			}
			if actual.Stats.ActiveCount != tc.wantActive {
				t.Errorf("Want active count %d, got %d", tc.wantActive, actual.Stats.ActiveCount)
			}
			if actual.Stats.UnrecognizedCount != tc.wantUnrecognized {
				t.Errorf("Want unrecognized count %d, got %d", tc.wantUnrecognized, actual.Stats.UnrecognizedCount)
			}
			if actual.Stats.RedundantCount != tc.wantRedundant {
				t.Errorf("Want redundant count %d, got %d", tc.wantRedundant, actual.Stats.RedundantCount)
			}
		})
	}
}

func TestProcessFlagOverridesGlobalArgs(t *testing.T) {
	origArgs := os.Args
	defer func() {
		os.Args = origArgs
	}()

	os.Args = []string{"app", "--override_unknown=val"}
	result := ProcessFlagOverrides()

	if result.Stats.UnrecognizedCount != 1 {
		t.Errorf("Want unrecognized count 1, got %d", result.Stats.UnrecognizedCount)
	}
	if result.HasFlagOverride("unknown") {
		t.Errorf("Want result.HasFlagOverride(\"unknown\") = false, got true")
	}
}

func TestHasFlagOverride(t *testing.T) {
	defined := map[string]string{
		"kube-client-qps":            "5",
		"max-scale-down-parallelism": "10",
		"untouched":                  "1",
	}
	args := []string{
		"app",
		"--kube-client-qps=200",
		"--override_kube-client-qps=150", // active override
		"--max-scale-down-parallelism=250",
		"--override_max-scale-down-parallelism=250", // redundant override (matches CLI)
		"--override_unrecognized=10",                // unrecognized override
	}

	result, err := mergeFlagOverrides(args, defined)
	if err != nil {
		t.Fatalf("mergeFlagOverrides() unexpected error: %v", err)
	}

	if !result.HasFlagOverride("kube-client-qps") {
		t.Errorf("Expected result.HasFlagOverride(\"kube-client-qps\") = true for active override")
	}
	if !result.HasFlagOverride("max-scale-down-parallelism") {
		t.Errorf("Expected result.HasFlagOverride(\"max-scale-down-parallelism\") = true for redundant override")
	}
	if result.HasFlagOverride("unrecognized") {
		t.Errorf("Expected result.HasFlagOverride(\"unrecognized\") = false for unrecognized override")
	}
	if result.HasFlagOverride("untouched") {
		t.Errorf("Expected result.HasFlagOverride(\"untouched\") = false for untouched flag")
	}
}
