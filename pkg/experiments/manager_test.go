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

package experiments

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
)

type fakeEvaluator struct {
	stringFlags map[string]string
	boolFlags   map[string]bool
}

func (f *fakeEvaluator) UpdateReleaseChannel(_ string) {}

func (f *fakeEvaluator) EvaluateStringFlagOrFailsafe(flag, failsafe string) string {
	if val, ok := f.stringFlags[flag]; ok {
		return val
	}
	return failsafe
}

func (f *fakeEvaluator) EvaluateBoolFlagOrFailsafe(flag string, failsafe bool) bool {
	if val, ok := f.boolFlags[flag]; ok {
		return val
	}
	return failsafe
}

func (f *fakeEvaluator) DirectLaunchBoolFlag(flag string) bool {
	if val, ok := f.boolFlags[flag]; ok && !val {
		return false
	}
	return true
}

func (f *fakeEvaluator) SubscribeToUpdate(subscriber Subscriber) {
}

func TestManagerVersionEvaluation(t *testing.T) {
	testCases := []struct {
		desc             string
		componentVersion string
		flags            map[string]string
		flagToTest       string
		failsafe         bool
		wantEnabled      bool
	}{
		{
			desc:             "fails with too early version",
			componentVersion: "27.100.0",
			flags:            map[string]string{"somefeature": "27.110.0"},
			flagToTest:       "somefeature",
			wantEnabled:      false,
		},
		{
			desc:             "check passes with the same version",
			componentVersion: "1.2.3",
			flags:            map[string]string{"somefeature": "1.2.3"},
			flagToTest:       "somefeature",
			wantEnabled:      true,
		},
		{
			desc:             "correct version comparison",
			componentVersion: "1.2.20",
			flags:            map[string]string{"somefeature": "1.2.3"},
			flagToTest:       "somefeature",
			wantEnabled:      true,
		},
		{
			desc:             "check passes with later version",
			componentVersion: "3.2.1",
			flags:            map[string]string{"somefeature": "1.2.3"},
			flagToTest:       "somefeature",
			wantEnabled:      true,
		},
		{
			desc:             "flag not found, fail-open",
			componentVersion: "1.2.3",
			flagToTest:       "nonexistentflag",
			failsafe:         true,
			wantEnabled:      true,
		},
		{
			desc:             "flag not found, fail-close",
			componentVersion: "1.2.3",
			flagToTest:       "nonexistentflag",
			wantEnabled:      false,
		},
		{
			desc:             "flag enabled for current CA major",
			componentVersion: "28.122.0",
			flags:            map[string]string{"somefeatureFor28": "28.122.0"},
			flagToTest:       "somefeature",
			wantEnabled:      true,
		},
		{
			desc:             "flag disabled for current CA major",
			componentVersion: "28.122.0",
			flags:            map[string]string{"somefeatureFor28": "28.123.0"},
			flagToTest:       "somefeature",
			wantEnabled:      false,
		},
		{
			desc:             "flag enabled, but disabled for current CA major",
			componentVersion: "28.122.0",
			flags: map[string]string{
				"somefeature":      "28.122.0",
				"somefeatureFor28": "28.123.0",
			},
			flagToTest:  "somefeature",
			wantEnabled: false,
		},
		{
			desc:             "flag disabled, but enabled for current CA major",
			componentVersion: "28.122.0",
			flags: map[string]string{
				"somefeature":      "29.130.0",
				"somefeatureFor28": "28.122.0",
			},
			flagToTest:  "somefeature",
			wantEnabled: true,
		},
		{
			desc:             "flag enabled only for a different CA major",
			componentVersion: "28.122.0",
			flags: map[string]string{
				"somefeatureFor27": "27.110.0",
			},
			flagToTest:  "somefeature",
			wantEnabled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			evaluator := &fakeEvaluator{
				stringFlags: tc.flags,
			}
			v, err := version.FromString(tc.componentVersion)
			if err != nil {
				t.Fatalf("component version %s is not a correct version: %v", tc.componentVersion, err)
			}
			manager := NewManager(v, evaluator)
			enabled := manager.EvaluateMinimumVersionFlagOrFailsafe(tc.flagToTest, tc.failsafe)
			if enabled != tc.wantEnabled {
				t.Errorf("unexpected flag evaluation for %v, got %v, want %v", tc.flagToTest, enabled, tc.wantEnabled)
			}
		})
	}
}

func TestManagerDurationEvaluation(t *testing.T) {
	testCases := []struct {
		desc     string
		flags    map[string]string
		flag     string
		fallback time.Duration
		expected time.Duration
	}{
		{
			desc: "happy path",
			flags: map[string]string{
				"myflag": "1234",
			},
			flag:     "myflag",
			fallback: time.Duration(5678) * time.Second,
			expected: time.Duration(1234) * time.Second,
		},
		{
			desc: "negative values are ok",
			flags: map[string]string{
				"myflag": "-1234",
			},
			flag:     "myflag",
			fallback: time.Duration(5678) * time.Second,
			expected: time.Duration(-1234) * time.Second,
		},
		{
			desc:     "flag missing",
			flag:     "myflag",
			fallback: time.Duration(5678) * time.Second,
			expected: time.Duration(5678) * time.Second,
		},
		{
			desc: "flag has an unparsable value",
			flags: map[string]string{
				"myflag": "28.123.0",
			},
			flag:     "myflag",
			fallback: time.Duration(5678) * time.Second,
			expected: time.Duration(5678) * time.Second,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			evaluator := &fakeEvaluator{
				stringFlags: tc.flags,
			}
			manager := NewManager(version.Version{}, evaluator)
			assert.Equal(t, tc.expected, manager.EvaluateDurationSecondsFlagOrFailsafe(tc.flag, tc.fallback))
		})
	}
}

func TestManagerIntEvaluation(t *testing.T) {
	testCases := []struct {
		desc     string
		flags    map[string]string
		flag     string
		fallback int
		expected int
	}{
		{
			desc: "happy path",
			flags: map[string]string{
				"myflag": "1234",
			},
			flag:     "myflag",
			fallback: 5678,
			expected: 1234,
		},
		{
			desc: "negative values are ok",
			flags: map[string]string{
				"myflag": "-1234",
			},
			flag:     "myflag",
			fallback: 5678,
			expected: -1234,
		},
		{
			desc:     "flag missing",
			flag:     "myflag",
			fallback: 5678,
			expected: 5678,
		},
		{
			desc: "flag has an unparsable value",
			flags: map[string]string{
				"myflag": "28.123.0",
			},
			flag:     "myflag",
			fallback: 5678,
			expected: 5678,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			evaluator := &fakeEvaluator{
				stringFlags: tc.flags,
			}
			manager := NewManager(version.Version{}, evaluator)
			assert.Equal(t, tc.expected, manager.EvaluateIntFlagOrFailsafe(tc.flag, tc.fallback))
		})
	}
}

func TestManagerDirectLaunchBoolFlag(t *testing.T) {
	flag := "SOME_FLAG"
	testCases := []struct {
		desc  string
		flags map[string]bool
		want  bool
	}{
		{
			desc: "true_without_flag_config",
			want: true,
		},
		{
			desc: "true_when_explicitly_enabled",
			flags: map[string]bool{
				flag: true,
			},
			want: true,
		},
		{
			desc: "true_when_another_flag_enabled",
			flags: map[string]bool{
				"OTHER_FLAG": true,
			},
			want: true,
		},
		{
			desc: "true_when_another_flag_disabled",
			flags: map[string]bool{
				"OTHER_FLAG": false,
			},
			want: true,
		},
		{
			desc: "false_when_explicitly_disabled",
			flags: map[string]bool{
				flag: false,
			},
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			evaluator := &fakeEvaluator{
				boolFlags: tc.flags,
			}
			manager := NewManager(version.Version{}, evaluator)
			assert.Equal(t, tc.want, manager.DirectLaunchBoolFlag(flag))
		})
	}
}
