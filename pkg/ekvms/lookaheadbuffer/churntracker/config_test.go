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

package churntracker

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
)

func TestParseChurnCircuitBreakerFlag(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		jsonStr  string
		wantFlag ChurnCircuitBreakerFlag
		wantErr  bool
	}{
		{
			desc:     "empty string",
			jsonStr:  "",
			wantFlag: ChurnCircuitBreakerFlag{},
		},
		{
			desc:     "empty json",
			jsonStr:  "{}",
			wantFlag: ChurnCircuitBreakerFlag{},
		},
		{
			desc:    "invalid string",
			jsonStr: "this-is-invalid-json-string",
			wantErr: true,
		},
		{
			desc: "all fields set",
			jsonStr: `{
  "minCaVersion": "35.197.0",
  "churnLifetimeThreshold": 600,
  "churnWindow": 3600,
  "churnThreshold": 3,
  "backoffDurations": [600, 1800, 3600],
  "resetWindow": 2700,
  "resetMaxChurnEvents": 1
}`,
			wantFlag: ChurnCircuitBreakerFlag{
				MinCaVersion:           "35.197.0",
				ChurnLifetimeThreshold: 600,
				ChurnWindow:            3600,
				ChurnThreshold:         3,
				BackoffDurations:       []int{600, 1800, 3600},
				ResetWindow:            2700,
				ResetMaxChurnEvents:    1,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := parseChurnCircuitBreakerFlag(tc.jsonStr)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("parseChurnCircuitBreakerFlag() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if diff := cmp.Diff(tc.wantFlag, got); diff != "" {
				t.Errorf("parseChurnCircuitBreakerFlag() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewConfig(t *testing.T) {
	caVersion, err := version.FromString("35.197.0")
	if err != nil {
		t.Fatal(err)
	}
	enabledDefaults := defaultConfig()
	enabledDefaults.enabled = true

	for _, tc := range []struct {
		desc       string
		flag       ChurnCircuitBreakerFlag
		wantConfig *Config
	}{
		{
			desc:       "min version not set",
			flag:       ChurnCircuitBreakerFlag{ChurnThreshold: 3},
			wantConfig: defaultConfig(),
		},
		{
			desc:       "invalid min version",
			flag:       ChurnCircuitBreakerFlag{MinCaVersion: "invalid", ChurnThreshold: 3},
			wantConfig: defaultConfig(),
		},
		{
			desc:       "ca version lower than min version",
			flag:       ChurnCircuitBreakerFlag{MinCaVersion: "999.999.999", ChurnThreshold: 3},
			wantConfig: defaultConfig(),
		},
		{
			desc:       "enabled without values uses defaults",
			flag:       ChurnCircuitBreakerFlag{MinCaVersion: "35.197.0"},
			wantConfig: enabledDefaults,
		},
		{
			desc:       "enabled with non-positive values uses defaults",
			flag:       ChurnCircuitBreakerFlag{MinCaVersion: "35.197.0", ChurnWindow: -1, ChurnThreshold: -1, ResetMaxChurnEvents: -1},
			wantConfig: enabledDefaults,
		},
		{
			desc:       "enabled with a non-positive backoff duration uses default backoff durations",
			flag:       ChurnCircuitBreakerFlag{MinCaVersion: "35.197.0", BackoffDurations: []int{900, 0, 3600}},
			wantConfig: enabledDefaults,
		},
		{
			desc: "enabled with all values",
			flag: ChurnCircuitBreakerFlag{
				MinCaVersion:           "35.197.0",
				ChurnLifetimeThreshold: 600,
				ChurnWindow:            3600,
				ChurnThreshold:         3,
				BackoffDurations:       []int{600, 1800, 3600},
				ResetWindow:            2700,
				ResetMaxChurnEvents:    1,
			},
			wantConfig: &Config{
				enabled:                true,
				churnLifetimeThreshold: 10 * time.Minute,
				churnWindow:            time.Hour,
				churnThreshold:         3,
				backoffDurations:       []time.Duration{10 * time.Minute, 30 * time.Minute, time.Hour},
				resetWindow:            45 * time.Minute,
				resetMaxChurnEvents:    1,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			got := NewConfig(tc.flag, caVersion)
			if diff := cmp.Diff(tc.wantConfig, got, cmp.AllowUnexported(Config{})); diff != "" {
				t.Errorf("NewConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
