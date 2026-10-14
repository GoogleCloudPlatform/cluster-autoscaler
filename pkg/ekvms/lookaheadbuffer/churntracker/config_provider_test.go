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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestConfigProvider(t *testing.T) {
	caVersion, err := version.FromString("37.231.0")
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		desc                       string
		flagValue                  string
		wantEnabled                bool
		wantChurnLifetimeThreshold time.Duration
		wantChurnWindow            time.Duration
		wantChurnThreshold         int
		wantBackoffDurations       []time.Duration
		wantResetWindow            time.Duration
		wantResetMaxChurnEvents    int
	}{
		{
			desc:                       "flag not set (failsafe)",
			wantEnabled:                false,
			wantChurnLifetimeThreshold: defaultChurnLifetimeThreshold,
			wantChurnWindow:            defaultChurnWindow,
			wantChurnThreshold:         defaultChurnThreshold,
			wantBackoffDurations:       defaultBackoffDurations,
			wantResetWindow:            defaultResetWindow,
			wantResetMaxChurnEvents:    defaultResetMaxChurnEvents,
		},
		{
			desc:                       "invalid flag",
			flagValue:                  "this-is-invalid-json-string",
			wantEnabled:                false,
			wantChurnLifetimeThreshold: defaultChurnLifetimeThreshold,
			wantChurnWindow:            defaultChurnWindow,
			wantChurnThreshold:         defaultChurnThreshold,
			wantBackoffDurations:       defaultBackoffDurations,
			wantResetWindow:            defaultResetWindow,
			wantResetMaxChurnEvents:    defaultResetMaxChurnEvents,
		},
		{
			desc:                       "experiment disabled for ca version",
			flagValue:                  `{"minCaVersion": "999.0.0", "churnThreshold": 3}`,
			wantEnabled:                false,
			wantChurnLifetimeThreshold: defaultChurnLifetimeThreshold,
			wantChurnWindow:            defaultChurnWindow,
			wantChurnThreshold:         defaultChurnThreshold,
			wantBackoffDurations:       defaultBackoffDurations,
			wantResetWindow:            defaultResetWindow,
			wantResetMaxChurnEvents:    defaultResetMaxChurnEvents,
		},
		{
			desc:                       "experiment enabled",
			flagValue:                  `{"minCaVersion": "37.225.0", "churnLifetimeThreshold": 600, "churnWindow": 3600, "churnThreshold": 3, "backoffDurations": [600, 1800, 3600], "resetWindow": 2700, "resetMaxChurnEvents": 1}`,
			wantEnabled:                true,
			wantChurnLifetimeThreshold: 10 * time.Minute,
			wantChurnWindow:            time.Hour,
			wantChurnThreshold:         3,
			wantBackoffDurations:       []time.Duration{10 * time.Minute, 30 * time.Minute, time.Hour},
			wantResetWindow:            45 * time.Minute,
			wantResetMaxChurnEvents:    1,
		},
		{
			desc:                       "experiment enabled - with different order of fields",
			flagValue:                  `{"minCaVersion": "37.225.0", "backoffDurations": [600, 1800, 3600], "churnWindow": 3600, "churnThreshold": 3, "resetWindow": 2700, "resetMaxChurnEvents": 1, "churnLifetimeThreshold": 600}`,
			wantEnabled:                true,
			wantChurnLifetimeThreshold: 10 * time.Minute,
			wantChurnWindow:            time.Hour,
			wantChurnThreshold:         3,
			wantBackoffDurations:       []time.Duration{10 * time.Minute, 30 * time.Minute, time.Hour},
			wantResetWindow:            45 * time.Minute,
			wantResetMaxChurnEvents:    1,
		},
		{
			desc:                       "experiment enabled - backoffDurations are incorrectly setup, so they fallback to default values",
			flagValue:                  `{"minCaVersion": "37.225.0", "backoffDurations": [600, -1800, 3600], "churnWindow": 3600, "churnThreshold": 3, "resetWindow": 2700, "resetMaxChurnEvents": 1, "churnLifetimeThreshold": 600}`,
			wantEnabled:                true,
			wantChurnLifetimeThreshold: 10 * time.Minute,
			wantChurnWindow:            time.Hour,
			wantChurnThreshold:         3,
			wantBackoffDurations:       []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour, 8 * time.Hour},
			wantResetWindow:            45 * time.Minute,
			wantResetMaxChurnEvents:    1,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stringFlags := map[string]string{}
			if tc.flagValue != "" {
				stringFlags[experiments.ResizableVmChurnCircuitBreakerFlag] = tc.flagValue
			}
			p := NewConfigProvider(experiments.NewMockManagerWithOptions(version.Version{}, nil, stringFlags), "", caVersion)

			if got := p.IsEnabled(); got != tc.wantEnabled {
				t.Errorf("IsEnabled() = %v, want %v", got, tc.wantEnabled)
			}
			if got := p.ChurnLifetimeThreshold(); got != tc.wantChurnLifetimeThreshold {
				t.Errorf("ChurnLifetimeThreshold() = %v, want %v", got, tc.wantChurnLifetimeThreshold)
			}
			if got := p.ChurnWindow(); got != tc.wantChurnWindow {
				t.Errorf("ChurnWindow() = %v, want %v", got, tc.wantChurnWindow)
			}
			if got := p.ChurnThreshold(); got != tc.wantChurnThreshold {
				t.Errorf("ChurnThreshold() = %v, want %v", got, tc.wantChurnThreshold)
			}
			if diff := cmp.Diff(tc.wantBackoffDurations, p.BackoffDurations()); diff != "" {
				t.Errorf("BackoffDurations() mismatch (-want +got):\n%s", diff)
			}
			if got := p.ResetWindow(); got != tc.wantResetWindow {
				t.Errorf("ResetWindow() = %v, want %v", got, tc.wantResetWindow)
			}
			if got := p.ResetMaxChurnEvents(); got != tc.wantResetMaxChurnEvents {
				t.Errorf("ResetMaxChurnEvents() = %v, want %v", got, tc.wantResetMaxChurnEvents)
			}
		})
	}
}

func TestConfigProviderNilExperimentsManager(t *testing.T) {
	p := NewConfigProvider(nil, "", version.Version{})
	if p.IsEnabled() {
		t.Errorf("IsEnabled() = true, want false")
	}
	if got := p.ChurnWindow(); got != defaultChurnWindow {
		t.Errorf("ChurnWindow() = %v, want %v", got, defaultChurnWindow)
	}
}

func TestProviderProtoConfig(t *testing.T) {
	caVersion, err := version.FromString("37.231.0")
	if err != nil {
		t.Fatal(err)
	}
	const (
		experimentFlag        = `{"minCaVersion": "37.225.0", "churnThreshold": 3}`
		updatedExperimentFlag = `{"minCaVersion": "37.225.0", "churnThreshold": 9}`
	)

	for _, tc := range []struct {
		desc               string
		manifestConfig     string
		experimentFlag     string
		wantFromProto      bool
		wantEnabled        bool
		wantChurnThreshold int
	}{
		{
			desc:               "manifest config not set, experiment is used",
			experimentFlag:     experimentFlag,
			wantEnabled:        true,
			wantChurnThreshold: 3,
		},
		{
			desc:               "empty manifest config, experiment is used",
			manifestConfig:     "'{}'",
			experimentFlag:     experimentFlag,
			wantEnabled:        true,
			wantChurnThreshold: 3,
		},
		{
			desc:               "invalid manifest config, experiment is used",
			manifestConfig:     "'this-is-invalid-json-string'",
			experimentFlag:     experimentFlag,
			wantEnabled:        true,
			wantChurnThreshold: 3,
		},
		{
			desc:               "manifest config takes precedence over the experiment",
			manifestConfig:     `'{"minCaVersion": "37.225.0", "churnThreshold": 7}'`,
			experimentFlag:     experimentFlag,
			wantFromProto:      true,
			wantEnabled:        true,
			wantChurnThreshold: 7,
		},
		{
			desc:               "whitespace-only manifest config is treated as not set, experiment is used",
			manifestConfig:     "'   '",
			experimentFlag:     experimentFlag,
			wantEnabled:        true,
			wantChurnThreshold: 3,
		},
		{
			desc:               "empty manifest config surrounded by whitespace is treated as not set, experiment is used",
			manifestConfig:     "' {} '",
			experimentFlag:     experimentFlag,
			wantEnabled:        true,
			wantChurnThreshold: 3,
		},
		{
			desc:               "manifest config surrounded by whitespace is trimmed and takes precedence over the experiment",
			manifestConfig:     "' \t{\"minCaVersion\": \"37.225.0\", \"churnThreshold\": 7}\n '",
			experimentFlag:     experimentFlag,
			wantFromProto:      true,
			wantEnabled:        true,
			wantChurnThreshold: 7,
		},
		{
			desc:               "manifest config without minCaVersion is disabled, the experiment is not used",
			manifestConfig:     `'{"churnThreshold": 7}'`,
			experimentFlag:     experimentFlag,
			wantFromProto:      true,
			wantEnabled:        false,
			wantChurnThreshold: defaultChurnThreshold,
		},
		{
			desc:               "manifest config with minCaVersion higher than the CA version is disabled",
			manifestConfig:     `'{"minCaVersion": "999.0.0", "churnThreshold": 7}'`,
			experimentFlag:     experimentFlag,
			wantFromProto:      true,
			wantEnabled:        false,
			wantChurnThreshold: defaultChurnThreshold,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			p := NewConfigProvider(newMockManagerWithChurnFlag(tc.experimentFlag), tc.manifestConfig, caVersion)

			if got := p.(*configProviderImpl).fromProto; got != tc.wantFromProto {
				t.Errorf("fromProto = %v, want %v", got, tc.wantFromProto)
			}
			if got := p.IsEnabled(); got != tc.wantEnabled {
				t.Errorf("IsEnabled() = %v, want %v", got, tc.wantEnabled)
			}
			if got := p.ChurnThreshold(); got != tc.wantChurnThreshold {
				t.Errorf("ChurnThreshold() = %v, want %v", got, tc.wantChurnThreshold)
			}

			// The manifest config is static, while the experiment config follows the experiment flag changes.
			p.(*configProviderImpl).experimentsManager = newMockManagerWithChurnFlag(updatedExperimentFlag)
			p.Refresh()
			wantChurnThresholdAfterRefresh := 9
			if tc.wantFromProto {
				wantChurnThresholdAfterRefresh = tc.wantChurnThreshold
			}
			if got := p.ChurnThreshold(); got != wantChurnThresholdAfterRefresh {
				t.Errorf("ChurnThreshold() after Refresh() = %v, want %v", got, wantChurnThresholdAfterRefresh)
			}
		})
	}
}

// newMockManagerWithChurnFlag returns a mock experiments manager with the churn circuit breaker flag set to flagValue.
// The flag is not set if flagValue is empty.
func newMockManagerWithChurnFlag(flagValue string) experiments.Manager {
	stringFlags := map[string]string{}
	if flagValue != "" {
		stringFlags[experiments.ResizableVmChurnCircuitBreakerFlag] = flagValue
	}
	return experiments.NewMockManagerWithOptions(version.Version{}, nil, stringFlags)
}
