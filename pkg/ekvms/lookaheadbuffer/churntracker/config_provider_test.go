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
			p := NewConfigProvider(experiments.NewMockManagerWithOptions(version.Version{}, nil, stringFlags), "", caVersion, nil)

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
	p := NewConfigProvider(nil, "", version.Version{}, nil)
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
			p := NewConfigProvider(newMockManagerWithChurnFlag(tc.experimentFlag), tc.manifestConfig, caVersion, nil)

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

func TestProviderLaunchStatusMetric(t *testing.T) {
	caVersion, err := version.FromString("37.231.0")
	if err != nil {
		t.Fatal(err)
	}
	const (
		invalidFlag            = "this-is-invalid-json-string"
		disabledFlag           = `{"minCaVersion": "999.0.0", "churnThreshold": 3}`
		enabledFlag            = `{"minCaVersion": "37.225.0", "churnThreshold": 3}`
		manifestConfig         = `{"minCaVersion": "37.225.0", "churnThreshold": 7}`
		disabledManifestConfig = `{"minCaVersion": "999.0.0", "churnThreshold": 7}`
	)

	// The launch status is emitted on construction (oldFlagValue) and on Refresh() (newFlagValue). The manifest config
	// is emitted instead of the experiment flag value if it is set and valid. enabled reports whether the config in use is
	// enabled: if the flag value cannot be parsed, the previous config is kept.
	for _, tc := range []struct {
		desc              string
		manifestConfig    string
		oldFlagValue      string
		newFlagValue      string
		wantEmittedValues []launchStatus
	}{
		{
			desc:              "flag not set (failsafe)",
			wantEmittedValues: []launchStatus{{false, experimentSource, failsafeFlagValue}, {false, experimentSource, failsafeFlagValue}},
		},
		{
			desc:              "invalid flag is still emitted",
			oldFlagValue:      invalidFlag,
			newFlagValue:      invalidFlag,
			wantEmittedValues: []launchStatus{{false, experimentSource, invalidFlag}, {false, experimentSource, invalidFlag}},
		},
		{
			desc:              "experiment disabled for ca version, then invalid flag",
			oldFlagValue:      disabledFlag,
			newFlagValue:      invalidFlag,
			wantEmittedValues: []launchStatus{{false, experimentSource, disabledFlag}, {false, experimentSource, invalidFlag}},
		},
		{
			desc:              "experiment enabled, then invalid flag",
			oldFlagValue:      enabledFlag,
			newFlagValue:      invalidFlag,
			wantEmittedValues: []launchStatus{{true, experimentSource, enabledFlag}, {true, experimentSource, invalidFlag}},
		},
		{
			desc:              "invalid flag, then experiment enabled",
			oldFlagValue:      invalidFlag,
			newFlagValue:      enabledFlag,
			wantEmittedValues: []launchStatus{{false, experimentSource, invalidFlag}, {true, experimentSource, enabledFlag}},
		},
		{
			desc:              "experiment enabled for ca version, then disabled",
			oldFlagValue:      enabledFlag,
			newFlagValue:      disabledFlag,
			wantEmittedValues: []launchStatus{{true, experimentSource, enabledFlag}, {false, experimentSource, disabledFlag}},
		},
		{
			desc:              "experiment disabled for ca version, then enabled",
			oldFlagValue:      disabledFlag,
			newFlagValue:      enabledFlag,
			wantEmittedValues: []launchStatus{{false, experimentSource, disabledFlag}, {true, experimentSource, enabledFlag}},
		},
		{
			desc:              "manifest config is emitted without the single quotes instead of the experiment flag value",
			manifestConfig:    "'" + manifestConfig + "'",
			oldFlagValue:      enabledFlag,
			newFlagValue:      disabledFlag,
			wantEmittedValues: []launchStatus{{true, clusterProtoSource, manifestConfig}, {true, clusterProtoSource, manifestConfig}},
		},
		{
			desc:              "disabled manifest config is emitted as disabled instead of the experiment flag value",
			manifestConfig:    "'" + disabledManifestConfig + "'",
			oldFlagValue:      enabledFlag,
			newFlagValue:      enabledFlag,
			wantEmittedValues: []launchStatus{{false, clusterProtoSource, disabledManifestConfig}, {false, clusterProtoSource, disabledManifestConfig}},
		},
		{
			desc:              "invalid manifest config is not emitted, the experiment flag value is emitted instead",
			manifestConfig:    "'" + invalidFlag + "'",
			oldFlagValue:      enabledFlag,
			newFlagValue:      disabledFlag,
			wantEmittedValues: []launchStatus{{true, experimentSource, enabledFlag}, {false, experimentSource, disabledFlag}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			metrics := &fakeProviderMetrics{}
			p := NewConfigProvider(newMockManagerWithChurnFlag(tc.oldFlagValue), tc.manifestConfig, caVersion, metrics)

			p.(*configProviderImpl).experimentsManager = newMockManagerWithChurnFlag(tc.newFlagValue)
			p.Refresh()

			if diff := cmp.Diff(tc.wantEmittedValues, metrics.statuses, cmp.AllowUnexported(launchStatus{})); diff != "" {
				t.Errorf("UpdateLookaheadChurnBreakerLaunchStatus() calls mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// launchStatus is a recorded UpdateLookaheadChurnBreakerLaunchStatus call.
type launchStatus struct {
	enabled      bool
	launchedFrom launchSource
	config       string
}

type fakeProviderMetrics struct {
	statuses []launchStatus
}

func (f *fakeProviderMetrics) UpdateLookaheadChurnBreakerLaunchStatus(enabled bool, launchedFrom, config string) {
	f.statuses = append(f.statuses, launchStatus{enabled: enabled, launchedFrom: launchSource(launchedFrom), config: config})
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
