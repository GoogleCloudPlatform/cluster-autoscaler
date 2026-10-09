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
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// Default values used when the experiment flag does not set a value.
const (
	defaultChurnLifetimeThreshold = 5 * time.Minute
	defaultChurnWindow            = 30 * time.Minute
	defaultChurnThreshold         = 5
	defaultResetWindow            = 30 * time.Minute
	defaultResetMaxChurnEvents    = 0
)

// defaultBackoffDurations are the durations of the consecutive lookahead backoffs of a workload separation.
// Decided to define it as an array to have more flexibility over the boundaries of the backoff time (minimum, maximum and rate).
var defaultBackoffDurations = []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour, 8 * time.Hour}

// ChurnCircuitBreakerFlag refers to the AutopilotResizableVM::LookaheadChurnCircuitBreaker experiment flag parsing.
// The experiment is enabled if the CA version is at least MinCaVersion. All durations are in seconds.
type ChurnCircuitBreakerFlag struct {
	MinCaVersion string `json:"minCaVersion,omitempty"`
	// ChurnLifetimeThreshold is the maximum time between the churn tracker first seeing a node and its removal
	// for the removal to be considered a lookahead churn event.
	ChurnLifetimeThreshold int `json:"churnLifetimeThreshold"`
	// ChurnWindow is the period of time for which churn events are kept.
	ChurnWindow int `json:"churnWindow"`
	// ChurnThreshold is the number of churn events of a workload separation within ChurnWindow that triggers the backoff.
	ChurnThreshold int `json:"churnThreshold"`
	// BackoffDurations are the durations of the consecutive lookahead backoffs of a workload separation,
	// e.g. [900, 1800, 3600, 7200]. The last duration is used for all the following backoffs.
	BackoffDurations []int `json:"backoffDurations"`
	// ResetWindow is the period of time used to decide whether the backoff of a workload separation can be reset.
	ResetWindow int `json:"resetWindow"`
	// ResetMaxChurnEvents is the maximum number of churn events within ResetWindow for the backoff to be reset.
	ResetMaxChurnEvents int `json:"resetMaxChurnEvents"`
}

// Config is the churn tracker configuration derived from the experiment flag.
type Config struct {
	enabled                bool
	churnLifetimeThreshold time.Duration
	churnWindow            time.Duration
	churnThreshold         int
	backoffDurations       []time.Duration
	resetWindow            time.Duration
	resetMaxChurnEvents    int
}

// defaultConfig returns a disabled Config with default values.
func defaultConfig() *Config {
	return &Config{
		enabled:                false,
		churnLifetimeThreshold: defaultChurnLifetimeThreshold,
		churnWindow:            defaultChurnWindow,
		churnThreshold:         defaultChurnThreshold,
		backoffDurations:       slices.Clone(defaultBackoffDurations),
		resetWindow:            defaultResetWindow,
		resetMaxChurnEvents:    defaultResetMaxChurnEvents,
	}
}

// NewConfig creates a new Config from the experiment flag. Values that are not set (or not positive) fall back to defaults.
// If the experiment is disabled or misconfigured, a disabled Config with default values is returned.
func NewConfig(flag ChurnCircuitBreakerFlag, caVersion version.Version) *Config {
	config := defaultConfig()
	experimentVersion, err := version.FromString(flag.MinCaVersion)
	if err != nil {
		klog.Warningf("Experiment %q provided invalid min version %q, ChurnCircuitBreaker is disabled", experiments.ResizableVmChurnCircuitBreakerFlag, flag.MinCaVersion)
		return config
	}
	if caVersion.LessThan(experimentVersion) {
		klog.V(4).Infof("Experiment %q provided min version %q, ChurnCircuitBreaker is disabled", experiments.ResizableVmChurnCircuitBreakerFlag, flag.MinCaVersion)
		return config
	}
	config.enabled = true
	if flag.ChurnLifetimeThreshold > 0 {
		config.churnLifetimeThreshold = time.Duration(flag.ChurnLifetimeThreshold) * time.Second
	}
	if flag.ChurnWindow > 0 {
		config.churnWindow = time.Duration(flag.ChurnWindow) * time.Second
	}
	if flag.ChurnThreshold > 0 {
		config.churnThreshold = flag.ChurnThreshold
	}
	if backoffDurations, ok := toDurations(flag.BackoffDurations); ok {
		config.backoffDurations = backoffDurations
	}
	if flag.ResetWindow > 0 {
		config.resetWindow = time.Duration(flag.ResetWindow) * time.Second
	}
	if flag.ResetMaxChurnEvents > 0 {
		config.resetMaxChurnEvents = flag.ResetMaxChurnEvents
	}
	return config
}

// toDurations converts durations in seconds to time.Duration. It returns false if the list is empty or any value is not positive.
func toDurations(seconds []int) ([]time.Duration, bool) {
	if len(seconds) == 0 {
		return nil, false
	}
	durations := make([]time.Duration, 0, len(seconds))
	for _, s := range seconds {
		if s <= 0 {
			return nil, false
		}
		durations = append(durations, time.Duration(s)*time.Second)
	}
	return durations, true
}

// parseChurnCircuitBreakerFlag reads JSON configuration from a string and returns the populated ChurnCircuitBreakerFlag instance.
func parseChurnCircuitBreakerFlag(s string) (ChurnCircuitBreakerFlag, error) {
	flag := ChurnCircuitBreakerFlag{}

	// When the experiment is enabled but filtered out by some criteria, an empty string is passed,
	// which can cause parsing errors.
	if len(s) == 0 {
		s = "{}"
	}

	if err := json.Unmarshal([]byte(s), &flag); err != nil {
		return flag, fmt.Errorf("parsing ChurnCircuitBreakerFlag from a JSON string failed: %v", err)
	}

	return flag, nil
}
