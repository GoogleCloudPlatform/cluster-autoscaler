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
	"strconv"
	"strings"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/klog/v2"
)

// Manager is a convenience interface for checking flag values
// using common patterns, such as version comparison.
type Manager interface {
	EvaluateMinimumVersionFlagOrFailsafe(flag string, failsafe bool) bool
	EvaluateBoolFlagOrFailsafe(flag string, failsafe bool) bool
	EvaluateDurationSecondsFlagOrFailsafe(flag string, failsafe time.Duration) time.Duration
	EvaluateStringFlagOrFailsafe(flag, fallback string) string
	EvaluateIntFlagOrFailsafe(flag string, fallback int) int
	DirectLaunchBoolFlag(flag string) bool
	UpdateReleaseChannel(releaseChannel string)
}

type manager struct {
	componentVersion version.Version
	evaluator        Evaluator
}

// NewManager creates and returns an instance of Manager.
func NewManager(version version.Version, evaluator Evaluator) *manager {
	return &manager{
		componentVersion: version,
		evaluator:        evaluator,
	}
}

func (m *manager) UpdateReleaseChannel(releaseChannel string) {
	m.evaluator.UpdateReleaseChannel(releaseChannel)
}

// EvaluateBoolFlagOrFailsafe evaluates bool flag value.
func (m *manager) EvaluateBoolFlagOrFailsafe(flag string, failsafe bool) bool {
	return m.evaluator.EvaluateBoolFlagOrFailsafe(flag, failsafe)
}

// EvaluateMinimumVersionFlagOrFailsafe evaluates string flag value containing version
// and compares it to the actual version of Cluster Autoscaler.
// 'failsafe' parameter tells whether we want to pass the check when the flag isn't set
// by any experiment in the environment.
// Since this is a string flag, not bool, we use impossibly high component version as 'false'
// and current component version as 'true' in version comparison.
func (m *manager) EvaluateMinimumVersionFlagOrFailsafe(flag string, failsafe bool) bool {
	fallbackVersion := "999.999.999"
	if failsafe {
		// If the fallback is used, the check will pass.
		fallbackVersion = m.componentVersion.String()
	}
	minVersion := m.evaluator.EvaluateStringFlagOrFailsafe(flag, fallbackVersion)
	// If a flag is defined for the relevant CA version major, it overrides the generic minimum version
	flagForCurrentMajor := flag + "For" + versionMajor(m.componentVersion.String())
	minVersionForCurrentMajorValue := m.evaluator.EvaluateStringFlagOrFailsafe(flagForCurrentMajor, minVersion)
	minVersionForCurrentMajor, err := version.FromString(minVersionForCurrentMajorValue)
	if err != nil {
		klog.Errorf("Evaluation using experiment flags %q and %q failed: %q is not a correct version, using failsafe: %v", flag, flagForCurrentMajor, minVersionForCurrentMajorValue, failsafe)
		return failsafe
	}
	return !m.componentVersion.LessThan(minVersionForCurrentMajor)
}

// EvaluateStringFlagOrFailsafe evaluates string flag value.
func (m *manager) EvaluateStringFlagOrFailsafe(flag, fallback string) string {
	return m.evaluator.EvaluateStringFlagOrFailsafe(flag, fallback)
}

func (m *manager) EvaluateDurationSecondsFlagOrFailsafe(flag string, fallback time.Duration) time.Duration {
	durationString := m.EvaluateStringFlagOrFailsafe(flag, "")
	parsedSeconds, err := strconv.ParseInt(durationString, 10, 64)
	if err != nil {
		return fallback
	}
	return time.Duration(parsedSeconds) * time.Second
}

func (m *manager) EvaluateIntFlagOrFailsafe(flag string, fallback int) int {
	valueString := m.EvaluateStringFlagOrFailsafe(flag, "")
	parsedValue, err := strconv.Atoi(valueString)
	if err != nil {
		return fallback
	}
	return parsedValue
}

func (m *manager) DirectLaunchBoolFlag(flag string) bool {
	return m.evaluator.DirectLaunchBoolFlag(flag)
}

func versionMajor(v string) string {
	return strings.SplitN(v, ".", 2)[0]
}
