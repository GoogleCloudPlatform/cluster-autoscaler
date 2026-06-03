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
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
)

type testManager interface {
	Manager
	DisableAllExperiments()
}

type mockManager struct {
	componentVersion version.Version
	boolFlags        map[string]bool
	stringFlags      map[string]string
}

// NewMockManager creates and returns an instance of mockManager.
func NewMockManager(enabledFeatures ...string) testManager {
	boolFlags := map[string]bool{}
	for _, f := range enabledFeatures {
		boolFlags[f] = true
	}
	return NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{})
}

// NewMockManagerWithOptions creates and returns an instance of mockManager with more customizable options.
func NewMockManagerWithOptions(componentVersion version.Version, boolFlags map[string]bool, stringFlags map[string]string) testManager {
	return &mockManager{
		componentVersion: componentVersion,
		boolFlags:        boolFlags,
		stringFlags:      stringFlags,
	}
}

func (m *mockManager) UpdateReleaseChannel(_ string) {}

// EvaluateBoolFlagOrFailsafe returns true i.f.f. provided flag is enabled
func (m *mockManager) EvaluateBoolFlagOrFailsafe(flag string, failsafe bool) bool {
	val, found := m.boolFlags[flag]
	if !found {
		return failsafe
	}
	return val
}

// EvaluateMinimumVersionFlagOrFailsafe returns true i.f.f. provided flag is enabled or the provided version is lower than the component version
func (m *mockManager) EvaluateMinimumVersionFlagOrFailsafe(flag string, failsafe bool) bool {
	if val, found := m.boolFlags[flag]; found {
		return val
	}
	val, found := m.stringFlags[flag]
	if !found {
		return failsafe
	}
	cv, err := version.FromString(val)
	if err != nil {
		return failsafe
	}
	return cv.LessThan(m.componentVersion)
}

func (m *mockManager) EvaluateStringFlagOrFailsafe(flag, failsafe string) string {
	val, found := m.stringFlags[flag]
	if !found {
		return failsafe
	}
	return val
}

func (m *mockManager) EvaluateIntFlagOrFailsafe(flag string, failsafe int) int {
	valStr, found := m.stringFlags[flag]
	if !found {
		return failsafe
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return failsafe
	}
	return val
}

func (m *mockManager) EvaluateDurationSecondsFlagOrFailsafe(flag string, failsafe time.Duration) time.Duration {
	val, err := strconv.ParseInt(m.stringFlags[flag], 10, 64)
	if err != nil {
		return failsafe
	}
	return time.Duration(val) * time.Second
}

func (m *mockManager) DirectLaunchBoolFlag(flag string) bool {
	if val, ok := m.boolFlags[flag]; ok && !val {
		return false
	}
	return true
}

func (m *mockManager) DisableAllExperiments() {
	m.componentVersion = version.Version{}
	for k := range m.boolFlags {
		m.boolFlags[k] = false
	}
	for k := range m.stringFlags {
		m.stringFlags[k] = ""
	}
}
