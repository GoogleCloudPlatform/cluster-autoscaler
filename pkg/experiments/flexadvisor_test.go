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

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
)

func TestIsFlexAdvisorSendRankEnabled(t *testing.T) {
	testCases := []struct {
		name     string
		manager  Manager
		expected bool
	}{
		{
			name:     "nil manager defaults to true",
			manager:  nil,
			expected: true,
		},
		{
			name:     "flags not configured defaults to true",
			manager:  NewMockManager(),
			expected: true,
		},
		{
			name: "disabled via enabled flag killswitch",
			manager: NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{
					FlexAdvisorSendRankEnabledFlag: false,
				},
				map[string]string{},
			),
			expected: false,
		},
		{
			name: "min version not met",
			manager: NewMockManagerWithOptions(
				version.Version{1, 20, 0, 0},
				map[string]bool{},
				map[string]string{
					FlexAdvisorSendRankMinCAVersionFlag: "1.25.0-gke.100",
				},
			),
			expected: false,
		},
		{
			name: "min version met",
			manager: NewMockManagerWithOptions(
				version.Version{1, 26, 0, 0},
				map[string]bool{},
				map[string]string{
					FlexAdvisorSendRankMinCAVersionFlag: "1.25.0-gke.100",
				},
			),
			expected: true,
		},
		{
			name: "min version met, but disabled via killswitch",
			manager: NewMockManagerWithOptions(
				version.Version{1, 26, 0, 0},
				map[string]bool{
					FlexAdvisorSendRankEnabledFlag: false,
				},
				map[string]string{
					FlexAdvisorSendRankMinCAVersionFlag: "1.25.0-gke.100",
				},
			),
			expected: false,
		},
		{
			name:     "mock manager with min version flag enabled",
			manager:  NewMockManager(FlexAdvisorSendRankMinCAVersionFlag),
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsFlexAdvisorSendRankEnabled(tc.manager)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestIsFlexAdvisorSkipUnneededSpotConfigsEnabled(t *testing.T) {
	testCases := []struct {
		name     string
		manager  Manager
		expected bool
	}{
		{
			name:     "nil manager defaults to true",
			manager:  nil,
			expected: true,
		},
		{
			name:     "flags not configured defaults to true",
			manager:  NewMockManager(),
			expected: true,
		},
		{
			name: "disabled via enabled flag killswitch",
			manager: NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{
					FlexAdvisorSkipUnneededSpotConfigsEnabledFlag: false,
				},
				map[string]string{},
			),
			expected: false,
		},
		{
			name: "min version not met",
			manager: NewMockManagerWithOptions(
				version.Version{1, 20, 0, 0},
				map[string]bool{},
				map[string]string{
					FlexAdvisorSkipUnneededSpotConfigsMinCAVersionFlag: "1.25.0-gke.100",
				},
			),
			expected: false,
		},
		{
			name: "min version met",
			manager: NewMockManagerWithOptions(
				version.Version{1, 26, 0, 0},
				map[string]bool{},
				map[string]string{
					FlexAdvisorSkipUnneededSpotConfigsMinCAVersionFlag: "1.25.0-gke.100",
				},
			),
			expected: true,
		},
		{
			name: "min version met, but disabled via killswitch",
			manager: NewMockManagerWithOptions(
				version.Version{1, 26, 0, 0},
				map[string]bool{
					FlexAdvisorSkipUnneededSpotConfigsEnabledFlag: false,
				},
				map[string]string{
					FlexAdvisorSkipUnneededSpotConfigsMinCAVersionFlag: "1.25.0-gke.100",
				},
			),
			expected: false,
		},
		{
			name:     "mock manager with min version flag enabled",
			manager:  NewMockManager(FlexAdvisorSkipUnneededSpotConfigsMinCAVersionFlag),
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsFlexAdvisorSkipUnneededSpotConfigsEnabled(tc.manager)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestIsFlexAdvisorSpotUptimeEnabled(t *testing.T) {
	testCases := []struct {
		name    string
		manager Manager
		want    bool
	}{
		{
			name:    "nil_manager_defaults_true",
			manager: nil,
			want:    true,
		},
		{
			name:    "unconfigured_flags_defaults_true",
			manager: NewMockManager(),
			want:    true,
		},
		{
			name: "disabled_via_enabled_flag",
			manager: NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{
					FlexAdvisorSpotUptimeEnabledFlag: false,
				},
				map[string]string{},
			),
			want: false,
		},
		{
			name: "min_version_not_met",
			manager: NewMockManagerWithOptions(
				version.Version{1, 20, 0, 0},
				map[string]bool{},
				map[string]string{
					FlexAdvisorSpotUptimeMinCAVersionFlag: "1.25.0-gke.100",
				},
			),
			want: false,
		},
		{
			name: "min_version_met",
			manager: NewMockManagerWithOptions(
				version.Version{1, 26, 0, 0},
				map[string]bool{},
				map[string]string{
					FlexAdvisorSpotUptimeMinCAVersionFlag: "1.25.0-gke.100",
				},
			),
			want: true,
		},
		{
			name: "min_version_met_but_disabled_via_enabled_flag",
			manager: NewMockManagerWithOptions(
				version.Version{1, 26, 0, 0},
				map[string]bool{
					FlexAdvisorSpotUptimeEnabledFlag: false,
				},
				map[string]string{
					FlexAdvisorSpotUptimeMinCAVersionFlag: "1.25.0-gke.100",
				},
			),
			want: false,
		},
		{
			name:    "mock_manager_with_min_version_flag_enabled",
			manager: NewMockManager(FlexAdvisorSpotUptimeMinCAVersionFlag),
			want:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsFlexAdvisorSpotUptimeEnabled(tc.manager)
			if got != tc.want {
				t.Errorf("IsFlexAdvisorSpotUptimeEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsFlexAdvisorDefaultSpotUptimeEnabled(t *testing.T) {
	testCases := []struct {
		name    string
		manager Manager
		want    bool
	}{
		{
			name:    "nil_manager_defaults_false",
			manager: nil,
			want:    false,
		},
		{
			name:    "unconfigured_flags_defaults_false",
			manager: NewMockManager(),
			want:    false,
		},
		{
			name: "enabled_bool_flag_without_min_version_stays_false",
			manager: NewMockManagerWithOptions(
				version.Version{1, 38, 0, 0},
				map[string]bool{
					FlexAdvisorDefaultSpotUptimeEnabledFlag: true,
				},
				map[string]string{},
			),
			want: false,
		},
		{
			name: "min_version_not_met",
			manager: NewMockManagerWithOptions(
				version.Version{1, 36, 0, 0},
				map[string]bool{},
				map[string]string{
					FlexAdvisorDefaultSpotUptimeMinCAVersionFlag: "1.37.0-gke.100",
				},
			),
			want: false,
		},
		{
			name: "min_version_met",
			manager: NewMockManagerWithOptions(
				version.Version{1, 38, 0, 0},
				map[string]bool{},
				map[string]string{
					FlexAdvisorDefaultSpotUptimeMinCAVersionFlag: "1.37.0-gke.100",
				},
			),
			want: true,
		},
		{
			name: "min_version_met_but_disabled_via_enabled_flag",
			manager: NewMockManagerWithOptions(
				version.Version{1, 38, 0, 0},
				map[string]bool{
					FlexAdvisorDefaultSpotUptimeEnabledFlag: false,
				},
				map[string]string{
					FlexAdvisorDefaultSpotUptimeMinCAVersionFlag: "1.37.0-gke.100",
				},
			),
			want: false,
		},
		{
			name:    "mock_manager_with_min_version_flag_enabled",
			manager: NewMockManager(FlexAdvisorDefaultSpotUptimeMinCAVersionFlag),
			want:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsFlexAdvisorDefaultSpotUptimeEnabled(tc.manager)
			if got != tc.want {
				t.Errorf("IsFlexAdvisorDefaultSpotUptimeEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}
