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

package computeclass

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestIsComputeClassMinCapacityEnabled(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		manager  experiments.Manager
		expected bool
	}{
		{
			name:     "nil manager",
			manager:  nil,
			expected: true,
		},
		{
			name: "flags not configured (defaults to true)",
			manager: experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{},
				map[string]string{},
			),
			expected: true,
		},
		{
			name: "enabled flag false",
			manager: experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{experiments.ComputeClassMinCapacityEnabledFlag: false},
				map[string]string{},
			),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsComputeClassMinCapacityEnabled(tc.manager))
		})
	}
}

func TestIsComputeClassEnhancedObservabilityEnabled(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		manager  experiments.Manager
		expected bool
	}{
		{
			name:     "nil manager",
			manager:  nil,
			expected: false,
		},
		{
			name: "flags configured true",
			manager: experiments.NewMockManagerWithOptions(
				version.Version{2, 0, 0, 0},
				map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true},
				map[string]string{experiments.ComputeClassEnhancedObservabilityMinCAVersionFlag: "1.0.0"},
			),
			expected: true,
		},
		{
			name: "enabled flag false",
			manager: experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: false},
				map[string]string{},
			),
			expected: false,
		},
		{
			name: "cluster version too old",
			manager: experiments.NewMockManagerWithOptions(
				version.Version{1, 28, 0, 0},
				map[string]bool{experiments.ComputeClassEnhancedObservabilityEnabledFlag: true},
				map[string]string{experiments.ComputeClassEnhancedObservabilityMinCAVersionFlag: "1.29.0"},
			),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsComputeClassEnhancedObservabilityEnabled(tc.manager))
		})
	}
}

func TestIsComputeClassConfigHashEnabled(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		manager  experiments.Manager
		expected bool
	}{
		{
			name:     "nil manager",
			manager:  nil,
			expected: false,
		},
		{
			name: "flags not configured (defaults to true)",
			manager: experiments.NewMockManagerWithOptions(
				version.Version{},
				map[string]bool{},
				map[string]string{},
			),
			expected: true,
		},
		{
			name: "flags configured true",
			manager: experiments.NewMockManagerWithOptions(
				version.Version{2, 0, 0, 0},
				map[string]bool{experiments.ComputeClassConfigHashEnabledFlag: true},
				map[string]string{experiments.ComputeClassConfigHashMinCAVersionFlag: "1.0.0"},
			),
			expected: true,
		},
		{
			name: "enabled flag false",
			manager: experiments.NewMockManagerWithOptions(
				version.Version{2, 0, 0, 0},
				map[string]bool{experiments.ComputeClassConfigHashEnabledFlag: false},
				map[string]string{experiments.ComputeClassConfigHashMinCAVersionFlag: "1.0.0"},
			),
			expected: false,
		},
		{
			name: "cluster version too old",
			manager: experiments.NewMockManagerWithOptions(
				version.Version{1, 28, 0, 0},
				map[string]bool{experiments.ComputeClassConfigHashEnabledFlag: true},
				map[string]string{experiments.ComputeClassConfigHashMinCAVersionFlag: "1.29.0"},
			),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsComputeClassConfigHashEnabled(tc.manager))
		})
	}
}
