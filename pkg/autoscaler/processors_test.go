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

package autoscaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestInitCapacityBufferMetricsProcessor(t *testing.T) {
	testCases := []struct {
		name            string
		buffersEnabled  bool
		directLaunch    bool
		expectProcessor bool
	}{
		{
			name:            "Buffers disabled",
			buffersEnabled:  false,
			directLaunch:    true,
			expectProcessor: false,
		},
		{
			name:            "Buffers enabled, DirectLaunch True",
			buffersEnabled:  true,
			directLaunch:    true,
			expectProcessor: true,
		},
		{
			name:            "Buffers enabled, DirectLaunch False",
			buffersEnabled:  true,
			directLaunch:    false,
			expectProcessor: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			boolFlags := map[string]bool{
				experiments.CapacityBuffersMetricProcessor: tc.directLaunch,
			}
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{})
			registry := fakepods.NewRegistry(nil)

			processor := initCapacityBufferMetricsProcessor(experimentsManager, nil, registry, tc.buffersEnabled)

			if tc.expectProcessor {
				assert.NotNil(t, processor)
			} else {
				assert.Nil(t, processor)
			}
		})
	}
}
