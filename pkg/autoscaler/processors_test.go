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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/capacitybuffer/fakepods"
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

// TestBuildScaleDownSetProcessorsOrdering guards the ordering invariant documented
// on buildScaleDownSetProcessors: every per-node processor must run before
// nodes.AtomicResizeFilteringProcessor, and only whole-group processors may run
// after it. Getting this wrong silently breaks atomicity (a partially filtered
// atomic group would be drained node by node).
func TestBuildScaleDownSetProcessorsOrdering(t *testing.T) {
	testCases := []struct {
		name          string
		options       *internalopts.AutoscalingOptions
		expectedOrder []string
	}{
		{
			name:    "no optional processors",
			options: &internalopts.AutoscalingOptions{},
			expectedOrder: []string{
				"*processors.TotalMinSizeProcessor",
				"*nodes.AtomicResizeFilteringProcessor",
			},
		},
		{
			name: "blocking labels processor runs before the atomic filter",
			options: &internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					ScaleDownBlockingNodeLabels: []string{labels.TPUSliceLabel},
				},
			},
			expectedOrder: []string{
				"*processors.TotalMinSizeProcessor",
				"*scaledown.BlockingLabelsFilteringProcessor",
				"*nodes.AtomicResizeFilteringProcessor",
			},
		},
		{
			name: "min capacity processor runs after the atomic filter",
			options: &internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					EnableComputeClassMinCapacity: true,
				},
			},
			expectedOrder: []string{
				"*processors.TotalMinSizeProcessor",
				"*nodes.AtomicResizeFilteringProcessor",
				"*scaledown.AtomicMinCapacityProcessor",
			},
		},
		{
			name: "both optional processors sit on the correct side of the atomic filter",
			options: &internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					ScaleDownBlockingNodeLabels:   []string{labels.TPUSliceLabel},
					EnableComputeClassMinCapacity: true,
				},
			},
			expectedOrder: []string{
				"*processors.TotalMinSizeProcessor",
				"*scaledown.BlockingLabelsFilteringProcessor",
				"*nodes.AtomicResizeFilteringProcessor",
				"*scaledown.AtomicMinCapacityProcessor",
			},
		},
		{
			name: "empty blocking labels do not add the blocking labels processor",
			options: &internalopts.AutoscalingOptions{
				InternalOptions: internalopts.InternalOptions{
					ScaleDownBlockingNodeLabels: nil,
				},
			},
			expectedOrder: []string{
				"*processors.TotalMinSizeProcessor",
				"*nodes.AtomicResizeFilteringProcessor",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{}, map[string]string{})

			built := buildScaleDownSetProcessors(nil, tc.options, nil, experimentsManager)

			gotOrder := make([]string, 0, len(built))
			for _, p := range built {
				gotOrder = append(gotOrder, reflect.TypeOf(p).String())
			}
			assert.Equal(t, tc.expectedOrder, gotOrder)
		})
	}
}
