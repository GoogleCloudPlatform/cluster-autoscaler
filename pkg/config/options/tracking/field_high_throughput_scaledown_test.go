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

package tracking

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
)

func TestHighThroughputScaledownFieldsSetValue(t *testing.T) {
	const caVersion = "37.0.0-gke.0"

	highThroughputOpts := internalopts.AutoscalingOptions{
		AutoscalingOptions: config.AutoscalingOptions{
			KubeClientOpts: config.KubeClientOptions{
				Master:          "https://10.0.0.1",
				KubeClientQPS:   200,
				KubeClientBurst: 400,
			},
			MaxScaleDownParallelism:    250,
			MaxConcurrentNodesTainting: 50,
		},
	}

	baselineOpts := internalopts.AutoscalingOptions{
		AutoscalingOptions: config.AutoscalingOptions{
			KubeClientOpts: config.KubeClientOptions{
				Master:          "https://10.0.0.1",
				KubeClientQPS:   BaselineKubeClientQPS,
				KubeClientBurst: BaselineKubeClientBurst,
			},
			MaxScaleDownParallelism:    BaselineMaxScaleDownParallelism,
			MaxConcurrentNodesTainting: BaselineMaxConcurrentNodesTainting,
		},
	}

	lowerThanBaselineOpts := internalopts.AutoscalingOptions{
		AutoscalingOptions: config.AutoscalingOptions{
			KubeClientOpts: config.KubeClientOptions{
				Master:          "https://10.0.0.1",
				KubeClientQPS:   50,
				KubeClientBurst: 80,
			},
			MaxScaleDownParallelism:    10,
			MaxConcurrentNodesTainting: 3,
		},
	}

	testCases := []struct {
		name            string
		optsFromFlags   internalopts.AutoscalingOptions
		boolExps        map[string]bool
		stringExps      map[string]string
		overriddenFlags map[string]bool
		wantOpts        internalopts.AutoscalingOptions
	}{
		{
			name:          "high throughput values with both experiments undefined (failsafe true)",
			optsFromFlags: highThroughputOpts,
			wantOpts:      highThroughputOpts,
		},
		{
			name:          "high throughput values with Enabled=true and MinCAVersion satisfied",
			optsFromFlags: highThroughputOpts,
			boolExps:      map[string]bool{experiments.HighThroughputScaledownEnabledFlag: true},
			stringExps:    map[string]string{experiments.HighThroughputScaledownMinCAVersionFlag: "36.0.0-gke.0"},
			wantOpts:      highThroughputOpts,
		},
		{
			name:          "high throughput values clamped when Enabled=false",
			optsFromFlags: highThroughputOpts,
			boolExps:      map[string]bool{experiments.HighThroughputScaledownEnabledFlag: false},
			wantOpts:      baselineOpts,
		},
		{
			name:          "high throughput values clamped when MinCAVersion is higher than current CA version",
			optsFromFlags: highThroughputOpts,
			stringExps:    map[string]string{experiments.HighThroughputScaledownMinCAVersionFlag: "37.0.0-gke.1"},
			wantOpts:      baselineOpts,
		},
		{
			name:          "high throughput values clamped when Enabled=false even if MinCAVersion is satisfied",
			optsFromFlags: highThroughputOpts,
			boolExps:      map[string]bool{experiments.HighThroughputScaledownEnabledFlag: false},
			stringExps:    map[string]string{experiments.HighThroughputScaledownMinCAVersionFlag: "36.0.0-gke.0"},
			wantOpts:      baselineOpts,
		},
		{
			name:          "lower than baseline values are preserved even when Enabled=false",
			optsFromFlags: lowerThanBaselineOpts,
			boolExps:      map[string]bool{experiments.HighThroughputScaledownEnabledFlag: false},
			wantOpts:      lowerThanBaselineOpts,
		},
		{
			name:          "explicit --override_ flags bypass clamping when Enabled=false",
			optsFromFlags: highThroughputOpts,
			boolExps:      map[string]bool{experiments.HighThroughputScaledownEnabledFlag: false},
			overriddenFlags: map[string]bool{
				kubeClientQPSFlag:              true,
				kubeClientBurstFlag:            true,
				maxScaleDownParallelismFlag:    true,
				maxConcurrentNodesTaintingFlag: true,
			},
			wantOpts: highThroughputOpts,
		},
		{
			name:          "partial --override_ flags only bypass clamping for overridden flags",
			optsFromFlags: highThroughputOpts,
			boolExps:      map[string]bool{experiments.HighThroughputScaledownEnabledFlag: false},
			overriddenFlags: map[string]bool{
				kubeClientQPSFlag:           true,
				maxScaleDownParallelismFlag: true,
			},
			wantOpts: internalopts.AutoscalingOptions{
				AutoscalingOptions: config.AutoscalingOptions{
					KubeClientOpts: config.KubeClientOptions{
						Master:          "https://10.0.0.1",
						KubeClientQPS:   200,
						KubeClientBurst: BaselineKubeClientBurst,
					},
					MaxScaleDownParallelism:    250,
					MaxConcurrentNodesTainting: BaselineMaxConcurrentNodesTainting,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			optsFromFlags := tc.optsFromFlags
			optsFromFlags.FlagOverrideResult = internalopts.FlagOverrideResult{
				OverriddenFlags: tc.overriddenFlags,
			}

			v, err := version.FromString(caVersion)
			assert.NoError(t, err)
			em := experiments.NewMockManagerWithOptions(v, tc.boolExps, tc.stringExps)

			var gotOpts internalopts.AutoscalingOptions
			assert.NoError(t, kubeClientOptsField.setValue(optsFromFlags, em, &gotOpts))
			assert.NoError(t, maxScaleDownParallelismField.setValue(optsFromFlags, em, &gotOpts))
			assert.NoError(t, maxConcurrentNodesTaintingField.setValue(optsFromFlags, em, &gotOpts))

			assert.Equal(t, tc.wantOpts.KubeClientOpts, gotOpts.KubeClientOpts)
			assert.Equal(t, tc.wantOpts.MaxScaleDownParallelism, gotOpts.MaxScaleDownParallelism)
			assert.Equal(t, tc.wantOpts.MaxConcurrentNodesTainting, gotOpts.MaxConcurrentNodesTainting)
		})
	}
}

func TestHighThroughputScaledownFieldsValueEqualAndGetValueStr(t *testing.T) {
	optsA := internalopts.AutoscalingOptions{
		AutoscalingOptions: config.AutoscalingOptions{
			KubeClientOpts: config.KubeClientOptions{
				KubeClientQPS:   200,
				KubeClientBurst: 400,
			},
			MaxScaleDownParallelism:    250,
			MaxConcurrentNodesTainting: 50,
		},
	}
	optsB := internalopts.AutoscalingOptions{
		AutoscalingOptions: config.AutoscalingOptions{
			KubeClientOpts: config.KubeClientOptions{
				KubeClientQPS:   100,
				KubeClientBurst: 100,
			},
			MaxScaleDownParallelism:    100,
			MaxConcurrentNodesTainting: 5,
		},
	}

	assert.True(t, kubeClientOptsField.valueEqual(optsA, optsA))
	assert.False(t, kubeClientOptsField.valueEqual(optsA, optsB))
	assert.Equal(t, "{KubeClientQPS:200 KubeClientBurst:400}", kubeClientOptsField.getValueStr(optsA))

	assert.True(t, maxScaleDownParallelismField.valueEqual(optsA, optsA))
	assert.False(t, maxScaleDownParallelismField.valueEqual(optsA, optsB))
	assert.Equal(t, "250", maxScaleDownParallelismField.getValueStr(optsA))

	assert.True(t, maxConcurrentNodesTaintingField.valueEqual(optsA, optsA))
	assert.False(t, maxConcurrentNodesTaintingField.valueEqual(optsA, optsB))
	assert.Equal(t, "50", maxConcurrentNodesTaintingField.getValueStr(optsA))
}
