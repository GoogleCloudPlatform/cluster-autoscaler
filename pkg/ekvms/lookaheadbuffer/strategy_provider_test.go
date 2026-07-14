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

package lookaheadbuffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestSelectStrategyOnNilProvider(t *testing.T) {
	var p *strategyProviderImpl
	_, err := p.Strategy()
	assert.Error(t, err)
}

func TestStrategy(t *testing.T) {
	protoTieredStrategy := &TieredStrategy{
		[]Tier{
			{
				NumLookaheadPods:       0,
				LookaheadPodPercentage: 30,
				MaxLookaheadCPU:        640,
				LookaheadPodMilliCPU:   32000,
				LookaheadPodMemKib:     128 * 1024 * 1024,
				MinTargetNodesCPU:      400,
			},
			{
				NumLookaheadPods:     1,
				LookaheadPodMilliCPU: 8000,
				LookaheadPodMemKib:   32 * 1024 * 1024,
				MinTargetNodesCPU:    0,
			},
		},
	}
	protoTieredMetricStrategy := `{"tieredStrategy":{"tiers":[{"numLookaheadPods":0,"lookaheadPodPercentage":30,"maxLookaheadCpu":640,"lookaheadPodMilliCpu":32000,"lookaheadPodMemKib":134217728,"minTargetNodesCpu":400},{"numLookaheadPods":1,"lookaheadPodPercentage":0,"maxLookaheadCpu":0,"lookaheadPodMilliCpu":8000,"lookaheadPodMemKib":33554432,"minTargetNodesCpu":0}]}}`
	experimentTieredStrategy := &TieredStrategy{
		[]Tier{
			{
				NumLookaheadPods:     2,
				LookaheadPodMilliCPU: 8000,
				LookaheadPodMemKib:   24 * 1024 * 1024,
				MinTargetNodesCPU:    200,
			},
		},
	}
	experimentTieredMetricStrategy := `{"tieredStrategy":{"tiers":[{"numLookaheadPods":2,"lookaheadPodPercentage":0,"maxLookaheadCpu":0,"lookaheadPodMilliCpu":8000,"lookaheadPodMemKib":25165824,"minTargetNodesCpu":200}]}}`

	testCases := []struct {
		desc                    string
		flagConfig              LookaheadPodStrategy
		experimentConfig        LookaheadPodStrategy
		ekResizingEnabled       bool
		wantStrategy            LookaheadPodStrategy
		wantEmitMetrics         bool
		wantMetricsPhase        string
		wantMetricsLaunchedFrom string
		wantMetricsStrategy     string
	}{
		{
			desc: "using proto when proto status is enabled",
			flagConfig: LookaheadPodStrategy{
				TieredStrategy: protoTieredStrategy,
				MinCaVersion:   "v1.2.3",
				Status:         Enabled,
			},
			experimentConfig: LookaheadPodStrategy{
				TieredStrategy: experimentTieredStrategy,
				MinCaVersion:   "v9.9.9",
				Status:         Enabled,
			},
			ekResizingEnabled: true,
			wantEmitMetrics:   true,
			wantStrategy: LookaheadPodStrategy{
				TieredStrategy: protoTieredStrategy,
				MinCaVersion:   "v1.2.3",
				Status:         Enabled,
			},
			wantMetricsPhase:        string(Enabled),
			wantMetricsLaunchedFrom: string(clusterProtoSource),
			wantMetricsStrategy:     protoTieredMetricStrategy,
		},
		{
			desc: "unspecified strategy when proto status is enabled but ekResizingEnabled is false",
			flagConfig: LookaheadPodStrategy{
				TieredStrategy: protoTieredStrategy,
				MinCaVersion:   "v1.2.3",
				Status:         Enabled,
			},
			experimentConfig: LookaheadPodStrategy{
				TieredStrategy: experimentTieredStrategy,
				MinCaVersion:   "v9.9.9",
				Status:         Enabled,
			},
			ekResizingEnabled: false,
			wantEmitMetrics:   false,
			wantStrategy:      LookaheadPodStrategy{Status: Unspecified},
		},
		{
			desc: "using proto when proto status is disabled",
			flagConfig: LookaheadPodStrategy{
				TieredStrategy: protoTieredStrategy,
				MinCaVersion:   "v1.2.3",
				Status:         Disabled,
			},
			experimentConfig: LookaheadPodStrategy{
				TieredStrategy: experimentTieredStrategy,
				MinCaVersion:   "v9.9.9",
				Status:         Enabled,
			},
			ekResizingEnabled: true,
			wantEmitMetrics:   true,
			wantStrategy: LookaheadPodStrategy{
				TieredStrategy: protoTieredStrategy,
				MinCaVersion:   "v1.2.3",
				Status:         Disabled,
			},
			wantMetricsPhase:        string(Disabled),
			wantMetricsLaunchedFrom: string(clusterProtoSource),
		},
		{
			desc: "using experiment when proto status is unspecified and experiment config status is enabled",
			flagConfig: LookaheadPodStrategy{
				TieredStrategy: protoTieredStrategy,
				MinCaVersion:   "v1.2.3",
				Status:         Unspecified,
			},
			experimentConfig: LookaheadPodStrategy{
				TieredStrategy: experimentTieredStrategy,
				MinCaVersion:   "v9.9.9",
				Status:         Enabled,
			},
			ekResizingEnabled: true,
			wantEmitMetrics:   true,
			wantStrategy: LookaheadPodStrategy{
				TieredStrategy: experimentTieredStrategy,
				MinCaVersion:   "v9.9.9",
				Status:         Enabled,
			},
			wantMetricsPhase:        string(Enabled),
			wantMetricsLaunchedFrom: string(experimentSource),
			wantMetricsStrategy:     experimentTieredMetricStrategy,
		},
		{
			desc: "using experiment when proto status is unspecified and experiment config status is disabled",
			flagConfig: LookaheadPodStrategy{
				TieredStrategy: protoTieredStrategy,
				MinCaVersion:   "v1.2.3",
				Status:         Unspecified,
			},
			experimentConfig: LookaheadPodStrategy{
				TieredStrategy: experimentTieredStrategy,
				MinCaVersion:   "v9.9.9",
				Status:         Disabled,
			},
			ekResizingEnabled: true,
			wantEmitMetrics:   true,
			wantStrategy: LookaheadPodStrategy{
				TieredStrategy: experimentTieredStrategy,
				MinCaVersion:   "v9.9.9",
				Status:         Disabled,
			},
			wantMetricsPhase:        string(Disabled),
			wantMetricsLaunchedFrom: string(experimentSource),
		},
		{
			desc: "unspecified strategy when proto status is unspecified and experiment config status is unspecified",
			flagConfig: LookaheadPodStrategy{
				TieredStrategy: protoTieredStrategy,
				MinCaVersion:   "v1.2.3",
				Status:         Unspecified,
			},
			experimentConfig: LookaheadPodStrategy{
				TieredStrategy: experimentTieredStrategy,
				MinCaVersion:   "v9.9.9",
				Status:         Unspecified,
			},
			ekResizingEnabled:       true,
			wantEmitMetrics:         true,
			wantStrategy:            LookaheadPodStrategy{Status: Unspecified},
			wantMetricsPhase:        string(Unspecified),
			wantMetricsLaunchedFrom: string(undefinedSource),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			metrics := &MockMetrics{}
			metrics.On("UpdateLookaheadLaunchStatus", mock.Anything, mock.Anything, mock.Anything).Return()
			p := &strategyProviderImpl{
				flagStrategy:       tc.flagConfig,
				experimentStrategy: tc.experimentConfig,
				laMetrics:          metrics,
			}
			p.SetEkResizingEnabled(tc.ekResizingEnabled)
			gotConfig, err := p.Strategy()
			assert.NoError(t, err)
			assert.Equal(t, tc.wantStrategy, gotConfig)
			if tc.wantEmitMetrics {
				metrics.AssertCalled(t, "UpdateLookaheadLaunchStatus", tc.wantMetricsPhase, tc.wantMetricsLaunchedFrom, tc.wantMetricsStrategy)
			} else {
				metrics.AssertNotCalled(t, "UpdateLookaheadLaunchStatus")
			}
		})
	}
}

func TestRefreshStrategySafeOnNil(t *testing.T) {
	var provider *strategyProviderImpl
	assert.NotPanics(t, func() {
		provider.RefreshStrategy()
	})
}

func TestRefreshStrategy(t *testing.T) {
	componentVersion := version.Version{31, 157, 3}
	enabledFlags := map[string]bool{}

	for _, tc := range []struct {
		desc string
		experiments.Manager
		want LookaheadPodStrategy
	}{
		{
			desc: "experiment strategy is unspecified when AutopilotEk::LookaheadPodsV1 flag is unset",
			Manager: experiments.NewMockManagerWithOptions(
				componentVersion,
				enabledFlags,
				map[string]string{},
			),
			want: LookaheadPodStrategy{Status: Unspecified},
		},
		{
			desc: "experiment strategy is unspecified when AutopilotEk::LookaheadPodsV1 flag is empty",
			Manager: experiments.NewMockManagerWithOptions(
				componentVersion,
				enabledFlags,
				map[string]string{experiments.EkLookaheadPodsV1Flag: ""},
			),
			want: LookaheadPodStrategy{Status: Unspecified},
		},
		{
			desc: "experiment strategy is unspecified when AutopilotEk::LookaheadPodsV1 flag is invalid JSON",
			Manager: experiments.NewMockManagerWithOptions(
				componentVersion,
				enabledFlags,
				map[string]string{experiments.EkLookaheadPodsV1Flag: "{"},
			),
			want: LookaheadPodStrategy{Status: Unspecified},
		},
		{
			desc: "experiment strategy is unspecified when MinCaVersion is bigger than component version",
			Manager: experiments.NewMockManagerWithOptions(
				componentVersion,
				enabledFlags,
				map[string]string{experiments.EkLookaheadPodsV1Flag: "{\"minCaVersion\": \"999.999.999\"}"},
			),
			want: LookaheadPodStrategy{Status: Unspecified},
		},
		{
			desc: "experiment strategy is set when AutopilotEk::LookaheadPodsV1 flag is valid",
			Manager: experiments.NewMockManagerWithOptions(
				componentVersion,
				enabledFlags,
				map[string]string{experiments.EkLookaheadPodsV1Flag: `
				{
					"minCaVersion": "30.0.0",
					"status": "STATUS_ENABLED",
					"tieredStrategy": {
						"tiers": [
							{
								"numLookaheadPods": 1,
								"lookaheadPodMilliCpu": 8000,
								"lookaheadPodMemKib": 134217728,
								"minTargetNodesCpu": 200
							}
						]
					}
				}
				`},
			),
			want: LookaheadPodStrategy{
				MinCaVersion: "30.0.0",
				Status:       Enabled,
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   134217728,
							MinTargetNodesCPU:    200,
						},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			p := &strategyProviderImpl{Manager: tc.Manager, componentVersion: componentVersion}
			p.RefreshStrategy()
			assert.Equal(t, tc.want, p.experimentStrategy)
		})
	}
}
