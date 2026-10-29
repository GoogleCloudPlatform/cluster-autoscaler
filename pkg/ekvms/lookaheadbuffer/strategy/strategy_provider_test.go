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

package strategy

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

const experimentFlag = "AutopilotTestFamily::LookaheadPodsV1"

func TestStrategy(t *testing.T) {
	protoTieredStrategy := &TieredStrategy{
		Tiers: []Tier{
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
		Tiers: []Tier{
			{
				NumLookaheadPods:     2,
				LookaheadPodMilliCPU: 8000,
				LookaheadPodMemKib:   24 * 1024 * 1024,
				MinTargetNodesCPU:    200,
			},
		},
	}
	experimentTieredMetricStrategy := `{"tieredStrategy":{"tiers":[{"numLookaheadPods":2,"lookaheadPodPercentage":0,"maxLookaheadCpu":0,"lookaheadPodMilliCpu":8000,"lookaheadPodMemKib":25165824,"minTargetNodesCpu":200}]}}`
	experimentConfigEnabled := `{"status":"STATUS_ENABLED","minCaVersion":"v9.9.9","tieredStrategy":{"tiers":[{"numLookaheadPods":2,"lookaheadPodPercentage":0,"maxLookaheadCpu":0,"lookaheadPodMilliCpu":8000,"lookaheadPodMemKib":25165824,"minTargetNodesCpu":200}]}}`
	experimentConfigDisabled := `{"status":"STATUS_DISABLED","minCaVersion":"v9.9.9","tieredStrategy":{"tiers":[{"numLookaheadPods":2,"lookaheadPodPercentage":0,"maxLookaheadCpu":0,"lookaheadPodMilliCpu":8000,"lookaheadPodMemKib":25165824,"minTargetNodesCpu":200}]}}`
	experimentConfigUnspecified := `{"status":"STATUS_UNSPECIFIED","minCaVersion":"v9.9.9","tieredStrategy":{"tiers":[{"numLookaheadPods":2,"lookaheadPodPercentage":0,"maxLookaheadCpu":0,"lookaheadPodMilliCpu":8000,"lookaheadPodMemKib":25165824,"minTargetNodesCpu":200}]}}`

	flagConfigEnabled := `'{"status":"STATUS_ENABLED","minCaVersion":"v1.2.3","tieredStrategy":{"tiers":[{"numLookaheadPods":0,"lookaheadPodPercentage":30,"maxLookaheadCpu":640,"lookaheadPodMilliCpu":32000,"lookaheadPodMemKib":134217728,"minTargetNodesCpu":400},{"numLookaheadPods":1,"lookaheadPodPercentage":0,"maxLookaheadCpu":0,"lookaheadPodMilliCpu":8000,"lookaheadPodMemKib":33554432,"minTargetNodesCpu":0}]}}'`
	flagConfigDisabled := `'{"status":"STATUS_DISABLED","minCaVersion":"v1.2.3","tieredStrategy":{"tiers":[{"numLookaheadPods":0,"lookaheadPodPercentage":30,"maxLookaheadCpu":640,"lookaheadPodMilliCpu":32000,"lookaheadPodMemKib":134217728,"minTargetNodesCpu":400},{"numLookaheadPods":1,"lookaheadPodPercentage":0,"maxLookaheadCpu":0,"lookaheadPodMilliCpu":8000,"lookaheadPodMemKib":33554432,"minTargetNodesCpu":0}]}}'`
	flagConfigUnspecified := `'{"status":"STATUS_UNSPECIFIED","minCaVersion":"v1.2.3","tieredStrategy":{"tiers":[{"numLookaheadPods":0,"lookaheadPodPercentage":30,"maxLookaheadCpu":640,"lookaheadPodMilliCpu":32000,"lookaheadPodMemKib":134217728,"minTargetNodesCpu":400},{"numLookaheadPods":1,"lookaheadPodPercentage":0,"maxLookaheadCpu":0,"lookaheadPodMilliCpu":8000,"lookaheadPodMemKib":33554432,"minTargetNodesCpu":0}]}}'`

	componentVersion := version.Version{10, 0, 0}

	for _, family := range []string{machinetypes.EK.Name(), machinetypes.E4A.Name(), machinetypes.E4.Name()} {
		t.Run(family, func(t *testing.T) {
			testCases := []struct {
				desc               string
				flagConfig         string
				experimentConfig   string
				resizingEnabled    bool
				wantStrategy       LookaheadPodStrategy
				wantEmitMetrics    bool
				wantLaunchPhase    string
				wantLaunchedFrom   string
				wantLaunchStrategy string
			}{
				{
					desc:             "using proto when proto status is enabled",
					flagConfig:       flagConfigEnabled,
					experimentConfig: experimentConfigEnabled,
					resizingEnabled:  true,
					wantEmitMetrics:  true,
					wantStrategy: LookaheadPodStrategy{
						TieredStrategy: protoTieredStrategy,
						MinCaVersion:   "v1.2.3",
						Status:         Enabled,
					},
					wantLaunchPhase:    string(Enabled),
					wantLaunchedFrom:   string(clusterProtoSource),
					wantLaunchStrategy: protoTieredMetricStrategy,
				},
				{
					desc:             "using proto when proto status is enabled but resizing is not enabled",
					flagConfig:       flagConfigEnabled,
					experimentConfig: experimentConfigEnabled,
					resizingEnabled:  false,
					wantEmitMetrics:  false,
					wantStrategy:     LookaheadPodStrategy{Status: Unspecified},
				},
				{
					desc:             "using proto when proto status is disabled",
					flagConfig:       flagConfigDisabled,
					experimentConfig: experimentConfigEnabled,
					resizingEnabled:  true,
					wantEmitMetrics:  true,
					wantStrategy: LookaheadPodStrategy{
						TieredStrategy: protoTieredStrategy,
						MinCaVersion:   "v1.2.3",
						Status:         Disabled,
					},
					wantLaunchPhase:  string(Disabled),
					wantLaunchedFrom: string(clusterProtoSource),
				},
				{
					desc:             "using experiment when proto status is unspecified and experiment config status is enabled",
					flagConfig:       flagConfigUnspecified,
					experimentConfig: experimentConfigEnabled,
					resizingEnabled:  true,
					wantEmitMetrics:  true,
					wantStrategy: LookaheadPodStrategy{
						TieredStrategy: experimentTieredStrategy,
						MinCaVersion:   "v9.9.9",
						Status:         Enabled,
					},
					wantLaunchPhase:    string(Enabled),
					wantLaunchedFrom:   string(experimentSource),
					wantLaunchStrategy: experimentTieredMetricStrategy,
				},
				{
					desc:             "using experiment when proto status is unspecified and experiment config status is disabled",
					flagConfig:       flagConfigUnspecified,
					experimentConfig: experimentConfigDisabled,
					resizingEnabled:  true,
					wantEmitMetrics:  true,
					wantStrategy: LookaheadPodStrategy{
						TieredStrategy: experimentTieredStrategy,
						MinCaVersion:   "v9.9.9",
						Status:         Disabled,
					},
					wantLaunchPhase:  string(Disabled),
					wantLaunchedFrom: string(experimentSource),
				},
				{
					desc:             "unspecified strategy when proto status is unspecified and experiment config status is unspecified",
					flagConfig:       flagConfigUnspecified,
					experimentConfig: experimentConfigUnspecified,
					resizingEnabled:  true,
					wantEmitMetrics:  true,
					wantStrategy:     LookaheadPodStrategy{Status: Unspecified},
					wantLaunchPhase:  string(Unspecified),
					wantLaunchedFrom: string(undefinedSource),
				},
			}
			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					metrics := &mockMetrics{}
					metrics.On("UpdateLookaheadLaunchStatus", mock.Anything, mock.Anything, mock.Anything).Return()

					experimentFlags := map[string][]string{family: {experimentFlag}}

					manager := experiments.NewMockManagerWithOptions(
						componentVersion,
						nil,
						map[string]string{experimentFlag: tc.experimentConfig},
					)

					p, err := NewProvider(
						manager,
						map[string]string{family: tc.flagConfig},
						experimentFlags,
						metrics,
						componentVersion,
					)
					assert.NoError(t, err)
					p.SetResizingEnabled(&mockAutoprovisioningProvider{resizingEnabled: tc.resizingEnabled})
					gotStrategy, err := p.Strategy(family)
					assert.NoError(t, err)
					assert.Equal(t, tc.wantStrategy, gotStrategy)
					// Currently lookahead_launch_status only tracks EK LA launch status.
					// TODO(b/567108065): Update test when metrics are supported for all resizable VMs.
					if tc.wantEmitMetrics && family == machinetypes.EK.Name() {
						metrics.AssertCalled(t, "UpdateLookaheadLaunchStatus", tc.wantLaunchPhase, tc.wantLaunchedFrom, tc.wantLaunchStrategy)
					} else {
						metrics.AssertNotCalled(t, "UpdateLookaheadLaunchStatus")
					}
				})
			}
		})
	}
}

func TestStrategyWithPrioritizedExperimentFlags(t *testing.T) {
	const (
		v1Flag = experimentFlag
		v2Flag = "AutopilotTestFamily::LookaheadPodsV2"
	)
	family := machinetypes.EK.Name()
	componentVersion := version.Version{31, 157, 3}
	v1Config := `{"minCaVersion": "30.0.0", "status": "STATUS_ENABLED", "tieredStrategy": {"tiers": [{"numLookaheadPods": 3, "lookaheadPodMilliCpu": 6000}]}}`
	v1Strategy := LookaheadPodStrategy{
		MinCaVersion:   "30.0.0",
		Status:         Enabled,
		TieredStrategy: &TieredStrategy{Tiers: []Tier{{NumLookaheadPods: 3, LookaheadPodMilliCPU: 6000}}},
	}
	v2Config := `{"minCaVersion": "31.0.0", "status": "STATUS_ENABLED", "tieredStrategy": {"tiers": [{"numLookaheadPods": 1, "lookaheadPodMilliCpu": 6000}], "newClusterTier": {"numLookaheadPods": 3, "lookaheadPodMilliCpu": 6000}, "newClusterMaxAgeHours": 24}}`
	v2Strategy := LookaheadPodStrategy{
		MinCaVersion: "31.0.0",
		Status:       Enabled,
		TieredStrategy: &TieredStrategy{
			Tiers:                 []Tier{{NumLookaheadPods: 1, LookaheadPodMilliCPU: 6000}},
			NewClusterTier:        &Tier{NumLookaheadPods: 3, LookaheadPodMilliCPU: 6000},
			NewClusterMaxAgeHours: 24,
		},
	}

	testCases := []struct {
		desc    string
		configs map[string]string
		want    LookaheadPodStrategy
	}{
		{
			desc:    "uses V2 when enabled",
			configs: map[string]string{v1Flag: v1Config, v2Flag: v2Config},
			want:    v2Strategy,
		},
		{
			desc:    "uses V2 when disabled",
			configs: map[string]string{v1Flag: v1Config, v2Flag: `{"minCaVersion": "30.0.0", "status": "STATUS_DISABLED"}`},
			want:    LookaheadPodStrategy{MinCaVersion: "30.0.0", Status: Disabled},
		},
		{
			desc:    "falls back to V1 when V2 is unset",
			configs: map[string]string{v1Flag: v1Config},
			want:    v1Strategy,
		},
		{
			desc:    "falls back to V1 when V2 requires a newer CA version",
			configs: map[string]string{v1Flag: v1Config, v2Flag: `{"minCaVersion": "99.0.0", "status": "STATUS_ENABLED", "tieredStrategy": {"tiers": [{"numLookaheadPods": 1}]}}`},
			want:    v1Strategy,
		},
		{
			desc:    "falls back to V1 when V2 is invalid",
			configs: map[string]string{v1Flag: v1Config, v2Flag: "{"},
			want:    v1Strategy,
		},
		{
			desc:    "unspecified when neither is set",
			configs: map[string]string{},
			want:    LookaheadPodStrategy{Status: Unspecified},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			configs := map[string]string{}
			p, err := NewProvider(
				experiments.NewMockManagerWithOptions(componentVersion, nil, configs),
				map[string]string{family: ""},
				map[string][]string{family: {v2Flag, v1Flag}},
				nil,
				componentVersion,
			)
			assert.NoError(t, err)
			p.SetResizingEnabled(&mockAutoprovisioningProvider{resizingEnabled: true})

			// Flags are set after creation to verify that Refresh updates all experiment sources.
			maps.Copy(configs, tc.configs)
			p.Refresh()

			got, err := p.Strategy(family)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSelectStrategyOnNilProvider(t *testing.T) {
	var p *Provider
	_, err := p.Strategy(machinetypes.EK.Name())
	assert.Error(t, err)
}

func TestRefreshSafeOnNil(t *testing.T) {
	var provider *Provider
	assert.NotPanics(t, func() {
		provider.Refresh()
	})
}

func TestStrategyUnknownFamily(t *testing.T) {
	p, err := NewProvider(nil, map[string]string{}, map[string][]string{}, nil, version.Version{})
	assert.NoError(t, err)
	_, err = p.Strategy("unknown-family")
	assert.Error(t, err)
}

func TestStrategyWithoutExperimentFlags(t *testing.T) {
	family := machinetypes.EK.Name()
	p, err := NewProvider(nil, map[string]string{family: ""}, map[string][]string{}, nil, version.Version{})
	assert.NoError(t, err)
	p.SetResizingEnabled(&mockAutoprovisioningProvider{resizingEnabled: true})
	got, err := p.Strategy(family)
	assert.NoError(t, err)
	assert.Equal(t, LookaheadPodStrategy{Status: Unspecified}, got)
}

type mockAutoprovisioningProvider struct {
	resizingEnabled bool
}

func (m *mockAutoprovisioningProvider) ResizingEnabled(machineFamily string) bool {
	return m.resizingEnabled
}

type mockMetrics struct {
	mock.Mock
}

func (m *mockMetrics) UpdateLookaheadLaunchStatus(launchPhase, launchedFrom, strategy string) {
	m.MethodCalled("UpdateLookaheadLaunchStatus", launchPhase, launchedFrom, strategy)
}
