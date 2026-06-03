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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

func TestGetLookaheadPods(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		laStrategy      LookaheadPodStrategy
		laStrategyError error
		targetNodesCPUs int
		want            []*apiv1.Pod
	}{
		{
			desc:            "enabled status",
			targetNodesCPUs: 100,
			laStrategy: LookaheadPodStrategy{
				Status: Enabled,
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * 1024 * 1024,
							MinTargetNodesCPU:    0,
						},
					},
				},
			},
			want: GenerateLookaheadPods(1, *resource.NewMilliQuantity(8000, resource.DecimalSI), *resource.NewQuantity(32*size.GiB, resource.BinarySI), ""),
		},
		{
			desc:            "disabled status",
			targetNodesCPUs: 100,
			laStrategy: LookaheadPodStrategy{
				Status: Disabled,
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * 1024 * 1024,
							MinTargetNodesCPU:    0,
						},
					},
				},
			},
			want: nil,
		},
		{
			desc:            "unspecified status",
			targetNodesCPUs: 100,
			laStrategy: LookaheadPodStrategy{
				Status: Unspecified,
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * 1024 * 1024,
							MinTargetNodesCPU:    0,
						},
					},
				},
			},
			want: nil,
		},
		{
			desc:            "stragey provider returns error",
			targetNodesCPUs: 100,
			laStrategyError: errors.New("Some error"),
			want:            nil,
		},
		{
			desc:            "no tiered strategy",
			targetNodesCPUs: 100,
			laStrategy: LookaheadPodStrategy{
				Status: Enabled,
			},
			want: nil,
		},
		{
			desc:            "tiered strategy has no tiers",
			targetNodesCPUs: 100,
			laStrategy: LookaheadPodStrategy{
				Status:         Enabled,
				TieredStrategy: &TieredStrategy{},
			},
			want: nil,
		},
		{
			desc:            "1 la pod in first tier",
			targetNodesCPUs: 50,
			laStrategy: LookaheadPodStrategy{
				Status: Enabled,
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     2,
							LookaheadPodMilliCPU: 32000,
							LookaheadPodMemKib:   128 * 1024 * 1024,
							MinTargetNodesCPU:    100,
						},
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * 1024 * 1024,
							MinTargetNodesCPU:    0,
						},
					},
				},
			},
			want: GenerateLookaheadPods(1, *resource.NewMilliQuantity(8000, resource.DecimalSI), *resource.NewQuantity(32*size.GiB, resource.BinarySI), ""),
		},
		{
			desc:            "2 la pods in second tier",
			targetNodesCPUs: 200,
			laStrategy: LookaheadPodStrategy{
				Status: Enabled,
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     2,
							LookaheadPodMilliCPU: 32000,
							LookaheadPodMemKib:   128 * 1024 * 1024,
							MinTargetNodesCPU:    100,
						},
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * 1024 * 1024,
							MinTargetNodesCPU:    0,
						},
					},
				},
			},
			want: GenerateLookaheadPods(2, *resource.NewMilliQuantity(32000, resource.DecimalSI), *resource.NewQuantity(128*size.GiB, resource.BinarySI), ""),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {

			mockStrategyProvider := &mockStrategyProvider{}
			mockStrategyProvider.On("Strategy").Return(tc.laStrategy, tc.laStrategyError)
			p := NewPodProvider(mockStrategyProvider)
			got := p.GetLookaheadPods(tc.targetNodesCPUs, "")
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestGetLookaheadPodNumber(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		targetNodesCPUs int
		tier            Tier
		want            int
	}{
		{
			desc:            "numLookaheadPods>0_lookaheadPodPercentage>0",
			targetNodesCPUs: 100,
			tier: Tier{
				NumLookaheadPods:       5,
				LookaheadPodPercentage: 10,
				LookaheadPodMilliCPU:   1000,
			},
			want: 15,
		},
		{
			desc:            "numLookaheadPods>0_lookaheadPodPercentage=0",
			targetNodesCPUs: 100,
			tier: Tier{
				NumLookaheadPods:       5,
				LookaheadPodPercentage: 0,
				LookaheadPodMilliCPU:   1000,
			},
			want: 5,
		},
		{
			desc:            "numLookaheadPods=0_lookaheadPodPercentage>0",
			targetNodesCPUs: 100,
			tier: Tier{
				NumLookaheadPods:       0,
				LookaheadPodPercentage: 10,
				LookaheadPodMilliCPU:   1000,
			},
			want: 10,
		},
		{
			desc:            "numLookaheadPods=0_lookaheadPodPercentage=0",
			targetNodesCPUs: 100,
			tier: Tier{
				NumLookaheadPods:       0,
				LookaheadPodPercentage: 0,
				LookaheadPodMilliCPU:   1000,
			},
			want: 0,
		},
		{
			desc:            "pod_calculation_is_rounded_down",
			targetNodesCPUs: 100,
			tier: Tier{
				NumLookaheadPods:       0,
				LookaheadPodPercentage: 15,
				LookaheadPodMilliCPU:   4000,
			},
			want: 3,
		},
		{
			desc:            "maxLookaheadCpu=0",
			targetNodesCPUs: 100,
			tier: Tier{
				NumLookaheadPods:       0,
				LookaheadPodPercentage: 50,
				LookaheadPodMilliCPU:   1000,
				MaxLookaheadCPU:        0,
			},
			want: 50,
		},
		{
			desc:            "maxLookaheadCpu_doesn't_bound_lookahead_pod_number",
			targetNodesCPUs: 100,
			tier: Tier{
				NumLookaheadPods:       5,
				LookaheadPodPercentage: 20,
				LookaheadPodMilliCPU:   1000,
				MaxLookaheadCPU:        30,
			},
			want: 25,
		},
		{
			desc:            "maxLookaheadCpu_bounds_lookahead_pod_number",
			targetNodesCPUs: 100,
			tier: Tier{
				NumLookaheadPods:       5,
				LookaheadPodPercentage: 20,
				LookaheadPodMilliCPU:   1000,
				MaxLookaheadCPU:        10,
			},
			want: 10,
		},
		{
			desc: "lookaheadPodMilliCpu=0_cpu_fields_ignored",
			tier: Tier{
				NumLookaheadPods:       5,
				LookaheadPodPercentage: 10,
				LookaheadPodMilliCPU:   0,
				MaxLookaheadCPU:        10,
			},
			want: 5,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			got := getLookaheadPodNumber(tc.targetNodesCPUs, tc.tier)
			assert.Equal(t, tc.want, got)
		})
	}
}

// mockStrategyProvider is a mock implementation of StrategyProvider.
type mockStrategyProvider struct {
	mock.Mock
}

func (m *mockStrategyProvider) SetEkResizingEnabled(bool) {}

func (m *mockStrategyProvider) RefreshStrategy() {}

func (m *mockStrategyProvider) Strategy() (LookaheadPodStrategy, error) {
	args := m.Called()
	return args.Get(0).(LookaheadPodStrategy), args.Error(1)
}
