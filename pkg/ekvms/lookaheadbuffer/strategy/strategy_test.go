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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		desc    string
		jsonStr string
		wantLps LookaheadPodStrategy
		wantErr bool
	}{
		{
			desc:    "empty string",
			jsonStr: "",
			wantLps: LookaheadPodStrategy{
				Status: Unspecified,
			},
			wantErr: false,
		},
		{
			desc:    "empty json",
			jsonStr: "{}",
			wantLps: LookaheadPodStrategy{
				Status: Unspecified,
			},
			wantErr: false,
		},
		{
			desc:    "invalid string",
			jsonStr: "this-is-invalid-yaml-string",
			wantLps: LookaheadPodStrategy{},
			wantErr: true,
		},
		{
			desc: "missing status and version",
			jsonStr: `
{
  "tieredStrategy": {
    "tiers": [
      {
        "numLookaheadPods": 1,
        "lookaheadPodMilliCpu": 8000,
        "lookaheadPodMemKib": 33554432,
        "minTargetNodesCpu": 0
      }
    ]
  }
}
`,
			wantLps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * size.GiB / size.KiB,
							MinTargetNodesCPU:    0,
						},
					},
				},
				Status: Unspecified,
			},
			wantErr: false,
		},
		{
			desc:    "status set to disabled",
			jsonStr: `{"status": "STATUS_DISABLED"}`,
			wantLps: LookaheadPodStrategy{
				Status: Disabled,
			},
			wantErr: false,
		},
		{
			desc:    "status set to unspecified",
			jsonStr: `{"status": "STATUS_UNSPECIFIED"}`,
			wantLps: LookaheadPodStrategy{
				Status: Unspecified,
			},
			wantErr: false,
		},
		{
			desc: "sorts two tiers by vcpu",
			jsonStr: `
{
  "tieredStrategy": {
    "tiers": [
      {
        "numLookaheadPods": 1,
        "lookaheadPodMilliCpu": 8000,
        "lookaheadPodMemKib": 33554432,
        "minTargetNodesCpu": 0
      },
      {
        "numLookaheadPods": 2,
        "lookaheadPodPercentage": 10,
        "maxLookaheadCpu": 640,
        "lookaheadPodMilliCpu": 32000,
        "lookaheadPodMemKib": 134217728,
        "minTargetNodesCpu": 800
      },
      {
        "numLookaheadPods": 1,
        "lookaheadPodMilliCpu": 32000,
        "lookaheadPodMemKib": 134217728,
        "minTargetNodesCpu": 400
      }
    ]
  },
  "minCaVersion": "v1.2.3",
  "status": "STATUS_ENABLED"
}
`,
			wantLps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:       2,
							LookaheadPodPercentage: 10,
							MaxLookaheadCPU:        640,
							LookaheadPodMilliCPU:   32000,
							LookaheadPodMemKib:     128 * size.GiB / size.KiB,
							MinTargetNodesCPU:      800,
						},
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 32000,
							LookaheadPodMemKib:   128 * size.GiB / size.KiB,
							MinTargetNodesCPU:    400,
						},
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * size.GiB / size.KiB,
							MinTargetNodesCPU:    0,
						},
					},
				},
				MinCaVersion: "v1.2.3",
				Status:       Enabled,
			},
			wantErr: false,
		},
		{
			desc: "parses new cluster tier separately from sorted tiers",
			jsonStr: `
{
  "tieredStrategy": {
    "newClusterMaxAgeHours": 24,
    "newClusterTier": {
      "numLookaheadPods": 3,
      "lookaheadPodMilliCpu": 6000,
      "lookaheadPodMemKib": 20971520,
      "minTargetNodesCpu": 1
    },
    "tiers": [
      {
        "numLookaheadPods": 0,
        "minTargetNodesCpu": 0
      },
      {
        "numLookaheadPods": 1,
        "lookaheadPodMilliCpu": 4000,
        "lookaheadPodMemKib": 16777216,
        "minTargetNodesCpu": 1
      }
    ]
  },
  "minCaVersion": "v1.2.3",
  "status": "STATUS_ENABLED"
}
`,
			wantLps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 4000,
							LookaheadPodMemKib:   16 * size.GiB / size.KiB,
							MinTargetNodesCPU:    1,
						},
						{
							NumLookaheadPods:  0,
							MinTargetNodesCPU: 0,
						},
					},
					NewClusterTier: &Tier{
						NumLookaheadPods:     3,
						LookaheadPodMilliCPU: 6000,
						LookaheadPodMemKib:   20 * size.GiB / size.KiB,
						MinTargetNodesCPU:    1,
					},
					NewClusterMaxAgeHours: 24,
				},
				MinCaVersion: "v1.2.3",
				Status:       Enabled,
			},
			wantErr: false,
		},
		{
			desc: "status set to enabled with invalid strategy (empty tiers)",
			jsonStr: `
{
  "tieredStrategy": {
    "tiers": []
  },
  "minCaVersion": "v1.2.3",
  "status": "STATUS_ENABLED"
}
`,
			wantLps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{},
				},
				MinCaVersion: "v1.2.3",
				Status:       Enabled,
			},
			wantErr: true,
		},
		{
			desc: "status set to enabled with invalid strategy (nil tiers array)",
			jsonStr: `
{
  "tieredStrategy": {},
  "minCaVersion": "v1.2.3",
  "status": "STATUS_ENABLED"
}
`,
			wantLps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{},
				},
				MinCaVersion: "v1.2.3",
				Status:       Enabled,
			},
			wantErr: true,
		},
		{
			desc: "status set to enabled with invalid strategy (nil tieredStrategy)",
			jsonStr: `
{
  "minCaVersion": "v1.2.3",
  "status": "STATUS_ENABLED"
}
`,
			wantLps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{},
				},
				MinCaVersion: "v1.2.3",
				Status:       Enabled,
			},
			wantErr: true,
		},
		{
			desc: "status set to enabled with newClusterTier but missing newClusterMaxAgeHours",
			jsonStr: `
{
  "tieredStrategy": {
    "newClusterTier": {
      "numLookaheadPods": 3,
      "lookaheadPodMilliCpu": 6000,
      "lookaheadPodMemKib": 20971520,
      "minTargetNodesCpu": 1
    },
    "tiers": [
      {
        "numLookaheadPods": 1,
        "lookaheadPodMilliCpu": 4000,
        "lookaheadPodMemKib": 16777216,
        "minTargetNodesCpu": 1
      }
    ]
  },
  "minCaVersion": "v1.2.3",
  "status": "STATUS_ENABLED"
}
`,
			wantLps: LookaheadPodStrategy{},
			wantErr: true,
		},
		{
			desc: "status set to enabled with newClusterTier and non-positive newClusterMaxAgeHours",
			jsonStr: `
{
  "tieredStrategy": {
    "newClusterMaxAgeHours": 0,
    "newClusterTier": {
      "numLookaheadPods": 3,
      "lookaheadPodMilliCpu": 6000,
      "lookaheadPodMemKib": 20971520,
      "minTargetNodesCpu": 1
    },
    "tiers": [
      {
        "numLookaheadPods": 1,
        "lookaheadPodMilliCpu": 4000,
        "lookaheadPodMemKib": 16777216,
        "minTargetNodesCpu": 1
      }
    ]
  },
  "minCaVersion": "v1.2.3",
  "status": "STATUS_ENABLED"
}
`,
			wantLps: LookaheadPodStrategy{},
			wantErr: true,
		},
		{
			desc: "status set to enabled with newClusterTier and negative newClusterMaxAgeHours",
			jsonStr: `
{
  "tieredStrategy": {
    "newClusterMaxAgeHours": -5,
    "newClusterTier": {
      "numLookaheadPods": 3,
      "lookaheadPodMilliCpu": 6000,
      "lookaheadPodMemKib": 20971520,
      "minTargetNodesCpu": 1
    },
    "tiers": [
      {
        "numLookaheadPods": 1,
        "lookaheadPodMilliCpu": 4000,
        "lookaheadPodMemKib": 16777216,
        "minTargetNodesCpu": 1
      }
    ]
  },
  "minCaVersion": "v1.2.3",
  "status": "STATUS_ENABLED"
}
`,
			wantLps: LookaheadPodStrategy{},
			wantErr: true,
		},
		{
			desc: "status set to enabled with newClusterMaxAgeHours but nil newClusterTier",
			jsonStr: `
{
  "tieredStrategy": {
    "newClusterMaxAgeHours": 24,
    "tiers": [
      {
        "numLookaheadPods": 1,
        "lookaheadPodMilliCpu": 4000,
        "lookaheadPodMemKib": 16777216,
        "minTargetNodesCpu": 1
      }
    ]
  },
  "minCaVersion": "v1.2.3",
  "status": "STATUS_ENABLED"
}
`,
			wantLps: LookaheadPodStrategy{},
			wantErr: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			lps, err := ParsePodStrategy(tc.jsonStr)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantLps, lps)
		})
	}
}

func TestSelectTier(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		lps             LookaheadPodStrategy
		targetNodesCpus int
		wantTier        Tier
		wantErr         bool
	}{
		{
			desc:     "calling SelectTiers on LPS with nil tiered strategy",
			lps:      LookaheadPodStrategy{},
			wantTier: Tier{},
			wantErr:  true,
		},
		{
			desc: "LPS with no tiers defined",
			lps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{},
				},
			},
			wantTier: Tier{},
			wantErr:  true,
		},
		{
			desc: "Selects small tier",
			lps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 32000,
							LookaheadPodMemKib:   128 * size.GiB / size.KiB,
							MinTargetNodesCPU:    400,
						},
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * size.GiB / size.KiB,
							MinTargetNodesCPU:    0,
						},
					},
				},
			},
			targetNodesCpus: 100,
			wantTier: Tier{
				NumLookaheadPods:     1,
				LookaheadPodMilliCPU: 8000,
				LookaheadPodMemKib:   32 * size.GiB / size.KiB,
				MinTargetNodesCPU:    0,
			},
		},
		{
			desc: "Selects large tier",
			lps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 32000,
							LookaheadPodMemKib:   128 * size.GiB / size.KiB,
							MinTargetNodesCPU:    400,
						},
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * size.GiB / size.KiB,
							MinTargetNodesCPU:    0,
						},
					},
				},
			},
			targetNodesCpus: 500,
			wantTier: Tier{
				NumLookaheadPods:     1,
				LookaheadPodMilliCPU: 32000,
				LookaheadPodMemKib:   128 * size.GiB / size.KiB,
				MinTargetNodesCPU:    400,
			},
		},
		{
			desc: "no matching tier",
			lps: LookaheadPodStrategy{
				TieredStrategy: &TieredStrategy{
					Tiers: []Tier{
						{
							NumLookaheadPods:     1,
							LookaheadPodMilliCPU: 8000,
							LookaheadPodMemKib:   32 * size.GiB / size.KiB,
							MinTargetNodesCPU:    100, // No strategy is defined for clusters with fewer than 100 cpus.
						},
					},
				},
			},
			targetNodesCpus: 0,
			wantTier:        Tier{},
			wantErr:         true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tier, err := tc.lps.TieredStrategy.GetTier(tc.targetNodesCpus, 0)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantTier, tier)
		})
	}
}

func TestGetTierNewClusterTier(t *testing.T) {
	newClusterTier := Tier{
		NumLookaheadPods:     3,
		LookaheadPodMilliCPU: 6000,
		LookaheadPodMemKib:   20 * size.GiB / size.KiB,
		MinTargetNodesCPU:    1,
	}
	regularTier := Tier{
		NumLookaheadPods:     1,
		LookaheadPodMilliCPU: 4000,
		LookaheadPodMemKib:   16 * size.GiB / size.KiB,
		MinTargetNodesCPU:    1,
	}
	emptyTier := Tier{NumLookaheadPods: 0, MinTargetNodesCPU: 0}
	tiers := []Tier{regularTier, emptyTier}
	tieredStrategy := &TieredStrategy{
		Tiers:                 tiers,
		NewClusterTier:        &newClusterTier,
		NewClusterMaxAgeHours: 24,
	}

	for _, tc := range []struct {
		desc            string
		tieredStrategy  *TieredStrategy
		clusterAge      time.Duration
		targetNodesCPUs int
		want            Tier
	}{
		{
			desc:            "new cluster uses new cluster tier",
			tieredStrategy:  tieredStrategy,
			clusterAge:      time.Hour,
			targetNodesCPUs: 2,
			want:            newClusterTier,
		},
		{
			desc:            "negative cluster age due to clock skew uses new cluster tier",
			tieredStrategy:  tieredStrategy,
			clusterAge:      -2 * time.Minute,
			targetNodesCPUs: 2,
			want:            newClusterTier,
		},
		{
			desc:            "new cluster at new cluster tier min target nodes cpu uses new cluster tier",
			tieredStrategy:  tieredStrategy,
			clusterAge:      time.Hour,
			targetNodesCPUs: 1,
			want:            newClusterTier,
		},
		{
			desc:            "new cluster below new cluster tier min target nodes cpu uses regular tiers",
			tieredStrategy:  tieredStrategy,
			clusterAge:      time.Hour,
			targetNodesCPUs: 0,
			want:            emptyTier,
		},
		{
			desc:            "cluster exactly at max age uses regular tiers",
			tieredStrategy:  tieredStrategy,
			clusterAge:      24 * time.Hour,
			targetNodesCPUs: 2,
			want:            regularTier,
		},
		{
			desc:            "mature cluster uses regular tiers",
			tieredStrategy:  tieredStrategy,
			clusterAge:      48 * time.Hour,
			targetNodesCPUs: 2,
			want:            regularTier,
		},
		{
			desc:            "mature cluster at regular tier min target nodes cpu uses regular tiers",
			tieredStrategy:  tieredStrategy,
			clusterAge:      48 * time.Hour,
			targetNodesCPUs: 1,
			want:            regularTier,
		},
		{
			desc:            "new cluster without new cluster tier uses regular tiers",
			tieredStrategy:  &TieredStrategy{Tiers: tiers},
			clusterAge:      time.Hour,
			targetNodesCPUs: 2,
			want:            regularTier,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := tc.tieredStrategy.GetTier(tc.targetNodesCPUs, tc.clusterAge)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestHasTieredStrategy(t *testing.T) {
	for _, tc := range []struct {
		desc string
		lps  *LookaheadPodStrategy
		want bool
	}{
		{
			desc: "nil strategy",
			lps:  nil,
			want: false,
		},
		{
			desc: "no tiered strategy",
			lps:  &LookaheadPodStrategy{},
			want: false,
		},
		{
			desc: "has tiered strategy",
			lps:  &LookaheadPodStrategy{TieredStrategy: &TieredStrategy{}},
			want: true,
		},
	} {

		t.Run(tc.desc, func(t *testing.T) {
			got := tc.lps.HasTieredStrategy()
			assert.Equal(t, tc.want, got)
		})
	}
}
