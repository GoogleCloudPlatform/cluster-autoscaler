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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

type Status string

const (
	Unspecified Status = "STATUS_UNSPECIFIED"
	Enabled     Status = "STATUS_ENABLED"
	Disabled    Status = "STATUS_DISABLED"
)

type TieredStrategy struct {
	Tiers []Tier `json:"tiers"`
}

type Tier struct {
	NumLookaheadPods       int `json:"numLookaheadPods"`
	LookaheadPodPercentage int `json:"lookaheadPodPercentage"`
	MaxLookaheadCPU        int `json:"maxLookaheadCpu"`
	LookaheadPodMilliCPU   int `json:"lookaheadPodMilliCpu"`
	LookaheadPodMemKib     int `json:"lookaheadPodMemKib"`
	MinTargetNodesCPU      int `json:"minTargetNodesCpu"`
}

type LookaheadPodStrategy struct {
	Status         Status          `json:"status,omitempty"`
	MinCaVersion   string          `json:"minCaVersion,omitempty"`
	TieredStrategy *TieredStrategy `json:"tieredStrategy"`
}

// ParsePodStrategy reads YAML configuration from a string
func ParsePodStrategy(s string) (LookaheadPodStrategy, error) {
	lps := LookaheadPodStrategy{}

	// when experiment is enabled but filtered out by some criteria empty string is passed
	// it can cause parsing errors
	if len(s) == 0 {
		s = "{}"
	}

	err := json.Unmarshal([]byte(s), &lps)
	if err != nil {
		return lps, fmt.Errorf("parsing LookaheadPodStrategy from a JSON string failed: %v", err)
	}

	if lps.TieredStrategy != nil {
		// Sort tiers in descending order.
		sort.Slice(lps.TieredStrategy.Tiers, func(i, j int) bool {
			return lps.TieredStrategy.Tiers[i].MinTargetNodesCPU > lps.TieredStrategy.Tiers[j].MinTargetNodesCPU
		})
	}

	if lps.Status == "" {
		lps.Status = Unspecified
	}

	if err := lps.validate(); err != nil {
		return lps, fmt.Errorf("LookaheadPodStrategy is not valid: %v", err)
	}

	return lps, nil
}

func (lps *LookaheadPodStrategy) validate() error {
	if lps.Status != Enabled {
		return nil
	}
	if lps.TieredStrategy == nil {
		return errors.New("tiered strategy found nil")
	}
	if len(lps.TieredStrategy.Tiers) == 0 {
		return errors.New("tiered strategy defines no tiers")
	}
	return nil
}

// HasTieredStrategy returns if LPS uses a tiered strategy. Safe to call on nil LPS.
func (lps *LookaheadPodStrategy) HasTieredStrategy() bool {
	if lps == nil {
		return false
	}
	if lps.TieredStrategy != nil {
		return true
	}
	return false
}

// GetTier returns the largest tier whose MinTargetNodesCpu is smaller than or equal to totalCores. It's assumed that tiers are sorted in descending order.
func (ts *TieredStrategy) GetTier(totalCores int) (Tier, error) {
	if ts == nil {
		return Tier{}, errors.New("TieredStrategy is nil. Use HasTieredStrategy before calling SelectTier")
	}
	for _, tier := range ts.Tiers {
		if totalCores >= tier.MinTargetNodesCPU {
			return tier, nil
		}
	}
	return Tier{}, fmt.Errorf("no matching tier found for %d cores. The tier configuration is invalid", totalCores)
}
