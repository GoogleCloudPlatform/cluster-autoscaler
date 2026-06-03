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
	"math"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	klog "k8s.io/klog/v2"
)

type PodProvider interface {
	GetLookaheadPods(targetNodesCPU int, workloadIDHash string) []*apiv1.Pod
}

type podProviderImpl struct {
	strategyProvider StrategyProvider
}

func NewPodProvider(strategyProvider StrategyProvider) *podProviderImpl {
	return &podProviderImpl{
		strategyProvider: strategyProvider,
	}
}

func (p *podProviderImpl) GetLookaheadPods(targetNodesCPU int, workloadIDHash string) []*apiv1.Pod {
	if p == nil {
		return nil
	}
	laStrategy, err := p.strategyProvider.Strategy()
	if err != nil {
		klog.Errorf("Error during getting lookahead pods, assuming no lookahead pods: %v", err)
		return nil
	}
	if laStrategy.Status != Enabled {
		klog.V(4).Infof("Skipping lookahead pods as status is %q", laStrategy.Status)
		return nil
	}
	if !laStrategy.HasTieredStrategy() {
		klog.Warning("Lookahead is enabled but no tiered strategy is defined. Skipping lookahead pods")
		return nil
	}
	tier, err := laStrategy.TieredStrategy.GetTier(targetNodesCPU)
	if err != nil {
		klog.Errorf("Error during getting tier for lookahead pods tieredStrategy, assuming no lookahead pods: %v", err)
		return nil
	}
	cpu := *resource.NewMilliQuantity(int64(tier.LookaheadPodMilliCPU), resource.DecimalSI)
	memory := *resource.NewQuantity(int64(tier.LookaheadPodMemKib*size.KiB), resource.BinarySI)
	return GenerateLookaheadPods(getLookaheadPodNumber(targetNodesCPU, tier), cpu, memory, workloadIDHash)
}

func getLookaheadPodNumber(targetNodesCPU int, tier Tier) int {
	if tier.LookaheadPodMilliCPU == 0 {
		klog.Warning("LookaheadPodMilliCpu is 0, cannot calculate lookahead pod number from CPU-based settings. Returning a fixed number of lookahead pods")
		return tier.NumLookaheadPods
	}

	calculateLAPodNumberFromCPU := func(cpu float64) int {
		return int(math.Floor(cpu * 1000 / float64(tier.LookaheadPodMilliCPU)))
	}

	laCPU := float64(targetNodesCPU*tier.LookaheadPodPercentage) / 100
	laPodNumber := calculateLAPodNumberFromCPU(laCPU) + tier.NumLookaheadPods
	if tier.MaxLookaheadCPU == 0 {
		return laPodNumber
	}
	maxLaPodNumber := calculateLAPodNumberFromCPU(float64(tier.MaxLookaheadCPU))
	return min(laPodNumber, maxLaPodNumber)
}
