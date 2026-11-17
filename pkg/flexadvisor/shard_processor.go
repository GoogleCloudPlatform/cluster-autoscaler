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

package flexadvisor

import (
	"sync"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	"k8s.io/klog/v2"
)

type shardProcessorMetrics interface {
	UpdateFlexAdvisorRejectedScopes(rejected int)
}

var _ ScaleUpEndProcessor = (*ShardProcessor)(nil)

// ShardProcessor processes incoming pod shards selected by LruPodShardSelector.
// In shard-based scheduling mode, it registers and activates fast polling for
// the active shard (shards[0]) before scale-up, and remembers the immediate
// next shard's scope key (or "" if shards[1] uses no FlexAdvisor scope) so
// OnScaleUpEnd can lazily prefetch it in slow polling mode once the active
// scale-up pass completes.
type ShardProcessor struct {
	provider           instanceavailability.Provider
	cccLister          lister.Lister
	experimentsManager experiments.Manager
	metrics            shardProcessorMetrics

	mu             sync.Mutex
	nextScopeKey   string
	rejectedInLoop int
}

type shardProcessorOption func(*ShardProcessor)

func withShardProcessorMetrics(m shardProcessorMetrics) shardProcessorOption {
	return func(p *ShardProcessor) {
		p.metrics = m
	}
}

// NewShardProcessor returns an instance of ShardProcessor for Flex Advisor.
func NewShardProcessor(
	provider instanceavailability.Provider,
	cccLister lister.Lister,
	experimentsManager experiments.Manager,
	opts ...shardProcessorOption,
) *ShardProcessor {
	p := &ShardProcessor{
		provider:           provider,
		cccLister:          cccLister,
		experimentsManager: experimentsManager,
		metrics:            metrics.Metrics,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// ShouldProcess returns whether ProcessIncomingShards should be invoked during shard selection.
func (p *ShardProcessor) ShouldProcess() bool {
	return p != nil &&
		p.provider != nil &&
		IsFlexAdvisorProcessingEnabled(p.experimentsManager) &&
		IsFlexAdvisorShardBasedScheduling(p.experimentsManager)
}

// ProcessIncomingShards registers the active shard's flexibility scope
// (shards[0]) with the active TTL and activates fast polling for it, and stores
// the immediate next shard's flexibility scope (shards[1], or "" if it has none)
// to be lazily prefetched by OnScaleUpEnd after the active scale-up finishes.
func (p *ShardProcessor) ProcessIncomingShards(shards []*podsharding.PodShard) {
	if p == nil {
		return
	}
	if !p.ShouldProcess() {
		p.setPendingPrefetch("", 0)
		return
	}
	if len(shards) == 0 {
		p.setPendingPrefetch("", 0)
		if p.metrics != nil {
			p.metrics.UpdateFlexAdvisorRejectedScopes(0)
		}
		return
	}

	activeKey, activeFound := flexibilityScopeKeyFromShard(shards[0], p.cccLister)
	failures := 0
	if activeFound {
		if err := p.provider.RegisterFlexibilityScope(activeKey); err != nil {
			klog.Warningf("FlexAdvisor: failed to register flexibility scope for incoming shard, flexibilityScopeKey=%v, err=%v", activeKey, err)
			failures = 1
		} else {
			klog.Infof("FlexAdvisor: activating fast polling based on incoming shard flexibilityScopeKey=%v", activeKey)
			p.provider.ActivateFastPolling(activeKey)
		}
	}
	if p.metrics != nil {
		p.metrics.UpdateFlexAdvisorRejectedScopes(failures)
	}

	// Only inspect the immediate next shard (shards[1]). If it uses no
	// FlexAdvisor scope (e.g. ProvisioningRequest or VM-billed workload), store
	// "" rather than skipping ahead to shards[2], so a later scope does not poll
	// unnecessarily throughout that next loop.
	nextKey := ""
	if len(shards) > 1 && isFlexAdvisorShardBasedSchedulingPrefetchEnabled(p.experimentsManager) {
		if candidate, found := flexibilityScopeKeyFromShard(shards[1], p.cccLister); found && candidate != activeKey {
			nextKey = candidate
		}
	}
	p.setPendingPrefetch(nextKey, failures)
}

// OnScaleUpEnd prefetches the immediate next shard's flexibility scope (if any)
// once in slow polling mode after the active scale-up pass finishes, allowing
// its initial fetch to overlap scale-down and the next scan interval without
// polling during a long active loop. Consumed at most once per
// ProcessIncomingShards call so Salvo sub-iterations for the same active shard
// only trigger a single prefetch.
func (p *ShardProcessor) OnScaleUpEnd() {
	if p == nil {
		return
	}
	nextKey, rejected := p.takePendingPrefetch()
	if nextKey == "" || !p.ShouldProcess() || !isFlexAdvisorShardBasedSchedulingPrefetchEnabled(p.experimentsManager) {
		return
	}
	// TODO(b/590053313): Register prefetched scopes with cooldownScopeTtl instead of activeScopeTtl.
	if err := p.provider.RegisterFlexibilityScope(nextKey); err != nil {
		klog.Warningf("FlexAdvisor: failed to prefetch next shard flexibility scope, flexibilityScopeKey=%v, err=%v", nextKey, err)
		rejected++
	}
	if p.metrics != nil {
		p.metrics.UpdateFlexAdvisorRejectedScopes(rejected)
	}
}

func (p *ShardProcessor) setPendingPrefetch(key string, rejectedInLoop int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextScopeKey = key
	p.rejectedInLoop = rejectedInLoop
}

func (p *ShardProcessor) takePendingPrefetch() (string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key, rejected := p.nextScopeKey, p.rejectedInLoop
	p.nextScopeKey = ""
	p.rejectedInLoop = 0
	return key, rejected
}

// flexibilityScopeKeyFromShard derives the FlexAdvisor flexibility scope key for a pod shard:
//  1. An explicit ComputeClass label maps to the CCC name.
//  2. Shards without an explicit selector map to the default CCC when enabled.
//  3. Shards tagged with HasPayPerPodWorkloads map to the synthetic
//     "general-purpose" scope (Autopilot mode and experiment flags are already
//     checked when setting HasPayPerPodWorkloads during pod sharding).
//
// Provisioning Request shards are skipped.
func flexibilityScopeKeyFromShard(shard *podsharding.PodShard, cccLister lister.Lister) (string, bool) {
	if shard == nil || shard.NodeGroupDescriptor.ProvisioningClassName != "" {
		return "", false
	}
	if cccName := shard.NodeGroupDescriptor.SystemLabels[labels.ComputeClassLabel]; cccName != "" {
		return cccName, true
	}
	if cccLister != nil {
		if _, defaultName, _ := cccLister.PodCrd(&apiv1.Pod{}); defaultName != "" {
			return defaultName, true
		}
	}
	if shard.HasPayPerPodWorkloads {
		return ccc.AutopilotPodFamilyCrdName, true
	}
	return "", false
}
