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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	"k8s.io/klog/v2"
)

// ShardProcessor processes incoming pod shards selected by LruPodShardSelector.
// When adaptive polling is enabled, it restores the active shard's flexibility scope to normal polling mode.
type ShardProcessor struct {
	provider           instanceavailability.Provider
	cccLister          lister.Lister
	experimentsManager experiments.Manager
}

// NewShardProcessor returns an instance of ShardProcessor for Flex Advisor.
func NewShardProcessor(
	provider instanceavailability.Provider,
	cccLister lister.Lister,
	experimentsManager experiments.Manager,
) *ShardProcessor {
	return &ShardProcessor{
		provider:           provider,
		cccLister:          cccLister,
		experimentsManager: experimentsManager,
	}
}

// ShouldProcess reports whether incoming shard notifications should be processed.
func (p *ShardProcessor) ShouldProcess() bool {
	return p != nil && p.provider != nil && IsFlexAdvisorAdaptivePollingEnabled(p.experimentsManager)
}

// ProcessIncomingShards moves the active shard's flexibility scope (shards[0]) back to normal polling mode
// when adaptive polling is enabled.
func (p *ShardProcessor) ProcessIncomingShards(shards []*podsharding.PodShard) {
	if !p.ShouldProcess() {
		klog.Infof("FlexAdvisor: not setting polling mode based on incoming shard, FlexAdvisor::AdaptivePollingEnabled not enabled")
		return
	}
	if len(shards) == 0 {
		klog.Infof("FlexAdvisor: not setting polling mode based on incoming shard, len(shards) == 0")
		return
	}
	if key, found := flexibilityScopeKeyFromShard(shards[0], p.cccLister); found {
		klog.Infof("FlexAdvisor: setting polling mode based on incoming shard flexibilityScopeKey=%v", key)
		p.provider.SetPollingMode(key, PollingModeNormal)
	}
}

// flexibilityScopeKeyFromShard derives the CCC flexibility scope key for a pod shard:
// an explicit CCC selector maps to the CCC name, pods without a selector map to the default CCC when it is enabled.
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
	return "", false
}
