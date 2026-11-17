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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	"k8s.io/klog/v2"
)

// ShardProcessor processes incoming pod shards selected by LruPodShardSelector.
// When adaptive polling is enabled, it activates fast polling for the active
// shard's flexibility scope.
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

// ProcessIncomingShards activates fast polling for the active shard's flexibility scope (shards[0])
// when adaptive polling is enabled.
func (p *ShardProcessor) ProcessIncomingShards(shards []*podsharding.PodShard) {
	if !p.ShouldProcess() || len(shards) == 0 {
		return
	}
	if key, found := flexibilityScopeKeyFromShard(shards[0], p.cccLister); found {
		klog.Infof("FlexAdvisor: activating fast polling based on incoming shard flexibilityScopeKey=%v", key)
		p.provider.ActivateFastPolling(key)
	}
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
