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
	"context"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
)

// ScaleUpStatusProcessor deactivates fast polling (switching to slow polling
// and cooldown TTL) for the selected pod shard's scope in shard-based mode when
// scale-up was skipped before WrapperOrchestrator.ScaleUp.
type ScaleUpStatusProcessor struct {
	provider           instanceavailability.Provider
	cccLister          lister.Lister
	experimentsManager experiments.Manager
}

// NewScaleUpStatusProcessor returns an instance of ScaleUpStatusProcessor for Flex Advisor.
func NewScaleUpStatusProcessor(
	provider instanceavailability.Provider,
	cccLister lister.Lister,
	experimentsManager experiments.Manager,
) *ScaleUpStatusProcessor {
	return &ScaleUpStatusProcessor{
		provider:           provider,
		cccLister:          cccLister,
		experimentsManager: experimentsManager,
	}
}

// Process deactivates fast polling for the selected pod shard's scope when
// scale-up was skipped before entering WrapperOrchestrator.ScaleUp.
func (p *ScaleUpStatusProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, scaleUpStatus *status.ScaleUpStatus) {
	if p.provider == nil || autoscalingCtx == nil || autoscalingCtx.ProcessorCallbacks == nil || scaleUpStatus == nil {
		return
	}
	if !IsFlexAdvisorProcessingEnabled(p.experimentsManager) || !IsFlexAdvisorShardBasedScheduling(p.experimentsManager) {
		return
	}
	// Deactivate fast polling when pod sharder selected a shard, but
	// WrapperOrchestrator.ScaleUp was never called due to shouldScaleUp()
	// returning false (e.g. ScaleUpLimitedByMaxNodesTotal, ScaleUpInCooldown,
	// ScaleUpNotNeeded, ScaleUpNotTried). Guard on Result first so async
	// HTNAP or DWS InitializeNodeGroup callbacks do not touch the
	// unsynchronized ProcessorCallbacks map.
	if scaleUpStatus.Result != status.ScaleUpSuccessful && scaleUpStatus.Result != status.ScaleUpError && scaleUpStatus.Result != status.ScaleUpNoOptionsAvailable {
		shard := podsharding.GetSelectedPodShard(autoscalingCtx)
		if key, found := flexibilityScopeKeyFromShard(shard, p.cccLister); found {
			p.provider.DeactivateFastPolling(key)
		}
	}
}

// CleanUp is a no-op.
func (p *ScaleUpStatusProcessor) CleanUp() {}
