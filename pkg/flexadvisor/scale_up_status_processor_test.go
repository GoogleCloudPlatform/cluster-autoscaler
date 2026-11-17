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
	"testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/callbacks"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
)

type recordingCallbacks struct {
	callbacks.ProcessorCallbacks
	extraValueReads int
}

func (c *recordingCallbacks) GetExtraValue(key string) (any, bool) {
	c.extraValueReads++
	return c.ProcessorCallbacks.GetExtraValue(key)
}

func TestScaleUpStatusProcessor_Process(t *testing.T) {
	shardBasedManager := func() experiments.Manager {
		return experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
			experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeShardBased),
		})
	}

	taggedProvReqShard := provReqShard("")
	taggedProvReqShard.HasPayPerPodWorkloads = true

	testCases := []struct {
		name                string
		experimentsManager  experiments.Manager
		cccLister           lister.Lister
		nilProvider         bool
		nilAutoscalingCtx   bool
		nilCallbacks        bool
		forbidCallbackReads bool
		scaleUpStatus       *status.ScaleUpStatus
		selectedShard       *podsharding.PodShard
		wantSlowPollScope   string
	}{
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with explicit CCC shard in shard-based mode - calls DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:               "ScaleUpInCooldown with explicit CCC shard in shard-based mode - calls DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpInCooldown},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:               "ScaleUpNotNeeded with explicit CCC shard in shard-based mode - calls DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:               "ScaleUpNotTried with explicit CCC shard in shard-based mode - calls DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpNotTried},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:                "ScaleUpSuccessful - does not call DeactivateFastPolling or read ProcessorCallbacks",
			experimentsManager:  shardBasedManager(),
			cccLister:           mockShardTestLister(),
			forbidCallbackReads: true,
			scaleUpStatus:       &status.ScaleUpStatus{Result: status.ScaleUpSuccessful},
			selectedShard:       cccShard("ccc-1"),
		},
		{
			name:                "ScaleUpError - does not call DeactivateFastPolling or read ProcessorCallbacks",
			experimentsManager:  shardBasedManager(),
			cccLister:           mockShardTestLister(),
			forbidCallbackReads: true,
			scaleUpStatus:       &status.ScaleUpStatus{Result: status.ScaleUpError},
			selectedShard:       cccShard("ccc-1"),
		},
		{
			name:                "ScaleUpNoOptionsAvailable - does not call DeactivateFastPolling or read ProcessorCallbacks",
			experimentsManager:  shardBasedManager(),
			cccLister:           mockShardTestLister(),
			forbidCallbackReads: true,
			scaleUpStatus:       &status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable},
			selectedShard:       cccShard("ccc-1"),
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with plain shard and default CCC enabled in shard-based mode - calls DeactivateFastPolling for default scope",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestListerWithDefault(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      plainShard("a"),
			wantSlowPollScope:  labels.DefaultCCCName,
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with plain shard without default CCC - does not call DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      plainShard("a"),
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with provisioning request shard - skipped without calling DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      provReqShard("ccc-pr"),
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with no selected shard in context - does not call DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      nil,
		},
		{
			name:               "default scheduling mode (shard-based) - calls DeactivateFastPolling",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name: "late scheduling mode - does not call DeactivateFastPolling",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeLate),
			}),
			cccLister:     mockShardTestLister(),
			scaleUpStatus: &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard: cccShard("ccc-1"),
		},
		{
			name: "classic scheduling mode - does not call DeactivateFastPolling",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeClassic),
			}),
			cccLister:     mockShardTestLister(),
			scaleUpStatus: &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard: cccShard("ccc-1"),
		},
		{
			name: "FlexAdvisorProcessing disabled - does not call DeactivateFastPolling",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			}, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeShardBased),
			}),
			cccLister:     mockShardTestLister(),
			scaleUpStatus: &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard: cccShard("ccc-1"),
		},
		{
			name:               "nil experiments manager (defaults to shard-based mode) - calls DeactivateFastPolling",
			experimentsManager: nil,
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:               "nil provider - does not panic",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			nilProvider:        true,
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "nil autoscalingCtx - does not panic or call DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			nilAutoscalingCtx:  true,
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "nil ProcessorCallbacks on autoscalingCtx - does not panic or call DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			nilCallbacks:       true,
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "nil scaleUpStatus - does not panic or call DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      nil,
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with tagged pay-per-pod shard in shard-based mode - calls DeactivateFastPolling for general-purpose",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      payPerPodShard("a"),
			wantSlowPollScope:  ccc.AutopilotPodFamilyCrdName,
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with untagged plain shard (VM-billed) - does not call DeactivateFastPolling",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      plainShard("vm-billed"),
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with tagged shard and default CCC enabled - deactivates default CCC",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestListerWithDefault(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      payPerPodShard("a"),
			wantSlowPollScope:  labels.DefaultCCCName,
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with tagged ProvisioningRequest shard - skipped",
			experimentsManager: shardBasedManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      taggedProvReqShard,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var provider instanceavailability.Provider
			mockProvider := &instanceavailability.MockProvider{}
			if !tc.nilProvider {
				if tc.wantSlowPollScope != "" {
					mockProvider.On("DeactivateFastPolling", tc.wantSlowPollScope).Return().Once()
				}
				provider = mockProvider
			}

			var autoscalingCtx *ca_context.AutoscalingContext
			var recCallbacks *recordingCallbacks
			if !tc.nilAutoscalingCtx {
				autoscalingCtx = &ca_context.AutoscalingContext{}
				if !tc.nilCallbacks {
					autoscalingCtx.ProcessorCallbacks = callbacks.NewTestProcessorCallbacks()
					if tc.selectedShard != nil {
						podsharding.TestSetPodShard(autoscalingCtx, tc.selectedShard)
					}
					if tc.forbidCallbackReads {
						recCallbacks = &recordingCallbacks{ProcessorCallbacks: autoscalingCtx.ProcessorCallbacks}
						autoscalingCtx.ProcessorCallbacks = recCallbacks
					}
				}
			}

			processor := NewScaleUpStatusProcessor(provider, tc.cccLister, tc.experimentsManager)
			processor.Process(t.Context(), autoscalingCtx, tc.scaleUpStatus)
			processor.CleanUp()

			if recCallbacks != nil && recCallbacks.extraValueReads != 0 {
				t.Errorf("processor.Process(ctx, autoscalingCtx, %+v) called ProcessorCallbacks.GetExtraValue %d times, want 0", tc.scaleUpStatus, recCallbacks.extraValueReads)
			}
			mockProvider.AssertExpectations(t)
		})
	}
}
