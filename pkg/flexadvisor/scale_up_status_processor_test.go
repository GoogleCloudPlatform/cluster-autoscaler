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
	"testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/callbacks"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
)

func TestScaleUpStatusProcessor_Process(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		cccLister          lister.Lister
		nilProvider        bool
		nilAutoscalingCtx  bool
		nilCallbacks       bool
		scaleUpStatus      *status.ScaleUpStatus
		selectedShard      *podsharding.PodShard
		wantSlowPollScope  string
	}{
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with explicit CCC shard - sets PollingModeSlow",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:               "ScaleUpInCooldown with explicit CCC shard - sets PollingModeSlow",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpInCooldown},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:               "ScaleUpNotNeeded with explicit CCC shard - sets PollingModeSlow",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpNotNeeded},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:               "ScaleUpNotTried with explicit CCC shard - sets PollingModeSlow",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpNotTried},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:               "ScaleUpSuccessful - does not call SetPollingMode (handled by WrapperOrchestrator)",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpSuccessful},
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "ScaleUpError - does not call SetPollingMode (handled by WrapperOrchestrator / ignores async HTNAP errors)",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpError},
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "ScaleUpNoOptionsAvailable - does not call SetPollingMode (handled by WrapperOrchestrator)",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable},
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with plain shard and default CCC enabled - sets PollingModeSlow for default scope",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestListerWithDefault(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      plainShard("a"),
			wantSlowPollScope:  labels.DefaultCCCName,
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with plain shard without default CCC - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      plainShard("a"),
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with provisioning request shard - skipped without calling SetPollingMode",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      provReqShard("ccc-pr"),
		},
		{
			name:               "ScaleUpLimitedByMaxNodesTotal with no selected shard in context - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      nil,
		},
		{
			name: "adaptive polling disabled via bool flag - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag: false,
			}, nil),
			cccLister:     mockShardTestLister(),
			scaleUpStatus: &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard: cccShard("ccc-1"),
		},
		{
			name: "adaptive polling disabled via min CA version flag - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: false,
			}, nil),
			cccLister:     mockShardTestLister(),
			scaleUpStatus: &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard: cccShard("ccc-1"),
		},
		{
			name: "FlexAdvisorProcessing disabled - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			}, nil),
			cccLister:     mockShardTestLister(),
			scaleUpStatus: &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard: cccShard("ccc-1"),
		},
		{
			name:               "nil experiments manager - defaults to enabled and sets PollingModeSlow",
			experimentsManager: nil,
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
			wantSlowPollScope:  "ccc-1",
		},
		{
			name:               "nil provider - does not panic",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			nilProvider:        true,
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "nil autoscalingCtx - does not panic or call SetPollingMode",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			nilAutoscalingCtx:  true,
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "nil ProcessorCallbacks on autoscalingCtx - does not panic or call SetPollingMode",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			nilCallbacks:       true,
			scaleUpStatus:      &status.ScaleUpStatus{Result: status.ScaleUpLimitedByMaxNodesTotal},
			selectedShard:      cccShard("ccc-1"),
		},
		{
			name:               "nil scaleUpStatus - does not panic or call SetPollingMode",
			experimentsManager: experiments.NewMockManager(),
			cccLister:          mockShardTestLister(),
			scaleUpStatus:      nil,
			selectedShard:      cccShard("ccc-1"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var provider instanceavailability.Provider
			mockProvider := &instanceavailability.MockProvider{}
			if !tc.nilProvider {
				if tc.wantSlowPollScope != "" {
					mockProvider.On("SetPollingMode", tc.wantSlowPollScope, PollingModeSlow).Return().Once()
				}
				provider = mockProvider
			}

			var autoscalingCtx *ca_context.AutoscalingContext
			if !tc.nilAutoscalingCtx {
				autoscalingCtx = &ca_context.AutoscalingContext{}
				if !tc.nilCallbacks {
					autoscalingCtx.ProcessorCallbacks = callbacks.NewTestProcessorCallbacks()
					if tc.selectedShard != nil {
						podsharding.TestSetPodShard(autoscalingCtx, tc.selectedShard)
					}
				}
			}

			processor := NewScaleUpStatusProcessor(provider, tc.cccLister, tc.experimentsManager)
			processor.Process(context.Background(), autoscalingCtx, tc.scaleUpStatus)
			processor.CleanUp()

			mockProvider.AssertExpectations(t)
		})
	}
}
