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
	"fmt"
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
)

func cccShard(cccName string) *podsharding.PodShard {
	return &podsharding.PodShard{
		NodeGroupDescriptor: podsharding.NodeGroupDescriptor{
			SystemLabels: map[string]string{
				labels.ComputeClassLabel: cccName,
			},
		},
	}
}

func plainShard(label string) *podsharding.PodShard {
	return &podsharding.PodShard{
		NodeGroupDescriptor: podsharding.NodeGroupDescriptor{
			Labels: map[string]string{"app": label},
		},
	}
}

func payPerPodShard(label string) *podsharding.PodShard {
	shard := plainShard(label)
	shard.HasPayPerPodWorkloads = true
	return shard
}

func provReqShard(cccName string) *podsharding.PodShard {
	return &podsharding.PodShard{
		NodeGroupDescriptor: podsharding.NodeGroupDescriptor{
			ProvisioningClassName: "queued-provisioning.gke.io",
			SystemLabels: map[string]string{
				labels.ComputeClassLabel: cccName,
			},
		},
	}
}

func mockShardTestLister() *lister.MockCrdLister {
	return lister.NewMockCrdListerWithLabel(nil, labels.ComputeClassLabel)
}

func mockShardTestListerWithDefault() *lister.MockCrdLister {
	mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{
		ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: labels.DefaultCCCName}}, "", false, nil, nil),
	}, labels.ComputeClassLabel)
	mockLister.SetDefaultCrdName(labels.DefaultCCCName)
	return mockLister
}

func TestShardProcessor_ShouldProcess(t *testing.T) {
	testCases := []struct {
		name               string
		nilProcessor       bool
		nilProvider        bool
		experimentsManager experiments.Manager
		want               bool
	}{
		{
			name:         "nil ShardProcessor receiver - returns false without panicking",
			nilProcessor: true,
			want:         false,
		},
		{
			name:        "nil provider - returns false",
			nilProvider: true,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeShardBased),
			}),
			want: false,
		},
		{
			name: "FlexAdvisorProcessing disabled - returns false",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			}, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeShardBased),
			}),
			want: false,
		},
		{
			name: "shard-based mode - returns true",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeShardBased),
			}),
			want: true,
		},
		{
			name:               "default flags (shard-based mode) - returns true",
			experimentsManager: experiments.NewMockManager(),
			want:               true,
		},
		{
			name:               "nil experiments manager (defaults to shard-based mode) - returns true",
			experimentsManager: nil,
			want:               true,
		},
		{
			name: "explicit late mode - returns false",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeLate),
			}),
			want: false,
		},
		{
			name: "classic mode - returns false",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeClassic),
			}),
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nilProcessor {
				var nilProc *ShardProcessor
				assert.Equal(t, tc.want, nilProc.ShouldProcess())
				return
			}
			var provider instanceavailability.Provider
			if !tc.nilProvider {
				provider = &instanceavailability.MockProvider{}
			}
			processor := NewShardProcessor(provider, nil, tc.experimentsManager)
			assert.Equal(t, tc.want, processor.ShouldProcess())
		})
	}
}

func TestShardProcessor_ProcessIncomingShardsAndPrefetch(t *testing.T) {
	shardBasedManager := func(boolFlags map[string]bool) experiments.Manager {
		return experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{
			experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeShardBased),
		})
	}

	taggedProvReqShard := provReqShard("")
	taggedProvReqShard.HasPayPerPodWorkloads = true

	testCases := []struct {
		name                      string
		experimentsManager        experiments.Manager
		defaultCccEnabled         bool
		nilLister                 bool
		nilProvider               bool
		expectInactive            bool
		shards                    []*podsharding.PodShard
		onScaleUpEndCalls         int
		wantActiveRegister        string
		wantFastPollScope         string
		wantPrefetchScope         string
		activeRegisterErr         error
		prefetchErr               error
		wantRejectedAfterActive   int
		wantRejectedAfterPrefetch int
	}{
		{
			name:               "shard-based with prefetch enabled by default - registers only active shard immediately, queues next shard until OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				cccShard("ccc-1"),
				cccShard("ccc-2"),
				cccShard("ccc-3"),
			},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
			wantPrefetchScope:  "ccc-2",
		},
		{
			name: "shard-based with prefetch explicitly enabled - registers next shard in slow mode on OnScaleUpEnd",
			experimentsManager: shardBasedManager(map[string]bool{
				experiments.FlexAdvisorShardBasedSchedulingPrefetchEnabledFlag: true,
			}),
			shards: []*podsharding.PodShard{
				cccShard("ccc-1"),
				cccShard("ccc-2"),
			},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
			wantPrefetchScope:  "ccc-2",
		},
		{
			name: "shard-based with prefetch disabled - registers only active shard and skips OnScaleUpEnd prefetch",
			experimentsManager: shardBasedManager(map[string]bool{
				experiments.FlexAdvisorShardBasedSchedulingPrefetchEnabledFlag: false,
			}),
			shards: []*podsharding.PodShard{
				cccShard("ccc-1"),
				cccShard("ccc-2"),
			},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
		},
		{
			name:               "multiple OnScaleUpEnd calls during Salvo - prefetches next shard only once without advancing to ccc-3",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				cccShard("ccc-1"),
				cccShard("ccc-2"),
				cccShard("ccc-3"),
			},
			onScaleUpEndCalls:  3,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
			wantPrefetchScope:  "ccc-2",
		},
		{
			name:               "skipped scale-up (OnScaleUpEnd not called) - does not prefetch next shard",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				cccShard("ccc-1"),
				cccShard("ccc-2"),
			},
			onScaleUpEndCalls:  0,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
		},
		{
			name:               "single incoming shard - registers active shard with nothing queued for OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards:             []*podsharding.PodShard{cccShard("ccc-1")},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
		},
		{
			name:               "next shard shares same scope key as active shard - does not re-register on OnScaleUpEnd or skip ahead to ccc-2",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				cccShard("ccc-1"),
				cccShard("ccc-1"),
				cccShard("ccc-2"),
			},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
		},
		{
			name:               "non-scope ProvisioningRequest shard next - does not skip ahead to prefetch ccc-2",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				cccShard("ccc-1"),
				provReqShard("ccc-pr"),
				cccShard("ccc-2"),
			},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
		},
		{
			name:               "tagged ProvisioningRequest shard next - does not skip ahead to prefetch ccc-2",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				cccShard("ccc-1"),
				taggedProvReqShard,
				cccShard("ccc-2"),
			},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
		},
		{
			name:               "untagged plain shard next without default CCC - does not skip ahead to prefetch ccc-2",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				cccShard("ccc-1"),
				plainShard("vm-billed"),
				cccShard("ccc-2"),
			},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
		},
		{
			name:               "ProvisioningRequest shard active with CCC shard next - skips active registration and prefetches ccc-1 on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				provReqShard("ccc-pr"),
				cccShard("ccc-1"),
			},
			onScaleUpEndCalls: 1,
			wantPrefetchScope: "ccc-1",
		},
		{
			name:               "untagged plain active shard without default CCC and CCC shard next - skips active and prefetches ccc-1 on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				plainShard("vm-billed"),
				cccShard("ccc-1"),
			},
			onScaleUpEndCalls: 1,
			wantPrefetchScope: "ccc-1",
		},
		{
			name:               "untagged plain active shard without default CCC and pay-per-pod shard next - skips active and prefetches general-purpose on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards: []*podsharding.PodShard{
				plainShard("vm-billed"),
				payPerPodShard("plain-a"),
			},
			onScaleUpEndCalls: 1,
			wantPrefetchScope: ccc.AutopilotPodFamilyCrdName,
		},
		{
			name:               "plain active shard with default CCC enabled - registers default CCC and prefetches ccc-1 on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			defaultCccEnabled:  true,
			shards:             []*podsharding.PodShard{plainShard("a"), cccShard("ccc-1")},
			onScaleUpEndCalls:  1,
			wantActiveRegister: labels.DefaultCCCName,
			wantFastPollScope:  labels.DefaultCCCName,
			wantPrefetchScope:  "ccc-1",
		},
		{
			name:               "plain shards only without default CCC - does not register scopes and resets rejected scopes gauge to 0",
			experimentsManager: shardBasedManager(nil),
			shards:             []*podsharding.PodShard{plainShard("a"), plainShard("b")},
			onScaleUpEndCalls:  1,
		},
		{
			name:               "nil active shard entry with CCC shard next - skips active without panicking and prefetches ccc-1 on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards:             []*podsharding.PodShard{nil, cccShard("ccc-1")},
			onScaleUpEndCalls:  1,
			wantPrefetchScope:  "ccc-1",
		},
		{
			name:               "nil next shard entry - does not panic and skips prefetch",
			experimentsManager: shardBasedManager(nil),
			shards:             []*podsharding.PodShard{cccShard("ccc-1"), nil, cccShard("ccc-2")},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
		},
		{
			name:               "active shard with nil lister - does not panic, skips default CCC lookup and prefetches explicit CCC shard on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			nilLister:          true,
			shards:             []*podsharding.PodShard{plainShard("a"), cccShard("ccc-1")},
			onScaleUpEndCalls:  1,
			wantPrefetchScope:  "ccc-1",
		},
		{
			name: "late scheduling mode - does not register, prefetch, or emit rejected scopes metric",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeLate),
			}),
			expectInactive:    true,
			shards:            []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
			onScaleUpEndCalls: 1,
		},
		{
			name: "classic scheduling mode - does not register, prefetch, or emit rejected scopes metric",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorSchedulingModeFlag: string(SchedulingModeClassic),
			}),
			expectInactive:    true,
			shards:            []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
			onScaleUpEndCalls: 1,
		},
		{
			name: "FlexAdvisorProcessing disabled - does not register, prefetch, or emit rejected scopes metric",
			experimentsManager: shardBasedManager(map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			}),
			expectInactive:    true,
			shards:            []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
			onScaleUpEndCalls: 1,
		},
		{
			name:               "nil experiments manager (defaults to shard-based mode with prefetch enabled) - registers active and prefetches next shard",
			experimentsManager: nil,
			shards:             []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
			wantPrefetchScope:  "ccc-2",
		},
		{
			name:               "default flags (shard-based mode) - registers active shard and prefetches next shard on OnScaleUpEnd",
			experimentsManager: experiments.NewMockManager(),
			shards:             []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2"), cccShard("ccc-3")},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
			wantPrefetchScope:  "ccc-2",
		},
		{
			name:               "empty shards - resets rejected scopes gauge to 0",
			experimentsManager: shardBasedManager(nil),
			shards:             nil,
			onScaleUpEndCalls:  1,
		},
		{
			name:               "nil provider - does not panic or emit metrics",
			experimentsManager: shardBasedManager(nil),
			nilProvider:        true,
			expectInactive:     true,
			shards:             []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
			onScaleUpEndCalls:  1,
		},
		{
			name:                      "active shard registration error - skips fast polling, reports rejected=1, still prefetches next shard on OnScaleUpEnd",
			experimentsManager:        shardBasedManager(nil),
			shards:                    []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
			onScaleUpEndCalls:         1,
			wantActiveRegister:        "ccc-1",
			activeRegisterErr:         fmt.Errorf("limit reached"),
			wantPrefetchScope:         "ccc-2",
			wantRejectedAfterActive:   1,
			wantRejectedAfterPrefetch: 1,
		},
		{
			name:                      "prefetch registration error on OnScaleUpEnd - records total rejected count including prefetch failure",
			experimentsManager:        shardBasedManager(nil),
			shards:                    []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
			onScaleUpEndCalls:         1,
			wantActiveRegister:        "ccc-1",
			wantFastPollScope:         "ccc-1",
			wantPrefetchScope:         "ccc-2",
			prefetchErr:               fmt.Errorf("limit reached"),
			wantRejectedAfterPrefetch: 1,
		},
		{
			name:                      "non-scope active shard with prefetch registration error - reports 0 then 1 on OnScaleUpEnd",
			experimentsManager:        shardBasedManager(nil),
			shards:                    []*podsharding.PodShard{provReqShard("ccc-pr"), cccShard("ccc-1")},
			onScaleUpEndCalls:         1,
			wantPrefetchScope:         "ccc-1",
			prefetchErr:               fmt.Errorf("limit reached"),
			wantRejectedAfterActive:   0,
			wantRejectedAfterPrefetch: 1,
		},
		{
			name:                      "active and prefetch registration errors in same loop - reports rejected=1 then cumulative rejected=2",
			experimentsManager:        shardBasedManager(nil),
			shards:                    []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
			onScaleUpEndCalls:         1,
			wantActiveRegister:        "ccc-1",
			activeRegisterErr:         fmt.Errorf("limit reached"),
			wantPrefetchScope:         "ccc-2",
			prefetchErr:               fmt.Errorf("limit reached"),
			wantRejectedAfterActive:   1,
			wantRejectedAfterPrefetch: 2,
		},
		{
			name:               "active tagged pay-per-pod shard - registers general-purpose, activates fast polling, and prefetches ccc-1 on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards:             []*podsharding.PodShard{payPerPodShard("plain-a"), cccShard("ccc-1")},
			onScaleUpEndCalls:  1,
			wantActiveRegister: ccc.AutopilotPodFamilyCrdName,
			wantFastPollScope:  ccc.AutopilotPodFamilyCrdName,
			wantPrefetchScope:  "ccc-1",
		},
		{
			name:               "tagged pay-per-pod shard next behind active CCC shard - prefetches general-purpose on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards:             []*podsharding.PodShard{cccShard("ccc-1"), payPerPodShard("plain-a")},
			onScaleUpEndCalls:  1,
			wantActiveRegister: "ccc-1",
			wantFastPollScope:  "ccc-1",
			wantPrefetchScope:  ccc.AutopilotPodFamilyCrdName,
		},
		{
			name:               "duplicate tagged pay-per-pod shards - deduplicates general-purpose without re-registering on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards:             []*podsharding.PodShard{payPerPodShard("team-a"), payPerPodShard("team-b")},
			onScaleUpEndCalls:  1,
			wantActiveRegister: ccc.AutopilotPodFamilyCrdName,
			wantFastPollScope:  ccc.AutopilotPodFamilyCrdName,
		},
		{
			name:               "default CCC enabled with tagged pay-per-pod shard - prefers default CCC over general-purpose",
			experimentsManager: shardBasedManager(nil),
			defaultCccEnabled:  true,
			shards:             []*podsharding.PodShard{payPerPodShard("plain-a"), cccShard("ccc-1")},
			onScaleUpEndCalls:  1,
			wantActiveRegister: labels.DefaultCCCName,
			wantFastPollScope:  labels.DefaultCCCName,
			wantPrefetchScope:  "ccc-1",
		},
		{
			name:               "tagged ProvisioningRequest active shard - skipped without registering general-purpose, prefetches ccc-1 on OnScaleUpEnd",
			experimentsManager: shardBasedManager(nil),
			shards:             []*podsharding.PodShard{taggedProvReqShard, cccShard("ccc-1")},
			onScaleUpEndCalls:  1,
			wantPrefetchScope:  "ccc-1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var provider instanceavailability.Provider
			mockProvider := &instanceavailability.MockProvider{}
			if !tc.nilProvider {
				if tc.wantActiveRegister != "" {
					mockProvider.On("RegisterFlexibilityScope", tc.wantActiveRegister).Return(tc.activeRegisterErr).Once()
				}
				if tc.wantFastPollScope != "" {
					mockProvider.On("ActivateFastPolling", tc.wantFastPollScope).Return().Once()
				}
				provider = mockProvider
			}

			var cccLister lister.Lister
			if !tc.nilLister {
				crds := []crd.CRD{
					ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "ccc-1"}}, "", false, nil, nil),
					ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "ccc-2"}}, "", false, nil, nil),
					ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "ccc-3"}}, "", false, nil, nil),
					ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "ccc-pr"}}, "", false, nil, nil),
				}
				if tc.defaultCccEnabled {
					crds = append(crds, ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: labels.DefaultCCCName}}, "", false, nil, nil))
				}
				mockLister := lister.NewMockCrdListerWithLabel(crds, labels.ComputeClassLabel)
				if tc.defaultCccEnabled {
					mockLister.SetDefaultCrdName(labels.DefaultCCCName)
				}
				cccLister = mockLister
			}

			mockMetrics := &mockPodListProcessorMetrics{}
			if !tc.expectInactive {
				mockMetrics.On("UpdateFlexAdvisorRejectedScopes", tc.wantRejectedAfterActive).Return().Once()
			}

			processor := NewShardProcessor(provider, cccLister, tc.experimentsManager, withShardProcessorMetrics(mockMetrics))
			processor.ProcessIncomingShards(tc.shards)

			// Verify active shard expectations BEFORE OnScaleUpEnd so prefetch cannot happen eagerly.
			mockProvider.AssertExpectations(t)
			mockMetrics.AssertExpectations(t)

			if !tc.expectInactive && tc.wantPrefetchScope != "" {
				mockProvider.On("RegisterFlexibilityScope", tc.wantPrefetchScope).Return(tc.prefetchErr).Once()
				mockMetrics.On("UpdateFlexAdvisorRejectedScopes", tc.wantRejectedAfterPrefetch).Return().Once()
			}
			for range tc.onScaleUpEndCalls {
				processor.OnScaleUpEnd()
			}

			mockProvider.AssertExpectations(t)
			mockMetrics.AssertExpectations(t)
		})
	}
}

func TestShardProcessor_ProcessIncomingShards_ClearsStaleNextScope(t *testing.T) {
	testCases := []struct {
		name         string
		secondShards []*podsharding.PodShard
		wantActive   string
		wantPrefetch string
	}{
		{
			name:         "single shard on subsequent loop - clears previous pending prefetch",
			secondShards: []*podsharding.PodShard{cccShard("ccc-3")},
			wantActive:   "ccc-3",
		},
		{
			name:         "empty shards on subsequent loop - clears previous pending prefetch",
			secondShards: nil,
		},
		{
			name:         "non-scope shard next on subsequent loop - clears previous pending prefetch",
			secondShards: []*podsharding.PodShard{cccShard("ccc-3"), provReqShard("ccc-pr"), cccShard("ccc-4")},
			wantActive:   "ccc-3",
		},
		{
			name:         "unscoped provreq shard on subsequent loop - clears previous pending prefetch and rejected count",
			secondShards: []*podsharding.PodShard{provReqShard("ccc-pr")},
		},
		{
			name:         "new next shard on subsequent loop - replaces previous pending prefetch",
			secondShards: []*podsharding.PodShard{cccShard("ccc-3"), cccShard("ccc-4")},
			wantActive:   "ccc-3",
			wantPrefetch: "ccc-4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := &instanceavailability.MockProvider{}
			mockMetrics := &mockPodListProcessorMetrics{}
			processor := NewShardProcessor(mockProvider, mockShardTestLister(), experiments.NewMockManager(), withShardProcessorMetrics(mockMetrics))

			// Loop 1 fails active registration (rejected=1) and skips OnScaleUpEnd (e.g., blocked by MaxNodesTotal), leaving ccc-2 queued.
			mockProvider.On("RegisterFlexibilityScope", "ccc-1").Return(fmt.Errorf("limit reached")).Once()
			mockMetrics.On("UpdateFlexAdvisorRejectedScopes", 1).Return().Once()
			processor.ProcessIncomingShards([]*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")})

			// Loop 2 replaces the queued scope and resets rejectedInLoop to 0.
			if tc.wantActive != "" {
				mockProvider.On("RegisterFlexibilityScope", tc.wantActive).Return(nil).Once()
				mockProvider.On("ActivateFastPolling", tc.wantActive).Return().Once()
			}
			mockMetrics.On("UpdateFlexAdvisorRejectedScopes", 0).Return().Once()
			processor.ProcessIncomingShards(tc.secondShards)
			mockProvider.AssertExpectations(t)
			mockMetrics.AssertExpectations(t)

			if tc.wantPrefetch != "" {
				mockProvider.On("RegisterFlexibilityScope", tc.wantPrefetch).Return(nil).Once()
				mockMetrics.On("UpdateFlexAdvisorRejectedScopes", 0).Return().Once()
			}
			processor.OnScaleUpEnd()

			mockProvider.AssertExpectations(t)
			mockMetrics.AssertExpectations(t)
		})
	}
}

func TestShardProcessor_OnScaleUpEnd_NilReceiver(t *testing.T) {
	var nilProc *ShardProcessor
	assert.NotPanics(t, func() {
		nilProc.ProcessIncomingShards([]*podsharding.PodShard{cccShard("ccc-1")})
		nilProc.OnScaleUpEnd()
	})
}
