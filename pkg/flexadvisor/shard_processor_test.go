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
	mockProvider := &instanceavailability.MockProvider{}
	testCases := []struct {
		name      string
		processor *ShardProcessor
		want      bool
	}{
		{
			name:      "adaptive polling enabled by default - returns true",
			processor: NewShardProcessor(mockProvider, mockShardTestLister(), experiments.NewMockManager()),
			want:      true,
		},
		{
			name:      "nil experiments manager - defaults to enabled and returns true",
			processor: NewShardProcessor(mockProvider, mockShardTestLister(), nil),
			want:      true,
		},
		{
			name: "adaptive polling flag disabled - returns false",
			processor: NewShardProcessor(mockProvider, mockShardTestLister(), experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag: false,
			}, nil)),
			want: false,
		},
		{
			name: "adaptive polling min CA version not met - returns false",
			processor: NewShardProcessor(mockProvider, mockShardTestLister(), experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: false,
			}, nil)),
			want: false,
		},
		{
			name: "FlexAdvisorProcessing disabled - returns false",
			processor: NewShardProcessor(mockProvider, mockShardTestLister(), experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:           false,
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: true,
			}, nil)),
			want: false,
		},
		{
			name:      "nil provider - returns false",
			processor: NewShardProcessor(nil, mockShardTestLister(), experiments.NewMockManager()),
			want:      false,
		},
		{
			name:      "nil ShardProcessor receiver - returns false without panicking",
			processor: nil,
			want:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.processor.ShouldProcess())
		})
	}
}

func TestShardProcessor_ProcessIncomingShards(t *testing.T) {
	adaptivePollingFlags := map[string]bool{
		experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
		experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: true,
	}

	testCases := []struct {
		name                string
		experimentsManager  experiments.Manager
		cccLister           lister.Lister
		nilProvider         bool
		shards              []*podsharding.PodShard
		wantNormalPollScope string
	}{
		{
			name:                "adaptive polling enabled by default - sets PollingModeNormal for active shard",
			experimentsManager:  experiments.NewMockManager(),
			cccLister:           mockShardTestLister(),
			shards:              []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
			wantNormalPollScope: "ccc-1",
		},
		{
			name: "adaptive polling flag disabled - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag: false,
			}, nil),
			cccLister: mockShardTestLister(),
			shards:    []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2")},
		},
		{
			name: "adaptive polling flag enabled but min CA version not met - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: false,
			}, nil),
			cccLister: mockShardTestLister(),
			shards:    []*podsharding.PodShard{cccShard("ccc-1")},
		},
		{
			name: "adaptive polling enabled but FlexAdvisorProcessing disabled - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:           false,
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: true,
			}, nil),
			cccLister: mockShardTestLister(),
			shards:    []*podsharding.PodShard{cccShard("ccc-1")},
		},
		{
			name:                "nil experiments manager - defaults to enabled and sets PollingModeNormal",
			experimentsManager:  nil,
			cccLister:           mockShardTestLister(),
			shards:              []*podsharding.PodShard{cccShard("ccc-1")},
			wantNormalPollScope: "ccc-1",
		},
		{
			name:               "adaptive polling enabled, empty shards - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, adaptivePollingFlags, nil),
			cccLister:          mockShardTestLister(),
			shards:             nil,
		},
		{
			name:               "adaptive polling enabled, nil provider - does not panic",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, adaptivePollingFlags, nil),
			cccLister:          mockShardTestLister(),
			nilProvider:        true,
			shards:             []*podsharding.PodShard{cccShard("ccc-1")},
		},
		{
			name:                "adaptive polling enabled, explicit CCC on active shard - sets PollingModeNormal only for shards[0]",
			experimentsManager:  experiments.NewMockManagerWithOptions(version.Version{}, adaptivePollingFlags, nil),
			cccLister:           mockShardTestLister(),
			shards:              []*podsharding.PodShard{cccShard("ccc-1"), cccShard("ccc-2"), cccShard("ccc-3")},
			wantNormalPollScope: "ccc-1",
		},
		{
			name:                "adaptive polling enabled, plain active shard with default CCC enabled - sets PollingModeNormal for default scope",
			experimentsManager:  experiments.NewMockManagerWithOptions(version.Version{}, adaptivePollingFlags, nil),
			cccLister:           mockShardTestListerWithDefault(),
			shards:              []*podsharding.PodShard{plainShard("a"), cccShard("ccc-1")},
			wantNormalPollScope: labels.DefaultCCCName,
		},
		{
			name:               "adaptive polling enabled, plain active shard without default CCC - does not call SetPollingMode",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, adaptivePollingFlags, nil),
			cccLister:          mockShardTestLister(),
			shards:             []*podsharding.PodShard{plainShard("a"), cccShard("ccc-1")},
		},
		{
			name:               "adaptive polling enabled, plain active shard with nil lister - does not panic or call SetPollingMode",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, adaptivePollingFlags, nil),
			cccLister:          nil,
			shards:             []*podsharding.PodShard{plainShard("a"), cccShard("ccc-1")},
		},
		{
			name:               "adaptive polling enabled, provisioning request active shard - skipped even when it carries a CCC",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, adaptivePollingFlags, nil),
			cccLister:          mockShardTestLister(),
			shards:             []*podsharding.PodShard{provReqShard("ccc-pr"), cccShard("ccc-1")},
		},
		{
			name:               "adaptive polling enabled, nil active shard entry - skipped without panicking",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, adaptivePollingFlags, nil),
			cccLister:          mockShardTestLister(),
			shards:             []*podsharding.PodShard{nil, cccShard("ccc-1")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var provider instanceavailability.Provider
			mockProvider := &instanceavailability.MockProvider{}
			if !tc.nilProvider {
				if tc.wantNormalPollScope != "" {
					mockProvider.On("SetPollingMode", tc.wantNormalPollScope, PollingModeNormal).Return().Once()
				}
				provider = mockProvider
			}

			processor := NewShardProcessor(provider, tc.cccLister, tc.experimentsManager)
			processor.ProcessIncomingShards(tc.shards)

			mockProvider.AssertExpectations(t)
		})
	}
}
