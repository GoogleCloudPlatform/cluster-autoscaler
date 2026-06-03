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

package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

func TestGetsOtherNodeGroupsInSameNodePool(t *testing.T) {
	migA := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "project", Zone: "zone-1", Name: "test-A"}).Build()
	migB := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "project", Zone: "zone-2", Name: "test-B"}).Build()
	migC := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "project", Zone: "zone-2", Name: "test-C"}).Build()

	gke.AddMigsToNodePool("wonderful-pool", migA, migB)
	gke.AddMigsToNodePool("other-pool", migC)

	migs := []*gke.GkeMig{migA, migB, migC}

	tests := []struct {
		name  string
		input *gke.GkeMig
		want  []cloudprovider.NodeGroup
	}{
		{
			name:  "single nodegroup in a nodepool",
			input: migC,
			want:  []cloudprovider.NodeGroup{},
		},
		{
			name:  "two nodegroups in a nodepool",
			input: migA,
			want:  []cloudprovider.NodeGroup{migB},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			ctx := &context.AutoscalingContext{}
			innerProcessor := &mockNodeGroupSetProcessor{}

			processor := NewNodePoolAwareNodeGroupSetProcessor(innerProcessor)
			gotNgs, err := processor.FindSimilarNodeGroups(ctx, test.input, buildTestNodeInfosForMigs(migs...))

			assert.NoError(t, err)
			assert.Equal(t, len(test.want), len(gotNgs))
			for i := range test.want {
				assert.Contains(t, gotNgs, test.want[i])
			}
		})
	}
}

func TestFallbackToInnerProcessor(t *testing.T) {
	// These migs will have SimilarNodeGroups == nil and should trigger a fallback.
	migWithNilSimilarGroups1 := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "project", Zone: "zone-1", Name: "test-B"}).Build()
	migWithNilSimilarGroups2 := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "project", Zone: "zone-1", Name: "test-C"}).SetNodePoolName("other-pool").Build()

	notGkeMig := testprovider.NewTestNodeGroup("not-a-gkemig", 0, 0, 0, false, false, "", nil, nil)

	tests := []struct {
		name  string
		input cloudprovider.NodeGroup
	}{
		{
			name:  "GkeMig with nil SimilarNodeGroups",
			input: migWithNilSimilarGroups1,
		},
		{
			name:  "Another GkeMig with nil SimilarNodeGroups",
			input: migWithNilSimilarGroups2,
		},
		{
			name:  "Not a GkeMig",
			input: notGkeMig,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := &context.AutoscalingContext{}
			innerProcessor := &mockNodeGroupSetProcessor{}
			processor := NewNodePoolAwareNodeGroupSetProcessor(innerProcessor)

			// Expect a fallback call to inner processor
			innerProcessor.On("FindSimilarNodeGroups", ctx, test.input, mock.Anything).Return([]cloudprovider.NodeGroup{}, nil).Once()
			gotNgs, err := processor.FindSimilarNodeGroups(ctx, test.input, buildTestNodeInfosForMigs([]*gke.GkeMig{migWithNilSimilarGroups1, migWithNilSimilarGroups2}...))

			assert.NoError(t, err)
			assert.Equal(t, 0, len(gotNgs))
		})
	}
}

func buildTestNodeInfosForMigs(migs ...*gke.GkeMig) map[string]*framework.NodeInfo {
	nodeInfos := make(map[string]*framework.NodeInfo)

	for _, mig := range migs {
		node := test.BuildTestNode("foo", 1000, 1000)
		AddNodePoolLabelToNode(node, mig.NodePoolName())
		nodeInfos[mig.Id()] = framework.NewNodeInfo(node, nil)
	}

	return nodeInfos
}

type mockNodeGroupSetProcessor struct {
	mock.Mock
}

func (m *mockNodeGroupSetProcessor) FindSimilarNodeGroups(ctx *context.AutoscalingContext, ng cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, errors.AutoscalerError) {
	args := m.Called(ctx, ng, nodeInfos)
	return args.Get(0).([]cloudprovider.NodeGroup), nil
}

func (m *mockNodeGroupSetProcessor) BalanceScaleUpBetweenGroups(ctx *context.AutoscalingContext, ngs []cloudprovider.NodeGroup, n int) ([]nodegroupset.ScaleUpInfo, errors.AutoscalerError) {
	args := m.Called(ctx, ngs, n)
	return args.Get(0).([]nodegroupset.ScaleUpInfo), nil
}

func (m *mockNodeGroupSetProcessor) CleanUp() {
	m.Called()
}
