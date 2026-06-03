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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

type mockTemplateNodeInfoProvider struct {
	mock.Mock
}

func (m *mockTemplateNodeInfoProvider) Process(ctx *context.AutoscalingContext, nodes []*apiv1.Node, daemonsets []*appsv1.DaemonSet, taintConfig taints.TaintConfig, now time.Time) (map[string]*framework.NodeInfo, errors.AutoscalerError) {
	args := m.Called(ctx, nodes, daemonsets, taintConfig, now)
	var nodeInfos map[string]*framework.NodeInfo
	if args.Get(0) != nil {
		nodeInfos = args.Get(0).(map[string]*framework.NodeInfo)
	}
	var err errors.AutoscalerError
	if args.Get(1) != nil {
		err = args.Get(1).(errors.AutoscalerError)
	}
	return nodeInfos, err
}

func (m *mockTemplateNodeInfoProvider) CleanUp() {
	m.Called()
}

func TestNodeInfoProvider_Process(t *testing.T) {
	csnMig := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{Labels: map[string]string{
		csn.SoftWorkloadSeparationKey: csn.SoftWorkloadSeparationValue,
	}}).Build()
	nonCsnMig := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{Labels: map[string]string{}}).Build()

	testCases := []struct {
		name               string
		experimentEnabled  bool
		nodeInCsnMig       bool
		initialState       csn.NodeState
		expectChilling     bool
		expectRemoveBuffer bool
	}{
		{
			name:               "experiment disabled, nothing happens",
			experimentEnabled:  false,
			nodeInCsnMig:       true,
			initialState:       csn.NodeStateSuspended,
			expectChilling:     false,
			expectRemoveBuffer: false,
		},
		{
			name:               "experiment enabled, node in CSN mig, marked as chilling and buffer removed",
			experimentEnabled:  true,
			nodeInCsnMig:       true,
			initialState:       csn.NodeStateSuspended,
			expectChilling:     true,
			expectRemoveBuffer: true,
		},
		{
			name:               "experiment enabled, node already chilling in CSN mig, buffer removed",
			experimentEnabled:  true,
			nodeInCsnMig:       true,
			initialState:       csn.NodeStateChilling,
			expectChilling:     true,
			expectRemoveBuffer: true,
		},
		{
			name:               "experiment enabled, node not in CSN mig, nothing happens",
			experimentEnabled:  true,
			nodeInCsnMig:       false,
			initialState:       csn.NodeStateSuspended,
			expectChilling:     false,
			expectRemoveBuffer: false,
		},
		{
			name:               "experiment enabled, node not in CSN mig but already chilling, nothing happens",
			experimentEnabled:  true,
			nodeInCsnMig:       false,
			initialState:       csn.NodeStateChilling,
			expectChilling:     true,
			expectRemoveBuffer: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := test.BuildTestNode("node-1", 1000, 1000)
			node, err := csn.SetNodeAs(node, tc.initialState)
			assert.NoError(t, err)

			// Always assign a buffer initially to test if it's removed or stays
			node, err = assignNodeToBufferForProcessors(node, "ns/buffer")
			assert.NoError(t, err)

			nodeInfo := framework.NewNodeInfo(node, nil)
			nodeInfos := map[string]*framework.NodeInfo{"node-1": nodeInfo}

			mockBaseProvider := &mockTemplateNodeInfoProvider{}
			mockBaseProvider.On("Process", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nodeInfos, nil)

			mockCloudProvider := &gke.GkeCloudProviderMock{}
			if tc.nodeInCsnMig {
				mockCloudProvider.On("GkeMigForNode", mock.Anything).Return(csnMig, nil)
			} else {
				mockCloudProvider.On("GkeMigForNode", mock.Anything).Return(nonCsnMig, nil)
			}

			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.ColdStandbyNodesProcessTemplateNodeInfosFlag: tc.experimentEnabled,
			}, nil)

			p := NewNodeInfoProvider(mockBaseProvider, mockCloudProvider, experimentsManager)
			res, err := p.Process(&context.AutoscalingContext{}, nil, nil, taints.TaintConfig{}, time.Now())

			assert.NoError(t, err)
			assert.Len(t, res, 1)
			processedNode := res["node-1"].Node()

			if tc.expectChilling {
				assert.Equal(t, csn.NodeStateChilling, csn.ClassifyNode(processedNode))
			} else {
				assert.Equal(t, tc.initialState, csn.ClassifyNode(processedNode))
			}

			if tc.expectRemoveBuffer {
				assert.Equal(t, "", csn.GetBufferIdFromNode(processedNode), "Buffer should have been removed")
			} else {
				assert.Equal(t, "ns/buffer", csn.GetBufferIdFromNode(processedNode), "Buffer should have stayed")
			}
		})
	}
}
