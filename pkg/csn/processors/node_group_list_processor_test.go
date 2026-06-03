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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	test_utils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestNodeGroupListProcessor(t *testing.T) {
	normalNg := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-normal").
		SetGceRefName("nodepool-normal").
		Build()

	csnNg := gke.NewTestGkeMigBuilder().
		SetNodePoolName("nodepool-csn").
		SetGceRefName("nodepool-csn").
		Build()

	normalNode := test_utils.BuildTestNode("normal-node", 1000, 1000)
	csnNode := test_utils.BuildTestNode("csn-node", 1000, 1000)
	csnNode.Labels[csn.SoftWorkloadSeparationKey] = csn.SoftWorkloadSeparationValue

	csnPod := test_utils.BuildTestPod("csn-pod", 100, 100)
	csn.MakePodCSN(csnPod, "ns/buffer")

	normalPod := test_utils.BuildTestPod("normal-pod", 100, 100)

	testCases := []struct {
		name               string
		unschedulablePods  []*apiv1.Pod
		nodeGroups         []cloudprovider.NodeGroup
		expectedGroups     []cloudprovider.NodeGroup
		err                bool
		experimentDisabled bool
	}{
		{
			name:              "CSN pod present, no filtering",
			unschedulablePods: []*apiv1.Pod{csnPod, normalPod},
			nodeGroups:        []cloudprovider.NodeGroup{normalNg, csnNg},
			expectedGroups:    []cloudprovider.NodeGroup{normalNg, csnNg},
		},
		{
			name:              "No CSN pod present, filtering CSN node group",
			unschedulablePods: []*apiv1.Pod{normalPod},
			nodeGroups:        []cloudprovider.NodeGroup{normalNg, csnNg},
			expectedGroups:    []cloudprovider.NodeGroup{normalNg},
		},
		{
			name:              "No CSN pod present, no CSN node group",
			unschedulablePods: []*apiv1.Pod{normalPod},
			nodeGroups:        []cloudprovider.NodeGroup{normalNg},
			expectedGroups:    []cloudprovider.NodeGroup{normalNg},
		},
		{
			name:              "Empty pods, filtering CSN node group",
			unschedulablePods: []*apiv1.Pod{},
			nodeGroups:        []cloudprovider.NodeGroup{normalNg, csnNg},
			expectedGroups:    []cloudprovider.NodeGroup{normalNg},
		},
		{
			name:              "Underlying processor error",
			unschedulablePods: []*apiv1.Pod{},
			nodeGroups:        []cloudprovider.NodeGroup{normalNg, csnNg},
			expectedGroups:    nil,
			err:               true,
		},
		{
			name:               "Flag not present, no filtering",
			unschedulablePods:  []*apiv1.Pod{normalPod},
			nodeGroups:         []cloudprovider.NodeGroup{normalNg, csnNg},
			expectedGroups:     []cloudprovider.NodeGroup{normalNg, csnNg},
			experimentDisabled: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			underlyingProcessor := &mockNodeGroupListProcessor{err: tc.err}
			var experimentsManager experiments.Manager
			if tc.experimentDisabled {
				falseValueDirectLaunch := map[string]bool{
					experiments.ColdStandbyNodesPreventCSNScaleUpForNonCSNPodsFlag: false,
				}

				experimentsManager = experiments.NewMockManagerWithOptions(version.Version{}, falseValueDirectLaunch, map[string]string{})
			} else {
				experimentsManager = experiments.NewMockManager()
			}
			processor := NewNodeGroupListProcessor(underlyingProcessor, experimentsManager)

			nodeInfos := map[string]*framework.NodeInfo{
				normalNg.Id(): framework.NewTestNodeInfo(normalNode),
				csnNg.Id():    framework.NewTestNodeInfo(csnNode),
			}

			actualGroups, _, err := processor.Process(&context.AutoscalingContext{}, tc.nodeGroups, nodeInfos, tc.unschedulablePods)
			if tc.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.ElementsMatch(t, tc.expectedGroups, actualGroups)
			}
		})
	}
}

func TestCleanUp(t *testing.T) {
	underlyingProcessor := &mockNodeGroupListProcessor{}
	experimentsManager := experiments.NewMockManager()
	processor := NewNodeGroupListProcessor(underlyingProcessor, experimentsManager)

	processor.CleanUp()
	assert.True(t, underlyingProcessor.wasCleanUpCalled)
}

type mockNodeGroupListProcessor struct {
	err              bool
	wasCleanUpCalled bool
}

func (p *mockNodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	if p.err {
		return nil, nil, assert.AnError
	}
	return nodeGroups, nodeInfos, nil
}

func (m *mockNodeGroupListProcessor) CleanUp() {
	m.wasCleanUpCalled = true
}
