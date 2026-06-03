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

package autoprovisioning

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

func TestSortedNodeGroupListProcessor_CleanUp(t *testing.T) {
	testCases := []int{0, 1, 100}
	for _, expectedCallsCount := range testCases {
		t.Run(fmt.Sprintf("call count: %d", expectedCallsCount), func(t *testing.T) {
			p := &mockNodeGroupListProcessor{}
			processor := NewSortedNodeGroupListProcessor(p)

			for i := 0; i < expectedCallsCount; i++ {
				processor.CleanUp()
			}

			if p.cleanUpCallsCount != expectedCallsCount {
				t.Errorf("expected clean to be called %d times but was %d", expectedCallsCount, p.cleanUpCallsCount)
			}
		})
	}
}

func TestSortedNodeGroupListProcessor_Process(t *testing.T) {
	ng100 := buildFakeNodeGroup("ng100", 100)
	ngError := buildFakeNodeGroupWithError("ngError", 100)
	ng50 := buildFakeNodeGroup("ng50", 50)
	ng200 := buildFakeNodeGroup("ng200", 200)

	for tn, tc := range map[string]struct {
		nodeGroups     []cloudprovider.NodeGroup
		expectedResult []cloudprovider.NodeGroup
	}{
		"empty list": {
			nodeGroups:     []cloudprovider.NodeGroup{},
			expectedResult: []cloudprovider.NodeGroup{},
		},
		"simple list": {
			nodeGroups:     []cloudprovider.NodeGroup{ng100, ngError, ng50, ng200},
			expectedResult: []cloudprovider.NodeGroup{ng200, ng100, ng50, ngError},
		},
		"repeated elements": {
			nodeGroups:     []cloudprovider.NodeGroup{ng50, ng100, ngError, ng50, ng200, ng200, ngError, ng50},
			expectedResult: []cloudprovider.NodeGroup{ng200, ng200, ng100, ng50, ng50, ng50, ngError, ngError},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			processor := NewSortedNodeGroupListProcessor(&mockNodeGroupListProcessor{})
			got, _, err := processor.Process(nil, tc.nodeGroups, nil, nil)
			if err != nil {
				t.Errorf("sortedNodeGroupListProcessor.Process(nil, %v, nil, nil) returned error %v", tc.nodeGroups, err)
			}
			if !reflect.DeepEqual(got, tc.expectedResult) {
				t.Errorf("sortedNodeGroupListProcessor.Process(nil, %v, nil, nil) = %v, want %v", tc.nodeGroups, got, tc.expectedResult)
			}
		})
	}
}

func TestNewSortedNodeGroupListProcessor_Process_Error(t *testing.T) {
	want := errors.New("test-error")
	p := &mockNodeGroupListProcessor{errorToReturn: want}
	processor := NewSortedNodeGroupListProcessor(p)

	_, _, got := processor.Process(nil, nil, nil, nil)

	if got != want {
		t.Errorf("sortedNodeGroupListProcessor.Process(nil, nil, nil, nil) = %v, want %v", got, want)
	}
}

type mockNodeGroupListProcessor struct {
	cleanUpCallsCount int
	errorToReturn     error
}

func (m *mockNodeGroupListProcessor) CleanUp() {
	m.cleanUpCallsCount++
}

func (m *mockNodeGroupListProcessor) Process(_ *context.AutoscalingContext,
	nodeGroups []cloudprovider.NodeGroup,
	nodeInfos map[string]*framework.NodeInfo,
	_ []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	return nodeGroups, nodeInfos, m.errorToReturn
}

type fakeNodeGroup struct {
	*test.TestNodeGroup
	milliCPUAllocatable            int64
	errorInsteadOfTemplateNodeInfo bool
	nodeId                         string
}

func (f *fakeNodeGroup) TemplateNodeInfo() (*framework.NodeInfo, error) {
	if f.errorInsteadOfTemplateNodeInfo {
		return nil, errors.New("test error")
	}
	return framework.NewTestNodeInfo(testutils.BuildTestNode("test-node", f.milliCPUAllocatable, 0)), nil
}

func (f *fakeNodeGroup) Id() string {
	return f.nodeId
}

func buildFakeNodeGroup(id string, cpu int64) *fakeNodeGroup {
	return &fakeNodeGroup{
		nodeId:                         id,
		milliCPUAllocatable:            cpu,
		errorInsteadOfTemplateNodeInfo: false,
	}
}

func buildFakeNodeGroupWithError(id string, cpu int64) *fakeNodeGroup {
	return &fakeNodeGroup{
		nodeId:                         id,
		milliCPUAllocatable:            cpu,
		errorInsteadOfTemplateNodeInfo: true,
	}
}
