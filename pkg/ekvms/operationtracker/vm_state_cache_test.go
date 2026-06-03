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

package operationtracker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
)

var (
	providerID  = "gce://project1/us-central1-b/node1"
	gceRef      = gce.GceRef{Project: "project1", Zone: "us-central1-b", Name: "node1"}
	wantVmState = ekvmtypes.ResizableVmState{
		Size:   size.VmSize{MilliCpus: 1000, KBytes: 1024},
		Status: ekvmtypes.ResizeStatusAtIntent,
	}
)

func TestRefresh(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024)
	node.Spec.ProviderID = providerID
	cloudProvider := &mockCloudProvider{}
	cloudProvider.On("BulkFetchCurrentResizableVmStates").Return(map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: wantVmState}, nil)

	cache := newVmStateCache(cloudProvider)
	_, err := cache.getState(node)
	assert.Error(t, err)
	cache.refresh()
	gotVmState, err := cache.getState(node)
	assert.NoError(t, err)
	assert.Equal(t, wantVmState, gotVmState)
}

func TestRefreshOverridesPreviousState(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024)
	node.Spec.ProviderID = providerID
	cloudProvider := &mockCloudProvider{}
	cloudProvider.On("BulkFetchCurrentResizableVmStates").Return(map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: wantVmState}, nil)

	cache := newVmStateCache(cloudProvider)
	prevoiusVmState := ekvmtypes.ResizableVmState{}
	err := cache.updateState(node, prevoiusVmState)
	assert.NoError(t, err)
	vmState, err := cache.getState(node)
	assert.NoError(t, err)
	assert.Equal(t, prevoiusVmState, vmState)
	cache.refresh()
	gotVmState, err := cache.getState(node)
	assert.NoError(t, err)
	assert.Equal(t, wantVmState, gotVmState)
}

func TestGetStateOrRefresh(t *testing.T) {
	type getCurrentEkVmStateResult struct {
		vmState ekvmtypes.ResizableVmState
		err     error
	}

	testCases := []struct {
		desc                string
		initState           map[gce.GceRef]ekvmtypes.ResizableVmState
		getCurrentEkVmState getCurrentEkVmStateResult
		want                ekvmtypes.ResizableVmState
		wantErr             error
	}{
		{
			desc:      "Refresh is triggered",
			initState: map[gce.GceRef]ekvmtypes.ResizableVmState{},
			getCurrentEkVmState: getCurrentEkVmStateResult{
				vmState: wantVmState,
			},
			want: wantVmState,
		},
		{
			desc:      "Value in cache",
			initState: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: wantVmState},
			getCurrentEkVmState: getCurrentEkVmStateResult{
				vmState: ekvmtypes.ResizableVmState{},
			},
			want: wantVmState,
		},
		{
			desc:      "Error is handled",
			initState: map[gce.GceRef]ekvmtypes.ResizableVmState{},
			getCurrentEkVmState: getCurrentEkVmStateResult{
				err: fmt.Errorf("error"),
			},
			want:    ekvmtypes.ResizableVmState{},
			wantErr: fmt.Errorf("error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			node := test.BuildTestNode("node1", 1000, 1024)
			node.Spec.ProviderID = providerID

			cloudProvider := &mockCloudProvider{}
			cloudProvider.On("BulkFetchCurrentResizableVmStates").Return(tc.initState, nil)
			cloudProvider.On("GetCurrentResizableVmState", mock.Anything).Return(tc.getCurrentEkVmState.vmState, tc.getCurrentEkVmState.err)

			cache := newVmStateCache(cloudProvider)
			cache.refresh()
			gotVmState, err := cache.getStateOrRefresh(node)
			assert.Equal(t, tc.want, gotVmState)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestUpdateState(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024)
	node.Spec.ProviderID = providerID

	cloudProvider := &mockCloudProvider{}

	cache := newVmStateCache(cloudProvider)
	_, err := cache.getState(node)
	assert.Error(t, err)
	err = cache.updateState(node, wantVmState)
	assert.NoError(t, err)
	gotVmState, err := cache.getState(node)
	assert.NoError(t, err)
	assert.Equal(t, wantVmState, gotVmState)
}

func TestMalformedNodesAreHandled(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024)
	node.Spec.ProviderID = ""

	cloudProvider := &mockCloudProvider{}
	cloudProvider.On("GetCurrentResizableVmState", mock.Anything).Return(ekvmtypes.ResizableVmState{}, fmt.Errorf("error"))
	cache := newVmStateCache(cloudProvider)
	_, err := cache.getState(node)
	assert.Error(t, err)
	_, err = cache.getStateOrRefresh(node)
	assert.Error(t, err)
	err = cache.updateState(node, ekvmtypes.ResizableVmState{})
	assert.Error(t, err)
}

func TestInvalidate(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 1024)
	node.Spec.ProviderID = providerID
	cloudProvider := &mockCloudProvider{}
	cloudProvider.On("GetCurrentResizableVmState", node).Return(wantVmState, nil)

	cache := newVmStateCache(cloudProvider)
	prevoiusVmState := ekvmtypes.ResizableVmState{}
	err := cache.updateState(node, prevoiusVmState)
	assert.NoError(t, err)
	vmState, err := cache.getState(node)
	assert.NoError(t, err)
	assert.Equal(t, prevoiusVmState, vmState)
	err = cache.invalidate(node)
	assert.NoError(t, err)
	gotVmState, err := cache.getStateOrRefresh(node)
	assert.NoError(t, err)
	assert.Equal(t, wantVmState, gotVmState)
}
