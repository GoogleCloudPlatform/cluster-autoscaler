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

package providers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	resizable_vm_types "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestRun(t *testing.T) {
	tests := []struct {
		name                                 string
		dryRunErrors                         []error // Sequence of errors/success
		expectedIsBalloonPodCreatable        bool
		expectedBalloonPodCreationErrorCount int
		expectedBalloonPodSizeIndex          int
	}{
		{
			name: "3 consecutive failures - disable balloon pods creation",
			dryRunErrors: []error{
				assert.AnError, assert.AnError, assert.AnError,
			},
			expectedIsBalloonPodCreatable:        false,
			expectedBalloonPodCreationErrorCount: 3,
			expectedBalloonPodSizeIndex:          0,
		},
		{
			name: "success after 3 failures - re-enables balloon pods creation",
			dryRunErrors: []error{
				assert.AnError, assert.AnError, assert.AnError, nil,
			},
			expectedIsBalloonPodCreatable:        true,
			expectedBalloonPodCreationErrorCount: 0,
			expectedBalloonPodSizeIndex:          1,
		},
		{
			name: "success after 2 failures - reset error count",
			dryRunErrors: []error{
				assert.AnError, assert.AnError, nil, assert.AnError,
			},
			expectedIsBalloonPodCreatable:        true,
			expectedBalloonPodCreationErrorCount: 1,
			expectedBalloonPodSizeIndex:          1,
		},
		{
			name: "simple rotation on 12 success",
			dryRunErrors: []error{
				nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
			},
			expectedIsBalloonPodCreatable:        true,
			expectedBalloonPodCreationErrorCount: 0,
			expectedBalloonPodSizeIndex:          12 % len(balloonPodTestSizes),
		},
		{
			name: "Index does not rotate on failure",
			dryRunErrors: []error{
				nil, assert.AnError, assert.AnError,
			},
			expectedIsBalloonPodCreatable:        true,
			expectedBalloonPodCreationErrorCount: 2,
			expectedBalloonPodSizeIndex:          1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			fakeClient := fake.NewSimpleClientset()

			provider := &ResizableVmAutoprovisioningProvider{
				balloonPodChecker: &balloonPodChecker{
					isBalloonPodCreatable: true,
					runInterval:           time.Millisecond,
					clientSet:             fakeClient,
				},
			}

			callCount := 0
			ctx, cancel := context.WithCancel(context.Background())
			fakeClient.PrependReactor("create", "pods", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
				callCount++
				if callCount == len(tc.dryRunErrors) { // Stop the clock exactly when all calls are executed
					cancel()
				}
				return true, nil, tc.dryRunErrors[callCount-1]
			})

			provider.Run(ctx)

			assert.Equal(t, tc.expectedIsBalloonPodCreatable, provider.balloonPodChecker.isBalloonPodCreatable)
			assert.Equal(t, tc.expectedBalloonPodCreationErrorCount, provider.balloonPodChecker.balloonPodCreationErrorCount)
			assert.Equal(t, tc.expectedBalloonPodSizeIndex, provider.balloonPodChecker.balloonPodSizeIndex)

		})
	}
}

func TestNodesCount(t *testing.T) {
	mockMetrics := &mockResizableVmMetrics{}
	em := experiments.NewMockManagerWithOptions(version.Version{}, nil, nil)
	bpChecker := &balloonPodChecker{isBalloonPodCreatable: true}

	ekProvider, _ := newEkAutoprovisioningProvider(string(resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize), em, bpChecker, true, true, mockMetrics)
	e4Provider := newE4AutoprovisioningProvider(em, bpChecker, true, true, mockMetrics)
	e4aProvider, _ := newE4aAutoprovisioningProvider(string(resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize), em, bpChecker, true, true, mockMetrics)

	provider := &ResizableVmAutoprovisioningProvider{
		machineConfigProvider:       machinetypes.NewMachineConfigProvider(nil),
		ekAutoprovisioningProvider:  ekProvider,
		e4AutoprovisioningProvider:  e4Provider,
		e4aAutoprovisioningProvider: e4aProvider,
	}

	countProvider := &mockNodesCountProvider{}
	countProvider.On("NodesCount", machinetypes.EK.Name()).Return(5)
	countProvider.On("NodesCount", machinetypes.E4.Name()).Return(4)
	countProvider.On("NodesCount", machinetypes.E4A.Name()).Return(3)
	provider.RegisterNodesCountProvider(countProvider)

	assert.Equal(t, 5, provider.NodesCount(machinetypes.EK.Name()))
	assert.Equal(t, 4, provider.NodesCount(machinetypes.E4.Name()))
	assert.Equal(t, 3, provider.NodesCount(machinetypes.E4A.Name()))
}

func TestHasActiveResizableNodes(t *testing.T) {
	mockMetrics := &mockResizableVmMetrics{}
	mockMetrics.On("UpdateResizableVmLaunchStatus", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("UpdateResizableVmAutopilotComputeClassStatus", mock.Anything, mock.Anything).Return()
	em := experiments.NewMockManagerWithOptions(version.Version{}, nil, nil)
	bpChecker := &balloonPodChecker{isBalloonPodCreatable: true}

	tests := []struct {
		name           string
		ekMode         string
		e4aMode        string
		ekNodes        int
		e4aNodes       int
		expectedResult bool
	}{
		{
			name:           "EK has nodes, E4A has nodes -> true",
			ekMode:         string(resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize),
			e4aMode:        string(resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize),
			ekNodes:        5,
			e4aNodes:       3,
			expectedResult: true,
		},
		{
			name:           "EK has nodes, E4A has no nodes -> true",
			ekMode:         string(resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize),
			e4aMode:        string(resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize),
			ekNodes:        5,
			e4aNodes:       0,
			expectedResult: true,
		},
		{
			name:           "EK has no nodes, E4A has nodes -> true",
			ekMode:         string(resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize),
			e4aMode:        string(resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize),
			ekNodes:        0,
			e4aNodes:       3,
			expectedResult: true,
		},
		{
			name:           "Neither has nodes -> false",
			ekMode:         string(resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize),
			e4aMode:        string(resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize),
			ekNodes:        0,
			e4aNodes:       0,
			expectedResult: false,
		},
		{
			name:           "EK has nodes but disabled -> false",
			ekMode:         string(resizable_vm_types.EkAutoprovisioningDisabled),
			e4aMode:        string(resizable_vm_types.E4aAutoprovisioningEnabledCoarseGrainedResize),
			ekNodes:        5,
			e4aNodes:       0,
			expectedResult: false,
		},
		{
			name:           "E4A has nodes but disabled -> false",
			ekMode:         string(resizable_vm_types.EkAutoprovisioningEnabledCoarseGrainedResize),
			e4aMode:        string(resizable_vm_types.E4aAutoprovisioningDisabled),
			ekNodes:        0,
			e4aNodes:       3,
			expectedResult: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ekProvider, _ := newEkAutoprovisioningProvider(tc.ekMode, em, bpChecker, true, true, mockMetrics)
			e4Provider := newE4AutoprovisioningProvider(em, bpChecker, true, true, mockMetrics)
			e4aProvider, _ := newE4aAutoprovisioningProvider(tc.e4aMode, em, bpChecker, true, true, mockMetrics)

			provider := &ResizableVmAutoprovisioningProvider{
				machineConfigProvider:       machinetypes.NewMachineConfigProvider(nil),
				ekAutoprovisioningProvider:  ekProvider,
				e4AutoprovisioningProvider:  e4Provider,
				e4aAutoprovisioningProvider: e4aProvider,
			}

			countProvider := &mockNodesCountProvider{}
			provider.RegisterNodesCountProvider(countProvider)
			provider.Refresh()

			countProvider.On("NodesCount", machinetypes.EK.Name()).Return(tc.ekNodes)
			countProvider.On("NodesCount", machinetypes.E4.Name()).Return(0)
			countProvider.On("NodesCount", machinetypes.E4A.Name()).Return(tc.e4aNodes)

			assert.Equal(t, tc.expectedResult, provider.HasActiveResizableNodes())
		})
	}
}
