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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	ca_taints "k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	client_testing "k8s.io/client-go/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	ek_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	calculator_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator/test"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/kubernetes/pkg/util/taints"
	clock "k8s.io/utils/clock/testing"
)

const (
	giBToKiB   = int64(1024 * 1024)
	giBToBytes = int64(1024 * 1024 * 1024)
	miBToKiB   = int64(1024)

	testResizableNodeName       = "resizable-test"
	testResizableNodeProviderID = "gce://project1/us-central1-b/node1"

	fixerInterval = 10 * time.Second
)

var (
	testStartTime           = time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC)
	supportedMachineType    = fmt.Sprintf("%s-%s-%s", machinetypes.EK.Name(), "standard", "32")
	notSupportedMachineType = fmt.Sprintf("%s-%s-%s", machinetypes.EK.Name(), "notsupported", "32")
	nonEkMachineType        = fmt.Sprintf("%s-%s-%s", "xx", "notsupported", "32")
)

func TestOnUpdateNode(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			supportedMachineType := fmt.Sprintf("%s-%s-%s", family, "standard", "32")
			cloudProvider := &mockCloudProvider{}
			cloudProvider.On("GetCurrentResizableVmState", mock.Anything).Return(ekvmtypes.ResizableVmState{
				Size:   size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB},
				Status: ekvmtypes.ResizeStatusAtIntent,
			}, nil)
			cloudProvider.On("MachineConfigProvider", mock.Anything).Return(machinetypes.NewMachineConfigProvider(nil))
			testClock := clock.NewFakeClock(testStartTime)
			metrics := &mockMetrics{}
			mockBackoff := &mockBackoff{}
			mockBackoff.On("DeleteNode", family, testResizableNodeName).Once()
			nodeStateManager := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, mockBackoff, &identitySizeCalculator{}, testClock)
			ot := newOperationTracker(nil, nil, cloudProvider, nodeStateManager, metrics, &identitySizeCalculator{}, 1, false, fixerInterval, testClock)

			node := ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 8000, 32).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build()
			ekNode := ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 1000, 1).Build()
			bPod := mustGenerateRunningBalloonPod(t, ekNode, 2000, 2*giBToBytes)

			mockBalloonPodResizer := &mockBalloonPodResizer{}
			mockBalloonPodResizer.On("addTaint", mock.Anything, mock.Anything).Return(node, nil)
			mockBalloonPodResizer.On("hasTaint", mock.Anything).Return(false)
			mockBalloonPodResizer.On("removeTaint", mock.Anything).Return(node, nil)
			mockBalloonPodResizer.On("init").Return(nil)
			mockBalloonPodResizer.On("getPodForNode", mock.Anything).Return(bPod)
			mockBalloonPodResizer.On("listAllBalloonPods", mock.Anything).Return([]*v1.Pod{bPod}, nil)
			ot.balloonPodResizer = mockBalloonPodResizer

			ot.onAddNode(node)
			ot.waitingOnAdd.Wait()

			newNode := node.DeepCopy()
			newNode.SetLabels(map[string]string{v1.LabelInstanceTypeStable: supportedMachineType, "random": "string"})
			ot.onUpdateNode(node, newNode)

			assert.Equal(t, 1, nodeStateManager.nodesCount(family))
			assert.Equal(t, map[string]string{v1.LabelInstanceTypeStable: supportedMachineType, "random": "string"}, nodeStateManager.snapshot(true)[newNode.Name].Node.Labels)

			// Adding ToBeDeletedTaint removes node from ot.nodes cache.
			toBeDeletedNode := newNode.DeepCopy()
			toBeDeletedNode.Spec.Taints = append(toBeDeletedNode.Spec.Taints, apiv1.Taint{
				Key:    ca_taints.ToBeDeletedTaint,
				Value:  fmt.Sprint(time.Now().Unix()),
				Effect: apiv1.TaintEffectNoSchedule,
			})
			ot.onUpdateNode(newNode, toBeDeletedNode)
			assert.Equal(t, 0, nodeStateManager.nodesCount(family))
			mockBackoff.AssertExpectations(t)

			// Node with ToBeDeletedTaint are not added to ot.ekNodes cache.
			mockBackoff.On("DeleteNode", family, testResizableNodeName).Once()
			ot.onUpdateNode(newNode, toBeDeletedNode)
			ot.waitingOnAdd.Wait()
			assert.Equal(t, 0, nodeStateManager.nodesCount(family))
		})
	}
}

func TestOnAddNode(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			emptySize := size.VmSize{}
			ekNode := test.BuildTestNode(testResizableNodeName, 1000, 1024*1024)
			bPod := mustGenerateRunningBalloonPod(t, ekNode, 2000, 2*giBToBytes)
			gceRef, err := gce.GceRefFromProviderId(testResizableNodeProviderID)
			assert.NoError(t, err)
			defaultGceSchedulingSize := size.VmSize{MilliCpus: 32000, KBytes: 128 * giBToKiB}
			supportedMachineType := fmt.Sprintf("%s-%s-%s", family, "standard", "32")
			notSupportedMachineType := fmt.Sprintf("%s-%s-%s", family, "notsupported", "32")

			testCases := []struct {
				desc                            string
				node                            *v1.Node
				bPod                            *v1.Pod
				resizeErr                       error
				setLabelsAfterOnAddNode         map[string]string
				gceSchedulingSize               size.VmSize
				cachedCurrentEkVmStates         map[gce.GceRef]ekvmtypes.ResizableVmState
				expectedEkNodes                 ResizableNodesSnapshot
				expectedResizeBalloonPod        bool
				expectedCachedCurrentEkVmStates map[gce.GceRef]ekvmtypes.ResizableVmState
				expectedUnknownStateNodes       []string
			}{
				{
					desc:                            "node with no instance label",
					node:                            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 1000, 1).WithProvider(testResizableNodeProviderID).Build(),
					expectedEkNodes:                 ResizableNodesSnapshot{},
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{},
				},
				{
					desc:                            "node with non-ek instance label",
					node:                            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 1000, 1).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(nonEkMachineType).Build(),
					expectedEkNodes:                 ResizableNodesSnapshot{},
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{},
				},
				{
					desc:                            "node with resizable instance label, but not not ready",
					node:                            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 1000, 1).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).Build(),
					expectedEkNodes:                 ResizableNodesSnapshot{},
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{},
				},
				{
					desc:                            "node with not supported resizable machine type",
					node:                            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 1000, 1).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(notSupportedMachineType).WithReadyStatus().Build(),
					expectedEkNodes:                 ResizableNodesSnapshot{},
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{},
				},
				{
					desc:                            "resizable - without balloon pod - no cache - no gce response",
					node:                            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
					expectedEkNodes:                 ResizableNodesSnapshot{},
					expectedResizeBalloonPod:        false,
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{},
				},
				{
					desc: "new resizable node - without balloon pod",
					node: ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
					expectedEkNodes: ResizableNodesSnapshot{
						testResizableNodeName: ResizableNode{
							DesiredSize:     size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							Node:            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
							MachineFamily:   family,
						},
					},
					gceSchedulingSize:               defaultGceSchedulingSize,
					expectedResizeBalloonPod:        true,
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 32000, KBytes: 128 * giBToKiB}, Status: ekvmtypes.ResizeStatusAtIntent}},
				},
				{
					desc: "old downsized resizable node - without balloon pod",
					node: ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
					expectedEkNodes: ResizableNodesSnapshot{
						testResizableNodeName: ResizableNode{
							DesiredSize:     size.Allocatable{MilliCpus: 3000, KBytes: 12 * giBToKiB},
							PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							Node:            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
							MachineFamily:   family,
						},
					},
					gceSchedulingSize:               size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB},
					expectedResizeBalloonPod:        true,
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB}, Status: ekvmtypes.ResizeStatusAtIntent}},
				},
				{
					desc:                    "old downsized resizable - without balloon pod - machine type label is added after onAddNode",
					node:                    ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithReadyStatus().Build(),
					setLabelsAfterOnAddNode: map[string]string{v1.LabelInstanceTypeStable: supportedMachineType},
					expectedEkNodes: ResizableNodesSnapshot{
						testResizableNodeName: ResizableNode{
							DesiredSize:     size.Allocatable{MilliCpus: 3000, KBytes: 12 * giBToKiB},
							PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							Node:            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
							MachineFamily:   family,
						},
					},
					gceSchedulingSize:               size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB},
					expectedResizeBalloonPod:        true,
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB}, Status: ekvmtypes.ResizeStatusAtIntent}},
				},
				{
					desc:                    "old downsized resizable - without balloon pod - random label is added after onAddNode",
					node:                    ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
					setLabelsAfterOnAddNode: map[string]string{v1.LabelInstanceTypeStable: supportedMachineType, "random": "string"},
					expectedEkNodes: ResizableNodesSnapshot{
						testResizableNodeName: ResizableNode{
							DesiredSize:     size.Allocatable{MilliCpus: 3000, KBytes: 12 * giBToKiB},
							PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							Node:            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().WithLabel("random", "string").Build(),
							MachineFamily:   family,
						},
					},
					gceSchedulingSize:               size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB},
					expectedResizeBalloonPod:        true,
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB}, Status: ekvmtypes.ResizeStatusAtIntent}},
				},
				{
					desc: "resizable - with balloon pod",
					node: ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 8000, 32).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
					bPod: bPod,
					expectedEkNodes: ResizableNodesSnapshot{
						testResizableNodeName: ResizableNode{
							DesiredSize:     size.Allocatable{MilliCpus: 6000, KBytes: 30 * giBToKiB},
							PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							Node:            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 8000, 32).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
							MachineFamily:   family,
						},
					},
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 8000, KBytes: 40 * giBToKiB}, Status: ekvmtypes.ResizeStatusAtIntent}},
				},
				{
					desc: "resizable - with balloon pod - cache available with error status -> fix cache size to balloon pod and register unknown state node",
					node: ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 8000, 32).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
					bPod: bPod,
					cachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{
						gceRef: {
							Size:   size.VmSize{MilliCpus: 9000, KBytes: 41 * giBToKiB},
							Status: ekvmtypes.ResizeStatusGuestAgentError,
						},
					},
					expectedEkNodes: ResizableNodesSnapshot{
						testResizableNodeName: ResizableNode{
							DesiredSize:     size.Allocatable{MilliCpus: 6000, KBytes: 30 * giBToKiB},
							PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							Node:            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 8000, 32).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
							MachineFamily:   family,
						},
					},
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 8000, KBytes: 40 * giBToKiB}, Status: ekvmtypes.ResizeStatusGuestAgentError}},
					expectedUnknownStateNodes:       []string{testResizableNodeName},
				},
				{
					desc:      "resizable - balloon pod creation err",
					node:      ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
					resizeErr: fmt.Errorf("resize error"),
					expectedEkNodes: ResizableNodesSnapshot{
						testResizableNodeName: ResizableNode{
							DesiredSize:     size.Allocatable{MilliCpus: 3000, KBytes: 12 * giBToKiB},
							PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							Node:            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
							MachineFamily:   family,
						},
					},
					gceSchedulingSize:               size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB},
					expectedResizeBalloonPod:        true,
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB}, Status: ekvmtypes.ResizeStatusAtIntent}},
				},
				{
					desc:                    "resizable - without balloon pod - cache available with at intent status",
					node:                    ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32000, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
					cachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB}, Status: ekvmtypes.ResizeStatusAtIntent}},
					expectedEkNodes: ResizableNodesSnapshot{
						testResizableNodeName: ResizableNode{
							DesiredSize:     size.Allocatable{MilliCpus: 3000, KBytes: 12 * giBToKiB},
							PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							Node:            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32000, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
							MachineFamily:   family,
						},
					},
					expectedResizeBalloonPod:        true,
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB}, Status: ekvmtypes.ResizeStatusAtIntent}},
				},
				{
					desc:                    "resizable - without balloon pod - cache available with error status",
					node:                    ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32000, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
					cachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB}, Status: ekvmtypes.ResizeStatusGuestAgentError}},
					expectedEkNodes: ResizableNodesSnapshot{
						testResizableNodeName: ResizableNode{
							DesiredSize:     size.Allocatable{MilliCpus: 3000, KBytes: 12 * giBToKiB},
							PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 96 * giBToKiB},
							Node:            ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 32000, 128).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build(),
							MachineFamily:   family,
						},
					},
					expectedResizeBalloonPod:        true,
					expectedCachedCurrentEkVmStates: map[gce.GceRef]ekvmtypes.ResizableVmState{gceRef: {Size: size.VmSize{MilliCpus: 4000, KBytes: 16 * giBToKiB}, Status: ekvmtypes.ResizeStatusGuestAgentError}},
					expectedUnknownStateNodes:       []string{testResizableNodeName},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					waitForBalloonPod := make(chan bool, 1)
					mockBalloonPodResizer := &mockBalloonPodResizer{}
					mockBalloonPodResizer.On("init").Return(nil)
					mockBalloonPodResizer.On("getPodForNode", mock.Anything).Return(tc.bPod)
					if tc.bPod != nil {
						mockBalloonPodResizer.On("listAllBalloonPods", mock.Anything).Return([]*v1.Pod{tc.bPod}, nil)
					} else {
						mockBalloonPodResizer.On("listAllBalloonPods", mock.Anything).Return([]*v1.Pod{}, nil)
					}
					if tc.expectedResizeBalloonPod {
						mockBalloonPodResizer.On("resizeBalloonPod", mock.Anything, tc.expectedEkNodes[tc.node.Name].DesiredSize).
							Run(func(_ mock.Arguments) {
								waitForBalloonPod <- true
							}).Return(tc.resizeErr).Once()
					}
					mockBalloonPodResizer.On("addTaint", mock.Anything, mock.Anything).Return(tc.node, nil)
					mockBalloonPodResizer.On("hasTaint", mock.Anything).Return(false)
					mockBalloonPodResizer.On("removeTaint", mock.Anything).Return(tc.node, nil)
					metrics := &mockMetrics{}
					metrics.On("RegisterResizableVmFixerEvents", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
					cloudProvider := &mockCloudProvider{}
					if tc.gceSchedulingSize == emptySize {
						cloudProvider.On("GetCurrentResizableVmState", mock.Anything).Return(ekvmtypes.ResizableVmState{}, fmt.Errorf("no size"))
					} else {
						cloudProvider.On("GetCurrentResizableVmState", mock.Anything).Return(ekvmtypes.ResizableVmState{
							Size:   tc.gceSchedulingSize,
							Status: ekvmtypes.ResizeStatusAtIntent,
						}, nil)
					}
					cloudProvider.On("MachineConfigProvider", mock.Anything).Return(machinetypes.NewMachineConfigProvider(nil))
					testClock := clock.NewFakeClock(testStartTime)

					ekCalculator := calculator_test.New()
					nodeStateManager := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, nil, ekCalculator, testClock)
					ot := newOperationTracker(nil, nil, cloudProvider, nodeStateManager, metrics, ekCalculator, 1, false, fixerInterval, testClock)
					ot.balloonPodResizer = mockBalloonPodResizer
					if tc.cachedCurrentEkVmStates != nil {
						ot.vmStateCache.vmStates = tc.cachedCurrentEkVmStates
					}

					ot.onAddNode(tc.node)
					// Sync internal goroutines
					ot.waitingOnAdd.Wait()

					if tc.setLabelsAfterOnAddNode != nil {
						newNode := tc.node.DeepCopy()
						newNode.SetLabels(tc.setLabelsAfterOnAddNode)
						ot.onUpdateNode(tc.node, newNode)
						tc.node = newNode
					}

					// To sync internal goroutines.
					ot.waitingOnAdd.Wait()

					nodes := nodeStateManager.snapshot(true)
					assert.Equal(t, tc.expectedEkNodes, nodes)
					if tc.expectedResizeBalloonPod {
						<-waitForBalloonPod
					}
					assert.Equal(t, tc.expectedCachedCurrentEkVmStates, ot.vmStateCache.vmStates)
					assert.ElementsMatch(t, tc.expectedUnknownStateNodes, ot.nodeStateManager.getUnhealthyNodesWithStatus(UnknownResizeStatus))
				})
			}
		})
	}
}

func TestOnDeleteNode(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			supportedMachineType := fmt.Sprintf("%s-%s-%s", family, "standard", "32")
			ekNode := test.BuildTestNode(testResizableNodeName, 1000, 1024*1024)
			bPod := mustGenerateRunningBalloonPod(t, ekNode, 2000, 8*giBToBytes)
			node := ekvms_test.NewResizableNodeBuilder(testResizableNodeName, 8000, 32).WithProvider(testResizableNodeProviderID).WithSupportedMachineType(supportedMachineType).WithReadyStatus().Build()
			mockBalloonPodResizer := &mockBalloonPodResizer{}
			mockBalloonPodResizer.On("init").Return(nil)
			mockBalloonPodResizer.On("getPodForNode", mock.Anything).Return(bPod)
			mockBalloonPodResizer.On("listAllBalloonPods", mock.Anything).Return([]*v1.Pod{bPod}, nil)
			mockBalloonPodResizer.On("addTaint", mock.Anything, mock.Anything).Return(node, nil)
			mockBalloonPodResizer.On("hasTaint", mock.Anything).Return(false)
			mockBalloonPodResizer.On("removeTaint", mock.Anything).Return(node, nil)
			mockBalloonPodResizer.On("resizeBalloonPod", mock.Anything, mock.Anything).Return(nil)
			cloudProvider := &mockCloudProvider{}
			vmState := ekvmtypes.ResizableVmState{
				Size:   size.VmSize{MilliCpus: 8000, KBytes: 32 * giBToKiB},
				Status: ekvmtypes.ResizeStatusAtIntent,
			}
			cloudProvider.On("GetCurrentResizableVmState", mock.Anything).Return(vmState, nil)
			cloudProvider.On("MachineConfigProvider", mock.Anything).Return(machinetypes.NewMachineConfigProvider(nil))
			testClock := clock.NewFakeClock(testStartTime)
			metrics := &mockMetrics{}
			metrics.On("RegisterResizableVmFixerEvents", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
			ekCalculator := calculator_test.New()
			mockBackoff := &mockBackoff{}
			mockBackoff.On("DeleteNode", family, node.Name).Once()
			nodeStateManager := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, mockBackoff, ekCalculator, testClock)

			ot := newOperationTracker(nil, nil, cloudProvider, nodeStateManager, metrics, calculator_test.New(), 1, false, fixerInterval, testClock)
			ot.balloonPodResizer = mockBalloonPodResizer
			ot.onAddNode(node)
			ot.waitingOnAdd.Wait()
			expectedEkNodes := ResizableNodesSnapshot{
				node.Name: ResizableNode{
					DesiredSize:     size.Allocatable{MilliCpus: 6000, KBytes: 24 * giBToKiB},
					PhysicalMaxSize: size.Allocatable{MilliCpus: 24000, KBytes: 100663296},
					Node:            node,
					MachineFamily:   family,
				},
			}

			nodes := nodeStateManager.snapshot(true)
			assert.Equal(t, expectedEkNodes, nodes)
			cachedState, err := ot.vmStateCache.getState(node)
			assert.NoError(t, err)
			assert.Equal(t, vmState, cachedState)

			// Removes entry for deleted node.
			ot.onDeleteNode(node)
			assert.Equal(t, 0, nodeStateManager.nodesCount(family))
			_, err = ot.vmStateCache.getState(node)
			assert.Error(t, err) // No cache entry.
			mockBackoff.AssertExpectations(t)
		})
	}
}

func TestResize(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			var (
				bpResizeErr          = fmt.Errorf("BP resize error")
				vmResizeErr          = fmt.Errorf("VM resize error")
				downsizeErr          = fmt.Errorf("downsize error")
				gceUpsizeErr         = ek_errors.NewGuestAgentResizeTimeout(family, fmt.Errorf("gce upsize error"))
				gceDownsizeErr       = ek_errors.NewGuestAgentFailedToResizeError(family, fmt.Errorf("gce downsize error"))
				gceInstanceIsBusyErr = ek_errors.NewInstanceIsBusyError(family, fmt.Errorf("gce resize error"))
			)

			ekNode := test.BuildTestNode("node1", 1000, 1024*1024)
			bPod := mustGenerateRunningBalloonPod(t, ekNode, 10000, 32*giBToBytes)

			testCases := []struct {
				desc                         string
				startingSize                 size.VmSize
				desiredSize                  size.VmSize
				podList                      *v1.PodList
				bpResizeErr                  error
				vmResizeErr                  error
				vmResizeRollbackErr          error
				podListErr                   error
				expectedOnSuccess            int
				expectedOnFailure            int
				expectedVmResizerCallCount   int
				expectedPendingOperations    int
				expectedBalloonResizes       []balloonPodResizeArgs
				expectedResultState          ekvmtypes.ResizableVmState
				expectedPodListCount         int
				expectedRemoveTaintCallCount int
				expectedAddTaintCallCount    int
				expectedReconcileOp          bool
				expectedErr                  error
			}{
				{
					desc:                         "resize_to_same_size",
					startingSize:                 newSize(1000, 1024*giBToKiB),
					desiredSize:                  newSize(1000, 1024*giBToKiB),
					expectedResultState:          ekvmtypes.ResizableVmState{Size: newSize(1000, 1024*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent},
					expectedOnSuccess:            0,
					expectedVmResizerCallCount:   0,
					expectedPendingOperations:    0,
					expectedBalloonResizes:       []balloonPodResizeArgs{},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    0,
				},
				{
					desc:                       "Upsize - successful",
					startingSize:               newSize(1000, 1024*giBToKiB),
					desiredSize:                newSize(2000, 2048*giBToKiB),
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(2000, 2048*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent},
					expectedOnSuccess:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  0,
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size: size.Allocatable{MilliCpus: 1500, KBytes: 1536 * giBToKiB},
						},
					},
					expectedRemoveTaintCallCount: 1,
					expectedAddTaintCallCount:    1,
				},
				{
					desc:                       "Upsize - vm resize non-gce error",
					startingSize:               newSize(1000, 1024*giBToKiB),
					desiredSize:                newSize(2000, 2048*giBToKiB),
					vmResizeErr:                vmResizeErr,
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(1000, 1024*giBToKiB), Status: ekvmtypes.ResizeStatusUnknownCA},
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size: size.Allocatable{MilliCpus: 1500, KBytes: 1536 * giBToKiB},
						},
					},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    1,
					expectedReconcileOp:          true,
					expectedErr:                  vmResizeErr,
				},
				{
					desc:                       "Upsize - vm resize gce error",
					startingSize:               newSize(1000, 1024*giBToKiB),
					desiredSize:                newSize(2000, 2048*giBToKiB),
					vmResizeErr:                gceUpsizeErr,
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(1000, 1024*giBToKiB), Status: ekvmtypes.ResizeStatusUnknownCA},
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size: size.Allocatable{MilliCpus: 1500, KBytes: 1536 * giBToKiB},
						},
					},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    1,
					expectedReconcileOp:          true,
					expectedErr:                  gceUpsizeErr,
				},
				{
					desc:         "Upsize mixed resize - balloon pod resize error",
					startingSize: newSize(1000, 2048*giBToKiB),
					desiredSize:  newSize(2000, 1024*giBToKiB),
					bpResizeErr:  bpResizeErr,
					expectedResultState: ekvmtypes.ResizableVmState{
						Size:   newSize(2000, 2048*giBToKiB), // Only first upsize is executed, and nodeStateManager should reflect that.
						Status: ekvmtypes.ResizeStatusAtIntent},
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size:        size.Allocatable{MilliCpus: 1500, KBytes: 1536 * giBToKiB},
							bpResizeErr: bpResizeErr,
						},
					},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    1,
					expectedErr:                  bpResizeErr,
				},
				{
					desc:                       "Upsize - vm resize gce instance is busy error - ek at starting size and status at intent",
					startingSize:               newSize(1000, 1024*giBToKiB),
					desiredSize:                newSize(2000, 2048*giBToKiB),
					vmResizeErr:                gceInstanceIsBusyErr,
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(1000, 1024*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent},
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size: size.Allocatable{MilliCpus: 1500, KBytes: 1536 * giBToKiB},
						},
					},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    1,
					expectedErr:                  gceUpsizeErr,
				},
				{
					desc:                       "Upsize - balloon pod resize error",
					startingSize:               newSize(1000, 1024*giBToKiB),
					desiredSize:                newSize(2000, 2048*giBToKiB),
					bpResizeErr:                bpResizeErr,
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(2000, 2048*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent}, // Balloon pod resize error during upsize doesn't prevent VM resize from finishing.
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size:        size.Allocatable{MilliCpus: 1500, KBytes: 1536 * giBToKiB},
							bpResizeErr: bpResizeErr,
						},
					},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    1,
					expectedErr:                  bpResizeErr,
				},
				{
					desc:                       "Upsize - both vm and balloon pod resize error",
					startingSize:               newSize(1000, 1024*giBToKiB),
					desiredSize:                newSize(2000, 2048*giBToKiB),
					vmResizeErr:                vmResizeErr,
					bpResizeErr:                bpResizeErr,
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(1000, 1024*giBToKiB), Status: ekvmtypes.ResizeStatusUnknownCA},
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size:        size.Allocatable{MilliCpus: 1500, KBytes: 1536 * giBToKiB},
							bpResizeErr: bpResizeErr,
						},
					},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    1,
					expectedReconcileOp:          true,
					expectedErr:                  vmResizeErr,
				},
				{
					desc:                       "Downsize - successful",
					startingSize:               newSize(2000, 2048*giBToKiB),
					desiredSize:                newSize(1000, 1024*giBToKiB),
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(1000, 1024*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent},
					podList:                    &v1.PodList{Items: []v1.Pod{*test.BuildTestPod("pod1", 500, 500*giBToKiB)}},
					expectedOnSuccess:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  0,
					expectedPodListCount:       1,
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size: size.Allocatable{MilliCpus: 750, KBytes: 768 * giBToKiB},
						},
					},
					expectedRemoveTaintCallCount: 1,
					expectedAddTaintCallCount:    1,
				},
				{
					desc:                       "Downsize - balloon pod is omitted",
					startingSize:               newSize(2000, 2048*giBToKiB),
					desiredSize:                newSize(1000, 1024*giBToKiB),
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(1000, 1024*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent},
					podList:                    &v1.PodList{Items: []v1.Pod{*test.BuildTestPod("pod1", 500, 500*giBToKiB), *bPod}},
					expectedOnSuccess:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  0,
					expectedPodListCount:       1,
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size: size.Allocatable{MilliCpus: 750, KBytes: 768 * giBToKiB},
						},
					},
					expectedRemoveTaintCallCount: 1,
					expectedAddTaintCallCount:    1,
				},
				{
					desc:                       "Downsize - vm resize non-gce error",
					startingSize:               newSize(2000, 2048*giBToKiB),
					desiredSize:                newSize(1000, 1024*giBToKiB),
					vmResizeErr:                vmResizeErr,
					podList:                    &v1.PodList{Items: []v1.Pod{*test.BuildTestPod("pod1", 500, 500*giBToKiB)}},
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(1000, 1024*giBToKiB), Status: ekvmtypes.ResizeStatusUnknownCA},
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedPodListCount:       1,
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size: size.Allocatable{MilliCpus: 750, KBytes: 768 * giBToKiB},
						},
					},
					expectedRemoveTaintCallCount: 1,
					expectedAddTaintCallCount:    1,
					expectedReconcileOp:          true,
					expectedErr:                  vmResizeErr,
				},
				{
					desc:                       "Downsize - vm resize gce error",
					startingSize:               newSize(2000, 2048*giBToKiB),
					desiredSize:                newSize(1000, 1024*giBToKiB),
					vmResizeErr:                gceDownsizeErr,
					podList:                    &v1.PodList{Items: []v1.Pod{*test.BuildTestPod("pod1", 500, 500*giBToKiB)}},
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(1000, 1024*giBToKiB), Status: ekvmtypes.ResizeStatusUnknownCA},
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedPodListCount:       1,
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size: size.Allocatable{MilliCpus: 750, KBytes: 768 * giBToKiB},
						},
					},
					expectedRemoveTaintCallCount: 1,
					expectedAddTaintCallCount:    1,
					expectedReconcileOp:          true,
					expectedErr:                  gceDownsizeErr,
				},
				{
					desc:                       "Downsize - vm resize gce instance is busy error - ek at starting size and status at intent",
					startingSize:               newSize(2000, 2048*giBToKiB),
					desiredSize:                newSize(1000, 1024*giBToKiB),
					vmResizeErr:                gceInstanceIsBusyErr,
					podList:                    &v1.PodList{Items: []v1.Pod{*test.BuildTestPod("pod1", 500, 500*giBToKiB)}},
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(2000, 2048*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent},
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 1,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedPodListCount:       1,
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size: size.Allocatable{MilliCpus: 750, KBytes: 768 * giBToKiB},
						},
					},
					expectedRemoveTaintCallCount: 1,
					expectedAddTaintCallCount:    1,
					expectedErr:                  gceDownsizeErr,
				},
				{
					desc:                       "Downsize - balloon pod resize error",
					startingSize:               newSize(2000, 2048*giBToKiB),
					desiredSize:                newSize(1000, 1024*giBToKiB),
					bpResizeErr:                bpResizeErr,
					podList:                    &v1.PodList{Items: []v1.Pod{*test.BuildTestPod("pod1", 500, 500*giBToKiB)}},
					expectedResultState:        ekvmtypes.ResizableVmState{Size: newSize(2000, 2048*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent},
					expectedOnFailure:          1,
					expectedVmResizerCallCount: 0,
					expectedPendingOperations:  1, // failure clears the queue, and we queue up fix operation.
					expectedPodListCount:       1,
					expectedBalloonResizes: []balloonPodResizeArgs{
						{
							size:        size.Allocatable{MilliCpus: 750, KBytes: 768 * giBToKiB},
							bpResizeErr: bpResizeErr,
						},
					},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    1,
					expectedErr:                  bpResizeErr,
				},
				{
					desc:                         "Downsize - pod listing error",
					startingSize:                 newSize(2000, 2048*giBToKiB),
					desiredSize:                  newSize(1000, 1024*giBToKiB),
					podList:                      &v1.PodList{Items: []v1.Pod{*test.BuildTestPod("pod1", 500, 500*giBToKiB)}},
					podListErr:                   downsizeErr,
					expectedResultState:          ekvmtypes.ResizableVmState{Size: newSize(2000, 2048*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent},
					expectedOnFailure:            1,
					expectedPendingOperations:    1, // failure clears the queue, and we queue up fix operation.
					expectedPodListCount:         1,
					expectedBalloonResizes:       []balloonPodResizeArgs{},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    1,
					expectedErr:                  downsizeErr,
				},
				{
					desc:                         "Downsize - pod requests bigger than desired size",
					startingSize:                 newSize(2000, 2048*giBToKiB),
					desiredSize:                  newSize(1000, 1024*giBToKiB),
					podList:                      &v1.PodList{Items: []v1.Pod{*test.BuildTestPod("pod1", 1500, 1500*giBToBytes)}},
					expectedResultState:          ekvmtypes.ResizableVmState{Size: newSize(2000, 2048*giBToKiB), Status: ekvmtypes.ResizeStatusAtIntent},
					expectedOnFailure:            1,
					expectedVmResizerCallCount:   0,
					expectedPendingOperations:    1, // failure clears the queue, and we queue up fix operation.
					expectedPodListCount:         1,
					expectedBalloonResizes:       []balloonPodResizeArgs{},
					expectedRemoveTaintCallCount: 0,
					expectedAddTaintCallCount:    1,
					expectedErr:                  downsizeErr,
				},
			}

			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					// given
					sizeCalc := calculator_test.New()
					node := test.BuildTestNode("node1", tc.startingSize.MilliCpus, tc.startingSize.KBytes*1024)
					node.Spec.ProviderID = "gce://project1/us-central1-b/node1"
					ro := ResizeOperation{
						NodeName:     node.Name,
						StartingSize: tc.startingSize,
						DesiredSize:  tc.desiredSize,
					}
					fakeClient := &fake.Clientset{}
					fakeClient.AddReactor("list", "pods", func(_ client_testing.Action) (bool, runtime.Object, error) { return true, tc.podList, tc.podListErr })
					mockMetrics := &mockMetrics{}
					mockMetrics.On("ObserveVmGceResizeRequestDuration", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
					mockMetrics.On("RegisterVmResizeOperation", mock.Anything, mock.Anything, mock.Anything, metrics.OperationSucceeded).Times(tc.expectedOnSuccess).Return()
					mockMetrics.On("RegisterVmResizeOperation", mock.Anything, mock.Anything, mock.Anything, metrics.OperationFailed).Times(tc.expectedOnFailure).Return()
					testClock := clock.NewFakeClock(testStartTime)
					mockBackoff := &mockBackoff{}
					mockBackoff.On("Backoff", mock.Anything, mock.Anything).Return()
					nsm := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, mockBackoff, sizeCalc, testClock)
					nsm.setNode(node.Name, ResizableNode{Node: node, MachineFamily: family})

					var vmResizerCalls []vmResizerArgs
					op := newOperationTracker(fakeClient, informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), vmResizerFunc(func(ctx context.Context, node *v1.Node, actualDesiredSize size.VmSize) error {
						if len(vmResizerCalls) == 0 {
							if !tc.desiredSize.IsDownsizeFrom(tc.startingSize) && !tc.desiredSize.IsUpsizeFrom(tc.startingSize) {
								firstUpsize := size.MaxSize(tc.startingSize, tc.desiredSize)
								vmResizerCalls = append(vmResizerCalls, vmResizerArgs{expectedSize: firstUpsize, actualSize: actualDesiredSize})
							} else {
								vmResizerCalls = append(vmResizerCalls, vmResizerArgs{expectedSize: tc.desiredSize, actualSize: actualDesiredSize})

							}
							return tc.vmResizeErr
						}
						vmResizerCalls = append(vmResizerCalls, vmResizerArgs{expectedSize: tc.startingSize, actualSize: actualDesiredSize})
						return tc.vmResizeRollbackErr
					}), nsm, mockMetrics, sizeCalc, 1, false, fixerInterval, testClock)
					// Override resizer
					mockBalloonPodResizer := &mockBalloonPodResizer{}
					mockBalloonPodResizer.On("init").Return(nil)
					for _, resizeArgs := range tc.expectedBalloonResizes {
						mockBalloonPodResizer.On("resizeBalloonPod", mock.Anything, resizeArgs.size).Return(resizeArgs.bpResizeErr)
					}
					mockBalloonPodResizer.On("addTaint", mock.Anything, mock.Anything).Return(node, nil)
					mockBalloonPodResizer.On("hasTaint", mock.Anything).Return(false)
					mockBalloonPodResizer.On("removeTaint", mock.Anything).Return(node, nil)
					op.balloonPodResizer = mockBalloonPodResizer

					// Manually set original size for this node
					err := op.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{
						Size:   tc.startingSize,
						Status: ekvmtypes.ResizeStatusAtIntent,
					})
					assert.NoError(t, err)

					// when resize is queued, with another resize also pending
					op.nodeStateManager.setNodeSize(node, op.sizeCalculator.ToAllocatable(node, ro.DesiredSize))
					op.Resize(ro)

					// and when a single operation is processed
					_ = op.processNextOperation()

					// then
					cachedState, err := op.vmStateCache.getState(node)
					assert.NoError(t, err)
					assert.Equal(t, tc.expectedResultState, cachedState)

					expectedAlloc := op.sizeCalculator.ToAllocatable(node, tc.expectedResultState.Size)
					nsm_node, _ := op.nodeStateManager.getNode(node.Name)
					assert.Equal(t, expectedAlloc, nsm_node.DesiredSize)
					assert.Equal(t, tc.expectedVmResizerCallCount, len(vmResizerCalls))
					assert.Len(t, op.opQueue.operationsPerNode[node.Name], tc.expectedPendingOperations)

					// If there's an error, pending operations (except for reconcile) should be cleared and fix operation should be queued up.
					if tc.expectedErr != nil && len(op.opQueue.operationsPerNode[node.Name]) > 0 {
						if tc.expectedReconcileOp {
							assert.NotNil(t, op.opQueue.operationsPerNode[node.Name][0].reconcileNodeState)
						} else {
							assert.NotNil(t, op.opQueue.operationsPerNode[node.Name][0].fix)
						}
					}
					for _, vmResizerCall := range vmResizerCalls {
						assert.Equal(t, vmResizerCall.expectedSize, vmResizerCall.actualSize)
					}
					actions := fakeClient.Actions()
					podListCount := 0
					for _, a := range actions {
						if a.GetVerb() == "list" {
							podListCount++
						}
					}
					assert.Equal(t, tc.expectedPodListCount, podListCount)

					// mockMetrics.AssertExpectations(t)
					mockBalloonPodResizer.AssertNumberOfCalls(t, "resizeBalloonPod", len(tc.expectedBalloonResizes))
					mockBalloonPodResizer.AssertNumberOfCalls(t, "addTaint", tc.expectedAddTaintCallCount)
					mockBalloonPodResizer.AssertNumberOfCalls(t, "removeTaint", tc.expectedRemoveTaintCallCount)
				})
			}
		})
	}
}

func TestUpsize_NonExistingNode(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			sizeCalc := calculator_test.New()

			fakeClient := &fake.Clientset{}
			cloudProvider := &mockCloudProvider{}
			testClock := clock.NewFakeClock(testStartTime)
			metrics := &mockMetrics{}
			nodeStateManager := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, nil, sizeCalc, testClock)

			op := newOperationTracker(fakeClient, informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cloudProvider, nodeStateManager, metrics, sizeCalc, 1, false, fixerInterval, testClock)
			// The method should not panic.
			err := op.upsize(ResizeOperation{
				NodeName:     "node1",
				StartingSize: newSize(2000, 2048*giBToKiB),
				DesiredSize:  newSize(4000, 4096*giBToKiB),
			})
			assert.NoError(t, err)
		})
	}
}

func TestDownsize_NonExistingNode(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			sizeCalc := calculator_test.New()

			fakeClient := &fake.Clientset{}
			cloudProvider := &mockCloudProvider{}
			testClock := clock.NewFakeClock(testStartTime)
			metrics := &mockMetrics{}
			nodeStateManager := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, nil, sizeCalc, testClock)

			op := newOperationTracker(fakeClient, informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cloudProvider, nodeStateManager, metrics, sizeCalc, 1, false, fixerInterval, testClock)
			// The method should not panic.
			err := op.downsize(ResizeOperation{
				NodeName:     "node1",
				StartingSize: newSize(4000, 4096*giBToKiB),
				DesiredSize:  newSize(2000, 2048*giBToKiB),
			})
			assert.NoError(t, err)
		})
	}
}

func TestReconcileNodeStateOperation(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			supportedMachineType := fmt.Sprintf("%s-%s-%s", family, "standard", "32")
			nodeMilliCpu := int64(10 * 1000)
			nodeMem := int64(10 * size.GiB)
			node := test.WithAllocatable(test.BuildTestNode("node1", nodeMilliCpu, nodeMem), nodeMilliCpu*3/4, nodeMem*3/4)
			node.Spec.ProviderID = "gce://project1/us-central1-b/node1"
			node.SetLabels(map[string]string{v1.LabelInstanceTypeStable: supportedMachineType})

			vmCurrentSize := size.VmSize{MilliCpus: nodeMilliCpu, KBytes: nodeMem/size.KiB - 100*miBToKiB}

			testCases := []struct {
				desc               string
				resizeReturnValues []error
				ekReturnedState    ekvmtypes.ResizableVmState
				wantFinalOpInQueue []operation
				wantIsUnhealthy    bool
				wantMetricStatus   reconcileNodeStateStatus
			}{
				{
					desc:               "node already is at intent",
					resizeReturnValues: []error{},
					ekReturnedState: ekvmtypes.ResizableVmState{
						Size:   vmCurrentSize,
						Status: ekvmtypes.ResizeStatusAtIntent,
					},
					wantFinalOpInQueue: []operation{},
					wantIsUnhealthy:    false,
					wantMetricStatus:   reconcileNodeStateSuccess,
				},
				{
					desc:               "node already is in progress",
					resizeReturnValues: []error{}, // Empty array means no resize occurs.
					ekReturnedState: ekvmtypes.ResizableVmState{
						Size:   vmCurrentSize,
						Status: ekvmtypes.ResizeStatusInProgress,
					},
					wantFinalOpInQueue: []operation{
						{
							reconcileNodeState: &reconcileNodeStateOperation{
								nodeName:   node.Name,
								targetSize: vmCurrentSize,
								attempts:   1,
							},
						},
					},
					wantIsUnhealthy:  true,
					wantMetricStatus: reconcileNodeStateInProgress,
				},
				{
					desc:               "node is in error state - error during resize",
					resizeReturnValues: []error{fmt.Errorf("resize error")},
					ekReturnedState: ekvmtypes.ResizableVmState{
						Size:   vmCurrentSize,
						Status: ekvmtypes.ResizeStatusGuestAgentError,
					},
					wantFinalOpInQueue: []operation{
						{
							reconcileNodeState: &reconcileNodeStateOperation{
								nodeName:   node.Name,
								targetSize: vmCurrentSize,
								attempts:   1,
							},
						},
					},
					wantIsUnhealthy:  true,
					wantMetricStatus: reconcileNodeStateFailure,
				},
				{
					desc:               "node is in error state - resize is successful",
					resizeReturnValues: []error{nil},
					ekReturnedState: ekvmtypes.ResizableVmState{
						Size:   vmCurrentSize,
						Status: ekvmtypes.ResizeStatusGuestAgentError,
					},
					wantFinalOpInQueue: []operation{},
					wantIsUnhealthy:    false,
					wantMetricStatus:   reconcileNodeStateSuccess,
				},
			}

			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					// Given
					sizeCalc := calculator_test.New() // Allocatable is 3/4 of VMSize

					node := node.DeepCopy()

					fakeClient := &fake.Clientset{}
					cloudProvider := &mockCloudProvider{}
					for _, resizeErr := range tc.resizeReturnValues {
						cloudProvider.On("ResizeVm", mock.Anything, mock.Anything, mock.Anything).Return(resizeErr).Once()
					}
					cloudProvider.On("GetCurrentResizableVmState", mock.Anything).Return(tc.ekReturnedState, nil)
					metrics := &mockMetrics{}
					metrics.On("ObserveVmGceResizeRequestDuration", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
					metrics.On("RegisterResizableVmFixerEvents", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
					metrics.On("RegisterResizableVmReconcileNodeStateEvents", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
					testClock := clock.NewFakeClock(testStartTime)
					nsm := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, nil, sizeCalc, testClock)
					nsm.setNode(node.Name, ResizableNode{Node: node, MachineFamily: family})
					op := newOperationTracker(fakeClient, informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cloudProvider, nsm, metrics, sizeCalc, 1, false, fixerInterval, testClock)

					// Manually cache EK size, this would normally be done during
					// operation tracker initialization and after successful resizes.
					err1 := op.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{
						Size:   vmCurrentSize,
						Status: ekvmtypes.ResizeStatusAtIntent,
					})
					assert.NoError(t, err1)

					op.reconcileNodeStateRequeueBackoff = 0
					op.registerNodeToBeReconciled(node.Name, vmCurrentSize)
					assert.True(t, op.nodeStateManager.isUnhealthy(node.Name))

					opFromQueue, _ := op.opQueue.Get()
					op.handleReconcileNodeStateOperation(*opFromQueue.reconcileNodeState)

					metrics.AssertCalled(t, "RegisterResizableVmReconcileNodeStateEvents", family, 1, string(tc.wantMetricStatus), len(tc.wantFinalOpInQueue) > 0)

					cloudProvider.AssertNumberOfCalls(t, "ResizeVm", len(tc.resizeReturnValues))
					assert.Equal(t, tc.wantFinalOpInQueue, op.opQueue.operationsPerNode[node.Name])
					assert.Equal(t, tc.wantIsUnhealthy, op.nodeStateManager.isUnhealthy(node.Name))
				})
			}
		})
	}
}

func TestFix(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			var (
				bpResizeErr    = fmt.Errorf("BP resize error")
				addTaintErr    = fmt.Errorf("add taint error")
				removeTaintErr = fmt.Errorf("remove taint error")
				nilNode        *v1.Node
			)
			supportedMachineType := fmt.Sprintf("%s-%s-%s", family, "standard", "32")
			nodeMilliCpu := int64(10 * 1000)
			nodeMem := int64(10 * size.GiB)
			node := test.WithAllocatable(test.BuildTestNode("node1", nodeMilliCpu, nodeMem), nodeMilliCpu*3/4, nodeMem*3/4)
			node.Spec.ProviderID = "gce://project1/us-central1-b/node1"
			node.SetLabels(map[string]string{v1.LabelInstanceTypeStable: supportedMachineType})

			vmCurrentSize := size.VmSize{MilliCpus: nodeMilliCpu, KBytes: nodeMem/size.KiB - 100*miBToKiB}

			bPod1 := mustGenerateRunningBalloonPod(t, node, (nodeMilliCpu-vmCurrentSize.MilliCpus)*3/4, (nodeMem-vmCurrentSize.KBytes*size.KiB)*3/4)
			bPod2 := mustGenerateRunningBalloonPod(t, node, (nodeMilliCpu-vmCurrentSize.MilliCpus)*3/4, (nodeMem-vmCurrentSize.KBytes*size.KiB)*3/4)
			bPodWithIncorrectSize := mustGenerateRunningBalloonPod(t, node, vmCurrentSize.MilliCpus, vmCurrentSize.KBytes*size.KiB)

			bPodFailing, err := GenerateBalloonPod(
				node,
				*resource.NewMilliQuantity((nodeMilliCpu-vmCurrentSize.MilliCpus)*3/4, resource.DecimalSI),
				*resource.NewQuantity((nodeMem-vmCurrentSize.KBytes*size.KiB)*3/4, resource.DecimalSI),
				false,
			)
			assert.NoError(t, err)
			bPodFailing.Status.Phase = apiv1.PodFailed

			testCases := []struct {
				desc                               string
				bPods                              []*v1.Pod
				taintsOnNode                       []*v1.Taint
				targetSize                         *size.VmSize
				bpResizeErr                        error
				addTaintErr                        error
				removeTaintErr                     error
				expectedBalloonResizes             []size.Allocatable
				expectedAddResizeTaintCallCount    int
				expectedRemoveResizeTaintCallCount int
				expectedErrContains                error
				expectedDownsize                   *size.VmSize
			}{
				{
					desc:  "0 balloon pods -> Fix",
					bPods: []*v1.Pod{},
					expectedBalloonResizes: []size.Allocatable{
						{MilliCpus: vmCurrentSize.MilliCpus * 3 / 4, KBytes: vmCurrentSize.KBytes * 3 / 4},
					},
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 1,
				},
				{
					desc:        "0 balloon pods with resize error -> Returns error",
					bPods:       []*v1.Pod{},
					bpResizeErr: bpResizeErr,
					expectedBalloonResizes: []size.Allocatable{
						{MilliCpus: vmCurrentSize.MilliCpus * 3 / 4, KBytes: vmCurrentSize.KBytes * 3 / 4},
					},
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 0,
					expectedErrContains:                bpResizeErr,
				},
				{
					desc:                               "0 balloon pods with adding label error -> Returns error",
					bPods:                              []*v1.Pod{},
					addTaintErr:                        addTaintErr,
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 0,
					expectedErrContains:                addTaintErr,
				},
				{
					desc:           "0 balloon pods with removing label error -> Returns error",
					bPods:          []*v1.Pod{},
					removeTaintErr: removeTaintErr,
					expectedBalloonResizes: []size.Allocatable{
						{MilliCpus: vmCurrentSize.MilliCpus * 3 / 4, KBytes: vmCurrentSize.KBytes * 3 / 4},
					},
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 1,
					expectedErrContains:                removeTaintErr,
				},
				{
					desc:  "1 balloon pod -> Do nothing",
					bPods: []*v1.Pod{bPod1},
				},
				{
					desc:  "2 balloon pods -> Fix",
					bPods: []*v1.Pod{bPod1, bPod2},
					expectedBalloonResizes: []size.Allocatable{
						{MilliCpus: vmCurrentSize.MilliCpus * 3 / 4, KBytes: vmCurrentSize.KBytes * 3 / 4},
					},
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 1,
				},
				{
					desc:  "balloon pod with incorrect size -> Fix",
					bPods: []*v1.Pod{bPodWithIncorrectSize},
					expectedBalloonResizes: []size.Allocatable{
						{MilliCpus: vmCurrentSize.MilliCpus * 3 / 4, KBytes: vmCurrentSize.KBytes * 3 / 4},
					},
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 1,
				},
				{
					desc:        "balloon pod with incorrect size - resize error occurs -> Returns error",
					bPods:       []*v1.Pod{bPodWithIncorrectSize},
					bpResizeErr: bpResizeErr,
					expectedBalloonResizes: []size.Allocatable{
						{MilliCpus: vmCurrentSize.MilliCpus * 3 / 4, KBytes: vmCurrentSize.KBytes * 3 / 4},
					},
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 0,
					expectedErrContains:                bpResizeErr,
				},
				{
					desc:                               "balloon pod with incorrect size - adding label error occurs -> Returns error",
					bPods:                              []*v1.Pod{bPodWithIncorrectSize},
					addTaintErr:                        addTaintErr,
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 0,
					expectedErrContains:                addTaintErr,
				},
				{
					desc:           "balloon pod with incorrect size - removing label error occurs -> Returns error",
					bPods:          []*v1.Pod{bPodWithIncorrectSize},
					removeTaintErr: removeTaintErr,
					expectedBalloonResizes: []size.Allocatable{
						{MilliCpus: vmCurrentSize.MilliCpus * 3 / 4, KBytes: vmCurrentSize.KBytes * 3 / 4},
					},
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 1,
					expectedErrContains:                removeTaintErr,
				},
				{
					desc:  "failing balloon pod -> Fix",
					bPods: []*v1.Pod{bPodFailing},
					expectedBalloonResizes: []size.Allocatable{
						{MilliCpus: vmCurrentSize.MilliCpus * 3 / 4, KBytes: vmCurrentSize.KBytes * 3 / 4},
					},
					expectedAddResizeTaintCallCount:    1,
					expectedRemoveResizeTaintCallCount: 1,
				},
				{
					desc:                               "node with resize taint",
					bPods:                              []*v1.Pod{bPod1},
					taintsOnNode:                       []*v1.Taint{ekvmtypes.BPResizeTaint},
					expectedRemoveResizeTaintCallCount: 1,
				},
				{
					desc:                               "node with resize taint - remove taint error occurs -> Returns error",
					bPods:                              []*v1.Pod{bPod1},
					removeTaintErr:                     removeTaintErr,
					taintsOnNode:                       []*v1.Taint{ekvmtypes.BPResizeTaint},
					expectedRemoveResizeTaintCallCount: 1,
					expectedErrContains:                removeTaintErr,
				},
			}

			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					// Given
					sizeCalc := calculator_test.New() // Allocatable is 3/4 of VMSize

					node := node.DeepCopy()
					for _, taint := range tc.taintsOnNode {
						var err error
						node, _, err = taints.AddOrUpdateTaint(node, taint)
						assert.Nil(t, err)
					}
					if tc.targetSize == nil {
						tc.targetSize = &vmCurrentSize
					}

					fakeClient := &fake.Clientset{}
					cloudProvider := &mockCloudProvider{}
					cloudProvider.On("ResizeVm", mock.Anything, mock.Anything, mock.Anything).Return(nil)
					metrics := &mockMetrics{}
					metrics.On("ObserveVmGceResizeRequestDuration", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
					metrics.On("RegisterResizableVmFixerEvents", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
					testClock := clock.NewFakeClock(testStartTime)
					nsm := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, nil, sizeCalc, testClock)
					nsm.setNode(node.Name, ResizableNode{Node: node, MachineFamily: family})
					op := newOperationTracker(fakeClient, informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cloudProvider, nsm, metrics, sizeCalc, 1, false, fixerInterval, testClock)

					// Manually cache EK size, this would normally be done during
					// operation tracker initialization and after successful resizes.
					err1 := op.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{
						Size:   *tc.targetSize,
						Status: ekvmtypes.ResizeStatusAtIntent,
					})
					assert.NoError(t, err1)

					// Override resizer
					mockBalloonPodResizer := &mockBalloonPodResizer{}
					mockBalloonPodResizer.On("init").Return(nil)
					for _, resizeArg := range tc.expectedBalloonResizes {
						mockBalloonPodResizer.On("resizeBalloonPod", mock.Anything, resizeArg).Return(tc.bpResizeErr)
					}
					mockBalloonPodResizer.On("listAllBalloonPods", mock.Anything).Return(tc.bPods, nil)
					if tc.addTaintErr == nil {
						mockBalloonPodResizer.On("addTaint", mock.Anything, mock.Anything).Return(nodeWithTaint(t, node, ekvmtypes.BPResizeTaint), nil)
					} else {
						mockBalloonPodResizer.On("addTaint", mock.Anything, mock.Anything).Return(nilNode, tc.addTaintErr)
					}
					mockBalloonPodResizer.On("hasTaint", mock.Anything).Return(tc.expectedRemoveResizeTaintCallCount > 0)
					if tc.expectedRemoveResizeTaintCallCount > 0 {
						if tc.removeTaintErr == nil {
							mockBalloonPodResizer.On("removeTaint", mock.Anything).Return(nodeWithoutTaint(t, node, ekvmtypes.BPResizeTaint), nil).Times(tc.expectedRemoveResizeTaintCallCount)
						} else {
							mockBalloonPodResizer.On("removeTaint", mock.Anything).Return(nilNode, tc.removeTaintErr).Times(tc.expectedRemoveResizeTaintCallCount)
						}
					}
					op.balloonPodResizer = mockBalloonPodResizer

					// When fix operation is processed
					err := op.handleFixOperation(fixOperation{NodeName: node.Name, MachineFamily: family})
					if tc.expectedErrContains != nil {
						assert.ErrorContains(t, err, tc.expectedErrContains.Error())
					} else {
						assert.NoError(t, err)
					}

					// Then
					if tc.expectedDownsize != nil {
						// When downsize process is processed.
						_ = op.processNextOperation()
						cachedSize, err := op.vmStateCache.getState(node)
						assert.NoError(t, err)
						assert.Equal(t, *tc.expectedDownsize, cachedSize)
					}

					mockBalloonPodResizer.AssertNumberOfCalls(t, "addTaint", tc.expectedAddResizeTaintCallCount)
					mockBalloonPodResizer.AssertNumberOfCalls(t, "resizeBalloonPod", len(tc.expectedBalloonResizes))
					mockBalloonPodResizer.AssertNumberOfCalls(t, "removeTaint", tc.expectedRemoveResizeTaintCallCount)
				})
			}
		})
	}
}

func TestFix_NonExistingNode(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			sizeCalc := calculator_test.New()
			fakeClient := &fake.Clientset{}
			cloudProvider := &mockCloudProvider{}
			metrics := &mockMetrics{}
			testClock := clock.NewFakeClock(testStartTime)
			nsm := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, nil, sizeCalc, testClock)
			op := newOperationTracker(fakeClient, informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cloudProvider, nsm, metrics, sizeCalc, 1, false, fixerInterval, testClock)

			// The method should not panic.
			err := op.handleFixOperation(fixOperation{NodeName: "node1", MachineFamily: family})
			assert.NoError(t, err)
		})
	}
}

func TestFixerLoop(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			supportedMachineType := fmt.Sprintf("%s-%s-%s", family, "standard", "32")
			nodeMilliCpu := int64(10 * 1000)
			nodeMem := int64(10 * size.GiB)
			node := test.WithAllocatable(test.BuildTestNode("node1", nodeMilliCpu, nodeMem), nodeMilliCpu*3/4, nodeMem*3/4)
			node.Spec.ProviderID = "gce://project1/us-central1-b/node1"
			node.SetLabels(map[string]string{v1.LabelInstanceTypeStable: supportedMachineType})

			vmSize := size.VmSize{MilliCpus: nodeMilliCpu, KBytes: nodeMem / size.KiB}

			testCases := []struct {
				desc                    string
				bPods                   []*v1.Pod
				fixerEnabled            bool
				wantBalloonResizesCalls int
			}{
				{
					desc:                    "with fixerEnabled true",
					bPods:                   []*v1.Pod{},
					fixerEnabled:            true,
					wantBalloonResizesCalls: 1,
				},
				{
					desc:                    "with fixerEnabled false",
					bPods:                   []*v1.Pod{},
					fixerEnabled:            false,
					wantBalloonResizesCalls: 0,
				},
			}

			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					// Given
					sizeCalc := calculator_test.New() // Allocatable is 3/4 of VMSize

					node := node.DeepCopy()
					fakeClient := &fake.Clientset{}
					cloudProvider := &mockCloudProvider{}
					cloudProvider.On("BulkFetchCurrentResizableVmStates").Return(map[gce.GceRef]ekvmtypes.ResizableVmState{{Project: "project1", Zone: "us-central1-b", Name: "node1"}: {
						Size:   vmSize,
						Status: ekvmtypes.ResizeStatusAtIntent,
					}}, nil)
					testClock := clock.NewFakeClock(testStartTime)
					metrics := &mockMetrics{}
					metrics.On("RegisterResizableVmFixerEvents", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
					nodeStateManager := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, nil, sizeCalc, testClock)
					nodeStateManager.nodes = ResizableNodesSnapshot{
						node.Name: ResizableNode{
							Node:          node,
							MachineFamily: family,
						},
					}

					op := newOperationTracker(fakeClient, informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cloudProvider, nodeStateManager, metrics, sizeCalc, 1, tc.fixerEnabled, fixerInterval, testClock)

					err := op.vmStateCache.updateState(node, ekvmtypes.ResizableVmState{
						Size:   vmSize,
						Status: ekvmtypes.ResizeStatusAtIntent,
					})
					assert.NoError(t, err)

					// Override resizer
					resizeBalloonPodCalled := make(chan struct{}, 1)
					mockBalloonPodResizer := &mockBalloonPodResizer{resizeBalloonPodCalled: resizeBalloonPodCalled}
					mockBalloonPodResizer.On("init").Return(nil)
					mockBalloonPodResizer.On("addTaint", mock.Anything, mock.Anything).Return(node, nil)
					mockBalloonPodResizer.On("hasTaint", mock.Anything).Return(false)
					mockBalloonPodResizer.On("removeTaint", mock.Anything).Return(node, nil)
					mockBalloonPodResizer.On("resizeBalloonPod", mock.Anything, mock.Anything).Return(nil)
					mockBalloonPodResizer.On("listAllBalloonPods", mock.Anything).Return(tc.bPods, nil)
					op.balloonPodResizer = mockBalloonPodResizer

					// When the fixer loop is started
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					go op.Run(ctx)

					// Then
					for i := 0; i < tc.wantBalloonResizesCalls; i++ {
						<-resizeBalloonPodCalled
					}
					mockBalloonPodResizer.AssertNumberOfCalls(t, "resizeBalloonPod", tc.wantBalloonResizesCalls)
				})
			}
		})
	}
}

func TestConcurrentWorkers(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			testCases := []struct {
				desc                    string
				numOfWorkers            int
				workerRestartDelay      time.Duration
				blockingOperation       bool
				operationsNum           int
				wantCompletedOperations int
			}{
				{
					desc:                    "Operations use all available workers",
					numOfWorkers:            5,
					workerRestartDelay:      time.Hour, // we will have exactly 5 unique workers before timeout
					blockingOperation:       false,
					operationsNum:           5,
					wantCompletedOperations: 5,
				},
				{
					desc:                    "Workers limit is respected for long operations",
					numOfWorkers:            5,
					workerRestartDelay:      time.Hour, // we will have exactly 5 unique workers before timeout
					blockingOperation:       true,
					operationsNum:           5,
					wantCompletedOperations: 0,
				},
				{
					desc:                    "Workers restart and run multiple short operations",
					numOfWorkers:            5,
					workerRestartDelay:      time.Millisecond, // we will have more than 5 unique workers before timeout
					blockingOperation:       false,
					operationsNum:           10, // wait for at least 10 workers to start
					wantCompletedOperations: 10, // wait for at least 10 workers to finish
				},
			}
			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						workerBlockCh := make(chan struct{})

						defer cancel()

						startedOperations := sync.WaitGroup{}
						completedOperationsCount := atomic.Int32{}
						completedOperations := sync.WaitGroup{}

						opQueue := newOperationQueue("OperationTracker")
						fakeWorker := func() {
							item, quit := opQueue.Get()
							if quit {
								return
							}

							startedOperations.Done()

							if tc.blockingOperation {
								<-workerBlockCh
							}

							opQueue.Done(item)
							completedOperationsCount.Add(1)
							completedOperations.Done()
						}

						mockBPController := &mockBalloonPodController{}
						mockBPController.On("Init").Return(nil).Once()
						cloudProvider := &mockCloudProvider{}
						cloudProvider.On("BulkFetchCurrentResizableVmStates").Return(map[gce.GceRef]ekvmtypes.ResizableVmState{}, nil).Once()
						clientSet := &fake.Clientset{}
						ot := operationTracker{
							provider:        cloudProvider,
							clientSet:       clientSet,
							informerFactory: informers.NewSharedInformerFactory(clientSet, 0),
							balloonPodResizer: &defaultBalloonPodResizer{
								bPController: mockBPController,
								clientSet:    clientSet,
							},
							worker:             fakeWorker,
							numOfWorkers:       tc.numOfWorkers,
							workerRestartDelay: tc.workerRestartDelay,
							opQueue:            opQueue,
							vmStateCache:       newVmStateCache(cloudProvider),
						}

						for i := 0; i < tc.operationsNum; i++ {
							startedOperations.Add(1)
							completedOperations.Add(1)
							opQueue.Enqueue(operation{fix: &fixOperation{NodeName: fmt.Sprint(i), MachineFamily: family}}) //KS: test.BuildTestNode(fmt.Sprint(i), 0, 0)}})
						}
						// Start the operation tracker in the background.
						go ot.Run(ctx)

						// Wait for all operations to be picked up by workers.
						startedOperations.Wait()

						// If operations are non-blocking, wait for them to finish.
						if !tc.blockingOperation {
							completedOperations.Wait()
						}

						assert.Equal(t, int32(tc.wantCompletedOperations), completedOperationsCount.Load())

						// Unblock workers (if they were blocked).
						close(workerBlockCh)

						// Wait for any remaining blocked operations to finish (this unblocks the test).
						if tc.blockingOperation {
							completedOperations.Wait()
						}

						assert.Equal(t, int32(tc.operationsNum), completedOperationsCount.Load())
					})
				})
			}
		})
	}
}

func TestResizeTaintError(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			sizeCalc := calculator_test.New()
			testClock := clock.NewFakeClock(testStartTime)
			nodeStateManager := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, nil, sizeCalc, testClock)
			node := test.BuildTestNode("node1", 2000, 2048*giBToKiB)
			node.Spec.ProviderID = "gce://project1/us-central1-b/node1"
			nodeStateManager.setNode(node.Name, ResizableNode{Node: node, MachineFamily: family})

			cloudProvider := &mockCloudProvider{}
			cloudProvider.On("ResizeVm", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			metrics := &mockMetrics{}
			metrics.On("ObserveVmGceResizeRequestDuration", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
			op := newOperationTracker(&fake.Clientset{}, informers.NewSharedInformerFactory(fake.NewSimpleClientset(), 0), cloudProvider, nodeStateManager, metrics, sizeCalc, 1, false, fixerInterval, testClock)

			testCases := []struct {
				desc                 string
				desiredSize          size.VmSize
				resizeDirection      ResizeDirection
				errDuringAddTaint    error
				errDuringRemoveTaint error
				wantErr              error
			}{
				{
					desc:              "Add resize taint failed during upsize",
					desiredSize:       newSize(4000, 4096*giBToKiB),
					resizeDirection:   Upsize,
					errDuringAddTaint: fmt.Errorf("add taint error"),
					wantErr:           ek_errors.NewBalloonPodResizeTaintError(family, fmt.Errorf("adding taint failed for node \"node1\": add taint error"), ek_errors.StartingState),
				},
				{
					desc:              "Add resize taint failed during downsize",
					desiredSize:       newSize(1000, 1024*giBToKiB),
					resizeDirection:   Downsize,
					errDuringAddTaint: fmt.Errorf("add taint error"),
					wantErr:           ek_errors.NewBalloonPodResizeTaintError(family, fmt.Errorf("adding taint failed for node \"node1\": add taint error"), ek_errors.StartingState),
				},
				{
					desc:                 "Remove resize taint failed during upsize",
					desiredSize:          newSize(4000, 4096*giBToKiB),
					resizeDirection:      Upsize,
					errDuringRemoveTaint: fmt.Errorf("remove taint error"),
					wantErr:              ek_errors.NewBalloonPodResizeTaintError(family, fmt.Errorf("removing taint failed for node \"node1\": remove taint error"), ek_errors.DesiredState),
				},
				{
					desc:                 "Remove resize taint failed during downsize",
					desiredSize:          newSize(1000, 1024*giBToKiB),
					resizeDirection:      Downsize,
					errDuringRemoveTaint: fmt.Errorf("remove taint error"),
					wantErr:              ek_errors.NewBalloonPodResizeTaintError(family, fmt.Errorf("removing taint failed for node \"node1\": remove taint error"), ek_errors.StartingState),
				},
			}

			for _, tc := range testCases {
				t.Run(tc.desc, func(t *testing.T) {
					mockBalloonPodResizer := &mockBalloonPodResizer{}
					mockBalloonPodResizer.On("resizeBalloonPod", mock.Anything, mock.Anything).Return(nil)
					mockBalloonPodResizer.On("addTaint", mock.Anything, mock.Anything).Return(node, tc.errDuringAddTaint)
					mockBalloonPodResizer.On("removeTaint", mock.Anything, mock.Anything).Return(node, tc.errDuringRemoveTaint)
					op.balloonPodResizer = mockBalloonPodResizer
					var err error
					if tc.resizeDirection == Upsize {
						err = op.upsize(ResizeOperation{
							NodeName:     node.Name,
							StartingSize: newSize(2000, 2048*giBToKiB),
							DesiredSize:  newSize(4000, 4096*giBToKiB),
						})
					} else {
						err = op.downsize(ResizeOperation{
							NodeName:     node.Name,
							StartingSize: newSize(2000, 2048*giBToKiB),
							DesiredSize:  newSize(1000, 1024*giBToKiB),
						})
					}
					wantErrResizeError, isWantErrResizeError := tc.wantErr.(*ek_errors.ResizeError)
					errResizeError, isErrResizeError := err.(*ek_errors.ResizeError)
					assert.Equal(t, isWantErrResizeError, isErrResizeError)
					if isWantErrResizeError && isErrResizeError {
						assert.True(t, ek_errors.AreTwoResizeErrorsEqual(*wantErrResizeError, *errResizeError))
					}
				})
			}
		})
	}
}

func TestOnUpdateNodeDoesNotPanic(t *testing.T) {
	ot := operationTracker{}
	ot.onUpdateNode(struct{}{}, struct{}{})
}

func TestOnAddNodeDoesNotPanic(t *testing.T) {
	ot := operationTracker{}
	ot.onAddNode(struct{}{})
}

func TestOnDeleteNodeDoesNotPanic(t *testing.T) {
	ot := operationTracker{}
	ot.onDeleteNode(struct{}{})
}

func TestRegisterResizeSuccess(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			mockMetrics := &mockMetrics{}
			mockMetrics.On("RegisterVmResizeOperation", family, string(Downsize), "", metrics.OperationSucceeded).Once().Return()

			sizeCalc := calculator_test.New()
			testClock := clock.NewFakeClock(testStartTime)
			nsm := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, nil, sizeCalc, testClock)
			node := test.BuildTestNode("node1", 32000, 128*size.GiB)
			nsm.setNode("node1", ResizableNode{Node: node, MachineFamily: family})

			ot := operationTracker{
				metrics:          mockMetrics,
				nodeStateManager: nsm,
			}
			downsizeOp := ResizeOperation{
				NodeName: "node1", //KS: test.BuildTestNode("node1", 32, 128),
				StartingSize: size.VmSize{
					MilliCpus: 32000,
					KBytes:    128 * size.GiB,
				},
				DesiredSize: size.VmSize{
					MilliCpus: 2000,
					KBytes:    4 * size.GiB,
				},
			}

			ot.registerResizeSuccess(downsizeOp)
			mockMetrics.AssertExpectations(t)
		})
	}
}

func TestHandleResizeError(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			ek32FullAllocatable := size.Allocatable{
				MilliCpus: 32000,
				KBytes:    128 * size.GiB,
			}
			ek32HalfAllocatable := size.Allocatable{
				MilliCpus: 16000,
				KBytes:    64 * size.GiB,
			}
			providerID := "gce://project1/us-central1-b/node1"
			supportedMachineType := fmt.Sprintf("%s-%s-%s", family, "standard", "32")

			tests := []struct {
				name         string
				err          error
				node         *v1.Node
				operation    ResizeOperation
				fallbackSize size.Allocatable
			}{
				{
					name: "upsize: untyped error",
					err:  fmt.Errorf("not a resize error"),
					node: ekvms_test.NewResizableNodeBuilder("node1", 32, 128).WithSupportedMachineType(supportedMachineType).WithProvider(providerID).Build(),
					operation: ResizeOperation{
						NodeName:     "node1",
						StartingSize: size.VmSize(ek32HalfAllocatable),
						DesiredSize:  size.VmSize(ek32FullAllocatable),
					},
					fallbackSize: ek32HalfAllocatable,
				},
				{
					name: "upsize: resize error with unknown VM state",
					err:  ek_errors.NewHttp5xxError(family, fmt.Errorf("internal error")),
					node: ekvms_test.NewResizableNodeBuilder("node1", 32, 128).WithSupportedMachineType(supportedMachineType).WithProvider(providerID).Build(),
					operation: ResizeOperation{
						NodeName:     "node1",
						StartingSize: size.VmSize(ek32HalfAllocatable),
						DesiredSize:  size.VmSize(ek32FullAllocatable),
					},
					fallbackSize: ek32HalfAllocatable,
				},
				{
					name: "upsize: resize error with known VM state",
					err:  ek_errors.NewBalloonPodResizeError(family, fmt.Errorf("balloon pod resize error"), ek_errors.DesiredState),
					node: ekvms_test.NewResizableNodeBuilder("node1", 32, 128).WithSupportedMachineType(supportedMachineType).WithProvider(providerID).Build(),
					operation: ResizeOperation{
						NodeName:     "node1",
						StartingSize: size.VmSize(ek32HalfAllocatable),
						DesiredSize:  size.VmSize(ek32FullAllocatable),
					},
					fallbackSize: ek32FullAllocatable,
				},
				{
					name: "downsize: resize error with unknown VM state",
					err:  ek_errors.NewHttp5xxError(family, fmt.Errorf("internal error")),
					node: ekvms_test.NewResizableNodeBuilder("node1", 32, 128).WithSupportedMachineType(supportedMachineType).WithProvider(providerID).Build(),
					operation: ResizeOperation{
						NodeName:     "node1",
						StartingSize: size.VmSize(ek32FullAllocatable),
						DesiredSize:  size.VmSize(ek32HalfAllocatable),
					},
					fallbackSize: ek32HalfAllocatable,
				},
				{
					name: "downsize: resize error with known VM state",
					err:  ek_errors.NewBalloonPodResizeError(family, fmt.Errorf("balloon pod resize error"), ek_errors.StartingState),
					node: ekvms_test.NewResizableNodeBuilder("node1", 32, 128).WithSupportedMachineType(supportedMachineType).WithProvider(providerID).Build(),
					operation: ResizeOperation{
						NodeName:     "node1",
						StartingSize: size.VmSize(ek32FullAllocatable),
						DesiredSize:  size.VmSize(ek32HalfAllocatable),
					},
					fallbackSize: ek32FullAllocatable,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					mockBackoff := &mockBackoff{}
					mockBackoff.On("Backoff", mock.Anything, mock.Anything).Once().Return()
					mockMetrics := &mockMetrics{}
					mockMetrics.On("RegisterVmResizeOperation", family, string(tt.operation.direction()), mock.Anything, metrics.OperationFailed).Once().Return()
					vmSizeCache := newVmStateCache(nil)
					err := vmSizeCache.updateState(tt.node, ekvmtypes.ResizableVmState{
						Size:   tt.operation.StartingSize,
						Status: ekvmtypes.ResizeStatusAtIntent,
					})
					assert.NoError(t, err)
					testClock := clock.NewFakeClock(testStartTime)
					nsm := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, mockBackoff, &identitySizeCalculator{}, testClock)
					ot := &operationTracker{nodeStateManager: nsm, vmStateCache: vmSizeCache, sizeCalculator: &identitySizeCalculator{}, opQueue: newOperationQueue("OperationTracker"), metrics: mockMetrics, clock: testClock}
					node := ResizableNode{
						Node:             tt.node,
						DesiredSize:      tt.fallbackSize,
						UpsizableMaxSize: ek32FullAllocatable,
						PhysicalMaxSize:  ek32FullAllocatable,
						MachineFamily:    family,
					}
					nsm.setNode(tt.operation.NodeName, node)

					ot.handleResizeError(tt.err, tt.operation)

					got, exists := nsm.getNode(tt.operation.NodeName)
					assert.True(t, exists)
					assert.Equal(t, tt.fallbackSize, got.DesiredSize)
					mockBackoff.AssertExpectations(t)
					mockMetrics.AssertExpectations(t)
				})
			}
		})
	}
}

func TestHandleResizeError_NonExistingNode(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		t.Run(family, func(t *testing.T) {
			ek32FullAllocatable := size.Allocatable{
				MilliCpus: 32000,
				KBytes:    128 * size.GiB,
			}

			mockBackoff := &mockBackoff{}
			mockMetrics := &mockMetrics{}
			vmSizeCache := newVmStateCache(nil)
			testClock := clock.NewFakeClock(testStartTime)
			nsm := NewNodeStateManager(newMockResizingProvider(func(string) bool { return true }), nil, mockBackoff, &identitySizeCalculator{}, testClock)
			ot := &operationTracker{nodeStateManager: nsm, vmStateCache: vmSizeCache, sizeCalculator: &identitySizeCalculator{}, metrics: mockMetrics, clock: testClock}

			// The method should not panic.
			ot.handleResizeError(ek_errors.NewBalloonPodResizeError(family, fmt.Errorf("balloon pod resize error"), ek_errors.StartingState),
				ResizeOperation{
					NodeName:     "node1",
					StartingSize: size.VmSize(ek32FullAllocatable),
					DesiredSize:  size.VmSize(ek32FullAllocatable),
				})
		})
	}
}

func newSize(milliCpus, kBytes int64) size.VmSize {
	return size.VmSize{
		MilliCpus: milliCpus,
		KBytes:    kBytes,
	}
}

func nodeWithTaint(t *testing.T, node *v1.Node, taint *v1.Taint) *v1.Node {
	node, _, err := taints.AddOrUpdateTaint(node, taint)
	assert.Nil(t, err)
	return node
}

func nodeWithoutTaint(t *testing.T, node *v1.Node, taint *v1.Taint) *v1.Node {
	node, _, err := taints.RemoveTaint(node, taint)
	assert.Nil(t, err)
	return node
}

func mustGenerateRunningBalloonPod(t *testing.T, node *v1.Node, cpu, mem int64) *v1.Pod {
	bPod, err := GenerateBalloonPod(node, *resource.NewMilliQuantity(cpu, resource.DecimalSI), *resource.NewQuantity(mem, resource.DecimalSI), false)
	assert.NoError(t, err)
	bPod.Status.Phase = apiv1.PodRunning
	return bPod
}

type vmResizerFunc func(context.Context, *v1.Node, size.VmSize) error

func (f vmResizerFunc) GetCurrentResizableVmState(_ *v1.Node) (ekvmtypes.ResizableVmState, error) {
	return ekvmtypes.ResizableVmState{}, nil
}

func (f vmResizerFunc) BulkFetchCurrentResizableVmStates() (map[gce.GceRef]ekvmtypes.ResizableVmState, error) {
	return nil, nil
}

func (f vmResizerFunc) ResizeVm(ctx context.Context, node *v1.Node, size size.VmSize) error {
	return f(ctx, node, size)
}

func (f vmResizerFunc) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return nil
}

type balloonPodResizeArgs struct {
	size        size.Allocatable
	bpResizeErr error
}

type vmResizerArgs struct {
	expectedSize size.VmSize
	actualSize   size.VmSize
}

type mockBalloonPodResizer struct {
	mock.Mock
	resizeBalloonPodCalled chan struct{}
}

func (m *mockBalloonPodResizer) init() error {
	return m.MethodCalled("init").Error(0)
}

func (m *mockBalloonPodResizer) resizeBalloonPod(node *v1.Node, desiredSize size.Allocatable) error {
	err := m.MethodCalled("resizeBalloonPod", node, desiredSize).Error(0)
	if m.resizeBalloonPodCalled != nil {
		m.resizeBalloonPodCalled <- struct{}{}
	}
	return err
}

func (m *mockBalloonPodResizer) addTaint(node *v1.Node, timeAdded time.Time) (*v1.Node, error) {
	args := m.MethodCalled("addTaint", node, timeAdded)
	return args.Get(0).(*v1.Node), args.Error(1)
}

func (m *mockBalloonPodResizer) removeTaint(node *v1.Node) (*v1.Node, error) {
	args := m.MethodCalled("removeTaint", node)
	return args.Get(0).(*v1.Node), args.Error(1)
}

func (m *mockBalloonPodResizer) hasTaint(node *v1.Node) bool {
	args := m.MethodCalled("hasTaint", node)
	return args.Bool(0)
}

func (m *mockBalloonPodResizer) listAllBalloonPods(node *v1.Node) []*v1.Pod {
	return m.MethodCalled("listAllBalloonPods", node).Get(0).([]*v1.Pod)
}

func (m *mockBalloonPodResizer) getPodForNode(node *v1.Node) *v1.Pod {
	return m.MethodCalled("getPodForNode", node).Get(0).(*v1.Pod)
}

type mockBalloonPodController struct {
	mock.Mock
	balloonPodController
}

func (m *mockBalloonPodController) Init() error {
	return m.MethodCalled("Init").Error(0)
}

func (m *mockBalloonPodController) CreateBalloonPod(node *v1.Node, cpu, mem resource.Quantity) error {
	return m.MethodCalled("CreateBalloonPod", node, cpu, mem).Error(0)
}

func (m *mockBalloonPodController) DeleteAllBalloonPods(node *v1.Node) error {
	return m.MethodCalled("DeleteAllBalloonPods", node).Error(0)
}

func (m *mockBalloonPodController) List() []*v1.Pod {
	var pods []*v1.Pod
	pods, _ = (m.MethodCalled("List").Get(0)).([]*v1.Pod)
	return pods
}

type mockCloudProvider struct {
	mock.Mock
}

func (m *mockCloudProvider) ResizeVm(ctx context.Context, node *v1.Node, size size.VmSize) error {
	return m.MethodCalled("ResizeVm", ctx, node, size).Error(0)
}

func (m *mockCloudProvider) GetCurrentResizableVmState(node *v1.Node) (ekvmtypes.ResizableVmState, error) {
	args := m.MethodCalled("GetCurrentResizableVmState", node)
	return args.Get(0).(ekvmtypes.ResizableVmState), args.Error(1)
}

func (m *mockCloudProvider) BulkFetchCurrentResizableVmStates() (map[gce.GceRef]ekvmtypes.ResizableVmState, error) {
	args := m.MethodCalled("BulkFetchCurrentResizableVmStates")
	return args.Get(0).(map[gce.GceRef]ekvmtypes.ResizableVmState), args.Error(1)
}

func (m *mockCloudProvider) NodeGroupForNode(_ *v1.Node) (cloudprovider.NodeGroup, error) {
	args := m.MethodCalled("NodeGroupForNode")
	return args.Get(0).(cloudprovider.NodeGroup), args.Error(1)
}

func (m *mockCloudProvider) ResizingEnabled(machineFamily string) bool {
	return m.MethodCalled("ResizingEnabled", machineFamily).Bool(0)
}

func (m *mockCloudProvider) GetNodesScaleDownAllowedFromCache([]string) map[string]bool {
	return m.MethodCalled("GetNodesScaleDownAllowedFromCache").Get(0).(map[string]bool)
}

func (m *mockCloudProvider) InvalidateNodesScaleDownAllowedCache() {
	m.MethodCalled("InvalidateNodesScaleDownAllowedCache")
}

func (m *mockCloudProvider) UpdateNodesScaleDownAllowedCache(allowed map[string]bool) {
	m.MethodCalled("UpdateNodesScaleDownAllowedCache", allowed)
}

func (m *mockCloudProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return m.MethodCalled("MachineConfigProvider").Get(0).(*machinetypes.MachineConfigProvider)
}

type mockMetrics struct {
	mock.Mock
}

func (m *mockMetrics) ObserveVmGceResizeRequestDuration(machineFamily, direction, status string, duration time.Duration) {
	m.MethodCalled("ObserveVmGceResizeRequestDuration", machineFamily, direction, status, duration)
}

func (m *mockMetrics) RegisterResizableVmFixerEvents(machineFamily, fixType, status, source string) {
	m.MethodCalled("RegisterResizableVmFixerEvents", machineFamily, fixType, status, source)
}

func (m *mockMetrics) RegisterResizableVmReconcileNodeStateEvents(machineFamily string, attemptsNum int, status string, shouldRetry bool) {
	m.MethodCalled("RegisterResizableVmReconcileNodeStateEvents", machineFamily, attemptsNum, status, shouldRetry)
}

func (m *mockMetrics) RegisterVmResizeOperation(machineFamily, direction, reason string, status metrics.OperationStatus) {
	m.MethodCalled("RegisterVmResizeOperation", machineFamily, direction, reason, status)
}
