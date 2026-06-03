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

package processor

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	processor_proto "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/processor/proto"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	calculator_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator/test"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/apis/nodemanagement.gke.io/v1alpha1"
	update_infos_mock "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/client/listers/nodemanagement.gke.io/v1alpha1/mock"
	clock "k8s.io/utils/clock/testing"

	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

var testStartTime = time.Date(2024, 1, 1, 1, 1, 1, 1, time.UTC)
var resizable32MaxSize = size.Allocatable{MilliCpus: 32000, KBytes: 128 * giBToKiB}
var resizable8MaxSize = size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB}
var nonResizableMaxSize = size.Allocatable{MilliCpus: 0, KBytes: 0}
var nonUpsizableMaxSize = size.Allocatable{MilliCpus: 1000, KBytes: 1 * giBToKiB}

var (
	smallNode = &processor_proto.DownsizeConfig_DownsizeBehavior{
		MaxDesiredSizeFraction: 0.125,
		DownsizeDelay:          durationpb.New(time.Minute * 60),
		AllowedForScaledown:    true,
	}
	opportunisticReshape = &processor_proto.DownsizeConfig_DownsizeBehavior{
		MaxDesiredSizeFraction: 0.6,
		DownsizeDelay:          durationpb.New(time.Minute * 20),
		AllowedForScaledown:    true,
	}
	upsizeBuffer = &processor_proto.DownsizeConfig_DownsizeBehavior{
		MaxDesiredSizeFraction: 1,
		MinDownsizeFraction:    0.5,
		DownsizeDelay:          durationpb.New(time.Minute * 1),
		AllowedForScaledown:    false,
	}
	testDownsizeConfig = &processor_proto.DownsizeConfig{
		Behaviors:             []*processor_proto.DownsizeConfig_DownsizeBehavior{smallNode, opportunisticReshape, upsizeBuffer},
		SmoothingWindowLength: durationpb.New(time.Minute * 5),
	}
	testDownsizeConfigProvider = config.SimpleProvider[map[string]*processor_proto.DownsizeConfig]{
		Value: map[string]*processor_proto.DownsizeConfig{
			machinetypes.EK.Name():  testDownsizeConfig,
			machinetypes.E4A.Name(): testDownsizeConfig,
		},
	}
)

type testNodeWithPodsInfo struct {
	node                    *v1.Node
	pods                    []*v1.Pod
	IsNodeResizingOrPending bool
}

func TestScaleDownProcess(t *testing.T) {
	downsizeError := fmt.Errorf("downsize error")
	testCases := map[string]struct {
		nodes             []*testNodeWithPodsInfo
		updateInfo        []*v1alpha1.UpdateInfo
		ekSnapshot        operationtracker.ResizableNodesSnapshot
		resizableSnapshot operationtracker.ResizableNodesSnapshot
		maxWindowsSamples map[string][]struct {
			addTime     time.Time
			allocatable size.Allocatable
		}
		downsizePossibleSince         map[string]time.Time
		isResizingEnabled             bool
		useRoundingCalculator         bool
		downsizeNonResizable          bool
		nodesScaleDownAllowed         map[string]bool
		expectedNodesScaleDownAllowed map[string]bool
		expectedCandidates            []string
		expectedDownsizes             []string
		expectedTargetSizes           map[string]size.Allocatable
		expectedPods                  map[string][]*v1.Pod
		downsizeErr                   error
	}{
		"no nodes": {
			nodes:                         []*testNodeWithPodsInfo{},
			ekSnapshot:                    map[string]operationtracker.ResizableNode{},
			isResizingEnabled:             true,
			expectedNodesScaleDownAllowed: map[string]bool{},
			expectedCandidates:            []string{},
			expectedDownsizes:             []string{},
		},
		"missing config for family - abort": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-e4a", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-e4a": {
					MachineFamily:     "E4A",
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			isResizingEnabled: true,
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-e4a": true,
			},
			expectedCandidates: []string{"node-e4a"},
			expectedDownsizes:  []string{},
		},
		"non-EK nodes": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: nonEkNode("non-ek", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "non-ek", 2000, 11*size.GiB),
					},
				},
				{
					node: nonEkNode("resizable-non-ek", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "resizable-non-ek", 2000, 11*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("unresizable-ek", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("ek", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "ek", 2000, 11*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"resizable-non-ek": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"ek": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"non-ek":           testStartTime.Add(-2 * time.Hour),
				"resizable-non-ek": testStartTime.Add(-2 * time.Hour),
				"unresizable-ek":   testStartTime.Add(-2 * time.Hour),
				"ek":               testStartTime.Add(-2 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"ek":               false,
				"non-ek":           true,
				"resizable-non-ek": true,
				"unresizable-ek":   true,
			},
			expectedCandidates: []string{"non-ek", "resizable-non-ek", "unresizable-ek"},
			expectedDownsizes:  []string{"ek"},
		},
		"empty EK node with possible downsize - downsize allowed": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						systemPod("pod2", 3000, 13*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "node-32", 1000, 1*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-2 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": false,
			},
			expectedCandidates: []string{},
			expectedDownsizes:  []string{"node-32"},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					systemPod("pod2", 3000, 13*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					balloonPod(t, "node-32", 2000, 11*size.GiB),
				},
			},
		},
		"empty E4A node with possible downsize - downsize allowed": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.E4aNode32("node-e4a-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						systemPod("pod2", 3000, 13*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "node-e4a-32", 1000, 1*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-e4a-32": {
					MachineFamily:     machinetypes.E4A.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-e4a-32": testStartTime.Add(-2 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-e4a-32": false,
			},
			expectedCandidates: []string{},
			expectedDownsizes:  []string{"node-e4a-32"},
			expectedPods: map[string][]*v1.Pod{
				"node-e4a-32": {
					systemPod("pod2", 3000, 13*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					balloonPod(t, "node-e4a-32", 2000, 11*size.GiB),
				},
			},
		},
		"empty EK node downsized to limit - scale down allowed": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						systemPod("pod2", 3000, 13*size.GiB),
						daemonsetPod("pod3", 7000, 9*size.GiB),
						balloonPod(t, "node-32", 1234, 56*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": true,
			},
			expectedCandidates: []string{"node-32"},
			expectedDownsizes:  []string{},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					systemPod("pod2", 3000, 13*size.GiB),
					daemonsetPod("pod3", 7000, 9*size.GiB),
					balloonPod(t, "node-32", 1234, 56*size.GiB),
				},
			},
		},
		"empty EK node in process of resizing - no downsize and no scale down allowed": {
			nodes: []*testNodeWithPodsInfo{
				{
					node:                    ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					IsNodeResizingOrPending: true,
					pods: []*v1.Pod{
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-2 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": false,
			},
			expectedCandidates: []string{},
			expectedDownsizes:  []string{},
		},
		"EK nodes with surge": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("surge-node", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "surge-node", 2000, 11*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("downsizable-node", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "downsizable-node", 2000, 11*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("surge-target-node", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "surge-target-node", 2000, 11*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"surge-node": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"downsizable-node": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"surge-target-node": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"surge-node":        testStartTime.Add(-2 * time.Hour),
				"downsizable-node":  testStartTime.Add(-2 * time.Hour),
				"surge-target-node": testStartTime.Add(-2 * time.Hour),
			},
			updateInfo: []*v1alpha1.UpdateInfo{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ek1ek2",
						Namespace: "kube-system",
					},
					Spec: v1alpha1.UpdateInfoSpec{
						SurgeNode:        "surge-node",
						TargetNode:       "surge-target-node",
						Type:             "Upgrade",
						InstanceGroupUrl: "any",
						ValidUntil:       metav1.Time{Time: testStartTime.Add(1 * time.Hour)},
					},
				},
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"downsizable-node":  false,
				"surge-node":        true,
				"surge-target-node": true,
			},
			expectedCandidates: []string{"surge-node", "surge-target-node"},
			expectedDownsizes:  []string{"downsizable-node"},
		},
		"node bigger than all downsize configs": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node1", 34000, 132*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "node1", 2000, 11*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node1": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 34000, KBytes: 132 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node1": testStartTime.Add(-2 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node1": true,
			},
			expectedCandidates: []string{"node1"},
			expectedDownsizes:  []string{},
		},
		"not a downsize - no change in balloon pod size": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						userPod("pod4", 4000, 1*size.GiB),
						balloonPod(t, "node-32", 1234, 56*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode8("node-8", 2000, 8*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 250, 1.5*size.GiB),
						systemPod("pod2", 500, 1.75*size.GiB),
						daemonsetPod("pod3", 750, 2*size.GiB),
						userPod("pod4", 1000, 0.25*size.GiB),
						balloonPod(t, "node-8", 310, 14*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-8": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 2000, KBytes: 8 * giBToKiB},
					PhysicalMaxSize:   resizable8MaxSize,
					UpsizableMaxSize:  resizable8MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-1 * time.Hour),
				"node-8":  testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": true,
				"node-8":  true,
			},
			expectedCandidates: []string{"node-32", "node-8"},
			expectedDownsizes:  []string{},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					userPod("pod4", 4000, 1*size.GiB),
					balloonPod(t, "node-32", 1234, 56*size.GiB),
				},
				"node-8": {
					userPod("pod1", 250, 1.5*size.GiB),
					systemPod("pod2", 500, 1.75*size.GiB),
					daemonsetPod("pod3", 750, 2*size.GiB),
					userPod("pod4", 1000, 0.25*size.GiB),
					balloonPod(t, "node-8", 310, 14*size.GiB),
				},
			},
		},
		"resizing is disabled - no balloon pod adjustments": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "node1", 1000, 1*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node1": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			isResizingEnabled:     false,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node1": true,
			},
			expectedCandidates: []string{"node1"},
			expectedDownsizes:  []string{},
			expectedPods: map[string][]*v1.Pod{
				"node1": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					balloonPod(t, "node1", 1000, 1*size.GiB),
				},
			},
		},
		"successful downsize - balloon pod adjusted": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "node-32", 1000, 1*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("node-8", 2000, 8*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 250, 1.5*size.GiB),
						systemPod("pod2", 500, 1.75*size.GiB),
						daemonsetPod("pod3", 750, 2*size.GiB),
						balloonPod(t, "node-8", 250, 0.25*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-8": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 2000, KBytes: 8 * giBToKiB},
					PhysicalMaxSize:   resizable8MaxSize,
					UpsizableMaxSize:  resizable8MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-1 * time.Hour),
				"node-8":  testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": false,
				"node-8":  false,
			},
			expectedCandidates: []string{},
			expectedDownsizes:  []string{"node-32", "node-8"},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					balloonPod(t, "node-32", 2000, 11*size.GiB),
				},
				"node-8": {
					userPod("pod1", 250, 1.5*size.GiB),
					systemPod("pod2", 500, 1.75*size.GiB),
					daemonsetPod("pod3", 750, 2*size.GiB),
					balloonPod(t, "node-8", 500, 2.75*size.GiB),
				},
			},
		},
		"successful downsize with lookahead pod - lookahead pod space is downsized": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 750, 5*size.GiB),
						lookaheadPod("lookahead-pod-1", 250, 1*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "node-32", 1000, 1*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode8("node-8", 2000, 8*size.GiB),
					pods: []*v1.Pod{
						lookaheadPod("pod1", 250, 1.5*size.GiB),
						systemPod("pod2", 500, 1.75*size.GiB),
						daemonsetPod("pod3", 750, 2*size.GiB),
						balloonPod(t, "node-8", 250, 0.25*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-8": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 2000, KBytes: 8 * giBToKiB},
					PhysicalMaxSize:   resizable8MaxSize,
					UpsizableMaxSize:  resizable8MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-1 * time.Hour),
				"node-8":  testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": false,
				"node-8":  false,
			},
			expectedCandidates: []string{},
			expectedDownsizes:  []string{"node-32", "node-8"},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					userPod("pod1", 750, 5*size.GiB),
					lookaheadPod("lookahead-pod-1", 250, 1*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					balloonPod(t, "node-32", 2000, 11*size.GiB),
				},
				"node-8": {
					lookaheadPod("pod1", 250, 1.5*size.GiB),
					systemPod("pod2", 500, 1.75*size.GiB),
					daemonsetPod("pod3", 750, 2*size.GiB),
					balloonPod(t, "node-8", 500, 2.75*size.GiB),
				},
			},
		},
		"successful downsize with lookahead pod - headroom is kept": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 32000, 128*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 4*size.GiB),
						lookaheadPod("lookahead-pod-1", 4000, 16*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode8("node-8", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 4*size.GiB),
						lookaheadPod("pod1", 1000, 4*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       resizable32MaxSize,
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-8": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       resizable8MaxSize,
					PhysicalMaxSize:   resizable8MaxSize,
					UpsizableMaxSize:  resizable8MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-1 * time.Hour),
				"node-8":  testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": false,
				"node-8":  false,
			},
			expectedCandidates: []string{},
			expectedDownsizes:  []string{"node-32", "node-8"},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					userPod("pod1", 1000, 4*size.GiB),
					lookaheadPod("lookahead-pod-1", 4000, 16*size.GiB),
					balloonPod(t, "node-32", 12000, 48*size.GiB), // BP Pod = max upsizability (32, 128) - desired size (16, 64) - LA Pod(4, 16)
				},
				"node-8": {
					userPod("pod1", 1000, 4*size.GiB),
					lookaheadPod("lookahead-pod-1", 1000, 4*size.GiB),
					balloonPod(t, "node-8", 3000, 12*size.GiB), // BP Pod = max upsizability (8, 32) - desired size (4, 16) - LA Pod(1, 4)
				},
			},
		},
		"no downsize - desired size equal to current after rounding up": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 995, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "node-32", 2000, 11*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("node-8", 8000, 8*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 245, 1.5*size.GiB),
						systemPod("pod2", 500, 1.75*size.GiB),
						daemonsetPod("pod3", 750, 2*size.GiB),
						balloonPod(t, "node-8", 500, 2.75*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 6000, KBytes: 21 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			isResizingEnabled:     true,
			useRoundingCalculator: true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": true,
				"node-8":  true,
			},
			expectedCandidates: []string{"node-32", "node-8"},
			expectedDownsizes:  []string{},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					userPod("pod1", 995, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					balloonPod(t, "node-32", 2000, 11*size.GiB),
				},
				"node-8": {
					userPod("pod1", 245, 1.5*size.GiB),
					systemPod("pod2", 500, 1.75*size.GiB),
					daemonsetPod("pod3", 750, 2*size.GiB),
					balloonPod(t, "node-8", 500, 2.75*size.GiB),
				},
			},
		},
		"nodes with downsize target limited by MinDownsizeFraction": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32-cpu-downsize-limited", 20000, 100*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 2000, 80*size.GiB),
						balloonPod(t, "node1", 18000, 20*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("node-32-memory-downsize-limited", 20000, 100*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 18000, 50*size.GiB),
						balloonPod(t, "node2", 2000, 20*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("node-32-cpu-memory-downsize-limited", 20000, 100*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 10*size.GiB),
						systemPod("pod2", 1000, 10*size.GiB),
						daemonsetPod("pod3", 1000, 10*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode8("node-8-cpu-memory-downsize-limited", 5000, 25*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 250, 2.5*size.GiB),
						systemPod("pod2", 250, 2.5*size.GiB),
						daemonsetPod("pod3", 250, 2.5*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32-cpu-downsize-limited": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 20000, KBytes: 100 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-32-memory-downsize-limited": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 20000, KBytes: 100 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-32-cpu-memory-downsize-limited": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 20000, KBytes: 100 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-8-cpu-memory-downsize-limited": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 5000, KBytes: 25 * giBToKiB},
					PhysicalMaxSize:   resizable8MaxSize,
					UpsizableMaxSize:  resizable8MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32-cpu-downsize-limited":        testStartTime.Add(-1 * time.Hour),
				"node-32-memory-downsize-limited":     testStartTime.Add(-1 * time.Hour),
				"node-32-cpu-memory-downsize-limited": testStartTime.Add(-1 * time.Hour),
				"node-8-cpu-memory-downsize-limited":  testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32-cpu-downsize-limited":        false,
				"node-32-memory-downsize-limited":     false,
				"node-32-cpu-memory-downsize-limited": false,
				"node-8-cpu-memory-downsize-limited":  false,
			},
			expectedCandidates: []string{},
			expectedDownsizes: []string{
				"node-32-cpu-downsize-limited",
				"node-32-memory-downsize-limited",
				"node-32-cpu-memory-downsize-limited",
				"node-8-cpu-memory-downsize-limited",
			},
			expectedTargetSizes: map[string]size.Allocatable{
				"node-32-cpu-downsize-limited":        {MilliCpus: 16000, KBytes: 80 * giBToKiB},
				"node-32-memory-downsize-limited":     {MilliCpus: 18000, KBytes: 64 * giBToKiB},
				"node-32-cpu-memory-downsize-limited": {MilliCpus: 16000, KBytes: 64 * giBToKiB},
				"node-8-cpu-memory-downsize-limited":  {MilliCpus: 4000, KBytes: 16 * giBToKiB},
			},
		},
		"nodes with downsize target not limited by MinDownsize": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32-downsize-limited-cpu-under-limit", 10000, 100*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 2000, 10*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("node-32-downsize-limited-memory-under-limit", 20000, 50*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 2000, 10*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32-downsize-limited-cpu-under-limit": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 10000, KBytes: 100 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-32-downsize-limited-memory-under-limit": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 20000, KBytes: 50 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32-downsize-limited-cpu-under-limit":    testStartTime.Add(-1 * time.Hour),
				"node-32-downsize-limited-memory-under-limit": testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32-downsize-limited-cpu-under-limit":    false,
				"node-32-downsize-limited-memory-under-limit": false,
			},
			expectedCandidates: []string{},
			expectedDownsizes: []string{
				"node-32-downsize-limited-cpu-under-limit",
				"node-32-downsize-limited-memory-under-limit",
			},
			expectedTargetSizes: map[string]size.Allocatable{
				"node-32-downsize-limited-cpu-under-limit":    {MilliCpus: 2000, KBytes: round(10 * giBToKiB)},
				"node-32-downsize-limited-memory-under-limit": {MilliCpus: 2000, KBytes: round(10 * giBToKiB)},
			},
		},
		"target size affected by SmoothingWindow": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("sample-outside-does-not-block-the-downsize", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("sample-inside-blocks-the-downsize", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("max-over-each-dimension", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"sample-outside-does-not-block-the-downsize": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"sample-inside-blocks-the-downsize": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"max-over-each-dimension": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			maxWindowsSamples: map[string][]struct {
				addTime     time.Time
				allocatable size.Allocatable
			}{
				"sample-outside-does-not-block-the-downsize": {
					{testStartTime.Add(-30 * time.Minute), size.Allocatable{MilliCpus: 9000, KBytes: 20 * giBToKiB}},
				},
				"sample-inside-blocks-the-downsize": {
					{testStartTime.Add(-1 * time.Minute), size.Allocatable{MilliCpus: 9000, KBytes: 20 * giBToKiB}},
				},
				"max-over-each-dimension": {
					{testStartTime.Add(-4 * time.Minute), size.Allocatable{MilliCpus: 5000, KBytes: 20 * giBToKiB}},
					{testStartTime.Add(-1 * time.Minute), size.Allocatable{MilliCpus: 6000, KBytes: 18 * giBToKiB}},
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"sample-outside-does-not-block-the-downsize": testStartTime.Add(-1 * time.Hour),
				"sample-inside-blocks-the-downsize":          testStartTime.Add(-1 * time.Hour),
				"max-over-each-dimension":                    testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"sample-inside-blocks-the-downsize":          true,
				"sample-outside-does-not-block-the-downsize": false,
				"max-over-each-dimension":                    false,
			},
			expectedCandidates: []string{"sample-inside-blocks-the-downsize"},
			expectedDownsizes:  []string{"sample-outside-does-not-block-the-downsize", "max-over-each-dimension"},
			expectedTargetSizes: map[string]size.Allocatable{
				"sample-outside-does-not-block-the-downsize": {MilliCpus: 1000, KBytes: 6 * giBToKiB},
				"max-over-each-dimension":                    {MilliCpus: 6000, KBytes: 20 * giBToKiB},
			},
		},
		"error while downsizing - no change in balloon pod size": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						balloonPod(t, "node1", 2000, 11*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node1": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			expectedPods: map[string][]*v1.Pod{
				"node1": {
					userPod("pod1", 1000, 6*size.GiB),
					balloonPod(t, "node1", 2000, 11*size.GiB),
				},
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node1": true,
			},
			expectedCandidates: []string{"node1"},
			expectedDownsizes:  []string{},
			downsizeErr:        downsizeError,
		},
		"not a downsize - ek node is unresizable": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "node1", 1000, 1*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node1": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			isResizingEnabled:     true,
			resizableSnapshot:     map[string]operationtracker.ResizableNode{},
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node1": true,
			},
			expectedCandidates: []string{"node1"},
			expectedDownsizes:  []string{},
		},
		"scale down candidate are cached": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						userPod("pod4", 4000, 1*size.GiB),
						balloonPod(t, "node-32", 1234, 56*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode8("node-8", 2000, 8*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 250, 1.5*size.GiB),
						systemPod("pod2", 500, 1.75*size.GiB),
						daemonsetPod("pod3", 750, 2*size.GiB),
						userPod("pod4", 1000, 0.25*size.GiB),
						balloonPod(t, "node-8", 310, 14*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-8": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 2000, KBytes: 8 * giBToKiB},
					PhysicalMaxSize:   resizable8MaxSize,
					UpsizableMaxSize:  resizable8MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-1 * time.Hour),
				"node-8":  testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled: true,
			nodesScaleDownAllowed: map[string]bool{
				"node-32": true,
				"node-8":  true,
			},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": true,
				"node-8":  true,
			},
			expectedCandidates: []string{"node-32", "node-8"},
			expectedDownsizes:  []string{},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					userPod("pod4", 4000, 1*size.GiB),
					balloonPod(t, "node-32", 1234, 56*size.GiB),
				},
				"node-8": {
					userPod("pod1", 250, 1.5*size.GiB),
					systemPod("pod2", 500, 1.75*size.GiB),
					daemonsetPod("pod3", 750, 2*size.GiB),
					userPod("pod4", 1000, 0.25*size.GiB),
					balloonPod(t, "node-8", 310, 14*size.GiB),
				},
			},
		},
		"scale down candidates are not cached": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						userPod("pod4", 4000, 1*size.GiB),
						balloonPod(t, "node-32", 1234, 56*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode8("node-8", 2000, 8*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 250, 1.5*size.GiB),
						systemPod("pod2", 500, 1.75*size.GiB),
						daemonsetPod("pod3", 750, 2*size.GiB),
						userPod("pod4", 1000, 0.25*size.GiB),
						balloonPod(t, "node-8", 310, 14*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-8": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 2000, KBytes: 8 * giBToKiB},
					PhysicalMaxSize:   resizable8MaxSize,
					UpsizableMaxSize:  resizable8MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-1 * time.Hour),
				"node-8":  testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled:     true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": true,
				"node-8":  true,
			},
			expectedCandidates: []string{"node-32", "node-8"},
			expectedDownsizes:  []string{},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					userPod("pod4", 4000, 1*size.GiB),
					balloonPod(t, "node-32", 1234, 56*size.GiB),
				},
				"node-8": {
					userPod("pod1", 250, 1.5*size.GiB),
					systemPod("pod2", 500, 1.75*size.GiB),
					daemonsetPod("pod3", 750, 2*size.GiB),
					userPod("pod4", 1000, 0.25*size.GiB),
					balloonPod(t, "node-8", 310, 14*size.GiB),
				},
			},
		},
		"scale down candidates are partly cached": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("node-32", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						userPod("pod4", 4000, 1*size.GiB),
						balloonPod(t, "node-32", 1234, 56*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode8("node-8", 2000, 8*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 250, 1.5*size.GiB),
						systemPod("pod2", 500, 1.75*size.GiB),
						daemonsetPod("pod3", 750, 2*size.GiB),
						userPod("pod4", 1000, 0.25*size.GiB),
						balloonPod(t, "node-8", 310, 14*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node-32": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"node-8": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 2000, KBytes: 8 * giBToKiB},
					PhysicalMaxSize:   resizable8MaxSize,
					UpsizableMaxSize:  resizable8MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"node-32": testStartTime.Add(-1 * time.Hour),
				"node-8":  testStartTime.Add(-1 * time.Hour),
			},
			isResizingEnabled: true,
			nodesScaleDownAllowed: map[string]bool{
				"node-32": true,
			},
			expectedNodesScaleDownAllowed: map[string]bool{
				"node-32": true,
				"node-8":  true,
			},
			expectedCandidates: []string{"node-32", "node-8"},
			expectedDownsizes:  []string{},
			expectedPods: map[string][]*v1.Pod{
				"node-32": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					userPod("pod4", 4000, 1*size.GiB),
					balloonPod(t, "node-32", 1234, 56*size.GiB),
				},
				"node-8": {
					userPod("pod1", 250, 1.5*size.GiB),
					systemPod("pod2", 500, 1.75*size.GiB),
					daemonsetPod("pod3", 750, 2*size.GiB),
					userPod("pod4", 1000, 0.25*size.GiB),
					balloonPod(t, "node-8", 310, 14*size.GiB),
				},
			},
		},
		"unresizable-EK node": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("unresizable-ek", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						lookaheadPod("la-pod", 1000, 4*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("ek", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "ek", 2000, 11*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"unresizable-ek": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  nonResizableMaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"ek": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"unresizable-ek": testStartTime.Add(-2 * time.Hour),
				"ek":             testStartTime.Add(-2 * time.Hour),
			},
			isResizingEnabled:     true,
			downsizeNonResizable:  true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"ek":             false,
				"unresizable-ek": false,
			},
			expectedCandidates: []string{},
			expectedDownsizes:  []string{"ek", "unresizable-ek"},
			expectedPods: map[string][]*v1.Pod{
				"ek": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					balloonPod(t, "ek", 2000, 11*size.GiB),
				},
				"unresizable-ek": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					lookaheadPod("la-pod", 1000, 4*size.GiB),
					balloonPod(t, "unresizable-ek", 1000, 7*size.GiB),
				},
			},
		},
		"non-upsizable-EK node": {
			nodes: []*testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("non-upsizable-ek", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						lookaheadPod("la-pod", 1000, 4*size.GiB),
					},
				},
				{
					node: ekvms_test.EkNode32("ek", 8000, 32*size.GiB),
					pods: []*v1.Pod{
						userPod("pod1", 1000, 6*size.GiB),
						systemPod("pod2", 2000, 7*size.GiB),
						daemonsetPod("pod3", 3000, 8*size.GiB),
						balloonPod(t, "ek", 2000, 11*size.GiB),
					},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"non-upsizable-ek": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  nonUpsizableMaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
				"ek": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			},
			downsizePossibleSince: map[string]time.Time{
				"non-upsizable-ek": testStartTime.Add(-2 * time.Hour),
				"ek":               testStartTime.Add(-2 * time.Hour),
			},
			isResizingEnabled:     true,
			downsizeNonResizable:  true,
			nodesScaleDownAllowed: map[string]bool{},
			expectedNodesScaleDownAllowed: map[string]bool{
				"ek":               false,
				"non-upsizable-ek": false,
			},
			expectedCandidates: []string{},
			expectedDownsizes:  []string{"ek", "non-upsizable-ek"},
			expectedPods: map[string][]*v1.Pod{
				"ek": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					balloonPod(t, "ek", 2000, 11*size.GiB),
				},
				"non-upsizable-ek": {
					userPod("pod1", 1000, 6*size.GiB),
					systemPod("pod2", 2000, 7*size.GiB),
					daemonsetPod("pod3", 3000, 8*size.GiB),
					lookaheadPod("la-pod", 1000, 4*size.GiB),
					balloonPod(t, "non-upsizable-ek", 1000, 7*size.GiB),
				},
			},
		},
	}
	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {

			experimentFlags := []string{}
			filterMode := operationtracker.ResizableOnly
			resizableVmManager := newManagerMock()
			resizableVmManager.On("IsResizingEnabled", mock.Anything).Return(
				tc.isResizingEnabled)
			nodeNames := []string{}
			for _, node := range tc.nodes {
				nodeNames = append(nodeNames, node.node.Name)
			}
			resizableVmManager.On("GetNodesScaleDownAllowedFromCache", nodeNames)
			if tc.nodesScaleDownAllowed == nil {
				tc.nodesScaleDownAllowed = map[string]bool{}
			}
			resizableVmManager.nodesScaleDownAllowed = tc.nodesScaleDownAllowed
			resizableVmManager.On("FilteredNodesSnapshot", true, operationtracker.AllNodes).Return(tc.ekSnapshot)
			if tc.resizableSnapshot == nil {
				tc.resizableSnapshot = tc.ekSnapshot
			}

			if tc.downsizeNonResizable {
				experimentFlags = append(experimentFlags, experiments.EkDownsizeNonResizableFlag)
				filterMode = operationtracker.DownsizableOnly
			}
			resizableVmManager.On("FilteredNodesSnapshot", false, filterMode).Return(tc.resizableSnapshot)
			resizableVmManager.On("Downsize", mock.AnythingOfType("*v1.Node"), mock.AnythingOfType("size.Allocatable")).Return(tc.downsizeErr)
			resizableVmManager.On("UpdateNodesScaleDownAllowedCache", tc.expectedNodesScaleDownAllowed)
			mockUpdateInfoLister := update_infos_mock.NewMockUpdateInfoLister(gomock.NewController(t))
			mockUpdateInfoLister.EXPECT().List(labels.Everything()).Return(tc.updateInfo, nil).Times(1)
			metrics := &mockScaleDownMetrics{}
			metrics.On("UpdateNodesWithLookaheadPodsShape", mock.Anything).Return()
			testClock := clock.NewFakeClock(testStartTime)
			fetcher := kubernetes.NewUpdateInfoFetcher(mockUpdateInfoLister, testClock)
			assert.NoError(t, fetcher.Refresh())
			calc := calculator_test.New()
			if tc.useRoundingCalculator {
				calc = calculator_test.NewRoundingCalculator(10)
			}

			gm := experiments.NewMockManager(experimentFlags...)
			scaleDownProcessor := NewScaleDownNodeProcessor(machinetypes.NewMachineConfigProvider(nil), resizableVmManager, gm, fetcher, testDownsizeConfigProvider, calc, metrics, testClock)
			if tc.maxWindowsSamples != nil {
				for name, samples := range tc.maxWindowsSamples {
					scaleDownProcessor.requestedResourcesMaxWindows[name] = utils.NewTtlMaxWindow(testClock, testDownsizeConfig.SmoothingWindowLength.AsDuration())
					for _, sample := range samples {
						testClock.SetTime(sample.addTime)
						scaleDownProcessor.requestedResourcesMaxWindows[name].Add(sample.allocatable)
					}
				}
				testClock.SetTime(testStartTime)
			}
			if tc.downsizePossibleSince != nil {
				scaleDownProcessor.downsizePossibleSince = tc.downsizePossibleSince
			}

			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			candidateNodes := []*v1.Node{}
			for _, testNodeInfo := range tc.nodes {
				resizableVmManager.On("IsNodeResizingOrPending", testNodeInfo.node.Name).Return(testNodeInfo.IsNodeResizingOrPending)
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(testNodeInfo.node, testNodeInfo.pods...))
				assert.NoError(t, err)
				candidateNodes = append(candidateNodes, testNodeInfo.node)
			}
			migSpec := &gkeclient.NodePoolSpec{}
			nodeGroup := gke.NewTestGkeMigBuilder().SetSpec(migSpec).Build()
			cloudProvider := &gke.GkeCloudProviderMock{}
			cloudProvider.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(nodeGroup, nil)
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: snapshot,
				CloudProvider:   cloudProvider,
			}

			sourceCandidates, targetCandidates, newDesiredSizes := scaleDownProcessor.process(ctx, candidateNodes, false)

			sourceCandidateNames := []string{}
			for _, node := range sourceCandidates {
				sourceCandidateNames = append(sourceCandidateNames, node.Name)
			}
			assert.ElementsMatch(t, tc.expectedCandidates, sourceCandidateNames)
			targetCandidateNames := []string{}
			for _, node := range targetCandidates {
				targetCandidateNames = append(targetCandidateNames, node.Name)
			}
			assert.ElementsMatch(t, tc.expectedCandidates, targetCandidateNames)

			// Check the nodes are actually downsized.
			downsizedNodeNames := []string{}
			for name := range resizableVmManager.downsizedNodes {
				downsizedNodeNames = append(downsizedNodeNames, name)
			}
			assert.ElementsMatch(t, tc.expectedDownsizes, downsizedNodeNames)

			// Check that newDesiredSizes only returns the downsized nodes.
			newDesiredSizesNodeNames := []string{}
			for name := range newDesiredSizes {
				newDesiredSizesNodeNames = append(newDesiredSizesNodeNames, name)
			}
			assert.ElementsMatch(t, downsizedNodeNames, newDesiredSizesNodeNames)

			nodeInfos, err := snapshot.ListNodeInfos()
			assert.NoError(t, err)
			assert.Len(t, nodeInfos, len(tc.nodes))

			if tc.expectedPods != nil {
				for _, nodeInfo := range nodeInfos {
					wantPodSpecs := []v1.PodSpec{}
					assert.Len(t, nodeInfo.Pods(), len(tc.expectedPods[nodeInfo.Node().Name]))
					for _, pod := range tc.expectedPods[nodeInfo.Node().Name] {
						wantPodSpecs = append(wantPodSpecs, pod.Spec)
					}
					gotPodSpecs := []v1.PodSpec{}
					for _, podInfo := range nodeInfo.Pods() {
						gotPodSpecs = append(gotPodSpecs, podInfo.Pod.Spec)
					}
					if diff := cmp.Diff(wantPodSpecs, gotPodSpecs); diff != "" {
						t.Errorf("node %q has unexpected pods (-want +got):\n%s", nodeInfo.Node().Name, diff)
					}
				}
			}

			if tc.expectedTargetSizes != nil {
				if diff := cmp.Diff(tc.expectedTargetSizes, newDesiredSizes); diff != "" {
					t.Errorf("unexpected newDesiredSizes (-want +got):\n%s", diff)
				}
			}
		})
	}
}

type DownsizeDelayTestStep struct {
	sleep                             time.Duration
	nodeInfo                          testNodeWithPodsInfo
	updateLastOperation               bool
	nodesScaleDownAllowedCacheExpired bool
	expectDownsize                    bool
	expectedNewDesiredSizes           map[string]size.Allocatable
}

func TestScaleDownProcess_DownsizeDelay(t *testing.T) {
	testCases := []struct {
		desc  string
		steps []DownsizeDelayTestStep
	}{
		{
			desc: "downsize delay is respected",
			steps: []DownsizeDelayTestStep{
				{
					sleep: time.Minute * 0,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					expectDownsize: false,
				},
				{
					sleep: time.Minute * 10,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					nodesScaleDownAllowedCacheExpired: true,
					expectDownsize:                    false,
				},
				{
					sleep: time.Minute * 10,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					expectDownsize:                    true,
					nodesScaleDownAllowedCacheExpired: true,
					expectedNewDesiredSizes:           map[string]size.Allocatable{"node1": {MilliCpus: 6000, KBytes: 21 * giBToKiB}},
				},
			},
		},
		{
			desc: "downsize delay is respected, cache is not expired",
			steps: []DownsizeDelayTestStep{
				{
					sleep: time.Minute * 0,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					expectDownsize: false,
				},
				{
					sleep: time.Minute * 10,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					expectDownsize: false,
				},
				{
					sleep: time.Minute * 10,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					expectDownsize: false,
				},
			},
		},
		{
			desc: "downsize delay is reset on no downsize",
			steps: []DownsizeDelayTestStep{
				{
					sleep: time.Minute * 0,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					expectDownsize: false,
				},
				{
					sleep: time.Minute * 10,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 8000, 32*size.GiB),
						},
					},
					nodesScaleDownAllowedCacheExpired: true,
					expectDownsize:                    false,
				},
				{
					sleep: time.Minute * 10,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					nodesScaleDownAllowedCacheExpired: true,
					expectDownsize:                    false,
				},
			},
		},
		{
			desc: "downsize delay is restarted on new operation",
			steps: []DownsizeDelayTestStep{
				{
					sleep: time.Minute * 0,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					expectDownsize: false,
				},
				{
					sleep: time.Minute * 10,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					updateLastOperation:               true,
					nodesScaleDownAllowedCacheExpired: true,
					expectDownsize:                    false,
				},
				{
					sleep: time.Minute * 10,
					nodeInfo: testNodeWithPodsInfo{
						node: ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
						pods: []*v1.Pod{
							userPod("pod1", 6000, 21*size.GiB),
						},
					},
					nodesScaleDownAllowedCacheExpired: true,
					expectDownsize:                    false,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ekSnapshot := operationtracker.ResizableNodesSnapshot{
				"node1": {
					MachineFamily:     machinetypes.EK.Name(),
					DesiredSize:       size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					PhysicalMaxSize:   resizable32MaxSize,
					UpsizableMaxSize:  resizable32MaxSize,
					LastOperationTime: testStartTime.Add(-2 * time.Hour),
				},
			}

			resizableVmManager := newManagerMock()
			resizableVmManager.On("IsResizingEnabled", mock.Anything).Return(
				true)
			resizableVmManager.On("GetNodesScaleDownAllowedFromCache", mock.AnythingOfType("[]string"))
			resizableVmManager.On("FilteredNodesSnapshot", true, operationtracker.AllNodes).Return(ekSnapshot)
			resizableVmManager.On("FilteredNodesSnapshot", false, operationtracker.ResizableOnly).Return(ekSnapshot)
			resizableVmManager.On("Downsize", mock.AnythingOfType("*v1.Node"), mock.AnythingOfType("size.Allocatable")).Return(nil)
			resizableVmManager.On("UpdateNodesScaleDownAllowedCache", mock.AnythingOfType("map[string]bool"))
			resizableVmManager.On("InvalidateNodesScaleDownAllowedCache")
			resizableVmManager.On("IsNodeResizingOrPending", mock.AnythingOfType("string")).Return(false)
			mockUpdateInfoLister := update_infos_mock.NewMockUpdateInfoLister(gomock.NewController(t))
			mockUpdateInfoLister.EXPECT().List(labels.Everything()).Return(nil, nil).Times(1)
			metrics := &mockScaleDownMetrics{}
			metrics.On("UpdateNodesWithLookaheadPodsShape", mock.Anything).Return()
			testClock := clock.NewFakeClock(testStartTime)
			fetcher := kubernetes.NewUpdateInfoFetcher(mockUpdateInfoLister, testClock)
			assert.NoError(t, fetcher.Refresh())
			calc := calculator_test.New()
			gm := experiments.NewMockManager()
			scaleDownProcessor := NewScaleDownNodeProcessor(machinetypes.NewMachineConfigProvider(nil), resizableVmManager, gm, fetcher, testDownsizeConfigProvider, calc, metrics, testClock)

			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			migSpec := &gkeclient.NodePoolSpec{}
			nodeGroup := gke.NewTestGkeMigBuilder().SetSpec(migSpec).Build()
			cloudProvider := &gke.GkeCloudProviderMock{}
			cloudProvider.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(nodeGroup, nil)
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: snapshot,
				CloudProvider:   cloudProvider,
			}

			for _, step := range tc.steps {
				snapshot.Fork()
				candidateNodes := []*v1.Node{}
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(step.nodeInfo.node, step.nodeInfo.pods...))
				assert.NoError(t, err)
				candidateNodes = append(candidateNodes, step.nodeInfo.node)

				testClock.Sleep(step.sleep)
				if step.updateLastOperation {
					entry := ekSnapshot["node1"]
					entry.LastOperationTime = testClock.Now()
					ekSnapshot["node1"] = entry
				}

				// simulate cache expiration
				if step.nodesScaleDownAllowedCacheExpired {
					scaleDownProcessor.resizableVmManager.InvalidateNodesScaleDownAllowedCache()
				}

				sourceCandidates, targetCandidates, newDesiredSizes := scaleDownProcessor.process(ctx, candidateNodes, false)
				if step.expectDownsize {
					assert.Equal(t, []*v1.Node{}, sourceCandidates)
					assert.Equal(t, []*v1.Node{}, targetCandidates)
					assert.Equal(t, step.expectedNewDesiredSizes, newDesiredSizes)
				} else {
					assert.Equal(t, []*v1.Node{step.nodeInfo.node}, sourceCandidates)
					assert.Equal(t, []*v1.Node{step.nodeInfo.node}, targetCandidates)
					assert.Equal(t, map[string]size.Allocatable{}, newDesiredSizes)
				}
				snapshot.Revert()
			}
		})
	}
}

func TestEmitMetrics(t *testing.T) {
	testCases := []struct {
		name           string
		nodeInfos      []*framework.NodeInfo
		ekSnapshot     operationtracker.ResizableNodesSnapshot
		expectedToCall bool
		expectedShape  []metrics.LAPodNodeShape
	}{
		{
			name:           "no nodes",
			nodeInfos:      []*framework.NodeInfo{},
			ekSnapshot:     operationtracker.ResizableNodesSnapshot{},
			expectedToCall: false,
		},
		{
			name: "no lookahead pods",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
					userPod("pod1", 1000, 6*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"node1": {
					MachineFamily: machinetypes.EK.Name(),
					DesiredSize:   size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
				},
			},
			expectedToCall: false,
		},
		{
			name: "one node with lookahead pod",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
					userPod("pod1", 1000, 6*size.GiB),
					lookaheadPod("la-pod1", 2000, 10*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"node1": {
					MachineFamily: machinetypes.EK.Name(),
					DesiredSize:   size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
				},
			},
			expectedToCall: true,
			expectedShape: []metrics.LAPodNodeShape{
				{
					NodeSizeAllocatable:   size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					UserWorkloadPodsCount: 1,
				},
			},
		},
		{
			name: "one downsized node with lookahead pod",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
					userPod("pod1", 1000, 6*size.GiB),
					lookaheadPod("la-pod1", 2000, 10*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"node1": {
					MachineFamily: machinetypes.EK.Name(),
					DesiredSize:   size.Allocatable{MilliCpus: 4000, KBytes: 16 * giBToKiB},
				},
			},
			expectedToCall: true,
			expectedShape: []metrics.LAPodNodeShape{
				{
					NodeSizeAllocatable:   size.Allocatable{MilliCpus: 4000, KBytes: 16 * giBToKiB},
					UserWorkloadPodsCount: 1,
				},
			},
		},
		{
			name: "multiple nodes with lookahead pods",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
					userPod("pod1", 1000, 6*size.GiB),
					lookaheadPod("la-pod1", 2000, 10*size.GiB)),
				framework.NewTestNodeInfo(ekvms_test.EkNode32("node2", 4000, 16*size.GiB),
					userPod("pod2", 500, 3*size.GiB),
					lookaheadPod("la-pod2", 1000, 5*size.GiB),
					userPod("pod3", 250, 1*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"node1": {
					MachineFamily: machinetypes.EK.Name(),
					DesiredSize:   size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
				},
				"node2": {
					MachineFamily: machinetypes.EK.Name(),
					DesiredSize:   size.Allocatable{MilliCpus: 4000, KBytes: 16 * giBToKiB},
				},
			},
			expectedToCall: true,
			expectedShape: []metrics.LAPodNodeShape{
				{
					NodeSizeAllocatable:   size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					UserWorkloadPodsCount: 1,
				},
				{
					NodeSizeAllocatable:   size.Allocatable{MilliCpus: 4000, KBytes: 16 * giBToKiB},
					UserWorkloadPodsCount: 2,
				},
			},
		},
		{
			name: "node without ek info",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
					userPod("pod1", 1000, 6*size.GiB),
					lookaheadPod("la-pod1", 2000, 10*size.GiB)),
				framework.NewTestNodeInfo(ekvms_test.EkNode32("node2", 4000, 16*size.GiB),
					userPod("pod2", 500, 3*size.GiB),
					lookaheadPod("la-pod2", 1000, 5*size.GiB),
					userPod("pod3", 250, 1*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"node1": {
					MachineFamily: machinetypes.EK.Name(),
					DesiredSize:   size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
				},
			},
			expectedToCall: true,
			expectedShape: []metrics.LAPodNodeShape{
				{
					NodeSizeAllocatable:   size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					UserWorkloadPodsCount: 1,
				},
			},
		},
		{
			name: "upcoming node",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(ekvms_test.EkNode32("node1", 8000, 32*size.GiB),
					userPod("pod1", 1000, 6*size.GiB),
					lookaheadPod("la-pod1", 2000, 10*size.GiB)),
				framework.NewTestNodeInfo(upcomingNode("node2", 4000, 16*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"node1": {
					MachineFamily: machinetypes.EK.Name(),
					DesiredSize:   size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
				},
				"node2": { // this idealy should not exists in ekSnpashot, but to make sure that we test the upcoming condition
					MachineFamily: machinetypes.EK.Name(),
					DesiredSize:   size.Allocatable{MilliCpus: 4000, KBytes: 16 * giBToKiB},
				},
			},
			expectedToCall: true,
			expectedShape: []metrics.LAPodNodeShape{
				{
					NodeSizeAllocatable:   size.Allocatable{MilliCpus: 8000, KBytes: 32 * giBToKiB},
					UserWorkloadPodsCount: 1,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			resizableVmManager := newManagerMock()
			resizableVmManager.On("FilteredNodesSnapshot", false, operationtracker.AllNodes).Return(tc.ekSnapshot)

			mockMetrics := &mockScaleDownMetrics{}
			mockMetrics.On("UpdateNodesWithLookaheadPodsShape", mock.Anything).Return()

			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			for _, nodeInfo := range tc.nodeInfos {
				err := snapshot.AddNodeInfo(nodeInfo)
				assert.NoError(t, err)
			}

			ctx := &context.AutoscalingContext{
				ClusterSnapshot: snapshot,
			}

			processor := ScaleDownNodeProcessor{
				resizableVmManager: resizableVmManager,
				metrics:            mockMetrics,
			}
			processor.emitLookaheadMetrics(ctx)

			if tc.expectedToCall {
				calls := mockMetrics.Calls
				assert.Len(t, calls, 1)
				arguments := calls[0].Arguments
				assert.Len(t, arguments, 1)
				assert.ElementsMatch(t, tc.expectedShape, arguments[0])
			} else {
				mockMetrics.AssertNotCalled(t, "UpdateNodesWithLookaheadPodsShape")
			}
		})
	}
}

type resourcePair struct {
	cpu int64
	mem int64
}

func TestUpdateRequestedResources(t *testing.T) {
	testCases := []struct {
		name                         string
		nodeNames                    []string
		nodeInfos                    []*framework.NodeInfo
		ekSnapshot                   operationtracker.ResizableNodesSnapshot
		initialMaxResources          map[string]resourcePair
		wantMaxResources             map[string]resourcePair
		initialDownsizePossibleSince map[string]time.Time
		wantDownsizePossibleSince    map[string]time.Time
	}{
		{
			name:                "empty",
			nodeNames:           []string{},
			nodeInfos:           []*framework.NodeInfo{},
			ekSnapshot:          operationtracker.ResizableNodesSnapshot{},
			initialMaxResources: map[string]resourcePair{},
			wantMaxResources:    map[string]resourcePair{},
		},
		{
			name: "empty snapshot - no changes",
			nodeNames: []string{
				"node-8",
				"ek-8",
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(nonEkNode("node-8", 8000, 32*size.GiB), userPod("pod1", 1000, 6*size.GiB)),
				framework.NewTestNodeInfo(ekvms_test.EkNode8("ek-8", 8000, 32*size.GiB), userPod("pod2", 1000, 6*size.GiB)),
			},
			ekSnapshot:          operationtracker.ResizableNodesSnapshot{},
			initialMaxResources: map[string]resourcePair{},
			wantMaxResources:    map[string]resourcePair{},
		},
		{
			name: "ek in snapshot - updated max windows",
			nodeNames: []string{
				"node-8",
				"ek-8",
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(nonEkNode("node-8", 8000, 32*size.GiB), userPod("pod1", 1000, 6*size.GiB)),
				framework.NewTestNodeInfo(ekvms_test.EkNode8("ek-8", 8000, 32*size.GiB), userPod("pod2", 1000, 6*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"ek-8": {
					MachineFamily: machinetypes.EK.Name(),
				},
			},
			initialMaxResources: map[string]resourcePair{},
			wantMaxResources: map[string]resourcePair{
				"ek-8": {cpu: 1000, mem: 6 * giBToKiB},
			},
		},
		{
			name: "non-ek node has its downsizing state reset",
			nodeNames: []string{
				"node-8",
				"ek-8",
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(nonEkNode("node-8", 8000, 32*size.GiB), userPod("pod1", 1000, 6*size.GiB)),
				framework.NewTestNodeInfo(ekvms_test.EkNode8("ek-8", 8000, 32*size.GiB), userPod("pod2", 1000, 6*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{},
			initialMaxResources: map[string]resourcePair{
				"ek-8": {cpu: 1000, mem: 6 * giBToKiB},
			},
			wantMaxResources: map[string]resourcePair{},
			initialDownsizePossibleSince: map[string]time.Time{
				"ek-8": testStartTime.Add(-1 * time.Hour),
			},
			wantDownsizePossibleSince: map[string]time.Time{},
		},
		{
			name: "unresizabled ek in snapshot - updated max windows",
			nodeNames: []string{
				"unresizable-ek-8",
				"ek-8",
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(ekvms_test.EkNode8("unresizable-ek-8", 8000, 32*size.GiB), userPod("pod2", 1000, 6*size.GiB), lookaheadPod("la-pod2", 2000, 10*size.GiB)),
				framework.NewTestNodeInfo(ekvms_test.EkNode8("ek-8", 8000, 32*size.GiB), userPod("pod2", 1000, 6*size.GiB), lookaheadPod("la-pod2", 2000, 10*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"ek-8": {
					MachineFamily:    machinetypes.EK.Name(),
					UpsizableMaxSize: resizable8MaxSize,
				},
				"unresizable-ek-8": {
					MachineFamily:    machinetypes.EK.Name(),
					UpsizableMaxSize: nonResizableMaxSize,
				},
			},
			initialMaxResources: map[string]resourcePair{},
			wantMaxResources: map[string]resourcePair{
				"ek-8":             {cpu: 1000, mem: 6 * giBToKiB},
				"unresizable-ek-8": {cpu: 3000, mem: 16 * giBToKiB},
			},
		},
		{
			name: "non-upsizable ek in snapshot - updated max windows",
			nodeNames: []string{
				"non-upsizable-ek-8",
				"ek-8",
			},
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(ekvms_test.EkNode8("non-upsizable-ek-8", 8000, 32*size.GiB), userPod("pod2", 1000, 6*size.GiB), lookaheadPod("la-pod2", 2000, 10*size.GiB)),
				framework.NewTestNodeInfo(ekvms_test.EkNode8("ek-8", 8000, 32*size.GiB), userPod("pod2", 1000, 6*size.GiB), lookaheadPod("la-pod2", 2000, 10*size.GiB)),
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"ek-8": {
					MachineFamily:    machinetypes.EK.Name(),
					UpsizableMaxSize: resizable8MaxSize,
				},
				"non-upsizable-ek-8": {
					MachineFamily:    machinetypes.EK.Name(),
					UpsizableMaxSize: nonUpsizableMaxSize,
					DesiredSize:      resizable8MaxSize,
				},
			},
			initialMaxResources: map[string]resourcePair{},
			wantMaxResources: map[string]resourcePair{
				"ek-8":               {cpu: 1000, mem: 6 * giBToKiB},
				"non-upsizable-ek-8": {cpu: 3000, mem: 16 * giBToKiB},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			for _, nodeInfo := range tc.nodeInfos {
				err := snapshot.AddNodeInfo(nodeInfo)
				assert.NoError(t, err)
			}

			// I need the same node object both in the node info and the node slice passed to process.
			nodes := []*v1.Node{}
			for _, name := range tc.nodeNames {
				ni, err := snapshot.GetNodeInfo(name)
				assert.NoError(t, err)
				nodes = append(nodes, ni.Node())
			}

			fakeClock := clock.NewFakeClock(testStartTime)
			maxWindows := map[string]utils.MaxWindow{}
			for nodeName, resources := range tc.initialMaxResources {
				maxWindows[nodeName] = utils.NewTtlMaxWindow(fakeClock, testDownsizeConfig.SmoothingWindowLength.AsDuration())
				maxWindows[nodeName].Add(size.Allocatable{MilliCpus: resources.cpu, KBytes: resources.mem})
			}

			processor := ScaleDownNodeProcessor{
				requestedResourcesMaxWindows: maxWindows,
				downsizePossibleSince:        tc.initialDownsizePossibleSince,
				clock:                        clock.NewFakeClock(testStartTime),
			}

			ctx := &context.AutoscalingContext{
				ClusterSnapshot: snapshot,
			}

			processor.updateRequestedResources(ctx, testDownsizeConfigProvider.Provide(), nodes, tc.ekSnapshot)

			assert.Len(t, processor.requestedResourcesMaxWindows, len(tc.wantMaxResources))
			for nodeName, resources := range tc.wantMaxResources {
				got, ok := processor.requestedResourcesMaxWindows[nodeName]
				assert.True(t, ok)
				mcpu, err := got.MaxMilliCpus()
				assert.NoError(t, err)
				kbytes, err := got.MaxKBytes()
				assert.NoError(t, err)
				assert.Equal(t, resources.cpu, mcpu)
				assert.Equal(t, resources.mem, kbytes)
			}
			assert.Equal(t, tc.wantDownsizePossibleSince, processor.downsizePossibleSince)
		})
	}
}

func userPod(name string, cpu, mem int64) *v1.Pod {
	return test.BuildTestPod(name, cpu, mem)
}

func daemonsetPod(name string, cpu, mem int64) *v1.Pod {
	return test.BuildTestPod(name, cpu, mem, test.WithDSController())
}

func systemPod(name string, cpu, mem int64) *v1.Pod {
	return test.BuildTestPod(name, cpu, mem, func(p *v1.Pod) { p.Namespace = metav1.NamespaceSystem })
}

func lookaheadPod(name string, cpu, mem int64) *v1.Pod {
	return lookaheadbuffer.BuildTestLookaheadPod("", cpu, mem, lookaheadbuffer.WithName(name))
}

func resizingPod(name string, fromCpu, fromMem, toCpu, toMem int64) *v1.Pod {
	return test.BuildTestPod(name, toCpu, toMem, func(p *v1.Pod) {
		p.Status.Conditions = []v1.PodCondition{
			{Type: v1.PodResizePending, Reason: v1.PodReasonDeferred},
		}
	})
}

func nonEkNode(name string, cpu, mem int64) *v1.Node {
	node := ekvms_test.EkNode32(name, cpu, mem)
	node.Labels[v1.LabelInstanceTypeStable] = "e2-standard-32"
	return node
}

func upcomingNode(name string, cpu, mem int64) *v1.Node {
	node := ekvms_test.EkNode32(name, cpu, mem)
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[annotations.NodeUpcomingAnnotation] = "true"
	return node
}

func round(f float64) int64 {
	return int64(math.Round(f))
}

type mockScaleDownMetrics struct {
	ScaleUpNodeProcessor
	mock.Mock
}

func (m *mockScaleDownMetrics) UpdateNodesWithLookaheadPodsShape(laNodesShape []metrics.LAPodNodeShape) {
	m.MethodCalled("UpdateNodesWithLookaheadPodsShape", laNodesShape)
}
