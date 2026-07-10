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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	autoscalingctx "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/fake"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	ca_taints "k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	listersv1 "k8s.io/client-go/listers/apps/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/operationtracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator"
	calculator_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size/calculator/test"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	ekvmtypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/types"
	"k8s.io/kubernetes/pkg/controller/daemon"
	"k8s.io/kubernetes/pkg/util/taints"
)

type ProcessTestCase struct {
	desc                              string
	nodes                             []testNodeWithPodsInfo
	ekSnapshot                        operationtracker.ResizableNodesSnapshot
	unschedulable                     []*v1.Pod
	daemonSets                        []*appsv1.DaemonSet
	useRoundingCalculator             bool
	isResizingEnabled                 bool
	daemonSetListerThrowsError        bool
	expectedUpsizeAllocatable         size.Allocatable
	expectedScheduledPods             map[string][]*v1.Pod
	expectedNewlyScheduledLAPodsCount int
	expectedUnschedulable             []*v1.Pod
	expectedTotalUpsizable            *size.Allocatable
}

func TestProcess(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		testCases := []ProcessTestCase{
			{
				desc: "No schedule - same balloon pod",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{balloonPod(t, "ek-node", 1500, 150*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:     []*v1.Pod{},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1000,
					KBytes:    100 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {balloonPod(t, "ek-node", 1500, 150*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 2000, KBytes: 200 * miBToKiB},
			},
			{
				desc: "no_schedule_ek_backoff",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 750, 75*size.MiB), balloonPod(t, "ek-node", 1500, 150*size.MiB)},
				}},
				ekSnapshot:        map[string]operationtracker.ResizableNode{},
				unschedulable:     []*v1.Pod{userPod("pod2", 750, 75*size.MiB)},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1500,
					KBytes:    150 * miBToKiB,
				},
				expectedScheduledPods: map[string][]*v1.Pod{},
				expectedUnschedulable: []*v1.Pod{userPod("pod2", 750, 75*size.MiB)},
			},
			{
				desc: "Schedule within allocatable - same balloon pod",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{balloonPod(t, "ek-node", 1500, 150*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:     []*v1.Pod{userPod("pod1", 250, 1*size.MiB)},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1000,
					KBytes:    100 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {userPod("pod1", 250, 1*size.MiB), balloonPod(t, "ek-node", 1500, 150*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 1750, KBytes: 199 * miBToKiB},
			},
			{
				desc: "Schedule - daemonset lister throws error",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{balloonPod(t, "ek-node", 1500, 150*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:              []*v1.Pod{userPod("pod1", 250, 1*size.MiB)},
				isResizingEnabled:          true,
				daemonSetListerThrowsError: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1000,
					KBytes:    100 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {userPod("pod1", 250, 1*size.MiB), balloonPod(t, "ek-node", 1500, 150*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 1750, KBytes: 199 * miBToKiB},
			},
			{
				desc: "Schedule above allocatable and resizing is disabled - requested pod not scheduled",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 750, 75*size.MiB), balloonPod(t, "ek-node", 1500, 150*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:     []*v1.Pod{userPod("pod2", 750, 75*size.MiB)},
				isResizingEnabled: false,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1500,
					KBytes:    150 * miBToKiB,
				},
				expectedScheduledPods: map[string][]*v1.Pod{"ek-node": {userPod("pod1", 750, 75*size.MiB), balloonPod(t, "ek-node", 1500, 150*size.MiB)}},
				expectedUnschedulable: []*v1.Pod{userPod("pod2", 750, 75*size.MiB)},
			},
			{
				desc: "Schedule above allocatable and resizing enabled - shrunk balloon pod",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 750, 75*size.MiB), balloonPod(t, "ek-node", 1500, 150*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:     []*v1.Pod{userPod("pod2", 750, 75*size.MiB)},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1500,
					KBytes:    150 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {userPod("pod2", 750, 75*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 500, KBytes: 50 * miBToKiB},
			},
			{
				desc: "No schedule above capacity - same allocatable",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 2500, 200*size.MiB), balloonPod(t, "ek-node", 0, 50*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:     []*v1.Pod{userPod("pod2", 250, 50*size.MiB)},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 2500,
					KBytes:    200 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {balloonPod(t, "ek-node", 0, 50*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{userPod("pod2", 250, 50*size.MiB)},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 0, KBytes: 0 * miBToKiB},
			},
			{
				desc: "Mixed - upsized allocatable",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1500, 150*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    150 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:     []*v1.Pod{userPod("pod2", 250, 50*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1750,
					KBytes:    200 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {userPod("pod2", 250, 50*size.MiB), balloonPod(t, "ek-node", 750, 50*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{userPod("pod3", 500, 100*size.MiB)},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 750, KBytes: 0 * miBToKiB},
			},
			{
				desc: "Mixed - upsized allocatable with initially scheduled lookahead pod",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1250, 100*size.MiB), lookaheadPodForFamily(family, "lookahead-pod-1", 250, 50*size.MiB), balloonPod(t, "ek-node", 750, 50*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    150 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:             []*v1.Pod{userPod("pod2", 250, 50*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				isResizingEnabled:         true,
				expectedUpsizeAllocatable: size.Allocatable{},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {userPod("pod2", 250, 50*size.MiB), balloonPod(t, "ek-node", 750, 50*size.MiB)},
				},
				expectedUnschedulable:  []*v1.Pod{userPod("pod3", 500, 100*size.MiB)},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 1000, KBytes: 50 * miBToKiB},
			},
			{
				desc: "Mixed - upsized allocatable with initially unscheduled lookahead pod - upsize",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1250, 100*size.MiB), balloonPod(t, "ek-node", 750, 50*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1250,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:     []*v1.Pod{lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB), userPod("pod2", 250, 25*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1500,
					KBytes:    125 * miBToKiB,
				},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB), userPod("pod2", 250, 25*size.MiB), balloonPod(t, "ek-node", 750, 100*size.MiB)},
				},
				expectedNewlyScheduledLAPodsCount: 1,
				expectedUnschedulable:             []*v1.Pod{userPod("pod3", 500, 100*size.MiB)},
				expectedTotalUpsizable:            &size.Allocatable{MilliCpus: 1000, KBytes: 75 * miBToKiB},
			},
			{
				desc: "Mixed - upsized allocatable with initially unscheduled lookahead pod - no upsize",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1250, 100*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    150 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:             []*v1.Pod{lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB), userPod("pod2", 250, 25*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				isResizingEnabled:         true,
				expectedUpsizeAllocatable: size.Allocatable{},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB), userPod("pod2", 250, 25*size.MiB), balloonPod(t, "ek-node", 750, 75*size.MiB)},
				},
				expectedNewlyScheduledLAPodsCount: 1,
				expectedUnschedulable:             []*v1.Pod{userPod("pod3", 500, 100*size.MiB)},
				expectedTotalUpsizable:            &size.Allocatable{MilliCpus: 1000, KBytes: 75 * miBToKiB},
			},
			{
				desc: "Mixed - upsized allocatable with initially scheduled lookahead pod - deprioritize lookahead pod",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1250, 100*size.MiB), lookaheadPodForFamily(family, "lookahead-pod-1", 250, 50*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    150 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    150 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:             []*v1.Pod{userPod("pod2", 250, 50*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				isResizingEnabled:         true,
				expectedUpsizeAllocatable: size.Allocatable{},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {userPod("pod2", 250, 50*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)},
				},
				expectedUnschedulable:  []*v1.Pod{userPod("pod3", 500, 100*size.MiB), lookaheadPodForFamily(family, "lookahead-pod-1", 250, 50*size.MiB)},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 0, KBytes: 0 * miBToKiB},
			},
			{
				desc: "Mixed - upsized allocatable with initially scheduled lookahead pod on node not in ek snapshot - do not deprioritize lookahead pod",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1250, 100*size.MiB), lookaheadPodForFamily(family, "lookahead-pod-1", 250, 50*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)},
				}},
				ekSnapshot:                map[string]operationtracker.ResizableNode{},
				unschedulable:             []*v1.Pod{userPod("pod2", 250, 50*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				isResizingEnabled:         true,
				expectedUpsizeAllocatable: size.Allocatable{},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {lookaheadPodForFamily(family, "lookahead-pod-1", 250, 50*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)},
				},
				expectedUnschedulable: []*v1.Pod{userPod("pod2", 250, 50*size.MiB), userPod("pod3", 500, 100*size.MiB)},
			},
			{
				desc: "new desired size rounded up by calculator's RoundUp",
				nodes: []testNodeWithPodsInfo{
					{
						node: createNodeForFamily(family, "ek-node", 1200, 1200*size.MiB),
						pods: []*v1.Pod{
							balloonPod(t, "ek-node", 500, 500*miBToKiB),
							userPod("pod1", 700, 700*size.MiB),
						},
					},
				},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily:    family,
						DesiredSize:      size.Allocatable{MilliCpus: 700, KBytes: 700 * miBToKiB},
						UpsizableMaxSize: size.Allocatable{MilliCpus: 1000, KBytes: 1000 * miBToKiB},
						Node:             createNodeForFamily(family, "ek-node", 1200, 1200*size.MiB),
					},
				},
				unschedulable:             []*v1.Pod{userPod("pod2", 195, 200*size.MiB)},
				isResizingEnabled:         true,
				useRoundingCalculator:     true,
				expectedUpsizeAllocatable: size.Allocatable{MilliCpus: 900, KBytes: 900 * miBToKiB},
				expectedScheduledPods: map[string][]*v1.Pod{"ek-node": {
					userPod("pod1", 700, 700*size.MiB),
					userPod("pod2", 195, 200*size.MiB),
					balloonPod(t, "ek-node", 300, 300*size.MiB),
				}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 105, KBytes: 100 * miBToKiB},
			},
			{
				desc: "Schedule pod with prioritizing ready node over processing node - same balloon pod",
				nodes: []testNodeWithPodsInfo{
					{
						node: createNodeForFamily(family, "ek-node-in-process", 2500, 250*size.MiB),
						pods: []*v1.Pod{
							balloonPod(t, "ek-node-in-process", 1500, 150*size.MiB),
						},
					},
					{
						node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
						pods: []*v1.Pod{
							balloonPod(t, "ek-node", 1500, 150*size.MiB),
						},
					},
					{
						node: createNodeForFamily(family, "ek-node-in-process-2", 2500, 250*size.MiB),
						pods: []*v1.Pod{
							balloonPod(t, "ek-node-in-process", 1500, 150*size.MiB),
						},
					},
					{
						node: createNodeForFamily(family, "ek-node-2", 2500, 250*size.MiB),
						pods: []*v1.Pod{
							balloonPod(t, "ek-node", 1500, 150*size.MiB),
						},
					},
				},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node-in-process": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node-in-process", 2500, 250*size.MiB),
					},
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
					"ek-node-in-process-2": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node-in-process-2", 2500, 250*size.MiB),
					},
					"ek-node-2": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node-2", 2500, 250*size.MiB),
					},
				},
				unschedulable: []*v1.Pod{
					userPod("pod1", 250, 1*size.MiB),
					userPod("pod2", 250, 1*size.MiB),
				},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1000,
					KBytes:    100 * miBToKiB,
				},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {
						userPod("pod1", 250, 1*size.MiB),
						balloonPod(t, "ek-node", 1500, 150*size.MiB),
					},
					"ek-node-2": {
						userPod("pod2", 250, 1*size.MiB),
						balloonPod(t, "ek-node-2", 1500, 150*size.MiB),
					},
				},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 7500, KBytes: 798 * miBToKiB},
			},
			{
				desc: "Schedule on in-process node as idle node has no space",
				nodes: []testNodeWithPodsInfo{
					{
						node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
						pods: []*v1.Pod{
							userPod("pod1", 2500, 200*size.MiB),
							balloonPod(t, "ek-node", 0, 50*size.MiB),
						},
					},
					{
						node: createNodeForFamily(family, "ek-node-in-process", 2500, 250*size.MiB),
						pods: []*v1.Pod{
							balloonPod(t, "ek-node-in-process", 1500, 150*size.MiB),
						},
					},
				},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
					"ek-node-in-process": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node-in-process", 2500, 250*size.MiB),
					},
				},
				unschedulable:     []*v1.Pod{userPod("pod2", 250, 50*size.MiB)},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 2500,
					KBytes:    200 * miBToKiB,
				},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {balloonPod(t, "ek-node", 0, 50*size.MiB)},
					"ek-node-in-process": {
						userPod("pod2", 250, 50*size.MiB),
						balloonPod(t, "ek-node-in-process", 1500, 150*size.MiB),
					},
				},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 1750, KBytes: 150 * miBToKiB},
			},
			{
				desc: "missing DaemonSet Pod triggers upsize",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 750, 75*size.MiB), balloonPod(t, "ek-node", 1500, 150*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable: []*v1.Pod{},
				daemonSets: []*appsv1.DaemonSet{
					daemonSet("ds", 750, 75*size.MiB, nil),
				},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1500,
					KBytes:    150 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {balloonPod(t, "ek-node", 1000, 100*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 500, KBytes: 50 * miBToKiB},
			},
			{
				desc: "unschedulable DaemonSet Pod doesn't appear in unschedulable pods",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 750, 75*size.MiB), balloonPod(t, "ek-node", 1500, 150*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable: []*v1.Pod{},
				daemonSets: []*appsv1.DaemonSet{
					daemonSet("ds", 10750, 75*size.MiB, nil),
				},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1000,
					KBytes:    150 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {balloonPod(t, "ek-node", 1500, 150*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 1250, KBytes: 125 * miBToKiB},
			},
			{
				desc: "already running DaemonSet Pod is omitted",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{daemonSetPod("ds", 750, 75*size.MiB, "ek-node"), balloonPod(t, "ek-node", 1500, 150*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable: []*v1.Pod{},
				daemonSets: []*appsv1.DaemonSet{
					daemonSet("ds", 750, 75*size.MiB, nil),
				},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1000,
					KBytes:    150 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {balloonPod(t, "ek-node", 1500, 150*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 1250, KBytes: 125 * miBToKiB},
			},
			{
				desc: "cpu based eviction does not trigger upsizes",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod-1", 16000, 16*size.GiB), userPod("evictor", 16000, 16*size.GiB), balloonPod(t, "ek-node", 15000, 60*size.GiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 17000,
							KBytes:    68 * giBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 32000,
							KBytes:    128 * giBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 32000, 128*size.GiB),
					},
				},
				unschedulable:         []*v1.Pod{},
				isResizingEnabled:     true,
				expectedScheduledPods: map[string][]*v1.Pod{"ek-node": {userPod("pod-1", 16000, 16*size.GiB), userPod("evictor", 16000, 16*size.GiB), balloonPod(t, "ek-node", 15000, 60*size.GiB)}},
				expectedUnschedulable: []*v1.Pod{},
			},
			{
				desc: "memory based eviction does not trigger upsizes",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod-1", 2000, 64*size.GiB), userPod("evictor", 2000, 64*size.GiB), balloonPod(t, "ek-node", 24000, 60*size.GiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 8000,
							KBytes:    68 * giBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 32000,
							KBytes:    128 * giBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 32000, 218*size.GiB),
					},
				},
				unschedulable:         []*v1.Pod{},
				isResizingEnabled:     true,
				expectedScheduledPods: map[string][]*v1.Pod{"ek-node": {userPod("pod-1", 2000, 64*size.GiB), userPod("evictor", 2000, 64*size.GiB), balloonPod(t, "ek-node", 24000, 60*size.GiB)}},
				expectedUnschedulable: []*v1.Pod{},
			},
			{
				desc: "pod resize triggers upsize",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 32000, 128*size.GiB),
					pods: []*v1.Pod{resizingPod("pod-1", 2000, 32*size.GiB, 4000, 64*size.GiB), balloonPod(t, "ek-node", 30000, 96*size.GiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    32 * giBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 32000,
							KBytes:    128 * giBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 32000, 128*size.GiB),
					},
				},
				unschedulable:     []*v1.Pod{},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 4000,
					KBytes:    64 * giBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {resizingPod("pod-1", 2000, 32*size.GiB, 4000, 64*size.GiB), balloonPod(t, "ek-node", 28000, 64*size.GiB)}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 28000, KBytes: 64 * giBToKiB},
			},
			{
				desc: "Schedule above allocatable and resizing enabled - using desired memory over upsizable memory",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 300*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 750, 75*size.MiB), balloonPod(t, "ek-node", 1500, 225*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    200 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2000,
							KBytes:    50 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:     []*v1.Pod{userPod("pod2", 750, 25*size.MiB)},
				isResizingEnabled: true,
				expectedUpsizeAllocatable: size.Allocatable{
					MilliCpus: 1500,
					KBytes:    200 * miBToKiB,
				},
				expectedScheduledPods:  map[string][]*v1.Pod{"ek-node": {userPod("pod2", 750, 25*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)}},
				expectedUnschedulable:  []*v1.Pod{},
				expectedTotalUpsizable: &size.Allocatable{MilliCpus: 500, KBytes: 100 * miBToKiB},
			},
		}
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				snapshot := setupSnapshot(t, tc)
				autoscalingCtx := setupContext(tc, snapshot)
				manager := setupManager(tc)
				calculator := setupCalculator(tc)
				metrics := setupMetrics(tc, family)
				cccLister := lister.NewMockCrdListerWithLabel([]crd.CRD{}, gkelabels.ComputeClassLabel)

				cp := &gke.GkeCloudProviderMock{}
				cp.On("MachineConfigProvider").Return(machinetypes.NewMachineConfigProvider(nil))
				p := NewScaleUpNodeProcessor(cp, manager, calculator, metrics, nil, cccLister, nil)
				unschedulable, err := p.Process(autoscalingCtx, tc.unschedulable)
				assert.NoError(t, err)

				verifyResults(t, tc, snapshot, unschedulable, metrics, family)
			})
		}
	}
}

func setupSnapshot(t *testing.T, tc ProcessTestCase) clustersnapshot.ClusterSnapshot {
	snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
	for _, nodeWithPods := range tc.nodes {
		node := nodeWithPods.node
		pods := nodeWithPods.pods
		err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...))
		assert.Nil(t, err)
	}
	return snapshot
}

func setupContext(tc ProcessTestCase, snapshot clustersnapshot.ClusterSnapshot) *autoscalingctx.AutoscalingContext {
	daemonSetLister := &mockDSLister{}
	if tc.daemonSetListerThrowsError {
		daemonSetLister.On("List", labels.Everything()).Return(tc.daemonSets, errors.New("listing DaemonSets error")).Once()
	} else {
		daemonSetLister.On("List", labels.Everything()).Return(tc.daemonSets, nil).Once()
	}
	return &autoscalingctx.AutoscalingContext{
		ClusterSnapshot: snapshot,
		AutoscalingKubeClients: autoscalingctx.AutoscalingKubeClients{
			ListerRegistry: kubernetes.NewListerRegistry(nil, nil, nil, nil, daemonSetLister, nil, nil, nil, nil),
		},
	}
}

func setupManager(tc ProcessTestCase) *ManagerMock {
	m := newManagerMock()
	m.On("IsResizingEnabled", mock.Anything).Return(
		tc.isResizingEnabled)
	m.On("FilteredNodesSnapshot", true, operationtracker.ResizableOnly).Return(tc.ekSnapshot).Once()
	m.On("Upsize", mock.AnythingOfType("*v1.Node"), tc.expectedUpsizeAllocatable).Return(nil).Maybe()
	m.On("IsNodeInProcess", mock.MatchedBy(func(input string) bool {
		return strings.HasPrefix(input, `ek-node-in-process`)
	})).Return(true)
	m.On("IsNodeInProcess", mock.AnythingOfType("string")).Return(false)
	return m
}

func setupCalculator(tc ProcessTestCase) calculator.Calculator {
	if tc.useRoundingCalculator {
		return calculator_test.NewRoundingCalculator(10)
	}
	return calculator_test.New()
}

func setupMetrics(tc ProcessTestCase, family string) *mockScaleUpMetrics {
	metrics := &mockScaleUpMetrics{}
	metrics.On("RegisterResizableVmPodsSchedulableOnUpsizes", family, mock.AnythingOfType("int")).Return()
	if tc.expectedTotalUpsizable != nil {
		metrics.On("UpdateResizableVmTotalNodesLookaheadSpace", family, *tc.expectedTotalUpsizable).Return()
	}
	metrics.On("UpdateResizableVmTotalNodesLookaheadSpace", mock.AnythingOfType("string"), mock.Anything).Return().Maybe()
	return metrics
}

func verifyResults(t *testing.T, tc ProcessTestCase, snapshot clustersnapshot.ClusterSnapshot, unschedulable []*v1.Pod, metrics *mockScaleUpMetrics, family string) {
	nodeInfos, err := snapshot.ListNodeInfos()
	assert.NoError(t, err)
	assert.Len(t, nodeInfos, len(tc.nodes))
	podSpecs := []v1.PodSpec{}
	for _, nodeInfo := range nodeInfos {
		for _, pod := range toPodList(nodeInfo.Pods()) {
			podSpecs = append(podSpecs, pod.Spec)
		}
		for _, expectedScheduledPod := range tc.expectedScheduledPods[nodeInfo.Node().Name] {
			assert.Contains(t, podSpecs, expectedScheduledPod.Spec, "pod %q is not scheduled on node %q", expectedScheduledPod.Name, nodeInfo.Node().Name)
		}
	}
	assert.Equal(t, tc.expectedUnschedulable, unschedulable)
	if podsScheduledOnEks := len(tc.unschedulable) - len(tc.expectedUnschedulable) - tc.expectedNewlyScheduledLAPodsCount; podsScheduledOnEks > 0 {
		metrics.AssertCalled(t, "RegisterResizableVmPodsSchedulableOnUpsizes", family, podsScheduledOnEks)
	}
	if tc.expectedTotalUpsizable != nil {
		metrics.AssertCalled(t, "UpdateResizableVmTotalNodesLookaheadSpace", family, *tc.expectedTotalUpsizable)
	}
}

func createNodeForFamily(family string, name string, milliCpu, bytes int64) *v1.Node {
	b := ekvms_test.NewResizableNodeBuilderFromNode(test.BuildTestNode(name, milliCpu, bytes))
	return b.WithStandard32Capacity().WithSupportedMachineType(family + "-standard-32").WithMachineFamily(family).Build()
}

func lookaheadPodForFamily(family string, name string, cpu, mem int64) *v1.Pod {
	pod := lookaheadPod(name, cpu, mem)
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector["cloud.google.com/machine-family"] = family
	return pod
}

func TestScheduleLookaheadPods(t *testing.T) {
	for _, family := range []string{"ek", "e4a"} {
		testCases := []struct {
			desc                              string
			nodes                             []testNodeWithPodsInfo
			ekSnapshot                        operationtracker.ResizableNodesSnapshot
			unschedulable                     []*v1.Pod
			expectedScheduledPods             map[string][]*v1.Pod
			expectedUnschedulable             []*v1.Pod
			expectedUnschedulableLAPodsMetric int
		}{
			{
				desc: "Don't schedule anything if no LA pods in unschedulable list",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1250, 100*size.MiB), lookaheadPodForFamily(family, "lookahead-pod-1", 250, 50*size.MiB), balloonPod(t, "ek-node", 750, 50*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    150 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:                     []*v1.Pod{userPod("pod2", 250, 50*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				expectedScheduledPods:             map[string][]*v1.Pod{},
				expectedUnschedulable:             []*v1.Pod{userPod("pod2", 250, 50*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				expectedUnschedulableLAPodsMetric: 0,
			},
			{
				desc: "Schedule LA pod without upsizing - Adjust ballloon pod",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1250, 100*size.MiB), balloonPod(t, "ek-node", 1250, 50*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1250,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable: []*v1.Pod{lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB), userPod("pod2", 250, 25*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB), balloonPod(t, "ek-node", 1000, 125*size.MiB)},
				},
				expectedUnschedulable:             []*v1.Pod{userPod("pod2", 250, 25*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				expectedUnschedulableLAPodsMetric: 0,
			},
			{
				desc: "Schedule LA pod without upsizing - subtract LA Pod from Balloon Pod (only cpu upsizability)",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1250, 100*size.MiB), balloonPod(t, "ek-node", 1000, 50*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    200 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable: []*v1.Pod{lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB), userPod("pod2", 250, 25*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB), balloonPod(t, "ek-node", 750, 50*size.MiB)},
				},
				expectedUnschedulable:             []*v1.Pod{userPod("pod2", 250, 25*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				expectedUnschedulableLAPodsMetric: 0,
			},
			{
				desc: "Don't schedule LA pod when there is no space based on UAS, and move it to the end of the unscheduable list",
				nodes: []testNodeWithPodsInfo{{
					node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					pods: []*v1.Pod{userPod("pod1", 1500, 100*size.MiB), balloonPod(t, "ek-node", 1000, 50*size.MiB)},
				}},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
				},
				unschedulable:                     []*v1.Pod{lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB), userPod("pod2", 250, 25*size.MiB), userPod("pod3", 500, 100*size.MiB)},
				expectedScheduledPods:             map[string][]*v1.Pod{},
				expectedUnschedulable:             []*v1.Pod{userPod("pod2", 250, 25*size.MiB), userPod("pod3", 500, 100*size.MiB), lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB)},
				expectedUnschedulableLAPodsMetric: 1,
			},
			{
				desc: "Schedule LA pod - preferring resizable node over non-resizable",
				nodes: []testNodeWithPodsInfo{
					{
						node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
						pods: []*v1.Pod{userPod("pod1", 1500, 100*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)},
					},
					{
						node: createNodeForFamily(family, "ek-node-non-resizbale", 2500, 250*size.MiB),
						pods: []*v1.Pod{userPod("pod2", 1250, 100*size.MiB)},
					},
				},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
					"ek-node-non-resizbale": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    250 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 0,
							KBytes:    0,
						},
						Node: createNodeForFamily(family, "ek-node-non-resizbale", 2500, 250*size.MiB),
					},
				},
				unschedulable: []*v1.Pod{lookaheadPodForFamily(family, "lookahead-pod-1", 500, 25*size.MiB)},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {lookaheadPodForFamily(family, "lookahead-pod-1", 500, 25*size.MiB), balloonPod(t, "ek-node", 500, 125*size.MiB)},
				},
				expectedUnschedulableLAPodsMetric: 0,
			},
			{
				desc: "Schedule LA pod - preferring resizable node over no-upsizability",
				nodes: []testNodeWithPodsInfo{
					{
						node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
						pods: []*v1.Pod{userPod("pod1", 1500, 100*size.MiB), balloonPod(t, "ek-node", 1000, 100*size.MiB)},
					},
					{
						node: createNodeForFamily(family, "ek-node-no-upsizability", 2500, 250*size.MiB),
						pods: []*v1.Pod{userPod("pod2", 1500, 100*size.MiB), balloonPod(t, "ek-node", 1000, 50*size.MiB)},
					},
				},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    100 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
					"ek-node-no-upsizability": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    200 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 1000,
							KBytes:    100 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node-no-upsizability", 2500, 250*size.MiB),
					},
				},
				unschedulable: []*v1.Pod{lookaheadPodForFamily(family, "lookahead-pod-1", 500, 25*size.MiB)},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node": {lookaheadPodForFamily(family, "lookahead-pod-1", 500, 25*size.MiB), balloonPod(t, "ek-node", 500, 125*size.MiB)},
				},
				expectedUnschedulableLAPodsMetric: 0,
			},
			{
				desc: "Schedule LA pod - schedule on non-resizable if no place on resizable node",
				nodes: []testNodeWithPodsInfo{
					{
						node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
						pods: []*v1.Pod{userPod("pod1", 1500, 200*size.MiB), balloonPod(t, "ek-node", 1000, 50*size.MiB)},
					},
					{
						node: createNodeForFamily(family, "ek-node-non-resizbale", 2500, 250*size.MiB),
						pods: []*v1.Pod{userPod("pod2", 1250, 100*size.MiB)},
					},
				},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    200 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 1500,
							KBytes:    200 * miBToKiB,
						},
						Node: createNodeForFamily(family, "ek-node", 2500, 250*size.MiB),
					},
					"ek-node-non-resizbale": {
						MachineFamily: family,
						DesiredSize: size.Allocatable{
							MilliCpus: 2500,
							KBytes:    250 * miBToKiB,
						},
						UpsizableMaxSize: size.Allocatable{
							MilliCpus: 0,
							KBytes:    0,
						},
						Node: createNodeForFamily(family, "ek-node-non-resizbale", 2500, 250*size.MiB),
					},
				},
				unschedulable: []*v1.Pod{lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB)},
				expectedScheduledPods: map[string][]*v1.Pod{
					"ek-node-non-resizbale": {lookaheadPodForFamily(family, "lookahead-pod-1", 250, 25*size.MiB)},
				},
				expectedUnschedulableLAPodsMetric: 0,
			},
		}
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
				for _, nodeWithPods := range tc.nodes {
					node := nodeWithPods.node
					pods := nodeWithPods.pods
					err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...))
					assert.Nil(t, err)
				}
				daemonSetLister := &mockDSLister{}
				daemonSetLister.On("List", labels.Everything()).Return([]*appsv1.DaemonSet{}, nil).Once()
				autoscalingCtx := &autoscalingctx.AutoscalingContext{
					ClusterSnapshot: snapshot,
					AutoscalingKubeClients: autoscalingctx.AutoscalingKubeClients{
						ListerRegistry: kubernetes.NewListerRegistry(nil, nil, nil, nil, daemonSetLister, nil, nil, nil, nil),
					},
				}
				m := newManagerMock()
				m.On("IsResizingEnabled", mock.Anything).Return(true)
				m.On("FilteredNodesSnapshot", true, operationtracker.ResizableOnly).Return(tc.ekSnapshot).Once()
				m.On("Upsize", mock.AnythingOfType("*v1.Node"), mock.Anything).Return(nil).Once()
				m.On("IsNodeInProcess", mock.AnythingOfType("string")).Return(false)

				calc := calculator_test.New()
				metrics := &mockScaleUpMetrics{}
				metrics.On("RegisterResizableVmPodsSchedulableOnUpsizes", family, mock.AnythingOfType("int")).Return()
				metrics.On("UpdateResizableVmUnschedulableLookaheadPodsCount", mock.Anything, mock.Anything).Return()
				metrics.On("UpdateResizableVmTotalNodesLookaheadSpace", family, mock.AnythingOfType("size.Allocatable")).Return()
				metrics.On("UpdateResizableVmTotalNodesLookaheadSpace", mock.AnythingOfType("string"), mock.Anything).Return().Maybe()
				cccLister := lister.NewMockCrdListerWithLabel([]crd.CRD{}, gkelabels.ComputeClassLabel)
				cp := &gke.GkeCloudProviderMock{}
				cp.On("MachineConfigProvider").Return(machinetypes.NewMachineConfigProvider(nil))
				p := NewScaleUpNodeProcessor(cp, m, calc, metrics, nil, cccLister, nil)
				unschedulable, err := p.ScheduleLookaheadPods(autoscalingCtx, tc.unschedulable)
				assert.NoError(t, err)
				nodeInfos, err := snapshot.ListNodeInfos()
				assert.NoError(t, err)
				assert.Len(t, nodeInfos, len(tc.nodes))
				podSpecs := []v1.PodSpec{}
				for _, nodeInfo := range nodeInfos {
					for _, pod := range toPodList(nodeInfo.Pods()) {
						podSpecs = append(podSpecs, pod.Spec)
					}
					for _, expectedScheduledPod := range tc.expectedScheduledPods[nodeInfo.Node().Name] {
						assert.Contains(t, podSpecs, expectedScheduledPod.Spec, "pod %q is not scheduled on node %q", expectedScheduledPod.Name, nodeInfo.Node().Name)
					}
				}
				assert.Equal(t, tc.expectedUnschedulable, unschedulable)
				metrics.AssertNotCalled(t, "RegisterResizableVmPodsSchedulableOnUpsizes", mock.Anything, mock.Anything)
				m.AssertNotCalled(t, "Upsize", mock.Anything)
				metrics.AssertCalled(t, "UpdateResizableVmUnschedulableLookaheadPodsCount", family, tc.expectedUnschedulableLAPodsMetric)
			})
		}
	}
}

func toPodList(podInfos []*framework.PodInfo) []*v1.Pod {
	pods := make([]*v1.Pod, len(podInfos))
	for i, podInfo := range podInfos {
		pods[i] = podInfo.Pod
	}
	return pods
}

func TestSchedulePods(t *testing.T) {
	createNode := func(family string, name string, milliCpu, bytes int64) *v1.Node {
		b := ekvms_test.NewResizableNodeBuilderFromNode(test.BuildTestNode(name, milliCpu, bytes))
		return b.WithStandard32Capacity().WithSupportedMachineType(family + "-standard-32").WithMachineFamily(family).Build()
	}

	for _, family := range []string{"ek", "e4a"} {
		machineType1 := family + "-standard-8"
		machineType2 := family + "-standard-16"

		testCases := []struct {
			desc                       string
			nodes                      []testNodeWithPodsInfo
			backedOffRules             map[string]map[int]bool
			cccCrds                    []crd.CRD
			ekSnapshot                 operationtracker.ResizableNodesSnapshot
			nodeMigs                   map[string]*gke.GkeMig
			unschedulable              []*v1.Pod
			expectedSchedulablePerNode map[string][]*v1.Pod
			expectedUnschedulable      []*v1.Pod
		}{
			{
				desc: "Schedule on idle nodes",
				nodes: []testNodeWithPodsInfo{
					{
						node: createNode(family, "ek-node", 1000, 1024*size.KiB),
						pods: []*v1.Pod{},
					},
					{
						node: createNode(family, "ek-node-in-process", 1000, 1024*size.KiB),
						pods: []*v1.Pod{},
					},
				},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node":            {Node: createNode(family, "ek-node", 1000, 1024*size.KiB)},
					"ek-node-in-process": {Node: createNode(family, "ek-node-in-process", 1000, 1024*size.KiB)},
				},
				unschedulable:              []*v1.Pod{userPod("pod1", 250, 500)},
				expectedSchedulablePerNode: map[string][]*v1.Pod{"ek-node": {userPod("pod1", 250, 500)}},
				expectedUnschedulable:      []*v1.Pod{},
			},
			{
				desc: "Schedule on processing nodes",
				nodes: []testNodeWithPodsInfo{
					{
						node: createNode(family, "ek-node", 1000, 1024*size.KiB),
						pods: []*v1.Pod{},
					},
					{
						node: createNode(family, "ek-node-in-process", 1500, 1500*size.KiB),
						pods: []*v1.Pod{},
					},
				},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node":            {Node: createNode(family, "ek-node", 1000, 1024*size.KiB)},
					"ek-node-in-process": {Node: createNode(family, "ek-node-in-process", 1500, 1500*size.KiB)},
				},
				unschedulable:              []*v1.Pod{userPod("pod1", 1500, 1500*size.KiB)},
				expectedSchedulablePerNode: map[string][]*v1.Pod{"ek-node-in-process": {userPod("pod1", 1500, 1500*size.KiB)}},
				expectedUnschedulable:      []*v1.Pod{},
			},
			{
				desc: "Available allocatable for only 1 pod",
				nodes: []testNodeWithPodsInfo{
					{
						node: createNode(family, "ek-node", 1500, 1500*size.KiB),
						pods: []*v1.Pod{},
					},
				},
				ekSnapshot: map[string]operationtracker.ResizableNode{
					"ek-node": {Node: createNode(family, "ek-node", 1500, 1500*size.KiB)},
				},
				unschedulable:              []*v1.Pod{userPod("pod2", 1500, 1500*size.KiB), userPod("pod3", 500, 1000*size.KiB)},
				expectedSchedulablePerNode: map[string][]*v1.Pod{"ek-node": {userPod("pod2", 1500, 1500*size.KiB)}},
				expectedUnschedulable:      []*v1.Pod{userPod("pod3", 500, 1000*size.KiB)},
			},
			{
				desc: "Schedule CCC pods",
				nodes: []testNodeWithPodsInfo{
					{
						node: ekvms_test.NewResizableNodeBuilder("ccc1-rule0-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").WithMachineFamily(family).Build(),
						pods: []*v1.Pod{},
					},
					{
						node: ekvms_test.NewResizableNodeBuilder("ccc2-rule0-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc2").WithMachineFamily(family).Build(),
						pods: []*v1.Pod{userPod("pod1", 24, 100), balloonPod(t, "balloon-pod", 8, 28)},
					},
					{
						node: ekvms_test.NewResizableNodeBuilder("ccc2-rule1-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc2").WithMachineFamily(family).Build(),
						pods: []*v1.Pod{},
					},
				},
				ekSnapshot: operationtracker.ResizableNodesSnapshot{
					"ccc1-rule0-node": {Node: ekvms_test.NewResizableNodeBuilder("ccc1-rule0-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").WithMachineFamily(family).Build()},
					"ccc2-rule0-node": {Node: ekvms_test.NewResizableNodeBuilder("ccc2-rule0-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc2").WithMachineFamily(family).Build()},
					"ccc2-rule1-node": {Node: ekvms_test.NewResizableNodeBuilder("ccc2-rule1-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc2").WithMachineFamily(family).Build()},
				},
				backedOffRules: map[string]map[int]bool{
					"ccc2": {0: true},
				},
				cccCrds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("ccc1"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithRules(
							[]rules.Rule{
								rules.NewRule(rules.WithMachineTypeRule(&machineType1)),
							}),
					),
					crd.NewTestCrd(
						crd.WithName("ccc2"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithRules(
							[]rules.Rule{
								rules.NewRule(rules.WithMachineTypeRule(&machineType1)),
								rules.NewRule(rules.WithMachineTypeRule(&machineType2)),
							}),
					),
				},
				nodeMigs: map[string]*gke.GkeMig{
					"ccc1-rule0-node": gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
						MachineType: machineType1,
						Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc1"},
					}).Build(),
					"ccc2-rule0-node": gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
						MachineType: machineType1,
						Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc2"},
					}).Build(),
					"ccc2-rule1-node": gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
						MachineType: machineType2,
						Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc2"},
					}).Build(),
				},
				unschedulable: []*v1.Pod{
					buildPodWithNodeSelector("pod2", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc1"}),
					buildPodWithNodeSelector("pod3", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc2"}),
				},
				expectedSchedulablePerNode: map[string][]*v1.Pod{
					"ccc1-rule0-node": {buildPodWithNodeSelector("pod2", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc1"})},
					"ccc2-rule1-node": {buildPodWithNodeSelector("pod3", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc2"})},
				},
				expectedUnschedulable: []*v1.Pod{},
			},
			{
				desc: "Schedule CCC and non-CCC pods",
				nodes: []testNodeWithPodsInfo{
					{
						node: ekvms_test.NewResizableNodeBuilder("ccc-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc").WithMachineFamily(family).Build(),
						pods: []*v1.Pod{},
					},
					{
						node: createNode(family, "non-ccc-node", 32, 128),
						pods: []*v1.Pod{},
					},
				},
				ekSnapshot: operationtracker.ResizableNodesSnapshot{
					"ccc-node":     {Node: ekvms_test.NewResizableNodeBuilder("ccc-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc").WithMachineFamily(family).Build()},
					"non-ccc-node": {Node: createNode(family, "non-ccc-node", 32, 128)},
				},
				cccCrds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("ccc"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithScaleUpAnyway(),
					),
				},
				unschedulable: []*v1.Pod{
					test.BuildTestPod("non-ccc-pod", 16, 64),
					buildPodWithNodeSelector("ccc-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc"}),
				},
				nodeMigs: map[string]*gke.GkeMig{
					"ccc-node": gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
						MachineType: family + "-standard-32",
						Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc"},
					}).Build(),
				},
				expectedSchedulablePerNode: map[string][]*v1.Pod{
					"non-ccc-node": {test.BuildTestPod("non-ccc-pod", 16, 64)},
					"ccc-node":     {buildPodWithNodeSelector("ccc-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc"})},
				},
				expectedUnschedulable: []*v1.Pod{},
			},
			{
				desc: "Schedule on processing CCC node",
				nodes: []testNodeWithPodsInfo{
					{
						node: ekvms_test.NewResizableNodeBuilder("ccc-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc").WithMachineFamily(family).Build(),
						pods: []*v1.Pod{userPod("pod1", 24, 100), balloonPod(t, "balloon-pod", 8, 28)},
					},
					{
						node: ekvms_test.NewResizableNodeBuilder("ccc-node-in-process", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc").WithMachineFamily(family).Build(),
						pods: []*v1.Pod{},
					},
				},
				ekSnapshot: operationtracker.ResizableNodesSnapshot{
					"ccc-node":            {Node: ekvms_test.NewResizableNodeBuilder("ccc-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc").WithMachineFamily(family).Build()},
					"ccc-node-in-process": {Node: ekvms_test.NewResizableNodeBuilder("ccc-node-in-process", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc").WithMachineFamily(family).Build()},
				},
				cccCrds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("ccc"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithScaleUpAnyway(),
					),
				},
				unschedulable: []*v1.Pod{
					buildPodWithNodeSelector("ccc-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc"}),
				},
				nodeMigs: map[string]*gke.GkeMig{
					"ccc-node": gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
						MachineType: family + "-standard-32",
						Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc"},
					}).Build(),
					"ccc-node-in-process": gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
						MachineType: family + "-standard-32",
						Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc"},
					}).Build(),
				},
				expectedSchedulablePerNode: map[string][]*v1.Pod{
					"ccc-node-in-process": {buildPodWithNodeSelector("ccc-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc"})},
				},
				expectedUnschedulable: []*v1.Pod{},
			},
			{
				desc: "CCC pod is considered for scale up",
				nodes: []testNodeWithPodsInfo{
					{
						node: ekvms_test.NewResizableNodeBuilder("ccc-rule1-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc").WithMachineFamily(family).Build(),
						pods: []*v1.Pod{},
					},
				},
				ekSnapshot: operationtracker.ResizableNodesSnapshot{
					"ccc-rule1-node": {Node: ekvms_test.NewResizableNodeBuilder("ccc-rule1-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc").WithMachineFamily(family).Build()},
				},
				cccCrds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("ccc"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithRules(
							[]rules.Rule{
								rules.NewRule(rules.WithMachineTypeRule(&machineType1)),
								rules.NewRule(rules.WithMachineTypeRule(&machineType2)),
							}),
					),
				},
				nodeMigs: map[string]*gke.GkeMig{
					"ccc-rule1-node": gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
						MachineType: machineType2,
						Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc"},
					}).Build(),
				},
				unschedulable: []*v1.Pod{
					buildPodWithNodeSelector("ccc-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc"}),
				},
				expectedUnschedulable: []*v1.Pod{
					buildPodWithNodeSelector("ccc-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc"}),
				},
			},
			{
				desc: "Pod referencing non-EK CCC is returned as unschedulable",
				nodes: []testNodeWithPodsInfo{
					{
						node: ekvms_test.NewResizableNodeBuilder("ccc1-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").WithMachineFamily(family).Build(),
						pods: []*v1.Pod{},
					},
				},
				ekSnapshot: operationtracker.ResizableNodesSnapshot{
					"ccc1-node": {Node: ekvms_test.NewResizableNodeBuilder("ccc1-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").WithMachineFamily(family).Build()},
				},
				cccCrds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("ccc1"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithScaleUpAnyway(),
					),
					crd.NewTestCrd(
						crd.WithName("ccc-missing"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithScaleUpAnyway(),
					),
				},
				nodeMigs: map[string]*gke.GkeMig{
					"ccc1-node": gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
						MachineType: family + "-standard-32",
						Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc1"},
					}).Build(),
				},
				unschedulable: []*v1.Pod{
					buildPodWithNodeSelector("ccc-missing-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc-missing"}),
				},
				expectedSchedulablePerNode: map[string][]*v1.Pod{},
				expectedUnschedulable: []*v1.Pod{
					buildPodWithNodeSelector("ccc-missing-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "ccc-missing"}),
				},
			},
			{
				desc: "Pod with unknown CCC CRD is returned as unschedulable",
				nodes: []testNodeWithPodsInfo{
					{
						node: ekvms_test.NewResizableNodeBuilder("ccc1-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").WithMachineFamily(family).Build(),
						pods: []*v1.Pod{},
					},
				},
				ekSnapshot: operationtracker.ResizableNodesSnapshot{
					"ccc1-node": {Node: ekvms_test.NewResizableNodeBuilder("ccc1-node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").WithMachineFamily(family).Build()},
				},
				cccCrds: []crd.CRD{
					crd.NewTestCrd(
						crd.WithName("ccc1"),
						crd.WithLabel(gkelabels.ComputeClassLabel),
						crd.WithScaleUpAnyway(),
					),
				},
				nodeMigs: map[string]*gke.GkeMig{
					"ccc1-node": gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
						MachineType: family + "-standard-32",
						Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc1"},
					}).Build(),
				},
				unschedulable: []*v1.Pod{
					buildPodWithNodeSelector("unknown-ccc-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "unknown-ccc"}),
				},
				expectedSchedulablePerNode: map[string][]*v1.Pod{},
				expectedUnschedulable: []*v1.Pod{
					buildPodWithNodeSelector("unknown-ccc-pod", 16, 64, map[string]string{gkelabels.ComputeClassLabel: "unknown-ccc"}),
				},
			},
		}
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				m := newManagerMock()
				m.On("IsNodeInProcess", "ek-node-in-process").Return(true)
				m.On("IsNodeInProcess", "ccc-node-in-process").Return(true)
				m.On("IsNodeInProcess", mock.AnythingOfType("string")).Return(false)
				b := NewFakeCCCRuleBackoff(tc.backedOffRules)
				cccLister := lister.NewMockCrdListerWithLabel(tc.cccCrds, gkelabels.ComputeClassLabel)
				cloudProvider := &gke.GkeCloudProviderMock{}
				cloudProvider.On("IsAutopilotEnabled", mock.Anything).Return(true)
				p := NewScaleUpNodeProcessor(cloudProvider, m, calculator_test.New(), nil, b, cccLister, nil)

				snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
				for _, nodeWithPods := range tc.nodes {
					node := nodeWithPods.node
					pods := nodeWithPods.pods
					err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(node, pods...))
					assert.Nil(t, err)

					if _, found := tc.nodeMigs[node.Name]; found {
						cloudProvider.On("GkeMigForNode", node).Return(tc.nodeMigs[node.Name], nil)
					}
				}

				schedulable, unschedulable, err := p.schedulePods(snapshot, tc.ekSnapshot, tc.unschedulable)
				assert.NoError(t, err)
				nodeInfos, err := snapshot.ListNodeInfos()
				assert.NoError(t, err)
				assert.Len(t, nodeInfos, len(tc.nodes))
				for _, nodeInfo := range nodeInfos {
					podSpecs := []v1.PodSpec{}
					for _, pod := range toPodList(nodeInfo.Pods()) {
						podSpecs = append(podSpecs, pod.Spec)
					}
					for _, expectedScheduledPod := range tc.expectedSchedulablePerNode[nodeInfo.Node().Name] {
						assert.Contains(t, podSpecs, expectedScheduledPod.Spec, "pod %q is not scheduled on node %q", expectedScheduledPod.Name, nodeInfo.Node().Name)
					}
				}
				expectedSchedulable := []*v1.Pod{}
				for _, pods := range tc.expectedSchedulablePerNode {
					expectedSchedulable = append(expectedSchedulable, pods...)
				}

				schedulablePods := []*v1.Pod{}
				for _, status := range schedulable {
					schedulablePods = append(schedulablePods, status.Pod)
				}

				unschedulablePods := []*v1.Pod{}
				for _, status := range unschedulable {
					unschedulablePods = append(unschedulablePods, status.Pod)
				}

				if tc.expectedUnschedulable == nil {
					assert.Empty(t, unschedulablePods)
				} else {
					assert.ElementsMatch(t, tc.expectedUnschedulable, unschedulablePods)
				}
				assert.ElementsMatch(t, expectedSchedulable, schedulablePods)
			})
		}
	}
}

func TestPreprocess(t *testing.T) {
	nodeUnderDeletion := ekvms_test.EkNode32("node-under-deletion", 2000, 200*size.MiB)
	nodeUnderDeletion.Spec.Taints = append(nodeUnderDeletion.Spec.Taints, v1.Taint{
		Key:    ca_taints.ToBeDeletedTaint,
		Effect: v1.TaintEffectNoSchedule,
	})
	testCases := []struct {
		desc               string
		node               *v1.Node
		ekSnapshot         operationtracker.ResizableNodesSnapshot
		isResizingEnabled  bool
		existingBalloonPod *v1.Pod
		expectedBalloonPod *v1.Pod
	}{
		{
			desc: "Resize is not enabled",
			node: ekvms_test.EkNode32("node1", 2000, 200*size.MiB),
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node1": {
					DesiredSize: size.Allocatable{
						MilliCpus: 1000,
						KBytes:    100 * miBToKiB,
					},
				},
			},
			isResizingEnabled:  false,
			existingBalloonPod: balloonPod(t, "node1", 1500, 150*size.MiB),
			expectedBalloonPod: balloonPod(t, "node1", 1500, 150*size.MiB),
		},
		{
			desc:               "Non EK Node",
			node:               test.BuildTestNode("node1", 1000, 100*size.MiB),
			isResizingEnabled:  true,
			expectedBalloonPod: nil,
		},
		{
			desc:               "EK Node without balloon pod and under deletion",
			node:               nodeUnderDeletion,
			isResizingEnabled:  true,
			expectedBalloonPod: nil,
		},
		{
			desc: "EK Node",
			node: ekvms_test.EkNode32("node1", 2000, 200*size.MiB),
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node1": {
					DesiredSize: size.Allocatable{
						MilliCpus: 1000,
						KBytes:    100 * miBToKiB,
					},
				},
			},
			isResizingEnabled:  true,
			existingBalloonPod: balloonPod(t, "node1", 1500, 150*size.MiB),
			expectedBalloonPod: balloonPod(t, "node1", 1000, (200-100)*size.MiB),
		},
		{
			desc: "EK Node without balloon pod",
			node: ekvms_test.EkNode32("node1", 2000, 200*size.MiB),
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node1": {
					DesiredSize: size.Allocatable{
						MilliCpus: 1000,
						KBytes:    100 * miBToKiB,
					},
				},
			},
			isResizingEnabled:  true,
			existingBalloonPod: nil,
			expectedBalloonPod: balloonPod(t, "node1", 1000, (200-100)*size.MiB),
		},
		{
			desc: "EK node desired size leaves no room for balloon pod",
			node: ekvms_test.EkNode32("node1", 1000, 100*size.MiB),
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"node1": {
					DesiredSize: size.Allocatable{
						MilliCpus: 800,
						KBytes:    80 * miBToKiB,
					},
				},
			},
			isResizingEnabled:  true,
			existingBalloonPod: balloonPod(t, "node1", 600, 60*size.MiB),
			expectedBalloonPod: balloonPod(t, "node1", 200, 50*size.MiB),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			pod := userPod("pod1", 500, 50*size.MiB)
			nodePods := []*v1.Pod{pod}
			if tc.existingBalloonPod != nil {
				nodePods = append(nodePods, tc.existingBalloonPod)
			}
			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(tc.node, nodePods...))
			assert.Nil(t, err)
			m := newManagerMock()
			m.On("IsResizingEnabled", mock.Anything).Return(
				tc.isResizingEnabled)
			m.On("FilteredNodesSnapshot", true, operationtracker.AllNodes).Return(tc.ekSnapshot).Once()
			autoscalingCtx := &autoscalingctx.AutoscalingContext{
				ClusterSnapshot: snapshot,
			}
			cp := &gke.GkeCloudProviderMock{}
			cp.On("MachineConfigProvider").Return(machinetypes.NewMachineConfigProvider(nil))
			p := NewScaleUpNodeProcessor(cp, m, calculator_test.New(), nil, nil, nil, nil)
			err = p.Preprocess(autoscalingCtx)
			assert.NoError(t, err)
			nodeInfos, err := snapshot.ListNodeInfos()
			assert.NoError(t, err)
			assert.Len(t, nodeInfos, 1)
			pods := toPodList(nodeInfos[0].Pods())
			assert.Contains(t, pods, pod)
			for _, pod := range pods {
				if operationtracker.IsBalloonPod(pod) {
					assert.Equal(t, pod.Spec, tc.expectedBalloonPod.Spec)
					break
				}
			}
		})
	}
}

func TestPreprocessRemovesBPResizeTaint(t *testing.T) {
	node := test.BuildTestNode("node1", 1000, 100*size.MiB)
	node, _, err := taints.AddOrUpdateTaint(node, ekvmtypes.BPResizeTaint)
	assert.NoError(t, err)
	snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
	err = snapshot.AddNodeInfo(framework.NewTestNodeInfo(node))
	assert.NoError(t, err)
	autoscalingCtx := &autoscalingctx.AutoscalingContext{
		ClusterSnapshot: snapshot,
	}
	m := newManagerMock()
	m.On("FilteredNodesSnapshot", true, operationtracker.AllNodes).Return(operationtracker.ResizableNodesSnapshot{"node1": {}})
	m.On("IsResizingEnabled", mock.Anything).Return(
		true)
	cp := &gke.GkeCloudProviderMock{}
	cp.On("MachineConfigProvider").Return(machinetypes.NewMachineConfigProvider(nil))
	p := NewScaleUpNodeProcessor(cp, m, calculator_test.New(), nil, nil, nil, nil)
	err = p.Preprocess(autoscalingCtx)
	assert.NoError(t, err)
	nodeInfo, err := autoscalingCtx.ClusterSnapshot.GetNodeInfo(node.Name)
	assert.NoError(t, err)
	assert.False(t, taints.TaintExists(nodeInfo.Node().Spec.Taints, ekvmtypes.BPResizeTaint))
}

func TestPreprocessInjectsDefaultBalloonPods(t *testing.T) {
	nonEKNode := test.BuildTestNode("node1", 1000, 100*size.MiB)
	ekNodeWithBP := ekvms_test.EkNode8("ek-node-1", 8000, 32*size.GiB)
	ekNodeWithoutBP := ekvms_test.EkNode8("ek-node-2", 8000, 32*size.GiB)

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(nonEKNode))
	assert.NoError(t, err)
	bp := balloonPod(t, "ek-node-1", 2000, 4*size.GiB)
	err = snapshot.AddNodeInfo(framework.NewTestNodeInfo(ekNodeWithBP, bp))
	assert.NoError(t, err)
	err = snapshot.AddNodeInfo(framework.NewTestNodeInfo(ekNodeWithoutBP))
	assert.NoError(t, err)

	autoscalingCtx := &autoscalingctx.AutoscalingContext{
		ClusterSnapshot: snapshot,
	}

	m := newManagerMock()
	m.On("FilteredNodesSnapshot", true, operationtracker.AllNodes).Return(operationtracker.ResizableNodesSnapshot{"ek-node-1": {}})
	m.On("IsResizingEnabled", mock.Anything).Return(
		true)
	cp := &gke.GkeCloudProviderMock{}
	cp.On("MachineConfigProvider").Return(machinetypes.NewMachineConfigProvider(nil))
	p := NewScaleUpNodeProcessor(cp, m, calculator_test.NewWithProvider(machinetypes.NewMachineConfigProvider(nil)), nil, nil, nil, nil)
	err = p.Preprocess(autoscalingCtx)
	assert.NoError(t, err)

	for _, tc := range []struct {
		node                *v1.Node
		wantBalloonPodCount int
		wantBalloonPod      *v1.Pod
	}{
		{
			node:                nonEKNode,
			wantBalloonPodCount: 0,
		},
		{
			node:                ekNodeWithBP,
			wantBalloonPodCount: 1,
			wantBalloonPod:      bp,
		},
		{
			node:                ekNodeWithoutBP,
			wantBalloonPodCount: 1,
		},
	} {
		nodeInfo, err := autoscalingCtx.ClusterSnapshot.GetNodeInfo(tc.node.Name)
		assert.NoError(t, err)
		assert.Len(t, nodeInfo.Pods(), tc.wantBalloonPodCount)
		if tc.wantBalloonPodCount == 1 {
			assert.True(t, operationtracker.IsBalloonPod(nodeInfo.Pods()[0].Pod))
			if tc.wantBalloonPod != nil {
				assert.Equal(t, tc.wantBalloonPod.Spec.Resources, nodeInfo.Pods()[0].Pod.Spec.Resources)
			}
		}
	}
}

func TestTryScheduleCCCPods(t *testing.T) {
	machineType1 := "ek-standard-8"
	machineType2 := "ek-standard-16"

	cccCrd := crd.NewTestCrd(
		crd.WithRules(
			[]rules.Rule{
				rules.NewRule(rules.WithMachineTypeRule(&machineType1)),
				rules.NewRule(rules.WithMachineTypeRule(&machineType2)),
			}),
		crd.WithScaleUpAnyway(),
	)

	rule0Node := operationtracker.ResizableNode{Node: ekvms_test.EkNode32("rule0Node", 500, 1000)}
	rule1Node := operationtracker.ResizableNode{Node: ekvms_test.EkNode32("rule1Node", 500, 1000)}
	scaleUpAnywayNode := operationtracker.ResizableNode{Node: ekvms_test.EkNode32("scaleUpAnywayNode", 500, 1000)}

	testCases := []struct {
		desc              string
		nodes             []testNodeWithPodsInfo
		ekSnapshots       map[int]operationtracker.ResizableNodesSnapshot
		pods              []*v1.Pod
		backedOffRules    map[int]bool
		expectedPodToNode map[string]string // Mapping pod name to expected node name
	}{
		{
			desc: "first rule EKs with space - pod schedulable on first rule node",
			nodes: []testNodeWithPodsInfo{
				{node: rule0Node.Node},
				{node: rule1Node.Node},
				{node: scaleUpAnywayNode.Node},
			},
			ekSnapshots: map[int]operationtracker.ResizableNodesSnapshot{
				0: {"rule0Node": {}},
				1: {"rule1Node": {}},
				2: {"scaleUpAnywayNode": {}},
			},
			pods:              []*v1.Pod{userPod("pod1", 250, 500)},
			expectedPodToNode: map[string]string{"pod1": "rule0Node"},
		},
		{
			desc: "first rule EKs with no space - pod considered for scale up",
			nodes: []testNodeWithPodsInfo{
				{
					node: rule0Node.Node,
					pods: []*v1.Pod{userPod("pod1", 450, 500)},
				},
				{node: rule1Node.Node},
				{node: scaleUpAnywayNode.Node},
			},
			ekSnapshots: map[int]operationtracker.ResizableNodesSnapshot{
				0: {"rule0Node": {}},
				1: {"rule1Node": {}},
				2: {"scaleUpAnywayNode": {}},
			},
			pods:              []*v1.Pod{userPod("pod2", 250, 500)},
			expectedPodToNode: nil,
		},
		{
			desc: "first rule EKs with no space, rule under backoff - pod schedulable on second rule node",
			nodes: []testNodeWithPodsInfo{
				{
					node: rule0Node.Node,
					pods: []*v1.Pod{userPod("busy", 450, 500)},
				},
				{node: rule1Node.Node},
			},
			ekSnapshots: map[int]operationtracker.ResizableNodesSnapshot{
				0: {"rule0Node": {}},
				1: {"rule1Node": {}},
			},
			pods: []*v1.Pod{userPod("pod2", 250, 500)},
			backedOffRules: map[int]bool{
				0: true,
			},
			expectedPodToNode: map[string]string{"pod2": "rule1Node"},
		},
		{
			desc: "Batch overflow - some on rule 0, some on rule 1 (if rule 0 backed off)",
			nodes: []testNodeWithPodsInfo{
				{node: rule0Node.Node}, // 500m free
				{node: rule1Node.Node}, // 500m free
			},
			ekSnapshots: map[int]operationtracker.ResizableNodesSnapshot{
				0: {"rule0Node": {}},
				1: {"rule1Node": {}},
			},
			pods: []*v1.Pod{
				userPod("p1", 350, 500),
				userPod("p2", 350, 500),
			},
			backedOffRules: map[int]bool{
				0: true,
			},
			expectedPodToNode: map[string]string{
				"p1": "rule0Node",
				"p2": "rule1Node",
			},
		},
		{
			desc: "Batch split - rule 0 takes what it can, stops because rule 0 is NOT backed off",
			nodes: []testNodeWithPodsInfo{
				{node: rule0Node.Node}, // 500m free
				{node: rule1Node.Node},
			},
			ekSnapshots: map[int]operationtracker.ResizableNodesSnapshot{
				0: {"rule0Node": {}},
				1: {"rule1Node": {}},
			},
			pods: []*v1.Pod{
				userPod("p1", 350, 500),
				userPod("p2", 350, 500),
			},
			backedOffRules:    map[int]bool{},
			expectedPodToNode: map[string]string{"p1": "rule0Node"}, // Only p1 fits, p2 triggers scale-up
		},
		{
			desc: "scaleUpAnyway option EKs with space - pod schedulable on scaleUpAnyway node",
			nodes: []testNodeWithPodsInfo{
				{
					node: rule0Node.Node,
					pods: []*v1.Pod{userPod("busy1", 450, 500)},
				},
				{
					node: rule1Node.Node,
					pods: []*v1.Pod{userPod("busy2", 450, 500)},
				},
				{node: scaleUpAnywayNode.Node},
			},
			ekSnapshots: map[int]operationtracker.ResizableNodesSnapshot{
				0: {"rule0Node": {}},
				1: {"rule1Node": {}},
				2: {"scaleUpAnywayNode": {}},
			},
			pods: []*v1.Pod{userPod("pod3", 250, 500)},
			backedOffRules: map[int]bool{
				0: true,
				1: true,
			},
			expectedPodToNode: map[string]string{"pod3": "scaleUpAnywayNode"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			m := newManagerMock()
			m.On("IsNodeInProcess", mock.AnythingOfType("string")).Return(false)
			b := NewFakeCCCRuleBackoff(map[string]map[int]bool{
				cccCrd.Name(): tc.backedOffRules,
			})
			cp := &gke.GkeCloudProviderMock{}
			cp.On("MachineConfigProvider").Return(machinetypes.NewMachineConfigProvider(nil))
			p := NewScaleUpNodeProcessor(cp, m, nil, nil, b, nil, nil)

			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			for _, nodeWithPods := range tc.nodes {
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(nodeWithPods.node, nodeWithPods.pods...))
				assert.Nil(t, err)
			}

			var podsStatuses []scheduling.Status
			for _, pod := range tc.pods {
				podsStatuses = append(podsStatuses, scheduling.Status{Pod: pod, NodeName: ""})
			}

			schedulable, _, err := p.tryScheduleCCCPods(snapshot, tc.ekSnapshots, podsStatuses, cccCrd, false)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.expectedPodToNode), len(schedulable))

			// Verify each pod is on its specific expected node
			for podName, expectedNodeName := range tc.expectedPodToNode {
				nodeInfo, err := snapshot.GetNodeInfo(expectedNodeName)
				assert.NoError(t, err)

				found := false
				for _, podInfo := range nodeInfo.Pods() {
					if podInfo.Pod.Name == podName {
						found = true
						break
					}
				}
				assert.True(t, found, "pod %q was not found on expected node %q", podName, expectedNodeName)
			}
		})
	}
}

func TestTrySchedulePodsOnSpecifiedNodes(t *testing.T) {
	testCases := []struct {
		desc                  string
		nodes                 []testNodeWithPodsInfo
		ekSnapshot            operationtracker.ResizableNodesSnapshot
		pods                  []*v1.Pod
		nodesStateForSchedule NodesState
		expectedPodToNode     map[string]string
	}{
		{
			desc: "Available allocatable - multiple pods fit",
			nodes: []testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("ek-node", 1000, 1024*size.KiB),
					pods: []*v1.Pod{},
				}},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"ek-node": {},
			},
			pods: []*v1.Pod{
				userPod("pod1", 250, 500),
				userPod("pod2", 250, 500),
			},
			nodesStateForSchedule: allNodes,
			expectedPodToNode:     map[string]string{"pod1": "ek-node", "pod2": "ek-node"},
		},
		{
			desc: "Partial scheduling - only first pod fits",
			nodes: []testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("ek-node", 500, 1024*size.KiB),
					pods: []*v1.Pod{},
				}},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"ek-node": {},
			},
			pods: []*v1.Pod{
				userPod("pod1", 400, 500),
				userPod("pod2", 400, 500),
			},
			nodesStateForSchedule: allNodes,
			expectedPodToNode:     map[string]string{"pod1": "ek-node"},
		},
		{
			desc: "No available allocatable - none fit",
			nodes: []testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("ek-node", 1000, 1024*size.KiB),
					pods: []*v1.Pod{userPod("busy", 1000, 1024*size.KiB)},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"ek-node": {},
			},
			pods: []*v1.Pod{
				userPod("pod1", 250, 500*size.KiB),
			},
			nodesStateForSchedule: allNodes,
			expectedPodToNode:     map[string]string{},
		},
		{
			desc: "Schedule on idle nodes - multiple pods",
			nodes: []testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("ek-node", 1000, 1024*size.KiB),
					pods: []*v1.Pod{},
				},
				{
					node: ekvms_test.EkNode32("ek-node-in-process", 1000, 1024*size.KiB),
					pods: []*v1.Pod{},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"ek-node":            {},
				"ek-node-in-process": {},
			},
			pods: []*v1.Pod{
				userPod("pod1", 250, 500),
				userPod("pod2", 250, 500),
			},
			nodesStateForSchedule: idleNodes,
			expectedPodToNode:     map[string]string{"pod1": "ek-node", "pod2": "ek-node"},
		},
		{
			desc: "Schedule on processing nodes - multiple pods",
			nodes: []testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("ek-node", 1000, 1024*size.KiB),
					pods: []*v1.Pod{},
				},
				{
					node: ekvms_test.EkNode32("ek-node-in-process", 1000, 1024*size.KiB),
					pods: []*v1.Pod{},
				},
			},
			ekSnapshot: map[string]operationtracker.ResizableNode{
				"ek-node":            {},
				"ek-node-in-process": {},
			},
			pods: []*v1.Pod{
				userPod("pod1", 250, 500),
				userPod("pod2", 250, 500),
			},
			nodesStateForSchedule: processingNodes,
			expectedPodToNode:     map[string]string{"pod1": "ek-node-in-process", "pod2": "ek-node-in-process"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			for _, nodeWithPods := range tc.nodes {
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(nodeWithPods.node, nodeWithPods.pods...))
				assert.Nil(t, err)
			}
			m := newManagerMock()
			m.On("IsNodeInProcess", "ek-node-in-process").Return(true)
			m.On("IsNodeInProcess", mock.AnythingOfType("string")).Return(false)
			cp := &gke.GkeCloudProviderMock{}
			cp.On("MachineConfigProvider").Return(machinetypes.NewMachineConfigProvider(nil))
			p := NewScaleUpNodeProcessor(cp, m, calculator_test.New(), nil, nil, nil, nil)
			nodeFilter := p.createNodeFilterForPodNodeState(tc.ekSnapshot, tc.nodesStateForSchedule)

			var podsStatuses []scheduling.Status
			for _, pod := range tc.pods {
				podsStatuses = append(podsStatuses, scheduling.Status{Pod: pod, NodeName: ""})
			}

			schedulablePods, unschedulablePods, err := p.trySchedulePodsOnSpecifiedNodes(snapshot, podsStatuses, nodeFilter)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.expectedPodToNode), len(schedulablePods))
			assert.Equal(t, len(tc.pods)-len(tc.expectedPodToNode), len(unschedulablePods))

			// Verify each scheduled pod exists on one of the expected nodes
			for podName, expectedNodeName := range tc.expectedPodToNode {
				nodeInfo, err := snapshot.GetNodeInfo(expectedNodeName)
				assert.NoError(t, err)

				found := false
				for _, podInfo := range nodeInfo.Pods() {
					if podInfo.Pod.Name == podName {
						found = true
						break
					}
				}
				assert.True(t, found, "pod %q was not found on expected node %q", podName, expectedNodeName)
			}
		})
	}
}

func TestTrySchedulePods(t *testing.T) {
	testCases := []struct {
		desc                  string
		nodes                 []testNodeWithPodsInfo
		ekSnapshot            operationtracker.ResizableNodesSnapshot
		pods                  []*v1.Pod
		isLookaheadPods       bool
		expectedSchedulable   []*v1.Pod
		expectedUnschedulable []*v1.Pod
	}{
		{
			desc: "schedule_la_on_upcoming_ek_node",
			nodes: []testNodeWithPodsInfo{
				{
					node: ekvms_test.NewResizableNodeBuilder("upcoming-ek-node", 1000, 1024*size.KiB).WithSupportedMachineType("ek-standard-32").WithMachineFamily("ek").WithAnnotations(
						map[string]string{
							annotations.NodeUpcomingAnnotation: "true",
						}).Build(),
					pods: []*v1.Pod{},
				},
			},
			ekSnapshot:          operationtracker.ResizableNodesSnapshot{}, // Upcoming nodes are not in EK snapshot
			pods:                []*v1.Pod{lookaheadPod("pod1", 250, 500)},
			isLookaheadPods:     true,
			expectedSchedulable: []*v1.Pod{lookaheadPod("pod1", 250, 500)},
		},
		{
			desc: "all_pods_accounted_for_across_filters",
			nodes: []testNodeWithPodsInfo{
				{
					node: ekvms_test.EkNode32("resizable", 1000, 1024*size.KiB),
					pods: []*v1.Pod{},
				},
				{
					node: ekvms_test.EkNode32("non-resizable", 1000, 1024*size.KiB),
					pods: []*v1.Pod{},
				},
				{
					node: ekvms_test.NewResizableNodeBuilder("upcoming-ek-node", 1000, 1024*size.KiB).WithSupportedMachineType("ek-standard-32").WithMachineFamily("ek").WithAnnotations(
						map[string]string{
							annotations.NodeUpcomingAnnotation: "true",
						}).Build(),
					pods: []*v1.Pod{},
				},
			},
			ekSnapshot: operationtracker.ResizableNodesSnapshot{
				"resizable": operationtracker.ResizableNode{
					DesiredSize:      size.Allocatable{MilliCpus: 1000, KBytes: 1000},
					UpsizableMaxSize: size.Allocatable{MilliCpus: 2000, KBytes: 2000},
				},
				"non-resizable": operationtracker.ResizableNode{
					DesiredSize:      size.Allocatable{MilliCpus: 1000, KBytes: 1000},
					UpsizableMaxSize: size.Allocatable{MilliCpus: 100, KBytes: 100}, // not safely resizable
				},
			},
			pods: []*v1.Pod{
				lookaheadPod("pod1", 800, 500), // will fit on resizable node
				lookaheadPod("pod2", 800, 500), // will fit on non-resizable node
				lookaheadPod("pod3", 800, 500), // will fit on upcoming node
				lookaheadPod("pod4", 800, 500), // wont fit any
			},
			isLookaheadPods:       true,
			expectedSchedulable:   []*v1.Pod{lookaheadPod("pod1", 800, 500), lookaheadPod("pod2", 800, 500), lookaheadPod("pod3", 800, 500)},
			expectedUnschedulable: []*v1.Pod{lookaheadPod("pod4", 800, 500)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			m := newManagerMock()
			m.On("IsNodeInProcess", mock.AnythingOfType("string")).Return(false)
			cp := &gke.GkeCloudProviderMock{}
			cp.On("MachineConfigProvider").Return(machinetypes.NewMachineConfigProvider(nil))
			p := NewScaleUpNodeProcessor(cp, m, nil, nil, nil, nil, nil)
			snapshot := testsnapshot.NewCustomTestSnapshotOrDie(t, store.NewDeltaSnapshotStore())
			for _, nodeWithPods := range tc.nodes {
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(nodeWithPods.node, nodeWithPods.pods...))
				assert.Nil(t, err)
			}

			var podsStatuses []scheduling.Status
			for _, pod := range tc.pods {
				podsStatuses = append(podsStatuses, scheduling.Status{Pod: pod, NodeName: ""})
			}

			schedulable, unschedulable, err := p.trySchedulePods(snapshot, tc.ekSnapshot, podsStatuses, tc.isLookaheadPods)
			assert.NoError(t, err)

			schedulablePods := []*v1.Pod{}
			for _, status := range schedulable {
				schedulablePods = append(schedulablePods, status.Pod)
			}
			assert.Equal(t, tc.expectedSchedulable, schedulablePods)

			unschedulablePods := []*v1.Pod{}
			for _, status := range unschedulable {
				unschedulablePods = append(unschedulablePods, status.Pod)
			}
			if tc.expectedUnschedulable == nil {
				assert.Empty(t, unschedulablePods)
			} else {
				assert.ElementsMatch(t, tc.expectedUnschedulable, unschedulablePods)
			}

			// Map check to verify all pods accounted for
			allPods := make(map[string]bool)
			for _, p := range schedulable {
				allPods[p.Pod.Name] = true
			}
			for _, p := range unschedulable {
				allPods[p.Pod.Name] = true
			}
			expectedAllPods := make(map[string]bool)
			for _, p := range tc.pods {
				expectedAllPods[p.Name] = true
			}
			assert.Equal(t, expectedAllPods, allPods)
		})
	}
}

type nodeWithMig struct {
	node *v1.Node
	mig  *gke.GkeMig
}

func TestOrganizeByCCCByRule(t *testing.T) {
	machineType1 := "ek-standard-8"
	machineType2 := "ek-standard-16"

	cccCrds := []crd.CRD{
		crd.NewTestCrd(
			crd.WithName("ccc1"),
			crd.WithRules(
				[]rules.Rule{
					rules.NewRule(rules.WithMachineTypeRule(&machineType1)),
					rules.NewRule(rules.WithMachineTypeRule(&machineType2)),
				}),
			crd.WithLabel(gkelabels.ComputeClassLabel),
		),
		crd.NewTestCrd(
			crd.WithName("ccc2"),
			crd.WithLabel(gkelabels.ComputeClassLabel),
			crd.WithScaleUpAnyway(),
		),
	}

	ccc1Rule1Mig := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		MachineType: machineType2,
		Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc1"},
	}).Build()
	ccc1ScaleUpAnywayMig := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "ek-standard-32",
		Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc1"},
	}).Build()
	ccc2ScaleUpAnywayMig := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "ek-standard-32",
		Labels:      map[string]string{gkelabels.ComputeClassLabel: "ccc2"},
	}).Build()

	testCases := []struct {
		desc                string
		ekNodesWithMigs     []nodeWithMig
		expectedEkSnapshots resizableNodesSnapshotsByCCC
	}{
		{
			desc: "CCC nodes",
			ekNodesWithMigs: []nodeWithMig{
				{
					node: ekvms_test.NewResizableNodeBuilder("ccc2ScaleUpAnywayNode2", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc2").Build(),
					mig:  ccc2ScaleUpAnywayMig,
				},
				{
					node: ekvms_test.NewResizableNodeBuilder("ccc1Rule1Node1", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").Build(),
					mig:  ccc1Rule1Mig,
				},
				{
					node: ekvms_test.NewResizableNodeBuilder("ccc2ScaleUpAnywayNode1", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc2").Build(),
					mig:  ccc2ScaleUpAnywayMig,
				},
			},
			expectedEkSnapshots: resizableNodesSnapshotsByCCC{
				"ccc1": {
					1: {
						"ccc1Rule1Node1": operationtracker.ResizableNode{
							Node: ekvms_test.NewResizableNodeBuilder("ccc1Rule1Node1", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").Build(),
						},
					},
				},
				"ccc2": {
					0: {
						"ccc2ScaleUpAnywayNode1": operationtracker.ResizableNode{
							Node: ekvms_test.NewResizableNodeBuilder("ccc2ScaleUpAnywayNode1", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc2").Build(),
						},
						"ccc2ScaleUpAnywayNode2": operationtracker.ResizableNode{
							Node: ekvms_test.NewResizableNodeBuilder("ccc2ScaleUpAnywayNode2", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc2").Build(),
						},
					},
				},
			},
		},
		{
			desc: "Non-CCC nodes",
			ekNodesWithMigs: []nodeWithMig{
				{
					node: ekvms_test.EkNode32("nonCCCNode", 32, 128),
				},
			},
			expectedEkSnapshots: resizableNodesSnapshotsByCCC{
				"": {
					0: {
						"nonCCCNode": operationtracker.ResizableNode{
							Node: ekvms_test.EkNode32("nonCCCNode", 32, 128),
						},
					},
				},
			},
		},
		{
			desc: "Skip nodes without associated mig",
			ekNodesWithMigs: []nodeWithMig{
				{
					node: ekvms_test.NewResizableNodeBuilder("ccc1Node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").Build(),
				},
			},
			expectedEkSnapshots: make(resizableNodesSnapshotsByCCC),
		},
		{
			desc: "Skip nodes with unexisting CCC",
			ekNodesWithMigs: []nodeWithMig{
				{
					node: ekvms_test.NewResizableNodeBuilder("ccc3Node", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc3").Build(),
				},
			},
			expectedEkSnapshots: make(resizableNodesSnapshotsByCCC),
		},
		{
			desc: "Skip nodes with CCC not mathching any rules and withouth ScaleUpAnyway enabled",
			ekNodesWithMigs: []nodeWithMig{
				{
					node: ekvms_test.NewResizableNodeBuilder("ccc1ScaleUpAnywayNode", 32, 128).WithLabel(gkelabels.ComputeClassLabel, "ccc1").Build(),
					mig:  ccc1ScaleUpAnywayMig,
				},
			},
			expectedEkSnapshots: make(resizableNodesSnapshotsByCCC),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cloudProvider := &gke.GkeCloudProviderMock{}
			cloudProvider.On("IsAutopilotEnabled", mock.Anything).Return(true)
			ekSnapshot := operationtracker.ResizableNodesSnapshot{}
			for _, nodeWithMig := range tc.ekNodesWithMigs {
				cloudProvider.On("GkeMigForNode", nodeWithMig.node).Return(nodeWithMig.mig, nil)
				ekSnapshot[nodeWithMig.node.Name] = operationtracker.ResizableNode{Node: nodeWithMig.node}
			}

			cccLister := lister.NewMockCrdListerWithLabel(cccCrds, gkelabels.ComputeClassLabel)
			p := NewScaleUpNodeProcessor(cloudProvider, nil, nil, nil, nil, cccLister, nil)

			ekSnapshots := p.organizeByCCCByRule(ekSnapshot)
			assert.Equal(t, tc.expectedEkSnapshots, ekSnapshots)
		})
	}
}

func TestPodDetails(t *testing.T) {
	testCases := []struct {
		desc     string
		pods     []*v1.Pod
		expected []string
	}{
		{
			desc:     "no pods",
			pods:     []*v1.Pod{},
			expected: []string{},
		},
		{
			desc: "one pod with no workload separation",
			pods: []*v1.Pod{
				test.BuildTestPod("p1", 1000, 1*size.GiB),
			},
			expected: []string{"{name: p1, cpu: 1, memory: 1073741824, workload separation info: }"},
		},
		{
			desc: "one pod with toleration but no matching selector",
			pods: []*v1.Pod{
				test.BuildTestPod("p1", 1000, 1*size.GiB, func(p *v1.Pod) {
					p.Spec.Tolerations = []v1.Toleration{
						{Key: "key1", Operator: v1.TolerationOpExists},
					}
				}),
			},
			expected: []string{"{name: p1, cpu: 1, memory: 1073741824, workload separation info: }"},
		},
		{
			desc: "one pod with matching toleration and selector",
			pods: []*v1.Pod{
				test.BuildTestPod("p1", 500, 512*size.MiB, func(p *v1.Pod) {
					p.Spec.Tolerations = []v1.Toleration{
						{Key: "key1", Operator: v1.TolerationOpEqual, Value: "val1"},
					}
					p.Spec.NodeSelector = map[string]string{"key1": "val1"}
				}),
			},
			expected: []string{"{name: p1, cpu: 500m, memory: 536870912, workload separation info: key1=val1}"},
		},
		{
			desc: "one pod with multiple matching tolerations",
			pods: []*v1.Pod{
				test.BuildTestPod("p1", 500, 512*size.MiB, func(p *v1.Pod) {
					p.Spec.Tolerations = []v1.Toleration{
						{Key: "key1", Operator: v1.TolerationOpExists},
						{Key: "key2", Operator: v1.TolerationOpEqual, Value: "val2"},
					}
					p.Spec.NodeSelector = map[string]string{
						"key1": "any-value",
						"key2": "val2",
					}
				}),
			},
			expected: []string{"{name: p1, cpu: 500m, memory: 536870912, workload separation info: key1=,key2=val2}"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			actual := podDetails(tc.pods)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestTrackUnschedulablePods(t *testing.T) {
	p1 := test.BuildTestPod("pod1", 100, 100)
	p2 := test.BuildTestPod("pod2", 100, 100)
	p3 := test.BuildTestPod("pod3", 100, 100)
	fakePod := fake.WithFakePodAnnotation(test.BuildTestPod("pod4", 100, 100))

	testCases := []struct {
		desc                           string
		forceScaleUpDisabled           bool
		initialAttempts                map[podId]int
		unschedulableInTheBeginning    [][]*v1.Pod
		unschedulableBeforeEkProcessor [][]*v1.Pod
		unschedulableAfterEkProcessor  [][]*v1.Pod
		expectedAttempts               []map[podId]int
	}{
		{
			desc:                           "Feature disabled - no changes",
			forceScaleUpDisabled:           true,
			initialAttempts:                map[podId]int{},
			unschedulableInTheBeginning:    [][]*v1.Pod{{p1, p2}, {p1, p2}},
			unschedulableBeforeEkProcessor: [][]*v1.Pod{{p1, p2}, {p1, p2}},
			unschedulableAfterEkProcessor:  [][]*v1.Pod{{p1}, {p1}},
			expectedAttempts:               []map[podId]int{{}, {}},
		},
		{
			desc:                           "Feature enabled - everything is scheduled, nothing to track",
			initialAttempts:                map[podId]int{},
			unschedulableInTheBeginning:    [][]*v1.Pod{{}},
			unschedulableBeforeEkProcessor: [][]*v1.Pod{{}},
			unschedulableAfterEkProcessor:  [][]*v1.Pod{{}},
			expectedAttempts:               []map[podId]int{{}},
		},
		{
			desc:                           "Feature enabled - check the increase of counter for new real pod (fake is skipped)",
			initialAttempts:                map[podId]int{},
			unschedulableInTheBeginning:    [][]*v1.Pod{{p1, p2, fakePod}},
			unschedulableBeforeEkProcessor: [][]*v1.Pod{{p1, p2, fakePod}},
			unschedulableAfterEkProcessor:  [][]*v1.Pod{{p1}},            // pod2 and fakePod are tried to be scheduled on upcoming upsize
			expectedAttempts:               []map[podId]int{{p2.UID: 1}}, // increase the counter of pod2
		},
		{
			desc:                           "Feature enabled - check the map clean up for scheduled pods",
			initialAttempts:                map[podId]int{p1.UID: 2, p2.UID: 1},
			unschedulableInTheBeginning:    [][]*v1.Pod{{p1}}, // p2 is scheduled, so it is not present even here
			unschedulableBeforeEkProcessor: [][]*v1.Pod{{p1}},
			unschedulableAfterEkProcessor:  [][]*v1.Pod{{p1}},
			expectedAttempts:               []map[podId]int{{p1.UID: 2}},
		},
		{
			desc:                           "Feature enabled - test complex scenario",
			initialAttempts:                map[podId]int{},
			unschedulableInTheBeginning:    [][]*v1.Pod{{p1, p2, p3}, {p1, p2}, {p2}, {p2}},
			unschedulableBeforeEkProcessor: [][]*v1.Pod{{p1, p2, p3}, {}, {p2}, {p2}},
			unschedulableAfterEkProcessor:  [][]*v1.Pod{{p3}, {}, {p2}, {}},
			// 1. p1 and p2 tries an upsize (increase their counters), p3 is scheduled on scale up
			// 2. p1 and p2 are still unscheduled and filtered -> nothing to do
			// 3. p1 is scheduled, p2 faced failed upsize (because it is not filtered) and does not do anything yet (it is in both before and after list) -> remove p1 from the map
			// 4. p2 tries another upsize -> increment the counter of p2
			expectedAttempts: []map[podId]int{{p1.UID: 1, p2.UID: 1}, {p1.UID: 1, p2.UID: 1}, {p2.UID: 1}, {p2.UID: 2}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			customThresholdsProviderMock := &mockCustomThresholdsProvider{}
			customThresholdsProviderMock.On("IsForceScaleUpFeatureEnabled").Return(!tc.forceScaleUpDisabled)
			p := &ScaleUpNodeProcessor{
				cloudProvider:              &gke.GkeCloudProviderMock{},
				mcp:                        machinetypes.NewMachineConfigProvider(nil),
				customThresholdsProvider:   customThresholdsProviderMock,
				attemptsToScheduleOnUpsize: tc.initialAttempts,
			}
			for i := range tc.unschedulableInTheBeginning {
				p.TrackUnschedulablePods(tc.unschedulableInTheBeginning[i], tc.unschedulableBeforeEkProcessor[i], tc.unschedulableAfterEkProcessor[i])
				assert.Equal(t, tc.expectedAttempts[i], p.attemptsToScheduleOnUpsize)
			}

		})
	}
}

func TestFilterPodsForcingScaleUp(t *testing.T) {
	p1 := test.BuildTestPod("pod1", 100, 100)
	p2 := test.BuildTestPod("pod2", 100, 100)

	testCases := []struct {
		desc                      string
		threshold                 int
		numberOfAttemptsPerPod    map[podId]int
		initialUnschedulablePods  []*v1.Pod
		expectedUnschedulablePods []*v1.Pod
		expectedFilteredPods      []*v1.Pod
	}{
		{
			desc:                      "Two pods exceed the threshold",
			threshold:                 3,
			numberOfAttemptsPerPod:    map[podId]int{p1.UID: 5, p2.UID: 5},
			initialUnschedulablePods:  []*v1.Pod{p1, p2},
			expectedUnschedulablePods: []*v1.Pod{},
			expectedFilteredPods:      []*v1.Pod{p1, p2},
		},
		{
			desc:                      "Two pods attempts are equal to the threshold",
			threshold:                 5,
			numberOfAttemptsPerPod:    map[podId]int{p1.UID: 5, p2.UID: 5},
			initialUnschedulablePods:  []*v1.Pod{p1, p2},
			expectedUnschedulablePods: []*v1.Pod{p1, p2},
			expectedFilteredPods:      []*v1.Pod{},
		},
		{
			desc:                      "One pod exceeds the threshold",
			threshold:                 1,
			numberOfAttemptsPerPod:    map[podId]int{p1.UID: 1, p2.UID: 3},
			initialUnschedulablePods:  []*v1.Pod{p1, p2},
			expectedUnschedulablePods: []*v1.Pod{p1},
			expectedFilteredPods:      []*v1.Pod{p2},
		},
		{
			desc:                      "Zero pods exceed the threshold",
			threshold:                 10,
			numberOfAttemptsPerPod:    map[podId]int{p1.UID: 4, p2.UID: 3},
			initialUnschedulablePods:  []*v1.Pod{p1, p2},
			expectedUnschedulablePods: []*v1.Pod{p1, p2},
			expectedFilteredPods:      []*v1.Pod{},
		},
		{
			desc:                      "Map is empty, nothing to filter out",
			threshold:                 1,
			numberOfAttemptsPerPod:    map[podId]int{},
			initialUnschedulablePods:  []*v1.Pod{p1, p2},
			expectedUnschedulablePods: []*v1.Pod{p1, p2},
			expectedFilteredPods:      []*v1.Pod{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			unschedulablePods, filteredPods := filterPodsForcingScaleUp(tc.initialUnschedulablePods, tc.numberOfAttemptsPerPod, tc.threshold)
			assert.Equal(t, tc.expectedUnschedulablePods, unschedulablePods)
			assert.Equal(t, tc.expectedFilteredPods, filteredPods)
		})
	}
}

type fakeCCCRuleBackoff struct {
	backedOffRules map[string]map[int]bool
}

func NewFakeCCCRuleBackoff(backedOffRules map[string]map[int]bool) *fakeCCCRuleBackoff {
	return &fakeCCCRuleBackoff{backedOffRules: backedOffRules}
}

func (b *fakeCCCRuleBackoff) RuleBackoffStatus(npcCrd crd.CRD, ruleIdx int, currentTime time.Time) backoff.Status {
	if _, ok := b.backedOffRules[npcCrd.Name()]; !ok {
		return backoff.Status{IsBackedOff: false}
	}
	return backoff.Status{
		IsBackedOff: b.backedOffRules[npcCrd.Name()][ruleIdx],
	}
}

type ManagerMock struct {
	mock.Mock
	calculator            calculator.Calculator
	downsizedNodes        map[string]bool
	nodesScaleDownAllowed map[string]bool
}

func newManagerMock() *ManagerMock {
	return &ManagerMock{
		calculator:            calculator_test.New(),
		downsizedNodes:        map[string]bool{},
		nodesScaleDownAllowed: map[string]bool{},
	}
}

// Run is a mocked method.
func (m *ManagerMock) Run(ctx context.Context) {
	_ = m.Called(ctx)
}

// Upsize is a mocked method.
func (m *ManagerMock) Upsize(node *v1.Node, size size.Allocatable) error {
	args := m.Called(node, size)
	return args.Error(0)
}

// Downsize is a mocked method.
func (m *ManagerMock) Downsize(node *v1.Node, size size.Allocatable) error {
	args := m.Called(node, size)
	err := args.Error(0)
	if err == nil {
		m.downsizedNodes[node.Name] = true
	}
	return err
}

// FilteredNodesSnapshot is a mocked method.
func (m *ManagerMock) FilteredNodesSnapshot(forceRefresh bool, mode operationtracker.SnapshotFilterMode) operationtracker.ResizableNodesSnapshot {
	args := m.Called(forceRefresh, mode)
	return args.Get(0).(operationtracker.ResizableNodesSnapshot)
}

// UnhealthyNodesWithStatus is a mocked method.
func (m *ManagerMock) UnhealthyNodesWithStatus(status operationtracker.UnhealthyResizableNodeStatus) []string {
	args := m.Called(status)
	return args.Get(0).([]string)
}

// IsResizingEnabled is a mocked method.
func (m *ManagerMock) IsResizingEnabled(machineFamily string) bool {
	args := m.Called(machineFamily)
	return args.Get(0).(bool)
}

// IsNodeInProcess is a mocked method
func (m *ManagerMock) IsNodeInProcess(nodeName string) bool {
	args := m.Called(nodeName)
	return args.Get(0).(bool)
}

// IsNodeResizingOrPending is a mocked method
func (m *ManagerMock) IsNodeResizingOrPending(nodeName string) bool {
	args := m.Called(nodeName)
	return args.Get(0).(bool)
}

// GetNodesScaleDownAllowedFromCache is a mocked method
func (m *ManagerMock) GetNodesScaleDownAllowedFromCache(nodeNames []string) map[string]bool {
	m.Called(nodeNames)
	scaleDownAllowed := map[string]bool{}
	for _, nodeName := range nodeNames {
		if allowed, found := m.nodesScaleDownAllowed[nodeName]; found {
			scaleDownAllowed[nodeName] = allowed
		}
	}
	return scaleDownAllowed
}

// UpdateNodesScaleDownAllowedCache is a mocked method
func (m *ManagerMock) UpdateNodesScaleDownAllowedCache(nodesScaleDownAllowed map[string]bool) {
	m.Called(nodesScaleDownAllowed)
	for nodeName, allowed := range nodesScaleDownAllowed {
		m.nodesScaleDownAllowed[nodeName] = allowed
	}
}

// InvalidateNodesScaleDownAllowedCache is a mocked method
func (m *ManagerMock) InvalidateNodesScaleDownAllowedCache() {
	m.Called()
	m.nodesScaleDownAllowed = map[string]bool{}
}

// NodesCount is a mocked method.
func (m *ManagerMock) NodesCount(machineFamily string) int {
	args := m.Called(machineFamily)
	return args.Int(0)
}

// balloonPod creates a balloon pod with specified resource requests.
func balloonPod(t *testing.T, nodeName string, milliCpu, memBytes int64) *v1.Pod {
	cpu := *resource.NewMilliQuantity(milliCpu, resource.DecimalSI)
	mem := *resource.NewQuantity(memBytes, resource.DecimalSI)

	pod, err := operationtracker.GenerateBalloonPod(ekvms_test.EkNode32(nodeName, 1, 1), cpu, mem, true)
	assert.NoError(t, err)
	return pod
}

type mockScaleUpMetrics struct {
	mock.Mock
}

func (m *mockScaleUpMetrics) RegisterResizableVmPodsSchedulableOnUpsizes(machineFamily string, pods int) {
	m.MethodCalled("RegisterResizableVmPodsSchedulableOnUpsizes", machineFamily, pods)
}

func (m *mockScaleUpMetrics) UpdateResizableVmUnschedulableLookaheadPodsCount(machineFamily string, laPods int) {
	m.MethodCalled("UpdateResizableVmUnschedulableLookaheadPodsCount", machineFamily, laPods)
}

func (m *mockScaleUpMetrics) UpdateResizableVmTotalNodesLookaheadSpace(machineFamily string, size size.Allocatable) {
	m.MethodCalled("UpdateResizableVmTotalNodesLookaheadSpace", machineFamily, size)
}

type mockDSLister struct {
	mock.Mock
}

func (m *mockDSLister) List(selector labels.Selector) (ret []*appsv1.DaemonSet, err error) {
	args := m.Called(selector)
	return args.Get(0).([]*appsv1.DaemonSet), args.Error(1)
}

func (m *mockDSLister) DaemonSets(namespace string) listersv1.DaemonSetNamespaceLister {
	args := m.Called(namespace)
	return args.Get(0).(listersv1.DaemonSetNamespaceLister)
}

func (m *mockDSLister) GetPodDaemonSets(pod *v1.Pod) ([]*appsv1.DaemonSet, error) {
	args := m.Called(pod)
	return args.Get(0).([]*appsv1.DaemonSet), args.Error(1)
}

func (m *mockDSLister) GetHistoryDaemonSets(history *appsv1.ControllerRevision) ([]*appsv1.DaemonSet, error) {
	args := m.Called(history)
	return args.Get(0).([]*appsv1.DaemonSet), args.Error(1)
}

func daemonSet(name string, milliCPU, memBytes int64, selector map[string]string) *appsv1.DaemonSet {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID(name),
		},
		Spec: appsv1.DaemonSetSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					NodeSelector: selector,
				},
			},
		},
	}
	ds.Spec.Template.Spec.Containers = []v1.Container{{
		Name: "container",
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(memBytes, resource.BinarySI),
			},
		},
	}}
	return ds
}

func daemonSetPod(name string, milliCPU, memBytes int64, nodeName string) *v1.Pod {
	ds := daemonSet(name, milliCPU, memBytes, nil)
	dsPod := daemon.NewPod(ds, nodeName)
	dsPod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(ds, appsv1.SchemeGroupVersion.WithKind("DaemonSet"))}
	return dsPod
}

func buildPodWithNodeSelector(name string, millicpu, mem int64, nodeSelector map[string]string) *v1.Pod {
	pod := test.BuildTestPod(name, millicpu, mem)
	pod.Spec.NodeSelector = nodeSelector
	return pod
}

type mockCustomThresholdsProvider struct {
	mock.Mock
}

func (m *mockCustomThresholdsProvider) RefreshCustomThresholds() {
	m.Called()
}

func (m *mockCustomThresholdsProvider) IsErrorThresholdsFeatureEnabled() bool {
	return m.Called().Bool(0)
}

func (m *mockCustomThresholdsProvider) GetThreshold(errorType string) (int, bool) {
	args := m.Called(errorType)
	return args.Int(0), args.Bool(1)
}

func (m *mockCustomThresholdsProvider) IsForceScaleUpFeatureEnabled() bool {
	return m.Called().Bool(0)
}

func (m *mockCustomThresholdsProvider) GetUpsizeTriesThreshold() int {
	return m.Called().Int(0)
}
