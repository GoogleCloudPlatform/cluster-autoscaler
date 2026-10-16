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

package churntracker

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	ekvms_test "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	fakeclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/annotations"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const testWorkloadID = "NoSchedule:group:one"

var (
	nodeHistoryCmpOpts = []cmp.Option{cmp.AllowUnexported(nodeHistory{}), cmpopts.EquateEmpty()}
	testMcp            = machinetypes.NewMachineConfigProvider(nil)
	ekFamily           = machinetypes.EK.Name()
	e4aFamily          = machinetypes.E4A.Name()
)

func TestProcessNodeInfos(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	tMinus5m := now.Add(-5 * time.Minute)

	// The same node in different states, so every case shows how its history changes in one loop.
	const nodeName = "node"
	laPod := lookaheadPod("la-pod", 100, 100)
	separatedLAPod := lookaheadbuffer.BuildTestLookaheadPod(testWorkloadID, 100, 100, machinetypes.EK.Name(), lookaheadbuffer.WithName("separated-la-pod"))
	scheduledCxPod := bindTo(userPod("user-pod", 100, 100), nodeName)
	notScheduledCxPod := userPod("user-pod", 100, 100)
	sysPod := systemPod("system-pod", 100, 100)
	dsPod := daemonsetPod("ds-pod", 100, 100)

	emptyNode := ekNodeInfo(nodeName, sysPod)
	nodeWithLA := ekNodeInfo(nodeName, laPod)
	separatedNodeWithSeparatedLA := framework.NewTestNodeInfo(separatedEkNode(nodeName), separatedLAPod)
	nodeWithSeparatedLA := ekNodeInfo(nodeName, separatedLAPod)
	nodeWithScheduledCxPod := ekNodeInfo(nodeName, scheduledCxPod)
	nodeWithNotScheduledCxPod := ekNodeInfo(nodeName, notScheduledCxPod)
	nodeWithLAScheduledCxPod := ekNodeInfo(nodeName, scheduledCxPod, laPod)
	nodeWithLANotScheduledCxPod := ekNodeInfo(nodeName, notScheduledCxPod, laPod)
	nodeWithDaemonSetPod := ekNodeInfo(nodeName, dsPod)
	upcomingNodeWithLA := framework.NewTestNodeInfo(withUID(upcomingNode(nodeName, 1000, 1000)), laPod)
	noUIDNodeWithLA := framework.NewTestNodeInfo(ekvms_test.EkNode32(nodeName, 1000, 1000), laPod)
	nonResizableNodeWithLA := framework.NewTestNodeInfo(nonResizableNode(nodeName), laPod)
	noMachineTypeNodeWithLA := framework.NewTestNodeInfo(withUID(test.BuildTestNode(nodeName, 1000, 1000)), laPod)
	nodeUID := emptyNode.Node().UID

	for _, tc := range []struct {
		desc           string
		nodesBefore    map[types.UID]*nodeHistory
		nodeInfos      []*framework.NodeInfo
		wantNodesAfter map[types.UID]*nodeHistory
	}{
		{
			desc: "empty node gets a lookahead pod",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m}},
			nodeInfos: []*framework.NodeInfo{
				nodeWithLA,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true},
			},
		},
		{
			desc: "empty node of a separated workload gets a lookahead pod, the node's workload ID is recorded",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m}},
			nodeInfos: []*framework.NodeInfo{
				separatedNodeWithSeparatedLA,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
		},
		{
			desc: "workload ID is taken from the node, not from the lookahead pod",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m}},
			nodeInfos: []*framework.NodeInfo{
				nodeWithSeparatedLA,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true, lookaheadWorkloadID: ""},
			},
		},
		{
			desc: "empty node gets a scheduled customer pod",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m}},
			nodeInfos: []*framework.NodeInfo{
				nodeWithScheduledCxPod,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHadRunningCustomerPod: true},
			},
		},
		{
			desc: "empty node gets a lookahead pod and a scheduled customer pod",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m}},
			nodeInfos: []*framework.NodeInfo{
				nodeWithLAScheduledCxPod,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true, everHadRunningCustomerPod: true},
			},
		},
		{
			desc: "empty node gets a simulated customer pod, which is not counted as running",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m}},
			nodeInfos: []*framework.NodeInfo{
				nodeWithNotScheduledCxPod,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m},
			},
		},
		{
			desc: "empty node gets a lookahead pod and a simulated customer pod",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m},
			},
			nodeInfos: []*framework.NodeInfo{
				nodeWithLANotScheduledCxPod,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true},
			},
		},
		{
			desc: "empty node gets a daemonset pod, which is not counted as customer pod",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m},
			},
			nodeInfos: []*framework.NodeInfo{
				nodeWithDaemonSetPod,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m},
			},
		},
		{
			desc: "node that hosted a lookahead pod becomes empty, flag and workload IDs stay set",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true},
			},
			nodeInfos: []*framework.NodeInfo{
				emptyNode,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true},
			},
		},
		{
			desc: "node that hosted a lookahead pod gets a scheduled customer pod",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true},
			},
			nodeInfos: []*framework.NodeInfo{
				nodeWithScheduledCxPod,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true, everHadRunningCustomerPod: true},
			},
		},
		{
			desc: "node that had a running customer pod becomes empty, flag stays set",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHadRunningCustomerPod: true},
			},
			nodeInfos: []*framework.NodeInfo{
				emptyNode,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHadRunningCustomerPod: true},
			},
		},
		{
			desc: "node that had a running customer pod gets a lookahead pod",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHadRunningCustomerPod: true},
			},
			nodeInfos: []*framework.NodeInfo{
				nodeWithLA,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true, everHadRunningCustomerPod: true},
			},
		},
		{
			desc: "new empty node without creation timestamp is tracked with creationTime set to current time",
			nodeInfos: []*framework.NodeInfo{
				emptyNode,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: now, machineFamily: ekFamily, everHostedLookahead: false, everHadRunningCustomerPod: false},
			},
		},
		{
			desc: "new node with a lookahead pod without creation timestamp is tracked with creationTime set to current time",
			nodeInfos: []*framework.NodeInfo{
				nodeWithLA,
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: now, machineFamily: ekFamily, everHostedLookahead: true},
			},
		},
		{
			desc: "new node is tracked with its creation timestamp",
			nodeInfos: []*framework.NodeInfo{
				framework.NewTestNodeInfo(createdAt(withUID(ekvms_test.EkNode32(nodeName, 1000, 1000)), tMinus5m), laPod),
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, machineFamily: ekFamily, everHostedLookahead: true},
			},
		},
		{
			desc: "node removed from the snapshot is dropped",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true, everHadRunningCustomerPod: false},
			},
			nodeInfos:      []*framework.NodeInfo{},
			wantNodesAfter: map[types.UID]*nodeHistory{},
		},
		{
			desc:           "upcoming node is skipped",
			nodeInfos:      []*framework.NodeInfo{upcomingNodeWithLA},
			wantNodesAfter: map[types.UID]*nodeHistory{},
		},
		{
			desc:           "node without UID is skipped",
			nodeInfos:      []*framework.NodeInfo{noUIDNodeWithLA},
			wantNodesAfter: map[types.UID]*nodeHistory{},
		},
		{
			desc:           "node of a non-resizable machine family is skipped",
			nodeInfos:      []*framework.NodeInfo{nonResizableNodeWithLA},
			wantNodesAfter: map[types.UID]*nodeHistory{},
		},
		{
			desc:           "node without machine type label is skipped",
			nodeInfos:      []*framework.NodeInfo{noMachineTypeNodeWithLA},
			wantNodesAfter: map[types.UID]*nodeHistory{},
		},
		{
			desc: "tracked node is dropped if it is not resizable anymore",
			nodesBefore: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: tMinus5m, everHostedLookahead: true},
			},
			nodeInfos:      []*framework.NodeInfo{nonResizableNodeWithLA},
			wantNodesAfter: map[types.UID]*nodeHistory{},
		},
		{
			desc: "only resizable nodes are tracked from a mixed snapshot",
			nodeInfos: []*framework.NodeInfo{
				nodeWithLA,
				framework.NewTestNodeInfo(nonResizableNode("n2-node"), laPod),
			},
			wantNodesAfter: map[types.UID]*nodeHistory{
				nodeUID: {creationTime: now, machineFamily: ekFamily, everHostedLookahead: true},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tracker := NewChurnTracker(enabledChurnTrackerProvider(), testMcp, nil, fakeclock.NewFakePassiveClock(now))
			tracker.nodes = tc.nodesBefore

			tracker.ProcessNodeInfos(tc.nodeInfos)

			if diff := cmp.Diff(tc.wantNodesAfter, tracker.nodes, nodeHistoryCmpOpts...); diff != "" {
				t.Errorf("ProcessNodeInfos() nodes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCleanExpiredEvents(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	// Events before windowStart are expired, events at or after windowStart are kept.
	windowStart := now.Add(-defaultChurnWindow)
	windowStartMinus10m := windowStart.Add(-10 * time.Minute)
	windowStartMinus5m := windowStart.Add(-5 * time.Minute)
	windowStartPlus10m := windowStart.Add(10 * time.Minute)
	windowStartPlus20m := windowStart.Add(20 * time.Minute)

	ekDefaultWorkload := workloadKey{machineFamily: ekFamily, workloadID: ""}
	ekSepOne := workloadKey{machineFamily: ekFamily, workloadID: "sep-one"}
	ekSepTwo := workloadKey{machineFamily: ekFamily, workloadID: "sep-two"}
	e4aDefaultWorkload := workloadKey{machineFamily: e4aFamily, workloadID: ""}

	for _, tc := range []struct {
		desc       string
		events     churnEvents
		wantEvents churnEvents
	}{
		{
			desc:       "events obj is nil",
			events:     nil,
			wantEvents: nil,
		},
		{
			desc:       "empty events",
			events:     churnEvents{},
			wantEvents: churnEvents{},
		},
		{
			desc: "all events within the window are kept",
			events: churnEvents{
				ekDefaultWorkload: {windowStartPlus10m, windowStartPlus20m},
				ekSepOne:          {windowStart},
			},
			wantEvents: churnEvents{
				ekDefaultWorkload: {windowStartPlus10m, windowStartPlus20m},
				ekSepOne:          {windowStart},
			},
		},
		{
			desc: "expired events are removed",
			events: churnEvents{
				ekDefaultWorkload: {windowStartMinus10m, windowStartPlus10m, windowStartPlus20m},
				ekSepOne:          {windowStartMinus5m},
				ekSepTwo:          {windowStartPlus20m},
			},
			wantEvents: churnEvents{
				ekDefaultWorkload: {windowStartPlus10m, windowStartPlus20m},
				ekSepTwo:          {windowStartPlus20m},
			},
		},
		{
			desc: "events before the window start are removed, the event at the window start and later are kept",
			events: churnEvents{
				ekDefaultWorkload: {windowStartMinus10m, windowStartMinus5m, windowStart, windowStartPlus20m},
			},
			wantEvents: churnEvents{
				ekDefaultWorkload: {windowStart, windowStartPlus20m},
			},
		},
		{
			desc: "machine family without valid events is removed, other machine families are kept",
			events: churnEvents{
				ekDefaultWorkload:  {windowStartMinus10m},
				e4aDefaultWorkload: {windowStartPlus10m},
			},
			wantEvents: churnEvents{
				e4aDefaultWorkload: {windowStartPlus10m},
			},
		},
		{
			desc: "all events expired resets map",
			events: churnEvents{
				ekDefaultWorkload:  {windowStartMinus10m, windowStartMinus5m},
				ekSepOne:           {windowStartMinus5m},
				e4aDefaultWorkload: {windowStartMinus5m},
			},
			wantEvents: churnEvents{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tracker := NewChurnTracker(enabledChurnTrackerProvider(), testMcp, nil, fakeclock.NewFakePassiveClock(now))
			tracker.events = tc.events

			tracker.cleanExpiredEvents()

			if diff := cmp.Diff(tc.wantEvents, tracker.events); diff != "" {
				t.Errorf("cleanExpiredEvents() events mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestIndexOfFirstEventAfterTs(t *testing.T) {
	// windowStart is the timestamp the events are compared with.
	windowStart := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	windowStartMinus10m := windowStart.Add(-10 * time.Minute)
	windowStartPlus10m := windowStart.Add(10 * time.Minute)
	windowStartPlus20m := windowStart.Add(20 * time.Minute)
	windowStartPlus30m := windowStart.Add(30 * time.Minute)

	for _, tc := range []struct {
		desc       string
		timestamps []time.Time
		want       int
	}{
		{
			desc:       "nil timestamps",
			timestamps: nil,
			want:       0,
		},
		{
			desc:       "empty timestamps",
			timestamps: []time.Time{},
			want:       0,
		},
		{
			desc:       "all timestamps are before windowStart",
			timestamps: []time.Time{windowStartMinus10m, windowStartMinus10m},
			want:       2,
		},
		{
			desc:       "all timestamps are after windowStart",
			timestamps: []time.Time{windowStartPlus10m, windowStartPlus20m},
			want:       0,
		},
		{
			desc:       "first timestamp after windowStart in the middle",
			timestamps: []time.Time{windowStartMinus10m, windowStartPlus10m, windowStartPlus20m},
			want:       1,
		},
		{
			desc:       "timestamp equal to windowStart is not stale",
			timestamps: []time.Time{windowStartMinus10m, windowStart, windowStartPlus20m},
			want:       1,
		},
		{
			desc:       "only the last timestamp is after windowStart",
			timestamps: []time.Time{windowStartMinus10m, windowStartMinus10m, windowStartPlus30m},
			want:       2,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if got := indexOfFirstEventAfterTs(tc.timestamps, windowStart); got != tc.want {
				t.Errorf("indexOfFirstEventAfterTs() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRecordScaledDownNodes(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	// The default churnLifetimeThreshold is 5m and churnWindow is 30m.
	tMinus1m := now.Add(-1 * time.Minute)
	tMinus2m := now.Add(-2 * time.Minute)
	tMinus20m := now.Add(-20 * time.Minute)

	// Nodes of different workload separations: node1 of the default one, node2 and node3 of testWorkloadID.
	node1 := ekNodeInfo("node-1").Node()
	node2 := ekNodeInfo("node-2").Node()
	node3 := ekNodeInfo("node-3").Node()

	ekDefaultWorkload := workloadKey{machineFamily: ekFamily, workloadID: ""}
	ekTestWorkload := workloadKey{machineFamily: ekFamily, workloadID: testWorkloadID}
	e4aTestWorkload := workloadKey{machineFamily: e4aFamily, workloadID: testWorkloadID}

	for _, tc := range []struct {
		desc            string
		nodesBefore     map[types.UID]*nodeHistory
		eventsBefore    churnEvents
		scaledDownNodes []*apiv1.Node
		wantEvents      churnEvents
		// wantMetric is the number of churn events registered in the metric per machine family.
		wantMetric map[string]int
	}{
		{
			desc:            "no node history, scaled down nodes are not recorded",
			nodesBefore:     map[types.UID]*nodeHistory{},
			scaledDownNodes: []*apiv1.Node{node1, node2},
			wantEvents:      churnEvents{},
		},
		{
			desc: "scaled down node without history is not recorded",
			nodesBefore: map[types.UID]*nodeHistory{
				node1.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true},
			},
			scaledDownNodes: []*apiv1.Node{node2},
			wantEvents:      churnEvents{},
		},
		{
			desc: "scaled down node that never hosted lookahead pods is not recorded",
			nodesBefore: map[types.UID]*nodeHistory{
				node1.UID: {creationTime: tMinus2m, machineFamily: ekFamily},
			},
			scaledDownNodes: []*apiv1.Node{node1},
			wantEvents:      churnEvents{},
		},
		{
			desc: "scaled down node that hosted lookahead pods is recorded",
			nodesBefore: map[types.UID]*nodeHistory{
				node1.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: ""},
			},
			scaledDownNodes: []*apiv1.Node{node1},
			wantEvents: churnEvents{
				ekDefaultWorkload: {now},
			},
			wantMetric: map[string]int{ekFamily: 1},
		},
		{
			desc: "scaled down node that had a running customer pod is not recorded",
			nodesBefore: map[types.UID]*nodeHistory{
				node1.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: ""},
				node2.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true, everHadRunningCustomerPod: true, lookaheadWorkloadID: testWorkloadID},
			},
			scaledDownNodes: []*apiv1.Node{node1, node2},
			wantEvents: churnEvents{
				ekDefaultWorkload: {now},
			},
			wantMetric: map[string]int{ekFamily: 1},
		},
		{
			desc: "scaled down node created after churnLifetimeThreshold is not recorded",
			nodesBefore: map[types.UID]*nodeHistory{
				node1.UID: {creationTime: tMinus20m, machineFamily: ekFamily, everHostedLookahead: true},
			},
			scaledDownNodes: []*apiv1.Node{node1},
			wantEvents:      churnEvents{},
		},
		{
			desc: "event of another workload ID is added next to existing events",
			nodesBefore: map[types.UID]*nodeHistory{
				node2.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			eventsBefore: churnEvents{
				ekDefaultWorkload: {tMinus1m},
			},
			scaledDownNodes: []*apiv1.Node{node2},
			wantEvents: churnEvents{
				ekDefaultWorkload: {tMinus1m},
				ekTestWorkload:    {now},
			},
			wantMetric: map[string]int{ekFamily: 1},
		},
		{
			desc: "event is appended to existing events of the same workload ID",
			nodesBefore: map[types.UID]*nodeHistory{
				node3.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			// Events of node1 and node2 recorded previously.
			eventsBefore: churnEvents{
				ekDefaultWorkload: {tMinus1m},
				ekTestWorkload:    {tMinus1m},
			},
			scaledDownNodes: []*apiv1.Node{node3},
			wantEvents: churnEvents{
				ekDefaultWorkload: {tMinus1m},
				ekTestWorkload:    {tMinus1m, now},
			},
			wantMetric: map[string]int{ekFamily: 1},
		},
		{
			desc: "events of multiple nodes are recorded per workload ID",
			nodesBefore: map[types.UID]*nodeHistory{
				node1.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: ""},
				node2.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
				node3.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			scaledDownNodes: []*apiv1.Node{node1, node2, node3},
			wantEvents: churnEvents{
				ekDefaultWorkload: {now},
				ekTestWorkload:    {now, now},
			},
			wantMetric: map[string]int{ekFamily: 3},
		},
		{
			desc: "events of the same workload ID are recorded per machine family",
			nodesBefore: map[types.UID]*nodeHistory{
				node1.UID: {creationTime: tMinus2m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
				node2.UID: {creationTime: tMinus2m, machineFamily: e4aFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			eventsBefore: churnEvents{
				ekTestWorkload: {tMinus1m},
			},
			scaledDownNodes: []*apiv1.Node{node1, node2},
			wantEvents: churnEvents{
				ekTestWorkload:  {tMinus1m, now},
				e4aTestWorkload: {now},
			},
			wantMetric: map[string]int{ekFamily: 1, e4aFamily: 1},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			metrics := &fakeChurnMetrics{}
			tracker := NewChurnTracker(enabledChurnTrackerProvider(), testMcp, metrics, fakeclock.NewFakePassiveClock(now))
			tracker.nodes = tc.nodesBefore
			if tc.eventsBefore != nil {
				tracker.events = tc.eventsBefore
			}

			tracker.RecordScaledDownNodes(tc.scaledDownNodes)

			if diff := cmp.Diff(tc.wantEvents, tracker.events, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("RecordScaledDownNodes() events mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantMetric, metrics.counts, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("RecordScaledDownNodes() churn events metric mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRecordScaledDownNodesUpdatesBackoffs(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	// The default churnThreshold is 5 and the first backoff duration is 15m.
	// Scaled down nodes created after windowStart are churn events, nodes created before it are not.
	windowStart := now.Add(-defaultChurnLifetimeThreshold)
	windowStartPlus3m := windowStart.Add(3 * time.Minute)
	windowStartMinus15m := windowStart.Add(-15 * time.Minute)

	// scaledDownNodes returns count scaled down EK nodes of testWorkloadID created at creationTime and their history.
	scaledDownNodes := func(count int, creationTime time.Time) ([]*apiv1.Node, map[types.UID]*nodeHistory) {
		nodes := make([]*apiv1.Node, 0, count)
		history := make(map[types.UID]*nodeHistory, count)
		for i := range count {
			node := ekNodeInfo(fmt.Sprintf("node-%d", i)).Node()
			nodes = append(nodes, node)
			history[node.UID] = &nodeHistory{creationTime: creationTime, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID}
		}
		return nodes, history
	}

	for _, tc := range []struct {
		desc             string
		nodesCount       int
		nodeCreationTime time.Time
		wantBackedOff    bool
		wantBackoffsLen  int
	}{
		{
			desc:             "churn events below churnThreshold do not back off the workload separation",
			nodesCount:       4,
			nodeCreationTime: windowStartPlus3m,
		},
		{
			desc:             "churn events reaching churnThreshold back off the workload separation",
			nodesCount:       5,
			nodeCreationTime: windowStartPlus3m,
			wantBackedOff:    true,
			wantBackoffsLen:  1,
		},
		{
			desc:             "scaled down nodes created before windowStart are not churn events and do not update the backoffs",
			nodesCount:       5,
			nodeCreationTime: windowStartMinus15m,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tracker := NewChurnTracker(enabledChurnTrackerProvider(), testMcp, nil, fakeclock.NewFakePassiveClock(now))
			nodes, history := scaledDownNodes(tc.nodesCount, tc.nodeCreationTime)
			tracker.nodes = history

			tracker.RecordScaledDownNodes(nodes)

			if got := tracker.backoffManager.IsBackedOff(ekFamily, testWorkloadID); got != tc.wantBackedOff {
				t.Errorf("IsBackedOff(%q, %q) = %v, want %v", ekFamily, testWorkloadID, got, tc.wantBackedOff)
			}
			// Other workload separations are never backed off.
			if tracker.backoffManager.IsBackedOff(ekFamily, "") || tracker.backoffManager.IsBackedOff(e4aFamily, testWorkloadID) {
				t.Errorf("IsBackedOff() = true for a workload separation without churn events, want false")
			}
			if got := len(tracker.backoffManager.backoffs); got != tc.wantBackoffsLen {
				t.Errorf("number of backoffs = %d, want %d", got, tc.wantBackoffsLen)
			}
		})
	}
}

func TestChurnTrackerIsBackedOff(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	nowMinus5m := now.Add(-5 * time.Minute)
	nowPlus15m := now.Add(15 * time.Minute)

	tracker := NewChurnTracker(enabledChurnTrackerProvider(), testMcp, nil, fakeclock.NewFakePassiveClock(now))
	tracker.backoffManager.backoffs = backoffEvents{
		{machineFamily: ekFamily, workloadID: testWorkloadID}: {tier: 1, endTime: nowPlus15m},
		{machineFamily: e4aFamily, workloadID: ""}:            {tier: 1, endTime: nowMinus5m},
	}

	for _, tc := range []struct {
		desc          string
		machineFamily string
		workloadID    string
		want          bool
	}{
		{desc: "active backoff", machineFamily: ekFamily, workloadID: testWorkloadID, want: true},
		{desc: "ended backoff", machineFamily: e4aFamily, workloadID: "", want: false},
		{desc: "workload separation without backoff", machineFamily: ekFamily, workloadID: "", want: false},
		{desc: "same workload ID of another machine family", machineFamily: e4aFamily, workloadID: testWorkloadID, want: false},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if got := tracker.IsBackedOff(tc.machineFamily, tc.workloadID); got != tc.want {
				t.Errorf("IsBackedOff(%q, %q) = %v, want %v", tc.machineFamily, tc.workloadID, got, tc.want)
			}
		})
	}
}

func TestChurnTrackerDisabled(t *testing.T) {
	nodeInfo := ekNodeInfo("node", lookaheadPod("la-pod", 100, 100))
	for _, tc := range []struct {
		desc           string
		configProvider ConfigProvider
	}{
		{desc: "experiment disabled", configProvider: disabledChurnTrackerProvider()},
		{desc: "nil provider", configProvider: nil},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			metrics := &fakeChurnMetrics{}
			tracker := NewChurnTracker(tc.configProvider, testMcp, metrics, nil)

			tracker.ProcessNodeInfos([]*framework.NodeInfo{nodeInfo})
			if len(tracker.nodes) != 0 {
				t.Errorf("ProcessNodeInfos() tracked %d nodes, want 0", len(tracker.nodes))
			}

			tracker.nodes = map[types.UID]*nodeHistory{nodeInfo.Node().UID: {creationTime: time.Now(), everHostedLookahead: true}}
			tracker.RecordScaledDownNodes([]*apiv1.Node{nodeInfo.Node()})
			if len(tracker.events) != 0 {
				t.Errorf("RecordScaledDownNodes() recorded events %v, want none", tracker.events)
			}
			if len(metrics.counts) != 0 {
				t.Errorf("churn events metric reported %v, want nothing", metrics.counts)
			}
		})
	}
}

func TestRecordScaledDownNodesNilMetrics(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	node := ekNodeInfo("node").Node()
	tracker := NewChurnTracker(enabledChurnTrackerProvider(), testMcp, nil, fakeclock.NewFakePassiveClock(now))
	tracker.nodes = map[types.UID]*nodeHistory{
		node.UID: {creationTime: now, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: ""},
	}

	// Must not panic.
	tracker.RecordScaledDownNodes([]*apiv1.Node{node})

	if got := len(tracker.events[workloadKey{machineFamily: ekFamily, workloadID: ""}]); got != 1 {
		t.Errorf("RecordScaledDownNodes() recorded %d events, want 1", got)
	}
}

// fakeChurnMetrics counts the lookahead churn events registered per machine family.
type fakeChurnMetrics struct {
	counts map[string]int
}

func (f *fakeChurnMetrics) RegisterLookaheadChurnEvent(machineFamily string) {
	if f.counts == nil {
		f.counts = make(map[string]int)
	}
	f.counts[machineFamily]++
}

// bindTo marks the pod as bound to the node by kube-scheduler.
func bindTo(pod *apiv1.Pod, nodeName string) *apiv1.Pod {
	pod.Spec.NodeName = nodeName
	return pod
}

// withUID sets the node UID to its name, since the churn tracker keys node history by UID.
func withUID(node *apiv1.Node) *apiv1.Node {
	node.UID = types.UID(node.Name)
	return node
}

// ekNodeInfo returns a NodeInfo of an EK node with UID set to its name and the given pods.
func ekNodeInfo(name string, pods ...*apiv1.Pod) *framework.NodeInfo {
	return framework.NewTestNodeInfo(withUID(ekvms_test.EkNode32(name, 1000, 1000)), pods...)
}

// e4aNodeInfo returns a NodeInfo of an E4A node with UID set to its name and the given pods.
func e4aNodeInfo(name string, pods ...*apiv1.Pod) *framework.NodeInfo {
	return framework.NewTestNodeInfo(withUID(ekvms_test.E4aNode32(name, 1000, 1000)), pods...)
}

// createdAt sets the node creation timestamp.
func createdAt(node *apiv1.Node, creationTime time.Time) *apiv1.Node {
	node.CreationTimestamp = metav1.NewTime(creationTime)
	return node
}

// enabledChurnTrackerProvider returns a churn tracker configProvider with the experiment enabled and default values.
func enabledChurnTrackerProvider() ConfigProvider {
	flags := map[string]string{experiments.ResizableVmChurnCircuitBreakerFlag: `{"minCaVersion": "0.0.0"}`}
	return NewConfigProvider(experiments.NewMockManagerWithOptions(version.Version{}, nil, flags), "", version.Version{}, nil)
}

// disabledChurnTrackerProvider returns a churn tracker configProvider with the experiment disabled.
func disabledChurnTrackerProvider() ConfigProvider {
	return NewConfigProvider(experiments.NewMockManagerWithOptions(version.Version{}, nil, nil), "", version.Version{}, nil)
}

func userPod(name string, cpu, mem int64) *apiv1.Pod {
	return test.BuildTestPod(name, cpu, mem)
}

func daemonsetPod(name string, cpu, mem int64) *apiv1.Pod {
	return test.BuildTestPod(name, cpu, mem, test.WithDSController())
}

func systemPod(name string, cpu, mem int64) *apiv1.Pod {
	return test.BuildTestPod(name, cpu, mem, func(p *apiv1.Pod) { p.Namespace = metav1.NamespaceSystem })
}

func lookaheadPod(name string, cpu, mem int64) *apiv1.Pod {
	return lookaheadbuffer.BuildTestLookaheadPod("", cpu, mem, machinetypes.EK.Name(), lookaheadbuffer.WithName(name))
}

func upcomingNode(name string, cpu, mem int64) *apiv1.Node {
	node := ekvms_test.EkNode32(name, cpu, mem)
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[annotations.NodeUpcomingAnnotation] = "true"
	return node
}

// separatedEkNode returns an EK node of the testWorkloadID workload separation (matching label and taint) with UID set to its name.
func separatedEkNode(name string) *apiv1.Node {
	node := ekvms_test.NewNodeBuilderFromNode(ekvms_test.EkNode32(name, 1000, 1000)).
		WithLabel("group", "one").
		WithTaint(apiv1.Taint{Key: "group", Value: "one", Effect: apiv1.TaintEffectNoSchedule}).
		Build()
	return withUID(node)
}

// nonResizableNode returns a node of the non-resizable n2 machine family with UID set to its name.
func nonResizableNode(name string) *apiv1.Node {
	node := ekvms_test.NewNodeBuilderFromNode(test.BuildTestNode(name, 1000, 1000)).
		WithSupportedMachineType("n2-standard-32").
		WithMachineFamily("n2").
		Build()
	return withUID(node)
}
