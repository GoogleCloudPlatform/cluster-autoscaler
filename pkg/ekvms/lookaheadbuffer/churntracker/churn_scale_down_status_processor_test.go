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
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	fakeclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaledown/status"
)

func TestChurnScaleDownStatusProcessor(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	tMinus1m := now.Add(-time.Minute)
	tMinus1h := now.Add(-time.Hour)

	ekNode1 := ekNodeInfo("ek-node-1").Node()
	ekNode2 := ekNodeInfo("ek-node-2").Node()
	e4aNode := e4aNodeInfo("e4a-node").Node()

	ekDefaultWorkload := workloadKey{machineFamily: ekFamily, workloadID: ""}
	ekTestWorkload := workloadKey{machineFamily: ekFamily, workloadID: testWorkloadID}
	e4aTestWorkload := workloadKey{machineFamily: e4aFamily, workloadID: testWorkloadID}

	for _, tc := range []struct {
		desc            string
		nodesBefore     map[types.UID]*nodeHistory
		scaleDownStatus *status.ScaleDownStatus
		wantEvents      churnEvents
	}{
		{
			desc: "nil status",
			nodesBefore: map[types.UID]*nodeHistory{
				ekNode1.UID: {creationTime: tMinus1m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			scaleDownStatus: nil,
			wantEvents:      churnEvents{},
		},
		{
			desc: "no scaled down nodes",
			nodesBefore: map[types.UID]*nodeHistory{
				ekNode1.UID: {creationTime: tMinus1m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			scaleDownStatus: &status.ScaleDownStatus{Result: status.ScaleDownNoCandidates},
			wantEvents:      churnEvents{},
		},
		{
			desc: "churny scaled down node is recorded, nil entries are skipped",
			nodesBefore: map[types.UID]*nodeHistory{
				ekNode1.UID: {creationTime: tMinus1m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			scaleDownStatus: &status.ScaleDownStatus{
				Result:          status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{{Node: ekNode1}, nil},
			},
			wantEvents: churnEvents{
				ekTestWorkload: {now},
			},
		},
		{
			desc: "scaled down node created after churnLifetimeThreshold is not recorded",
			nodesBefore: map[types.UID]*nodeHistory{
				ekNode1.UID: {creationTime: tMinus1h, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			scaleDownStatus: &status.ScaleDownStatus{
				Result:          status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{{Node: ekNode1}},
			},
			wantEvents: churnEvents{},
		},
		{
			desc: "only churny nodes are recorded - exclude ek node2 and e4a node",
			nodesBefore: map[types.UID]*nodeHistory{
				ekNode1.UID: {creationTime: tMinus1m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
				ekNode2.UID: {creationTime: tMinus1m, machineFamily: ekFamily, everHostedLookahead: true, everHadRunningCustomerPod: true, lookaheadWorkloadID: testWorkloadID},
				e4aNode.UID: {creationTime: tMinus1h, machineFamily: e4aFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			scaleDownStatus: &status.ScaleDownStatus{
				Result:          status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{{Node: ekNode1}, {Node: ekNode2}, {Node: e4aNode}},
			},
			wantEvents: churnEvents{
				ekTestWorkload: {now},
			},
		},
		{
			desc: "nodes of different machine families are recorded per machine family",
			nodesBefore: map[types.UID]*nodeHistory{
				ekNode1.UID: {creationTime: tMinus1m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: ""},
				e4aNode.UID: {creationTime: tMinus1m, machineFamily: e4aFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			scaleDownStatus: &status.ScaleDownStatus{
				Result:          status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{{Node: ekNode1}, {Node: e4aNode}},
			},
			wantEvents: churnEvents{
				ekDefaultWorkload: {now},
				e4aTestWorkload:   {now},
			},
		},
		{
			desc: "nodes of the same workload ID in different machine families are recorded separately",
			nodesBefore: map[types.UID]*nodeHistory{
				ekNode1.UID: {creationTime: tMinus1m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
				ekNode2.UID: {creationTime: tMinus1m, machineFamily: ekFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
				e4aNode.UID: {creationTime: tMinus1m, machineFamily: e4aFamily, everHostedLookahead: true, lookaheadWorkloadID: testWorkloadID},
			},
			scaleDownStatus: &status.ScaleDownStatus{
				Result:          status.ScaleDownNodeDeleteStarted,
				ScaledDownNodes: []*status.ScaleDownNode{{Node: ekNode1}, {Node: ekNode2}, {Node: e4aNode}},
			},
			wantEvents: churnEvents{
				ekTestWorkload:  {now, now},
				e4aTestWorkload: {now},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tracker := NewChurnTracker(enabledChurnTrackerProvider(), testMcp, nil, fakeclock.NewFakePassiveClock(now))
			tracker.nodes = tc.nodesBefore
			p := NewChurnScaleDownStatusProcessor(tracker)

			p.Process(context.Background(), nil, tc.scaleDownStatus)

			if diff := cmp.Diff(tc.wantEvents, tracker.events, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Process() events mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestChurnScaleDownStatusProcessorNilTracker(t *testing.T) {
	p := NewChurnScaleDownStatusProcessor(nil)
	// Must not panic.
	p.Process(context.Background(), nil, &status.ScaleDownStatus{ScaledDownNodes: []*status.ScaleDownNode{{Node: &apiv1.Node{}}}})
}
