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

package noscaledown

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

func TestMarkingAndFilteringEveryReason(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	tracker := newReasonsReportTracker(time.Minute)

	reason1 := &vistypes.Message{Id: 0, Params: []string{"a"}}
	reason2 := &vistypes.Message{Id: 0, Params: []string{"b"}}

	node := &vistypes.Node{Name: "node-1"}
	nodeReason1 := &vistypes.NodeExplanation{Node: node, Reason: &vistypes.Message{Id: 0}}
	nodeReason2 := &vistypes.NodeExplanation{Node: node, Reason: &vistypes.Message{Id: 0, Params: []string{"a"}}}

	allReasons1 := &Reasons{
		TopLevel:         reason1,
		UnremovableNodes: []*vistypes.NodeExplanation{nodeReason1},
	}
	allReasons2 := &Reasons{
		TopLevel:         reason2,
		UnremovableNodes: []*vistypes.NodeExplanation{nodeReason2},
	}

	// Nothing has been reported yet, so nothing should be filtered out.
	filteredReasons := tracker.filterOutAlreadyTrackedReasons(allReasons1, now)
	assert.Equal(t, allReasons1, filteredReasons)

	// A top level reason is reported. It should be filtered out.
	tracker.markReported(&Reasons{TopLevel: reason1}, now.Add(10*time.Second))
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(11*time.Second))
	assert.Nil(t, filteredReasons.TopLevel)
	assert.Equal(t, allReasons1.UnremovableNodes, filteredReasons.UnremovableNodes)

	// Unremovable nodes are reported. Only 10 seconds passed since the first top level reason was reported, so they both should be filtered out.
	tracker.markReported(&Reasons{UnremovableNodes: []*vistypes.NodeExplanation{nodeReason1}}, now.Add(20*time.Second))
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(21*time.Second))
	assert.Nil(t, filteredReasons.TopLevel)
	assert.Nil(t, filteredReasons.UnremovableNodes)

	// A set of 2 completely different reasons appeared, so nothing should be filtered.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons2, now.Add(22*time.Second))
	assert.Equal(t, allReasons2, filteredReasons)

	// The 2 new reasons are now all reported, so they should all be filtered out.
	tracker.markReported(allReasons2, now.Add(23*time.Second))
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons2, now.Add(24*time.Second))
	assert.True(t, filteredReasons.IsEmpty())

	// The first reported reason passed the staleness threshold, so it should no longer be filtered out.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(71*time.Second))
	assert.Equal(t, allReasons1.TopLevel, filteredReasons.TopLevel)
	assert.Nil(t, filteredReasons.UnremovableNodes)

	// The second reported reason passed the staleness threshold too, so it should no longer be filtered out.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(81*time.Second))
	assert.Equal(t, allReasons1.TopLevel, filteredReasons.TopLevel)
	assert.Equal(t, allReasons1.UnremovableNodes, filteredReasons.UnremovableNodes)
}

func TestMarkReportedOverwrite(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	tracker := newReasonsReportTracker(time.Minute)

	reasons1 := &Reasons{
		TopLevel: &vistypes.Message{Id: 0, Params: []string{"a"}},
	}
	reasons2 := &Reasons{
		TopLevel:         &vistypes.Message{Id: 0, Params: []string{"a"}},
		UnremovableNodes: []*vistypes.NodeExplanation{{Node: &vistypes.Node{Name: "node-1"}, Reason: &vistypes.Message{Id: 0}}},
	}

	// An initial set of reasons is marked as reported.
	tracker.markReported(reasons1, now)
	filteredReasons := tracker.filterOutAlreadyTrackedReasons(reasons1, now)
	assert.Nil(t, filteredReasons.TopLevel)

	// Another set of reasons marks the same top level reason as reported, so the timeout should be extended.
	tracker.markReported(reasons2, now.Add(30*time.Second))

	// Even though its past the first staleness threshold, the second markReported should've extended it and
	// the reason should be filtered.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(reasons1, now.Add(61*time.Second))
	assert.Nil(t, filteredReasons.TopLevel)

	// When the staleness threshold of the second report is passed, the reason is no longer filtered.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(reasons1, now.Add(91*time.Second))
	assert.Equal(t, reasons1.TopLevel, filteredReasons.TopLevel)
}

func TestRealReasonsScenario(t *testing.T) {
	stalenessThresholdSeconds := 60
	singleReasonCount := 5
	periodsToSimulate := 100

	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	tracker := newReasonsReportTracker(time.Duration(stalenessThresholdSeconds) * time.Second)

	// Create 2 list of the 2 types of reasons, each containing singleReasonCount distinct reasons.
	topLevels := make([]*vistypes.Message, 0, singleReasonCount)
	for i := 0; i < singleReasonCount; i++ {
		topLevels = append(topLevels, &vistypes.Message{Id: vistypes.MessageId(i), Params: []string{"a"}})
	}

	unremovableNodesList := make([][]*vistypes.NodeExplanation, 0, singleReasonCount)
	for i := 0; i < singleReasonCount; i++ {
		unremovableNodesList = append(unremovableNodesList, []*vistypes.NodeExplanation{{
			Node:   &vistypes.Node{Name: "node-1"},
			Reason: &vistypes.Message{Id: vistypes.MessageId(i), Params: []string{"a", "a"}},
		}})
	}

	// Each second, try filter out a set of reasons and if the result is not empty, mark them as reported.
	// This roughly corresponds to a situation when we have reasons changing over time and this tests makes
	// sure that this scenario doesn't produce too many reports.
	//
	// All sets of reasons should be reported each stalenessThresholdSeconds+singleReasonCount seconds:
	// stalenessThresholdSeconds of waiting + singleReasonCount seconds to report all sets. We want to test
	// this periodsToSimulate times and make sure that the number of reports is
	// correct: singleReasonCount*periodsToSimulate.
	timesReported := 0
	for i := 0; i < (stalenessThresholdSeconds+singleReasonCount)*periodsToSimulate; i++ {
		reasons := &Reasons{
			TopLevel:         topLevels[i%singleReasonCount],
			UnremovableNodes: unremovableNodesList[i%singleReasonCount],
		}
		filteredReasons := tracker.filterOutAlreadyTrackedReasons(reasons, now.Add(time.Second*time.Duration(i)))
		if !filteredReasons.IsEmpty() {
			tracker.markReported(filteredReasons, now.Add(time.Second*time.Duration(i)))
			timesReported += 1
		}
	}

	assert.Equal(t, singleReasonCount*periodsToSimulate, timesReported)
}
