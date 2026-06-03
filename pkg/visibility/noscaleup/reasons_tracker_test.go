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

package noscaleup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	vistypes "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

func TestMarkingAndFilteringEveryReason(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	tracker := newReasonsReportTracker(time.Minute)

	pod := &vistypes.Pod{Name: "pod1", Uid: "pid1"}
	reason1 := &vistypes.Message{Id: 0, Params: []string{"a"}}
	reason2 := &vistypes.Message{Id: 0, Params: []string{"b"}}
	migReason1 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
		Reason: &vistypes.Message{Id: 0, Params: []string{"a", "a"}},
	}
	migReason2 := &vistypes.MigExplanation{
		Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
		Reason: &vistypes.Message{Id: 0, Params: []string{"a", "b"}},
	}
	podGroup1 := &vistypes.PodGroupExplanation{
		SamplePod:  pod,
		PodCount:   1337,
		MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason1},
		NapReasons: []*vistypes.Message{reason1},
	}
	podGroup2 := &vistypes.PodGroupExplanation{
		SamplePod:  pod,
		PodCount:   1337,
		MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason2},
		NapReasons: []*vistypes.Message{reason2},
	}

	allReasons1 := &Reasons{
		TopLevel:    reason1,
		TopLevelNap: reason1,
		SkippedMigs: []*vistypes.MigExplanation{migReason1},
		PodGroups:   []*vistypes.PodGroupExplanation{podGroup1},
	}
	allReasons2 := &Reasons{
		TopLevel:    reason2,
		TopLevelNap: reason2,
		SkippedMigs: []*vistypes.MigExplanation{migReason2},
		PodGroups:   []*vistypes.PodGroupExplanation{podGroup2},
	}

	// Nothing has been reported yet, so nothing should be filtered out.
	filteredReasons := tracker.filterOutAlreadyTrackedReasons(allReasons1, now)
	assert.Equal(t, allReasons1, filteredReasons)

	// A top level reason is reported. It should be filtered out.
	tracker.markReported(&Reasons{TopLevel: reason1}, now.Add(10*time.Second))
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(11*time.Second))
	assert.Nil(t, filteredReasons.TopLevel)
	assert.Equal(t, allReasons1.TopLevelNap, filteredReasons.TopLevelNap)
	assert.Equal(t, allReasons1.SkippedMigs, filteredReasons.SkippedMigs)
	assert.Equal(t, allReasons1.PodGroups, filteredReasons.PodGroups)

	// A top level NAP reason is reported. Only 10 seconds passed since the first top level reason was reported, so they both should be filtered out.
	tracker.markReported(&Reasons{TopLevelNap: reason1}, now.Add(20*time.Second))
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(21*time.Second))
	assert.Nil(t, filteredReasons.TopLevel)
	assert.Nil(t, filteredReasons.TopLevelNap)
	assert.Equal(t, allReasons1.SkippedMigs, filteredReasons.SkippedMigs)
	assert.Equal(t, allReasons1.PodGroups, filteredReasons.PodGroups)

	// Skipped MIGs are reported. Only 20 seconds passed since the first top level reason was reported (and 10 since the NAP top level reason),
	// so all 3 should be filtered out.
	tracker.markReported(&Reasons{SkippedMigs: []*vistypes.MigExplanation{migReason1}}, now.Add(30*time.Second))
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(31*time.Second))
	assert.Nil(t, filteredReasons.TopLevel)
	assert.Nil(t, filteredReasons.TopLevelNap)
	assert.Nil(t, filteredReasons.SkippedMigs)
	assert.Equal(t, allReasons1.PodGroups, filteredReasons.PodGroups)

	// Pod groups are reported. Only 30 seconds passed since the first top level reason was reported (and 20 since the NAP top level reason,
	// 10 seconds since the skipped MIGs), so all 4 should be filtered out.
	tracker.markReported(&Reasons{PodGroups: []*vistypes.PodGroupExplanation{podGroup1}}, now.Add(40*time.Second))
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(41*time.Second))
	assert.True(t, filteredReasons.IsEmpty())

	// A set of 4 completely different reasons appeared, so nothing should be filtered.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons2, now.Add(42*time.Second))
	assert.Equal(t, allReasons2, filteredReasons)

	// The 4 new reasons are now all reported, so they should all be filtered out.
	tracker.markReported(allReasons2, now.Add(43*time.Second))
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons2, now.Add(44*time.Second))
	assert.True(t, filteredReasons.IsEmpty())

	// The first reported reason passed the staleness threshold, so it should no longer be filtered out.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(71*time.Second))
	assert.Equal(t, allReasons1.TopLevel, filteredReasons.TopLevel)
	assert.Nil(t, filteredReasons.TopLevelNap)
	assert.Nil(t, filteredReasons.SkippedMigs)
	assert.Nil(t, filteredReasons.PodGroups)

	// The second reported reason passed the staleness threshold too, so it should no longer be filtered out.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(81*time.Second))
	assert.Equal(t, allReasons1.TopLevel, filteredReasons.TopLevel)
	assert.Equal(t, allReasons1.TopLevelNap, filteredReasons.TopLevelNap)
	assert.Nil(t, filteredReasons.SkippedMigs)
	assert.Nil(t, filteredReasons.PodGroups)

	// The third reported reason passed the staleness threshold too, so it should no longer be filtered out.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(91*time.Second))
	assert.Equal(t, allReasons1.TopLevel, filteredReasons.TopLevel)
	assert.Equal(t, allReasons1.TopLevelNap, filteredReasons.TopLevelNap)
	assert.Equal(t, allReasons1.SkippedMigs, filteredReasons.SkippedMigs)
	assert.Nil(t, filteredReasons.PodGroups)

	// All initial reported reason passed the staleness threshold too, so none should be filtered out.
	filteredReasons = tracker.filterOutAlreadyTrackedReasons(allReasons1, now.Add(101*time.Second))
	assert.Equal(t, allReasons1, filteredReasons)

}

func TestMarkReportedOverwrite(t *testing.T) {
	now := time.Date(2000, 1, 1, 10, 10, 10, 0, time.UTC)
	tracker := newReasonsReportTracker(time.Minute)

	reasons1 := &Reasons{TopLevel: &vistypes.Message{Id: 0, Params: []string{"a"}}}
	reasons2 := &Reasons{
		TopLevel:    &vistypes.Message{Id: 0, Params: []string{"a"}},
		TopLevelNap: &vistypes.Message{Id: 1, Params: []string{"a"}},
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

	// Create 4 list of the 4 types of reasons, each containing singleReasonCount distinct reasons.
	topLevels := make([]*vistypes.Message, 0, singleReasonCount)
	for i := 0; i < singleReasonCount; i++ {
		topLevels = append(topLevels, &vistypes.Message{Id: vistypes.MessageId(i), Params: []string{"a"}})
	}

	topLevelNaps := make([]*vistypes.Message, 0, singleReasonCount)
	for i := 0; i < singleReasonCount; i++ {
		topLevelNaps = append(topLevelNaps, &vistypes.Message{Id: vistypes.MessageId(i), Params: []string{"a"}})
	}

	skippedMigsList := make([][]*vistypes.MigExplanation, 0, singleReasonCount)
	for i := 0; i < singleReasonCount; i++ {
		skippedMigsList = append(skippedMigsList, []*vistypes.MigExplanation{
			{
				Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
				Reason: &vistypes.Message{Id: vistypes.MessageId(i), Params: []string{"a", "a"}},
			},
		})
	}

	podGroupsList := make([][]*vistypes.PodGroupExplanation, 0, singleReasonCount)
	for i := 0; i < singleReasonCount; i++ {
		pod := &vistypes.Pod{Name: "pod1", Uid: "pid1"}
		reason := &vistypes.Message{Id: vistypes.MessageId(i), Params: []string{"a"}}
		migReason1337 := &vistypes.MigExplanation{
			Mig:    &vistypes.GkeMig{Id: "mig1", Name: "mig1", NodePoolName: "np1", Zone: "z1"},
			Reason: &vistypes.Message{Id: 1337, Params: []string{"a", "a"}},
		}
		podGroupsList = append(podGroupsList, []*vistypes.PodGroupExplanation{
			{
				SamplePod:  pod,
				PodCount:   1337,
				MigReasons: map[string]*vistypes.MigExplanation{"mig1": migReason1337},
				NapReasons: []*vistypes.Message{reason},
			},
		})
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
			TopLevel:    topLevels[i%singleReasonCount],
			TopLevelNap: topLevelNaps[i%singleReasonCount],
			SkippedMigs: skippedMigsList[i%singleReasonCount],
			PodGroups:   podGroupsList[i%singleReasonCount],
		}
		filteredReasons := tracker.filterOutAlreadyTrackedReasons(reasons, now.Add(time.Second*time.Duration(i)))
		if !filteredReasons.IsEmpty() {
			tracker.markReported(filteredReasons, now.Add(time.Second*time.Duration(i)))
			timesReported += 1
		}
	}

	assert.Equal(t, singleReasonCount*periodsToSimulate, timesReported)
}
