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
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/apimachinery/pkg/util/sets"
	fakeclock "k8s.io/utils/clock/testing"
)

var backoffCmpOpts = []cmp.Option{cmp.AllowUnexported(backoffData{}), cmpopts.EquateEmpty()}

func TestBackoffManagerUpdateEvents(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	nowMinus61m := now.Add(-61 * time.Minute)
	nowMinus31m := now.Add(-31 * time.Minute)
	nowMinus20m := now.Add(-20 * time.Minute)
	nowMinus1m := now.Add(-1 * time.Minute)
	nowPlus15m := now.Add(15 * time.Minute)
	nowPlus1h := now.Add(time.Hour)

	ekTestWorkload := workloadKey{machineFamily: ekFamily, workloadID: testWorkloadID}

	// churnThreshold: 3, churnWindow: 30m, backoffDurations: [15m, 30m, 1h], resetWindow: 30m, resetMaxChurnEvents: 0.
	// The reset, creation and escalation rules are covered by TestBackoffManagerResetBackoffs and
	// TestBackoffManagerCreateOrUpdateEvents, this test covers their composition.
	for _, tc := range []struct {
		desc           string
		backoffsBefore backoffEvents
		events         churnEvents
		wantBackoffs   backoffEvents
		wantMetric     map[backoffTierKey]int
	}{
		{
			desc:         "churn events reaching churnThreshold start a tier 1 backoff",
			events:       churnEvents{ekTestWorkload: {nowMinus20m, nowMinus1m, now}},
			wantBackoffs: backoffEvents{ekTestWorkload: {tier: 1, endTime: nowPlus15m}},
			wantMetric:   map[backoffTierKey]int{{machineFamily: ekFamily, tier: 1}: 1},
		},
		{
			desc:           "backoff past resetWindow, which was quiet within resetWindow, is reset before the churn events are evaluated, so a tier 1 backoff is started",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 2, endTime: nowMinus61m}},
			events:         churnEvents{ekTestWorkload: {now, now, now}},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 1, endTime: nowPlus15m}},
			wantMetric:     map[backoffTierKey]int{{machineFamily: ekFamily, tier: 1}: 1},
		},
		{
			desc:           "backoff past resetWindow, which had churn events within resetWindow, is not reset and is escalated",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 2, endTime: nowMinus31m}},
			events:         churnEvents{ekTestWorkload: {nowMinus20m, nowMinus1m, now}},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 3, endTime: nowPlus1h}},
			wantMetric:     map[backoffTierKey]int{{machineFamily: ekFamily, tier: 3}: 1},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			metrics := &fakeChurnMetrics{}
			m := NewBackoffManager(fakeclock.NewFakePassiveClock(now), testBackoffProvider(), metrics)
			if tc.backoffsBefore != nil {
				m.backoffs = tc.backoffsBefore
			}

			m.UpdateEvents(tc.events, now)

			if diff := cmp.Diff(tc.wantBackoffs, m.backoffs, backoffCmpOpts...); diff != "" {
				t.Errorf("UpdateEvents() backoffs mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantMetric, metrics.backedOff, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("UpdateEvents() metric mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBackoffManagerBackoffDurationsChange(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	nowMinus31m := now.Add(-31 * time.Minute)
	nowMinus5m := now.Add(-5 * time.Minute)
	nowMinus1m := now.Add(-1 * time.Minute)
	nowPlus5m := now.Add(5 * time.Minute)
	nowPlus10m := now.Add(10 * time.Minute)
	nowPlus15m := now.Add(15 * time.Minute)

	ekTestWorkload := workloadKey{machineFamily: ekFamily, workloadID: testWorkloadID}

	// The backoffs before were created with the default durations of testBackoffProvider: [15m, 30m, 1h].
	// The experiment flag now provides shorter durations.
	// churnThreshold: 3, churnWindow: 30m, resetWindow: 30m, resetMaxChurnEvents: 0.
	shortDurations := []time.Duration{5 * time.Minute, 10 * time.Minute}

	for _, tc := range []struct {
		desc           string
		backoffsBefore backoffEvents
		events         churnEvents
		wantBackoffs   backoffEvents
		wantMetric     map[backoffTierKey]int
	}{
		{
			desc:           "existing backoffs are kept as is",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 1, endTime: nowPlus15m}},
			events:         churnEvents{},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 1, endTime: nowPlus15m}},
			wantMetric:     map[backoffTierKey]int{{machineFamily: ekFamily, tier: 1}: 1},
		},
		{
			desc:           "escalation uses the new durations",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 1, endTime: nowMinus5m}},
			events:         churnEvents{ekTestWorkload: {nowMinus1m, now, now}},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 2, endTime: nowPlus10m}},
			wantMetric:     map[backoffTierKey]int{{machineFamily: ekFamily, tier: 2}: 1},
		},
		{
			desc:           "escalation beyond the last of the new durations uses the new last tier",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 3, endTime: nowMinus5m}},
			events:         churnEvents{ekTestWorkload: {nowMinus1m, now, now}},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 2, endTime: nowPlus10m}},
			wantMetric:     map[backoffTierKey]int{{machineFamily: ekFamily, tier: 2}: 1},
		},
		{
			desc:           "backoff with a tier beyond the new durations is reset once past resetWindow without churn events",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 3, endTime: nowMinus31m}},
			events:         churnEvents{},
			wantBackoffs:   backoffEvents{},
		},
		{
			desc:           "backoff with a tier beyond the new durations is reset, then churn events start a new tier 1 backoff",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 3, endTime: nowMinus31m}},
			events:         churnEvents{ekTestWorkload: {now, now, now}},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 1, endTime: nowPlus5m}},
			wantMetric:     map[backoffTierKey]int{{machineFamily: ekFamily, tier: 1}: 1},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			provider := testBackoffProvider()
			provider.backoffDurations = shortDurations
			metrics := &fakeChurnMetrics{}
			m := NewBackoffManager(fakeclock.NewFakePassiveClock(now), provider, metrics)
			m.backoffs = tc.backoffsBefore

			m.UpdateEvents(tc.events, now)

			if diff := cmp.Diff(tc.wantBackoffs, m.backoffs, backoffCmpOpts...); diff != "" {
				t.Errorf("UpdateEvents() backoffs mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantMetric, metrics.backedOff, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("UpdateEvents() metric mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBackoffManagerIsBackedOff(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	nowMinus5m := now.Add(-5 * time.Minute)
	nowPlus10m := now.Add(10 * time.Minute)
	backoffs := backoffEvents{
		{machineFamily: ekFamily, workloadID: testWorkloadID}: {tier: 1, endTime: nowPlus10m}, // active backoff
		{machineFamily: ekFamily, workloadID: ""}:             {tier: 1, endTime: nowMinus5m}, // ended backoff
	}

	for _, tc := range []struct {
		desc          string
		provider      ConfigProvider
		machineFamily string
		workloadID    string
		want          bool
	}{
		{desc: "active backoff", provider: testBackoffProvider(), machineFamily: ekFamily, workloadID: testWorkloadID, want: true},
		{desc: "ended backoff within resetWindow", provider: testBackoffProvider(), machineFamily: ekFamily, workloadID: "", want: false},
		{desc: "same workload ID of another machine family", provider: testBackoffProvider(), machineFamily: e4aFamily, workloadID: testWorkloadID, want: false},
		{desc: "experiment disabled", provider: &fakeProvider{enabled: false}, machineFamily: ekFamily, workloadID: testWorkloadID, want: false},
		{desc: "nil provider", provider: nil, machineFamily: ekFamily, workloadID: testWorkloadID, want: false},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			metrics := &fakeChurnMetrics{}
			m := NewBackoffManager(fakeclock.NewFakePassiveClock(now), tc.provider, metrics)
			m.backoffs = backoffs

			if got := m.IsBackedOff(tc.machineFamily, tc.workloadID); got != tc.want {
				t.Errorf("IsBackedOff(%q, %q) = %v, want %v", tc.machineFamily, tc.workloadID, got, tc.want)
			}
			if len(metrics.backedOff) != 0 {
				t.Errorf("IsBackedOff(%q, %q) reported metric %v, want none", tc.machineFamily, tc.workloadID, metrics.backedOff)
			}
		})
	}
}

func TestBackoffManagerResetBackoffs(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	nowMinus31m := now.Add(-31 * time.Minute)
	nowMinus20m := now.Add(-20 * time.Minute)
	nowMinus5m := now.Add(-5 * time.Minute)
	nowMinus2m := now.Add(-2 * time.Minute)
	nowPlus10m := now.Add(10 * time.Minute)

	ekDefaultWorkload := workloadKey{machineFamily: ekFamily, workloadID: ""}
	ekTestWorkload := workloadKey{machineFamily: ekFamily, workloadID: testWorkloadID}
	e4aTestWorkload := workloadKey{machineFamily: e4aFamily, workloadID: testWorkloadID}
	ekTier1 := backoffTierKey{machineFamily: ekFamily, tier: 1}
	ekTier2 := backoffTierKey{machineFamily: ekFamily, tier: 2}
	e4aTier2 := backoffTierKey{machineFamily: e4aFamily, tier: 2}

	// resetWindow: 30m. A backoff that ended at nowMinus31m is past resetWindow, so it is reset if [nowMinus30m, now)
	// has at most resetMaxChurnEvents churn events. A backoff that ended at nowMinus5m is still within resetWindow.
	for _, tc := range []struct {
		desc                string
		resetMaxChurnEvents int
		backoffsBefore      backoffEvents
		// reportedTiersBefore are the tiers reported for backoffsBefore by the previous metric update.
		reportedTiersBefore sets.Set[backoffTierKey]
		events              churnEvents
		wantBackoffs        backoffEvents
		wantMetric          map[backoffTierKey]int
	}{
		{
			desc:                "ended backoff within resetWindow is kept, even without churn events",
			backoffsBefore:      backoffEvents{ekTestWorkload: {tier: 1, endTime: nowMinus5m}},
			reportedTiersBefore: sets.New(ekTier1),
			events:              churnEvents{},
			wantBackoffs:        backoffEvents{ekTestWorkload: {tier: 1, endTime: nowMinus5m}},
			// The backoff has ended, so it is reported as 0 although it is kept to remember its tier.
			wantMetric: map[backoffTierKey]int{ekTier1: 0},
		},
		{
			desc: "backoffs past resetWindow without churn events within resetWindow are reset per workload separation, active backoffs are kept",
			backoffsBefore: backoffEvents{
				ekDefaultWorkload: {tier: 1, endTime: nowMinus31m},
				ekTestWorkload:    {tier: 1, endTime: nowPlus10m},
				e4aTestWorkload:   {tier: 2, endTime: nowMinus31m},
			},
			reportedTiersBefore: sets.New(ekTier1, e4aTier2),
			// Churn events at now are not counted.
			events:       churnEvents{ekDefaultWorkload: {now}},
			wantBackoffs: backoffEvents{ekTestWorkload: {tier: 1, endTime: nowPlus10m}},
			// The reset e4a tier 2 backoff is reported as 0.
			wantMetric: map[backoffTierKey]int{ekTier1: 1, e4aTier2: 0},
		},
		{
			desc:                "backoff past resetWindow with up to resetMaxChurnEvents churn events within resetWindow is reset",
			resetMaxChurnEvents: 1,
			backoffsBefore:      backoffEvents{ekTestWorkload: {tier: 2, endTime: nowMinus31m}},
			reportedTiersBefore: sets.New(ekTier2),
			events:              churnEvents{ekTestWorkload: {nowMinus20m}},
			// resetMaxChurnEvents is overwritten to 1.
			wantBackoffs: backoffEvents{},
			wantMetric:   map[backoffTierKey]int{ekTier2: 0},
		},
		{
			desc:                "backoff past resetWindow with more than resetMaxChurnEvents churn events within resetWindow keeps its tier",
			resetMaxChurnEvents: 1,
			backoffsBefore:      backoffEvents{ekTestWorkload: {tier: 2, endTime: nowMinus31m}},
			reportedTiersBefore: sets.New(ekTier2),
			events:              churnEvents{ekTestWorkload: {nowMinus20m, nowMinus2m}},
			wantBackoffs:        backoffEvents{ekTestWorkload: {tier: 2, endTime: nowMinus31m}},
			// The kept backoff has ended, so it is reported as 0.
			wantMetric: map[backoffTierKey]int{ekTier2: 0},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			provider := testBackoffProvider()
			provider.resetMaxChurnEvents = tc.resetMaxChurnEvents
			metrics := &fakeChurnMetrics{}
			m := NewBackoffManager(fakeclock.NewFakePassiveClock(now), provider, metrics)
			m.backoffs = tc.backoffsBefore
			m.reportedTiers = tc.reportedTiersBefore

			m.ResetBackoffs(tc.events, now)

			if diff := cmp.Diff(tc.wantBackoffs, m.backoffs, backoffCmpOpts...); diff != "" {
				t.Errorf("ResetBackoffs() backoffs mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantMetric, metrics.backedOff, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ResetBackoffs() metric mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBackoffManagerCreateOrUpdateEvents(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	// churnWindow is 30m: churn events before windowStart are outdated.
	windowStartMinus1m := now.Add(-testBackoffProvider().churnWindow - time.Minute)
	nowMinus20m := now.Add(-20 * time.Minute)
	nowMinus5m := now.Add(-5 * time.Minute)
	nowMinus2m := now.Add(-2 * time.Minute)
	nowMinus1m := now.Add(-1 * time.Minute)
	nowPlus10m := now.Add(10 * time.Minute)
	nowPlus15m := now.Add(15 * time.Minute)
	nowPlus30m := now.Add(30 * time.Minute)
	nowPlus1h := now.Add(time.Hour)

	ekDefaultWorkload := workloadKey{machineFamily: ekFamily, workloadID: ""}
	ekTestWorkload := workloadKey{machineFamily: ekFamily, workloadID: testWorkloadID}
	e4aTestWorkload := workloadKey{machineFamily: e4aFamily, workloadID: testWorkloadID}

	// churnThreshold: 3, churnWindow: 30m, backoffDurations: [15m, 30m, 1h].
	for _, tc := range []struct {
		desc           string
		backoffsBefore backoffEvents
		events         churnEvents
		wantBackoffs   backoffEvents
	}{
		{
			desc:         "churn events below churnThreshold do not start a backoff",
			events:       churnEvents{ekTestWorkload: {nowMinus1m, now}},
			wantBackoffs: backoffEvents{},
		},
		{
			desc: "churn events reaching churnThreshold start a tier 1 backoff per workload separation",
			events: churnEvents{
				ekDefaultWorkload: {nowMinus2m, nowMinus1m, now},
				ekTestWorkload:    {now},
				e4aTestWorkload:   {nowMinus2m, nowMinus1m, now},
			},
			wantBackoffs: backoffEvents{
				ekDefaultWorkload: {tier: 1, endTime: nowPlus15m},
				e4aTestWorkload:   {tier: 1, endTime: nowPlus15m},
			},
		},
		{
			// The churn tracker cleans up the churn events older than churnWindow, so this should not happen.
			desc:         "churn events before the churn window are not counted towards churnThreshold",
			events:       churnEvents{ekTestWorkload: {windowStartMinus1m, nowMinus1m, now}},
			wantBackoffs: backoffEvents{},
		},
		{
			desc:           "active backoff is kept as is, even with churn events reaching churnThreshold",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 1, endTime: nowPlus10m}},
			events:         churnEvents{ekTestWorkload: {nowMinus2m, nowMinus1m, now}},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 1, endTime: nowPlus10m}},
		},
		{
			desc:           "ended backoff reaching churnThreshold since its end is escalated to the next tier",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 1, endTime: nowMinus5m}},
			events:         churnEvents{ekTestWorkload: {nowMinus2m, nowMinus1m, now}},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 2, endTime: nowPlus30m}},
		},
		{
			desc:           "churn events before the end of the backoff are not counted towards churnThreshold for the escalation",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 1, endTime: nowMinus5m}},
			events:         churnEvents{ekTestWorkload: {nowMinus20m, nowMinus20m, nowMinus1m, now}},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 1, endTime: nowMinus5m}},
		},
		{
			desc:           "ended backoff of the last tier is escalated with the last tier",
			backoffsBefore: backoffEvents{ekTestWorkload: {tier: 3, endTime: nowMinus5m}},
			events:         churnEvents{ekTestWorkload: {nowMinus2m, nowMinus1m, now}},
			wantBackoffs:   backoffEvents{ekTestWorkload: {tier: 3, endTime: nowPlus1h}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			m := NewBackoffManager(fakeclock.NewFakePassiveClock(now), testBackoffProvider(), nil)
			if tc.backoffsBefore != nil {
				m.backoffs = tc.backoffsBefore
			}

			m.createOrUpdateBackoffEvents(tc.events, now)

			if diff := cmp.Diff(tc.wantBackoffs, m.backoffs, backoffCmpOpts...); diff != "" {
				t.Errorf("createOrUpdateBackoffEvents() backoffs mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBackoffManagerUpdateMetrics(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	nowMinus5m := now.Add(-5 * time.Minute)
	nowPlus10m := now.Add(10 * time.Minute)
	ekTier1 := backoffTierKey{machineFamily: ekFamily, tier: 1}
	ekTier2 := backoffTierKey{machineFamily: ekFamily, tier: 2}
	e4aTier1 := backoffTierKey{machineFamily: e4aFamily, tier: 1}

	for _, tc := range []struct {
		desc string
		// reportedTiersBefore are the tiers reported by the previous call.
		reportedTiersBefore sets.Set[backoffTierKey]
		backoffs            backoffEvents
		wantMetric          map[backoffTierKey]int
		wantReportedTiers   sets.Set[backoffTierKey]
	}{
		{
			desc: "no backoffs - nothing is reported",
		},
		{
			desc: "backoffs are counted per machine family and tier, without the workload IDs",
			backoffs: backoffEvents{
				{machineFamily: ekFamily, workloadID: ""}:              {tier: 1, endTime: nowPlus10m},
				{machineFamily: ekFamily, workloadID: testWorkloadID}:  {tier: 1, endTime: nowPlus10m},
				{machineFamily: ekFamily, workloadID: "another"}:       {tier: 2, endTime: nowPlus10m},
				{machineFamily: e4aFamily, workloadID: testWorkloadID}: {tier: 1, endTime: nowPlus10m},
			},
			wantMetric:        map[backoffTierKey]int{ekTier1: 2, ekTier2: 1, e4aTier1: 1},
			wantReportedTiers: sets.New(ekTier1, ekTier2, e4aTier1),
		},
		{
			desc:                "ended backoffs, which are not reset yet, are not counted and their tier is reported as 0",
			reportedTiersBefore: sets.New(ekTier1, ekTier2),
			backoffs: backoffEvents{
				{machineFamily: ekFamily, workloadID: ""}:             {tier: 1, endTime: nowPlus10m},
				{machineFamily: ekFamily, workloadID: testWorkloadID}: {tier: 2, endTime: nowMinus5m},
			},
			wantMetric:        map[backoffTierKey]int{ekTier1: 1, ekTier2: 0},
			wantReportedTiers: sets.New(ekTier1),
		},
		{
			desc:                "tiers without backoffs anymore are reported as 0 once",
			reportedTiersBefore: sets.New(ekTier1, ekTier2),
			backoffs: backoffEvents{
				{machineFamily: ekFamily, workloadID: testWorkloadID}: {tier: 2, endTime: nowPlus10m},
			},
			wantMetric:        map[backoffTierKey]int{ekTier1: 0, ekTier2: 1},
			wantReportedTiers: sets.New(ekTier2),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			metrics := &fakeChurnMetrics{}
			m := NewBackoffManager(fakeclock.NewFakePassiveClock(now), testBackoffProvider(), metrics)
			m.reportedTiers = tc.reportedTiersBefore
			if tc.backoffs != nil {
				m.backoffs = tc.backoffs
			}

			m.updateMetrics(now)

			if diff := cmp.Diff(tc.wantMetric, metrics.backedOff, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("updateMetrics() metric mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantReportedTiers, m.reportedTiers, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("updateMetrics() reportedTiers mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBackoffManagerUpdateMetricsNilMetrics(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	m := NewBackoffManager(fakeclock.NewFakePassiveClock(now), testBackoffProvider(), nil)
	m.backoffs = backoffEvents{{machineFamily: ekFamily, workloadID: testWorkloadID}: {tier: 1, endTime: now.Add(time.Minute)}}

	// Must not panic.
	m.updateMetrics(now)
}

func TestBackoffManagerUpdateEventsDisabled(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	ekTestWorkload := workloadKey{machineFamily: ekFamily, workloadID: testWorkloadID}
	for _, tc := range []struct {
		desc     string
		provider ConfigProvider
	}{
		{desc: "experiment disabled", provider: &fakeProvider{enabled: false}},
		{desc: "nil provider", provider: nil},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			metrics := &fakeChurnMetrics{}
			m := NewBackoffManager(fakeclock.NewFakePassiveClock(now), tc.provider, metrics)

			m.UpdateEvents(churnEvents{ekTestWorkload: {now, now, now}}, now)

			if len(m.backoffs) != 0 {
				t.Errorf("UpdateEvents() backoffs = %v, want none", m.backoffs)
			}
			if len(metrics.backedOff) != 0 {
				t.Errorf("UpdateEvents() metric = %v, want none", metrics.backedOff)
			}
		})
	}
}

func TestBackoffManagerResetBackoffsDisabled(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	ekTestWorkload := workloadKey{machineFamily: ekFamily, workloadID: testWorkloadID}
	// The backoff would be reset if the experiment was enabled: it ended more than resetWindow ago without churn events.
	backoffsBefore := backoffEvents{ekTestWorkload: {tier: 1, endTime: now.Add(-time.Hour)}}
	for _, tc := range []struct {
		desc     string
		provider ConfigProvider
	}{
		{desc: "experiment disabled", provider: &fakeProvider{enabled: false}},
		{desc: "nil provider", provider: nil},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			metrics := &fakeChurnMetrics{}
			m := NewBackoffManager(fakeclock.NewFakePassiveClock(now), tc.provider, metrics)
			m.backoffs = backoffEvents{ekTestWorkload: backoffsBefore[ekTestWorkload]}

			m.ResetBackoffs(churnEvents{}, now)

			if diff := cmp.Diff(backoffsBefore, m.backoffs, backoffCmpOpts...); diff != "" {
				t.Errorf("ResetBackoffs() backoffs mismatch (-want +got):\n%s", diff)
			}
			if len(metrics.backedOff) != 0 {
				t.Errorf("ResetBackoffs() metric = %v, want none", metrics.backedOff)
			}
		})
	}
}

// testBackoffProvider returns an enabled fakeProvider with short backoff durations.
func testBackoffProvider() *fakeProvider {
	return &fakeProvider{
		enabled:             true,
		churnWindow:         30 * time.Minute,
		churnThreshold:      3,
		backoffDurations:    []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour},
		resetWindow:         30 * time.Minute,
		resetMaxChurnEvents: 0,
	}
}

// fakeProvider is a Provider with fixed values.
type fakeProvider struct {
	enabled                bool
	churnLifetimeThreshold time.Duration
	churnWindow            time.Duration
	churnThreshold         int
	backoffDurations       []time.Duration
	resetWindow            time.Duration
	resetMaxChurnEvents    int
}

func (f *fakeProvider) Refresh()                              {}
func (f *fakeProvider) IsEnabled() bool                       { return f.enabled }
func (f *fakeProvider) ChurnLifetimeThreshold() time.Duration { return f.churnLifetimeThreshold }
func (f *fakeProvider) ChurnWindow() time.Duration            { return f.churnWindow }
func (f *fakeProvider) ChurnThreshold() int                   { return f.churnThreshold }
func (f *fakeProvider) BackoffDurations() []time.Duration     { return slices.Clone(f.backoffDurations) }
func (f *fakeProvider) ResetWindow() time.Duration            { return f.resetWindow }
func (f *fakeProvider) ResetMaxChurnEvents() int              { return f.resetMaxChurnEvents }
