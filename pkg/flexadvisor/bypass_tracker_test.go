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

package flexadvisor

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestHasReachedBypassThreshold(t *testing.T) {
	testCases := []struct {
		name               string
		scopeKey           string
		blockIntervals     []time.Duration
		waitBeforeQuery    time.Duration
		experimentsManager experiments.Manager
		expectedReached    bool
	}{
		{
			name:            "no blocks recorded - returns false",
			scopeKey:        "scope-a",
			blockIntervals:  nil,
			waitBeforeQuery: 0,
			expectedReached: false,
		},
		{
			name:            "fewer than 3 blocks in window - returns false",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 10 * time.Minute},
			waitBeforeQuery: 10 * time.Minute,
			expectedReached: false,
		},
		{
			name:            "3 blocks within default 60m window - returns true",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 15 * time.Minute, 15 * time.Minute},
			waitBeforeQuery: 15 * time.Minute,
			expectedReached: true,
		},
		{
			name:            "oldest of 3 blocks falls outside 60m sliding window - returns false",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 15 * time.Minute, 15 * time.Minute},
			waitBeforeQuery: 31 * time.Minute, // total elapsed since 1st block = 61m
			expectedReached: false,
		},
		{
			name:            "4 blocks where oldest expired but 3 remain within 60m sliding window - returns true",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 15 * time.Minute, 15 * time.Minute, 35 * time.Minute}, // blocks at t=0, 15m, 30m, 65m
			waitBeforeQuery: 5 * time.Minute,                                                          // queried at t=70m; blocks at 15m, 30m, 65m are within [10m, 70m]
			expectedReached: true,
		},
		{
			name:            "nil experiments manager uses defaults (3 blocks in 60m) - returns true",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 10 * time.Minute, 10 * time.Minute},
			waitBeforeQuery: 5 * time.Minute,
			expectedReached: true,
		},
		{
			name:            "custom count threshold flag (5) with only 3 blocks - returns false",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 5 * time.Minute, 5 * time.Minute},
			waitBeforeQuery: 5 * time.Minute,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorRecommendationsBypassCountThresholdFlag: "5",
			}),
			expectedReached: false,
		},
		{
			name:            "custom count threshold flag (5) with 5 blocks - returns true",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 5 * time.Minute, 5 * time.Minute, 5 * time.Minute, 5 * time.Minute},
			waitBeforeQuery: 5 * time.Minute,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorRecommendationsBypassCountThresholdFlag: "5",
			}),
			expectedReached: true,
		},
		{
			name:            "negative count threshold flag (-1) falls back to default (3) - returns true",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 5 * time.Minute, 5 * time.Minute},
			waitBeforeQuery: 5 * time.Minute,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorRecommendationsBypassCountThresholdFlag: "-1",
			}),
			expectedReached: true,
		},
		{
			name:            "zero count threshold flag (0) falls back to default (3) - returns false with 2 blocks",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 5 * time.Minute},
			waitBeforeQuery: 5 * time.Minute,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorRecommendationsBypassCountThresholdFlag: "0",
			}),
			expectedReached: false,
		},
		{
			name:            "custom measurement window flag (30m) where oldest block is 31m old - returns false",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 10 * time.Minute, 10 * time.Minute},
			waitBeforeQuery: 11 * time.Minute, // total elapsed since 1st block = 31m > 30m
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorRecommendationsBypassMeasurementWindowFlag: "30",
			}),
			expectedReached: false,
		},
		{
			name:            "custom measurement window flag (30m) where all 3 blocks are within 25m - returns true",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 10 * time.Minute, 10 * time.Minute},
			waitBeforeQuery: 5 * time.Minute, // total elapsed since 1st block = 25m <= 30m
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorRecommendationsBypassMeasurementWindowFlag: "30",
			}),
			expectedReached: true,
		},
		{
			name:            "negative measurement window flag (-10) falls back to default 60m window - returns true at 45m",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 15 * time.Minute, 15 * time.Minute},
			waitBeforeQuery: 15 * time.Minute,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorRecommendationsBypassMeasurementWindowFlag: "-10",
			}),
			expectedReached: true,
		},
		{
			name:            "zero measurement window flag (0) falls back to default 60m window - returns true at 45m",
			scopeKey:        "scope-a",
			blockIntervals:  []time.Duration{0, 15 * time.Minute, 15 * time.Minute},
			waitBeforeQuery: 15 * time.Minute,
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, nil, map[string]string{
				experiments.FlexAdvisorRecommendationsBypassMeasurementWindowFlag: "0",
			}),
			expectedReached: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				tracker := NewRecommendationsBypassTracker(tc.experimentsManager)

				for _, d := range tc.blockIntervals {
					if d > 0 {
						time.Sleep(d)
					}
					tracker.MarkBlocked([]string{tc.scopeKey})
				}

				if tc.waitBeforeQuery > 0 {
					time.Sleep(tc.waitBeforeQuery)
				}

				got := tracker.HasReachedBypassThreshold(tc.scopeKey)
				assert.Equal(t, tc.expectedReached, got)
			})
		})
	}
}

func TestPauseRemoveBypassesAndResetLifecycle(t *testing.T) {
	testCases := []struct {
		name              string
		actions           func(tracker RecommendationsBypassTracker)
		expectedThreshold map[string]bool
		expectedPaused    map[string]bool
	}{
		{
			name: "pause enforcement after 3 blocks - sets paused to true and clears block counter",
			actions: func(tracker RecommendationsBypassTracker) {
				tracker.MarkBlocked([]string{"scope-1"})
				tracker.MarkBlocked([]string{"scope-1"})
				tracker.MarkBlocked([]string{"scope-1"})
				tracker.PauseRecommendationsEnforcement("scope-1")
			},
			expectedThreshold: map[string]bool{"scope-1": false},
			expectedPaused:    map[string]bool{"scope-1": true},
		},
		{
			name: "RemoveBypasses after pausing multiple scopes - unpauses all scopes and leaves thresholds false",
			actions: func(tracker RecommendationsBypassTracker) {
				for range 3 {
					tracker.MarkBlocked([]string{"scope-1", "scope-2"})
				}
				tracker.PauseRecommendationsEnforcement("scope-1")
				tracker.PauseRecommendationsEnforcement("scope-2")
				tracker.RemoveBypasses()
			},
			expectedThreshold: map[string]bool{"scope-1": false, "scope-2": false},
			expectedPaused:    map[string]bool{"scope-1": false, "scope-2": false},
		},
		{
			name: "ResetCounters on one scope - clears block counter only for target scope while preserving active bypass",
			actions: func(tracker RecommendationsBypassTracker) {
				for range 3 {
					tracker.MarkBlocked([]string{"scope-a", "scope-b"})
				}
				tracker.PauseRecommendationsEnforcement("scope-a")
				for range 3 {
					tracker.MarkBlocked([]string{"scope-a"})
				}
				tracker.ResetCounters([]string{"scope-a"})
			},
			expectedThreshold: map[string]bool{"scope-a": false, "scope-b": true},
			expectedPaused:    map[string]bool{"scope-a": true, "scope-b": false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewRecommendationsBypassTracker(experiments.NewMockManager())
			tc.actions(tracker)

			for scope, wantThreshold := range tc.expectedThreshold {
				assert.Equal(t, wantThreshold, tracker.HasReachedBypassThreshold(scope), "threshold mismatch for scope %q", scope)
			}
			for scope, wantPaused := range tc.expectedPaused {
				assert.Equal(t, wantPaused, tracker.IsRecommendationsEnforcementPaused(scope), "paused mismatch for scope %q", scope)
			}
		})
	}
}

func TestRecommendationsBypassTracker_ExperimentsDisabled(t *testing.T) {
	testCases := []struct {
		name               string
		experimentsManager experiments.Manager
		expectedEnabled    bool
	}{
		{
			name: "all flags enabled by default - tracker is active",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:            true,
				experiments.FlexAdvisorRecommendationsBypassEnabledFlag: true,
			}, nil),
			expectedEnabled: true,
		},
		{
			name: "FlexAdvisorRecommendationsBypassEnabledFlag disabled - tracker is inactive",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:            true,
				experiments.FlexAdvisorRecommendationsBypassEnabledFlag: false,
			}, nil),
			expectedEnabled: false,
		},
		{
			name: "FlexAdvisorProcessingEnabledFlag disabled - tracker is inactive",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:            false,
				experiments.FlexAdvisorRecommendationsBypassEnabledFlag: true,
			}, nil),
			expectedEnabled: false,
		},
		{
			name: "FlexAdvisorScaleUpLimiterTrackerEnabledFlag disabled - tracker is inactive",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:            true,
				experiments.FlexAdvisorScaleUpLimiterTrackerEnabledFlag: false,
				experiments.FlexAdvisorRecommendationsBypassEnabledFlag: true,
			}, nil),
			expectedEnabled: false,
		},
		{
			name: "FlexAdvisorRecommendationsBypassMinCAVersionFlag not met - tracker is inactive",
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:            true,
				experiments.FlexAdvisorRecommendationsBypassEnabledFlag: true,
			}, map[string]string{
				experiments.FlexAdvisorRecommendationsBypassMinCAVersionFlag: "1.35.0-gke.100",
			}),
			expectedEnabled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewRecommendationsBypassTracker(tc.experimentsManager)
			for range 3 {
				tracker.MarkBlocked([]string{"scope-a"})
			}
			assert.Equal(t, tc.expectedEnabled, tracker.HasReachedBypassThreshold("scope-a"))

			tracker.PauseRecommendationsEnforcement("scope-a")
			assert.Equal(t, tc.expectedEnabled, tracker.IsRecommendationsEnforcementPaused("scope-a"))

			// Pre-populate pausedRecommendationsEnforcement directly to verify RemoveBypasses
			// returns early without clearing the cache when the experiment is disabled.
			rawTracker := tracker.(*recommendationsBypassTracker)
			rawTracker.pausedRecommendationsEnforcement.Set("scope-prepopulated", true, time.Hour)
			tracker.RemoveBypasses()
			_, stillPresent := rawTracker.pausedRecommendationsEnforcement.Get("scope-prepopulated")
			assert.Equal(t, !tc.expectedEnabled, stillPresent)
		})
	}
}
