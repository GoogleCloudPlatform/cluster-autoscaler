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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	expfake "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments/fake"
)

func TestScaleUpLimiterTracker_RecordingAndQuerying(t *testing.T) {
	type recordedOption struct {
		nodeGroupId      string
		flexibilityScope string
	}
	testCases := []struct {
		name           string
		recorded       []recordedOption
		queryGroupId   string
		expectedRemove bool
		expectedScopes []string
	}{
		{
			name: "single node group with single scope - returns removed true and matching scope",
			recorded: []recordedOption{
				{nodeGroupId: "mig-1", flexibilityScope: "scope-a"},
			},
			queryGroupId:   "mig-1",
			expectedRemove: true,
			expectedScopes: []string{"scope-a"},
		},
		{
			name: "single node group with multiple scopes - returns sorted deduplicated scopes",
			recorded: []recordedOption{
				{nodeGroupId: "mig-1", flexibilityScope: "scope-b"},
				{nodeGroupId: "mig-1", flexibilityScope: "scope-a"},
				{nodeGroupId: "mig-1", flexibilityScope: "scope-b"},
				{nodeGroupId: "mig-2", flexibilityScope: "scope-c"},
			},
			queryGroupId:   "mig-1",
			expectedRemove: true,
			expectedScopes: []string{"scope-a", "scope-b"},
		},
		{
			name: "node group recorded with empty scope - returns removed true and nil scopes",
			recorded: []recordedOption{
				{nodeGroupId: "mig-1", flexibilityScope: ""},
			},
			queryGroupId:   "mig-1",
			expectedRemove: true,
			expectedScopes: nil,
		},
		{
			name: "queried node group not removed - returns removed false and nil scopes",
			recorded: []recordedOption{
				{nodeGroupId: "mig-1", flexibilityScope: "scope-a"},
			},
			queryGroupId:   "mig-unknown",
			expectedRemove: false,
			expectedScopes: nil,
		},
		{
			name: "empty node group ID ignored - returns removed false and nil scopes",
			recorded: []recordedOption{
				{nodeGroupId: "", flexibilityScope: "scope-a"},
			},
			queryGroupId:   "",
			expectedRemove: false,
			expectedScopes: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewScaleUpLimiterTracker(true, nil)
			for _, rec := range tc.recorded {
				tracker.MarkScaleUpOptionRemoved(rec.nodeGroupId, rec.flexibilityScope)
			}
			assert.Equal(t, tc.expectedRemove, tracker.WasNodeGroupRemovedByFlexAdvisor(tc.queryGroupId))
			assert.Equal(t, tc.expectedScopes, tracker.GetFlexibilityScopesForNodeGroupIfRemoved(tc.queryGroupId))
		})
	}
}

func TestReset_ClearsTrackedScopesAndNodeGroups(t *testing.T) {
	tracker := NewScaleUpLimiterTracker(true, nil)
	tracker.MarkScaleUpOptionRemoved("mig-1", "scope-1")

	tracker.Reset()

	assert.False(t, tracker.WasNodeGroupRemovedByFlexAdvisor("mig-1"))
	assert.Nil(t, tracker.GetFlexibilityScopesForNodeGroupIfRemoved("mig-1"))
}

func TestScaleUpLimiterTracker_Disabled(t *testing.T) {
	testCases := []struct {
		name                  string
		gceFlexAdvisorEnabled bool
		boolFlags             map[string]bool
	}{
		{
			name:                  "disabled by GCEFlexAdvisorEnabled flag - ignores recording and returns empty",
			gceFlexAdvisorEnabled: false,
		},
		{
			name:                  "disabled by ScaleUpLimiterTracker experiment flag - ignores recording and returns empty",
			gceFlexAdvisorEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorScaleUpLimiterTrackerEnabledFlag: false,
			},
		},
		{
			name:                  "disabled by main FlexAdvisorProcessing experiment flag - ignores recording and returns empty",
			gceFlexAdvisorEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var manager experiments.Manager
			if tc.boolFlags != nil {
				evaluator := expfake.NewEvaluator(tc.boolFlags, nil)
				manager = experiments.NewManager(version.Version{}, evaluator)
			}
			tracker := NewScaleUpLimiterTracker(tc.gceFlexAdvisorEnabled, manager)

			tracker.MarkScaleUpOptionRemoved("mig-1", "scope-1")

			assert.False(t, tracker.WasNodeGroupRemovedByFlexAdvisor("mig-1"))
			assert.Nil(t, tracker.GetFlexibilityScopesForNodeGroupIfRemoved("mig-1"))
		})
	}
}

func TestScaleUpLimiterTracker_ConcurrentAccess(t *testing.T) {
	tracker := NewScaleUpLimiterTracker(true, nil)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			tracker.MarkScaleUpOptionRemoved("mig-1", "scope-1")
		}()
		go func() {
			defer wg.Done()
			_ = tracker.WasNodeGroupRemovedByFlexAdvisor("mig-1")
		}()
		go func() {
			defer wg.Done()
			_ = tracker.GetFlexibilityScopesForNodeGroupIfRemoved("mig-1")
		}()
		go func() {
			defer wg.Done()
			tracker.Reset()
		}()
	}
	wg.Wait()
}

func TestIsFlexAdvisorScaleUpLimiterTrackerEnabled(t *testing.T) {
	testCases := []struct {
		name                  string
		gceFlexAdvisorEnabled bool
		boolFlags             map[string]bool
		stringFlags           map[string]string
		nilManager            bool
		want                  bool
	}{
		{
			name:                  "gceFlexAdvisorEnabled false - returns false",
			gceFlexAdvisorEnabled: false,
			want:                  false,
		},
		{
			name:                  "gceFlexAdvisorEnabled false with nil manager - returns false",
			gceFlexAdvisorEnabled: false,
			nilManager:            true,
			want:                  false,
		},
		{
			name:                  "nil manager - defaults to true",
			gceFlexAdvisorEnabled: true,
			nilManager:            true,
			want:                  true,
		},
		{
			name:                  "nothing set - returns true",
			gceFlexAdvisorEnabled: true,
			want:                  true,
		},
		{
			name:                  "FlexAdvisor::EnableProcessing off - returns false",
			gceFlexAdvisorEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			},
			want: false,
		},
		{
			name:                  "FlexAdvisor::ProcessingMinCAVersion doesn't match - returns false",
			gceFlexAdvisorEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorProcessingMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name:                  "FlexAdvisor::ScaleUpLimiterTracker off - returns false",
			gceFlexAdvisorEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorScaleUpLimiterTrackerEnabledFlag: false,
			},
			want: false,
		},
		{
			name:                  "FlexAdvisor::ScaleUpLimiterTrackerMinCAVersion doesn't match - returns false",
			gceFlexAdvisorEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorScaleUpLimiterTrackerMinCAVersionFlag: false,
			},
			want: false,
		},
		{
			name:                  "all flags enabled - returns true",
			gceFlexAdvisorEnabled: true,
			boolFlags: map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag:                 true,
				experiments.FlexAdvisorProcessingMinCAVersionFlag:            true,
				experiments.FlexAdvisorScaleUpLimiterTrackerEnabledFlag:      true,
				experiments.FlexAdvisorScaleUpLimiterTrackerMinCAVersionFlag: true,
			},
			want: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var manager experiments.Manager
			if tc.nilManager && (tc.boolFlags != nil || tc.stringFlags != nil) {
				t.Fatalf("Invalid usage: nilManager cannot be set along with experiments")
			}
			if !tc.nilManager {
				manager = experiments.NewMockManagerWithOptions(version.Version{}, tc.boolFlags, tc.stringFlags)
			}
			got := IsFlexAdvisorScaleUpLimiterTrackerEnabled(tc.gceFlexAdvisorEnabled, manager)
			assert.Equal(t, tc.want, got)
		})
	}
}
