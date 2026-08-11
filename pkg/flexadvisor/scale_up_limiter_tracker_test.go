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

func TestMarkScaleUpOptionRemoved_Enabled(t *testing.T) {
	tracker := NewScaleUpLimiterTracker(true, nil)

	tracker.MarkScaleUpOptionRemoved("mig-1", "scope-1")

	assert.True(t, tracker.HasRemovedScaleUpOptions())
	assert.Equal(t, []string{"scope-1"}, tracker.GetFlexibilityScopesWithRemovedScaleUpOptions())
	assert.Equal(t, []string{"mig-1"}, tracker.GetRemovedNodeGroupIds())
}

func TestGetFlexibilityScopesWithRemovedScaleUpOptions_ReturnsSortedUniqueScopes(t *testing.T) {
	tracker := NewScaleUpLimiterTracker(true, nil)

	tracker.MarkScaleUpOptionRemoved("mig-2", "scope-b")
	tracker.MarkScaleUpOptionRemoved("mig-1", "scope-a")
	tracker.MarkScaleUpOptionRemoved("mig-3", "scope-b")

	assert.Equal(t, []string{"scope-a", "scope-b"}, tracker.GetFlexibilityScopesWithRemovedScaleUpOptions())
	assert.Equal(t, []string{"mig-1", "mig-2", "mig-3"}, tracker.GetRemovedNodeGroupIds())
}

func TestReset_ClearsTrackedScopesAndNodeGroups(t *testing.T) {
	tracker := NewScaleUpLimiterTracker(true, nil)
	tracker.MarkScaleUpOptionRemoved("mig-1", "scope-1")

	tracker.Reset()

	assert.False(t, tracker.HasRemovedScaleUpOptions())
	assert.Empty(t, tracker.GetFlexibilityScopesWithRemovedScaleUpOptions())
	assert.Empty(t, tracker.GetRemovedNodeGroupIds())
}

func TestScaleUpLimiterTracker_DisabledByGCEFlexAdvisorEnabledFlag(t *testing.T) {
	tracker := NewScaleUpLimiterTracker(false, nil)

	tracker.MarkScaleUpOptionRemoved("mig-1", "scope-1")

	assert.False(t, tracker.HasRemovedScaleUpOptions())
	assert.Nil(t, tracker.GetFlexibilityScopesWithRemovedScaleUpOptions())
	assert.Nil(t, tracker.GetRemovedNodeGroupIds())
}

func TestScaleUpLimiterTracker_DisabledByExperiment(t *testing.T) {
	evaluator := expfake.NewEvaluator(map[string]bool{
		experiments.FlexAdvisorScaleUpLimiterTrackerEnabledFlag: false,
	}, nil)
	manager := experiments.NewManager(version.Version{}, evaluator)
	tracker := NewScaleUpLimiterTracker(true, manager)

	tracker.MarkScaleUpOptionRemoved("mig-1", "scope-1")

	assert.False(t, tracker.HasRemovedScaleUpOptions())
	assert.Nil(t, tracker.GetFlexibilityScopesWithRemovedScaleUpOptions())
	assert.Nil(t, tracker.GetRemovedNodeGroupIds())
}

func TestScaleUpLimiterTracker_DisabledByMainProcessingExperiment(t *testing.T) {
	evaluator := expfake.NewEvaluator(map[string]bool{
		experiments.FlexAdvisorProcessingEnabledFlag: false,
	}, nil)
	manager := experiments.NewManager(version.Version{}, evaluator)
	tracker := NewScaleUpLimiterTracker(true, manager)

	tracker.MarkScaleUpOptionRemoved("mig-1", "scope-1")

	assert.False(t, tracker.HasRemovedScaleUpOptions())
	assert.Nil(t, tracker.GetFlexibilityScopesWithRemovedScaleUpOptions())
	assert.Nil(t, tracker.GetRemovedNodeGroupIds())
}

func TestScaleUpLimiterTracker_ConcurrentAccess(t *testing.T) {
	tracker := NewScaleUpLimiterTracker(true, nil)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(5)
		go func() {
			defer wg.Done()
			tracker.MarkScaleUpOptionRemoved("mig-1", "scope-1")
		}()
		go func() {
			defer wg.Done()
			_ = tracker.HasRemovedScaleUpOptions()
		}()
		go func() {
			defer wg.Done()
			_ = tracker.GetFlexibilityScopesWithRemovedScaleUpOptions()
		}()
		go func() {
			defer wg.Done()
			_ = tracker.GetRemovedNodeGroupIds()
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
