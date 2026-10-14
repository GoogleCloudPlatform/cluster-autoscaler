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
	"context"
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/callbacks"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

type fakeInnerOrchestrator struct {
	initialized         bool
	minSizeCalled       bool
	onScaleUp           func(bypassTracker RecommendationsBypassTracker, limiterTracker ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError)
	pausedDuringScaleUp map[string]bool
	scopesToQuery       []string
}

func (f *fakeInnerOrchestrator) Initialize(
	_ *ca_context.AutoscalingContext,
	_ *ca_processors.AutoscalingProcessors,
	_ *clusterstate.ClusterStateRegistry,
	_ estimator.EstimatorBuilder,
	_ taints.TaintConfig,
	_ *resourcequotas.TrackerFactory,
) {
	f.initialized = true
}

func (f *fakeInnerOrchestrator) ScaleUp(
	_ context.Context,
	_ []*apiv1.Pod,
	_ []*apiv1.Node,
	_ []*appsv1.DaemonSet,
	_ map[string]*framework.NodeInfo,
	_ bool,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	if f.onScaleUp != nil {
		return f.onScaleUp(nil, nil)
	}
	return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
}

func (f *fakeInnerOrchestrator) ScaleUpToNodeGroupMinSize(
	_ context.Context,
	_ []*apiv1.Node,
	_ map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	f.minSizeCalled = true
	return &status.ScaleUpStatus{Result: status.ScaleUpNotTried}, nil
}

type mockWrapperOrchestratorMetrics struct {
	bypassAttempts int
}

func (m *mockWrapperOrchestratorMetrics) RegisterFlexAdvisorBypassAttempt() {
	m.bypassAttempts++
}

func TestWrapperOrchestrator_ScaleUp(t *testing.T) {
	crdScope1 := ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "scope-1"}}, "", false, nil, nil)
	crdScope2 := ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "scope-2"}}, "", false, nil, nil)
	crdDefault := ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "default"}}, "", false, nil, nil)

	podScope1 := testPod("scope-1")
	podWithoutCCC := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "basic-pod"}}

	testCases := []struct {
		name                      string
		crds                      []crd.CRD
		defaultCccEnabled         bool
		unschedulablePods         []*apiv1.Pod
		selectedShard             *podsharding.PodShard
		experimentsManager        experiments.Manager
		setupTrackers             func(bypassTracker RecommendationsBypassTracker, limiterTracker ScaleUpLimiterTracker)
		innerScaleUp              func(bypassTracker RecommendationsBypassTracker, limiterTracker ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError)
		expectedPausedDuringRun   map[string]bool
		expectedThresholdAfterRun map[string]bool
		expectedMetrics           int
		expectedConstrainedAfter  []string
	}{
		{
			name:              "ScaleUpNoOptionsAvailable with FA-constrained scope - marks scope blocked and keeps limiterTracker for downstream status processors",
			crds:              []crd.CRD{crdScope1, crdScope2},
			unschedulablePods: []*apiv1.Pod{podScope1},
			selectedShard:     cccShard("scope-1"),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, limiterTracker ScaleUpLimiterTracker) {
				limiterTracker.MarkScaleUpOptionRemovedByFlexAdvisor("stale-mig", "scope-2")
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, limiterTracker ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				limiterTracker.MarkScaleUpOptionRemovedByFlexAdvisor("mig-1", "scope-1")
				return &status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable}, nil
			},
			expectedPausedDuringRun:   map[string]bool{"scope-1": false},
			expectedThresholdAfterRun: map[string]bool{"scope-1": true, "scope-2": false},
			expectedMetrics:           0,
			expectedConstrainedAfter:  []string{"scope-1"},
		},
		{
			name:              "ScaleUpSuccessful after 2 prior blocks - resets counters for active scope",
			crds:              []crd.CRD{crdScope1, crdScope2},
			unschedulablePods: []*apiv1.Pod{podScope1},
			selectedShard:     cccShard("scope-1"),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, _ ScaleUpLimiterTracker) {
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-2"})
				bypassTracker.MarkBlocked([]string{"scope-2"})
				bypassTracker.MarkBlocked([]string{"scope-2"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				return &status.ScaleUpStatus{Result: status.ScaleUpSuccessful}, nil
			},
			expectedPausedDuringRun:   map[string]bool{"scope-1": false, "scope-2": false},
			expectedThresholdAfterRun: map[string]bool{"scope-1": false, "scope-2": true},
			expectedMetrics:           0,
		},
		{
			name:              "threshold reached (3 blocks) for active scope - pauses enforcement during ScaleUp and increments metric",
			crds:              []crd.CRD{crdScope1, crdScope2},
			unschedulablePods: []*apiv1.Pod{podScope1},
			selectedShard:     cccShard("scope-1"),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, _ ScaleUpLimiterTracker) {
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-2"})
				// Simulate a stale bypass from a previous run on scope-2 that should be cleared by RemoveBypasses() at start of ScaleUp
				bypassTracker.PauseRecommendationsEnforcement("scope-2")
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				return &status.ScaleUpStatus{Result: status.ScaleUpSuccessful}, nil
			},
			expectedPausedDuringRun:   map[string]bool{"scope-1": true, "scope-2": false},
			expectedThresholdAfterRun: map[string]bool{"scope-1": false, "scope-2": false},
			expectedMetrics:           1,
		},
		{
			name:              "mixed shard with plain pod first and adaptive polling enabled - resolves explicit CCC scope from autoscalingCtx instead of default",
			crds:              []crd.CRD{crdScope1, crdDefault},
			defaultCccEnabled: true,
			unschedulablePods: []*apiv1.Pod{podWithoutCCC, podScope1},
			selectedShard:     cccShard("scope-1"),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, _ ScaleUpLimiterTracker) {
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"default"})
				bypassTracker.MarkBlocked([]string{"default"})
				bypassTracker.MarkBlocked([]string{"default"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				return &status.ScaleUpStatus{Result: status.ScaleUpSuccessful}, nil
			},
			expectedPausedDuringRun:   map[string]bool{"scope-1": true, "default": false},
			expectedThresholdAfterRun: map[string]bool{"scope-1": false, "default": true},
			expectedMetrics:           1,
		},
		{
			name:              "mixed shard with plain pod first and adaptive polling disabled - still resolves explicit CCC scope from autoscalingCtx instead of default",
			crds:              []crd.CRD{crdScope1, crdDefault},
			defaultCccEnabled: true,
			unschedulablePods: []*apiv1.Pod{podWithoutCCC, podScope1},
			selectedShard:     cccShard("scope-1"),
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag: false,
			}, nil),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, _ ScaleUpLimiterTracker) {
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"default"})
				bypassTracker.MarkBlocked([]string{"default"})
				bypassTracker.MarkBlocked([]string{"default"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				return &status.ScaleUpStatus{Result: status.ScaleUpSuccessful}, nil
			},
			expectedPausedDuringRun:   map[string]bool{"scope-1": true, "default": false},
			expectedThresholdAfterRun: map[string]bool{"scope-1": false, "default": true},
			expectedMetrics:           1,
		},
		{
			name:              "provisioning request shard - does not consume bypass or reset counters for CCC scope",
			crds:              []crd.CRD{crdScope1},
			unschedulablePods: []*apiv1.Pod{podScope1},
			selectedShard:     provReqShard("scope-1"),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, _ ScaleUpLimiterTracker) {
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				return &status.ScaleUpStatus{Result: status.ScaleUpSuccessful}, nil
			},
			expectedPausedDuringRun:   map[string]bool{"scope-1": false},
			expectedThresholdAfterRun: map[string]bool{"scope-1": true},
			expectedMetrics:           0,
		},
		{
			name:              "non-CCC pod shard with Default CCC enabled - pauses default scope during ScaleUp when default reached threshold",
			crds:              []crd.CRD{crdScope1, crdDefault},
			defaultCccEnabled: true,
			unschedulablePods: []*apiv1.Pod{podWithoutCCC},
			selectedShard:     plainShard("basic"),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, _ ScaleUpLimiterTracker) {
				bypassTracker.MarkBlocked([]string{"default"})
				bypassTracker.MarkBlocked([]string{"default"})
				bypassTracker.MarkBlocked([]string{"default"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				return &status.ScaleUpStatus{Result: status.ScaleUpSuccessful}, nil
			},
			expectedPausedDuringRun:   map[string]bool{"default": true, "scope-1": false},
			expectedThresholdAfterRun: map[string]bool{"default": false},
			expectedMetrics:           1,
		},
		{
			name:              "non-CCC pod shard without Default CCC - does not pause blocked CCC scope",
			crds:              []crd.CRD{crdScope1},
			unschedulablePods: []*apiv1.Pod{podWithoutCCC},
			selectedShard:     plainShard("basic"),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, _ ScaleUpLimiterTracker) {
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				return &status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable}, nil
			},
			expectedPausedDuringRun:   map[string]bool{"scope-1": false},
			expectedThresholdAfterRun: map[string]bool{"scope-1": true},
			expectedMetrics:           0,
		},
		{
			name:              "ScaleUpError does not reset counters or mark blocked",
			crds:              []crd.CRD{crdScope1},
			unschedulablePods: []*apiv1.Pod{podScope1},
			selectedShard:     cccShard("scope-1"),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, _ ScaleUpLimiterTracker) {
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, limiterTracker ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				limiterTracker.MarkScaleUpOptionRemovedByFlexAdvisor("mig-1", "scope-1")
				return &status.ScaleUpStatus{Result: status.ScaleUpError}, errors.NewAutoscalerError(errors.CloudProviderError, "simulated error")
			},
			expectedPausedDuringRun:   map[string]bool{"scope-1": false},
			expectedThresholdAfterRun: map[string]bool{"scope-1": false},
			expectedMetrics:           0,
			expectedConstrainedAfter:  []string{"scope-1"},
		},
		{
			name:              "FlexAdvisorRecommendationsBypassEnabledFlag disabled - does not pause or mark blocked, and resets limiterTracker",
			crds:              []crd.CRD{crdScope1},
			unschedulablePods: []*apiv1.Pod{podScope1},
			selectedShard:     cccShard("scope-1"),
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorRecommendationsBypassEnabledFlag: false,
			}, nil),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, limiterTracker ScaleUpLimiterTracker) {
				limiterTracker.MarkScaleUpOptionRemovedByFlexAdvisor("stale-mig", "scope-1")
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				return &status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable}, nil
			},
			expectedPausedDuringRun:  map[string]bool{"scope-1": false},
			expectedMetrics:          0,
			expectedConstrainedAfter: []string{},
		},
		{
			name:              "FlexAdvisorProcessingEnabledFlag disabled - does not pause even with 3 blocks",
			crds:              []crd.CRD{crdScope1},
			unschedulablePods: []*apiv1.Pod{podScope1},
			selectedShard:     cccShard("scope-1"),
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorProcessingEnabledFlag: false,
			}, nil),
			setupTrackers: func(bypassTracker RecommendationsBypassTracker, _ ScaleUpLimiterTracker) {
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
				bypassTracker.MarkBlocked([]string{"scope-1"})
			},
			innerScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				return &status.ScaleUpStatus{Result: status.ScaleUpNoOptionsAvailable}, nil
			},
			expectedPausedDuringRun: map[string]bool{"scope-1": false},
			expectedMetrics:         0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := tc.experimentsManager
			if manager == nil {
				manager = experiments.NewMockManager()
			}
			bypassTracker := NewRecommendationsBypassTracker(manager)
			limiterTracker := NewScaleUpLimiterTracker(true, manager)
			if tc.setupTrackers != nil {
				tc.setupTrackers(bypassTracker, limiterTracker)
			}

			mockLister := lister.NewMockCrdListerWithLabel(tc.crds, labels.ComputeClassLabel)
			if tc.defaultCccEnabled {
				mockLister.SetDefaultCrdName("default")
			}

			pausedDuringScaleUp := make(map[string]bool)
			inner := &fakeInnerOrchestrator{}
			inner.onScaleUp = func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
				assert.Empty(t, limiterTracker.GetFlexibilityScopesConstrainedByFlexAdvisor(), "expected limiterTracker to be reset before inner ScaleUp")
				for scope := range tc.expectedPausedDuringRun {
					pausedDuringScaleUp[scope] = bypassTracker.IsRecommendationsEnforcementPaused(scope)
				}
				return tc.innerScaleUp(bypassTracker, limiterTracker)
			}

			autoscalingCtx := &ca_context.AutoscalingContext{
				ProcessorCallbacks: callbacks.NewTestProcessorCallbacks(),
			}
			if tc.selectedShard != nil {
				podsharding.TestSetPodShard(autoscalingCtx, tc.selectedShard)
			}

			mockMetrics := &mockWrapperOrchestratorMetrics{}
			wrapper := NewWrapperOrchestrator(inner, nil, mockLister, bypassTracker, limiterTracker, nil, manager, withWrapperOrchestratorMetrics(mockMetrics))
			wrapper.Initialize(autoscalingCtx, nil, nil, nil, taints.TaintConfig{}, nil)

			_, _ = wrapper.ScaleUp(context.Background(), tc.unschedulablePods, nil, nil, nil, false)

			for scope, wantPaused := range tc.expectedPausedDuringRun {
				assert.Equal(t, wantPaused, pausedDuringScaleUp[scope], "scope %s paused state during ScaleUp mismatch", scope)
			}
			for scope, wantThreshold := range tc.expectedThresholdAfterRun {
				assert.Equal(t, wantThreshold, bypassTracker.HasReachedBypassThreshold(scope), "scope %s threshold state after ScaleUp mismatch", scope)
			}
			if tc.expectedConstrainedAfter != nil {
				assert.ElementsMatch(t, tc.expectedConstrainedAfter, limiterTracker.GetFlexibilityScopesConstrainedByFlexAdvisor())
			}
			assert.Equal(t, tc.expectedMetrics, mockMetrics.bypassAttempts)
		})
	}
}

type fakeScaleUpEndProcessor struct {
	onEnd func()
}

func (f *fakeScaleUpEndProcessor) OnScaleUpEnd() {
	if f.onEnd != nil {
		f.onEnd()
	}
}

func TestWrapperOrchestrator_ScaleUp_ScaleUpEndProcessors(t *testing.T) {
	crdScope1 := ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "scope-1"}}, "", false, nil, nil)
	podScope1 := testPod("scope-1")

	testCases := []struct {
		name               string
		unschedulablePods  []*apiv1.Pod
		experimentsManager experiments.Manager
		innerErr           errors.AutoscalerError
		withNilProcessor   bool
		wantEvents         []string
	}{
		{
			name:               "normal ScaleUp - calls ScaleUpEndProcessors after inner ScaleUp completes",
			unschedulablePods:  []*apiv1.Pod{podScope1},
			experimentsManager: experiments.NewMockManager(),
			wantEvents:         []string{"inner_scale_up", "end_processor_1", "end_processor_2"},
		},
		{
			name:              "recommendations bypass disabled (early return path) - still calls ScaleUpEndProcessors after inner ScaleUp",
			unschedulablePods: []*apiv1.Pod{podScope1},
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorRecommendationsBypassEnabledFlag: false,
			}, nil),
			wantEvents: []string{"inner_scale_up", "end_processor_1", "end_processor_2"},
		},
		{
			name:               "inner ScaleUp returns error - still calls ScaleUpEndProcessors via defer",
			unschedulablePods:  []*apiv1.Pod{podScope1},
			experimentsManager: experiments.NewMockManager(),
			innerErr:           errors.NewAutoscalerError(errors.CloudProviderError, "simulated error"),
			wantEvents:         []string{"inner_scale_up", "end_processor_1", "end_processor_2"},
		},
		{
			name:               "slice containing nil ScaleUpEndProcessor - skips nil and calls non-nil processors without panic",
			unschedulablePods:  []*apiv1.Pod{podScope1},
			experimentsManager: experiments.NewMockManager(),
			withNilProcessor:   true,
			wantEvents:         []string{"inner_scale_up", "end_processor_1", "end_processor_2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockLister := lister.NewMockCrdListerWithLabel([]crd.CRD{crdScope1}, labels.ComputeClassLabel)
			bypassTracker := NewRecommendationsBypassTracker(tc.experimentsManager)
			limiterTracker := NewScaleUpLimiterTracker(true, tc.experimentsManager)

			var events []string
			p1 := &fakeScaleUpEndProcessor{onEnd: func() { events = append(events, "end_processor_1") }}
			p2 := &fakeScaleUpEndProcessor{onEnd: func() { events = append(events, "end_processor_2") }}
			processors := []ScaleUpEndProcessor{p1, p2}
			if tc.withNilProcessor {
				processors = []ScaleUpEndProcessor{p1, nil, p2}
			}

			inner := &fakeInnerOrchestrator{
				onScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
					events = append(events, "inner_scale_up")
					if tc.innerErr != nil {
						return &status.ScaleUpStatus{Result: status.ScaleUpError}, tc.innerErr
					}
					return &status.ScaleUpStatus{Result: status.ScaleUpSuccessful}, nil
				},
			}

			wrapper := NewWrapperOrchestrator(inner, nil, mockLister, bypassTracker, limiterTracker, processors, tc.experimentsManager)
			wrapper.Initialize(&ca_context.AutoscalingContext{ProcessorCallbacks: callbacks.NewTestProcessorCallbacks()}, nil, nil, nil, taints.TaintConfig{}, nil)
			_, _ = wrapper.ScaleUp(context.Background(), tc.unschedulablePods, nil, nil, nil, false)

			assert.Equal(t, tc.wantEvents, events)
		})
	}
}

func TestWrapperOrchestrator_ScaleUp_SetPollingMode(t *testing.T) {
	crdScope1 := ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "scope-1"}}, "", false, nil, nil)
	crdDefault := ccc.NewCccCrd(&v1.ComputeClass{ObjectMeta: metav1.ObjectMeta{Name: "default"}}, "", false, nil, nil)

	podScope1 := testPod("scope-1")
	podWithoutCCC := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "basic-pod"}}

	syntheticPayPerPodScopes := []string{rules.GeneralPurposePodFamily, rules.GeneralPurposeArmPodFamily}
	syntheticPayPerPodOrder := []string{
		"SetPollingMode:general-purpose:normal",
		"SetPollingMode:general-purpose-arm:normal",
		"innerScaleUp",
		"SetPollingMode:general-purpose-arm:slow",
		"SetPollingMode:general-purpose:slow",
	}

	testCases := []struct {
		name               string
		crds               []crd.CRD
		defaultCccEnabled  bool
		nilProvider        bool
		unschedulablePods  []*apiv1.Pod
		selectedShard      *podsharding.PodShard
		experimentsManager experiments.Manager
		innerScaleUpResult status.ScaleUpResult
		innerScaleUpErr    errors.AutoscalerError
		wantScopes         []string
		wantExpectedOrder  []string
	}{
		{
			name:               "adaptive polling enabled, explicit CCC shard - sets PollingModeNormal before and PollingModeSlow after inner ScaleUp",
			crds:               []crd.CRD{crdScope1},
			unschedulablePods:  []*apiv1.Pod{podScope1},
			selectedShard:      cccShard("scope-1"),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantScopes:         []string{"scope-1"},
			wantExpectedOrder:  []string{"SetPollingMode:scope-1:normal", "innerScaleUp", "SetPollingMode:scope-1:slow"},
		},
		{
			name:               "adaptive polling enabled, mixed shard with plain pod at unschedulablePods[0] without default CCC - resolves explicit CCC scope from autoscalingCtx",
			crds:               []crd.CRD{crdScope1},
			unschedulablePods:  []*apiv1.Pod{podWithoutCCC, podScope1},
			selectedShard:      cccShard("scope-1"),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantScopes:         []string{"scope-1"},
			wantExpectedOrder:  []string{"SetPollingMode:scope-1:normal", "innerScaleUp", "SetPollingMode:scope-1:slow"},
		},
		{
			name:               "adaptive polling enabled, mixed shard with plain pod at unschedulablePods[0] and default CCC enabled - resolves explicit CCC scope instead of default",
			crds:               []crd.CRD{crdScope1, crdDefault},
			defaultCccEnabled:  true,
			unschedulablePods:  []*apiv1.Pod{podWithoutCCC, podScope1},
			selectedShard:      cccShard("scope-1"),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantScopes:         []string{"scope-1"},
			wantExpectedOrder:  []string{"SetPollingMode:scope-1:normal", "innerScaleUp", "SetPollingMode:scope-1:slow"},
		},
		{
			name:              "adaptive polling enabled, recommendations bypass disabled (early return path) - still sets PollingModeNormal before and PollingModeSlow after inner ScaleUp",
			crds:              []crd.CRD{crdScope1},
			unschedulablePods: []*apiv1.Pod{podScope1},
			selectedShard:     cccShard("scope-1"),
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorRecommendationsBypassEnabledFlag: false,
			}, nil),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantScopes:         []string{"scope-1"},
			wantExpectedOrder:  []string{"SetPollingMode:scope-1:normal", "innerScaleUp", "SetPollingMode:scope-1:slow"},
		},
		{
			name:               "adaptive polling enabled, plain shard with default CCC enabled - sets PollingModeNormal before and PollingModeSlow after inner ScaleUp for default scope",
			crds:               []crd.CRD{crdScope1, crdDefault},
			defaultCccEnabled:  true,
			unschedulablePods:  []*apiv1.Pod{podWithoutCCC},
			selectedShard:      plainShard("basic"),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantScopes:         []string{"default"},
			wantExpectedOrder:  []string{"SetPollingMode:default:normal", "innerScaleUp", "SetPollingMode:default:slow"},
		},
		{
			name:               "adaptive polling enabled, plain shard without default CCC - activates synthetic Pay-Per-Pod scopes before and demotes after inner ScaleUp",
			crds:               []crd.CRD{crdScope1},
			unschedulablePods:  []*apiv1.Pod{podWithoutCCC},
			selectedShard:      plainShard("basic"),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantScopes:         syntheticPayPerPodScopes,
			wantExpectedOrder:  syntheticPayPerPodOrder,
		},
		{
			name:               "adaptive polling enabled, provisioning request shard - skipped without calling SetPollingMode",
			crds:               []crd.CRD{crdScope1},
			unschedulablePods:  []*apiv1.Pod{podScope1},
			selectedShard:      provReqShard("scope-1"),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantExpectedOrder:  []string{"innerScaleUp"},
		},
		{
			name:               "adaptive polling enabled, no selected shard in autoscalingCtx - does not call SetPollingMode",
			crds:               []crd.CRD{crdScope1},
			unschedulablePods:  []*apiv1.Pod{podScope1},
			selectedShard:      nil,
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantExpectedOrder:  []string{"innerScaleUp"},
		},
		{
			name:              "adaptive polling disabled via bool flag - does not call SetPollingMode",
			crds:              []crd.CRD{crdScope1},
			unschedulablePods: []*apiv1.Pod{podScope1},
			selectedShard:     plainShard("ignored"),
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag: false,
			}, nil),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantExpectedOrder:  []string{"innerScaleUp"},
		},
		{
			name:              "adaptive polling disabled via min CA version flag - does not call SetPollingMode",
			crds:              []crd.CRD{crdScope1, crdDefault},
			defaultCccEnabled: true,
			unschedulablePods: []*apiv1.Pod{podWithoutCCC, podScope1},
			selectedShard:     cccShard("scope-1"),
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag:      true,
				experiments.FlexAdvisorAdaptivePollingMinCAVersionFlag: false,
			}, nil),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantExpectedOrder:  []string{"innerScaleUp"},
		},
		{
			name:              "adaptive polling disabled, plain pod without default CCC - does not call SetPollingMode",
			crds:              []crd.CRD{crdScope1},
			unschedulablePods: []*apiv1.Pod{podWithoutCCC},
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag: false,
			}, nil),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantExpectedOrder:  []string{"innerScaleUp"},
		},
		{
			name:              "adaptive polling disabled, empty unschedulable pods - does not call SetPollingMode",
			crds:              []crd.CRD{crdScope1},
			unschedulablePods: nil,
			selectedShard:     cccShard("scope-1"),
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{
				experiments.FlexAdvisorAdaptivePollingEnabledFlag: false,
			}, nil),
			innerScaleUpResult: status.ScaleUpNotTried,
			wantExpectedOrder:  []string{"innerScaleUp"},
		},
		{
			name:               "inner ScaleUp returns error - still sets PollingModeSlow via defer",
			crds:               []crd.CRD{crdScope1},
			unschedulablePods:  []*apiv1.Pod{podScope1},
			selectedShard:      cccShard("scope-1"),
			innerScaleUpResult: status.ScaleUpError,
			innerScaleUpErr:    errors.NewAutoscalerError(errors.CloudProviderError, "simulated error"),
			wantScopes:         []string{"scope-1"},
			wantExpectedOrder:  []string{"SetPollingMode:scope-1:normal", "innerScaleUp", "SetPollingMode:scope-1:slow"},
		},
		{
			name:               "nil provider - does not panic",
			crds:               []crd.CRD{crdScope1},
			nilProvider:        true,
			unschedulablePods:  []*apiv1.Pod{podScope1},
			selectedShard:      cccShard("scope-1"),
			innerScaleUpResult: status.ScaleUpSuccessful,
			wantExpectedOrder:  []string{"innerScaleUp"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := tc.experimentsManager
			if manager == nil {
				manager = experiments.NewMockManager()
			}
			var callOrder []string
			mockProvider := new(instanceavailability.MockProvider)
			var provider instanceavailability.Provider
			if !tc.nilProvider {
				for _, scope := range tc.wantScopes {
					mockProvider.On("SetPollingMode", scope, PollingModeNormal).Run(func(args mock.Arguments) {
						callOrder = append(callOrder, "SetPollingMode:"+args.String(0)+":normal")
					}).Once()
					mockProvider.On("SetPollingMode", scope, PollingModeSlow).Run(func(args mock.Arguments) {
						callOrder = append(callOrder, "SetPollingMode:"+args.String(0)+":slow")
					}).Once()
				}
				provider = mockProvider
			}

			mockLister := lister.NewMockCrdListerWithLabel(tc.crds, labels.ComputeClassLabel)
			if tc.defaultCccEnabled {
				mockLister.SetDefaultCrdName("default")
			}

			inner := &fakeInnerOrchestrator{
				onScaleUp: func(_ RecommendationsBypassTracker, _ ScaleUpLimiterTracker) (*status.ScaleUpStatus, errors.AutoscalerError) {
					callOrder = append(callOrder, "innerScaleUp")
					return &status.ScaleUpStatus{Result: tc.innerScaleUpResult}, tc.innerScaleUpErr
				},
			}

			autoscalingCtx := &ca_context.AutoscalingContext{
				ProcessorCallbacks: callbacks.NewTestProcessorCallbacks(),
			}
			if tc.selectedShard != nil {
				podsharding.TestSetPodShard(autoscalingCtx, tc.selectedShard)
			}

			wrapper := NewWrapperOrchestrator(
				inner,
				provider,
				mockLister,
				NewRecommendationsBypassTracker(manager),
				NewScaleUpLimiterTracker(true, manager),
				nil,
				manager,
			)
			wrapper.Initialize(autoscalingCtx, nil, nil, nil, taints.TaintConfig{}, nil)
			_, _ = wrapper.ScaleUp(context.Background(), tc.unschedulablePods, nil, nil, nil, false)

			assert.Equal(t, tc.wantExpectedOrder, callOrder)
			mockProvider.AssertExpectations(t)
		})
	}
}

func TestWrapperOrchestrator_InitializeAndMinSizeDelegation(t *testing.T) {
	inner := &fakeInnerOrchestrator{}
	wrapper := NewWrapperOrchestrator(inner, nil, nil, nil, nil, nil, nil)
	autoscalingCtx := &ca_context.AutoscalingContext{}

	wrapper.Initialize(autoscalingCtx, nil, nil, nil, taints.TaintConfig{}, nil)
	assert.Same(t, autoscalingCtx, wrapper.autoscalingCtx)
	assert.True(t, inner.initialized)

	scaleUpStatus, err := wrapper.ScaleUpToNodeGroupMinSize(context.Background(), nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, status.ScaleUpNotTried, scaleUpStatus.Result)
	assert.True(t, inner.minSizeCalled)
}
