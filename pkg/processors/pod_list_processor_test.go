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

package processors

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	metrics_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/metrics"
)

func TestGetSchedulable(t *testing.T) {
	var p = test.BuildTestPod("p1", 100, 1)
	var p2 = test.BuildTestPod("p2", 200, 2)
	var p3 = test.BuildTestPod("p3", 200, 2, func(p *v1.Pod) { p.Namespace = metav1.NamespaceSystem })

	testCases := []struct {
		desc               string
		unchedulableBefore []*v1.Pod
		unchedulableAfter  []*v1.Pod
		wantSchedulable    []*v1.Pod
	}{
		{
			desc:               "No schedulable",
			unchedulableBefore: []*v1.Pod{p, p2, p3},
			unchedulableAfter:  []*v1.Pod{p2, p, p3},
			wantSchedulable:    []*v1.Pod{},
		},
		{
			desc:               "All schedulable",
			unchedulableBefore: []*v1.Pod{p, p2, p3},
			unchedulableAfter:  []*v1.Pod{},
			wantSchedulable:    []*v1.Pod{p, p2, p3},
		},
		{
			desc:               "One schedulable",
			unchedulableBefore: []*v1.Pod{p, p2, p3},
			unchedulableAfter:  []*v1.Pod{p, p3},
			wantSchedulable:    []*v1.Pod{p2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			schedulable := getSchedulable(tc.unchedulableBefore, tc.unchedulableAfter)
			if diff := cmp.Diff(tc.wantSchedulable, schedulable); diff != "" {
				t.Errorf("GetSchedulable returned unexpected diff for schedulable (-want +got):\n%s", diff)
			}
		})
	}
}

type mockScaleToZeroProcessor struct {
	called bool
}

func (m *mockScaleToZeroProcessor) Process(context *context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	m.called = true
	return unschedulablePods, nil
}

func (m *mockScaleToZeroProcessor) Drainable(drainContext *drainability.DrainContext, pod *v1.Pod, node *framework.NodeInfo) drainability.Status {
	return drainability.NewUndefinedStatus()
}

func (m *mockScaleToZeroProcessor) Name() string {
	return "mockScaleToZeroProcessor"
}

type mockDefaultPodListProcessor struct {
	called                  bool
	scaleToZeroCalledBefore bool
	scaleToZero             *mockScaleToZeroProcessor
}

func (m *mockDefaultPodListProcessor) Process(context *context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	m.called = true
	m.scaleToZeroCalledBefore = m.scaleToZero.called
	return unschedulablePods, nil
}

func (m *mockDefaultPodListProcessor) CleanUp() {}

func TestScaleToZeroLateRun(t *testing.T) {
	testCases := []struct {
		name                   string
		scaleToZeroLateRunFlag bool
		expectScaleToZeroEarly bool
	}{
		{
			name:                   "ScaleToZero late run disabled",
			scaleToZeroLateRunFlag: false,
			expectScaleToZeroEarly: true,
		},
		{
			name:                   "ScaleToZero late run enabled",
			scaleToZeroLateRunFlag: true,
			expectScaleToZeroEarly: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			boolFlags := map[string]bool{
				experiments.ScaleToZeroLateRunFlag: tc.scaleToZeroLateRunFlag,
			}
			em := experiments.NewMockManagerWithOptions(version.Version{}, boolFlags, map[string]string{})

			scaleToZero := &mockScaleToZeroProcessor{}
			defaultPodListProcessor := &mockDefaultPodListProcessor{scaleToZero: scaleToZero}

			processor := &GkeInternalPodListProcessor{
				scaleToZeroProcessor:      scaleToZero,
				defaultPodListerProcessor: defaultPodListProcessor,
				experimentsManager:        em,
				podStatusAggregator:       metrics_processors.NewPodStatusAggregator(),
			}

			processor.Process(&context.AutoscalingContext{}, []*v1.Pod{})

			if !scaleToZero.called {
				t.Errorf("Expected scaleToZeroProcessor to be called")
			}
			if !defaultPodListProcessor.called {
				t.Errorf("Expected defaultPodListProcessor to be called")
			}

			if defaultPodListProcessor.scaleToZeroCalledBefore != tc.expectScaleToZeroEarly {
				t.Errorf("Expected scaleToZeroProcessor called before defaultPodListProcessor: %v, got: %v", tc.expectScaleToZeroEarly, defaultPodListProcessor.scaleToZeroCalledBefore)
			}
		})
	}
}
