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
	"errors"
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
	if m.scaleToZero != nil {
		m.scaleToZeroCalledBefore = m.scaleToZero.called
	}
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

type mockDefragProcessor struct {
	called          bool
	cleanedUp       bool
	pickedCandidate bool
	podsToReturn    []*v1.Pod
	err             error
}

func (m *mockDefragProcessor) Process(context *context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	m.called = true
	if !m.pickedCandidate {
		return unschedulablePods, m.err
	}
	if m.podsToReturn != nil {
		return m.podsToReturn, m.err
	}
	return unschedulablePods, m.err
}

func (m *mockDefragProcessor) DefragPickedCandidate() bool {
	return m.pickedCandidate
}

func (m *mockDefragProcessor) CleanUp() {
	m.cleanedUp = true
}

type mockCSNBufferConsumptionProcessor struct {
	called       bool
	cleanedUp    bool
	receivedPods []*v1.Pod
	podsToReturn []*v1.Pod
	err          error
}

func (m *mockCSNBufferConsumptionProcessor) Process(context *context.AutoscalingContext, unschedulablePods []*v1.Pod) ([]*v1.Pod, error) {
	m.called = true
	m.receivedPods = unschedulablePods
	if m.podsToReturn != nil {
		return m.podsToReturn, m.err
	}
	return unschedulablePods, m.err
}

func (m *mockCSNBufferConsumptionProcessor) CleanUp() {
	m.cleanedUp = true
}

func TestNewGkeInternalPodListProcessorNil(t *testing.T) {
	processor := NewGkeInternalPodListProcessor(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if processor.defragProcessor != nil {
		t.Errorf("Expected defragProcessor to be strictly nil")
	}
	if processor.csnBufferConsumptionProcessor != nil {
		t.Errorf("Expected csnBufferConsumptionProcessor to be strictly nil")
	}
}

func TestDefragAndCSNBufferConsumptionInPodListProcessor(t *testing.T) {
	p1 := test.BuildTestPod("p1", 100, 1)
	p2 := test.BuildTestPod("p2", 200, 2)

	initialPods := []*v1.Pod{p1}

	testCases := []struct {
		name                         string
		defragPickedCandidate        bool
		defragErr                    error
		defragReturnPods             []*v1.Pod
		csnErr                       error
		expectCSNReceivedPods        []*v1.Pod
		expectErr                    bool
		expectUnschedulablePodsCount int
	}{
		{
			name:                         "Defrag candidate not picked, CSN receives unschedulable pods",
			defragPickedCandidate:        false,
			defragReturnPods:             []*v1.Pod{p1, p2},
			expectCSNReceivedPods:        []*v1.Pod{p1},
			expectUnschedulablePodsCount: 1,
		},
		{
			name:                         "Defrag picked candidate, CSN receives nil pods",
			defragPickedCandidate:        true,
			defragReturnPods:             []*v1.Pod{p1, p2},
			expectCSNReceivedPods:        nil,
			expectUnschedulablePodsCount: 2,
		},
		{
			name:                         "Defrag fails with error, defragPickedCandidate treated as false",
			defragPickedCandidate:        true, // Set on mock to ensure DefragPickedCandidate() isn't queried on error
			defragErr:                    errors.New("defrag failed"),
			defragReturnPods:             []*v1.Pod{p1, p2},
			expectCSNReceivedPods:        []*v1.Pod{p1},
			expectUnschedulablePodsCount: 1,
		},
		{
			name:                  "CSN processor fails with error",
			defragPickedCandidate: false,
			defragReturnPods:      []*v1.Pod{p1},
			csnErr:                errors.New("csn failed"),
			expectCSNReceivedPods: []*v1.Pod{p1},
			expectErr:             true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defragMock := &mockDefragProcessor{
				pickedCandidate: tc.defragPickedCandidate,
				err:             tc.defragErr,
				podsToReturn:    tc.defragReturnPods,
			}
			csnMock := &mockCSNBufferConsumptionProcessor{
				err: tc.csnErr,
			}
			defaultPodListProcessor := &mockDefaultPodListProcessor{}

			processor := &GkeInternalPodListProcessor{
				defaultPodListerProcessor:     defaultPodListProcessor,
				defragProcessor:               defragMock,
				csnBufferConsumptionProcessor: csnMock,
				podStatusAggregator:           metrics_processors.NewPodStatusAggregator(),
				experimentsManager:            experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{}, map[string]string{}),
			}

			res, err := processor.Process(&context.AutoscalingContext{}, initialPods)
			if (err != nil) != tc.expectErr {
				t.Fatalf("Expected error: %v, got: %v", tc.expectErr, err)
			}
			if tc.expectErr {
				return
			}

			if !defragMock.called {
				t.Errorf("Expected defragProcessor to be called")
			}
			if !csnMock.called {
				t.Errorf("Expected csnBufferConsumptionProcessor to be called")
			}
			if diff := cmp.Diff(tc.expectCSNReceivedPods, csnMock.receivedPods); diff != "" {
				t.Errorf("csnBufferConsumptionProcessor received unexpected pods diff (-want +got):\n%s", diff)
			}
			if len(res) != tc.expectUnschedulablePodsCount {
				t.Errorf("Expected %d unschedulable pods returned, got %d", tc.expectUnschedulablePodsCount, len(res))
			}
		})
	}
}

func TestGkeInternalPodListProcessorCleanUp(t *testing.T) {
	defragMock := &mockDefragProcessor{}
	csnMock := &mockCSNBufferConsumptionProcessor{}
	defaultPodListProcessor := &mockDefaultPodListProcessor{}

	processor := &GkeInternalPodListProcessor{
		defaultPodListerProcessor:     defaultPodListProcessor,
		defragProcessor:               defragMock,
		csnBufferConsumptionProcessor: csnMock,
	}

	processor.CleanUp()

	if !defragMock.cleanedUp {
		t.Errorf("Expected defragProcessor.CleanUp to be called")
	}
	if !csnMock.cleanedUp {
		t.Errorf("Expected csnBufferConsumptionProcessor.CleanUp to be called")
	}
}
