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

package fairness

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestSharedAdmit(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		unschedulable []*apiv1.Pod
		maxLoops      int
		want1         []bool
		want2         []bool
	}{
		{
			name:          "fair turns: P1 admits then P2 waits, and vice-versa",
			unschedulable: []*apiv1.Pod{},
			maxLoops:      0,
			want1:         []bool{true, false, true, false},
			want2:         []bool{false, true, false, true},
		},
		{
			name: "both waiting because of unschedulable pods",
			unschedulable: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1000),
			},
			maxLoops: 5,
			want1:    []bool{false, false, false, false, true, false, false, false, false, false, false},
			want2:    []bool{false, false, false, false, false, false, false, false, false, true, false},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			manager := NewSharedEnforcerManager(tc.maxLoops, fakeUnhelpableTracker{})
			p1 := manager.CreateEnforcer("p1")
			p2 := manager.CreateEnforcer("p2")

			for i := 0; i < len(tc.want1); i++ {
				manager.StartLoop()
				got1 := p1.Admit(tc.unschedulable, true)
				got2 := p2.Admit(tc.unschedulable, true)
				assert.Equal(t, tc.want1[i], got1, "processor 1 iteration %d", i)
				assert.Equal(t, tc.want2[i], got2, "processor 2 iteration %d", i)
			}
		})
	}
}

func TestSharedAdmitSingleProcessor(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		unschedulable []*apiv1.Pod
		maxLoops      int
		want          []bool
	}{
		{
			name:          "no pending pods",
			unschedulable: []*apiv1.Pod{},
			want:          []bool{true, true, true},
		},
		{
			name: "pending pod",
			unschedulable: []*apiv1.Pod{
				test.BuildTestPod("p1", 1, 1000),
			},
			maxLoops: 5,
			want:     []bool{false, false, false, false, true, false},
		},
		{
			name: "pending pod unhelpable",
			unschedulable: []*apiv1.Pod{
				unhelpablePod("p1"),
			},
			maxLoops: 10, // admit every 10/2==5 loops
			want:     []bool{false, false, false, false, true, false, false, false, false, true, false},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			manager := NewSharedEnforcerManager(tc.maxLoops, fakeUnhelpableTracker{})
			defrag := manager.CreateEnforcer("defrag")

			for i, want := range tc.want {
				manager.StartLoop()
				got := defrag.Admit(tc.unschedulable, true)
				assert.Equal(t, want, got, "Iteration %d", i)
			}
		})
	}
}

// TestSharedAdmitMinCapacityAndDefrag mirrors the production setup: defrag is
// created first, but the min capacity processor runs before it in each loop.
func TestSharedAdmitMinCapacityAndDefrag(t *testing.T) {
	t.Parallel()
	pendingPods := []*apiv1.Pod{test.BuildTestPod("p1", 1, 1000)}
	type loop struct {
		minCapacityWantsToRun bool
		wantMinCapacity       bool
		wantDefrag            bool
	}
	testCases := []struct {
		name          string
		unschedulable []*apiv1.Pod
		maxLoops      int
		loops         []loop
	}{
		{
			name: "idle min capacity: defrag admitted every loop",
			loops: []loop{
				{wantDefrag: true},
				{wantDefrag: true},
				{wantDefrag: true},
				{wantDefrag: true},
			},
		},
		{
			name:          "idle min capacity with pending pods: defrag admitted every maxLoops",
			unschedulable: pendingPods,
			maxLoops:      3,
			loops: []loop{
				{},
				{},
				{wantDefrag: true},
				{},
				{},
				{wantDefrag: true},
				{},
			},
		},
		{
			name: "both with work: turns alternate",
			loops: []loop{
				{minCapacityWantsToRun: true, wantDefrag: true},
				{minCapacityWantsToRun: true, wantMinCapacity: true},
				{minCapacityWantsToRun: true, wantDefrag: true},
				{minCapacityWantsToRun: true, wantMinCapacity: true},
			},
		},
		{
			name: "min capacity work comes and goes: neither is starved",
			loops: []loop{
				{wantDefrag: true},
				{minCapacityWantsToRun: true, wantMinCapacity: true},
				{minCapacityWantsToRun: true, wantDefrag: true},
				{minCapacityWantsToRun: true, wantMinCapacity: true},
				{wantDefrag: true},
				{wantDefrag: true},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			manager := NewSharedEnforcerManager(tc.maxLoops, fakeUnhelpableTracker{})
			defrag := manager.CreateEnforcer("defrag")
			minCapacity := manager.CreateEnforcer("min-capacity")

			for i, l := range tc.loops {
				manager.StartLoop()
				gotMinCapacity := minCapacity.Admit(tc.unschedulable, l.minCapacityWantsToRun)
				gotDefrag := defrag.Admit(tc.unschedulable, true)

				if gotMinCapacity != l.wantMinCapacity {
					t.Errorf("loop %d: minCapacity.Admit(%t) = %t, want %t", i, l.minCapacityWantsToRun, gotMinCapacity, l.wantMinCapacity)
				}
				if gotDefrag != l.wantDefrag {
					t.Errorf("loop %d: defrag.Admit(true) = %t, want %t", i, gotDefrag, l.wantDefrag)
				}
			}
		})
	}
}

// TestSharedAdmitAtMostOneProcessorPerLoop checks that StartLoop aligns the
// loop for every processor, whichever order they run in: when both always
// want to run, exactly one of them is admitted in each loop.
func TestSharedAdmitAtMostOneProcessorPerLoop(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name               string
		runInCreationOrder bool
	}{
		{name: "processors run in creation order", runInCreationOrder: true},
		{name: "processors run in reverse creation order", runInCreationOrder: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			manager := NewSharedEnforcerManager(0, fakeUnhelpableTracker{})
			enforcers := []FairnessEnforcer{manager.CreateEnforcer("p1"), manager.CreateEnforcer("p2")}
			if !tc.runInCreationOrder {
				slices.Reverse(enforcers)
			}

			for i := range 4 {
				manager.StartLoop()
				admitted := 0
				for _, e := range enforcers {
					if e.Admit(nil, true) {
						admitted++
					}
				}

				if admitted != 1 {
					t.Errorf("loop %d: %d processors admitted, want 1", i, admitted)
				}
			}
		})
	}
}

// TestSharedAdmitWithoutWorkAfterAdmissionKeepsNextTurn checks that a
// processor with nothing to do cannot pass its turn in a loop where another
// processor was already admitted: that turn belongs to the next loop.
func TestSharedAdmitWithoutWorkAfterAdmissionKeepsNextTurn(t *testing.T) {
	t.Parallel()
	manager := NewSharedEnforcerManager(0, fakeUnhelpableTracker{})
	p1 := manager.CreateEnforcer("p1")
	p2 := manager.CreateEnforcer("p2")
	manager.StartLoop()
	if !p1.Admit(nil, true) {
		t.Fatal("p1.Admit(nil, true) in the first loop = false, want true")
	}

	p2.Admit(nil, false)
	manager.StartLoop()
	got := p2.Admit(nil, true)

	if !got {
		t.Error("p2.Admit(nil, true) in the second loop = false, want true")
	}
}
