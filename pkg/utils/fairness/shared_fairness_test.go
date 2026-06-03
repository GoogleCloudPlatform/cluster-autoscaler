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
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
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
			want1:    []bool{true, false, false, false, false, false, false, false, false, false, true},
			want2:    []bool{false, false, false, false, false, true, false, false, false, false, false},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			manager := NewSharedEnforcerManager(tc.maxLoops)
			p1 := manager.CreateEnforcer("p1")
			p2 := manager.CreateEnforcer("p2")

			for i := 0; i < len(tc.want1); i++ {
				got1 := p1.Admit(tc.unschedulable)
				got2 := p2.Admit(tc.unschedulable)
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
			want:     []bool{true, false, false, false, false, true},
		},
		{
			name: "pending pod unhelpable",
			unschedulable: []*apiv1.Pod{
				unhelpablePod("p1"),
			},
			maxLoops: 10, // admit every 10/2==5 loops
			want:     []bool{true, false, false, false, false, true, false, false, false, false, true},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			manager := NewSharedEnforcerManager(tc.maxLoops)
			defrag := manager.CreateEnforcer("defrag")

			for i, want := range tc.want {
				got := defrag.Admit(tc.unschedulable)
				assert.Equal(t, want, got, "Iteration %d", i)
			}
		})
	}
}
