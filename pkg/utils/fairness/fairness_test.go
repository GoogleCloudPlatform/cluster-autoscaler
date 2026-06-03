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
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/annotator"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/klog/v2"
	. "k8s.io/utils/clock/testing"
)

func TestAdmit(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		unschedulable []*apiv1.Pod
		timeStep      time.Duration
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
			want: []bool{false, false, false},
		},
		{
			name: "pending pod unhelpable",
			unschedulable: []*apiv1.Pod{
				unhelpablePod("p1"),
			},
			want: []bool{true, false, true, false},
		},
		{
			name: "pending pod eventually allows defrag",
			unschedulable: []*apiv1.Pod{
				unhelpablePod("p1"),
				test.BuildTestPod("p2", 1, 1000),
				unhelpablePod("p3"),
			},
			timeStep: 1 * time.Minute,
			want: []bool{
				false, false, false, false, false, true,
				false, false, false, false, false, true,
				false, false, false, false, false, true,
			},
		},
		{
			name: "slow defrag doesn't starve other logic",
			unschedulable: []*apiv1.Pod{
				unhelpablePod("p1"),
				test.BuildTestPod("p2", 1, 1000),
				unhelpablePod("p3"),
			},
			timeStep: 10 * time.Minute,
			want:     []bool{true, false, true, false, true, false},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Arbitrary non-zero date.
			clock := NewFakePassiveClock(time.Date(1985, time.October, 21, 9, 0, 0, 0, time.UTC))
			fe := newEnforcerWithClock("defrag", 5*time.Minute, clock)
			for i, want := range tc.want {
				clock.SetTime(clock.Now().Add(tc.timeStep))
				got := fe.Admit(tc.unschedulable)
				klog.Infof("[%v] i = %d, got = %v", tc.name, i, got)
				assert.Equal(t, want, got)
			}
		})
	}
}

func unhelpablePod(name string) *apiv1.Pod {
	p := test.BuildTestPod(name, 1, 1000)
	p.Annotations[annotator.UnhelpableUntilAnnotation] = annotator.UnhelpableForever
	return p
}
