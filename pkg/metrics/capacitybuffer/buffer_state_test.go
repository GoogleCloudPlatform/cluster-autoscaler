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

package capacitybuffer

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// one step of the test - it does not represent a CA loop
// specify either processInjected or processScaleUp in one step, not both
type stepParams struct {
	minute         int
	injected       *injectedParams
	scaleUp        *scaleUpParams
	wantReport     *reactionsToReport // nil report will assert empty report
	wantUnreported int
}

type injectedParams struct {
	injected int
}

type scaleUpParams struct {
	schedulable   int
	scaleUp       int
	unschedulable int
	await         int
}

var startTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestBufferStateScenarios(t *testing.T) {
	// Fixed timeout for all tests
	timeout := 21 * time.Minute

	tests := []struct {
		name  string
		steps []stepParams
	}{
		{
			name: "Measuring time since the pod got first injected",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 1}, wantUnreported: 1},
				{minute: 1, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{
					minute:  2,
					scaleUp: &scaleUpParams{schedulable: 2},
					wantReport: &reactionsToReport{
						noActionNeeded: minutesToTimes(0, 1),
					},
				},
			},
		},
		{
			name: "If reporting injected pod is skipped, reactions are still processed",
			steps: []stepParams{
				{
					minute:         0,
					scaleUp:        &scaleUpParams{await: 1, schedulable: 2, scaleUp: 3, unschedulable: 4},
					wantUnreported: 1,
					wantReport: &reactionsToReport{
						noActionNeeded: minutesToTimes(0, 0),
						scaleUp:        minutesToTimes(0, 0, 0),
						unhelpable:     minutesToTimes(0, 0, 0, 0),
					},
				},
			},
		},
		{
			name: "Report less injected pods removes them from the list only during ProcessScaleUpStatus",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{minute: 1, injected: &injectedParams{injected: 1}, wantUnreported: 2},
				{
					minute:         2,
					scaleUp:        &scaleUpParams{schedulable: 1},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						noActionNeeded: minutesToTimes(0),
					},
				},
			},
		},
		{
			name: "All pods except awaiting evaluation are immediately reported",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 10}, wantUnreported: 10},
				{
					minute:         1,
					scaleUp:        &scaleUpParams{await: 1, schedulable: 2, scaleUp: 3, unschedulable: 4},
					wantUnreported: 1,
					wantReport: &reactionsToReport{
						noActionNeeded: minutesToTimes(0, 0),
						scaleUp:        minutesToTimes(0, 0, 0),
						unhelpable:     minutesToTimes(0, 0, 0, 0),
					},
				},
			},
		},
		{
			name: "Unschedulable pods can be promoted from awaiting evaluation",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 1}, wantUnreported: 1},
				{minute: 1, scaleUp: &scaleUpParams{await: 1}, wantUnreported: 1},
				{minute: 2, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{
					minute:         3,
					scaleUp:        &scaleUpParams{unschedulable: 2},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						unhelpable: minutesToTimes(0, 2),
					},
				},
			},
		},
		{
			name: "Pods triggering scale up can be promoted from unschedulable and awaiting evaluation",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{
					minute:         1,
					scaleUp:        &scaleUpParams{unschedulable: 1, await: 1},
					wantUnreported: 1,
					wantReport: &reactionsToReport{
						unhelpable: minutesToTimes(0),
					},
				},
				{minute: 2, injected: &injectedParams{injected: 3}, wantUnreported: 2},
				{
					minute:         3,
					scaleUp:        &scaleUpParams{scaleUp: 3},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						scaleUp: minutesToTimes(0, 2),
					},
				},
			},
		},
		{
			name: "Schedulable pods can be promoted from unschedulable and awaiting evaluation",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{
					minute:         1,
					scaleUp:        &scaleUpParams{unschedulable: 1, await: 1},
					wantUnreported: 1,
					wantReport: &reactionsToReport{
						unhelpable: minutesToTimes(0),
					},
				},
				{minute: 2, injected: &injectedParams{injected: 3}, wantUnreported: 2},
				{
					minute:         3,
					scaleUp:        &scaleUpParams{schedulable: 3},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						noActionNeeded: minutesToTimes(0, 2),
					},
				},
			},
		},
		{
			name: "Consecutive unschedulable are not reported separately",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 1}, wantUnreported: 1},
				{
					minute:         1,
					scaleUp:        &scaleUpParams{unschedulable: 1},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						unhelpable: minutesToTimes(0),
					},
				},
				{minute: 2, injected: &injectedParams{injected: 1}, wantUnreported: 0},
				{
					minute:         3,
					scaleUp:        &scaleUpParams{unschedulable: 1},
					wantUnreported: 0,
				},
			},
		},
		{
			name: "Consecutive scale-ups are reported separately",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 1}, wantUnreported: 1},
				{
					minute:         1,
					scaleUp:        &scaleUpParams{scaleUp: 1},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						scaleUp: minutesToTimes(0),
					},
				},
				{minute: 2, injected: &injectedParams{injected: 1}, wantUnreported: 0},
				{
					minute:         3,
					scaleUp:        &scaleUpParams{scaleUp: 1},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						scaleUp: minutesToTimes(2),
					},
				},
			},
		},
		{
			name: "Consecutive schedulable are not reported separately",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 1}, wantUnreported: 1},
				{
					minute:         1,
					scaleUp:        &scaleUpParams{schedulable: 1},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						noActionNeeded: minutesToTimes(0),
					},
				},
				{minute: 2, injected: &injectedParams{injected: 1}, wantUnreported: 0},
				{
					minute:         3,
					scaleUp:        &scaleUpParams{schedulable: 1},
					wantUnreported: 0,
				},
			},
		},
		{
			name: "Pods triggering scale up are not reported again when they become schedulable",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 1}, wantUnreported: 1},
				{
					minute:         1,
					scaleUp:        &scaleUpParams{scaleUp: 1},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						scaleUp: minutesToTimes(0),
					},
				},
				{minute: 2, injected: &injectedParams{injected: 1}, wantUnreported: 0},
				{
					minute:         3,
					scaleUp:        &scaleUpParams{schedulable: 1},
					wantUnreported: 0,
				},
			},
		},
		{
			name: "Pods are processed in order: schedulable, scale up, unschedulable, awaiting evaluation",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 1}, wantUnreported: 1},
				{minute: 1, scaleUp: &scaleUpParams{await: 1}, wantUnreported: 1},
				{minute: 2, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{minute: 3, scaleUp: &scaleUpParams{await: 2}, wantUnreported: 2},
				{minute: 4, injected: &injectedParams{injected: 3}, wantUnreported: 3},
				{minute: 5, scaleUp: &scaleUpParams{await: 3}, wantUnreported: 3},
				{minute: 6, injected: &injectedParams{injected: 4}, wantUnreported: 4},
				{minute: 7, scaleUp: &scaleUpParams{await: 4}, wantUnreported: 4},
				{minute: 8, injected: &injectedParams{injected: 4}, wantUnreported: 4},
				{
					minute:         9,
					scaleUp:        &scaleUpParams{schedulable: 1, scaleUp: 1, unschedulable: 1, await: 1},
					wantUnreported: 1,
					wantReport: &reactionsToReport{
						noActionNeeded: minutesToTimes(0),
						scaleUp:        minutesToTimes(2),
						unhelpable:     minutesToTimes(4),
					},
				},
			},
		},
		{
			name: "Timeouts are reported after injection",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 1}, wantUnreported: 1},
				{minute: 1, scaleUp: &scaleUpParams{await: 1}, wantUnreported: 1},
				{minute: 10, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{minute: 11, scaleUp: &scaleUpParams{await: 2}, wantUnreported: 2},
				{
					minute:         21,
					injected:       &injectedParams{injected: 2}, // pod from minute 0 should time out here
					wantUnreported: 1,
					wantReport: &reactionsToReport{
						timeouts: minutesToTimes(0),
					},
				},
			},
		},
		{
			name: "Timeouts are not reported during scale up status processing",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 1}, wantUnreported: 1},
				{minute: 1, scaleUp: &scaleUpParams{await: 1}, wantUnreported: 1},
				{minute: 10, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{minute: 11, scaleUp: &scaleUpParams{await: 2}, wantUnreported: 2},
				{minute: 20, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{
					minute:         21,
					scaleUp:        &scaleUpParams{schedulable: 2},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						noActionNeeded: minutesToTimes(0, 10),
					},
				},
			},
		},
		{
			name: "Pods are removed after other actions",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 3}, wantUnreported: 3},
				{minute: 1, scaleUp: &scaleUpParams{await: 3}, wantUnreported: 3}, // these should be reported (in minute 5)
				{minute: 2, injected: &injectedParams{injected: 4}, wantUnreported: 4},
				{minute: 3, scaleUp: &scaleUpParams{await: 4}, wantUnreported: 4}, // 1 new pod - this should be deleted (in minute 5)
				{minute: 4, injected: &injectedParams{injected: 3}, wantUnreported: 4},
				{
					minute:         5,
					scaleUp:        &scaleUpParams{schedulable: 1, scaleUp: 1, unschedulable: 1},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						noActionNeeded: minutesToTimes(0),
						scaleUp:        minutesToTimes(0),
						unhelpable:     minutesToTimes(0),
					},
				},
			},
		},
		{
			name: "Pods can transition between awaiting evaluation and unschedulable",
			steps: []stepParams{
				{minute: 0, injected: &injectedParams{injected: 2}, wantUnreported: 2},
				{
					minute:         1,
					scaleUp:        &scaleUpParams{unschedulable: 2},
					wantUnreported: 0,
					wantReport: &reactionsToReport{
						unhelpable: minutesToTimes(0, 0),
					},
				},
				{minute: 2, injected: &injectedParams{injected: 2}, wantUnreported: 0},
				{minute: 3, scaleUp: &scaleUpParams{unschedulable: 1, await: 1}, wantUnreported: 0},
				{minute: 4, injected: &injectedParams{injected: 2}, wantUnreported: 0},
				{
					minute:         5,
					scaleUp:        &scaleUpParams{unschedulable: 2}, // already reported in minute 1
					wantUnreported: 0,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bs := NewBufferState()
			var gotReport reactionsToReport

			for _, params := range tc.steps {
				now := startTime.Add(time.Duration(params.minute) * time.Minute)

				if params.injected != nil {
					gotReport = bs.ProcessInjectedPods(params.injected.injected, now, timeout)
				}

				if params.scaleUp != nil {
					gotReport = bs.ProcessScaleUpStatus(
						params.scaleUp.schedulable,
						params.scaleUp.scaleUp,
						params.scaleUp.unschedulable,
						params.scaleUp.await,
						now,
					)
				}

				wantReport := &reactionsToReport{}
				if params.wantReport != nil {
					wantReport = params.wantReport
				}
				if diff := cmp.Diff(wantReport, &gotReport, cmp.AllowUnexported(reactionsToReport{}), cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("ProcessScaleUpStatus() mismatch (-want +got):\n%s", diff)
				}

				if bs.unreportedQueue.Len() != params.wantUnreported {
					t.Errorf("unreportedQueue.Len() mismatch: got %d, want %d", bs.unreportedQueue.Len(), params.wantUnreported)
				}
			}
		})
	}
}

func minutesToTimes(minutes ...int) []time.Time {
	result := make([]time.Time, len(minutes))
	for i, minute := range minutes {
		result[i] = startTime.Add(time.Duration(minute) * time.Minute)
	}
	return result
}
