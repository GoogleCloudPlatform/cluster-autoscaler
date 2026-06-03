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

package taskqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockTaskQueueEventType string

const (
	awaitStarted mockTaskQueueEventType = "awaitStarted"
	finalise     mockTaskQueueEventType = "finalise"
	drop         mockTaskQueueEventType = "drop"
	scheduleBack mockTaskQueueEventType = "scheduleBack"
	scheduleNew  mockTaskQueueEventType = "scheduleNew"
)

type mockQueueEvent struct {
	taskIndex int
	eventType mockTaskQueueEventType
}

func TestTaskQueue(t *testing.T) {
	testCases := []struct {
		description string
		workers     int
		capacity    int
		actions     []mockQueueEvent
		want        []string
		wantErr     error
	}{
		{
			description: "single worker executes tasks one by one in order of scheduling",
			workers:     1,
			capacity:    2,
			actions:     []mockQueueEvent{{0, finalise}, {1, finalise}, {2, finalise}},
			want:        []string{"start:t0", "finish:t0", "start:t1", "finish:t1", "start:t2", "finish:t2"},
		},
		{
			description: "when the worker occupied by the first task is blocked(because the task takes longer to finish), remaining worker handles the workload",
			workers:     2,
			capacity:    2,
			actions:     []mockQueueEvent{{1, finalise}, {2, awaitStarted}, {2, finalise}, {3, awaitStarted}, {3, finalise}, {0, finalise}},
			want:        []string{"start:t0", "start:t1", "finish:t1", "start:t2", "finish:t2", "start:t3", "finish:t3", "finish:t0"},
		},
		{
			description: "dropping executed task should have no effect",
			workers:     2,
			capacity:    2,
			actions:     []mockQueueEvent{{0, drop}, {0, finalise}, {2, awaitStarted}, {1, finalise}, {3, awaitStarted}, {2, finalise}, {3, finalise}},
			want:        []string{"start:t0", "start:t1", "finish:t0", "start:t2", "finish:t1", "start:t3", "finish:t2", "finish:t3"},
		},
		{
			description: "dropping non executed task, should be dropped from the queue and will not be executed",
			workers:     2,
			capacity:    2,
			actions:     []mockQueueEvent{{2, drop}, {0, finalise}, {3, awaitStarted}, {1, finalise}, {3, finalise}},
			want:        []string{"start:t0", "start:t1", "finish:t0", "start:t3", "finish:t1", "finish:t3"},
		},
		{
			description: "dropping non executed task and adding it back, should result in executing the task as the last one",
			workers:     2,
			capacity:    2,
			actions:     []mockQueueEvent{{2, drop}, {2, scheduleBack}, {0, finalise}, {3, awaitStarted}, {1, finalise}, {2, awaitStarted}, {2, finalise}, {3, finalise}},
			want:        []string{"start:t0", "start:t1", "finish:t0", "start:t3", "finish:t1", "start:t2", "finish:t2", "finish:t3"},
		},
		{
			description: "dropping non existing task",
			workers:     2,
			capacity:    2,
			actions:     []mockQueueEvent{{5, drop}, {0, finalise}, {2, awaitStarted}, {1, finalise}, {3, awaitStarted}, {2, finalise}, {3, finalise}},
			want:        []string{"start:t0", "start:t1", "finish:t0", "start:t2", "finish:t1", "start:t3", "finish:t2", "finish:t3"},
		},
		{
			description: "should fail adding a new task to an already full queue",
			workers:     1,
			capacity:    2,
			actions:     []mockQueueEvent{{3, scheduleNew}, {0, finalise}, {1, finalise}, {2, finalise}},
			want:        []string{"start:t0", "finish:t0", "start:t1", "finish:t1", "start:t2", "finish:t2"},
			wantErr:     newTaskQueueError(fmt.Errorf("queue is full"), QueueIsFullErr),
		},
		{
			description: "dropping a queued task frees the queue",
			workers:     1,
			capacity:    2,
			actions:     []mockQueueEvent{{2, drop}, {3, scheduleNew}, {0, finalise}, {1, finalise}, {3, finalise}},
			want:        []string{"start:t0", "finish:t0", "start:t1", "finish:t1", "start:t3", "finish:t3"},
		},
		{
			description: "finalising a task frees the queue",
			workers:     1,
			capacity:    2,
			actions:     []mockQueueEvent{{0, finalise}, {1, awaitStarted}, {3, scheduleNew}, {1, finalise}, {2, awaitStarted}, {2, finalise}, {3, finalise}},
			want:        []string{"start:t0", "finish:t0", "start:t1", "finish:t1", "start:t2", "finish:t2", "start:t3", "finish:t3"},
		},
		{
			description: "should fail adding a new task with duplicate id",
			workers:     1,
			capacity:    2,
			actions:     []mockQueueEvent{{2, scheduleNew}, {0, finalise}, {1, finalise}, {2, finalise}},
			want:        []string{"start:t0", "finish:t0", "start:t1", "finish:t1", "start:t2", "finish:t2"},
			wantErr:     newTaskQueueError(fmt.Errorf("duplicated task id: t2"), DuplicatedIdErr),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			taskQueue, tasks := createFilledTaskQueue(t, tc.workers, tc.capacity)
			for _, action := range tc.actions {
				switch action.eventType {
				case finalise:
					tasks[action.taskIndex].finalizeAndAwait()
				case awaitStarted:
					tasks[action.taskIndex].awaitStarted()
				case drop:
					taskQueue.Drop(fmt.Sprintf("t%d", action.taskIndex))
				case scheduleBack:
					tsk := tasks[action.taskIndex]
					err := taskQueue.Schedule(Task{Id: tsk.id, Action: tsk.execute})
					assert.NoError(t, err)
				case scheduleNew:
					task := newMockTask(fmt.Sprintf("t%d", action.taskIndex))
					tasks = append(tasks, task)
					err := taskQueue.Schedule(Task{Id: task.id, Action: task.execute})
					assert.Equal(t, tc.wantErr, err)
				}
			}
			closeAndWait(t, taskQueue)
			assert.Equal(t, tc.want, timeline(tasks))
		})
	}
}

func createFilledTaskQueue(t *testing.T, workers int, capacity int) (*TaskQueue, []*taskMock) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	taskQueue := NewTaskQueue(ctx, workers, capacity, []TaskType{})
	tasks := newMockTasks(workers + capacity)
	for i, task := range tasks {
		err := taskQueue.Schedule(Task{Id: task.id, Action: task.execute})
		if err != nil {
			t.Fatalf("Could not schedule task: %v", err)
		}
		// waits for for all tasks can be picked by workers to eliminate test race conditions
		if i < workers {
			task.awaitStarted()
		}
	}
	return taskQueue, tasks
}

func closeAndWait(t *testing.T, taskQueue *TaskQueue) {
	err := taskQueue.CloseAndWait(10 * time.Second)
	assert.NoError(t, err)
}
