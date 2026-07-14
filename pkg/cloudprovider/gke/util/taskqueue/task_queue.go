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
	"sync"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
)

const (
	defaultWorkerInvokerInterval        = 5 * time.Second
	defaultTimeoutCheckInterval         = 5 * time.Second
	defaultTimeoutNotificationThreshold = 5 * time.Minute
)

type TaskType string

type Task struct {
	Id       string
	TaskType TaskType
	Action   func() error
}

type scheduledTask struct {
	Task
	scheduledAt time.Time
}

type TaskQueue struct {
	mutex            sync.Mutex
	queue            *synchronizedQueue
	queued           chan struct{}
	closed           bool
	workers          int
	workerGroup      sync.WaitGroup
	taskCountsByType map[TaskType]*taskTypeCounter
}

// TaskQueueOpts holds options for creating a TaskQueue.
type TaskQueueOpts struct {
	context                      context.Context
	workers                      int
	capacity                     int
	workerInvokerInterval        time.Duration
	timeoutCheckInterval         time.Duration
	timeoutNotificationThreshold time.Duration
	taskTypes                    []TaskType
}

type taskTypeCounter struct {
	queue     int
	execution int
}

// NewTaskQueue return a new TaskQueue created with default options.
func NewTaskQueue(context context.Context, workers int, capacity int, taskTypes []TaskType) *TaskQueue {
	return NewTaskQueueWithOpts(TaskQueueOpts{
		context:                      context,
		workers:                      workers,
		capacity:                     capacity,
		workerInvokerInterval:        defaultWorkerInvokerInterval,
		timeoutCheckInterval:         defaultTimeoutCheckInterval,
		timeoutNotificationThreshold: defaultTimeoutNotificationThreshold,
		taskTypes:                    taskTypes,
	})
}

// NewTaskQueueWithOpts returns a new TaskQueue created with the given TaskQueueOpts.
func NewTaskQueueWithOpts(opts TaskQueueOpts) *TaskQueue {
	if opts.workers <= 0 {
		klog.Fatalf("expected workers > 0, got: %v", opts.workers)
	}
	if opts.capacity < 1 {
		klog.Fatalf("expected capacity > 0, got: %v", opts.capacity)
	}
	taskQueue := &TaskQueue{
		workers:          opts.workers,
		queue:            newSynchronizedQueue(opts.capacity),
		queued:           make(chan struct{}, opts.capacity),
		taskCountsByType: make(map[TaskType]*taskTypeCounter),
	}
	taskQueue.workerGroup.Add(opts.workers)
	for _, tt := range opts.taskTypes {
		// This metric is used to find clusters with HTNAP enabled.
		// Details in go/htnap-mitigation
		metrics.Metrics.UpdateTaskQueueSize(string(tt), metrics.QueuePhase, 0)
		metrics.Metrics.UpdateTaskQueueSize(string(tt), metrics.ExecutionPhase, 0)
	}
	for i := 0; i < opts.workers; i++ {
		go func() {
			runWorker(taskQueue)
			taskQueue.workerGroup.Done()
		}()
	}
	// As a backup for a situation when some notifications in the queued channel are lost,
	// invoke workers based on a timer.
	if opts.workerInvokerInterval > 0 {
		go runWorkerInvoker(taskQueue, opts)
	}
	// Register timeout checkers
	if opts.timeoutCheckInterval > 0 {
		go runTimeoutNotifier(taskQueue, opts)
	}
	// Clean up when context is closed
	go func() {
		<-opts.context.Done()
		taskQueue.Close()
	}()
	return taskQueue
}

func runWorker(taskQueue *TaskQueue) {
	for {
		_, more := <-taskQueue.queued
		// stop when scheduler is closed
		if !more {
			break
		}
		t := taskQueue.queue.dequeue()
		// task could be removed
		if t != nil {
			execStartAt := time.Now()
			taskQueue.updateTaskCountAtTaskStartExecution(t, execStartAt)
			err := t.Action()
			taskQueue.updateTaskCountAtTaskFinishExecution(t, execStartAt, err)
		}
	}
}

func (q *TaskQueue) updateTaskCountAtTaskStartExecution(t *scheduledTask, execStartAt time.Time) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	taskCounts, found := q.taskCountsByType[t.TaskType]
	if !found {
		taskCounts = &taskTypeCounter{}
		q.taskCountsByType[t.TaskType] = taskCounts
	} else {
		taskCounts.queue -= 1
	}
	taskCounts.execution += 1
	metrics.Metrics.UpdateTaskQueueSize(string(t.TaskType), metrics.QueuePhase, taskCounts.queue)
	metrics.Metrics.UpdateTaskQueueSize(string(t.TaskType), metrics.ExecutionPhase, taskCounts.execution)
	metrics.Metrics.ObserveTaskQueueDurationSeconds(string(t.TaskType), metrics.QueuePhase, "", execStartAt.Sub(t.scheduledAt))
}

func (q *TaskQueue) updateTaskCountAtTaskFinishExecution(t *scheduledTask, execStartAt time.Time, execErr error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	finishedAt := time.Now()
	var result metrics.TaskResult
	if execErr == nil {
		metrics.Metrics.IncrementTaskCompletedCount(string(t.TaskType), metrics.TaskSucceeded)
		result = metrics.TaskSucceeded
	} else {
		metrics.Metrics.IncrementTaskCompletedCount(string(t.TaskType), metrics.TaskFailed)
		result = metrics.TaskFailed
		klog.Errorf("Task %s failed: %v", t.Id, execErr)
	}
	metrics.Metrics.ObserveTaskQueueDurationSeconds(string(t.TaskType), metrics.ExecutionPhase, result, finishedAt.Sub(execStartAt))
	taskCounts := q.taskCountsByType[t.TaskType]
	taskCounts.execution -= 1
	metrics.Metrics.UpdateTaskQueueSize(string(t.TaskType), metrics.ExecutionPhase, taskCounts.execution)
}

func runWorkerInvoker(taskQueue *TaskQueue, opts TaskQueueOpts) {
	stop := false
	for !stop {
		select {
		case <-time.After(opts.workerInvokerInterval):
			select {
			case taskQueue.queued <- struct{}{}:
			default:
			}
		case <-opts.context.Done():
			stop = true
		}
	}
}

func runTimeoutNotifier(taskQueue *TaskQueue, opts TaskQueueOpts) {
	stop := false
	for !stop {
		select {
		case <-time.After(opts.timeoutCheckInterval):
			taskQueue.mutex.Lock()
			tasks := taskQueue.queue.taskList()
			taskQueue.mutex.Unlock()
			now := time.Now()
			var slowTasks []string
			for _, t := range tasks {
				scheduledFor := now.Sub(t.scheduledAt)
				if scheduledFor > opts.timeoutNotificationThreshold {
					slowTasks = append(slowTasks, fmt.Sprintf("%s:%v", t.Id, scheduledFor))
				}
			}
			if len(slowTasks) > 0 {
				klog.Warningf("Detected tasks %d (out of %d total tasks) that have been waiting long (%s) for execution: %v", len(slowTasks), len(tasks), opts.timeoutCheckInterval, slowTasks)
			}
		case <-opts.context.Done():
			stop = true
		}
	}
}

// Schedule adds a new task with the given taskId and action to the end of the task queue.
func (q *TaskQueue) Schedule(task Task) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.closed {
		return newTaskQueueError(fmt.Errorf("scheduler is closed"), OtherErr)
	}
	t := scheduledTask{Task: task, scheduledAt: time.Now()}
	if err := q.queue.enqueue(&t); err != nil {
		return err
	}
	taskCounts, found := q.taskCountsByType[t.TaskType]
	if !found {
		taskCounts = &taskTypeCounter{}
		q.taskCountsByType[t.TaskType] = taskCounts
	}
	taskCounts.queue += 1
	metrics.Metrics.UpdateTaskQueueSize(string(t.TaskType), metrics.QueuePhase, taskCounts.queue)
	select {
	case q.queued <- struct{}{}:
	default:
	}
	return nil
}

// Drop removes a task from the task queue.
func (q *TaskQueue) Drop(taskId string) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	t := q.queue.drop(taskId)
	if t == nil {
		return false
	}
	taskCounts := q.taskCountsByType[t.TaskType]
	taskCounts.queue -= 1
	metrics.Metrics.UpdateTaskQueueSize(string(t.TaskType), metrics.QueuePhase, taskCounts.queue)
	return true
}

// Close closes the task queue and returns immediately without waiting for all the workers to finish.
func (q *TaskQueue) Close() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if !q.closed {
		q.closed = true
		close(q.queued)
	}
}

// CloseAndWait closes the task queue and returns after all the workers are finished.
func (q *TaskQueue) CloseAndWait(timeout time.Duration) error {
	q.Close()
	c := make(chan struct{})
	go func() {
		defer close(c)
		q.workerGroup.Wait()
	}()
	select {
	case <-c:
		return nil
	case <-time.After(timeout):
		return newTaskQueueError(fmt.Errorf("reached timeout"), OtherErr)
	}
}
