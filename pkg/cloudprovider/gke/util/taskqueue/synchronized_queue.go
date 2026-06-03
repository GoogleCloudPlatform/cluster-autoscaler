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
	"container/list"
	"fmt"
	"sync"
)

type synchronizedQueue struct {
	mutex       sync.Mutex
	tasks       *list.List
	elementById map[string]*list.Element
	capacity    int
}

func newSynchronizedQueue(capacity int) *synchronizedQueue {
	return &synchronizedQueue{
		tasks:       list.New(),
		elementById: make(map[string]*list.Element),
		capacity:    capacity,
	}
}

func (q *synchronizedQueue) enqueue(task *scheduledTask) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if _, found := q.elementById[task.Id]; found {
		return newTaskQueueError(fmt.Errorf("duplicated task id: %v", task.Id), DuplicatedIdErr)
	}
	if q.tasks.Len() == q.capacity {
		return newTaskQueueError(fmt.Errorf("queue is full"), QueueIsFullErr)
	}
	element := q.tasks.PushBack(task)
	q.elementById[task.Id] = element
	return nil
}

func (q *synchronizedQueue) dequeue() *scheduledTask {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.tasks.Len() == 0 {
		return nil
	}
	front := q.tasks.Front()
	frontTask := front.Value.(*scheduledTask)
	q.tasks.Remove(front)
	delete(q.elementById, frontTask.Id)
	return frontTask
}

func (q *synchronizedQueue) drop(taskId string) *scheduledTask {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	element, found := q.elementById[taskId]
	if !found {
		return nil
	}
	delete(q.elementById, taskId)
	q.tasks.Remove(element)
	return element.Value.(*scheduledTask)
}

func (q *synchronizedQueue) taskList() []*scheduledTask {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	var tasks []*scheduledTask
	// preserving order
	for ele := q.tasks.Front(); ele != nil; ele = ele.Next() {
		t := ele.Value.(*scheduledTask)
		tasks = append(tasks, t)
	}
	return tasks
}
