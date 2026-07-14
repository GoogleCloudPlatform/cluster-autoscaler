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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var tasksOpsCounter atomic.Uint64

type taskMock struct {
	mutex         sync.Mutex
	finalizeMutex sync.Mutex
	id            string
	finishedAt    int
	startedAt     int
	started       chan struct{}
	finished      chan struct{}
}

func newMockTask(id string) *taskMock {
	t := &taskMock{
		id:         id,
		startedAt:  -1,
		finishedAt: -1,
		started:    make(chan struct{}, 1),
		finished:   make(chan struct{}, 1),
	}
	t.finalizeMutex.Lock()
	return t
}

func newMockTasks(count int) []*taskMock {
	var tasks []*taskMock
	for i := 0; i < count; i++ {
		tasks = append(tasks, newMockTask(fmt.Sprintf("t%d", i)))
	}
	return tasks
}

func (t *taskMock) execute() error {
	// mark as started
	t.mutex.Lock()
	t.startedAt = int(tasksOpsCounter.Add(1))
	t.mutex.Unlock()
	t.started <- struct{}{}
	// wait for finalization
	t.finalizeMutex.Lock()
	t.mutex.Lock()
	t.finishedAt = int(tasksOpsCounter.Add(1))
	t.mutex.Unlock()
	t.finished <- struct{}{}
	return nil
}

func (t *taskMock) finalizeAndAwait() {
	t.finalizeMutex.Unlock()
	select {
	case x := <-t.finished:
		t.finished <- x
	case <-time.After(3 * time.Second):
		panic(fmt.Sprintf("Task finalization timeout. Task: %v", t.id))
	}
}

func (t *taskMock) awaitStarted() {
	select {
	case x := <-t.started:
		t.started <- x
	case <-time.After(3 * time.Second):
		panic(fmt.Sprintf("Task startup timeout. Task: %v", t.id))
	}
}

func (t *taskMock) startTime() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.startedAt
}

func (t *taskMock) finishTime() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.finishedAt
}

func timeline(tasks []*taskMock) []string {
	var indexed = make(map[string]int)
	var result []string
	for _, t := range tasks {
		if startedAt := t.startTime(); startedAt >= 0 {
			opId := fmt.Sprintf("start:%v", t.id)
			indexed[opId] = startedAt
			result = append(result, opId)
		}
		if finishedAt := t.finishTime(); finishedAt >= 0 {
			opId := fmt.Sprintf("finish:%v", t.id)
			indexed[opId] = finishedAt
			result = append(result, opId)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return indexed[result[i]] < indexed[result[j]]
	})
	return result
}
