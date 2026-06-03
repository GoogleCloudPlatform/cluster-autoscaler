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

import "reflect"

type TaskQueueErrorType string

const (
	QueueIsFullErr  TaskQueueErrorType = "queue-is-full"
	DuplicatedIdErr TaskQueueErrorType = "duplicated-id"
	OtherErr        TaskQueueErrorType = "other"
)

func IsTaskQueueErr(err error, errType TaskQueueErrorType) bool {
	if err == nil || reflect.ValueOf(err).IsNil() {
		return false
	}
	if tqe, ok := err.(*TaskQueueError); ok {
		return tqe.Type == errType
	}
	return false
}

type TaskQueueError struct {
	Type TaskQueueErrorType
	Err  error
}

func newTaskQueueError(err error, errType TaskQueueErrorType) *TaskQueueError {
	return &TaskQueueError{
		Type: errType,
		Err:  err,
	}
}

func (e *TaskQueueError) Error() string {
	return e.Err.Error()
}
