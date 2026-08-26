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

package resizerequestclient

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/googleapi"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

const (
	deleteOperationsCacheSize = 2000

	cancelOpType = "compute.instanceGroupManagerResizeRequests.cancel"
	deleteOpType = "compute.instanceGroupManagerResizeRequests.delete"

	// Known error transmitted from within cluster autoscaler
	// TODO(b/381046606): remove after migration to CreateInstances API
	FragmentedRRWarningCode          = "FRAGMENTED_FLEX_START_SCALE_UP"
	FragmentedRRWarningReason        = "FragmentedFlexStartScaleUp"
	FragmentedRRWarningMessageFormat = "Scale-up Limitation: Only %d of %d expected processed due to flex start non-queued preview scalability. Overflow will be handled in a later scale-up."
)

var (
	errOperationStillRunning = errors.New("operation is still running")
	errZoneOperationsAPI     = errors.New("error while getting ResizeRequest operation")
	errCancelConditionNotMet = errors.New("ResizeRequest was cancelled too late, it already started provisioning or reached terminal state")
)

type ResizeRequestReportState int

const (
	UnspecifiedReportState ResizeRequestReportState = iota
	ToBeReportedState
	AlreadyReportedState
	CleanUpOnlyState
)

type failedRequestTracker struct {
	mu           sync.Mutex
	errorsPerMIG map[gce.GceRef]map[error]int
}

func newFailedRequestTracker() *failedRequestTracker {
	return &failedRequestTracker{
		errorsPerMIG: make(map[gce.GceRef]map[error]int),
	}
}

func (t *failedRequestTracker) record(migRef gce.GceRef, err error, count int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.errorsPerMIG[migRef] == nil {
		t.errorsPerMIG[migRef] = map[error]int{}
	}
	t.errorsPerMIG[migRef][err] += count
}

func (t *failedRequestTracker) reset(migRef gce.GceRef) map[error]int {
	t.mu.Lock()
	defer t.mu.Unlock()

	err := t.errorsPerMIG[migRef]
	t.errorsPerMIG[migRef] = map[error]int{}
	return err
}

type nextAction int

const (
	noAction nextAction = iota
	deleteAction
	cancelAction
	invalidAction
)

// IsConditionNotMetErr matches ConditionNotMet errors returned on invoking an operation
func IsConditionNotMetErr(err error) bool {
	gErr, ok := err.(*googleapi.Error)
	return ok && gErr.Code == http.StatusPreconditionFailed
}

func protoDuration(d time.Duration) *time.Duration {
	return &d
}

func IsResourcePoolExhaustedErrorCode(errorCode string) bool {
	return strings.Contains(errorCode, "RESOURCE_POOL_EXHAUSTED")
}
