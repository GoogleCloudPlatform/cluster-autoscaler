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

package noscaledown

import (
	"time"

	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/visibility/types"
)

// NoScaleDownMock is a mock of the NoScaleDown interface.
type NoScaleDownMock struct {
	mock.Mock
}

// GetNewReasons is a mocked method.
func (nsd *NoScaleDownMock) GetNewReasons(scaleDownStatus *types.ScaleDownStatus, now time.Time) *Reasons {
	args := nsd.Called(scaleDownStatus, now)
	return args.Get(0).(*Reasons)
}

// MarkReasonsReported is a mocked method.
func (nsd *NoScaleDownMock) MarkReasonsReported(reasons *Reasons, reportTime time.Time) {
	nsd.Called(reasons, reportTime)
}

// NewNoScaleDownMock returns a new mock of the NoScaleDown interface.
func NewNoScaleDownMock() *NoScaleDownMock {
	return &NoScaleDownMock{}
}
