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

package lookaheadbuffer

import (
	"github.com/stretchr/testify/mock"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
)

type MockMetrics struct {
	mock.Mock
}

func (m *MockMetrics) UpdateLookaheadLaunchStatus(launchPhase, launchedFrom, strategy string) {
	m.MethodCalled("UpdateLookaheadLaunchStatus", launchPhase, launchedFrom, strategy)
}

func (m *MockMetrics) UpdateResizableVmUnschedulableLookaheadPodsCount(machineFamily string, laPods int) {
	m.MethodCalled("UpdateResizableVmUnschedulableLookaheadPodsCount", machineFamily, laPods)
}

func (m *MockMetrics) UpdateLookaheadPodsCount(laPodsCount map[size.Allocatable]int) {
	m.MethodCalled("UpdateLookaheadPodsCount", laPodsCount)
}
