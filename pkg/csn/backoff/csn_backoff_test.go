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

package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
)

type mockBackoff struct {
	lastErrorInfo        cloudprovider.InstanceErrorInfo
	lastNodeInfo         *framework.NodeInfo
	removeBackoffCalls   int
	removeStaleDataCalls int
}

func (m *mockBackoff) Backoff(_ cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, errorInfo cloudprovider.InstanceErrorInfo, _ time.Time) time.Time {
	m.lastErrorInfo = errorInfo
	m.lastNodeInfo = nodeInfo
	return time.Time{}
}

func (m *mockBackoff) BackoffStatus(_ cloudprovider.NodeGroup, _ *framework.NodeInfo, _ time.Time) base_backoff.Status {
	return base_backoff.Status{}
}
func (m *mockBackoff) RemoveBackoff(_ cloudprovider.NodeGroup, _ *framework.NodeInfo) {
	m.removeBackoffCalls++
}
func (m *mockBackoff) RemoveStaleBackoffData(_ time.Time) {
	m.removeStaleDataCalls++
}

func TestCSNCompositeBackoff_ReportResumptionError(t *testing.T) {
	nodeGroup := testprovider.NewTestNodeGroup("mig1", 1, 10, 1, true, false, "e2-standard-2", nil, nil)

	tests := []struct {
		name                 string
		errorCode            string
		errorMessage         string
		instanceStatus       string
		expectCompositeError bool
		expectCSNError       bool
		expectedErrorClass   cloudprovider.InstanceErrorClass
		expectedErrorCode    string
		expectedErrorMessage string
	}{
		{
			name:                 "composite backoff error",
			errorCode:            "ZONE_RESOURCE_POOL_EXHAUSTED",
			errorMessage:         "something failed",
			instanceStatus:       "RUNNING",
			expectCompositeError: true,
			expectCSNError:       true,
			expectedErrorClass:   cloudprovider.OutOfResourcesErrorClass,
			expectedErrorCode:    "RESOURCE_POOL_EXHAUSTED",
			expectedErrorMessage: "something failed",
		},
		{
			name:                 "csn backoff error",
			errorCode:            "RESOURCE_NOT_FOUND",
			errorMessage:         "instance not found",
			instanceStatus:       "RUNNING",
			expectCompositeError: false,
			expectCSNError:       true,
			expectedErrorClass:   cloudprovider.OtherErrorClass,
			expectedErrorCode:    "RESOURCE_NOT_FOUND",
			expectedErrorMessage: "instance not found",
		},
	}

	nodeInfo := &framework.NodeInfo{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compositeBackoff := &mockBackoff{}
			csnBackoff := &mockBackoff{}
			backoff := NewCSNCompositeBackoff(compositeBackoff, csnBackoff)

			backoff.ReportResumptionError(nodeGroup, nodeInfo, tc.errorCode, tc.errorMessage, tc.instanceStatus)

			verifyMockBackoff := func(m *mockBackoff, expected bool) {
				if expected {
					assert.Equal(t, tc.expectedErrorCode, m.lastErrorInfo.ErrorCode)
					assert.Equal(t, tc.expectedErrorMessage, m.lastErrorInfo.ErrorMessage)
					assert.Equal(t, tc.expectedErrorClass, m.lastErrorInfo.ErrorClass)
					assert.Equal(t, nodeInfo, m.lastNodeInfo)
				} else {
					assert.Empty(t, m.lastErrorInfo.ErrorCode)
					assert.Nil(t, m.lastNodeInfo)
				}
			}

			verifyMockBackoff(compositeBackoff, tc.expectCompositeError)
			verifyMockBackoff(csnBackoff, tc.expectCSNError)
		})
	}
}

func TestCSNCompositeBackoff_BackoffMethods(t *testing.T) {
	compositeBackoff := &mockBackoff{}
	csnBackoff := &mockBackoff{}
	b := NewCSNCompositeBackoff(compositeBackoff, csnBackoff)

	nodeGroup := testprovider.NewTestNodeGroup("mig1", 1, 10, 1, true, false, "e2-standard-2", nil, nil)
	errInfo := cloudprovider.InstanceErrorInfo{ErrorCode: "ERR1"}
	currentTime := time.Now()

	nodeInfo := &framework.NodeInfo{}
	b.Backoff(nodeGroup, nodeInfo, errInfo, currentTime)
	assert.Equal(t, errInfo.ErrorCode, compositeBackoff.lastErrorInfo.ErrorCode)
	assert.Equal(t, errInfo.ErrorCode, csnBackoff.lastErrorInfo.ErrorCode)
	assert.Equal(t, nodeInfo, compositeBackoff.lastNodeInfo)
	assert.Equal(t, nodeInfo, csnBackoff.lastNodeInfo)

	b.BackoffStatus(nodeGroup, nil, currentTime)

	b.RemoveBackoff(nodeGroup, nil)
	assert.Equal(t, 1, compositeBackoff.removeBackoffCalls)
	assert.Equal(t, 1, csnBackoff.removeBackoffCalls)

	b.RemoveStaleBackoffData(currentTime)
	assert.Equal(t, 1, compositeBackoff.removeStaleDataCalls)
	assert.Equal(t, 1, csnBackoff.removeStaleDataCalls)
}
