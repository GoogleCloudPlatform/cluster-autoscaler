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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gke_backoff "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	ekvms_customthresholds "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/backoff/customthresholds"
	ek_errors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/nodetracker"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/test"
	resizable_vm_utils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/utils"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	clock "k8s.io/utils/clock/testing"
)

var caVersion = getCaVersion("35.197.0")
var defaultEkBackoffCustomThresholdsProvider = ekvms_customthresholds.NewCustomThresholdsProvider(nil, caVersion)

func TestEkBackoff(t *testing.T) {
	fakeClock := clock.NewFakePassiveClock(time.Now())
	ekNg := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "ek-standard-32",
	}).Build()
	mockCP := &mockCloudProvider{}
	mockCP.On("NodeGroupForNode").Return(ekNg, nil)

	for _, tt := range []struct {
		name string

		// inputs
		node        *v1.Node
		resizeError ek_errors.ResizeError

		// results
		wantBackoff bool
	}{
		{
			name:        "no backoff error",
			node:        test.EkNode32("node1", 1, 1),
			resizeError: ek_errors.ResizeError{Backoff: ek_errors.NoBackoff},

			wantBackoff: false,
		},
		{
			name:        "node level backoff",
			node:        test.EkNode32("node1", 1, 1),
			resizeError: ek_errors.ResizeError{Backoff: ek_errors.NodeLevel},

			wantBackoff: true,
		},
		{
			name:        "cluster level backoff",
			node:        test.EkNode32("node1", 1, 1),
			resizeError: ek_errors.ResizeError{Backoff: ek_errors.ClusterLevel, ErrType: ek_errors.Http5xxError, OriginalError: errors.New("New error")},

			wantBackoff: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			family, err := resizable_vm_utils.GetMachineFamilyName(tt.node)
			assert.NoError(t, err)

			backoffManager := NewManager(mockCP, defaultEkBackoffCustomThresholdsProvider, fakeClock)
			assert.False(t, backoffManager.isClusterBackedOff(family))
			backoffManager.Backoff(tt.node, tt.resizeError)
			got := backoffManager.IsBackedOff(family, tt.node.Name)
			assert.Equal(t, tt.wantBackoff, got)
		})
	}
}

func TestClusterLevelErrorTriggersClusterLevelBackoff(t *testing.T) {
	fakeClock := clock.NewFakePassiveClock(time.Now())
	ekNg := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "ek-standard-32",
	}).Build()
	mockCP := &mockCloudProvider{}
	mockCP.On("NodeGroupForNode").Return(ekNg, nil)

	ek1 := test.EkNode32("node1", 1, 1)
	ek2 := test.EkNode8("node2", 1, 1)
	family, err := resizable_vm_utils.GetMachineFamilyName(ek1)
	assert.NoError(t, err)

	backoffManager := NewManager(mockCP, defaultEkBackoffCustomThresholdsProvider, fakeClock)
	assert.False(t, backoffManager.isClusterBackedOff(family))

	clusterLevelError := ek_errors.ResizeError{Backoff: ek_errors.ClusterLevel, ErrType: ek_errors.Http5xxError, OriginalError: errors.New("New error")}

	backoffManager.Backoff(ek1, clusterLevelError)
	assert.True(t, backoffManager.IsBackedOff(family, ek1.Name))

	// 2nd node that we didn't explicitly backoff should be in cluster-wide backoff.
	assert.True(t, backoffManager.IsBackedOff(family, ek2.Name))
}

func TestNodeLevelBackoffTriggersClusterLevelBackoff(t *testing.T) {
	fakeClock := clock.NewFakePassiveClock(time.Now())
	ekNg := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "ek-standard-32",
	}).Build()
	mockCP := &mockCloudProvider{}
	mockCP.On("NodeGroupForNode").Return(ekNg, nil)

	ek1 := test.EkNode32("node1", 1, 1)
	ek2 := test.EkNode8("node2", 1, 1)
	ek3 := test.EkNode8("node3", 1, 1)
	ek4 := test.EkNode32("node4", 1, 1)
	ek5 := test.EkNode32("node5", 1, 1)
	ekNodes := []*v1.Node{ek1, ek2, ek3, ek4}
	family, err := resizable_vm_utils.GetMachineFamilyName(ek1)
	assert.NoError(t, err)
	rateLimitError, _ := ek_errors.ToResizeError(ek_errors.NewRateLimitExceededError("ek", ek_errors.GetInstance, errors.New("Rate limit error")))
	guestAgentResizeTimeoutError, _ := ek_errors.ToResizeError(ek_errors.NewGuestAgentResizeTimeout("ek", errors.New("GuestAgent resize timeout error")))
	testCases := []struct {
		desc                        string
		rateLimitErrorCount         int
		guestAgentResizeErrorCount  int
		errorThresholdsDisabled     bool
		customErrorThresholds       map[string]int
		expectedClusterLevelBackoff bool
	}{
		{
			desc:                        "0 RateLimit, 0 GuestAgent errors -> less than clusterBackoffThreshold errors -> no cluster level backoff",
			rateLimitErrorCount:         0,
			guestAgentResizeErrorCount:  0,
			errorThresholdsDisabled:     true,
			customErrorThresholds:       map[string]int{},
			expectedClusterLevelBackoff: false,
		},
		{
			desc:                        "3 RateLimit, 3 GuestAgent, errorThresholds feature is enabled -> none error type triggers cluster level backoff -> no cluster level backoff",
			rateLimitErrorCount:         3,
			guestAgentResizeErrorCount:  3,
			customErrorThresholds:       map[string]int{},
			expectedClusterLevelBackoff: false,
		},
		{
			desc:                       "2 RateLimit, 4 GuestAgent, errorThresholds feature is enabled -> calculate errors based on error type only for RateLimit -> no cluster level backoff",
			rateLimitErrorCount:        2,
			guestAgentResizeErrorCount: 4,
			customErrorThresholds: map[string]int{
				"rateLimitExceededError": 3,
			},
			expectedClusterLevelBackoff: false,
		},
		{
			desc:                       "3 RateLimit, 4 GuestAgent, errorThresholds feature is enabled -> calculate errors based on error type only for RateLimit -> cluster level backoff",
			rateLimitErrorCount:        3,
			guestAgentResizeErrorCount: 4,
			customErrorThresholds: map[string]int{
				"rateLimitExceededError": 3,
			},
			expectedClusterLevelBackoff: true,
		},
		{
			desc:                       "2 RateLimit, 2 GuestAgent, errorThresholds feature is enabled -> calculate errors based on error type -> no cluster level backoff",
			rateLimitErrorCount:        2,
			guestAgentResizeErrorCount: 2,
			customErrorThresholds: map[string]int{
				"rateLimitExceededError":  3,
				"guestAgentResizeTimeout": 3,
			},
			expectedClusterLevelBackoff: false,
		},
		{
			desc:                       "3 RateLimit, 0 GuestAgent, errorThresholds feature is enabled -> calculate errors based on error type -> cluster level backoff",
			rateLimitErrorCount:        3,
			guestAgentResizeErrorCount: 0,
			customErrorThresholds: map[string]int{
				"rateLimitExceededError":  4,
				"guestAgentResizeTimeout": 4,
			},
			expectedClusterLevelBackoff: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mockCustomThresholds := &mockCustomThresholdsProvider{}
			mockCustomThresholds.On("RefreshCustomThresholds").Return()
			mockCustomThresholds.On("IsErrorThresholdsFeatureEnabled").Return(
				!tc.errorThresholdsDisabled)
			for errType, threshold := range tc.customErrorThresholds {
				mockCustomThresholds.On("GetThreshold", errType).Return(threshold, true)
			}
			mockCustomThresholds.On("GetThreshold", mock.Anything).Return(0, false)
			backoffManager := NewManager(mockCP, mockCustomThresholds, fakeClock)
			assert.False(t, backoffManager.isClusterBackedOff(family))
			for i := range tc.rateLimitErrorCount {
				backoffManager.Backoff(ekNodes[i], *rateLimitError)
			}
			for i := range tc.guestAgentResizeErrorCount {
				backoffManager.Backoff(ekNodes[i], *guestAgentResizeTimeoutError)
			}
			// Check the status of the 5th node that we didn't explicitly backoff.
			assert.Equal(t, tc.expectedClusterLevelBackoff, backoffManager.IsBackedOff(family, ek5.Name))
		})
	}
}

func TestExpiredNodesDoNotTriggerClusterLevelBackoff(t *testing.T) {
	fakeClock := clock.NewFakePassiveClock(time.Now())
	ekNg := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "ek-standard-32",
	}).Build()
	mockCP := &mockCloudProvider{}
	mockCP.On("NodeGroupForNode").Return(ekNg, nil)

	ek1 := test.EkNode32("node1", 1, 1)
	ek2 := test.EkNode32("node2", 1, 1)
	ek3 := test.EkNode32("node3", 1, 1)
	family, err := resizable_vm_utils.GetMachineFamilyName(ek1)
	assert.NoError(t, err)

	backoffManager := NewManager(mockCP, defaultEkBackoffCustomThresholdsProvider, fakeClock)
	assert.False(t, backoffManager.isClusterBackedOff(family))

	nodeLevelError := ek_errors.ResizeError{Backoff: ek_errors.NodeLevel, ErrType: ek_errors.NotEnoughResourceOnHostError, OriginalError: errors.New("New error")}

	backoffManager.Backoff(ek1, nodeLevelError)
	assert.True(t, backoffManager.IsBackedOff(family, ek1.Name))
	assert.False(t, backoffManager.isClusterBackedOff(family))

	backoffManager.Backoff(ek2, nodeLevelError)
	assert.True(t, backoffManager.IsBackedOff(family, ek2.Name))

	// Forward to time where current two nodes in node level backoff are already expired.
	fakeClock.SetTime(fakeClock.Now().Add(nodeBackoffExpiry + 1*time.Minute))

	// 3rd node, but only 1 active in backoff. Should not trigger cluster wide backoff.
	backoffManager.Backoff(ek3, nodeLevelError)
	assert.False(t, backoffManager.IsBackedOff(family, ek1.Name))
	assert.False(t, backoffManager.IsBackedOff(family, ek2.Name))
	assert.True(t, backoffManager.IsBackedOff(family, ek3.Name))
}

func TestExpiredClusterLevelBackoff(t *testing.T) {
	fakeClock := clock.NewFakePassiveClock(time.Now())
	ekNg := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "ek-standard-32",
	}).Build()
	mockCP := &mockCloudProvider{}
	mockCP.On("NodeGroupForNode").Return(ekNg, nil)

	ek1 := test.EkNode32("node1", 1, 1)
	ek2 := test.EkNode8("node2", 1, 1)
	family, err := resizable_vm_utils.GetMachineFamilyName(ek1)
	assert.NoError(t, err)

	backoffManager := NewManager(mockCP, defaultEkBackoffCustomThresholdsProvider, fakeClock)
	assert.False(t, backoffManager.isClusterBackedOff(family))

	clusterLevelError := ek_errors.ResizeError{Backoff: ek_errors.ClusterLevel, ErrType: ek_errors.Http5xxError, OriginalError: errors.New("New error")}

	backoffManager.Backoff(ek1, clusterLevelError)
	assert.True(t, backoffManager.IsBackedOff(family, ek1.Name))
	assert.True(t, backoffManager.IsBackedOff(family, ek2.Name))

	fakeClock.SetTime(fakeClock.Now().Add(gke_backoff.ResizableFamilyInitialBackOffDuration + 1*time.Minute))

	// cluster backoff is expired
	assert.False(t, backoffManager.IsBackedOff(family, ek1.Name))
	assert.False(t, backoffManager.IsBackedOff(family, ek2.Name))
}

func TestIsClusterBackedOff(t *testing.T) {
	testStartTime := time.Now()
	errorType := string(ek_errors.RateLimitExceededError)
	machineFamily := machinetypes.EK.Name()
	mockCustomThresholds := &mockCustomThresholdsProvider{}
	mockCustomThresholds.On("IsErrorThresholdsFeatureEnabled").Return(
		false)
	for _, tc := range []struct {
		name                      string
		backedOffNodesCount       int
		clusterBackoffUntil       time.Duration
		expectedNodeBasedBackoffs map[string]time.Time
		expectedBackoffStatus     bool
	}{
		{
			name:                      "backed off nodes count below threshold, no cluster level backoff",
			backedOffNodesCount:       1,
			clusterBackoffUntil:       -2 * time.Hour,
			expectedNodeBasedBackoffs: map[string]time.Time{},
			expectedBackoffStatus:     false,
		},
		{
			name:                      "backed off nodes count below threshold, cluster level backoff",
			backedOffNodesCount:       1,
			clusterBackoffUntil:       10 * time.Hour,
			expectedNodeBasedBackoffs: map[string]time.Time{},
			expectedBackoffStatus:     true,
		},
		{
			name:                      "backed off nodes count above threshold, cluster level backoff",
			backedOffNodesCount:       4,
			clusterBackoffUntil:       -2 * time.Hour,
			expectedNodeBasedBackoffs: map[string]time.Time{machineFamily: testStartTime.Add(nodeBackoffExpiry)},
			expectedBackoffStatus:     true,
		},
		{
			name:                      "backed off nodes count above threshold, cluster level backoff",
			backedOffNodesCount:       4,
			clusterBackoffUntil:       2 * time.Hour,
			expectedNodeBasedBackoffs: map[string]time.Time{},
			expectedBackoffStatus:     true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeClock := clock.NewFakeClock(testStartTime)
			manager := NewManager(nil, mockCustomThresholds, fakeClock)

			nodeBackoff := &mockNodeBackoff{}
			nodeBackoff.On("Count").Return(tc.backedOffNodesCount, testStartTime.Add(nodeBackoffExpiry))
			manager.nodeBackoffs.setTracker(machineFamily, errorType, nodeBackoff)

			clusterBackoff := &mockClusterBackoff{}
			clusterBackoff.On("BackoffUntil").Return(testStartTime.Add(tc.clusterBackoffUntil))
			manager.clusterBackoffs[machineFamily] = clusterBackoff

			assert.Equal(t, tc.expectedBackoffStatus, manager.isClusterBackedOff(machineFamily))
			assert.Equal(t, tc.expectedNodeBasedBackoffs, manager.nodeBasedBackoffs)
		})
	}
}

func TestIsClusterBackedOff_StoresEarliestExpiration(t *testing.T) {
	testStartTime := time.Now()
	machineFamily := machinetypes.EK.Name()
	mockCustomThresholds := &mockCustomThresholdsProvider{}
	mockCustomThresholds.On("IsErrorThresholdsFeatureEnabled").Return(
		false)

	fakeClock := clock.NewFakeClock(testStartTime)
	manager := NewManager(nil, mockCustomThresholds, fakeClock)

	// Tracker 1: RateLimit, count 2, expires in 10 minutes
	tracker1 := nodetracker.New(fakeClock)
	tracker1.AddNode("node1", testStartTime.Add(10*time.Minute))
	tracker1.AddNode("node2", testStartTime.Add(10*time.Minute))
	manager.nodeBackoffs.setTracker(machineFamily, string(ek_errors.RateLimitExceededError), tracker1)

	// Tracker 2: GuestAgentResizeTimeout, count 2, expires in 5 minutes
	tracker2 := nodetracker.New(fakeClock)
	tracker2.AddNode("node3", testStartTime.Add(5*time.Minute))
	tracker2.AddNode("node4", testStartTime.Add(5*time.Minute))
	manager.nodeBackoffs.setTracker(machineFamily, string(ek_errors.GuestAgentResizeTimeout), tracker2)

	// Tracker 3: QuotaExceededError, count 2, expires in 15 minutes, but will add them to the manager later
	tracker3 := nodetracker.New(fakeClock)
	tracker3.AddNode("node5", testStartTime.Add(15*time.Minute))
	tracker3.AddNode("node6", testStartTime.Add(15*time.Minute))

	// Cluster backoff is not active
	clusterBackoff := &mockClusterBackoff{}
	clusterBackoff.On("BackoffUntil").Return(testStartTime.Add(-1 * time.Hour))
	manager.clusterBackoffs[machineFamily] = clusterBackoff

	// Total count is 4 >= 3 (threshold). Should trigger cluster backoff.
	assert.True(t, manager.isClusterBackedOff(machineFamily))
	expectedNodeBasedBackoffs := map[string]time.Time{
		machineFamily: testStartTime.Add(5 * time.Minute),
	}
	assert.Equal(t, expectedNodeBasedBackoffs, manager.nodeBasedBackoffs)

	// Advance clock by 5 minutes. The 2 nodes in tracker2 should expire.
	fakeClock.SetTime(testStartTime.Add(5 * time.Minute))

	// Total count should now be 2 (only tracker1 nodes) < 3. Cluster should NOT be backed off.
	assert.False(t, manager.isClusterBackedOff(machineFamily))
	assert.Equal(t, expectedNodeBasedBackoffs, manager.nodeBasedBackoffs)

	// Add two more backoff nodes to trigger the map update and cluster backoff.
	manager.nodeBackoffs.setTracker(machineFamily, string(ek_errors.QuotaExceededError), tracker3)
	expectedNodeBasedBackoffs = map[string]time.Time{
		machineFamily: testStartTime.Add(10 * time.Minute),
	}
	assert.True(t, manager.isClusterBackedOff(machineFamily))
	assert.Equal(t, expectedNodeBasedBackoffs, manager.nodeBasedBackoffs)
}

func TestUpdateBackoffMetrics(t *testing.T) {
	fakeClock := clock.NewFakeClock(time.Now())
	mockCP := &mockCloudProvider{}
	manager := NewManager(mockCP, defaultEkBackoffCustomThresholdsProvider, fakeClock)

	mockMetrics := &mockMetrics{}
	// For all resizable machine types, we expect UpdateResizeBackoffStatus to be called.
	// We don't want to hardcode all families here, but we can at least check it doesn't panic
	// and maybe check one specific family like "ek".
	mockMetrics.On("UpdateEkBackoffStatus", mock.Anything).Return()
	mockMetrics.On("UpdateResizeBackoffStatus", mock.Anything, mock.Anything).Return()
	manager.metrics = mockMetrics

	manager.updateBackoffMetrics()
	mockMetrics.AssertCalled(t, "UpdateResizeBackoffStatus", machinetypes.EK.Name(), false)
}

func TestBackoffIsolation(t *testing.T) {
	fakeClock := clock.NewFakePassiveClock(time.Now())
	mockCP := &mockCloudProvider{}

	t.Run("Node-level backoff isolation between families", func(t *testing.T) {
		backoffManager := NewManager(mockCP, defaultEkBackoffCustomThresholdsProvider, fakeClock)
		ekNode := test.EkNode32("ek-node", 1, 1)
		e4aNode := test.E4aNode32("e4a-node", 1, 1)

		backoffManager.Backoff(ekNode, ek_errors.ResizeError{Backoff: ek_errors.NodeLevel})
		assert.True(t, backoffManager.IsBackedOff("ek", ekNode.Name))
		assert.False(t, backoffManager.IsBackedOff("e4a", e4aNode.Name))
	})

	t.Run("Cluster-level backoff isolation between families", func(t *testing.T) {
		backoffManager := NewManager(mockCP, defaultEkBackoffCustomThresholdsProvider, fakeClock)
		ekNode := test.EkNode32("ek-node", 1, 1)

		backoffManager.Backoff(ekNode, ek_errors.ResizeError{Backoff: ek_errors.ClusterLevel, ErrType: ek_errors.Http5xxError, OriginalError: errors.New("error")})
		assert.True(t, backoffManager.isClusterBackedOff("ek"))
		assert.False(t, backoffManager.isClusterBackedOff("e4a"))
	})

	t.Run("Node-level backoff isolation within the same family", func(t *testing.T) {
		backoffManager := NewManager(mockCP, defaultEkBackoffCustomThresholdsProvider, fakeClock)
		ekNode1 := test.EkNode32("node1", 1, 1)
		ekNode2 := test.EkNode32("node2", 1, 1)

		backoffManager.Backoff(ekNode1, ek_errors.ResizeError{Backoff: ek_errors.NodeLevel})
		assert.True(t, backoffManager.IsBackedOff("ek", ekNode1.Name))
		assert.False(t, backoffManager.IsBackedOff("ek", ekNode2.Name))
	})

	t.Run("DeleteNode isolation", func(t *testing.T) {
		backoffManager := NewManager(mockCP, defaultEkBackoffCustomThresholdsProvider, fakeClock)
		ekNode1 := test.EkNode32("node1", 1, 1)
		ekNode2 := test.EkNode32("node2", 1, 1)

		backoffManager.Backoff(ekNode1, ek_errors.ResizeError{Backoff: ek_errors.NodeLevel})
		backoffManager.Backoff(ekNode2, ek_errors.ResizeError{Backoff: ek_errors.NodeLevel})
		assert.True(t, backoffManager.IsBackedOff("ek", ekNode1.Name))
		assert.True(t, backoffManager.IsBackedOff("ek", ekNode2.Name))

		backoffManager.DeleteNode("ek", ekNode1.Name)
		assert.False(t, backoffManager.IsBackedOff("ek", ekNode1.Name))
		assert.True(t, backoffManager.IsBackedOff("ek", ekNode2.Name))
	})
}

func TestRefreshCustomThresholds(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		caVersion            string
		experimentFlagValue  string
		wantFeatureDisabled  bool
		wantCustomThresholds map[string]int
	}{
		{
			name:                 "disabled status",
			caVersion:            "1.2.3",
			experimentFlagValue:  `{"status": "STATUS_DISABLED"}`,
			wantFeatureDisabled:  true,
			wantCustomThresholds: map[string]int{},
		},
		{
			name:      "experiment is enabled, version match, feature is enabled",
			caVersion: "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "35.197.0",
  "errors": [
    {
      "name": "quotaExceededError",
      "threshold": 5
    }
  ]
}`,
			wantCustomThresholds: map[string]int{
				"quotaExceededError": 5,
			},
		},
		{
			name:      "experiment is enabled, version match, feature is disabled",
			caVersion: "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "35.197.0",
  "errors": [
    {
      "name": "quotaExceededError",
      "threshold": 5
    }
  ],
  "errorThresholdsDisabled": true
}`,
			wantFeatureDisabled: true,
			wantCustomThresholds: map[string]int{
				"quotaExceededError": 5,
			},
		},
		{
			name:      "enabled status, multiple errors (version too low)",
			caVersion: "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "999.999.999",
  "errors": [
    {
      "name": "quotaExceededError",
      "threshold": 3
    }
  ],
  "errorThresholdsDisabled": false
}`,
			wantFeatureDisabled:  true,
			wantCustomThresholds: map[string]int{},
		},
		{
			name:      "enabled status, multiple errors",
			caVersion: "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "0.0.0",
  "errors": [
    {
      "name": "quotaExceededError",
      "threshold": 3
    },
    {
      "name": "timeoutError",
      "threshold": 2
    }
  ]
}`,
			wantCustomThresholds: map[string]int{
				"quotaExceededError": 3,
				"timeoutError":       2,
			},
		},
		{
			name:                 "feature is enabled, but errors are empty",
			caVersion:            "35.197.0",
			experimentFlagValue:  `{"status": "STATUS_ENABLED", "minCaVersion": "0.0.0"}`,
			wantCustomThresholds: map[string]int{},
		},
		{
			name:                 "flag not set (failsafe)",
			caVersion:            "35.197.0",
			experimentFlagValue:  "",
			wantFeatureDisabled:  true,
			wantCustomThresholds: map[string]int{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stringFlags := map[string]string{}
			if tc.experimentFlagValue != "" {
				stringFlags[experiments.ResizableClusterBackoffCustomThresholdsPerErrorTypeFlag] = tc.experimentFlagValue
			}
			mockGM := experiments.NewMockManagerWithOptions(version.Version{}, nil, stringFlags)

			caVersion := getCaVersion(tc.caVersion)
			provider := ekvms_customthresholds.NewCustomThresholdsProvider(mockGM, caVersion)
			manager := NewManager(nil, provider, nil)

			for errType, wantThreshold := range tc.wantCustomThresholds {
				gotThreshold, found := manager.customThresholdsProvider.GetThreshold(errType)
				assert.True(t, found)
				assert.Equal(t, wantThreshold, gotThreshold)
			}
			assert.Equal(t, tc.wantFeatureDisabled, !provider.IsErrorThresholdsFeatureEnabled())
		})
	}
}

func TestNodeLevelBackoffTriggersClusterLevelBackoffWithExperimentManager(t *testing.T) {
	fakeClock := clock.NewFakePassiveClock(time.Now())
	ekNg := gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
		MachineType: "ek-standard-32",
	}).Build()
	mockCP := &mockCloudProvider{}
	mockCP.On("NodeGroupForNode").Return(ekNg, nil)

	ek1 := test.EkNode32("node1", 1, 1)
	ek2 := test.EkNode8("node2", 1, 1)
	ek3 := test.EkNode8("node3", 1, 1)
	ek4 := test.EkNode32("node4", 1, 1)
	ek5 := test.EkNode32("node5", 1, 1)
	ekNodes := []*v1.Node{ek1, ek2, ek3, ek4}
	family, err := resizable_vm_utils.GetMachineFamilyName(ek1)
	assert.NoError(t, err)
	quotaError, _ := ek_errors.ToResizeError(ek_errors.NewQuotaExceededError("ek", ek_errors.GetInstance, errors.New("quota error")))
	guestAgentResizeTimeoutError, _ := ek_errors.ToResizeError(ek_errors.NewGuestAgentResizeTimeout("ek", errors.New("guest agent resize timeout error")))
	testCases := []struct {
		desc                        string
		quotaErrorCount             int
		guestAgentResizeErrorCount  int
		caVersion                   string
		experimentFlagValue         string
		expectedClusterLevelBackoff bool
	}{
		{
			desc:                       "minCAVersion is too big -> rollback to calculate the total amount of errors per node (4 > threshold)",
			quotaErrorCount:            2,
			guestAgentResizeErrorCount: 2,
			caVersion:                  "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "999.999.999",
  "errors": [
    {
      "name": "quotaExceededError",
      "threshold": 3
    }
  ]
}`,
			expectedClusterLevelBackoff: true,
		},
		{
			desc:                       "status is disabled -> rollback to calculate the total amount of errors per node (3 >= threshold)",
			quotaErrorCount:            2,
			guestAgentResizeErrorCount: 1,
			caVersion:                  "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_DISABLED",
  "minCaVersion": "0.0.0",
  "errors": [
    {
      "name": "quotaExceededError",
      "threshold": 2
    }
  ],
  "errorThresholdsDisabled": false
}`,
			expectedClusterLevelBackoff: true,
		},
		{
			desc:                       "no thresholds specified and feature is fisabled  -> no cluster level backoff is triggered (2 < threshold)",
			quotaErrorCount:            2,
			guestAgentResizeErrorCount: 0,
			caVersion:                  "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "35.197.0",
  "errors": [],
  "errorThresholdsDisabled": true
}`,
			expectedClusterLevelBackoff: false,
		},
		{
			desc:                       "no thresholds specified, but feature is enabled -> do not trigger cluster level backoff for any error type",
			quotaErrorCount:            3,
			guestAgentResizeErrorCount: 4,
			caVersion:                  "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "35.197.0",
  "errors": []
}`,
			expectedClusterLevelBackoff: false,
		},
		{
			desc:                       "thresholds specified for one error type, for the other we do not trigger cluster backoff",
			quotaErrorCount:            1,
			guestAgentResizeErrorCount: 3,
			caVersion:                  "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "0.0.0",
  "errors": [
    {
      "name": "quotaExceededError",
      "threshold": 2
    }
  ]
}`,
			expectedClusterLevelBackoff: false,
		},
		{
			desc:                       "thresholds specified for two error types -> too many guestAgentResizeErrorCount errors",
			quotaErrorCount:            1,
			guestAgentResizeErrorCount: 3,
			caVersion:                  "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "0.0.0",
  "errors": [
    {
      "name": "quotaExceededError",
      "threshold": 2
    },
    {
      "name": "guestAgentResizeTimeout",
      "threshold": 3
    }
  ]
}`,
			expectedClusterLevelBackoff: true,
		},
		{
			desc:                       "thresholds specified for two other error types -> quota and guestAgentResize does not trigger the cluster level backoff",
			quotaErrorCount:            1,
			guestAgentResizeErrorCount: 3,
			caVersion:                  "35.197.0",
			experimentFlagValue: `{
  "status": "STATUS_ENABLED",
  "minCaVersion": "0.0.0",
  "errors": [
    {
      "name": "rateLimitExceededError",
      "threshold": 2
    },
    {
      "name": "instanceIsBusyError",
      "threshold": 3
    }
  ]
}`,
			expectedClusterLevelBackoff: false,
		},
		{
			desc:                        "experiment flag is not set properly -> rollback to calculate the total amount of errors per node (3 >= threshold)",
			quotaErrorCount:             1,
			guestAgentResizeErrorCount:  2,
			caVersion:                   "35.197.0",
			experimentFlagValue:         "",
			expectedClusterLevelBackoff: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stringFlags := map[string]string{}
			if tc.experimentFlagValue != "" {
				stringFlags[experiments.ResizableClusterBackoffCustomThresholdsPerErrorTypeFlag] = tc.experimentFlagValue
			}
			caVersion, _ := version.FromString(tc.caVersion)
			mockGM := experiments.NewMockManagerWithOptions(version.Version{}, nil, stringFlags)
			customThresholdsProvider := ekvms_customthresholds.NewCustomThresholdsProvider(mockGM, caVersion)
			backoffManager := NewManager(mockCP, customThresholdsProvider, fakeClock)
			assert.False(t, backoffManager.isClusterBackedOff(family))
			for i := range tc.quotaErrorCount {
				backoffManager.Backoff(ekNodes[i], *quotaError)
			}
			for i := range tc.guestAgentResizeErrorCount {
				backoffManager.Backoff(ekNodes[i], *guestAgentResizeTimeoutError)
			}
			// Check the status of the 5th node that we didn't explicitly backoff.
			assert.Equal(t, tc.expectedClusterLevelBackoff, backoffManager.IsBackedOff(family, ek5.Name))
			_, found := backoffManager.nodeBasedBackoffs[family]
			assert.Equal(t, tc.expectedClusterLevelBackoff, found)
		})
	}

}

type mockCloudProvider struct {
	mock.Mock
	cloudProvider
}

func (m *mockCloudProvider) NodeGroupForNode(_ *v1.Node) (cloudprovider.NodeGroup, error) {
	args := m.MethodCalled("NodeGroupForNode")
	return args.Get(0).(cloudprovider.NodeGroup), args.Error(1)
}

func (m *mockCloudProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}

type mockClusterBackoff struct {
	mock.Mock
	exponentialBackoff
}

func (m *mockClusterBackoff) BackoffUntil() time.Time {
	return m.MethodCalled("BackoffUntil").Get(0).(time.Time)
}

type mockNodeBackoff struct {
	mock.Mock
	nodetracker.Interface
}

func (m *mockNodeBackoff) Count() (int, time.Time) {
	args := m.MethodCalled("Count")
	return args.Get(0).(int), args.Get(1).(time.Time)
}

func (m *mockNodeBackoff) IsTracked(nodeId string) bool {
	return m.MethodCalled("IsTracked", nodeId).Bool(0)
}

func (m *mockNodeBackoff) DeleteNode(nodeId string) {
	m.MethodCalled("DeleteNode", nodeId)
}

type mockMetrics struct {
	mock.Mock
}

func (m *mockMetrics) UpdateEkBackoffStatus(isBackedOff bool) {
	m.Called(isBackedOff)
}

func (m *mockMetrics) UpdateResizeBackoffStatus(machineFamily string, isBackedOff bool) {
	m.Called(machineFamily, isBackedOff)
}

type mockCustomThresholdsProvider struct {
	mock.Mock
}

func (m *mockCustomThresholdsProvider) RefreshCustomThresholds() {
	m.Called()
}

func (m *mockCustomThresholdsProvider) GetThreshold(errorType string) (int, bool) {
	args := m.Called(errorType)
	return args.Int(0), args.Bool(1)
}

func (m *mockCustomThresholdsProvider) IsErrorThresholdsFeatureEnabled() bool {
	return m.Called().Bool(0)
}

func (m *mockCustomThresholdsProvider) IsForceScaleUpFeatureEnabled() bool {
	return m.Called().Bool(0)
}

func (m *mockCustomThresholdsProvider) GetUpsizeTriesThreshold() int {
	return m.Called().Int(0)
}

func getCaVersion(caVersionString string) version.Version {
	caVersion, _ := version.FromString(caVersionString)
	return caVersion
}
