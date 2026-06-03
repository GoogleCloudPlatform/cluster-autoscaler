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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

var Now = time.Now()

func TestReservationBackOff(t *testing.T) {
	backoffDuration := 2 * time.Second
	maxBackoffDuration := 5 * time.Second
	timeBetweenCalls := 2 * time.Second
	reservation := map[string]string{
		labels.ReservationNameLabel:    "test-reservation",
		labels.ReservationProjectLabel: "test-project",
		apiv1.LabelTopologyZone:        "test-zone",
		labels.MachineFamilyLabel:      "ct4p",
	}
	reservationWithOtherZone := map[string]string{
		labels.ReservationNameLabel:    "test-reservation",
		labels.ReservationProjectLabel: "test-project",
		apiv1.LabelTopologyZone:        "test-other-zone",
		labels.MachineFamilyLabel:      "ct4p",
	}
	reservationWithBlock := map[string]string{
		labels.ReservationNameLabel:    "test-reservation",
		labels.ReservationBlocksLabel:  "test-block",
		labels.ReservationProjectLabel: "test-project",
		apiv1.LabelTopologyZone:        "test-zone",
		labels.MachineFamilyLabel:      "ct4p",
	}
	reservationWithOtherBlock := map[string]string{
		labels.ReservationNameLabel:    "test-reservation",
		labels.ReservationBlocksLabel:  "other-block",
		labels.ReservationProjectLabel: "test-project",
		apiv1.LabelTopologyZone:        "test-zone",
		labels.MachineFamilyLabel:      "ct4p",
	}
	reservationWithSubBlock := map[string]string{
		labels.ReservationNameLabel:      "test-reservation",
		labels.ReservationBlocksLabel:    "test-block",
		labels.ReservationSubBlocksLabel: "test-subblock",
		labels.ReservationProjectLabel:   "test-project",
		apiv1.LabelTopologyZone:          "test-zone",
		labels.MachineFamilyLabel:        "ct4p",
	}
	reservationWithOtherSubBlock := map[string]string{
		labels.ReservationNameLabel:      "test-reservation",
		labels.ReservationBlocksLabel:    "test-block",
		labels.ReservationSubBlocksLabel: "test-other-subblock",
		labels.ReservationProjectLabel:   "test-project",
		apiv1.LabelTopologyZone:          "test-zone",
		labels.MachineFamilyLabel:        "ct4p",
	}
	reservationInOtherProject := map[string]string{
		labels.ReservationNameLabel:    "test-reservation",
		labels.ReservationProjectLabel: "other-project",
		apiv1.LabelTopologyZone:        "test-zone",
		labels.MachineFamilyLabel:      "ct4p",
	}
	otherReservation := map[string]string{
		labels.ReservationNameLabel:    "test-reservation-other",
		labels.ReservationProjectLabel: "test-project",
		apiv1.LabelTopologyZone:        "test-zone",
		labels.MachineFamilyLabel:      "ct4p",
	}
	reservationWithOtherMachineType := map[string]string{
		labels.ReservationNameLabel:    "res-test",
		labels.ReservationProjectLabel: "res-project",
		apiv1.LabelTopologyZone:        "test-zone",
		labels.MachineFamilyLabel:      "ct4l",
	}
	onlyMachineType := map[string]string{
		labels.MachineFamilyLabel: "ct4p",
	}
	onlyZone := map[string]string{
		apiv1.LabelTopologyZone: "test-zone",
	}
	onlyMachineTypeAndProject := map[string]string{
		labels.ReservationProjectLabel: "test-project",
		labels.MachineFamilyLabel:      "ct4p",
	}
	testCases := []struct {
		name                string
		deploymentType      gke.DeploymentTypeEnum
		errsPassedToBackoff []cloudprovider.InstanceErrorInfo
		nodeLabels          []map[string]string
		wantBackoffStatus   []backoff.Status
		wantBackoffDuration []time.Duration
	}{
		{
			name:                "2 Backoff calls, none triggers backoff",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{quotaError, quotaError},
			nodeLabels:          []map[string]string{reservation, reservation},
			wantBackoffStatus:   []backoff.Status{noBackoff, noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second, 0 * time.Second},
		},
		{
			name:                "1 Backoff call with RESERVATION_NOT_READY",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError},
			nodeLabels:          []map[string]string{reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationNotReady},
			wantBackoffDuration: []time.Duration{2 * time.Second},
		},
		{
			name:                "1 Backoff call with INVALID_RESERVATION",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError},
			nodeLabels:          []map[string]string{reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithInvalidReservation},
			wantBackoffDuration: []time.Duration{2 * time.Second},
		},
		{
			name:                "1 Backoff call with INVALID_RESERVATION then non related backoff calls",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, quotaError, quotaError, quotaError},
			nodeLabels:          []map[string]string{reservation, reservation, reservation, reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithInvalidReservation, backoffWithInvalidReservation, noBackoff, noBackoff},
			wantBackoffDuration: []time.Duration{2 * time.Second, 0 * time.Second, 0 * time.Second, 0 * time.Second},
		},
		{
			name:                "2 Backoff call with INVALID_RESERVATION",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, invalidReservationError},
			nodeLabels:          []map[string]string{reservation, reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithInvalidReservation, backoffWithInvalidReservation},
			wantBackoffDuration: []time.Duration{2 * time.Second, 4 * time.Second},
		},
		{
			name:                "2 consecutive Backoff calls with RESERVATION_NOT_READY",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError, reservationNotReadyError},
			nodeLabels:          []map[string]string{reservation, reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationNotReady, backoffWithReservationNotReady},
			wantBackoffDuration: []time.Duration{2 * time.Second, 4 * time.Second},
		},
		{
			name:                "2 consecutive Backoff calls with RESERVATION_NOT_READY with different reservations",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError, reservationNotReadyError},
			nodeLabels:          []map[string]string{reservation, reservationInOtherProject},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationNotReady, backoffWithReservationNotReady},
			wantBackoffDuration: []time.Duration{2 * time.Second, 2 * time.Second},
		},
		{
			name:                "2 consecutive Backoff calls with RESERVATION_NOT_READY with different reservation zones",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError, reservationNotReadyError},
			nodeLabels:          []map[string]string{reservation, reservationWithOtherZone},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationNotReady, backoffWithReservationNotReady},
			wantBackoffDuration: []time.Duration{2 * time.Second, 2 * time.Second},
		},
		{
			name:                "3 consecutive Backoff calls with RESERVATION_NOT_READY with different reservations",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError, reservationNotReadyError, reservationNotReadyError},
			nodeLabels:          []map[string]string{reservationInOtherProject, otherReservation, reservationWithOtherMachineType},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationNotReady, backoffWithReservationNotReady, backoffWithReservationNotReady},
			wantBackoffDuration: []time.Duration{2 * time.Second, 2 * time.Second, 2 * time.Second},
		},
		{
			name:                "4 consecutive Backoff calls with RESERVATION_NOT_READY with repeating reservations",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError, reservationNotReadyError, reservationNotReadyError, reservationNotReadyError},
			nodeLabels:          []map[string]string{reservation, reservationInOtherProject, reservation, reservationInOtherProject},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationNotReady, backoffWithReservationNotReady, backoffWithReservationNotReady, backoffWithReservationNotReady},
			wantBackoffDuration: []time.Duration{2 * time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second},
		},
		{
			name:                "3 consecutive Backoff calls with RESERVATION_NOT_READY capping the backoff duration",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError, reservationNotReadyError, reservationNotReadyError},
			nodeLabels:          []map[string]string{reservation, reservation, reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationNotReady, backoffWithReservationNotReady, backoffWithReservationNotReady},
			wantBackoffDuration: []time.Duration{2 * time.Second, 4 * time.Second, 5 * time.Second},
		},
		{
			name:                "1 Backoff call with INVALID_RESERVATION without reservation name and project",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError},
			nodeLabels:          []map[string]string{onlyMachineType},
			wantBackoffStatus:   []backoff.Status{noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second},
		},
		{
			name:                "1 Backoff call with INVALID_RESERVATION with only zone",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError},
			nodeLabels:          []map[string]string{onlyZone},
			wantBackoffStatus:   []backoff.Status{noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second},
		},
		{
			name:                "2 Backoff calls with INVALID_RESERVATION without reservation name and project",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, invalidReservationError},
			nodeLabels:          []map[string]string{onlyMachineType, onlyMachineType},
			wantBackoffStatus:   []backoff.Status{noBackoff, noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second, 0 * time.Second},
		},
		{
			name:                "1 Backoff call with INVALID_RESERVATION without reservation name only",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError},
			nodeLabels:          []map[string]string{onlyMachineTypeAndProject},
			wantBackoffStatus:   []backoff.Status{noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second},
		},
		{
			name:                "2 Backoff calls with INVALID_RESERVATION without reservation name only",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, invalidReservationError},
			nodeLabels:          []map[string]string{onlyMachineTypeAndProject, onlyMachineTypeAndProject},
			wantBackoffStatus:   []backoff.Status{noBackoff, noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second, 0 * time.Second},
		},
		{
			name:                "1 Backoff call with RESERVATION_NOT_READY without reservation name and project",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError},
			nodeLabels:          []map[string]string{onlyMachineType},
			wantBackoffStatus:   []backoff.Status{noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second},
		},
		{
			name:                "2 Backoff calls with RESERVATION_NOT_READY without reservation name and project",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError, reservationNotReadyError},
			nodeLabels:          []map[string]string{onlyMachineType, onlyMachineType},
			wantBackoffStatus:   []backoff.Status{noBackoff, noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second, 0 * time.Second},
		},
		{
			name:                "1 Backoff call with RESERVATION_NOT_READY without reservation name only",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError},
			nodeLabels:          []map[string]string{onlyMachineTypeAndProject},
			wantBackoffStatus:   []backoff.Status{noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second},
		},
		{
			name:                "1 Backoff call with RESERVATION_NOT_READY without reservation name only",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationNotReadyError, reservationNotReadyError},
			nodeLabels:          []map[string]string{onlyMachineTypeAndProject, onlyMachineTypeAndProject},
			wantBackoffStatus:   []backoff.Status{noBackoff, noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second, 0 * time.Second},
		},
		{
			name:                "Backoff differentiates between reservation with and without a block",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, invalidReservationError},
			nodeLabels:          []map[string]string{reservation, reservationWithBlock},
			wantBackoffStatus:   []backoff.Status{backoffWithInvalidReservation, backoffWithInvalidReservation},
			wantBackoffDuration: []time.Duration{2 * time.Second, 2 * time.Second},
		},
		{
			name:                "Backoff differentiates between two different blocks",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, invalidReservationError},
			nodeLabels:          []map[string]string{reservationWithBlock, reservationWithOtherBlock},
			wantBackoffStatus:   []backoff.Status{backoffWithInvalidReservation, backoffWithInvalidReservation},
			wantBackoffDuration: []time.Duration{2 * time.Second, 2 * time.Second},
		},
		{
			name:                "Backoff differentiates between a block and a sub-block",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, invalidReservationError},
			nodeLabels:          []map[string]string{reservationWithBlock, reservationWithSubBlock},
			wantBackoffStatus:   []backoff.Status{backoffWithInvalidReservation, backoffWithInvalidReservation},
			wantBackoffDuration: []time.Duration{2 * time.Second, 2 * time.Second},
		},
		{
			name:                "Backoff differentiates between two different sub-blocks",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, invalidReservationError},
			nodeLabels:          []map[string]string{reservationWithSubBlock, reservationWithOtherSubBlock},
			wantBackoffStatus:   []backoff.Status{backoffWithInvalidReservation, backoffWithInvalidReservation},
			wantBackoffDuration: []time.Duration{2 * time.Second, 2 * time.Second},
		},
		{
			name:                "Exponential backoff works for a reservation with a block",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, invalidReservationError},
			nodeLabels:          []map[string]string{reservationWithBlock, reservationWithBlock},
			wantBackoffStatus:   []backoff.Status{backoffWithInvalidReservation, backoffWithInvalidReservation},
			wantBackoffDuration: []time.Duration{2 * time.Second, 4 * time.Second},
		},
		{
			name:                "Exponential backoff works for a reservation with a sub-block",
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{invalidReservationError, invalidReservationError},
			nodeLabels:          []map[string]string{reservationWithSubBlock, reservationWithSubBlock},
			wantBackoffStatus:   []backoff.Status{backoffWithInvalidReservation, backoffWithInvalidReservation},
			wantBackoffDuration: []time.Duration{2 * time.Second, 4 * time.Second},
		},
		{
			name:                "1 Backoff call with RESERVATION_CAPACITY_EXCEEDED",
			deploymentType:      gke.DeploymentTypeUnspecified,
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationCapacityExceededError},
			nodeLabels:          []map[string]string{reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationCapacityExceeded},
			wantBackoffDuration: []time.Duration{2 * time.Second},
		},
		{
			name:                "2 consecutive Backoff calls with RESERVATION_CAPACITY_EXCEEDED",
			deploymentType:      gke.DeploymentTypeUnspecified,
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationCapacityExceededError, reservationCapacityExceededError},
			nodeLabels:          []map[string]string{reservation, reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationCapacityExceeded, backoffWithReservationCapacityExceeded},
			wantBackoffDuration: []time.Duration{2 * time.Second, 4 * time.Second},
		},
		{
			name:                "1 Backoff call with RESERVATION_CAPACITY_EXCEEDED then non related backoff calls",
			deploymentType:      gke.DeploymentTypeUnspecified,
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{reservationCapacityExceededError, randomError, randomError, randomError},
			nodeLabels:          []map[string]string{reservation, reservation, reservation, reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithReservationCapacityExceeded, backoffWithReservationCapacityExceeded, noBackoff, noBackoff},
			wantBackoffDuration: []time.Duration{2 * time.Second, 0 * time.Second, 0 * time.Second, 0 * time.Second},
		},
		{
			name:                "1 Backoff call with stockout error on mig with no none deployment type",
			deploymentType:      gke.DeploymentTypeNone,
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{stockoutError},
			nodeLabels:          []map[string]string{reservation},
			wantBackoffStatus:   []backoff.Status{noBackoff},
			wantBackoffDuration: []time.Duration{0 * time.Second},
		},
		{
			name:                "1 Backoff call with stockout error on mig with unspecified deployment type",
			deploymentType:      gke.DeploymentTypeUnspecified,
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{stockoutError},
			nodeLabels:          []map[string]string{reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithStockoutError},
			wantBackoffDuration: []time.Duration{2 * time.Second},
		},
		{
			name:                "1 Backoff call with stockout error with on mig with dense deployment type",
			deploymentType:      gke.DeploymentTypeDense,
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{stockoutError},
			nodeLabels:          []map[string]string{reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithStockoutError},
			wantBackoffDuration: []time.Duration{2 * time.Second},
		},
		{
			name:                "1 backoff call with stockout and then with quota error then unrelated error",
			deploymentType:      gke.DeploymentTypeDense,
			errsPassedToBackoff: []cloudprovider.InstanceErrorInfo{stockoutError, quotaError, randomError},
			nodeLabels:          []map[string]string{reservation, reservation, reservation},
			wantBackoffStatus:   []backoff.Status{backoffWithStockoutError, backoffWithStockoutError, noBackoff},
			wantBackoffDuration: []time.Duration{2 * time.Second, 0 * time.Second, 0 * time.Second},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewReservationsBackoff(backoffDuration, maxBackoffDuration, &mockProvider{})

			deploymentType := tc.deploymentType
			if deploymentType == "" {
				deploymentType = gke.DeploymentTypeNone
			}
			node := test.BuildTestNode("test-node", 1000, 1000)
			ni := framework.NewTestNodeInfo(node)
			ng := gke.NewTestGkeMigBuilder().SetDeploymentType(deploymentType).Build()
			currentTime := Now

			for i, err := range tc.errsPassedToBackoff {
				currentTime = currentTime.Add(timeBetweenCalls)
				node.ObjectMeta.SetLabels(tc.nodeLabels[i])
				endTime := b.Backoff(ng, ni, err, currentTime)
				gotBackoffDuration := 0 * time.Second
				if endTime.After(currentTime) {
					gotBackoffDuration = endTime.Sub(currentTime)
				}
				b.RemoveStaleBackoffData(currentTime)

				assert.Equal(t, tc.wantBackoffStatus[i], b.BackoffStatus(ng, ni, currentTime))
				assert.Equal(t, tc.wantBackoffDuration[i], gotBackoffDuration)
			}
		})
	}
}

func TestBackoffStatus(t *testing.T) {
	backoffDuration := 100 * time.Second
	maxBackoffDuration := 200 * time.Second
	outOfResourcesErrorInfo := cloudprovider.InstanceErrorInfo{
		ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
		ErrorCode:    gce.ErrorCodeResourcePoolExhausted,
		ErrorMessage: "Out of resources",
	}
	invalidReservationErrorInfo := cloudprovider.InstanceErrorInfo{
		ErrorClass:   cloudprovider.OtherErrorClass,
		ErrorCode:    gce.ErrorInvalidReservation,
		ErrorMessage: "invalid reservation",
	}

	testReservationName := "res-name"
	testReservationBlock := "res-block"
	testReservationSublock := "res-subblock"
	testMachineFamily := "machine-family"

	nodeLabels := map[string]string{
		labels.ReservationNameLabel:      testReservationName,
		labels.ReservationBlocksLabel:    testReservationBlock,
		labels.ReservationSubBlocksLabel: testReservationSublock,
		labels.MachineFamilyLabel:        testMachineFamily,
	}
	nodeInfo := newNodeInfo(nodeLabels)
	testCases := []struct {
		name            string
		nodeGroup       cloudprovider.NodeGroup
		nodeInfo        *framework.NodeInfo
		backoffTime     time.Time
		wantErrorInfo   cloudprovider.InstanceErrorInfo
		wantIsBackedOff bool
	}{
		{
			name:            "Nil nodeGroup",
			nodeGroup:       nil,
			nodeInfo:        nil,
			wantIsBackedOff: false,
		},
		{
			name:            "Nil nodeInfo",
			nodeGroup:       gke.NewTestGkeMigBuilder().SetDeploymentType(gke.DeploymentTypeDense).Build(),
			nodeInfo:        nil,
			wantIsBackedOff: false,
		},
		{
			name:            "Not a reservation deployment, not an invalid reservation",
			nodeGroup:       gke.NewTestGkeMigBuilder().SetDeploymentType(gke.DeploymentTypeNone).Build(),
			nodeInfo:        nodeInfo,
			wantIsBackedOff: false,
		},
		{
			name:            "Invalid labels for key",
			nodeGroup:       gke.NewTestGkeMigBuilder().SetDeploymentType(gke.DeploymentTypeDense).Build(),
			nodeInfo:        newNodeInfo(map[string]string{}),
			wantIsBackedOff: false,
		},
		{
			name:            "Backed off invalid reservation",
			nodeGroup:       gke.NewTestGkeMigBuilder().Build(),
			nodeInfo:        nodeInfo,
			backoffTime:     Now,
			wantErrorInfo:   invalidReservationErrorInfo,
			wantIsBackedOff: true,
		},
		{
			name:            "Backed off dense",
			nodeGroup:       gke.NewTestGkeMigBuilder().SetDeploymentType(gke.DeploymentTypeDense).Build(),
			nodeInfo:        nodeInfo,
			backoffTime:     Now.Add(-1 * backoffDuration / 2),
			wantErrorInfo:   outOfResourcesErrorInfo,
			wantIsBackedOff: true,
		},
		{
			name:            "Backed off unspecified",
			nodeGroup:       gke.NewTestGkeMigBuilder().SetDeploymentType(gke.DeploymentTypeUnspecified).Build(),
			nodeInfo:        nodeInfo,
			backoffTime:     Now.Add(-1 * backoffDuration / 2),
			wantErrorInfo:   outOfResourcesErrorInfo,
			wantIsBackedOff: true,
		},
		{
			name:            "Backoff expired",
			nodeGroup:       gke.NewTestGkeMigBuilder().SetDeploymentType(gke.DeploymentTypeDense).Build(),
			nodeInfo:        nodeInfo,
			backoffTime:     Now.Add(-2 * backoffDuration),
			wantErrorInfo:   outOfResourcesErrorInfo,
			wantIsBackedOff: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewReservationsBackoff(backoffDuration, maxBackoffDuration, &mockProvider{})
			if !tc.backoffTime.IsZero() {
				b.Backoff(tc.nodeGroup, tc.nodeInfo, tc.wantErrorInfo, tc.backoffTime)
			}

			status := b.BackoffStatus(tc.nodeGroup, tc.nodeInfo, Now)
			assert.Equal(t, tc.wantIsBackedOff, status.IsBackedOff)
			if tc.wantIsBackedOff {
				assert.Equal(t, tc.wantErrorInfo, status.ErrorInfo)
			}
		})
	}
}
