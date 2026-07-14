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
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

// these are used in more than 1 test
var (
	futureReservationName = "test-future-reservation-01"
	projectID             = "test-gke-project"
	correctLabels         = map[string]string{
		labels.ReservationNameLabel:     futureReservationName,
		labels.ReservationAffinityLabel: "specific",
		labels.ReservationProjectLabel:  projectID,
	}
)

func TestBackoffAndStatus(t *testing.T) {
	// RFC3389 doesn't have fractions of seconds
	currentTime := time.Now().UTC().Truncate(time.Second)
	newStdFR := func() *gceclient.GceFutureReservation {
		return newReadyFR(futureReservationName, currentTime.AddDate(0, 1, 0))
	}

	testCases := []struct {
		name               string
		futureReservations []*gceclient.GceFutureReservation
		nodeLabels         map[string]string
		wantBackoffTime    time.Time
		wantBackoffStatus  backoff.Status
	}{
		{
			name:               "no reservation name in labels",
			futureReservations: []*gceclient.GceFutureReservation{newStdFR()},
			nodeLabels:         copyWithout(correctLabels, labels.ReservationNameLabel),
			wantBackoffTime:    currentTime,
			wantBackoffStatus:  noBackoff,
		},
		{
			name:               "no project id in labels",
			futureReservations: []*gceclient.GceFutureReservation{newStdFR()},
			nodeLabels:         copyWithout(correctLabels, labels.ReservationProjectLabel),
			wantBackoffTime:    currentTime,
			wantBackoffStatus:  noBackoff,
		},
		{
			name:               "different project id in labels",
			futureReservations: []*gceclient.GceFutureReservation{newStdFR()},
			nodeLabels:         copyAndChange(correctLabels, labels.ReservationProjectLabel, "different project"),
			wantBackoffTime:    currentTime,
			wantBackoffStatus:  noBackoff,
		},
		{
			name:               "no future reservations",
			futureReservations: []*gceclient.GceFutureReservation{},
			nodeLabels:         correctLabels,
			wantBackoffTime:    currentTime,
			wantBackoffStatus:  noBackoff,
		},
		{
			name:               "no matching future reservation",
			futureReservations: []*gceclient.GceFutureReservation{newReadyFR("different FR name", currentTime.AddDate(0, 1, 0))},
			nodeLabels:         correctLabels,
			wantBackoffTime:    currentTime,
			wantBackoffStatus:  noBackoff,
		},
		{
			name:               "future reservation in the past",
			futureReservations: []*gceclient.GceFutureReservation{newReadyFR(futureReservationName, currentTime.AddDate(0, -1, 0))},
			nodeLabels:         correctLabels,
			wantBackoffTime:    currentTime,
			wantBackoffStatus:  noBackoff,
		},
		{
			name:               "positive test case",
			futureReservations: []*gceclient.GceFutureReservation{newStdFR()},
			nodeLabels:         correctLabels,
			wantBackoffTime:    currentTime.AddDate(0, 1, 0).Add(-30 * time.Minute),
			wantBackoffStatus:  backoffWithReservationNotReady,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			providerMock := frProviderMock{tc.futureReservations}
			config := FutureReservationsBackoffConfig{
				Enabled:   true,
				Provider:  &providerMock,
				ProjectID: projectID,
			}
			frBackoff := NewFutureReservationsBackoff(&config)

			nodeGroup := newFRMig()
			nodeInfo := newNodeInfo(tc.nodeLabels)
			errorInfo := reservationNotReadyError

			backoffTime := frBackoff.Backoff(nodeGroup, nodeInfo, errorInfo, currentTime)
			backoffStatus := frBackoff.BackoffStatus(nodeGroup, nodeInfo, currentTime)

			assert.Equal(t, tc.wantBackoffTime, backoffTime)
			assertBackoffStatus(t, tc.wantBackoffStatus, backoffStatus)
		})
	}
}

func assertBackoffStatus(t assert.TestingT, want backoff.Status, actual backoff.Status) {
	assert.Equal(t, want.IsBackedOff, actual.IsBackedOff)
}

func TestRemoveBackoff(t *testing.T) {
	// RFC3389 doesn't have fractions of seconds
	currentTime := time.Now().UTC().Truncate(time.Second)
	newStdFR := func() *gceclient.GceFutureReservation {
		return newReadyFR(futureReservationName, currentTime.AddDate(0, 1, 0))
	}
	nodeInfo := newNodeInfo(correctLabels)
	testCases := []struct {
		name           string
		nodeInfo       *framework.NodeInfo
		nodeInfoRemove *framework.NodeInfo
		wantStatus     backoff.Status
	}{
		{
			name:           "nil nodeInfo to remove",
			nodeInfo:       nodeInfo,
			nodeInfoRemove: nil,
			wantStatus:     backoffWithReservationNotReady,
		},
		{
			name:           "positive test case",
			nodeInfo:       nodeInfo,
			nodeInfoRemove: nodeInfo,
			wantStatus:     noBackoff,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			futureReservations := []*gceclient.GceFutureReservation{newStdFR()}
			providerMock := frProviderMock{futureReservations}
			config := FutureReservationsBackoffConfig{
				Enabled:   true,
				Provider:  &providerMock,
				ProjectID: projectID,
			}
			frBackoff := NewFutureReservationsBackoff(&config)

			nodeGroup := newFRMig()

			frBackoff.Backoff(nodeGroup, tc.nodeInfo, reservationNotReadyError, currentTime)

			statusAfterBackoff := frBackoff.BackoffStatus(nodeGroup, tc.nodeInfo, currentTime)
			assertBackoffStatus(t, backoffWithReservationNotReady, statusAfterBackoff)

			frBackoff.RemoveBackoff(nodeGroup, tc.nodeInfoRemove)
			statusAfterRemove := frBackoff.BackoffStatus(nodeGroup, tc.nodeInfo, currentTime)
			assertBackoffStatus(t, tc.wantStatus, statusAfterRemove)
		})
	}
}

func TestRemoveStaleBackoffData(t *testing.T) {
	// RFC3389 doesn't have fractions of seconds
	currentTime := time.Now().UTC().Truncate(time.Second)
	fr1Name := "fr-01"
	fr2Name := "fr-02"
	fr1Time := currentTime.AddDate(0, 1, 0)
	fr2Time := currentTime.AddDate(0, 2, 0)
	futureReservations := []*gceclient.GceFutureReservation{
		newReadyFR(fr1Name, fr1Time),
		newReadyFR(fr2Name, fr2Time),
	}
	providerMock := frProviderMock{futureReservations}
	frBackoff := frBackoff{
		provider:  &providerMock,
		projectID: projectID,
		backoffs:  make(map[frBackoffKey]frBackoffValue),
	}
	nodeGroup := newFRMig()
	nodeInfo1 := newNodeInfo(copyAndChange(correctLabels, labels.ReservationNameLabel, fr1Name))
	nodeInfo2 := newNodeInfo(copyAndChange(correctLabels, labels.ReservationNameLabel, fr2Name))
	assert.Equal(t, 0, len(frBackoff.backoffs))

	backoffKey := func(frName string) frBackoffKey {
		return frBackoffKey{nodeGroupId: nodeGroup.Id(), reservationName: frName}
	}

	fr1Backoff := frBackoffValue{backoffUntil: fr1Time.Add(-30 * time.Minute), originalErrorInfo: reservationNotReadyError}
	fr2Backoff := frBackoffValue{backoffUntil: fr2Time.Add(-30 * time.Minute), originalErrorInfo: reservationNotReadyError}

	frBackoff.Backoff(nodeGroup, nodeInfo1, reservationNotReadyError, currentTime)
	frBackoff.Backoff(nodeGroup, nodeInfo2, reservationNotReadyError, currentTime)
	assert.Equal(t, 2, len(frBackoff.backoffs))
	assertBackoffValue(t, fr1Backoff, frBackoff.backoffs[backoffKey(fr1Name)])
	assertBackoffValue(t, fr2Backoff, frBackoff.backoffs[backoffKey(fr2Name)])

	frBackoff.RemoveStaleBackoffData(currentTime)
	assert.Equal(t, 2, len(frBackoff.backoffs))
	assertBackoffValue(t, fr1Backoff, frBackoff.backoffs[backoffKey(fr1Name)])
	assertBackoffValue(t, fr2Backoff, frBackoff.backoffs[backoffKey(fr2Name)])

	frBackoff.RemoveStaleBackoffData(fr1Time)
	assert.Equal(t, 1, len(frBackoff.backoffs))
	assertBackoffValue(t, frBackoffValue{}, frBackoff.backoffs[backoffKey(fr1Name)])
	assertBackoffValue(t, fr2Backoff, frBackoff.backoffs[backoffKey(fr2Name)])

	frBackoff.RemoveStaleBackoffData(fr2Time)
	assert.Equal(t, 0, len(frBackoff.backoffs))
	assertBackoffValue(t, frBackoffValue{}, frBackoff.backoffs[backoffKey(fr1Name)])
	assertBackoffValue(t, frBackoffValue{}, frBackoff.backoffs[backoffKey(fr2Name)])
}

func assertBackoffValue(t assert.TestingT, expected frBackoffValue, actual frBackoffValue) {
	assert.Equal(t, expected.backoffUntil, actual.backoffUntil)
	assert.Equal(t, expected.originalErrorInfo, actual.originalErrorInfo)
}

func TestCreateKey(t *testing.T) {
	frBackoff := frBackoff{
		provider:  nil, // not relevant in this test
		projectID: projectID,
		backoffs:  nil, // not relevant in this test
	}
	nodeGroup := newFRMig()

	testCases := []struct {
		name     string
		nodeInfo *framework.NodeInfo
		wantKey  frBackoffKey
		wantOk   bool
	}{
		{
			name:     "nodeInfo=nil",
			nodeInfo: nil,
			wantKey:  frBackoffKey{},
			wantOk:   false,
		},
		{
			name:     "no reservation name in labels",
			nodeInfo: newNodeInfo(copyWithout(correctLabels, labels.ReservationNameLabel)),
			wantKey:  frBackoffKey{},
			wantOk:   false,
		},
		{
			name:     "no project in labels",
			nodeInfo: newNodeInfo(copyWithout(correctLabels, labels.ReservationProjectLabel)),
			wantKey:  frBackoffKey{},
			wantOk:   false,
		},
		{
			name:     "no project in labels",
			nodeInfo: newNodeInfo(copyAndChange(correctLabels, labels.ReservationProjectLabel, "other project")),
			wantKey:  frBackoffKey{},
			wantOk:   false,
		},
		{
			name:     "positive test case",
			nodeInfo: newNodeInfo(correctLabels),
			wantKey:  frBackoffKey{nodeGroupId: nodeGroup.Id(), reservationName: futureReservationName},
			wantOk:   true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := frBackoff.createKey(nodeGroup, tc.nodeInfo)
			assert.Equal(t, tc.wantOk, ok)
			assert.Equal(t, tc.wantKey, key)
		})
	}
}

func TestValidate(t *testing.T) {
	// RFC3389 doesn't have fractions of seconds
	currentTime := time.Now().UTC().Truncate(time.Second)
	tcs := []struct {
		name              string
		frName            string
		planningStatus    gceclient.PlanningStatusEnum
		procurementStatus gceclient.ProcurementStatusEnum
		startTime         time.Time
		wantCount         int
	}{
		{
			name:              "empty future reservation name",
			frName:            "",
			planningStatus:    gceclient.PlanningStatusSubmitted,
			procurementStatus: gceclient.ProcurementStatusApproved,
			startTime:         currentTime,
			wantCount:         0,
		},
		{
			name:              "wrong planning status",
			frName:            "test-fr",
			planningStatus:    gceclient.PlanningStatusDraft,
			procurementStatus: gceclient.ProcurementStatusApproved,
			startTime:         currentTime,
			wantCount:         0,
		},
		{
			name:              "wrong procurement status",
			frName:            "test-fr",
			planningStatus:    gceclient.PlanningStatusSubmitted,
			procurementStatus: gceclient.ProcurementStatusDeclined,
			startTime:         currentTime,
			wantCount:         0,
		},
		{
			name:              "StartTime in the past",
			frName:            "test-fr",
			planningStatus:    gceclient.PlanningStatusSubmitted,
			procurementStatus: gceclient.ProcurementStatusApproved,
			startTime:         currentTime.AddDate(0, 0, -1),
			wantCount:         0,
		},
		{
			name:              "positive test case",
			frName:            "test-fr",
			planningStatus:    gceclient.PlanningStatusSubmitted,
			procurementStatus: gceclient.ProcurementStatusApproved,
			startTime:         currentTime,
			wantCount:         1,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			input := []*gceclient.GceFutureReservation{newFR(tc.frName, tc.planningStatus, tc.procurementStatus, tc.startTime)}
			frs := validate(input, currentTime)
			assert.Equal(t, tc.wantCount, len(frs))
		})
	}
}

func newNodeInfo(nodeLabels map[string]string) *framework.NodeInfo {
	node := test.BuildTestNode("fr-test-node", 1000, 1000)
	node.ObjectMeta.SetLabels(nodeLabels)
	nodeInfo := framework.NewNodeInfo(node, nil)
	return nodeInfo
}

func copyAndChange(src map[string]string, k, v string) map[string]string {
	dst := copyWithout(src, k)
	dst[k] = v
	return dst
}

func copyWithout(src map[string]string, without string) map[string]string {
	dst := make(map[string]string)
	for k, v := range src {
		dst[k] = v
	}
	delete(dst, without)
	return dst
}

func newReadyFR(name string, startTime time.Time) *gceclient.GceFutureReservation {
	return newFR(name, gceclient.PlanningStatusSubmitted, gceclient.ProcurementStatusApproved, startTime)
}

func newFR(
	name string,
	planningStatus gceclient.PlanningStatusEnum,
	procurementStatus gceclient.ProcurementStatusEnum,
	startTime time.Time) *gceclient.GceFutureReservation {
	return &gceclient.GceFutureReservation{
		Id:                uint64(time.Now().UnixNano()),
		Name:              name,
		PlanningStatus:    planningStatus,
		ProcurementStatus: procurementStatus,
		StartTime:         startTime,
	}
}

func newFRMig() *gke.GkeMig {
	return gke.NewTestGkeMigBuilder().
		SetGceRefName("fr-ref").
		SetMaxSize(10).
		SetExist(true).
		SetNodePoolName("fr-pool").
		Build()
}

type frProviderMock struct {
	futureReservations []*gceclient.GceFutureReservation
}

func (m *frProviderMock) GetLocalFutureReservations() []*gceclient.GceFutureReservation {
	return m.futureReservations
}
