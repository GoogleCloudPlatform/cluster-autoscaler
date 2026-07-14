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

package api

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
)

func TestMarkUsed(t *testing.T) {
	testCases := []struct {
		name                      string
		initialZonalInstanceCount map[string]int
		initialProvisions         map[string]int
		zonalInstancesToProvision map[string]int
		decisionId                string
		guidanceId                string
		wantFinalProvisions       map[string]int
		wantNotification          ProvisioningDecisionNotification
	}{
		{
			name:                      "should increment provisions for a known zone",
			initialZonalInstanceCount: map[string]int{"us-central1-a": 10},
			initialProvisions:         map[string]int{"us-central1-a": 2},
			zonalInstancesToProvision: map[string]int{"us-central1-a": 3},
			decisionId:                "decision-abc",
			guidanceId:                "guidance-abc",
			wantFinalProvisions:       map[string]int{"us-central1-a": 5},
			wantNotification: ProvisioningDecisionNotification{
				instanceConfigKey:         "config-1",
				decisionId:                "decision-abc",
				guidanceId:                "guidance-abc",
				zonalInstancesToProvision: map[string]int{"us-central1-a": 3},
			},
		},
		{
			name:                      "should not increment provisions for an unknown zone",
			initialZonalInstanceCount: map[string]int{"us-central1-a": 10},
			initialProvisions:         map[string]int{"us-central1-a": 1},
			zonalInstancesToProvision: map[string]int{"us-central1-b": 5},
			decisionId:                "decision-abc",
			guidanceId:                "guidance-abc",
			wantFinalProvisions:       map[string]int{"us-central1-a": 1},
			wantNotification: ProvisioningDecisionNotification{
				instanceConfigKey:         "config-1",
				decisionId:                "decision-abc",
				guidanceId:                "guidance-abc",
				zonalInstancesToProvision: map[string]int{"us-central1-b": 5},
			},
		},
		{
			name:                      "should handle multiple zones correctly",
			initialZonalInstanceCount: map[string]int{"us-central1-a": 10, "us-central1-c": 20},
			initialProvisions:         map[string]int{"us-central1-a": 1, "us-central1-c": 2},
			zonalInstancesToProvision: map[string]int{"us-central1-a": 4, "us-central1-c": 5},
			decisionId:                "decision-abc",
			guidanceId:                "guidance-abc",
			wantFinalProvisions:       map[string]int{"us-central1-a": 5, "us-central1-c": 7},
			wantNotification: ProvisioningDecisionNotification{
				instanceConfigKey:         "config-1",
				decisionId:                "decision-abc",
				guidanceId:                "guidance-abc",
				zonalInstancesToProvision: map[string]int{"us-central1-a": 4, "us-central1-c": 5},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				decisionChan := make(chan ProvisioningDecisionNotification, 1)
				ia := NewTestInstanceAvailabilityBuilder("", "config-1").WithZonalInstanceCount(tc.initialZonalInstanceCount).WithZonalProvisionsSinceLastRefresh(tc.initialProvisions).Build()
				ia.SetDecisionChan(decisionChan)

				ia.MarkUsed(tc.zonalInstancesToProvision, tc.decisionId, tc.guidanceId)

				assert.Equal(t, tc.wantFinalProvisions, ia.zonalProvisionsSinceLastRefresh, "Final provision count is incorrect")

				synctest.Wait()
				got := <-decisionChan
				assert.Equal(t, tc.wantNotification, got)
			})
		})
	}
}

func TestReconcileAndUpdate(t *testing.T) {
	testCases := []struct {
		name                                   string
		initialFlexAdvisorInstanceAvailability *InstanceAvailability
		newConfigDataFromAPI                   *InstanceAvailability
		provisionsBeforeAPICall                map[string]int
		actionsDuringAPICall                   map[string]int
		wantInstanceCount                      map[string]int
		wantProvisionsMapIsReset               bool
	}{
		{
			name: "Simple refresh with no in-flight provisions",
			initialFlexAdvisorInstanceAvailability: NewTestInstanceAvailabilityBuilder("", "").WithZonalInstanceCount(map[string]int{
				"us-central1-a": 10,
				"us-central1-b": 10,
			}).WithZonalProvisionsSinceLastRefresh(map[string]int{
				"us-central1-a": 2,
			}).Build(),
			newConfigDataFromAPI: NewTestInstanceAvailabilityBuilder("", "").WithZonalInstanceCount(map[string]int{
				"us-central1-a": 20,
				"us-central1-b": 5,
			}).Build(),
			provisionsBeforeAPICall: map[string]int{
				"us-central1-a": 2,
			},
			actionsDuringAPICall: nil,
			wantInstanceCount: map[string]int{
				"us-central1-a": 20,
				"us-central1-b": 5,
			},
			wantProvisionsMapIsReset: true,
		},
		{
			name: "Refresh with in-flight provisions",
			initialFlexAdvisorInstanceAvailability: NewTestInstanceAvailabilityBuilder("", "").WithZonalInstanceCount(map[string]int{
				"us-central1-a": 10,
				"us-central1-b": 10,
			}).WithZonalProvisionsSinceLastRefresh(map[string]int{
				"us-central1-a": 1,
			}).Build(),
			newConfigDataFromAPI: NewTestInstanceAvailabilityBuilder("", "").WithZonalInstanceCount(map[string]int{
				"us-central1-a": 20,
				"us-central1-b": 15,
			}).Build(),
			provisionsBeforeAPICall: map[string]int{
				"us-central1-a": 1,
			},
			actionsDuringAPICall: map[string]int{
				"us-central1-a": 2,
			},
			wantInstanceCount: map[string]int{
				"us-central1-a": 18, // 20 (from API) - 2 (in-flight)
				"us-central1-b": 15, // No in-flight provisions for us-central1-b
			},
			wantProvisionsMapIsReset: true,
		},
		{
			name: "Refresh where a zone's availability drops",
			initialFlexAdvisorInstanceAvailability: NewTestInstanceAvailabilityBuilder("", "").WithZonalInstanceCount(map[string]int{
				"us-central1-a": 10,
			}).WithZonalProvisionsSinceLastRefresh(map[string]int{
				"us-central1-a": 1,
			}).Build(),
			newConfigDataFromAPI: NewTestInstanceAvailabilityBuilder("", "").WithZonalInstanceCount(map[string]int{
				"us-central1-a": 5,
			}).Build(),
			provisionsBeforeAPICall: map[string]int{
				"us-central1-a": 1,
			},
			actionsDuringAPICall: map[string]int{
				"us-central1-a": 2,
			},
			wantInstanceCount: map[string]int{
				"us-central1-a": 3, // 5 (from API) - 2 (in-flight)
			},
			wantProvisionsMapIsReset: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				if tc.actionsDuringAPICall != nil {
					tc.initialFlexAdvisorInstanceAvailability.MarkUsed(tc.actionsDuringAPICall, "", "")
				}

				tc.newConfigDataFromAPI.ReconcileAndUpdate(tc.initialFlexAdvisorInstanceAvailability, tc.provisionsBeforeAPICall)

				assert.Equal(t, tc.wantInstanceCount, tc.newConfigDataFromAPI.zonalInstanceCount, "Reconciled instance counts do not match wanted values.")
				if assert.NotNil(t, tc.newConfigDataFromAPI.zonalProvisionsSinceLastRefresh, "Provisions map should not be nil after reset") {
					assert.Len(t, tc.newConfigDataFromAPI.zonalProvisionsSinceLastRefresh, 0, "Provisions counter should be reset to an empty map.")
				}
			})
		})
	}
}

func TestNewSnapshot(t *testing.T) {
	testCases := []struct {
		name                   string
		zonalInstanceCount     map[string]int
		zonalProvisions        map[string]int
		wantZonalInstanceCount map[string]int
	}{
		{
			name:                   "no provisions",
			zonalInstanceCount:     map[string]int{"us-central1-a": 10, "us-central1-b": 5},
			zonalProvisions:        map[string]int{},
			wantZonalInstanceCount: map[string]int{"us-central1-a": 10, "us-central1-b": 5},
		},
		{
			name:                   "some provisions",
			zonalInstanceCount:     map[string]int{"us-central1-a": 10, "us-central1-b": 5},
			zonalProvisions:        map[string]int{"us-central1-a": 3, "us-central1-b": 1},
			wantZonalInstanceCount: map[string]int{"us-central1-a": 7, "us-central1-b": 4},
		},
		{
			name:                   "provisions equal to total count",
			zonalInstanceCount:     map[string]int{"us-central1-a": 10, "us-central1-b": 5},
			zonalProvisions:        map[string]int{"us-central1-a": 10, "us-central1-b": 2},
			wantZonalInstanceCount: map[string]int{"us-central1-a": 0, "us-central1-b": 3},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ia := NewTestInstanceAvailabilityBuilder("", "").WithZonalInstanceCount(tc.zonalInstanceCount).WithZonalProvisionsSinceLastRefresh(tc.zonalProvisions).Build()

			snapshot := ia.NewSnapshot()

			for zone, wantCount := range tc.wantZonalInstanceCount {
				gotCount, _ := snapshot.MaxAvailableInstances(zone)
				assert.Equalf(t, wantCount, gotCount, "Max availability for zone: %s is incorrect. got: %d, want: %d", zone, gotCount, wantCount)
			}
		})
	}
}
