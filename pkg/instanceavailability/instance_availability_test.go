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

package instanceavailability

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaxAvailableInstances(t *testing.T) {
	baseZonalCounts := map[string]int{"us-central1-a": 10, "us-central1-b": 20}
	baseSnapshot := NewSnapshot(nil, "", "", "", baseZonalCounts, nil)

	tests := []struct {
		name          string
		zone          string
		want          int
		wantZoneFound bool
	}{
		{
			name:          "Existing us-central1-a",
			zone:          "us-central1-a",
			want:          10,
			wantZoneFound: true,
		},
		{
			name:          "Existing us-central1-b",
			zone:          "us-central1-b",
			want:          20,
			wantZoneFound: true,
		},
		{
			name: "Non-existing us-central1-c",
			zone: "us-central1-c",
			want: 0,
		},
		{
			name: "Empty zone string",
			zone: "",
			want: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, found := baseSnapshot.MaxAvailableInstances(tc.zone)
			assert.Equal(t, tc.wantZoneFound, found)
			assert.Equal(t, tc.want, got, "MaxAvailableInstances should return correct count for zone %s", tc.zone)
		})
	}
}

func TestGcePreferenceScore(t *testing.T) {
	baseZonalScores := map[string]float64{"us-central1-a": 0.9, "us-central1-b": 0.8}
	baseSnapshot := NewSnapshot(nil, "", "", "", nil, baseZonalScores)

	tests := []struct {
		name string
		zone string
		want float64
	}{
		{
			name: "Existing us-central1-a",
			zone: "us-central1-a",
			want: 0.9,
		},
		{
			name: "Existing us-central1-b",
			zone: "us-central1-b",
			want: 0.8,
		},
		{
			name: "Non-existing us-central1-c",
			zone: "us-central1-c",
			want: 0.0,
		},
		{
			name: "Empty zone string",
			zone: "",
			want: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := baseSnapshot.GcePreferenceScore(tt.zone)
			assert.Equal(t, tt.want, got, "GcePreferenceScore should return correct score for zone %s", tt.zone)
		})
	}
}

func TestMarkUsed(t *testing.T) {
	flexibilityScopeKey := "scope-1"
	instanceConfigKey := "instanceConfigKey-1"
	guidanceId := "guidance-abc"

	testCases := []struct {
		name                          string
		initialCounts                 map[string]int
		zonalInstancesToProvision     map[string]int
		decisionId                    string
		wantErr                       error
		wantUpdatedZonalInstanceCount map[string]int
	}{
		{
			name:                          "Successful MarkUsed - All instances used from existing zones",
			initialCounts:                 map[string]int{"us-central1-a": 10, "us-central1-b": 20},
			zonalInstancesToProvision:     map[string]int{"us-central1-a": 3, "us-central1-b": 5},
			decisionId:                    "decision-1",
			wantErr:                       nil,
			wantUpdatedZonalInstanceCount: map[string]int{"us-central1-a": 7, "us-central1-b": 15},
		},
		{
			name:                          "Successful MarkUsed - Some instances used, including non-existent zone",
			initialCounts:                 map[string]int{"us-central1-a": 10},
			zonalInstancesToProvision:     map[string]int{"us-central1-a": 2, "us-central1-c": 5},
			decisionId:                    "decision-2",
			wantErr:                       nil,
			wantUpdatedZonalInstanceCount: map[string]int{"us-central1-a": 8},
		},
		{
			name:                          "Provider returns error",
			initialCounts:                 map[string]int{"us-central1-a": 10, "us-central1-b": 20},
			zonalInstancesToProvision:     map[string]int{"us-central1-a": 3},
			decisionId:                    "decision-3",
			wantErr:                       errors.New("provider error"),
			wantUpdatedZonalInstanceCount: map[string]int{"us-central1-a": 10, "us-central1-b": 20},
		},
		{
			name:                          "MarkUsed with zero instances to provision",
			initialCounts:                 map[string]int{"us-central1-a": 10},
			zonalInstancesToProvision:     map[string]int{"us-central1-a": 0},
			decisionId:                    "decision-4",
			wantErr:                       nil,
			wantUpdatedZonalInstanceCount: map[string]int{"us-central1-a": 10},
		},
		{
			name:                          "MarkUsed with empty provision map",
			initialCounts:                 map[string]int{"us-central1-a": 10},
			zonalInstancesToProvision:     map[string]int{},
			decisionId:                    "decision-5",
			wantErr:                       nil,
			wantUpdatedZonalInstanceCount: map[string]int{"us-central1-a": 10},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockProvider := new(MockProvider)
			mockProvider.On("MarkUsed", flexibilityScopeKey, instanceConfigKey, guidanceId, tc.decisionId, tc.zonalInstancesToProvision).Return(tc.wantErr).Once()

			initialCountsCopy := make(map[string]int)
			for k, v := range tc.initialCounts {
				initialCountsCopy[k] = v
			}
			snapshot := NewSnapshot(mockProvider, flexibilityScopeKey, instanceConfigKey, guidanceId, initialCountsCopy, nil)

			err := snapshot.MarkUsed(tc.zonalInstancesToProvision, tc.decisionId)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.wantUpdatedZonalInstanceCount, snapshot.zonalInstanceCount)

			mockProvider.AssertExpectations(t)
		})
	}
}
