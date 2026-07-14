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

package gceclient

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gce_api_beta "google.golang.org/api/compute/v0.beta"
)

func TestToEnumWithDefault(t *testing.T) {
	for _, tc := range []struct {
		str  string
		want ProcurementStatusEnum
	}{
		{
			str:  "APPROVED",
			want: ProcurementStatusApproved,
		},
		{
			str:  "PROCURING",
			want: ProcurementStatusProcuring,
		},
		{
			str:  "not existing status",
			want: ProcurementStatusUnknown,
		},
	} {
		t.Run(tc.str, func(t *testing.T) {
			enum := toEnumWithDefault(procurementStatusMapping, tc.str, ProcurementStatusUnknown)
			assert.Equal(t, tc.want, enum)
		})
	}
}

func TestToGceFutureReservation(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		fr, err := toGceFutureReservation(nil)
		assert.Nil(t, fr)
		assert.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "reservation is nil"))
	})

	status := &gce_api_beta.FutureReservationStatus{ProcurementStatus: "APPROVED"}
	tw := &gce_api_beta.FutureReservationTimeWindow{StartTime: time.Now().Format(time.RFC3339)}
	testCases := []struct {
		tcName     string
		status     *gce_api_beta.FutureReservationStatus
		timeWindow *gce_api_beta.FutureReservationTimeWindow
		wantErr    string // empty means no error
	}{
		{
			tcName:     "Status=nil",
			status:     nil,
			timeWindow: tw,
			wantErr:    "Status is nil",
		},
		{
			tcName:     "TimeWindow=nil",
			status:     status,
			timeWindow: nil,
			wantErr:    "TimeWindow is nil",
		},
		{
			tcName:     "not parsable TimeWindow.StartTime",
			status:     status,
			timeWindow: &gce_api_beta.FutureReservationTimeWindow{StartTime: "not a RFC3339 time"},
			wantErr:    "not parsable to RFC3339",
		},
		{
			tcName:     "positive test case",
			status:     status,
			timeWindow: tw,
			wantErr:    "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.tcName, func(t *testing.T) {
			id := uint64(time.Now().UnixNano())
			name := fmt.Sprintf("fr-%d", id)
			input := gce_api_beta.FutureReservation{
				Id:             id,
				Name:           name,
				PlanningStatus: "DRAFT",
				Status:         tc.status,
				TimeWindow:     tc.timeWindow,
			}
			fr, err := toGceFutureReservation(&input)
			if tc.wantErr == "" {
				assert.NotNil(t, fr)
				assert.Nil(t, err)
				assert.Equal(t, id, fr.Id)
				assert.Equal(t, name, fr.Name)
				assert.Equal(t, PlanningStatusDraft, fr.PlanningStatus)
				assert.Equal(t, procurementStatusMapping[tc.status.ProcurementStatus], fr.ProcurementStatus)
				expectedStartTime, _ := time.Parse(time.RFC3339, tc.timeWindow.StartTime)
				assert.Equal(t, expectedStartTime, fr.StartTime)
			} else {
				assert.Nil(t, fr)
				assert.NotNil(t, err)
				assert.True(t, strings.Contains(err.Error(), tc.wantErr))
			}
		})
	}
}
