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

package gke

import (
	"errors"
	"testing"
)

func TestIsServiceAccountDeletedError(t *testing.T) {
	testCases := []struct {
		err                   error
		isServiceAccountError bool
	}{
		{
			err:                   nil,
			isServiceAccountError: false,
		},
		{
			err:                   errors.New("general error"),
			isServiceAccountError: false,
		},
		{
			err:                   errors.New("Service account xyz does not exist"),
			isServiceAccountError: true,
		},
	}
	for _, tc := range testCases {
		got := IsServiceAccountDeletedError(tc.err)
		if got != tc.isServiceAccountError {
			t.Fatalf("Wanted %t but got %t for error %v", tc.isServiceAccountError, got, tc.err)
		}
	}
}

func TestIsOutOfQuotaError(t *testing.T) {
	testCases := []struct {
		descr   string
		err     error
		isError bool
	}{
		{
			descr:   "nil case",
			err:     nil,
			isError: false,
		},
		{
			descr:   "non quota error",
			err:     errors.New("general error"),
			isError: false,
		},
		{
			descr:   "quota error",
			err:     errors.New("googleapi: Error 403: Insufficient regional quota to satisfy request: resource \"IN_USE_ADDRESSES\": request requires '0.0' and is short '2.0'. project has a quota of '8.0' with '-2.0' available. View and manage quotas at https://console.cloud.google.com/iam-admin/quotas?usage=USED&project=xyz., forbidden"),
			isError: true,
		},
		{
			descr:   "quota error, case insensitive",
			err:     errors.New("qUoTa"),
			isError: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.descr, func(t *testing.T) {
			got := IsOutOfQuotaError(tc.err)
			if got != tc.isError {
				t.Fatalf("Wanted %t but got %t for error %v", tc.isError, got, tc.err)
			}
		})
	}
}

func TestIsInvalidReservationError(t *testing.T) {
	testCases := map[string]struct {
		err     error
		isError bool
	}{
		"nil": {
			err:     nil,
			isError: false,
		},
		"not an invalid reservation error": {
			err:     errors.New("Error not related to an invalid reservation."),
			isError: false,
		},
		"incompatible VMFamily error": {
			err:     errors.New("Incompatible AggregateReservation VMFamily: some-family"),
			isError: true,
		},
		"invalid reservation name error": {
			err:     errors.New("creation failed: Could not find the given reservation with the following name: fictional-res"),
			isError: true,
		},
		"invalid reservation affinity error": {
			err:     errors.New("creation failed: Reserved VMs with machine type ssv-normandy-sr2 must use ReservationAffinity of XYZ type."),
			isError: true,
		},
		"reservation not in same project error": {
			err:     errors.New("creation failed: The reservation must exist in the same project as the instance"),
			isError: true,
		},
		"incompatible reservation type error": {
			err:     errors.New("creation failed: Reserved VMs with machine type ssv-normandy-sr2 are only compatible with Aggregate Reservations."),
			isError: true,
		},
		"wrong workload_type error": {
			err:     errors.New("The instance configuration is optimized for serving workloads like XYZXYZ. Please target a reservation with workload_type = XYZXYZ"),
			isError: true,
		},
		"instance type - VMFamily mismatch error": {
			err:     errors.New("AggregateReservation VMFamily: should be a XYZ VM Family for instance with ssv-normandy machine type"),
			isError: true,
		},
		"VMFamily not supported error": {
			err:     errors.New("VM Family: XYZ is not supported for aggregate reservations. It must be one of the following: XX, YY, ZZ"),
			isError: true,
		},
		"zone do not have capacity error": {
			err:     errors.New("Zone does not currently have sufficient capacity for the requested resources"),
			isError: true,
		},
		"insufficient capacity error": {
			err:     errors.New("Reservation fictional-res does not have sufficient capacity for the requested resources."),
			isError: true,
		},
	}
	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			got := IsInvalidReservationError(tc.err)
			if got != tc.isError {
				t.Fatalf("Want %t but got %t for error %v", tc.isError, got, tc.err)
			}
		})
	}
}

func TestIsReservationNotReadyError(t *testing.T) {
	testCases := map[string]struct {
		err     error
		isError bool
	}{
		"nil case": {
			err:     nil,
			isError: false,
		},
		"general error case": {
			err:     errors.New("Error not related to reservation readiness."),
			isError: false,
		},
		"reservation not ready error": {
			err:     errors.New("Cannot insert instance to a reservation with status XYZ, as it requires reservation to be in ready state"),
			isError: true,
		},
	}
	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			got := IsReservationNotReadyError(tc.err)
			if got != tc.isError {
				t.Fatalf("Want %t but got %t for error %v", tc.isError, got, tc.err)
			}
		})
	}
}

func TestIsProjectMetaConflictStartupScriptUrl(t *testing.T) {
	testCases := []struct {
		descr   string
		err     error
		isError bool
	}{
		{
			descr:   "nil case",
			err:     nil,
			isError: false,
		},
		{
			descr:   "general error",
			err:     errors.New("general error"),
			isError: false,
		},
		{
			descr:   "project metadata conflict",
			err:     errors.New("message prefix Clusters/node pools cannot be created while \"startup-script-url\" is specified in the project metadata"),
			isError: true,
		},
		{
			descr:   "project metadata conflict, case insensitive",
			err:     errors.New("message prefix clusters/node pools cannot be created while \"startup-script-url\" is specified in the project metadata"),
			isError: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.descr, func(t *testing.T) {
			got := IsProjectMetadataStartupScriptUrlConflict(tc.err)
			if got != tc.isError {
				t.Errorf("IsProjectMetadataStartupScriptUrlConflict(%v) = %v, want %v", tc.err, got, tc.isError)
			}
		})
	}
}

func TestIsGKEServiceAccountPermissionError(t *testing.T) {
	testCases := []struct {
		descr   string
		err     error
		isError bool
	}{
		{
			descr:   "nil case",
			err:     nil,
			isError: false,
		},
		{
			descr:   "general error",
			err:     errors.New("general error"),
			isError: false,
		},
		{
			descr:   "GKE SA permissions error returned by GKE API",
			err:     errors.New("The Kubernetes Engine service account is missing required permissions"),
			isError: true,
		},
		{
			descr:   "GKE SA permissions error returned by GCE API",
			err:     errors.New("Google Compute Engine: Required 'compute.instanceGroupManagers.create' permission for 'projects/xx'"),
			isError: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.descr, func(t *testing.T) {
			got := IsGKEServiceAccountPermissionError(tc.err)
			if got != tc.isError {
				t.Errorf("IsGKEServiceAccountPermissionError(%v) = %v, want %v", tc.err, got, tc.isError)
			}
		})
	}
}

func TestReservationName(t *testing.T) {
	tests := []struct {
		name          string
		affinityValue string
		want          string
	}{
		{
			name:          "simple local reservation",
			affinityValue: "rsv-name",
			want:          "rsv-name",
		},
		{
			name:          "local reservation with blocks",
			affinityValue: "rsv-name/reservationBlocks/block-1",
			want:          "rsv-name",
		},
		{
			name:          "shared reservation",
			affinityValue: "projects/shared-project/reservations/rsv-name",
			want:          "rsv-name",
		},
		{
			name:          "shared reservation with blocks",
			affinityValue: "projects/shared-project/reservations/rsv-name/reservationBlocks/block-1",
			want:          "rsv-name",
		},
		{
			name:          "shared reservation with sub-blocks",
			affinityValue: "projects/shared-project/reservations/rsv-name/reservationBlocks/block-1/reservationSubBlocks/sub-1",
			want:          "rsv-name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reservationName(tt.affinityValue); got != tt.want {
				t.Errorf("reservationName(%q) = %q, want %q", tt.affinityValue, got, tt.want)
			}
		})
	}
}
