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
	"testing"

	gce_api "google.golang.org/api/compute/v1"
)

func TestReservationRef_Serialize(t *testing.T) {
	tests := []struct {
		name           string
		reservation    ReservationRef
		clusterProject string
		expected       string
	}{
		{
			name:           "Only name, no project, no block",
			reservation:    ReservationRef{Name: "my-res-name"},
			clusterProject: "project-a",
			expected:       "my-res-name",
		},
		{
			name:           "Same project as cluster project",
			reservation:    ReservationRef{Name: "my-res-name", Project: "project-a"},
			clusterProject: "project-a",
			expected:       "my-res-name",
		},
		{
			name:           "Different project from cluster project",
			reservation:    ReservationRef{Name: "my-res-name", Project: "project-b"},
			clusterProject: "project-a",
			expected:       "projects/project-b/reservations/my-res-name",
		},
		{
			name:           "Different project, cluster project is empty, ",
			reservation:    ReservationRef{Name: "my-res-name", Project: "project-b"},
			clusterProject: "",
			expected:       "projects/project-b/reservations/my-res-name",
		},

		{
			name:           "Empty ReservationRef (missing name)",
			reservation:    ReservationRef{},
			clusterProject: "project-a",
			expected:       "",
		},
		{
			name:           "Only Project and empty Name (missing name)",
			reservation:    ReservationRef{Project: "project-x"},
			clusterProject: "project-a",
			expected:       "",
		},
		{
			name:           "Name exists, empty Project, but clusterProject is not empty (short form)",
			reservation:    ReservationRef{Name: "res-only-name", Project: ""},
			clusterProject: "project-a",
			expected:       "res-only-name",
		},
		{
			name:           "With BlockName, same project",
			reservation:    ReservationRef{Name: "my-res", Project: "project-a", BlockName: "block-1"},
			clusterProject: "project-a",
			expected:       "my-res/reservationBlocks/block-1",
		},
		{
			name:           "With BlockName, different project",
			reservation:    ReservationRef{Name: "my-res", Project: "project-b", BlockName: "block-1"},
			clusterProject: "project-a",
			expected:       "projects/project-b/reservations/my-res/reservationBlocks/block-1",
		},
		{
			name:           "With BlockName, empty project, cluster project not empty",
			reservation:    ReservationRef{Name: "my-res", Project: "", BlockName: "block-1"},
			clusterProject: "project-a",
			expected:       "my-res/reservationBlocks/block-1",
		},
		{
			name:           "With BlockName and SubBlockName, same project",
			reservation:    ReservationRef{Name: "my-res", Project: "project-a", BlockName: "block-1", SubBlockName: "sub-block-a"},
			clusterProject: "project-a",
			expected:       "my-res/reservationBlocks/block-1/reservationSubBlocks/sub-block-a",
		},
		{
			name:           "With BlockName and SubBlockName, different project",
			reservation:    ReservationRef{Name: "my-res", Project: "project-b", BlockName: "block-1", SubBlockName: "sub-block-a"},
			clusterProject: "project-a",
			expected:       "projects/project-b/reservations/my-res/reservationBlocks/block-1/reservationSubBlocks/sub-block-a",
		},
		{
			name:           "With BlockName and SubBlockName, empty project, cluster project not empty",
			reservation:    ReservationRef{Name: "my-res", Project: "", BlockName: "block-1", SubBlockName: "sub-block-a"},
			clusterProject: "project-a",
			expected:       "my-res/reservationBlocks/block-1/reservationSubBlocks/sub-block-a",
		},
		{
			name:           "Only Name and SubBlock (BlockName is required for SubBlock to appear)",
			reservation:    ReservationRef{Name: "my-res", SubBlockName: "sub-block-a"},
			clusterProject: "project-a",
			expected:       "my-res",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.reservation.RelativePath(tt.clusterProject)
			if got != tt.expected {
				t.Errorf("Serialize(%q) for ReservationRef %+v got %q, want %q",
					tt.clusterProject, tt.reservation, got, tt.expected)
			}
		})
	}
}

func TestReservationRef_ShortenSelfLink(t *testing.T) {
	tests := []struct {
		name        string
		reservation *gce_api.Reservation
		expected    string
	}{
		{
			name:        "production_SelfLink",
			reservation: &gce_api.Reservation{Name: "my-res-name", SelfLink: "https://www.googleapis.com/compute/v1/projects/my-test-projects/zones/us-central1/reservations/my-res-name-1"},
			expected:    "/projects/my-test-projects/zones/us-central1/reservations/my-res-name-1",
		},
		{
			name:        "alpha_v1_SelfLink",
			reservation: &gce_api.Reservation{Name: "my-res-name", SelfLink: "https://www.googleapis.com/compute/alpha_v1/projects/my-test-projects/zones/us-central1/reservations/my-res-name-1"},
			expected:    "/projects/my-test-projects/zones/us-central1/reservations/my-res-name-1",
		},
		{
			name:        "unexpected_SelfLink",
			reservation: &gce_api.Reservation{Name: "my-res-name", SelfLink: "this-is-not-a-standard-self-link"},
			expected:    "this-is-not-a-standard-self-link",
		},
		{
			name:        "empty_SelfLink",
			reservation: &gce_api.Reservation{Name: "my-res-name", SelfLink: ""},
			expected:    "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetShortSelfLink(tt.reservation)
			if got != tt.expected {
				t.Errorf("ShortenSelfLink(%v) got %q, want %q", tt.reservation, got, tt.expected)
			}
		})
	}
}
