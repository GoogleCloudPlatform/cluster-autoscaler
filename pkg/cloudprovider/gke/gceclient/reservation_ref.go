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
	"path"
	"strings"

	gce_api "google.golang.org/api/compute/v1"
)

// ReservationRef keeps all the information needed to identify a reservation,
// together with optional block name and sublock name, which are required to
// resolve full reservation path.
type ReservationRef struct {
	Project      string
	Zone         string
	Name         string
	BlockName    string
	SubBlockName string
}

// String provides a string representation of the ReservationRef
// It returns an empty string if any of the key components are missing.
func (r ReservationRef) String() string {
	if r.Project == "" || r.Zone == "" || r.Name == "" {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(path.Join(r.Project, r.Zone, r.Name))
	if r.BlockName != "" {
		sb.WriteString(fmt.Sprintf("/%s", r.BlockName))
		if r.SubBlockName != "" {
			sb.WriteString(fmt.Sprintf("/%s", r.SubBlockName))
		}
	}
	return sb.String()
}

// Path provides a string representation of the ReservationRef in long notation.
// If reservation block is specified it will append .../reservationBlocks/res-block.
// If reservation block and subblock is specified it will append
// .../reservationSubBlocks/res-sub-block.
func (r ReservationRef) Path() string {
	return r.RelativePath("")
}

// RelativePath provides a string representation of the ReservationPath. Produces long
// form notation in cases where reservation is located in the different project
// or a short form for reservations contained in the cluster project.
// If reservation block is specified it will append .../reservationBlocks/res-block.
// If reservation block and subblock is specified it will append
// .../reservationSubBlocks/res-sub-block.
func (r ReservationRef) RelativePath(clusterProject string) string {
	if r.Name == "" {
		// name is required to generate reservation path
		return ""
	}
	var sb strings.Builder
	sb.WriteString(r.Name)
	if r.Project != clusterProject && r.Project != "" {
		sb.Reset()
		sb.WriteString(path.Join("projects", r.Project, "reservations", r.Name))
	}
	if r.BlockName != "" {
		sb.WriteString("/")
		sb.WriteString(path.Join("reservationBlocks", r.BlockName))
		if r.SubBlockName != "" {
			sb.WriteString("/")
			sb.WriteString(path.Join("reservationSubBlocks", r.SubBlockName))
		}
	}
	return sb.String()
}

// GetReservationRefFromReservation creates and returns a ReservationRef by value.
// It extracts the necessary information from a gce_api.Reservation object.
func GetReservationRefFromReservation(reservation gce_api.Reservation) ReservationRef {
	return ReservationRef{
		Project: GetReservationProject(&reservation),
		Zone:    GetReservationZone(&reservation),
		Name:    reservation.Name,
	}
}

// GetReservationProject extracts project from Reservation URL
func GetReservationProject(rsv *gce_api.Reservation) string {
	// Expected form https://www.googleapis.com/compute/v1/projects/[Project]/zones/[Zone]/reservations/[Reservation]
	parts := strings.Split(rsv.SelfLink, "/")
	for i, part := range parts {
		if part == "projects" && len(parts) > i+1 {
			return parts[i+1]
		}
	}

	return ""
}

// GetReservationZone extracts zone from Reservation URL
func GetReservationZone(rsv *gce_api.Reservation) string {
	// Expected form https://www.googleapis.com/compute/v1/projects/[Project]/zones/[Zone]
	temp := strings.Split(rsv.Zone, "/")
	rsvZone := temp[len(temp)-1]
	return rsvZone
}
