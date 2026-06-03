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
	"fmt"
	"regexp"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
)

var (
	regexServiceAccountDeleted               = regexp.MustCompile(`Service account.*does not exist`)
	regexOutOfQuota                          = regexp.MustCompile(`(?i)quota`)
	regexProjectMetaConflictStartupScriptUrl = regexp.MustCompile(`(?i)Clusters/node pools cannot be created while .* is specified in the project metadata`)
	// Error message returned by GCE API
	regexGKESAPermissionErrorGCE = regexp.MustCompile(`Google Compute Engine: Required .* permission`)
	// Error message returned by GKE API
	regexGKESAPermissionErrorGKE = regexp.MustCompile(`The Kubernetes Engine service account is missing required permissions`)
	// Reservations related error messages returned by GCE API
	regexReservationErrors = []*regexp.Regexp{
		regexp.MustCompile("Incompatible AggregateReservation VMFamily"),
		regexp.MustCompile("Could not find the given reservation with the following name"),
		regexp.MustCompile("must use ReservationAffinity of"),
		regexp.MustCompile("The reservation must exist in the same project as the instance"),
		regexp.MustCompile("only compatible with Aggregate Reservations"),
		regexp.MustCompile("Please target a reservation with workload_type ="),
		regexp.MustCompile("AggregateReservation VMFamily: should be a (.*) VM Family for instance with (.*) machine type"),
		regexp.MustCompile("VM Family: (.*) is not supported for aggregate reservations. It must be one of"),
		regexp.MustCompile("Reservation (.*) is incorrect for the requested resources"),
		regexp.MustCompile("Zone does not currently have sufficient capacity for the requested resources"),
		regexp.MustCompile("Reservation (.*) does not have sufficient capacity for the requested resources."),
	}
	regexReservationNotReadyError = regexp.MustCompile("(?i)it requires reservation to be in READY state")
)

type machineTypeGetter interface {
	MachineType() string
}

// GetMachineFamilyFromNodeGroup Gets the machine family from a node group.
func GetMachineFamilyFromNodeGroup(nodeGroup cloudprovider.NodeGroup) (string, error) {
	if mig, ok := nodeGroup.(machineTypeGetter); ok {
		machineFamily, err := gce.GetMachineFamily(mig.MachineType())
		if err != nil {
			return "", fmt.Errorf("could not get machine family from machine type %q", mig.MachineType())
		}
		return machineFamily, nil
	}
	return "", fmt.Errorf("%v is not a GkeNodeGroup", nodeGroup)
}

// IsGKEServiceAccountPermissionError returns whether the error represents
// permission error for when GKE service uses GKE P4SA.
func IsGKEServiceAccountPermissionError(err error) bool {
	if err == nil {
		return false
	}
	if regexGKESAPermissionErrorGCE.MatchString(err.Error()) {
		return true
	}

	if regexGKESAPermissionErrorGKE.MatchString(err.Error()) {
		return true
	}
	return false
}

// IsServiceAccountDeletedError returns whether the error represents a deleted
// service account error.
func IsServiceAccountDeletedError(err error) bool {
	if err == nil {
		return false
	}
	if regexServiceAccountDeleted.MatchString(err.Error()) {
		return true
	}
	return false
}

// IsOutOfQuotaError returns whether the error represents an out of quota error.
func IsOutOfQuotaError(err error) bool {
	if err == nil {
		return false
	}
	if regexOutOfQuota.MatchString(err.Error()) {
		return true
	}
	return false
}

// IsInvalidReservationError returns whether the error represents an invalid
// reservation error.
func IsInvalidReservationError(err error) bool {
	if err == nil {
		return false
	}
	for _, re := range regexReservationErrors {
		if re.MatchString(err.Error()) {
			return true
		}
	}
	return false
}

// IsReservationNotReadyError returns whether the error represernts a reservation
// not ready error.
func IsReservationNotReadyError(err error) bool {
	if err == nil {
		return false
	}
	return regexReservationNotReadyError.MatchString(err.Error())
}

// IsProjectMetadataStartupScriptUrlConflict returns whether the error
// represents project metadata startup script URL conflict
func IsProjectMetadataStartupScriptUrlConflict(err error) bool {
	if err != nil {
		return regexProjectMetaConflictStartupScriptUrl.MatchString(err.Error())
	}
	return false
}

// AppendAndOverwriteMap appends/overrides new values on an existing map
func AppendAndOverwriteMap(initialMap, newMap map[string]float64) map[string]float64 {
	for key, val := range newMap {
		initialMap[key] = val
	}
	return initialMap
}

// reservationName extracts the reservation name from a given reservation affinity value.
// It isolates the core reservation name by stripping blocks/sub-blocks and path prefixes.
func reservationName(affinityValue string) string {
	// Remove reservationBlocks and anything after it to isolate the reservation name or path.
	if idx := strings.Index(affinityValue, "/reservationBlocks/"); idx != -1 {
		affinityValue = affinityValue[:idx]
	}
	parts := strings.Split(affinityValue, "/")
	return parts[len(parts)-1]
}
