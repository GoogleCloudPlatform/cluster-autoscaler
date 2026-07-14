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

package reservations

import (
	"sort"

	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

const (
	SpecificAffinity string = "specific"
	AnyAffinity      string = "any"
	AnyThenFail      string = "any-reservation-then-fail"
	NoneAffinity     string = "none"
)

// supportedReservationAffinitySelectorValuesToAffinity
// Contains the mapping between user provided nodepool selector value
// to gke reservation affinity for supported values.
var supportedReservationAffinitySelectorValuesToAffinity = map[string]string{
	SpecificAffinity: gkeclient.ReservationAffinitySpecific,
	AnyAffinity:      gkeclient.ReservationAffinityAny,
	NoneAffinity:     gkeclient.ReservationAffinityNone,
	AnyThenFail:      gkeclient.ReservationAffinityAnyThenFail,
}

// GkeAffinityFromSelectorValue parses the matching affinity from the provided value.
// Returns if an affinity was found as well as the value of said affinity if found.
func GkeAffinityFromSelectorValue(nodeselectorValue string) (string, bool) {
	a, ok := supportedReservationAffinitySelectorValuesToAffinity[nodeselectorValue]
	return a, ok
}

// SupportedReservationAffinitySelectorValues returns the list of supported
// values for node affinity node selector
func SupportedReservationAffinitySelectorValues() []string {
	v := make([]string, 0, len(supportedReservationAffinitySelectorValuesToAffinity))
	for k := range supportedReservationAffinitySelectorValuesToAffinity {
		v = append(v, k)
	}
	sort.Strings(v)
	return v
}

// isSupportedGkeReservationAffinity determines whether given GKE affinity is supported.
func isSupportedGkeReservationAffinity(gkeAffinity string) bool {
	for _, v := range supportedReservationAffinitySelectorValuesToAffinity {
		if gkeAffinity == v {
			return true
		}
	}

	return false
}

// NewNodepoolReservationAffinity builds nodepool reservation affinity based on reservation affinity and path.
func NewNodepoolReservationAffinity(reservationPath string, gkeReservationAffinity string) (*gke_api_beta.ReservationAffinity, error) {
	if !isSupportedGkeReservationAffinity(gkeReservationAffinity) {
		return nil, NewUnsupportedReservationAffinityError(gkeReservationAffinity, "unsupported reservation affinity")
	}

	affinity := &gke_api_beta.ReservationAffinity{
		ConsumeReservationType: gkeReservationAffinity,
	}

	if gkeReservationAffinity == gkeclient.ReservationAffinitySpecific {
		if reservationPath == "" {
			return nil, NewUnsupportedReservationAffinityError(gkeReservationAffinity, "unsupported to both specify no reservation and specific reservation affinity")
		}

		affinity.Key = gkeclient.ReservationNameKey
		affinity.Values = []string{reservationPath}
	}

	return affinity, nil
}
