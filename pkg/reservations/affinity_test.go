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
	"testing"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

// TestSupportedReservationAffinitySelectorValues validates SupportedReservationAffinitySelectorValues
// behaves as expected without hardcoding the individual values.
func TestSupportedReservationAffinitySelectorValues(t *testing.T) {
	v := SupportedReservationAffinitySelectorValues()
	if len(supportedReservationAffinitySelectorValuesToAffinity) != len(v) {
		t.Errorf("Unexpected len(SupportedReservationAffinitySelectorValues()). Got %d; Want %d", len(v), len(supportedReservationAffinitySelectorValuesToAffinity))
	}
}

// TestSupportedReservationAffinityFromValues tests SupportedReservationAffinityFromValue behaves
// as expected without hardcoding the individual values.
func TestSupportedReservationAffinityFromValue(t *testing.T) {
	_, ok := GkeAffinityFromSelectorValue("no way this is a legit value")
	if ok {
		t.Errorf("SupportedReservationAffinityFromValue()=ok; Want not ok")
	}
}

func TestNewNodepoolReservationAffinity(t *testing.T) {
	tests := map[string]struct {
		reservationPath     string
		reservationAffinity string

		wantAffinity *gke_api_beta.ReservationAffinity
		wantError    error
	}{
		"SpecificReservation_LocalProject": {
			reservationPath:     "local-reservation",
			reservationAffinity: gkeclient.ReservationAffinitySpecific,
			wantAffinity: &gke_api_beta.ReservationAffinity{
				Key:                    gkeclient.ReservationNameKey,
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Values:                 []string{"local-reservation"},
			},
			wantError: nil,
		},
		"SpecificReservation_DifferentProject": {
			reservationPath:     "projects/other/reservations/specific",
			reservationAffinity: gkeclient.ReservationAffinitySpecific,
			wantAffinity: &gke_api_beta.ReservationAffinity{
				Key:                    gkeclient.ReservationNameKey,
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Values:                 []string{"projects/other/reservations/specific"},
			},
			wantError: nil,
		},
		"SpecificReservation_ReservationBlock": {
			reservationPath:     "local-reservation/reservationBlocks/block-reservation",
			reservationAffinity: gkeclient.ReservationAffinitySpecific,
			wantAffinity: &gke_api_beta.ReservationAffinity{
				Key:                    gkeclient.ReservationNameKey,
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Values:                 []string{"local-reservation/reservationBlocks/block-reservation"},
			},
			wantError: nil,
		},
		"AnyReservation": {
			reservationAffinity: gkeclient.ReservationAffinityAny,
			wantAffinity: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinityAny,
			},
			wantError: nil,
		},
		"AnyThenFail": {
			reservationAffinity: gkeclient.ReservationAffinityAnyThenFail,
			wantAffinity: &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinityAnyThenFail,
			},
			wantError: nil,
		},
		"InvalidAffinity": {
			reservationAffinity: "invalid",
			wantAffinity:        nil,
			wantError:           NewUnsupportedReservationAffinityError("invalid", "unsupported reservation affinity"),
		},
		"NoAffinity": {
			reservationPath: "specific",
			wantAffinity:    nil,
			wantError:       NewUnsupportedReservationAffinityError("", "unsupported reservation affinity"),
		},
		"NoNameSpecific": {
			reservationPath:     "",
			reservationAffinity: gkeclient.ReservationAffinitySpecific,
			wantAffinity:        nil,
			wantError:           NewUnsupportedReservationAffinityError(gkeclient.ReservationAffinitySpecific, "unsupported to both specify no reservation and specific reservation affinity"),
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			gotAffinity, gotError := NewNodepoolReservationAffinity(test.reservationPath, test.reservationAffinity)

			assert.Equal(t, test.wantAffinity, gotAffinity)
			assert.Equal(t, test.wantError, gotError)
		})
	}
}
