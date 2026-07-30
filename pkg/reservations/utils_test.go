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

	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestIsReservationAffinitySpecified(t *testing.T) {
	testCases := []struct {
		name      string
		nodeGroup gke.NodeGroup
		want      bool
	}{
		{
			name:      "nil nodeGroup",
			nodeGroup: nil,
			want:      false,
		},
		{
			name:      "nil spec",
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(nil).Build(),
			want:      false,
		},
		{
			name:      "nil ReservationAffinity",
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{}).Build(),
			want:      false,
		},
		{
			name: "ReservationAffinityNone",
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinityNone,
				},
			}).Build(),
			want: false,
		},
		{
			name: "ReservationAffinityAny",
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinityAny,
				},
			}).Build(),
			want: true,
		},
		{
			name: "ReservationAffinityAnyThenFail",
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinityAnyThenFail,
				},
			}).Build(),
			want: true,
		},
		{
			name: "ReservationAffinitySpecific",
			nodeGroup: gke.NewTestGkeMigBuilder().SetSpec(&gkeclient.NodePoolSpec{
				ReservationAffinity: &gke_api_beta.ReservationAffinity{
					ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
					Key:                    gkeclient.ReservationNameKey,
					Values:                 []string{"res-1"},
				},
			}).Build(),
			want: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsReservationAffinitySpecified(tc.nodeGroup); got != tc.want {
				t.Errorf("IsReservationAffinitySpecified() = %v, want %v", got, tc.want)
			}
		})
	}
}
