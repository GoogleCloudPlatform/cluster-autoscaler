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
	"github.com/stretchr/testify/assert"
	gce_api_beta "google.golang.org/api/compute/v0.beta"
	"strings"
	"testing"
)

func TestToGceResourcePolicy(t *testing.T) {
	testCases := []struct {
		name       string
		item       *gce_api_beta.ResourcePolicy
		wantPolicy *GceResourcePolicy
		wantErr    bool
		err        string
	}{
		{
			name: "Workload policy",
			item: &gce_api_beta.ResourcePolicy{
				Name: "test-rp",
				WorkloadPolicy: &gce_api_beta.ResourcePolicyWorkloadPolicy{
					Type: "HIGH_AVAILABILITY",
				},
			},
			wantPolicy: &GceResourcePolicy{
				Name: "test-rp",
				WorkloadPolicy: WorkloadPolicy{
					Type: "HIGH_AVAILABILITY",
				},
			},
		},
		{
			name: "Placement policy",
			item: &gce_api_beta.ResourcePolicy{
				Name: "test-rp",
				GroupPlacementPolicy: &gce_api_beta.ResourcePolicyGroupPlacementPolicy{
					MaxDistance: 1,
					TpuTopology: "2x2",
					Collocation: "COLLOCATED",
				},
			},
			wantPolicy: &GceResourcePolicy{
				Name: "test-rp",
				PlacementPolicy: PlacementPolicy{
					TpuTopology: "2x2",
					MaxDistance: 1,
					Collocation: "COLLOCATED",
				},
			},
		},
		{
			name:    "Policy is missing",
			item:    nil,
			wantErr: true,
			err:     "GCE resource policy is nil",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			rp, err := toGceResourcePolicy(tc.item)

			if tc.wantErr {
				assert.Nil(t, rp)
				assert.NotNil(t, err)
				assert.True(t, strings.Contains(err.Error(), tc.err))
			} else {
				assert.NotNil(t, rp)
				assert.Nil(t, err)
				assert.Equal(t, tc.wantPolicy, rp)
			}
		})
	}
}
