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

package placement

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

func TestResourcePoliciesPuller(t *testing.T) {
	projectID := "cluster-project"

	wpReady := &gceclient.GceResourcePolicy{Name: "wpReady", Status: "READY", WorkloadPolicy: gceclient.WorkloadPolicy{MaxTopologyDistance: "3", AcceleratorTopology: "4x4", Type: "HIGH_THROUGHPUT"}}
	ppReady := &gceclient.GceResourcePolicy{Name: "ppReady", Status: "READY", PlacementPolicy: gceclient.PlacementPolicy{MaxDistance: 2, TpuTopology: "2x2"}}
	ppCreating := &gceclient.GceResourcePolicy{Name: "ppCreating", Status: "CREATING", PlacementPolicy: gceclient.PlacementPolicy{MaxDistance: 2, TpuTopology: "2x2"}}
	ppDeleting := &gceclient.GceResourcePolicy{Name: "ppDeleting", Status: "DELETING", PlacementPolicy: gceclient.PlacementPolicy{MaxDistance: 2, TpuTopology: "2x2"}}
	ppExpired := &gceclient.GceResourcePolicy{Name: "ppExpired", Status: "EXPIRED", PlacementPolicy: gceclient.PlacementPolicy{MaxDistance: 2, TpuTopology: "2x2"}}
	ppInvalid := &gceclient.GceResourcePolicy{Name: "ppInvalid", Status: "INVALID", PlacementPolicy: gceclient.PlacementPolicy{MaxDistance: 2, TpuTopology: "2x2"}}

	testCases := []struct {
		name                 string
		disabledExperiment   bool
		returnPolicies       []*gceclient.GceResourcePolicy
		returnError          error
		wantResourcePolicies map[string]*gceclient.GceResourcePolicy
	}{
		{
			name:                 "ResourcePoliciesAvailable",
			returnPolicies:       []*gceclient.GceResourcePolicy{wpReady, ppReady, ppCreating, ppDeleting, ppExpired, ppInvalid},
			returnError:          nil,
			wantResourcePolicies: map[string]*gceclient.GceResourcePolicy{wpReady.Name: wpReady, ppReady.Name: ppReady},
		},
		{
			name:                 "ExpDisabled_ResourcePoliciesAvailable",
			disabledExperiment:   true,
			returnPolicies:       []*gceclient.GceResourcePolicy{wpReady, ppReady, ppCreating, ppDeleting, ppExpired, ppInvalid},
			returnError:          nil,
			wantResourcePolicies: map[string]*gceclient.GceResourcePolicy{},
		},
		{
			name:           "NoResourcePoliciesAvailable",
			returnPolicies: []*gceclient.GceResourcePolicy{},
			returnError:    nil,
		},
		{
			name:           "ResourcePoliciesError",
			returnPolicies: []*gceclient.GceResourcePolicy{wpReady, ppReady, ppCreating, ppDeleting, ppExpired, ppInvalid},
			returnError:    fmt.Errorf("api error"),
		},
		{
			name:           "DeadlineExceededError",
			returnPolicies: []*gceclient.GceResourcePolicy{wpReady, ppReady, ppCreating, ppDeleting, ppExpired, ppInvalid},
			returnError:    context.DeadlineExceeded,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			enabledExperiments := []string{experiments.ResourcePolicyPullerFlag}
			if tc.disabledExperiment {
				enabledExperiments = []string{}
			}
			fakeProvider := NewFakeResourcePolicyPullerProvider(tc.returnPolicies, tc.returnError)
			rpPuller := NewResourcePolicyPuller(experiments.NewMockManager(enabledExperiments...), fakeProvider, projectID)
			rpPuller.Loop()

			// test rpPuller.Loop()
			if diff := cmp.Diff(tc.wantResourcePolicies, rpPuller.resourcePolicies, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("resource policies diff (-want +got):\n%s", diff)
			}

			// test rpPuller.GetResourcePolicy()
			for _, wantRP := range tc.wantResourcePolicies {
				gotRP := rpPuller.GetResourcePolicy(wantRP.Name)
				if diff := cmp.Diff(wantRP, gotRP, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("resource policy diff (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestResourcePoliciesPullerExperiment(t *testing.T) {
	projectID := "cluster-project"

	wp := &gceclient.GceResourcePolicy{Name: "wpReady", Status: "READY", WorkloadPolicy: gceclient.WorkloadPolicy{MaxTopologyDistance: "3", AcceleratorTopology: "4x4", Type: "HIGH_THROUGHPUT"}}
	returnPolicies := []*gceclient.GceResourcePolicy{wp}

	// enable the experiment
	fakeProvider := NewFakeResourcePolicyPullerProvider(returnPolicies, nil)
	rpPuller := NewResourcePolicyPuller(experiments.NewMockManager(experiments.ResourcePolicyPullerFlag), fakeProvider, projectID)

	// pull and save the policies
	rpPuller.Loop()
	assert.Equal(t, map[string]*gceclient.GceResourcePolicy{wp.Name: wp}, rpPuller.resourcePolicies)
	assert.Equal(t, wp, rpPuller.GetResourcePolicy(wp.Name))

	// disable the experiment
	rpPuller.experimentsManager = experiments.NewMockManager()

	// policies are cleared
	rpPuller.Loop()
	assert.Empty(t, rpPuller.resourcePolicies)
	assert.Nil(t, rpPuller.GetResourcePolicy(wp.Name))
}
