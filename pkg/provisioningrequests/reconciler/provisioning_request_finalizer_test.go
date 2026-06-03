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

package reconciler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
)

func TestFinalize(t *testing.T) {
	mig := &gce.GceRef{Project: "testProject", Name: "gke-test-cluster-example-test2-840da30b-grp", Zone: "us-east7-b"}
	tests := []struct {
		name          string
		pr            *provreqwrapper.ProvisioningRequest
		expectedState provreqstate.ProvisioningRequestState
	}{
		{
			name:          "PR_Provisioned_recently_stays_Provisioned",
			pr:            provreqstate.ProvisioningRequestInStateForTests("default", "prov3", "gke-default-prov3-693c08ad911d8c40", mig.Name, provreqstate.ProvisionedState, exampleInitTime, exampleTimeInc),
			expectedState: provreqstate.ProvisionedState,
		},
		{
			name:          "PR_Provisioned_for_over_10min_goes_BookingExpired",
			pr:            provreqstate.ProvisioningRequestInStateForTests("default", "prov3", "gke-default-prov3-693c08ad911d8c40", mig.Name, provreqstate.ProvisionedState, exampleInitTime.Add(-provreqstate.BookingDuration), exampleTimeInc),
			expectedState: provreqstate.BookingExpiredState,
		},
		{
			name:          "PR_BookingExpired_was_provisioned_for_over_MRD_goes_CapacityRevoked",
			pr:            provreqstate.ProvisioningRequestInStateForTests("default", "prov3", "gke-default-prov3-693c08ad911d8c40", mig.Name, provreqstate.BookingExpiredState, exampleInitTime.Add(-10*time.Hour), exampleTimeInc, provreqstate.WithMaxRunDuration(fmt.Sprintf("%d", 10*60*60))),
			expectedState: provreqstate.CapacityRevokedState,
		},
		{
			name:          "PR_BookingExpired_recently_stays_BookingExpired",
			pr:            provreqstate.ProvisioningRequestInStateForTests("default", "prov3", "gke-default-prov3-693c08ad911d8c40", mig.Name, provreqstate.BookingExpiredState, exampleInitTime, exampleTimeInc, provreqstate.WithMaxRunDuration(fmt.Sprintf("%d", 10*60*60))),
			expectedState: provreqstate.BookingExpiredState,
		},
		{
			name:          "PR_Failed_old_gets_deleted",
			pr:            provreqstate.ProvisioningRequestInStateForTests("default", "prov-failed", "gke-default-prov-failed-2ba917d6dc64dab7", mig.Name, provreqstate.FailedState, exampleInitTime.Add(-terminalProvisioningRequestTTL), exampleTimeInc),
			expectedState: "",
		},
		{
			name:          "PR_Failed_recent_stays",
			pr:            provreqstate.ProvisioningRequestInStateForTests("default", "prov-failed", "gke-default-prov-failed-2ba917d6dc64dab7", mig.Name, provreqstate.FailedState, exampleInitTime, exampleTimeInc),
			expectedState: provreqstate.FailedState,
		},
		{
			name:          "PR_CapacityRevoked_old_gets_deleted",
			pr:            provreqstate.ProvisioningRequestInStateForTests("default", "prov-cap-revoked", "gke-default-prov-failed-2ba917d6dc64dab7", mig.Name, provreqstate.CapacityRevokedState, exampleInitTime.Add(-terminalProvisioningRequestTTL), exampleTimeInc),
			expectedState: "",
		},
		{
			name:          "PR_CapacityRevoked_recent_stays",
			pr:            provreqstate.ProvisioningRequestInStateForTests("default", "prov-cap-revoked", "gke-default-prov-failed-2ba917d6dc64dab7", mig.Name, provreqstate.CapacityRevokedState, exampleInitTime, exampleTimeInc),
			expectedState: provreqstate.CapacityRevokedState,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, tt.pr)

			prFinalizer := &provisioningRequestFinalizer{prClient: fakeClient}
			prs := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{
				provreqstate.StateOfProvisioningRequest(tt.pr): {tt.pr},
			}

			gotUnreconciled, err := prFinalizer.reconcileRequests(&reconcilingInput{
				prs: prs,
				now: recentTimestamp,
			})
			assert.NoError(t, err)
			for _, v := range gotUnreconciled {
				assert.Empty(t, v)
			}

			newPR, err := fakeClient.ProvisioningRequestNoCache(tt.pr.Namespace, tt.pr.Name)
			if tt.expectedState != "" {
				assert.NoError(t, err, tt)
				assert.Equal(t, tt.expectedState, provreqstate.StateOfProvisioningRequest(newPR))
			} else { // Provisioning Request is expected to be missing
				assert.Error(t, err, tt)
			}
		})
	}
}

func TestDeleteOldProvisioningRequestsOverQuota(t *testing.T) {
	mig := &gce.GceRef{Name: "gke-test-cluster-example-test2-840da30b-grp", Zone: "us-east7-b"}
	tests := []struct {
		name               string
		capacityRevokedPRs int
		failedPRs          int
		wantDeleted        int
		now                time.Time
	}{
		{
			name:               "all new ProvReqs, nothing deleted",
			capacityRevokedPRs: defaultDeletedProvisioningRequestsPerLoop,
			failedPRs:          defaultDeletedProvisioningRequestsPerLoop,
			wantDeleted:        0,
			now:                recentTimestamp,
		},
		{
			name:               "all old ProvReqs get deleted",
			capacityRevokedPRs: 1,
			failedPRs:          1,
			wantDeleted:        2,
			now:                recentTimestamp.Add(terminalProvisioningRequestTTL),
		},
		{
			name:               "`defaultDeletedProvisioningRequestsPerLoop` get deleted, 1 stays",
			capacityRevokedPRs: defaultDeletedProvisioningRequestsPerLoop - 1,
			failedPRs:          2,
			wantDeleted:        defaultDeletedProvisioningRequestsPerLoop,
			now:                recentTimestamp.Add(terminalProvisioningRequestTTL),
		},
		{
			name:               "all Provisioned ProvReqs - `defaultDeletedProvisioningRequestsPerLoop` get deleted, 1 stays",
			capacityRevokedPRs: defaultDeletedProvisioningRequestsPerLoop + 1,
			failedPRs:          0,
			wantDeleted:        defaultDeletedProvisioningRequestsPerLoop,
			now:                recentTimestamp.Add(terminalProvisioningRequestTTL),
		},
		{
			name:               "`defaultDeletedProvisioningRequestsPerLoop` get deleted and the rest `defaultDeletedProvisioningRequestsPerLoop` stays",
			capacityRevokedPRs: defaultDeletedProvisioningRequestsPerLoop,
			failedPRs:          defaultDeletedProvisioningRequestsPerLoop,
			wantDeleted:        defaultDeletedProvisioningRequestsPerLoop,
			now:                recentTimestamp.Add(terminalProvisioningRequestTTL),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			prsMap := map[provreqstate.ProvisioningRequestState][]*provreqwrapper.ProvisioningRequest{}
			prsMap[provreqstate.CapacityRevokedState] = make([]*provreqwrapper.ProvisioningRequest, 0, tt.capacityRevokedPRs)
			prsMap[provreqstate.FailedState] = make([]*provreqwrapper.ProvisioningRequest, 0, tt.failedPRs)
			for i := 0; i < tt.capacityRevokedPRs; i++ {
				prName := fmt.Sprintf("prCapacityRevoked%d", i)
				prsMap[provreqstate.CapacityRevokedState] = append(prsMap[provreqstate.CapacityRevokedState], provReqInState("default", prName, "", mig.Name, provreqstate.CapacityRevokedState))
			}
			for i := 0; i < tt.failedPRs; i++ {
				prName := fmt.Sprintf("prFailed%d", i)
				prsMap[provreqstate.FailedState] = append(prsMap[provreqstate.FailedState], provReqInState("default", prName, "", mig.Name, provreqstate.FailedState))
			}

			prs := append(prsMap[provreqstate.CapacityRevokedState], prsMap[provreqstate.FailedState]...)
			fakeClient := provreqclient.NewFakeProvisioningRequestClient(ctx, t, prs...)
			r := &provisioningRequestFinalizer{prClient: fakeClient}

			gotUnreconciled, err := r.reconcileRequests(&reconcilingInput{
				prs: prsMap,
				now: tt.now,
			})
			assert.NoError(t, err, tt)
			for _, v := range gotUnreconciled {
				assert.Empty(t, v)
			}

			gotExisting, gotDeleted := 0, 0
			for _, wantPR := range prs {
				gotPR, err := fakeClient.ProvisioningRequestNoCache(wantPR.Namespace, wantPR.Name)
				if err != nil {
					gotDeleted++
				} else {
					gotExisting++
					assert.Equal(t, wantPR, gotPR)
				}
			}
			assert.Equal(t, tt.wantDeleted, gotDeleted)
			assert.Equal(t, len(prs)-tt.wantDeleted, gotExisting)
		})
	}
}
