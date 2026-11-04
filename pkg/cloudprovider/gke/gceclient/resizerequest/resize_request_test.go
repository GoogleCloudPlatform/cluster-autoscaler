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

package resizerequestclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResizeRequestStatusRef(t *testing.T) {
	t.Parallel()

	rr := ResizeRequestStatus{
		Zone:    "us-central1-a",
		MigName: "mig-gpu-pool",
		Name:    "gke-default-job-1234",
	}
	assert.Equal(t, "us-central1-a/mig-gpu-pool/gke-default-job-1234", rr.Ref())
}

func TestSortResizeRequestsByCreationTimestampAsc(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	rrs := []ResizeRequestStatus{
		{Name: "third", CreationTime: t0.Add(20 * time.Minute)},
		{Name: "first", CreationTime: t0},
		{Name: "fourth", CreationTime: t0.Add(30 * time.Minute)},
		{Name: "second", CreationTime: t0.Add(5 * time.Minute)},
	}

	SortResizeRequestsByCreationTimestampAsc(rrs)

	gotNames := []string{rrs[0].Name, rrs[1].Name, rrs[2].Name, rrs[3].Name}
	assert.Equal(t, []string{"first", "second", "third", "fourth"}, gotNames)
}

func TestStateFromGCE(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		want    ResizeRequestState
		wantErr bool
	}{
		{input: "ACCEPTED", want: ResizeRequestStateAccepted},
		{input: "accepted", want: ResizeRequestStateAccepted},
		{input: "CREATING", want: ResizeRequestStateCreating},
		{input: "DELETING", want: ResizeRequestStateDeleting},
		{input: "CANCELLED", want: ResizeRequestStateCancelled},
		{input: "FAILED", want: ResizeRequestStateFailed},
		{input: "PROVISIONING", want: ResizeRequestStateProvisioning},
		{input: "SUCCEEDED", want: ResizeRequestStateSucceeded},
		{input: "UNKNOWN_STATE", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got, err := stateFromGCE(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorContains(t, err, "unknown resize request state")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestResizeRequestNamingHelpers(t *testing.T) {
	t.Parallel()

	t.Run("ProvisioningRequest prefix", func(t *testing.T) {
		t.Parallel()
		name := ResizeRequestName("team-ns", "train-job")
		assert.True(t, IsProvisioningRequestManagedResizeRequest(name))
		assert.False(t, IsAtomicResizeRequest(name))
		assert.False(t, IsFlexStartNonQueuedScaleUpResizeRequest(name))
	})

	t.Run("Atomic TPU prefix", func(t *testing.T) {
		t.Parallel()
		name1 := AtomicResizeRequestName()
		name2 := AtomicResizeRequestName()
		assert.NotEqual(t, name1, name2)
		assert.LessOrEqual(t, len(name1), 63)
		assert.True(t, IsAtomicResizeRequest(name1))
		assert.False(t, IsProvisioningRequestManagedResizeRequest(name1))
		assert.False(t, IsFlexStartNonQueuedScaleUpResizeRequest(name1))
	})

	t.Run("FlexStart non-queued prefix and scale-up ID round-trip", func(t *testing.T) {
		t.Parallel()
		scaleUpID1 := NewFlexStartNonQueuedScaleUpId()
		scaleUpID2 := NewFlexStartNonQueuedScaleUpId()
		assert.NotEqual(t, scaleUpID1, scaleUpID2)

		rrName := FlexStartNonQueuedResizeRequestName(scaleUpID1, 12)
		assert.LessOrEqual(t, len(rrName), 63)
		assert.True(t, IsFlexStartNonQueuedScaleUpResizeRequest(rrName))
		assert.False(t, IsAtomicResizeRequest(rrName))
		assert.False(t, IsProvisioningRequestManagedResizeRequest(rrName))
		assert.Equal(t, scaleUpID1, FlexScaleUpId(rrName))
	})
}
