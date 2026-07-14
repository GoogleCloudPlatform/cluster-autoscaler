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

package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
)

func TestAnyThenFailReservationsBackoff(t *testing.T) {
	currentTime := time.Now()
	initialDuration := 5 * time.Minute
	maxDuration := 30 * time.Minute
	resetTime := 2 * time.Hour

	anyThenFailMig := gke.NewTestGkeMigBuilder().
		SetNodePoolName("any-then-fail-mig").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "n1-standard-1",
			Labels: map[string]string{
				gkelabels.ReservationAffinityLabel: reservations.AnyThenFail,
			},
		}).
		Build()

	otherAffinityMig := gke.NewTestGkeMigBuilder().
		SetNodePoolName("other-affinity-mig").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "n1-standard-1",
			Labels: map[string]string{
				gkelabels.ReservationAffinityLabel: reservations.SpecificAffinity,
			},
		}).
		Build()

	noLabelMig := gke.NewTestGkeMigBuilder().
		SetNodePoolName("no-label-mig").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "n1-standard-1",
		}).
		Build()

	validErrorInfo := cloudprovider.InstanceErrorInfo{
		ErrorCode: gce.ErrorAutomaticReservationsNoCapacity,
	}

	invalidErrorInfo := cloudprovider.InstanceErrorInfo{
		ErrorCode: "some-other-error",
	}

	testCases := []struct {
		name                string
		nodeGroup           cloudprovider.NodeGroup
		errorInfo           cloudprovider.InstanceErrorInfo
		expectedIsBackedOff bool
		callRemoveBackoff   bool
	}{
		{
			name:                "valid any_then_fail mig with relevant error",
			nodeGroup:           anyThenFailMig,
			errorInfo:           validErrorInfo,
			expectedIsBackedOff: true,
		},
		{
			name:                "valid any_then_fail mig with irrelevant error",
			nodeGroup:           anyThenFailMig,
			errorInfo:           invalidErrorInfo,
			expectedIsBackedOff: false,
		},
		{
			name:                "mig with different affinity",
			nodeGroup:           otherAffinityMig,
			errorInfo:           validErrorInfo,
			expectedIsBackedOff: false,
		},
		{
			name:                "mig with no affinity label",
			nodeGroup:           noLabelMig,
			errorInfo:           validErrorInfo,
			expectedIsBackedOff: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewAnyThenFailReservationsBackoff(initialDuration, maxDuration, resetTime)

			// Initial state
			status := b.BackoffStatus(tc.nodeGroup, nil, currentTime)
			assert.False(t, status.IsBackedOff)

			// Apply backoff
			b.Backoff(tc.nodeGroup, nil, tc.errorInfo, currentTime)

			// Check status
			status = b.BackoffStatus(tc.nodeGroup, nil, currentTime.Add(time.Minute))
			assert.Equal(t, tc.expectedIsBackedOff, status.IsBackedOff)
		})
	}
}

func TestAnyThenFailReservationsBackoff_SharedShape(t *testing.T) {
	currentTime := time.Now()
	initialDuration := 5 * time.Minute
	maxDuration := 30 * time.Minute
	resetTime := 2 * time.Hour

	b := NewAnyThenFailReservationsBackoff(initialDuration, maxDuration, resetTime)

	baseLabels := map[string]string{
		gkelabels.ReservationAffinityLabel: reservations.AnyThenFail,
	}

	migBase := gke.NewTestGkeMigBuilder().
		SetNodePoolName("mig-base").
		SetGceRefZone("us-central1-a").
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "n1-standard-1",
			Labels:      baseLabels,
		}).
		Build()

	validErrorInfo := cloudprovider.InstanceErrorInfo{
		ErrorCode: gce.ErrorAutomaticReservationsNoCapacity,
	}

	// Apply backoff on migBase
	b.Backoff(migBase, nil, validErrorInfo, currentTime)

	testCases := []struct {
		name          string
		migToTest     cloudprovider.NodeGroup
		expectBackoff bool
	}{
		{
			name: "same shape",
			migToTest: gke.NewTestGkeMigBuilder().
				SetNodePoolName("mig-same-shape").
				SetGceRefZone("us-central1-a").
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "n1-standard-1",
					Labels:      baseLabels,
				}).
				Build(),
			expectBackoff: true,
		},
		{
			name: "different zone",
			migToTest: gke.NewTestGkeMigBuilder().
				SetNodePoolName("mig-diff-zone").
				SetGceRefZone("us-central1-b").
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "n1-standard-1",
					Labels:      baseLabels,
				}).
				Build(),
			expectBackoff: false,
		},
		{
			name: "different cpu platform",
			migToTest: gke.NewTestGkeMigBuilder().
				SetNodePoolName("mig-diff-cpu").
				SetGceRefZone("us-central1-a").
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType:    "n1-standard-1",
					MinCpuPlatform: "Intel Skylake",
					Labels:         baseLabels,
				}).
				Build(),
			expectBackoff: false,
		},
		{
			name: "different accelerator",
			migToTest: gke.NewTestGkeMigBuilder().
				SetNodePoolName("mig-diff-accel").
				SetGceRefZone("us-central1-a").
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "n1-standard-1",
					Accelerators: []*gke_api_beta.AcceleratorConfig{
						{AcceleratorType: "nvidia-tesla-t4", AcceleratorCount: 1},
					},
					Labels: baseLabels,
				}).
				Build(),
			expectBackoff: false,
		},
		{
			name: "different ssd",
			migToTest: gke.NewTestGkeMigBuilder().
				SetNodePoolName("mig-diff-ssd").
				SetGceRefZone("us-central1-a").
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "n1-standard-1",
					LocalSSDConfig: &gkeclient.LocalSSDConfig{
						LocalSsdCount: 1,
					},
					Labels: baseLabels,
				}).
				Build(),
			expectBackoff: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectBackoff, b.BackoffStatus(tc.migToTest, nil, currentTime.Add(time.Minute)).IsBackedOff)
		})
	}

	// Apply backoff to the accelerator mig and verify it doesn't affect base
	migDiffAccelerator := testCases[3].migToTest
	b.Backoff(migDiffAccelerator, nil, validErrorInfo, currentTime)
	assert.True(t, b.BackoffStatus(migDiffAccelerator, nil, currentTime.Add(time.Minute)).IsBackedOff)

	// Remove backoff using a mig with the same shape as migBase
	migSameShape := testCases[0].migToTest
	b.RemoveBackoff(migSameShape, nil)

	// Both migBase and migSameShape should no longer be backed off
	assert.False(t, b.BackoffStatus(migBase, nil, currentTime.Add(time.Minute)).IsBackedOff)
	assert.False(t, b.BackoffStatus(migSameShape, nil, currentTime.Add(time.Minute)).IsBackedOff)

	// migDiffAccelerator should still be backed off as it has a different shape and is isolated
	assert.True(t, b.BackoffStatus(migDiffAccelerator, nil, currentTime.Add(time.Minute)).IsBackedOff)
}
