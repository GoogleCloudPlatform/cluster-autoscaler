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
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestReservationNotMatchedByPreemptibleNodePools verifies that Spot and preemptible
// node pools aren't treated as consumers of GCE reservations. Such VMs can't consume
// reservations, so they must not get the reservation discount in the expander.
//
// The test simulates:
//  1. An on-demand node pool and a Spot (or preemptible) node pool of the same machine
//     type, both in the zone of the reservation.
//  2. A GCE Reservation ("res-1") with 2 instances of that machine type.
//  3. Two unschedulable pods that fit on either node pool.
//
// The test verifies that the on-demand node pool is scaled up to consume the
// reservation, and the cheaper Spot (or preemptible) node pool is left untouched.
func TestReservationNotMatchedByPreemptibleNodePools(t *testing.T) {
	const machineType = "n1-standard-4"

	testCases := []struct {
		name                string
		preemptibleNodePool func() *gke_api_beta.NodePool
	}{
		{
			name: "spot node pool",
			preemptibleNodePool: func() *gke_api_beta.NodePool {
				return integration.EmptyNodePool("spot-pool").WithMachineType(machineType).WithLocations(reservationZone).WithSpot().Build()
			},
		},
		{
			name: "preemptible node pool",
			preemptibleNodePool: func() *gke_api_beta.NodePool {
				return integration.EmptyNodePool("preemptible-pool").WithMachineType(machineType).WithLocations(reservationZone).WithPreemptible().Build()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			onDemandNodePool := integration.EmptyNodePool("on-demand-pool").WithMachineType(machineType).WithLocations(reservationZone).Build()
			onDemandNodePool.Config.ReservationAffinity = &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinityAny,
			}
			preemptibleNodePool := tc.preemptibleNodePool()

			testConfig := integration.NewTestConfig().
				WithNodePools(onDemandNodePool, preemptibleNodePool).
				AddReservation(integration.DefaultProject(), createReservation(machineType, "res-1", reservationZone))

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Each pod needs its own node, the reservation has room for both of them.
				for i := 0; i < 2; i++ {
					infra.Fakes.K8s.AddPod(tu.BuildTestPod(fmt.Sprintf(podName, i), 3000, 10000, tu.MarkUnschedulable()))
				}
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				infra.Fakes.RunScheduler(ctx, t)
				assertScheduledPods(ctx, t, infra, 2)
				assert.Equal(t, int64(2), infra.Fakes.GkeService.MustGetTargetSize(t, onDemandNodePool), "Expected the on-demand node pool to be scaled up to consume the reservation")
				assert.Equal(t, int64(0), infra.Fakes.GkeService.MustGetTargetSize(t, preemptibleNodePool), "Expected the node pool that can't consume the reservation to not be scaled up")
				assertReservationInUseCount(t, infra, "res-1", 2)
			})
		})
	}
}
