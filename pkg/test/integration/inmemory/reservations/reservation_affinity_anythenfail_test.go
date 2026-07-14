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
	"math/rand/v2"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gceapi "google.golang.org/api/compute/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/core"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"

	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	ccc_builder "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/recorder"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

const podName = "my-pod-%d"
const reservationZone = "us-central1-b"

// TestCCCReservationAnyThenFailSuccess verifies that the CA can successfully scale up
// a node pool for pods requesting a ComputeClass with "AnyThenFail" reservation affinity
// when a matching GCE reservation is available.
//
// The test simulates:
//  1. A ComputeClass CRD ("my-ccc") configured with NAP enabled and "AnyThenFail" reservation
//     affinity for machine family n1.
//  2. A GCE Reservation ("res-1") with 2 instances of "n1-standard-4" in zone "us-central1-b".
//  3. Two replicas of an unschedulable pod requesting "my-ccc" via node selector.
//
// The test verifies that:
// 1. Both pods are successfully scheduled on the new nodes in "us-central1-b".
// 2. The created NodePool has the correct "ANY_RESERVATION_THEN_FAIL" reservation affinity set.
// 3. That the scale up happens for a MIG that is in the same zone as the reservation.
// 4. The reservation "InUseCount" is correctly updated to 2 by the GKE ReservationBalancingProcessor.
func TestCCCReservationAnyThenFailSuccess(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(integration.WithAutoProvisioningEnabled(), integration.WithBalanceSimilarNodeGroups()).
		WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
		WithCccCrds(createCCCCRD()).
		AddReservation(integration.DefaultProject(), createReservation("n1-standard-4", "res-1", reservationZone))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		for i := 0; i < 2; i++ {
			pod := tu.BuildTestPod(fmt.Sprintf(podName, i), 3000, 10000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"))
			infra.Fakes.K8s.AddPod(pod)
		}
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		infra.Fakes.RunScheduler(ctx, t)
		assertScheduledPods(ctx, t, infra, 2)
		assertNodePoolReservationAffinity(t, infra)
		assertNodeGroupLocationAndTargetSize(t, autoscaler, reservationZone, 2)
		assertReservationInUseCount(t, infra, "res-1", 2)
	})
}

// TestCCCReservationAnyThenFailRejectsNoReservation verifies that the CA will NOT scale up
// a node pool for pods requesting a ComputeClass with "AnyThenFail" reservation affinity
// when NO matching GCE reservation is available.
func TestCCCReservationAnyThenFailRejectsNoReservation(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(integration.WithAutoProvisioningEnabled(), integration.WithBalanceSimilarNodeGroups()).
		WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
		WithCccCrds(createCCCCRD()).
		// add reservation with non-matching machine type
		AddReservation(integration.DefaultProject(), createReservation("n2-standard-4", "res-1", reservationZone))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		for i := 0; i < 2; i++ {
			pod := tu.BuildTestPod(fmt.Sprintf(podName, i), 3000, 10000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"))
			infra.Fakes.K8s.AddPod(pod)
		}
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// The pods should remain unschedulable
		infra.Fakes.RunScheduler(ctx, t)
		assertUnscheduledPods(ctx, t, infra, 0, 2)
		assertNoScaleUps(t, autoscaler)
	})
}

// TestCCCReservationAnyThenFailFallsBackToSecondPriorityGracefully verifies that the CA
// will do a scale-up only till available capacity in reservation for first priorty, and
// that in the next loop, CA provisions remaining nodes from lower priority
func TestCCCReservationAnyThenFailFallsBackToSecondPriorityGracefully(t *testing.T) {
	cc := ccc_builder.NewComputeClassBuilder("my-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(
			v1.Priority{
				MachineFamily: ptr.To("n1"),
				Reservations: &v1.Reservations{
					Affinity: v1.AnyThenFail,
				},
			},
			v1.Priority{
				MachineFamily: ptr.To("n2"),
			},
		).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(integration.WithAutoProvisioningEnabled(), integration.WithBalanceSimilarNodeGroups()).
		WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
		WithCccCrds(cc).
		AddReservation(integration.DefaultProject(), createReservation("n1-standard-4", "res-1", reservationZone))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// reservation has room for 2 nodes, for 2 pods, 2 pods will be unschedulable after first loop
		for i := 0; i < 4; i++ {
			pod := tu.BuildTestPod(fmt.Sprintf(podName, i), 3000, 10000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"))
			infra.Fakes.K8s.AddPod(pod)
		}
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// 2 pods should be scheduled on first priority and two unscheduled
		infra.Fakes.RunScheduler(ctx, t)
		assertScheduledPods(ctx, t, infra, 2)
		assertUnscheduledPods(ctx, t, infra, 2, 2)
		assertNodePoolReservationAffinity(t, infra)
		assertNodeGroupLocationAndTargetSize(t, autoscaler, reservationZone, 2)
		assertReservationInUseCount(t, infra, "res-1", 2)

		// second loop, 2 remaining pods should be scheduled on a new, second node pool,
		// that doesn't have reservation affinity
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		infra.Fakes.RunScheduler(ctx, t)
		assertScheduledPods(ctx, t, infra, 4)
		assertNodePoolNoneReservationAffinity(t, infra)
	})
}

// TestCCCReservationAnyThenFailBackoff verifies that the CA correctly applies the
// anyThenFailReservationsBackoff when scale-up fails due to reservation capacity
// limits and that this backoff strictly isolates to the node shape in the failing zone,
// allowing another zone to successfully scale up.
func TestCCCReservationAnyThenFailBackoff(t *testing.T) {
	zoneA := "us-central1-a"
	zoneB := "us-central1-b"

	testConfig := integration.NewTestConfig().
		WithOverrides(integration.WithAutoProvisioningEnabled(), integration.WithBalanceSimilarNodeGroups()).
		WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
		WithCccCrds(createCCCCRD()).
		AddReservation(integration.DefaultProject(), createReservation("n1-standard-4", "res-a", zoneA)).
		AddReservation(integration.DefaultProject(), createReservation("n1-standard-4", "res-b", zoneB))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		autoscaler.AutoscalingContext.Recorder = recorder.SetupFakeRecorder(autoscaler)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Create 2 pods to consume the reservation
		for i := 0; i < 2; i++ {
			pod := tu.BuildTestPod(fmt.Sprintf(podName, i), 3000, 10000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"))
			infra.Fakes.K8s.AddPod(pod)
		}

		// inject the error for the zone B
		infra.Fakes.GceService.SetCreateInstanceForZoneError(zoneB, cloudprovider.InstanceErrorInfo{
			ErrorClass: cloudprovider.OtherErrorClass,
			ErrorCode:  gce.ErrorAutomaticReservationsNoCapacity,
		})

		// Loop 1: NAP creates node groups
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Loop 2: Scale-up triggers resize and hits injected error
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		assertBackoffForMigInZone(t, autoscaler, zoneB)
		recorder.AssertEventsContains(t, autoscaler.AutoscalingContext.Recorder, "Any affinity reservations no capacity", time.Second)

		// Because zone B is backed off, CA should try to scale up zone A next.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		infra.Fakes.RunScheduler(ctx, t)
		assertScheduledPods(ctx, t, infra, 2)
		assertNodeGroupLocationAndTargetSize(t, autoscaler, zoneA, 2)
		assertReservationInUseCount(t, infra, "res-a", 2)
	})
}

func createCCCCRD() *v1.ComputeClass {
	cc := ccc_builder.NewComputeClassBuilder("my-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(v1.Priority{
			MachineFamily: ptr.To("n1"),
			Reservations: &v1.Reservations{
				Affinity: v1.AnyThenFail,
			},
		}).
		Build()
	return cc
}

func createReservation(machineType, name, zone string) *gceapi.Reservation {
	rsv := reservations.NewTestReservationBuilder().
		WithId(rand.Uint64()).
		WithName(name).
		WithZone(zone).
		WithMachineType(machineType).
		WithCounts(0, 2).
		WithSpecificReservationRequired(false).
		Build()
	rsv.SelfLink = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/test-project/zones/%s/reservations/%s", zone, name)
	return rsv
}

func assertScheduledPods(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, podCount int) {
	t.Helper()
	for i := 0; i < podCount; i++ {
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, fmt.Sprintf(podName, i), metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, fmt.Sprintf("Expected %s to be scheduled by RunScheduler", fmt.Sprintf(podName, i)))
	}
}

func assertUnscheduledPods(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, startNumber int, podCount int) {
	t.Helper()
	for i := startNumber; i < podCount; i++ {
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, fmt.Sprintf(podName, i), metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPod.Spec.NodeName, fmt.Sprintf("Expected %s to not be scheduled by RunScheduler", fmt.Sprintf(podName, i)))
	}
}

func assertNodePoolReservationAffinity(t *testing.T, infra *integration.TestInfrastructure) {
	t.Helper()
	cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
	assert.NoError(t, err)
	assert.Equal(t, len(cluster.NodePools), 1, "Expected to find exactly 1 NAP node pool")
	np := cluster.NodePools[0]
	assert.NotNil(t, np.Config, "Expected node pool config to be non-nil")
	assert.NotNil(t, np.Config.ReservationAffinity, "Expected ReservationAffinity to be non-nil")
	assert.Equal(t, gkeclient.ReservationAffinityAnyThenFail, np.Config.ReservationAffinity.ConsumeReservationType)
}

func assertNodePoolNoneReservationAffinity(t *testing.T, infra *integration.TestInfrastructure) {
	cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(cluster.NodePools), "Expected to find exactly 2 NAP node pools")

	var foundFallback bool
	for _, np := range cluster.NodePools {
		if np.Config != nil && (np.Config.ReservationAffinity == nil || np.Config.ReservationAffinity.ConsumeReservationType == gkeclient.ReservationAffinityNone) {
			foundFallback = true
		}
	}
	assert.True(t, foundFallback, "Expected to find a fallback node pool with NO_RESERVATION affinity or nil affinity")
}

func assertNodeGroupLocationAndTargetSize(t *testing.T, autoscaler *core.StaticAutoscaler, expectedZone string, expectedTargetSize int) {
	t.Helper()
	var scaledUpGroup cloudprovider.NodeGroup
	var actualTargetSize int
	for _, ng := range autoscaler.CloudProvider.NodeGroups() {
		targetSize, err := ng.TargetSize()
		assert.NoError(t, err)
		if targetSize > 0 {
			if scaledUpGroup != nil {
				t.Fatalf("Expected only one node group to be scaled up, but found multiple: %s and %s", scaledUpGroup.Id(), ng.Id())
			}
			scaledUpGroup = ng
			actualTargetSize = targetSize
		}
	}
	if assert.NotNil(t, scaledUpGroup, "Expected to find a scaled up node group") {
		if gkeMig, ok := scaledUpGroup.(*gke.GkeMig); ok {
			assert.Equal(t, expectedZone, gkeMig.GceRef().Zone, "The scaled up node group should be in the reservation zone.")
			assert.Equal(t, expectedTargetSize, actualTargetSize, "The scaled up node group should have the expected target size.")
		} else {
			t.Fatalf("Expected scaled up node group to be a GkeMig, but got %T", scaledUpGroup)
		}
	}
}

func assertReservationInUseCount(t *testing.T, infra *integration.TestInfrastructure, resName string, expectedCount int64) {
	t.Helper()
	rsv, err := infra.Fakes.GceService.FetchReservation("test-project", resName)
	assert.NoError(t, err)
	assert.Equal(t, expectedCount, rsv.SpecificReservation.InUseCount)
}

func assertNoScaleUps(t *testing.T, autoscaler *core.StaticAutoscaler) {
	for _, ng := range autoscaler.CloudProvider.NodeGroups() {
		targetSize, err := ng.TargetSize()
		assert.NoError(t, err)
		assert.Equal(t, 0, targetSize, "Expected node group %s to not scale up", ng.Id())
	}
}

func assertBackoffForMigInZone(t *testing.T, autoscaler *core.StaticAutoscaler, zone string) {
	var mig cloudprovider.NodeGroup
	now := time.Now()
	for _, ng := range autoscaler.CloudProvider.NodeGroups() {
		gkeMig, _ := ng.(*gke.GkeMig)
		if gkeMig.GceRef().Zone == zone {
			mig = ng
		}
	}
	assert.NotNil(t, mig, "Expected to find a mig in zone %s", zone)
	assert.True(t, autoscaler.ClusterStateRegistry.BackoffStatusForNodeGroup(mig, now).IsBackedOff, "Expected mig %s in zone %s to be backed off at time %v", mig.Id(), zone, now)
}
