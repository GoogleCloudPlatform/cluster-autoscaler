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

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gceapi "google.golang.org/api/compute/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/core"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"

	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	ccc_builder "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
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
		WithOverrides(integration.WithAutoProvisioningEnabled()).
		WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
		WithNpcCrds(createCCCCRD()).
		AddReservation(integration.DefaultProject(), createReservation())

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		stopCh := make(chan struct{})

		autoscaler, err := integration.SetupAutoscaler(t, ctx, testConfig, infra, stopCh)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel, stopCh)

		for i := 0; i < 2; i++ {
			pod := tu.BuildTestPod(fmt.Sprintf(podName, i), 3000, 10000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc"))
			infra.Fakes.K8s.AddPod(pod)
		}
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

		infra.Fakes.RunScheduler(ctx, t)
		assertScheduledPods(t, infra, ctx, 2)
		assertNodePoolReservationAffinity(t, infra)
		assertNodeGroupLocation(t, autoscaler)
		assertReservationInUseCount(t, infra)
	})
}

func createCCCCRD() crd.CRD {
	cc := ccc_builder.NewComputeClassBuilder("my-ccc").
		WithNodePoolAutoCreation(true).
		WithPriorities(v1.Priority{
			MachineFamily: ptr.To("n1"),
			Reservations: &v1.Reservations{
				Affinity: v1.AnyThenFail,
			},
		}).
		Build()
	return ccc_builder.NewCccCrdBuilder(cc).Build()
}

func createReservation() *gceapi.Reservation {
	return reservations.NewTestReservationBuilder().
		WithName("res-1").
		WithZone(reservationZone).
		WithMachineType("n1-standard-4").
		WithCounts(0, 2).
		WithSpecificReservationRequired(false).
		Build()
}

func assertScheduledPods(t *testing.T, infra *integration.TestInfrastructure, ctx context.Context, podCount int) {
	t.Helper()
	for i := 0; i < podCount; i++ {
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, fmt.Sprintf(podName, i), metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, fmt.Sprintf("Expected %s to be scheduled by RunScheduler", fmt.Sprintf(podName, i)))
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

func assertNodeGroupLocation(t *testing.T, autoscaler *core.StaticAutoscaler) {
	t.Helper()
	var scaledUpGroup cloudprovider.NodeGroup
	for _, ng := range autoscaler.CloudProvider.NodeGroups() {
		targetSize, err := ng.TargetSize()
		assert.NoError(t, err)
		if targetSize > 0 {
			if scaledUpGroup != nil {
				t.Fatalf("Expected only one node group to be scaled up, but found multiple: %s and %s", scaledUpGroup.Id(), ng.Id())
			}
			scaledUpGroup = ng
		}
	}
	if assert.NotNil(t, scaledUpGroup, "Expected to find a scaled up node group") {
		if gkeMig, ok := scaledUpGroup.(*gke.GkeMig); ok {
			assert.Equal(t, reservationZone, gkeMig.GceRef().Zone, "The scaled up node group should be in the reservation zone.")
		} else {
			t.Fatalf("Expected scaled up node group to be a GkeMig, but got %T", scaledUpGroup)
		}
	}
}

func assertReservationInUseCount(t *testing.T, infra *integration.TestInfrastructure) {
	t.Helper()
	rsv, err := infra.Fakes.GceService.FetchReservation("test-project", "res-1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rsv.SpecificReservation.InUseCount)
}
