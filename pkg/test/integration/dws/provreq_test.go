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

package dws

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	provreqtest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/provreq"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

func TestProvisioningRequest_Stockout(t *testing.T) {
	dummyPod := tu.BuildTestPod("dummy", 1000, 1000)
	prw := provreqtest.BuildTestProvisioningRequest("default", "test-pr", nil, dummyPod.Spec)

	testConfig := integration.NewTestConfig().
		WithProvisioningRequests(prw).
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		// Simulate a stockout error for the n1-highcpu-2 machine type in all zones by setting its capacity to 0 in the fake GCE service.
		for _, zone := range integration.DefaultZones {
			infra.Fakes.GceService.SetBackendMachineCount(zone, "n1-highcpu-2", 0)
		}

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		p := tu.BuildTestPod("fs-queued-pod", 1000, 1000, tu.MarkUnschedulable(),
			pod.WithNodeSelectorEntry("cloud.google.com/gke-machine-type", "n1-highcpu-2"),
			pod.WithAnnotations(map[string]string{
				"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "test-pr",
			}),
			pod.WithCreationTimestamp(metav1.Time{Time: time.Now().Add(-1 * time.Hour)}),
		)
		infra.Fakes.K8s.AddPod(p)

		// Run autoscaler
		// The first loop triggers the scale-up attempt which encounters the injected ZONE_RESOURCE_POOL_EXHAUSTED stockout error.
		integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
		// The second loop advances the virtual clock by the scale-up failure timeout,
		// allowing the ProvisioningRequest Reconciler to mark the PR as Failed.
		// We use RunOnceAfter and ignore the error because the synchronous deletion from
		// fakeGCE causes a brief "not found" error in the CA node registry during this loop.
		_ = integration_synctest.RunOnceAfter(t, autoscaler, reconciler.MaxObtainabilityStrategyUnreconciledPeriod)

		// Verify that ProvisioningRequest gets a Failed condition.
		updatedPR, err := infra.Fakes.PRClientset.AutoscalingV1().ProvisioningRequests("default").Get(ctx, "test-pr", metav1.GetOptions{})
		assert.NoError(t, err)

		failedCond := findCondition(updatedPR, prv1.Failed)

		assert.NotNil(t, failedCond, "Expected PR to have Failed condition due to stockout")
		if failedCond != nil {
			assert.Equal(t, "InternalErrorFailedToQueue", failedCond.Reason)
			assert.Contains(t, failedCond.Message, "ZONE_RESOURCE_POOL_EXHAUSTED")
			assert.Contains(t, failedCond.Message, "GCE API error: stock out")
		}
	})
}
