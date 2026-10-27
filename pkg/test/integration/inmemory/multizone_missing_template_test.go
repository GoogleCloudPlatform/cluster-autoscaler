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

package inmemory

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gcev1 "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	gceinternal "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestMultiZoneMissingTemplateNodeInfoDoesNotPanic (b/556139549) verifies that
// when a multi-zonal node pool has one zone with a real running node (us-central1-a)
// and another zone with 0 nodes (us-central1-b) while template generation fails in both
// zones (e.g. machine type 404), CA does not panic in UpdateNodeInfosWithinNodePools /
// createAugmentedTemplateNodeInfo.
func TestMultiZoneMissingTemplateNodeInfoDoesNotPanic(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolName("multi-zone-pool"),
			integration.WithNodePoolMachineType("mock-zonal-type-2"),
			integration.WithNodePoolSize(0),
			integration.WithNodePoolLocations("us-central1-a", "us-central1-b"),
		))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)
		defer integration_synctest.TearDown(cancel)

		// Add the machine type in us-central1-a so ResizeMig can create a running node there.
		// us-central1-b does not have it (returns 404 on FetchMachineType).
		infra.Fakes.GceService.AddCustomMachineType(&gcev1.MachineType{
			Name:      "mock-zonal-type-2",
			GuestCpus: 2,
			MemoryMb:  4096,
			Zone:      "us-central1-a",
		})

		// Create 1 real node in us-central1-a MIG.
		migARef := gceinternal.GceRef{
			Project: "test-project",
			Zone:    "us-central1-a",
			Name:    "multi-zone-pool",
		}
		err := infra.Fakes.GceService.ResizeMig(ctx, migARef, 1)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		// Make FetchMachineType fail in us-central1-a as well during RunOnce so
		// AnnotationNodeInfoProvider cannot copy NodeGeneratedFromTemplateAnnotation
		// onto migA's real NodeInfo, preserving isNodeInfoReal(nodeInfos[migA.Id()]) == true
		// while migB has no entry in nodeInfos (reproducing b/556139549).
		infra.Fakes.GceService.SetFetchMachineTypeHandler("us-central1-a", "mock-zonal-type-2", func() error {
			return &googleapi.Error{Code: http.StatusNotFound, Message: "Not Found"}
		})

		// Add an unschedulable pod that requires scale up.
		pod := tu.BuildTestPod("test-pod", 3000, 1000, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(pod)

		// Advance virtual time past the node stabilization delay so the node in us-central1-a
		// populates nodeInfos with a real nodeInfo.
		time.Sleep(10 * time.Minute)

		// Run autoscaler loop. Without the fix in UpdateNodeInfosWithinNodePools, this panics
		// with a nil-pointer dereference inside createAugmentedTemplateNodeInfo.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
	})
}
