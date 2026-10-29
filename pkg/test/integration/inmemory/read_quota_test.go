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
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// readQuotaExceededError mirrors the error returned by GCE when the per-minute
// read requests quota is exhausted (see b/573583546).
func readQuotaExceededError() error {
	const message = "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute per region' of service 'compute.googleapis.com' for consumer 'project_number:123456789'."
	return &googleapi.Error{
		Code:    http.StatusForbidden,
		Message: message,
		Errors:  []googleapi.ErrorItem{{Reason: "rateLimitExceeded", Message: message}},
		Details: []any{
			map[string]any{
				"@type":  "type.googleapis.com/google.rpc.ErrorInfo",
				"reason": "RATE_LIMIT_EXCEEDED",
				"domain": "googleapis.com",
			},
		},
	}
}

func TestReadQuotaExhaustedListsMigsOncePerLoop(t *testing.T) {
	zones := []string{"us-central1-a", "us-central1-b", "us-central1-c"}
	testConfig := integration.NewTestConfig().
		WithNodePools(
			integration.DefaultNodePool(
				integration.WithNodePoolName("default-pool"),
				integration.WithNodePoolMachineType("n1-standard-4"),
				integration.WithNodePoolSize(1),
				integration.WithNodePoolLocations(zones...),
			),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if err != nil {
			t.Fatalf("SetupAutoscaler failed: %v", err)
		}

		// Initial healthy loop to populate caches.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Len(t, infra.Fakes.K8s.Nodes().Items, 3)

		// Simulate read quota exhaustion across all zones.
		quotaErr := readQuotaExceededError()
		for _, zone := range zones {
			infra.Fakes.GceService.SetZoneError(zone, quotaErr)
		}

		infra.Fakes.GceService.ResetCallCounts()
		err = integration_synctest.RunOnceAfter(ctx, t, autoscaler, 10*time.Second)
		assert.Error(t, err, "RunOnce should fail when all zones fail GCE read quota")

		for _, zone := range zones {
			assert.Equal(t, 1, infra.Fakes.GceService.FetchAllMigsCalls(zone), "zone %s should only list MIGs once per loop during read quota exhaustion", zone)
		}
		for _, mig := range infra.GkeCache.GetMigs() {
			assert.Equal(t, 0, infra.Fakes.GceService.FetchMigCalls(mig.GceRef()), "MIG %s should not be fetched individually while its zone is out of read quota", mig.GceRef())
			assert.False(t, infra.GkeCache.IsMigBlocked(mig.GceRef()), "MIG %s should not be blocked on HTTP 403 quota error", mig.GceRef())
		}

		// Next loop: Refresh() clears migListingErrors so CA retries listing
		// once per zone.
		infra.Fakes.GceService.ResetCallCounts()
		err = integration_synctest.RunOnceAfter(ctx, t, autoscaler, 10*time.Second)
		assert.Error(t, err)
		for _, zone := range zones {
			assert.Equal(t, 1, infra.Fakes.GceService.FetchAllMigsCalls(zone), "zone %s should retry listing MIGs once on the next loop", zone)
		}
		for _, mig := range infra.GkeCache.GetMigs() {
			assert.Equal(t, 0, infra.Fakes.GceService.FetchMigCalls(mig.GceRef()), "MIG %s should not be fetched individually while its zone is out of read quota", mig.GceRef())
		}

		// Recovery: clear quota errors, fill existing nodes, add an
		// unschedulable pod, and verify scale-up.
		for _, zone := range zones {
			infra.Fakes.GceService.ClearZoneError(zone)
		}
		for i, node := range infra.Fakes.K8s.Nodes().Items {
			fillerPod := tu.BuildTestPod(fmt.Sprintf("filler-pod-%d", i), 3500, 0)
			fillerPod.Spec.NodeName = node.Name
			infra.Fakes.K8s.AddPod(fillerPod)
		}
		unschedulablePod := tu.BuildTestPod("unschedulable-pod", 3000, 0, tu.MarkUnschedulable())
		infra.Fakes.K8s.AddPod(unschedulablePod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)
		assert.Len(t, infra.Fakes.K8s.Nodes().Items, 4, "cluster should scale up after quota recovery")
	})
}

func TestReadQuotaExhaustedInOneZoneDoesNotRelistHealthyZones(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(
			integration.DefaultNodePool(
				integration.WithNodePoolName("pool-a"),
				integration.WithNodePoolMachineType("n1-standard-4"),
				integration.WithNodePoolSize(1),
				integration.WithNodePoolLocations(unreachableZone),
			),
			integration.DefaultNodePool(
				integration.WithNodePoolName("pool-b"),
				integration.WithNodePoolMachineType("n1-standard-4"),
				integration.WithNodePoolSize(1),
				integration.WithNodePoolLocations(healthyZone),
			),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if err != nil {
			t.Fatalf("SetupAutoscaler failed: %v", err)
		}

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		infra.Fakes.GceService.SetZoneError(unreachableZone, readQuotaExceededError())

		infra.Fakes.GceService.ResetCallCounts()
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Second)

		assert.Equal(t, 1, infra.Fakes.GceService.FetchAllMigsCalls(unreachableZone), "failing zone should only list MIGs once per loop")
		assert.Equal(t, 1, infra.Fakes.GceService.FetchAllMigsCalls(healthyZone), "healthy zone should not be re-listed when failing zone misses cache")
		for _, mig := range infra.GkeCache.GetMigs() {
			assert.Equal(t, 0, infra.Fakes.GceService.FetchMigCalls(mig.GceRef()), "MIG %s should not be fetched individually: failing zone is out of quota, healthy zone is served from the listing cache", mig.GceRef())
		}
	})
}
