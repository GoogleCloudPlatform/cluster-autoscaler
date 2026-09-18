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

package dualca

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/status"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestDualCA_ComputeClassHandover checks that a CA instance keeps working when
// a ComputeClass it has been scaling up is handed over to the other instance
// while the CA instance still has state for it: an active FlexAdvisor scope,
// refreshed every 10s, and a pending status update, flushed every 30s. Neither
// may crash the CA instance once the ComputeClass is gone from its lister, and
// the CA instance must neither write the status of the
// ComputeClass, which is up to the other instance now, nor scale up for it. It
// must still scale up for the workloads it owns, here pods without a
// ComputeClass.
func TestDualCA_ComputeClassHandover(t *testing.T) {
	stdPool := newComputeClassPool(stdPoolName, stdComputeClass, 0, "us-central1-a")
	solverPool := newComputeClassPool(solverPoolName, solverComputeClass, 0, "us-central1-a")
	unannotatedPool := integration.EmptyNodePool("unannotated-pool").WithSize(0).WithMin(0).WithLocations("us-central1-a").Build()
	testConfig := newDualCATestConfig(standardCANames, stdPool, solverPool).
		WithNodePools(unannotatedPool).
		WithOverrides(
			integration.WithFlexAdvisorEnabled(),
			integration.WithEnhancedCrdStatusReportingEnabled(),
		)

	// Status updates are flushed and FlexAdvisor scopes refreshed in background
	// goroutines on their own timers, not in the autoscaler loop, so the test
	// has to wait for a flush, and for a scope refresh, which is due more
	// often, to verify they don't panic.
	flushWait := 2 * status.BatchFlushInterval

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		// Given: the standard CA scaled up for a pod of its ComputeClass, which
		// registered a FlexAdvisor scope, and wrote the status of the
		// ComputeClass.
		infra.Fakes.K8s.AddPod(pendingPod("std-pod-1", stdComputeClass))
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, flushWait)
		require.NotEmpty(t, mustGetComputeClassStatus(ctx, t, infra, stdComputeClass).Conditions, "ComputeClass status conditions written by the owner")

		// Given: the standard CA scaled up for another pod of the ComputeClass,
		// which queued another status update, and the scheduler scheduled both
		// pods, so std-pool has no room left for other pods.
		infra.Fakes.K8s.AddPod(pendingPod("std-pod-2", stdComputeClass))
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		require.Equal(t, int64(2), infra.Fakes.GkeService.MustGetTargetSize(t, stdPool), "std-pool size before the handover")
		infra.Fakes.RunScheduler(ctx, t)
		statusBeforeHandover := mustGetComputeClassStatus(ctx, t, infra, stdComputeClass)

		// When: the ComputeClass is handed over to the solver CA, another pod
		// of the ComputeClass and a pod without a ComputeClass are pending,
		// and the standard CA runs again once a status update flush and a
		// FlexAdvisor scope refresh are due.
		handOverComputeClass(ctx, t, infra, stdComputeClass, options.SolverAutoscalerName)
		infra.Fakes.K8s.AddPod(pendingPod("std-pod-3", stdComputeClass))
		infra.Fakes.K8s.AddPod(pendingPod("unannotated-pod-1", ""))
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, flushWait)

		// Then: the standard CA didn't crash (a panic in any of its goroutines
		// fails the test), neither wrote the status of the ComputeClass nor
		// scaled up for it, and still scaled up the unannotated pool for the
		// pod without a ComputeClass.
		assert.Equal(t, statusBeforeHandover, mustGetComputeClassStatus(ctx, t, infra, stdComputeClass), "ComputeClass status after the handover")
		assert.Equal(t, int64(2), infra.Fakes.GkeService.MustGetTargetSize(t, stdPool), "std-pool size after the handover")
		assert.Equal(t, int64(1), infra.Fakes.GkeService.MustGetTargetSize(t, unannotatedPool), "unannotated-pool size after the handover")
	})
}
