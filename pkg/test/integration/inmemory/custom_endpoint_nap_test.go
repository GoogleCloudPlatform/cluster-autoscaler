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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// customGceEndpoint is a non-standard compute endpoint. Its host
// (compute.example.com) differs from the public www.googleapis.com host. The
// async NAP path synthesizes "upcoming" node names by embedding the MIG
// self-link, which is derived from this endpoint.
const customGceEndpoint = "https://compute.example.com/compute/v1/"

// TestCustomEndpointNapScaleUp is a Big Unit Test for b/515414613.
//
// It verifies end-to-end that asynchronous NAP scale-up on a custom compute
// endpoint successfully provisions a node pool and schedules the pending pod.
//
// Scenario:
//   - The cluster's compute endpoint is a custom endpoint.
//   - High-Throughput (async) NAP is enabled with no pre-existing node pools.
//     Only the async path synthesizes "upcoming" node names embedding the MIG
//     self-link (async_gke_manager.go); the production node name looked like
//     "...-async-0-...-upcoming-0".
//   - A single unschedulable CPU pod forces NAP to asynchronously create a
//     brand new node pool.
//
// What this guards: the async NAP scale-up must ultimately provision the node
// pool and schedule the pod on a custom endpoint. On the custom endpoint,
// there is exactly one loop where the async "upcoming" node is present in the
// snapshot while the ScaleUp quotas tracker is built. Before the fix in
// instanceRefFromUpcomingNodeName, that parse rejected the custom host and
// the loop logged "could not create quotas tracker: ... wrong upcoming node
// name". That error is transient (the async node-pool creation completes
// independently of ScaleUp, so the node registers regardless), so it leaves no
// durable state difference on its own; the precise regression guard for the
// parser lives in the unit test.
func TestCustomEndpointNapScaleUp(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			integration.WithHighThroughputNAPEnabled(10 /*maxParallelOps*/, 100 /*maxQueuedOps*/),
			integration.WithGceEndpoint(customGceEndpoint),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		pod := buildCpuPod("custom-endpoint-cpu-pod", "us-central1-a")
		addPods(infra, pod)

		// Drive several autoscaler loops so the asynchronous NAP node-pool
		// creation finalizes across loops. We advance the virtual clock by one
		// second per loop.
		for i := 0; i < 5; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		}

		// The autoscaler must recover and provision: exactly one NAP node pool
		// exists and at least one real node registered.
		cluster := integration.GetTestCluster(t, infra, testConfig)
		assert.Equal(t, 1, len(cluster.NodePools), "expected exactly one NAP-created node pool")

		nodes, err := infra.Fakes.K8s.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(nodes.Items), 1, "expected NAP to register at least one real node")

		infra.Fakes.RunScheduler(ctx, t)
		assertScheduledPodsCount(t, ctx, infra, []*apiv1.Pod{pod}, 1)
	})
}
