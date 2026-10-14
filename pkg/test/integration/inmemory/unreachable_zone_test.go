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
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	fakegce "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	unreachableZone = "us-central1-a"
	healthyZone     = "us-central1-b"

	// Loop deltas long enough to trigger a cluster refresh / a blocked MIG list refresh.
	unreachableZonePartialRefresh = gke.ClusterRefreshInterval + time.Second
	unreachableZoneFullRefresh    = gke.BlockListRefreshIntervalTime + time.Second
)

// Every loop fails each MIG of an unreachable zone at least once, so this function runs it
// required amount of times to block the whole zone
func runZoneBlockingLoops(ctx context.Context, t *testing.T, autoscaler core.Autoscaler) {
	t.Helper()
	for i := 0; i < gke.MaxIrretrievableErrsBeforeBlocked; i++ {
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unreachableZonePartialRefresh)
	}
}

func gceCallsInNextLoop(ctx context.Context, t *testing.T, autoscaler core.Autoscaler, infra *integration.TestInfrastructure) (unreachable, healthy int) {
	t.Helper()
	infra.Fakes.GceService.ResetCallCounts()
	integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unreachableZonePartialRefresh)
	return infra.Fakes.GceService.TotalCallsForZone(unreachableZone), infra.Fakes.GceService.TotalCallsForZone(healthyZone)
}

func nodesInZone(infra *integration.TestInfrastructure, zone string) int {
	count := 0
	for _, node := range infra.Fakes.K8s.Nodes().Items {
		if strings.Contains(node.Spec.ProviderID, "/"+zone+"/") {
			count++
		}
	}
	return count
}

func autoprovisionedNodePoolNames(infra *integration.TestInfrastructure) []string {
	var names []string
	for _, np := range infra.Fakes.GkeService.GetAutoprovisionedNodePools() {
		names = append(names, np.Name)
	}
	return names
}

func TestUnreachableZoneStopsGceTraffic(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(
			integration.DefaultNodePool(
				integration.WithNodePoolName("pool-a"),
				integration.WithNodePoolMachineType("n1-standard-4"),
				integration.WithNodePoolSize(2),
				integration.WithNodePoolLocations(unreachableZone),
			),
			integration.DefaultNodePool(
				integration.WithNodePoolName("pool-b"),
				integration.WithNodePoolMachineType("n1-standard-4"),
				integration.WithNodePoolSize(2),
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

		unhealthyCallsBefore, healthyCallsBefore := gceCallsInNextLoop(ctx, t, autoscaler, infra)
		assert.Greater(t, healthyCallsBefore, 0)
		assert.Equal(t, healthyCallsBefore, unhealthyCallsBefore, "both zones should see the same traffic while healthy")

		infra.Fakes.GceService.SetZoneError(unreachableZone, nil)
		runZoneBlockingLoops(ctx, t, autoscaler)

		unhealthyNodesAfter, healthyCallsAfter := gceCallsInNextLoop(ctx, t, autoscaler, infra)
		assert.Equal(t, 0, unhealthyNodesAfter, "unreachable zone should get no traffic")
		assert.Equal(t, healthyCallsBefore, healthyCallsAfter, "healthy zone traffic should be unchanged")
	})
}

func TestUnreachableZoneDoesNotAffectHealthyZoneScaleUp(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(
			integration.DefaultNodePool(
				integration.WithNodePoolName("pool-a"),
				integration.WithNodePoolMachineType("n1-standard-4"),
				integration.WithNodePoolSize(2),
				integration.WithNodePoolLocations(unreachableZone),
			),
			integration.DefaultNodePool(
				integration.WithNodePoolName("pool-b"),
				integration.WithNodePoolMachineType("n1-standard-4"),
				integration.WithNodePoolSize(2),
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

		infra.Fakes.GceService.SetZoneError(unreachableZone, nil)
		runZoneBlockingLoops(ctx, t, autoscaler)

		// n1-standard-4 has 4000m: fill every node so the pending pods needs a new node
		for i, node := range infra.Fakes.K8s.Nodes().Items {
			fillerPod := tu.BuildTestPod(fmt.Sprintf("filler-pod-%d", i), 3500, 0)
			fillerPod.Spec.NodeName = node.Name
			infra.Fakes.K8s.AddPod(fillerPod)
		}
		for i := 0; i < 50; i++ {
			podName := fmt.Sprintf("unschedulable-pod-%d", i)
			pod := tu.BuildTestPod(podName, 3000, 0, tu.MarkUnschedulable())
			infra.Fakes.K8s.AddPod(pod)
		}

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unreachableZonePartialRefresh)

		assert.Equal(t, 52, nodesInZone(infra, healthyZone), "healthy zone should scale up")
		assert.Equal(t, 2, nodesInZone(infra, unreachableZone), "unreachable zone should be left alone")
	})
}

func TestUnreachableZoneGceTrafficResumesAfterRecovery(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(
			integration.DefaultNodePool(
				integration.WithNodePoolName("pool-a"),
				integration.WithNodePoolMachineType("n1-standard-4"),
				integration.WithNodePoolSize(2),
				integration.WithNodePoolLocations(unreachableZone),
			),
			integration.DefaultNodePool(
				integration.WithNodePoolName("pool-b"),
				integration.WithNodePoolMachineType("n1-standard-4"),
				integration.WithNodePoolSize(2),
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

		infra.Fakes.GceService.SetZoneError(unreachableZone, nil)
		runZoneBlockingLoops(ctx, t, autoscaler)
		callsWhileDown, _ := gceCallsInNextLoop(ctx, t, autoscaler, infra)
		assert.Equal(t, 0, callsWhileDown, "unreachable zone should be blocked")

		infra.Fakes.GceService.ClearZoneError(unreachableZone)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unreachableZoneFullRefresh)

		callsAfter, healthyCallsAfter := gceCallsInNextLoop(ctx, t, autoscaler, infra)
		assert.Greater(t, callsAfter, 0, "traffic to the recovered zone should resume")
		assert.Equal(t, healthyCallsAfter, callsAfter, "both zones should see the same traffic after recovery")
	})
}

func TestNapKeepsNodePoolsInUnreachableZone(t *testing.T) {
	const napPool = "nap-empty-a"
	emptyNapPool := integration.DefaultNodePool(
		integration.WithNodePoolName(napPool),
		integration.WithNodePoolMachineType("n1-standard-4"),
		integration.WithNodePoolSize(0),
		integration.WithNodePoolLocations(unreachableZone),
	)
	emptyNapPool.Autoscaling.Autoprovisioned = true

	testConfig := integration.NewTestConfig().
		WithNodePools(emptyNapPool).
		WithOverrides(integration.WithAutoProvisioningEnabled()).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations(unreachableZone),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if err != nil {
			t.Fatalf("SetupAutoscaler failed: %v", err)
		}

		// Zone is down before the first loop, so NAP never sees the empty pool while healthy.
		infra.Fakes.GceService.SetZoneError(unreachableZone, nil)
		runZoneBlockingLoops(ctx, t, autoscaler)
		assert.ElementsMatch(t, []string{napPool}, autoprovisionedNodePoolNames(infra), "NAP must not delete node pools in an unreachable zone")

		infra.Fakes.GceService.ClearZoneError(unreachableZone)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unreachableZoneFullRefresh)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unreachableZonePartialRefresh)
		assert.Empty(t, autoprovisionedNodePoolNames(infra), "NAP should remove the empty pool once its zone is reachable")
	})
}

func TestUnreachableZoneBlockingPerGceError(t *testing.T) {
	urlErr := func(err error) error {
		return &url.Error{Op: "Get", URL: "https://compute.googleapis.com/compute/v1/projects/test-project/zones/" + unreachableZone + "/instanceGroupManagers", Err: err}
	}
	tests := map[string]struct {
		err         error
		wantBlocked bool
	}{
		"TimeoutInURLError": {
			err:         fakegce.UnreachableError(unreachableZone),
			wantBlocked: true,
		},
		"ConnectionRefused": {
			err:         urlErr(&net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}),
			wantBlocked: true,
		},
		"ConnectionReset": {
			err:         urlErr(&net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}),
			wantBlocked: true,
		},
		"DNSNoSuchHost": {
			err:         urlErr(&net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "compute.googleapis.com", IsNotFound: true}}),
			wantBlocked: true,
		},
		"HTTP2GoAway": {
			err:         urlErr(errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR, debug=\"\"")),
			wantBlocked: true,
		},
		"ContextDeadlineExceeded": {
			err:         context.DeadlineExceeded,
			wantBlocked: true,
		},
		"ServerError503": {
			err:         &googleapi.Error{Code: http.StatusServiceUnavailable, Message: "Service Unavailable"},
			wantBlocked: true,
		},
		"NotFound404": {
			err:         &googleapi.Error{Code: http.StatusNotFound, Message: "Not Found"},
			wantBlocked: true,
		},
		"RateLimited429": {
			err:         &googleapi.Error{Code: http.StatusTooManyRequests, Message: "Rate Limit Exceeded"},
			wantBlocked: false,
		},
		"BadRequest400": {
			err:         &googleapi.Error{Code: http.StatusBadRequest, Message: "Bad Request"},
			wantBlocked: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// The loop itself errors when no node group is reachable, hence the healthy pool.
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

				infra.Fakes.GceService.SetZoneError(unreachableZone, tc.err)
				runZoneBlockingLoops(ctx, t, autoscaler)

				calls, healthyCalls := gceCallsInNextLoop(ctx, t, autoscaler, infra)
				assert.Greater(t, healthyCalls, 0)
				if tc.wantBlocked {
					assert.Equal(t, 0, calls, "failing zone should be blocked")
				} else {
					assert.Greater(t, calls, 0, "CA should keep retrying the failing zone")
				}
			})
		})
	}
}
