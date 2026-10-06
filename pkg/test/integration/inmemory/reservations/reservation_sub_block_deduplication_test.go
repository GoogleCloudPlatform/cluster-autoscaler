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
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	ccc_builder "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	cccName          = "tpu-ccc"
	policyName       = "workload-policy"
	existingPoolName = "existing-pool-A"
	tpuType          = "tpu7x"
	topology         = "4x4x4"
	project          = "test-project"
	resName          = "test-res"
	blockName        = "test-block"
	zone             = "us-central1-b"
)

// TestReservationSubBlockDeduplication verifies that NAP does not create a dynamic slicing
// (PROVISION_ONLY) node pool on a reservation sub-block already targeted by another node pool, and
// that static slices (AUTO_CONNECT or no workload policy) are exempt because GCE lets them share a
// sub-block.
//
// The cluster starts with a node pool pinned to sub-block A. A ComputeClass lists priorities
// that only differ by the sub-block they pin, so the outcome is deterministic.
func TestReservationSubBlockDeduplication(t *testing.T) {
	testCases := []struct {
		name string
		// dedupEnabled toggles the --enable-reservation-subblock-deduplication flag.
		dedupEnabled bool
		// topologyMode is the AcceleratorTopologyMode of the workload policy attached to the
		// ComputeClass priorities. Empty means no workload policy at all (plain static slice).
		topologyMode string
		// prioritySubBlocks lists the sub-blocks in the ComputeClass priorities, in order.
		prioritySubBlocks []string
		// expectedSubBlock is the sub-block the newly created NAP node pool is expected to target,
		// or "" when no new NAP node pool should be created.
		expectedSubBlock string
	}{
		{
			name:              "dynamic slicing candidate skips the taken sub-block",
			dedupEnabled:      true,
			topologyMode:      gceclient.AcceleratorTopologyModeProvisionOnly,
			prioritySubBlocks: []string{"sub-block-A", "sub-block-B"},
			expectedSubBlock:  "sub-block-B",
		},
		{
			name:              "dynamic slicing candidate with all sub-blocks taken creates no node pool",
			dedupEnabled:      true,
			topologyMode:      gceclient.AcceleratorTopologyModeProvisionOnly,
			prioritySubBlocks: []string{"sub-block-A"},
			expectedSubBlock:  "",
		},
		{
			name:              "dynamic slicing candidate lands on the taken sub-block when dedup is disabled",
			dedupEnabled:      false,
			topologyMode:      gceclient.AcceleratorTopologyModeProvisionOnly,
			prioritySubBlocks: []string{"sub-block-A", "sub-block-B"},
			expectedSubBlock:  "sub-block-A",
		},
		{
			name:              "static slice with AUTO_CONNECT policy may share the sub-block",
			dedupEnabled:      true,
			topologyMode:      "AUTO_CONNECT",
			prioritySubBlocks: []string{"sub-block-A", "sub-block-B"},
			expectedSubBlock:  "sub-block-A",
		},
		{
			name:              "static slice without workload policy may share the sub-block",
			dedupEnabled:      true,
			topologyMode:      "",
			prioritySubBlocks: []string{"sub-block-A", "sub-block-B"},
			expectedSubBlock:  "sub-block-A",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingPool := integration.DefaultNodePool(
				integration.WithNodePoolName(existingPoolName),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations(zone),
				integration.WithTPUConfig(fmt.Sprintf("%s-standard-4t", tpuType), tpuType, topology, "4", 16),
			)
			existingPool.Config.ReservationAffinity = &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Key:                    "compute.googleapis.com/reservation-name",
				Values: []string{
					fmt.Sprintf("projects/%s/reservations/%s/reservationBlocks/%s/reservationSubBlocks/sub-block-A", project, resName, blockName),
				},
			}

			var priorities []v1.Priority
			for _, sb := range tc.prioritySubBlocks {
				priorities = append(priorities, subBlockPriority(sb, tc.topologyMode != ""))
			}
			cc := ccc_builder.NewComputeClassBuilder(cccName).
				WithNodePoolAutoCreation(true).
				WithPriorities(priorities...).
				Build()

			testConfig := integration.NewTestConfig().
				WithExperiments(experiments.ResourcePolicyPullerFlag).
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithCompactPlacementEnabled(true),
					integration.WithReservationSubBlockDeduplicationEnabled(tc.dedupEnabled),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
						{ResourceType: "cpu", Maximum: 10000},
						{ResourceType: "memory", Maximum: 10000000},
						{ResourceType: tpuType, Maximum: 1000},
					}),
				).
				WithNodePools(existingPool).
				WithCccCrds(cc)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				defer integration_synctest.TearDown(cancel)

				infra := integration.SetupInfrastructure(ctx, t)
				if tc.topologyMode != "" {
					infra.Fakes.GceService.WithResourcePolicies(&gceclient.GceResourcePolicy{
						Name:   policyName,
						Status: "READY",
						WorkloadPolicy: gceclient.WorkloadPolicy{
							AcceleratorTopology:     topology,
							AcceleratorTopologyMode: tc.topologyMode,
						},
					})
				}
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				p := tu.BuildTestPod("test-pod", 1000, 1000, tu.MarkUnschedulable(),
					pod.WithCCC(cccName),
					pod.WithTPU(tpuType, topology, "4"),
				)
				infra.Fakes.K8s.AddPod(p)

				for i := 0; i < 10; i++ {
					assert.NoError(t, integration_synctest.RunOnceAfter(ctx, t, autoscaler, time.Second))
				}

				cluster, err := infra.Fakes.GkeService.GetCluster(fmt.Sprintf("projects/%s/locations/us-central1/clusters/test-cluster", project))
				if err != nil {
					t.Fatalf("GetCluster() failed: %v", err)
				}
				var napPools []*gke_api_beta.NodePool
				for _, np := range cluster.NodePools {
					if np.Name != existingPoolName {
						napPools = append(napPools, np)
					}
				}
				if tc.expectedSubBlock == "" {
					assert.Empty(t, napPools, "expected no NAP node pool to be created when all sub-blocks are taken")
					return
				}
				if len(napPools) != 1 {
					t.Fatalf("expected exactly one NAP node pool, got %d", len(napPools))
				}
				napPool := napPools[0]
				if napPool.Config == nil || napPool.Config.ReservationAffinity == nil {
					t.Fatalf("NAP node pool %q has no reservation affinity", napPool.Name)
				}
				assert.Equal(t, gkeclient.ReservationAffinitySpecific, napPool.Config.ReservationAffinity.ConsumeReservationType)
				expectedPath := fmt.Sprintf("%s/reservationBlocks/%s/reservationSubBlocks/%s", resName, blockName, tc.expectedSubBlock)
				assert.Equal(t, []string{expectedPath}, napPool.Config.ReservationAffinity.Values, "NAP node pool targets the wrong sub-block")
			})
		})
	}
}

// subBlockPriority returns a tpu7x 4x4x4 ComputeClass priority pinned to the given sub-block of
// test-res/test-block, optionally attached to the workload policy.
func subBlockPriority(subBlock string, withPolicy bool) v1.Priority {
	p := v1.Priority{
		MachineFamily: ptr.To(tpuType),
		Tpu: &v1.TPU{
			Type:     tpuType,
			Count:    4,
			Topology: topology,
		},
		Reservations: &v1.Reservations{
			Affinity: v1.SpecificAffinity,
			Specific: []v1.SpecificReservation{{
				Name:    resName,
				Project: project,
				Zones:   []string{zone},
				ReservationBlock: &v1.ReservationBlock{
					Name: blockName,
					ReservationSubBlock: &v1.ReservationSubBlock{
						Name: subBlock,
					},
				},
			}},
		},
	}
	if withPolicy {
		p.Placement = &v1.Placement{PolicyName: policyName}
	}
	return p
}
