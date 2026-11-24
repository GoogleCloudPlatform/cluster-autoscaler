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

	"github.com/google/go-cmp/cmp"
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
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
	// topology is a tpu7x 4x4x4 cube: 16 hosts, as many as one tpu7x sub-block.
	topology = "4x4x4"
	// smallTopology is a 2x2x4 tpu7x slice: 4 hosts, a quarter of a sub-block.
	smallTopology = "2x2x4"
	project       = "test-project"
	resName       = "test-res"
	blockName     = "test-block"
	zone          = "us-central1-b"

	a4xMaxMachineType = "a4x-maxgpu-4g-metal"
	gb300GPUType      = "nvidia-gb300"
	a4xGPUsPerNode    = 4
	a4xNVL72Topology  = "1x72"
	// a4xNVL72Nodes is the number of A4X nodes in a 1x72 slice: 72 GPUs / 4 GPUs per node.
	a4xNVL72Nodes = 18
)

// TestReservationSubBlockDeduplication verifies that NAP does not create a dynamic slicing
// (PROVISION_ONLY) node pool, or a static slice as large as a sub-block, on a reservation sub-block
// already targeted by another node pool, and that smaller static TPU slices (AUTO_CONNECT or no
// workload policy) are exempt because GCE lets them share a sub-block.
//
// The cluster starts with a node pool pinned to sub-block A. A ComputeClass lists priorities
// that only differ by the sub-block they pin, so the outcome is deterministic.
func TestReservationSubBlockDeduplication(t *testing.T) {
	testCases := []struct {
		name string
		// dedupEnabled toggles the --enable-reservation-subblock-deduplication flag.
		dedupEnabled bool
		// tpuTopology is the TPU topology of the slice requested by the pod and the priorities.
		tpuTopology string
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
			name:              "dynamic_slicing_skips_taken_sub_block",
			dedupEnabled:      true,
			tpuTopology:       topology,
			topologyMode:      gceclient.AcceleratorTopologyModeProvisionOnly,
			prioritySubBlocks: []string{"sub-block-A", "sub-block-B"},
			expectedSubBlock:  "sub-block-B",
		},
		{
			name:              "dynamic_slicing_all_sub_blocks_taken_creates_no_node_pool",
			dedupEnabled:      true,
			tpuTopology:       topology,
			topologyMode:      gceclient.AcceleratorTopologyModeProvisionOnly,
			prioritySubBlocks: []string{"sub-block-A"},
			expectedSubBlock:  "",
		},
		{
			name:              "dynamic_slicing_dedup_disabled_lands_on_taken_sub_block",
			dedupEnabled:      false,
			tpuTopology:       topology,
			topologyMode:      gceclient.AcceleratorTopologyModeProvisionOnly,
			prioritySubBlocks: []string{"sub-block-A", "sub-block-B"},
			expectedSubBlock:  "sub-block-A",
		},
		{
			name:              "whole_sub_block_static_slice_auto_connect_skips_taken_sub_block",
			dedupEnabled:      true,
			tpuTopology:       topology,
			topologyMode:      "AUTO_CONNECT",
			prioritySubBlocks: []string{"sub-block-A", "sub-block-B"},
			expectedSubBlock:  "sub-block-B",
		},
		{
			name:              "whole_sub_block_static_slice_without_policy_skips_taken_sub_block",
			dedupEnabled:      true,
			tpuTopology:       topology,
			topologyMode:      "",
			prioritySubBlocks: []string{"sub-block-A", "sub-block-B"},
			expectedSubBlock:  "sub-block-B",
		},
		{
			name:              "smaller_static_slice_auto_connect_shares_sub_block",
			dedupEnabled:      true,
			tpuTopology:       smallTopology,
			topologyMode:      "AUTO_CONNECT",
			prioritySubBlocks: []string{"sub-block-A", "sub-block-B"},
			expectedSubBlock:  "sub-block-A",
		},
		{
			name:              "smaller_static_slice_without_policy_shares_sub_block",
			dedupEnabled:      true,
			tpuTopology:       smallTopology,
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
				priorities = append(priorities, subBlockPriority(sb, tc.tpuTopology, tc.topologyMode != ""))
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
				WithCccCrds(cc).
				AddReservation(project, reservations.NewV1(
					resName, zone,
					reservations.WithMachine(fmt.Sprintf("%s-standard-4t", tpuType)),
					reservations.WithProject(project),
					reservations.WithAvailableCount(16),
				))

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
							AcceleratorTopology:     tc.tpuTopology,
							AcceleratorTopologyMode: tc.topologyMode,
						},
					})
				}
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				p := tu.BuildTestPod("test-pod", 1000, 1000, tu.MarkUnschedulable(),
					pod.WithCCC(cccName),
					pod.WithTPU(tpuType, tc.tpuTopology, "4"),
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

// subBlockPriority returns a tpu7x ComputeClass priority for the given TPU topology, pinned to the
// given sub-block of test-res/test-block, optionally attached to the workload policy.
func subBlockPriority(subBlock, tpuTopology string, withPolicy bool) v1.Priority {
	p := v1.Priority{
		MachineFamily: ptr.To(tpuType),
		Tpu: &v1.TPU{
			Type:     tpuType,
			Count:    4,
			Topology: tpuTopology,
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

// TestReservationSubBlockDeduplication_NVL72GPUSlice verifies that NAP does not create a 1x72 GPU
// slice node pool (e.g. A4X Max) on a reservation sub-block already targeted by another node pool,
// whatever the accelerator topology mode of the workload policy, and that 1x4 GPU slices are exempt
// because several of them fit in one sub-block.
//
// The cluster starts with a node pool pinned to sub-block A. A ComputeClass with a spec-level
// minimum capacity of one slice targets sub-block A first, so the outcome is deterministic.
func TestReservationSubBlockDeduplication_NVL72GPUSlice(t *testing.T) {
	testCases := []struct {
		name string
		// dedupEnabled toggles the --enable-reservation-subblock-deduplication flag.
		dedupEnabled bool
		// gpuTopology and topologyMode define the workload policy attached to the priorities.
		gpuTopology  string
		topologyMode string
		// targetNodeCount is the spec-level minimum capacity: the nodes of one slice of gpuTopology.
		targetNodeCount int
		// prioritySubBlocks lists, for each ComputeClass priority in order, the sub-blocks under its
		// reservations.specific[].
		prioritySubBlocks [][]string
		// wantSubBlocks lists the sub-blocks targeted by the newly created NAP node pools; empty
		// when no new NAP node pool should be created.
		wantSubBlocks []string
	}{
		{
			name:              "1x72_auto_connect_skips_taken_sub_block",
			dedupEnabled:      true,
			gpuTopology:       a4xNVL72Topology,
			topologyMode:      "AUTO_CONNECT",
			targetNodeCount:   a4xNVL72Nodes,
			prioritySubBlocks: [][]string{{"sub-block-A"}, {"sub-block-B"}},
			wantSubBlocks:     []string{"sub-block-B"},
		},
		{
			name:              "1x72_auto_connect_skips_taken_sub_block_in_same_priority",
			dedupEnabled:      true,
			gpuTopology:       a4xNVL72Topology,
			topologyMode:      "AUTO_CONNECT",
			targetNodeCount:   a4xNVL72Nodes,
			prioritySubBlocks: [][]string{{"sub-block-A", "sub-block-B"}},
			wantSubBlocks:     []string{"sub-block-B"},
		},
		{
			name:              "1x72_without_topology_mode_skips_taken_sub_block",
			dedupEnabled:      true,
			gpuTopology:       a4xNVL72Topology,
			targetNodeCount:   a4xNVL72Nodes,
			prioritySubBlocks: [][]string{{"sub-block-A"}, {"sub-block-B"}},
			wantSubBlocks:     []string{"sub-block-B"},
		},
		{
			name:              "1x72_all_sub_blocks_taken_creates_no_node_pool",
			dedupEnabled:      true,
			gpuTopology:       a4xNVL72Topology,
			topologyMode:      "AUTO_CONNECT",
			targetNodeCount:   a4xNVL72Nodes,
			prioritySubBlocks: [][]string{{"sub-block-A"}},
		},
		{
			name:              "1x72_dedup_disabled_lands_on_taken_sub_block",
			dedupEnabled:      false,
			gpuTopology:       a4xNVL72Topology,
			topologyMode:      "AUTO_CONNECT",
			targetNodeCount:   a4xNVL72Nodes,
			prioritySubBlocks: [][]string{{"sub-block-A"}, {"sub-block-B"}},
			wantSubBlocks:     []string{"sub-block-A"},
		},
		{
			name:              "1x4_shares_sub_block",
			dedupEnabled:      true,
			gpuTopology:       "1x4",
			topologyMode:      "AUTO_CONNECT",
			targetNodeCount:   1,
			prioritySubBlocks: [][]string{{"sub-block-A"}, {"sub-block-B"}},
			wantSubBlocks:     []string{"sub-block-A"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			existingPool := integration.DefaultNodePool(
				integration.WithNodePoolName(existingPoolName),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations(zone),
				integration.WithNodePoolMachineType(a4xMaxMachineType),
				integration.WithNodePoolAccelerators(&gke_api_beta.AcceleratorConfig{AcceleratorType: gb300GPUType, AcceleratorCount: a4xGPUsPerNode}),
			)
			existingPool.Config.ReservationAffinity = &gke_api_beta.ReservationAffinity{
				ConsumeReservationType: gkeclient.ReservationAffinitySpecific,
				Key:                    "compute.googleapis.com/reservation-name",
				Values: []string{
					fmt.Sprintf("projects/%s/reservations/%s/reservationBlocks/%s/reservationSubBlocks/sub-block-A", project, resName, blockName),
				},
			}
			existingPool.PlacementPolicy = &gke_api_beta.PlacementPolicy{PolicyName: policyName}

			var priorities []v1.Priority
			for _, subBlocks := range tc.prioritySubBlocks {
				priorities = append(priorities, a4xPriority(subBlocks...))
			}
			cc := ccc_builder.NewComputeClassBuilder(cccName).
				WithNodePoolAutoCreation(true).
				WithPriorities(priorities...).
				WithTargetNodeCount(ptr.To(tc.targetNodeCount)).
				Build()

			testConfig := integration.NewTestConfig().
				WithExperiments(experiments.ResourcePolicyPullerFlag).
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithCompactPlacementEnabled(true),
					integration.WithComputeClassMinCapacityEnabled(),
					integration.WithSpecificTypeReservationMatchEnabled(true),
					integration.WithReservationSubBlockDeduplicationEnabled(tc.dedupEnabled),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
						{ResourceType: "cpu", Maximum: 100000},
						{ResourceType: "memory", Maximum: 10000000},
						{ResourceType: gb300GPUType, Maximum: 1000},
					}),
				).
				WithNodePools(existingPool).
				WithCccCrds(cc).
				AddReservation(project, reservations.NewV1(
					resName, zone,
					reservations.WithMachine(a4xMaxMachineType),
					reservations.WithGuestAccelerators(reservations.BuildReservationAccelerators(gb300GPUType, a4xGPUsPerNode)),
					reservations.WithProject(project),
					reservations.WithAvailableCount(2*a4xNVL72Nodes),
				))

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				defer integration_synctest.TearDown(cancel)

				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.GceService.WithResourcePolicies(&gceclient.GceResourcePolicy{
					Name:   policyName,
					Status: "READY",
					WorkloadPolicy: gceclient.WorkloadPolicy{
						AcceleratorTopology:     tc.gpuTopology,
						AcceleratorTopologyMode: tc.topologyMode,
					},
				})
				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)
				var acceleratorTypes []*compute.AcceleratorType
				for _, z := range integration.DefaultZones {
					acceleratorTypes = append(acceleratorTypes, &compute.AcceleratorType{Name: gb300GPUType, Zone: z, MaximumCardsPerInstance: a4xGPUsPerNode})
				}
				infra.Fakes.GceService.WithAcceleratorTypes(&compute.AcceleratorTypeList{Items: acceleratorTypes})

				for i := 0; i < 10; i++ {
					if err := integration_synctest.RunOnceAfter(ctx, t, autoscaler, time.Second); err != nil {
						t.Fatalf("RunOnceAfter() failed at iteration %d: %v", i, err)
					}
				}

				cluster, err := infra.Fakes.GkeService.GetCluster(fmt.Sprintf("projects/%s/locations/us-central1/clusters/test-cluster", project))
				if err != nil {
					t.Fatalf("GetCluster() failed: %v", err)
				}
				var napSubBlocks []string
				for _, np := range cluster.NodePools {
					if np.Name == existingPoolName {
						continue
					}
					if np.Config == nil || np.Config.ReservationAffinity == nil {
						t.Fatalf("NAP node pool %q has no reservation affinity", np.Name)
					}
					napSubBlocks = append(napSubBlocks, np.Config.ReservationAffinity.Values...)
				}
				var want []string
				for _, sb := range tc.wantSubBlocks {
					want = append(want, fmt.Sprintf("%s/reservationBlocks/%s/reservationSubBlocks/%s", resName, blockName, sb))
				}
				if diff := cmp.Diff(want, napSubBlocks); diff != "" {
					t.Errorf("Sub-blocks targeted by the NAP node pools after 10 RunOnce loops (dedup=%v, topology=%q, mode=%q, priorities=%v) diff (-want +got):\n%s", tc.dedupEnabled, tc.gpuTopology, tc.topologyMode, tc.prioritySubBlocks, diff)
				}
			})
		})
	}
}

// a4xPriority returns an A4X Max ComputeClass priority attached to the workload policy and
// pinned to the given sub-blocks of test-res/test-block.
func a4xPriority(subBlocks ...string) v1.Priority {
	var specific []v1.SpecificReservation
	for _, sb := range subBlocks {
		specific = append(specific, v1.SpecificReservation{
			Name:    resName,
			Project: project,
			Zones:   []string{zone},
			ReservationBlock: &v1.ReservationBlock{
				Name:                blockName,
				ReservationSubBlock: &v1.ReservationSubBlock{Name: sb},
			},
		})
	}
	return v1.Priority{
		MachineType: ptr.To(a4xMaxMachineType),
		Gpu:         &v1.GPU{Type: gb300GPUType, Count: a4xGPUsPerNode, DriverVersion: "default"},
		Placement:   &v1.Placement{PolicyName: policyName},
		Reservations: &v1.Reservations{
			Affinity: v1.SpecificAffinity,
			Specific: specific,
		},
	}
}
