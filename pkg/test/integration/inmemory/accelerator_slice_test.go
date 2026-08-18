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
	"testing"
	"testing/synctest"
	"time"

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/reservations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	ccc_builder "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	podOpts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	// TPU accelerator slice constants for a 4x4x4 tpu7x topology.
	tpuAccelerator  = "tpu7x"
	tpuTopology     = "4x4x4"
	tpuChipsPerNode = "4"
	tpuMachineType  = "tpu7x-standard-4t"
	tpuPolicyName   = "test-policy"
	tpuCCCName      = "tpu-dynamic-slicing"
	// 4x4x4 topology with 4 chips per node = 64 / 4 = 16 nodes per slice.
	tpuNodesPerSlice = 16
)

// TestAcceleratorSlice_ScaleUp exercises the NAP-driven creation of multi-host
// TPU accelerator slices via the ComputeClass (CCC) path together with the
// GCE-side capacity feedback loop, and verifies the incremental "zero-or-max"
// semantics introduced for PROVISION_ONLY workload policies (dynamic slicing):
//   - AUTO_CONNECT uses atomic ResizeRequest for the full logical slice
//     with all-or-nothing behavior.
//   - PROVISION_ONLY grows the slice incrementally via CreateInstances, so
//     partial capacity is accepted during stockout, and the slice completes
//     once capacity is restored.
//   - Empty AcceleratorTopologyMode behaves like AUTO_CONNECT.
//
// The test is accelerator-agnostic: it uses tpu7x as the concrete machine
// family, but the PROVISION_ONLY logic is driven by the workload policy's
// AcceleratorTopologyMode, not the accelerator type. Any multi-host TPU
// (v6e, v7, v7x, etc.) with PROVISION_ONLY follows the same path.
func TestAcceleratorSlice_ScaleUp(t *testing.T) {
	testCases := []struct {
		name                                string
		acceleratorTopologyMode             string
		backendCapacity                     int64
		wantInitialTargetSize               int
		wantTargetSizeAfterCapacityRestored int
	}{
		{
			name:                                "EmptyMode_FullCapacity_SliceAtomic",
			backendCapacity:                     tpuNodesPerSlice,
			wantInitialTargetSize:               tpuNodesPerSlice,
			wantTargetSizeAfterCapacityRestored: tpuNodesPerSlice,
		},
		{
			name:                                "EmptyMode_Stockout_AllOrNothing",
			backendCapacity:                     8,
			wantInitialTargetSize:               0,
			wantTargetSizeAfterCapacityRestored: 0,
		},
		{
			name:                                "AutoConnect_FullCapacity_SliceAtomic",
			acceleratorTopologyMode:             "AUTO_CONNECT",
			backendCapacity:                     tpuNodesPerSlice,
			wantInitialTargetSize:               tpuNodesPerSlice,
			wantTargetSizeAfterCapacityRestored: tpuNodesPerSlice,
		},
		{
			name:                                "AutoConnect_Stockout_AllOrNothing",
			acceleratorTopologyMode:             "AUTO_CONNECT",
			backendCapacity:                     8,
			wantInitialTargetSize:               0,
			wantTargetSizeAfterCapacityRestored: 0,
		},
		{
			name:                                "ProvisionOnly_FullCapacity_SliceAtomic",
			acceleratorTopologyMode:             gceclient.AcceleratorTopologyModeProvisionOnly,
			backendCapacity:                     tpuNodesPerSlice,
			wantInitialTargetSize:               tpuNodesPerSlice,
			wantTargetSizeAfterCapacityRestored: tpuNodesPerSlice,
		},
		{
			name:                                "ProvisionOnly_Stockout_NonAtomic",
			acceleratorTopologyMode:             gceclient.AcceleratorTopologyModeProvisionOnly,
			backendCapacity:                     8,
			wantInitialTargetSize:               8,
			wantTargetSizeAfterCapacityRestored: tpuNodesPerSlice,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := acceleratorSliceTestConfig(tc.acceleratorTopologyMode)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				defer integration_synctest.TearDown(cancel)

				infra := integration.SetupInfrastructure(ctx, t)
				infra.Fakes.GceService.WithResourcePolicies(newWorkloadPolicy(tc.acceleratorTopologyMode))

				autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

				// Constrain backend capacity per zone.
				for _, zone := range []string{"us-central1-a", "us-central1-b", "us-central1-c"} {
					infra.Fakes.GceService.SetBackendMachineCount(zone, tpuMachineType, tc.backendCapacity)
				}

				addTPUPods(infra, tpuNodesPerSlice)

				// Step 1: initial scale-up.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				assert.Equal(t, tc.wantInitialTargetSize, firstTargetSize(t, autoscaler),
					"unexpected initial MIG target size")

				// Step 2: if the stockout path partially provisioned the slice, restore full
				// capacity and verify the autoscaler completes the slice.
				if tc.wantTargetSizeAfterCapacityRestored != tc.wantInitialTargetSize {
					infra.Fakes.GceService.ResetHardwareCapacity()
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 10*time.Minute)

					assert.Equal(t, tc.wantTargetSizeAfterCapacityRestored, firstTargetSize(t, autoscaler),
						"unexpected MIG target size after capacity restoration")
				}
			})
		})
	}
}

// TestAcceleratorSlice_ScaleUp_WithReservation verifies that PROVISION_ONLY
// dynamic slicing scale-up works correctly when backed by a GCE reservation
// with block/subblock structure. The reservation provides capacity for the
// TPU slice, and the autoscaler should provision incrementally against it.
func TestAcceleratorSlice_ScaleUp_WithReservation(t *testing.T) {
	const reservationZone = "us-central1-b"

	testConfig := acceleratorSliceTestConfig(gceclient.AcceleratorTopologyModeProvisionOnly).
		AddReservation(
			integration.DefaultProject(),
			reservations.New(
				"tpu7x-reservation", reservationZone,
				reservations.WithMachine(tpuMachineType),
				reservations.WithProject(integration.DefaultProject()),
				reservations.WithCounts(0, tpuNodesPerSlice),
			),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		infra.Fakes.GceService.WithResourcePolicies(newWorkloadPolicy(gceclient.AcceleratorTopologyModeProvisionOnly))

		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		// Backend capacity matches reservation count — full capacity.
		for _, zone := range []string{"us-central1-a", "us-central1-b", "us-central1-c"} {
			infra.Fakes.GceService.SetBackendMachineCount(zone, tpuMachineType, tpuNodesPerSlice)
		}

		addTPUPods(infra, tpuNodesPerSlice)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		assert.Equal(t, tpuNodesPerSlice, firstTargetSize(t, autoscaler),
			"PROVISION_ONLY with reservation should scale up to full slice")
	})
}

// TestAcceleratorSlice_ScaleUp_MinCapacity verifies that the ComputeClass
// MinimumCapacity (TargetNodeCount) path triggers a proactive scale-up for
// a PROVISION_ONLY dynamic slicing TPU, even without pending pods.
func TestAcceleratorSlice_ScaleUp_MinCapacity(t *testing.T) {
	cc := ccc_builder.NewComputeClassBuilder(tpuCCCName).
		WithNapEnabled().
		WithTargetNodeCount(ptr.To(tpuNodesPerSlice)).
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(cccv1.Priority{
			MachineFamily: ptr.To("tpu7x"),
			Tpu: &cccv1.TPU{
				Type:     tpuAccelerator,
				Count:    4,
				Topology: tpuTopology,
			},
		}).
		Build()

	testConfig := integration.NewTestConfig().
		WithExperiments(experiments.ResourcePolicyPullerFlag).
		WithOverrides(
			integration.WithScaleDownUnneededTime(1*time.Second),
			integration.WithAutoProvisioningEnabled(),
			integration.WithCompactPlacementEnabled(true),
			integration.WithComputeClassMinCapacityEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: integration.DefaultMaxCoresResourceLimit},
				{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
				{ResourceType: tpuAccelerator, Maximum: 1000000},
			}),
		).
		WithCccCrds(cc)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		infra.Fakes.GceService.WithResourcePolicies(newWorkloadPolicy(gceclient.AcceleratorTopologyModeProvisionOnly))

		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		// No pods — the MinimumCapacity should drive scale-up on its own.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Verify nodes were created to satisfy TargetNodeCount.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, tpuNodesPerSlice, len(nodes.Items),
			"expected nodes to be created to satisfy TargetNodeCount minimum capacity")
	})
}

// --- helpers ---

// acceleratorSliceTestConfig returns the shared test configuration for
// accelerator slice BUTs: CCC with TPU priority, NAP enabled, compact
// placement enabled, resource policy puller experiment on.
func acceleratorSliceTestConfig(acceleratorTopologyMode string) *integration.TestConfig {
	return integration.NewTestConfig().
		WithExperiments(experiments.ResourcePolicyPullerFlag).
		WithOverrides(
			integration.WithScaleDownUnneededTime(1*time.Second),
			integration.WithAutoProvisioningEnabled(),
			// Dynamic-slicing paths interact with placement groups; enable
			// compact placement so the slice-provisioning code-path is
			// exercised in the integration framework.
			integration.WithCompactPlacementEnabled(true),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: integration.DefaultMaxCoresResourceLimit},
				{ResourceType: "memory", Maximum: integration.DefaultMaxMemoryResourceLimit},
				{ResourceType: tpuAccelerator, Maximum: 1000000},
			}),
		).
		WithCccCrds(newTPUComputeClass(acceleratorTopologyMode))
}

// newTPUComputeClass creates a ComputeClass CRD for tpu7x dynamic slicing
// with the specified AcceleratorTopologyMode (used as the WhenUnsatisfiable
// hint to inform the CCC about the provisioning behavior).
func newTPUComputeClass(acceleratorTopologyMode string) *cccv1.ComputeClass {
	return ccc_builder.NewComputeClassBuilder(tpuCCCName).
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(cccv1.Priority{
			MachineFamily: ptr.To("tpu7x"),
			Tpu: &cccv1.TPU{
				Type:     tpuAccelerator,
				Count:    4,
				Topology: tpuTopology,
			},
			// The placement (workload) policy is defined by the ComputeClass
			// (BYOPP), not by a node selector on the pod. The CCC translation
			// turns this into a placement-policy rule for the provisioned slice.
			Placement: &cccv1.Placement{PolicyName: tpuPolicyName},
		}).
		Build()
}

// newWorkloadPolicy creates a GCE resource policy with the given
// AcceleratorTopologyMode for the standard test topology/policy name.
func newWorkloadPolicy(acceleratorTopologyMode string) *gceclient.GceResourcePolicy {
	return &gceclient.GceResourcePolicy{
		Name:   tpuPolicyName,
		Status: "READY",
		WorkloadPolicy: gceclient.WorkloadPolicy{
			AcceleratorTopology:     tpuTopology,
			AcceleratorTopologyMode: acceleratorTopologyMode,
		},
	}
}

// addTPUPods adds count unschedulable pods that drive NAP creation of a TPU
// slice through the ComputeClass path.
//
// The placement (workload) policy is defined by the ComputeClass (see
// newTPUComputeClass), so the pod does not carry a placement-policy node
// selector. The pod still sets the TPU (type/topology/count) selectors because
// the in-memory NAP framework sizes the slice from the pod's requirements; in
// production those are also derived from the ComputeClass.
func addTPUPods(infra *integration.TestInfrastructure, count int) {
	for i := 0; i < count; i++ {
		pod := tu.BuildTestPod(
			fmt.Sprintf("workload-pod-%d", i), 1000, 1000,
			tu.MarkUnschedulable(),
			// WithCCC sets the compute-class selector (replacing NodeSelector);
			// WithTPU then appends the TPU labels/resources.
			podOpts.WithCCC(tpuCCCName),
			podOpts.WithTPU(tpuAccelerator, tpuTopology, tpuChipsPerNode),
		)
		infra.Fakes.K8s.AddPod(pod)
	}
}

// firstTargetSize returns the TargetSize of the first autoscaled node group
// with non-null desired capacity (i.e. the "active" slice the autoscaler is
// currently growing). Returns 0 if no such node group exists.
func firstTargetSize(t *testing.T, autoscaler *core.StaticAutoscaler) int {
	t.Helper()
	for _, nodeGroup := range autoscaler.AutoscalingContext.CloudProvider.NodeGroups(context.Background()) {
		targetSize, err := nodeGroup.TargetSize(context.Background())
		if !assert.NoError(t, err) {
			continue
		}
		if targetSize > 0 {
			return targetSize
		}
	}
	return 0
}
