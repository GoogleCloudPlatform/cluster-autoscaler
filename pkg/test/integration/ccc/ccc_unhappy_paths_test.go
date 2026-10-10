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

package ccc_test

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestCccUnhappyPaths covers ComputeClass (CCC) and Predefined Compute Class (PCC)
// unhappy paths:
//  1. Pending pods targeting a non-existent ComputeClass CRD.
//  2. Mid-flight deletion of an active ComputeClass CRD across scale-up, scale-down,
//     and defrag/consolidation while nodes with orphaned compute-class labels remain
//     in the cluster.
//  3. Predefined Compute Classes mixed with custom ComputeClass CRDs, verifying
//     priority ordering and fallback without panicking.
//  4. ComputeClass CRDs with empty priorities or rules referencing invalid/unregistered
//     machine families/types falling back to valid rules without panicking.
func TestCccUnhappyPaths(t *testing.T) {
	t.Run("PodRequestingNonExistentComputeClassCRD", testPodRequestingNonExistentComputeClassCRD)
	t.Run("ComputeClassCRDDeletedMidFlight", testComputeClassCRDDeletedMidFlight)
	t.Run("PredefinedComputeClassesMixedWithCustomCCC", testPredefinedComputeClassesMixedWithCustomCCC)
	t.Run("EmptyPrioritiesAndInvalidMachineFamilyFallback", testEmptyPrioritiesAndInvalidMachineFamilyFallback)
}

func testPodRequestingNonExistentComputeClassCRD(t *testing.T) {
	existingCCC := ccc.NewComputeClassBuilder("existing-ccc").
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{MachineFamily: ptr.To("n2")}).
		Build()

	existingCCCPool := integration.EmptyNodePool("existing-ccc-pool").
		WithMachineType("n2-standard-2").
		WithCCCLabel("existing-ccc").
		WithCCCTaint("existing-ccc").
		WithMax(10).
		Build()

	unrelatedStandardPool := integration.EmptyNodePool("unrelated-standard-pool").
		WithMachineType("e2-standard-2").
		WithMax(10).
		Build()

	testCases := []struct {
		name string
		pod  *apiv1.Pod
	}{
		{
			name: "NodeSelectorOnly",
			pod: tu.BuildTestPod("pod-missing-ccc-selector-only", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithNodeSelector(map[string]string{
					labels.ComputeClassLabel: "non-existent-ccc",
				}),
			),
		},
		{
			name: "NodeSelectorAndToleration",
			pod: tu.BuildTestPod("pod-missing-ccc-with-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithCCC("non-existent-ccc"),
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithNodePools(existingCCCPool, unrelatedStandardPool).
				WithCccCrds(existingCCC).
				WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithCccNodeAutoprovisioningEnabled(),
					integration.WithEnhancedCrdStatusReporting(true),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)

				infra.Fakes.K8s.AddPod(tc.pod)

				// Verify RunOnce completes without panicking or returning an error.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				// Verify no unrelated node pool was scaled up and no new NAP pool was created.
				cluster := integration.GetTestCluster(t, infra, testConfig)
				assert.Len(t, cluster.NodePools, 2, "Expected no new NAP node pools to be provisioned for non-existent ComputeClass")

				nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)
				assert.Empty(t, nodes.Items, "Expected no nodes to be provisioned for non-existent ComputeClass")

				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods(tc.pod.Namespace).Get(ctx, tc.pod.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Empty(t, updatedPod.Spec.NodeName, "Expected pod requesting non-existent ComputeClass to remain unscheduled")
			})
		})
	}
}

func testComputeClassCRDDeletedMidFlight(t *testing.T) {
	const deletedCCCName = "ephemeral-ccc"

	ephemeralCCC := ccc.NewComputeClassBuilder(deletedCCCName).
		WithNapEnabled().
		WithActiveMigration(true).
		WithConfigDrift(true).
		WithMaxNodeDisruption(2).
		WithTargetNodeCount(ptr.To(2)).
		WithPriorities(
			v1.Priority{
				MachineFamily: ptr.To("n2"),
				MinimumCapacity: &v1.MinimumCapacity{
					TargetNodeCount: ptr.To(1),
				},
			},
			v1.Priority{
				MachineFamily: ptr.To("e2"),
				MinimumCapacity: &v1.MinimumCapacity{
					TargetNodeCount: ptr.To(1),
				},
			},
		).
		Build()

	npN2 := integration.DefaultNodePool(
		integration.WithNodePoolName("ng-ephemeral-n2"),
		integration.WithNodePoolMachineType("n2-standard-2"),
		integration.WithNodePoolSize(2),
		integration.WithNodePoolMin(0),
		integration.WithNodePoolMax(10),
		integration.WithNodePoolLocations("us-central1-a"),
		integration.WithNodePoolLabels(map[string]string{
			labels.ComputeClassLabel:  deletedCCCName,
			labels.MachineFamilyLabel: "n2",
		}),
		integration.WithNodePoolCCCTaint(deletedCCCName),
		integration.WithNodePoolAutoscalingEnabled(true),
	)

	npE2 := integration.DefaultNodePool(
		integration.WithNodePoolName("ng-ephemeral-e2"),
		integration.WithNodePoolMachineType("e2-standard-2"),
		integration.WithNodePoolSize(2),
		integration.WithNodePoolMin(0),
		integration.WithNodePoolMax(10),
		integration.WithNodePoolLocations("us-central1-a"),
		integration.WithNodePoolLabels(map[string]string{
			labels.ComputeClassLabel:  deletedCCCName,
			labels.MachineFamilyLabel: "e2",
		}),
		integration.WithNodePoolCCCTaint(deletedCCCName),
		integration.WithNodePoolAutoscalingEnabled(true),
	)

	testConfig := integration.NewTestConfig().
		WithCccCrds(ephemeralCCC).
		WithNodePools(npN2, npE2).
		WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			integration.WithCccNodeAutoprovisioningEnabled(),
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithComputeClassConfigHashEnabled(),
			integration.WithEnhancedCrdStatusReporting(true),
			integration.WithComputeClassScaleDownStatusEnabled(),
			integration.WithComputeClassConfigDriftReportingEnabled(),
			integration.WithScaleDownUnneededTime(time.Second),
			integration.WithScaleDownDelayAfterAdd(time.Second),
			integration.WithScaleDownUtilizationThreshold(0.5),
			integration.WithDefragEnabled("high-priority-migration,nodeconfigdrift"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		// Verify initial 4 nodes exist and carry the ComputeClass label.
		initialNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, initialNodes.Items, 4)

		var e2NodeName string
		for i := range initialNodes.Items {
			if initialNodes.Items[i].Labels[labels.MachineFamilyLabel] == "e2" {
				e2NodeName = initialNodes.Items[i].Name
				break
			}
		}
		assert.NotEmpty(t, e2NodeName)

		// Place a ReplicaSet pod on one e2 node so defrag / scale-down simulation inspects it.
		_, err = infra.Fakes.KubeClient.AppsV1().ReplicaSets("default").Create(ctx, &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: "active-rs", Namespace: "default"},
			Spec:       appsv1.ReplicaSetSpec{Replicas: ptr.To[int32](1)},
		}, metav1.CreateOptions{})
		assert.NoError(t, err)

		runningPod := tu.BuildTestPod("running-pod-on-e2", 1200, 100,
			pod.WithCCC(deletedCCCName),
			pod.WithOwnerReplicaSet("active-rs"),
		)
		runningPod.Spec.NodeName = e2NodeName
		_, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Create(ctx, runningPod, metav1.CreateOptions{})
		assert.NoError(t, err)

		// Step 1: Run initial RunOnce while the ComputeClass CRD exists.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Step 2: Delete the ComputeClass CRD mid-flight while nodes with its label still exist.
		err = infra.Fakes.CccClient.CloudV1().ComputeClasses().Delete(ctx, deletedCCCName, metav1.DeleteOptions{})
		assert.NoError(t, err)

		// Add an unschedulable pending pod requesting 3000m CPU targeting the now-deleted
		// ComputeClass CRD. Because 3000m exceeds the 2000m capacity of the existing
		// n2-standard-2 / e2-standard-2 nodes, FilterOutSchedulablePodListProcessor cannot
		// virtually pack it onto the idle nodes, ensuring scale-up is genuinely entered
		// while the 3 idle orphaned nodes remain eligible for scale-down.
		pendingPod := tu.BuildTestPod("pending-after-crd-delete", 3000, 1000,
			tu.MarkUnschedulable(),
			pod.WithCCC(deletedCCCName),
		)
		infra.Fakes.K8s.AddPod(pendingPod)

		// Step 3: Run multiple autoscaler loops across scale-up, scale-down, and defrag timelines.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Second)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 6*time.Minute)

		// Allow status aggregator flush window to pass without panicking on deleted CRD.
		time.Sleep(ccc.ConditionsFlushInterval)
		synctest.Wait()

		// Verify the pending pod for the deleted CRD was not scheduled and no new NAP pool was created.
		cluster := integration.GetTestCluster(t, infra, testConfig)
		assert.Len(t, cluster.NodePools, 2, "Expected no new NAP node pools after ComputeClass CRD deletion")

		updatedPendingPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, pendingPod.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Empty(t, updatedPendingPod.Spec.NodeName, "Expected pending pod targeting deleted ComputeClass CRD to remain unscheduled")

		// Verify the 3 idle orphaned nodes were scaled down once the deleted CRD's TargetNodeCount floor was removed,
		// leaving only the 1 node running runningPod.
		remainingNodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		if assert.Len(t, remainingNodes.Items, 1, "Expected 3 idle orphaned nodes to scale down, leaving only the 1 active node") {
			assert.Equal(t, e2NodeName, remainingNodes.Items[0].Name)
		}
	})
}

func testPredefinedComputeClassesMixedWithCustomCCC(t *testing.T) {
	t.Run("StandardClusterWithNodePoolFallbackAndPCC", func(t *testing.T) {
		const customCCCName = "custom-mixed-ccc"
		customCCC := ccc.NewComputeClassBuilder(customCCCName).
			WithPriorities(
				v1.Priority{Nodepools: []string{"custom-pool-p1"}},
				v1.Priority{Nodepools: []string{"custom-pool-p2"}},
			).
			Build()

		// Priority 1 pool is already at max capacity (0 -> max 0 or 1/1 full), forcing fallback to Priority 2.
		customPoolP1 := integration.DefaultNodePool(
			integration.WithNodePoolName("custom-pool-p1"),
			integration.WithNodePoolMachineType("n2-standard-2"),
			integration.WithNodePoolSize(1),
			integration.WithNodePoolMin(1),
			integration.WithNodePoolMax(1),
			integration.WithNodePoolLocations("us-central1-b"),
			integration.WithNodePoolCCCLabel(customCCCName),
			integration.WithNodePoolCCCTaint(customCCCName),
		)

		customPoolP2 := integration.EmptyNodePool("custom-pool-p2").
			WithMachineType("e2-standard-2").
			WithLocations("us-central1-b").
			WithCCCLabel(customCCCName).
			WithCCCTaint(customCCCName).
			WithMax(5).
			Build()

		pccBalancedPool := integration.EmptyNodePool("pcc-balanced-pool").
			WithMachineType("n2-standard-2").
			WithLocations("us-central1-b").
			WithCCCLabel("Balanced").
			WithCCCTaint("Balanced").
			WithMax(5).
			Build()

		testConfig := integration.NewTestConfig().
			WithNodePools(customPoolP1, customPoolP2, pccBalancedPool).
			WithCccCrds(customCCC)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer integration_synctest.TearDown(cancel)
			infra := integration.SetupInfrastructure(ctx, t)

			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			assert.NoError(t, err)

			// Occupy custom-pool-p1's single node so a new pod targeting custom-mixed-ccc must fall back to custom-pool-p2.
			nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, nodes.Items, 1)

			occupyingPod := tu.BuildTestPod("occupying-p1-pod", 1500, 1000, pod.WithCCC(customCCCName))
			occupyingPod.Spec.NodeName = nodes.Items[0].Name
			_, err = infra.Fakes.KubeClient.CoreV1().Pods("default").Create(ctx, occupyingPod, metav1.CreateOptions{})
			assert.NoError(t, err)

			// Add a pending pod targeting the Predefined Compute Class ("Balanced")
			// and a pending pod targeting the Custom Compute Class ("custom-mixed-ccc").
			pccPod := tu.BuildTestPod("pod-pcc-balanced", 1200, 1000,
				tu.MarkUnschedulable(),
				pod.WithCCC("Balanced"),
			)
			customPod := tu.BuildTestPod("pod-custom-ccc-fallback", 1200, 1000,
				tu.MarkUnschedulable(),
				pod.WithCCC(customCCCName),
			)
			infra.Fakes.K8s.AddPod(pccPod)
			infra.Fakes.K8s.AddPod(customPod)

			// Pod sharding processes one compute-class shard per RunOnce loop.
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
			infra.Fakes.RunScheduler(ctx, t)
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPccPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, pccPod.Name, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotEmpty(t, updatedPccPod.Spec.NodeName, "Expected PCC pod to be scheduled")
			assert.Contains(t, updatedPccPod.Spec.NodeName, "pcc-balanced-pool", "Expected PCC pod to schedule on pcc-balanced-pool")

			updatedCustomPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, customPod.Name, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotEmpty(t, updatedCustomPod.Spec.NodeName, "Expected custom CCC pod to be scheduled via fallback priority")
			assert.Contains(t, updatedCustomPod.Spec.NodeName, "custom-pool-p2", "Expected custom CCC pod to fall back to custom-pool-p2")
		})
	})

	t.Run("AutopilotNAPWithPCCAndCustomCCC", func(t *testing.T) {
		const customCCCName = "autopilot-custom-ccc"
		customCCC := ccc.NewComputeClassBuilder(customCCCName).
			WithNapEnabled().
			WithAutopilot(true).
			WithPriorities(
				v1.Priority{MachineFamily: ptr.To("c2")},
				v1.Priority{MachineFamily: ptr.To("e2")},
			).
			Build()

		testConfig := integration.NewTestConfig().
			WithCccCrds(customCCC).
			WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
			WithOverrides(
				integration.WithAutoProvisioningEnabled(),
				integration.WithCccNodeAutoprovisioningEnabled(),
				integration.WithEdpAutopilotOptions(),
			)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer integration_synctest.TearDown(cancel)
			infra := integration.SetupInfrastructure(ctx, t)

			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			assert.NoError(t, err)

			// Add both a Predefined Compute Class ("Scale-Out") pod and a Custom Compute Class pod.
			pccPod := tu.BuildTestPod("ap-pod-scale-out", 2000, 4000,
				tu.MarkUnschedulable(),
				pod.WithCCC("Scale-Out"),
				pod.WithTolerationValue("kubernetes.io/arch", apiv1.TolerationOpEqual, "amd64", apiv1.TaintEffectNoSchedule),
			)
			customPod := tu.BuildTestPod("ap-pod-custom-ccc", 2000, 4000,
				tu.MarkUnschedulable(),
				pod.WithCCC(customCCCName),
				pod.WithTolerationValue("kubernetes.io/arch", apiv1.TolerationOpEqual, "amd64", apiv1.TaintEffectNoSchedule),
			)
			infra.Fakes.K8s.AddPod(pccPod)
			infra.Fakes.K8s.AddPod(customPod)

			// Pod sharding processes one compute-class shard per RunOnce loop.
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
			infra.Fakes.RunScheduler(ctx, t)
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
			infra.Fakes.RunScheduler(ctx, t)

			cluster := integration.GetTestCluster(t, infra, testConfig)
			assert.Len(t, cluster.NodePools, 2, "Expected NAP to provision one pool for PCC Scale-Out and one pool for custom CCC")

			var foundScaleOutFamily, foundCustomC2Family bool
			for _, np := range cluster.NodePools {
				if strings.HasPrefix(np.Config.MachineType, "t2a-") || strings.HasPrefix(np.Config.MachineType, "t2d-") {
					foundScaleOutFamily = true
				}
				if strings.HasPrefix(np.Config.MachineType, "c2-") {
					foundCustomC2Family = true
				}
			}
			assert.True(t, foundScaleOutFamily, "Expected a T2A/T2D node pool to be provisioned for Scale-Out PCC")
			assert.True(t, foundCustomC2Family, "Expected a C2 node pool (priority 1) to be provisioned for custom CCC")
		})
	})
}

func testEmptyPrioritiesAndInvalidMachineFamilyFallback(t *testing.T) {
	t.Run("EmptyPrioritiesScaleUpAnyway", func(t *testing.T) {
		emptyScaleUpAnywayCCC := ccc.NewComputeClassBuilder("empty-priorities-scale-up-anyway").
			WithNapEnabled().
			WithWhenUnsatisfiable("ScaleUpAnyway").
			WithPriorities().
			Build()

		testConfig := integration.NewTestConfig().
			WithCccCrds(emptyScaleUpAnywayCCC).
			WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
			WithOverrides(
				integration.WithAutoProvisioningEnabled(),
				integration.WithCccNodeAutoprovisioningEnabled(),
				integration.WithEnhancedCrdStatusReporting(true),
			)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer integration_synctest.TearDown(cancel)
			infra := integration.SetupInfrastructure(ctx, t)

			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			assert.NoError(t, err)

			p := tu.BuildTestPod("pod-empty-scale-up-anyway", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithCCC(emptyScaleUpAnywayCCC.Name),
			)
			infra.Fakes.K8s.AddPod(p)

			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, p.Name, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to schedule via ScaleUpAnyway when Priorities is empty")
		})
	})

	t.Run("InvalidMachineFamilySkippedAndFallsBackToValidPriority", func(t *testing.T) {
		fallbackCCC := ccc.NewComputeClassBuilder("invalid-family-fallback-ccc").
			WithNapEnabled().
			WithWhenUnsatisfiable("DoNotScaleUp").
			WithPriorities(
				v1.Priority{
					MachineFamily: ptr.To("unregistered-invalid-family"),
				},
				v1.Priority{
					MachineType: ptr.To("unregistered-invalid-type-standard-4"),
				},
				v1.Priority{
					MachineFamily: ptr.To("n2"),
				},
			).
			Build()

		testConfig := integration.NewTestConfig().
			WithCccCrds(fallbackCCC).
			WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
			WithOverrides(
				integration.WithAutoProvisioningEnabled(),
				integration.WithCccNodeAutoprovisioningEnabled(),
				integration.WithEnhancedCrdStatusReporting(true),
			)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer integration_synctest.TearDown(cancel)
			infra := integration.SetupInfrastructure(ctx, t)

			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			assert.NoError(t, err)

			p := tu.BuildTestPod("pod-fallback-valid-rule", 2000, 4000,
				tu.MarkUnschedulable(),
				pod.WithCCC(fallbackCCC.Name),
			)
			infra.Fakes.K8s.AddPod(p)

			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
			infra.Fakes.RunScheduler(ctx, t)

			cluster := integration.GetTestCluster(t, infra, testConfig)
			assert.Len(t, cluster.NodePools, 1, "Expected CA to skip invalid rules and provision 1 node pool using the valid fallback rule")
			assert.True(t, strings.HasPrefix(cluster.NodePools[0].Config.MachineType, "n2-"),
				"Expected provisioned node pool to use fallback machine family n2, got %s", cluster.NodePools[0].Config.MachineType)

			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, p.Name, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to be scheduled on fallback node pool")
		})
	})
}
