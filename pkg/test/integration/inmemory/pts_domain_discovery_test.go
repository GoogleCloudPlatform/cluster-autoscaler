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

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	options "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	ccc_builder "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	pod "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestCCCDomainDiscovery verifies that the ComputeClass (CCC) Domain Discovery processor
// correctly discovers domains specified in the ComputeClass rules, assigns the pods to
// domains maintaining topology spread constraints, and triggers NAP to provision separate
// node pools satisfying the assigned rules.
func TestCCCDomainDiscovery(t *testing.T) {
	cccName := "my-ccc-1"
	cccDomainKey := "my-ccc-domain-key"
	ccc1 := ccc_builder.NewComputeClassBuilder(cccName).
		WithNapEnabled().
		WithPriorities(
			v1.Priority{
				NodeLabels: map[string]string{
					cccDomainKey: "domain-a",
				},
			},
			v1.Priority{
				NodeLabels: map[string]string{
					cccDomainKey: "domain-b",
				},
			},
		).
		Build()

	pts := apiv1.TopologySpreadConstraint{
		MaxSkew:           1,
		TopologyKey:       cccDomainKey,
		WhenUnsatisfiable: apiv1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "pts-app"},
		},
	}

	pod1 := buildTestPTSPod("ccc-pod-1", pts, pod.WithCCC(cccName))
	pod2 := buildTestPTSPod("ccc-pod-2", pts, pod.WithCCC(cccName))

	testConfig := integration.NewTestConfig().
		WithCaVersion("35.140.0").
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			func(o *options.AutoscalingOptions) *options.AutoscalingOptions {
				o.AllowlistedSystemLabels = cccDomainKey
				return o
			},
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations("us-central1-a"),
		).
		WithExperiments("PodTopologySpreadCCC::MinCAVersion").
		WithCccCrds(ccc1)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		infra.Fakes.K8s.AddPod(pod1)
		infra.Fakes.K8s.AddPod(pod2)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		// Run two autoscaler loops to allow both node pools to be created sequentially
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Verify that two new node pools were dynamically created by NAP matching Rule 1 ("domain-a") and Rule 2 ("domain-b")
		cluster := integration.GetTestCluster(t, infra, testConfig)
		// Since the default machineType is n2-standard-2 which has 2 cpu we cannot put pods on the same node.
		// If we do not force minDomains and with feature enabled, NAP correctly triggers to create two separete node pools and scales them both up.
		// With feature disabled, NAP creates only one node pool and creates two nodes there.
		assert.Len(t, cluster.NodePools, 2, "Expected NAP to create two new node pools")
		assertNodePoolWithLabels(t, cluster, map[string]string{
			gkelabels.ComputeClassLabel: cccName,
			cccDomainKey:                "domain-a",
		})
		assertNodePoolWithLabels(t, cluster, map[string]string{
			gkelabels.ComputeClassLabel: cccName,
			cccDomainKey:                "domain-b",
		})
	})
}

// TestZonalDomainDiscovery verifies that the Zonal Domain Discovery processor
// correctly discovers zone domains, applies zone node selectors to the pods,
// and triggers NAP to create separate node pools in the assigned zones.
func TestZonalDomainDiscovery(t *testing.T) {
	pts := apiv1.TopologySpreadConstraint{
		MaxSkew:           1,
		TopologyKey:       apiv1.LabelTopologyZone,
		WhenUnsatisfiable: apiv1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "pts-app"},
		},
	}

	pod1 := buildTestPTSPod("zonal-pod-1", pts)
	pod2 := buildTestPTSPod("zonal-pod-2", pts)

	testConfig := integration.NewTestConfig().
		WithCaVersion("35.140.0").
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations("us-central1-a", "us-central1-b"),
		).
		WithExperiments("PodTopologySpreadZonal::MinCAVersion")

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)
		infra.Fakes.K8s.AddPod(pod1)
		infra.Fakes.K8s.AddPod(pod2)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		// Run two loops to allow both zones to scale up.
		// If we do not force minDomains with feature enabled, NAP correctly triggers creation of two separate node pools and scales them both up.
		// With feature disabled, NAP creates only one node pool and scales it up to two nodes.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Assert that we have ONLY one MIG in each zone
		migsInZoneA, err := infra.Fakes.GceService.FetchAllMigs("us-central1-a")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(migsInZoneA))

		migsInZoneB, err := infra.Fakes.GceService.FetchAllMigs("us-central1-b")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(migsInZoneB))

		// Assert that we have ONLY one node in each MIG
		assert.Equal(t, int64(1), migsInZoneA[0].TargetSize)
		assert.Equal(t, int64(1), migsInZoneB[0].TargetSize)
	})
}

// TestNodeBasedDomainDiscovery verifies that the Node-Based Domain Discovery processor
// correctly discovers domains based on labels of node pool templates, assigns pods across
// the discovered domains, and triggers appropriate scale ups.
func TestNodeBasedDomainDiscovery(t *testing.T) {
	testCases := []struct {
		name                       string
		minDomains                 *int32
		numPods                    int
		expectedNodePoolTargetSize int64
	}{
		{
			// If we do not force minDomains and with feature enabled, Node-Based DD correctly discovers both domains and scales both node pools up.
			// With feature disabled, only one node pool would be provisioned and scaled up to 2 nodes.
			name:                       "WithoutMinDomains_pts_pods_are_properly_spread_among_available_domains",
			minDomains:                 nil,
			numPods:                    2,
			expectedNodePoolTargetSize: 1,
		},
		{
			// With minDomains specified, scaling up both node pools works even with the feature disabled, but much slower:
			// For 50 pods with flag enabled: we are done in 2 loops -
			// 		1. We scale up np-a to 25 nodes.
			//		2. We scale up np-b to 25 nodes.
			// With flag disabled: in 2 loops we create only 3 nodes:
			// 		1. We scale up np-a to 1 node (because np-b has 0 nodes and we have maxSkew = 1).
			// 		2. We scale up np-b to 2 nodes.
			//		(Next: scale up np-a to 3 nodes, etc.)
			// And we need many additional CA loops to have all nodes in place.
			name:                       "WithMinDomains_50_pts_pods_are_satisfied_in_2_loops",
			minDomains:                 new(int32(2)),
			numPods:                    50,
			expectedNodePoolTargetSize: 25,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			customDomainKey := "my-custom-domain-key"
			pts := apiv1.TopologySpreadConstraint{
				MaxSkew:           1,
				TopologyKey:       customDomainKey,
				WhenUnsatisfiable: apiv1.DoNotSchedule,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "pts-app"},
				},
				MinDomains: tc.minDomains,
			}

			pods := make([]*apiv1.Pod, tc.numPods)
			for i := 0; i < tc.numPods; i++ {
				pods[i] = buildTestPTSPod(fmt.Sprintf("custom-pod-%d", i), pts)
			}

			testConfig := integration.NewTestConfig().
				WithCaVersion("35.140.0").
				WithNodePools(
					integration.EmptyNodePool("np-a").
						WithLocations("us-central1-a").
						WithLabels(map[string]string{customDomainKey: "domain-a"}).
						Build(),
					integration.EmptyNodePool("np-b").
						WithLocations("us-central1-a").
						WithLabels(map[string]string{customDomainKey: "domain-b"}).
						Build(),
				).
				WithExperiments("PodTopologySpreadNodeBased::MinCAVersion")

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)

				infra := integration.SetupInfrastructure(ctx, t)
				for _, pod := range pods {
					infra.Fakes.K8s.AddPod(pod)
				}
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Verify that we still have two node pools in the cluster
				cluster := integration.GetTestCluster(t, infra, testConfig)
				assert.Len(t, cluster.NodePools, 2, "Expected cluster to have two node pools")
				// Verify labels and the target sizes of node pools
				npA := infra.Fakes.GkeService.MustGetNodePool(t, "np-a")
				npB := infra.Fakes.GkeService.MustGetNodePool(t, "np-b")
				assert.True(t, nodePoolHasAllMatchingLabels(npA, map[string]string{customDomainKey: "domain-a"}))
				assert.True(t, nodePoolHasAllMatchingLabels(npB, map[string]string{customDomainKey: "domain-b"}))
				assert.Equal(t, tc.expectedNodePoolTargetSize, infra.Fakes.GkeService.MustGetTargetSize(t, npA), fmt.Sprintf("Expected pool-a to scale up from 0 to %d", tc.expectedNodePoolTargetSize))
				assert.Equal(t, tc.expectedNodePoolTargetSize, infra.Fakes.GkeService.MustGetTargetSize(t, npB), fmt.Sprintf("Expected pool-b to scale up from 0 to %d", tc.expectedNodePoolTargetSize))
			})
		})
	}
}

// TestNodeBasedDomainDiscovery_PTSBackoffInteractions verifies that the Node-Based Domain Discovery processor
// in case of PTS backoff correctly falls back to the DEFAULT incremental node pool's scale ups without
// mutating the PTS constraint.
func TestNodeBasedDomainDiscovery_PTSBackoffInteractions(t *testing.T) {
	testCases := []struct {
		name                   string
		numOfPods              int
		initialMaxNodeCount    int64
		initialLoopCount       int
		postResizeLoopCount    int
		resizeNodePools        func(npA, npB *gke_api_beta.NodePool)
		expectedInitialNpSizes []int64
		expectedFinalNpSizes   []int64
	}{
		{
			name:                "NoBackoff_ScaleUpSucceedsInTwoLoops",
			numOfPods:           20,
			initialMaxNodeCount: 10,
			initialLoopCount:    2,
			// In this case we do not have backoffs, so we safely schedule all pods in first 2 CA loops
			expectedInitialNpSizes: []int64{10, 10},
			expectedFinalNpSizes:   []int64{10, 10},
		},
		{
			name:                "Backoff_ResizeBothNodePools_FallbackLogicScalesIncrementally",
			numOfPods:           20,
			initialMaxNodeCount: 5,
			// 5 loops: 2 to scale both pools to max (5 each), 1 to fail and annotate remaining pods as unhelpable,
			// 1 to register the controller in PTS backoff in Preprocess, and 1 to stabilize into steady-state backoff.
			initialLoopCount:    5,
			postResizeLoopCount: 2,
			resizeNodePools: func(npA, npB *gke_api_beta.NodePool) {
				npA.Autoscaling.MaxNodeCount = 10
				npB.Autoscaling.MaxNodeCount = 10
			},
			// We enter the PTS backoff here, so we have to fallback to slow/alternating scale ups of node pools respecting the maxSkew
			// 1st loop: npA - 5 -> 6, npB stays the same, 2nd loop: npB - 5->7, npA stays the same (because maxSkew = 1)
			// Due to randomness of the simulation it is possible that we did the scale ups in the opposite order and npB will have 7 and npA - 6 nodes.
			expectedInitialNpSizes: []int64{5, 5},
			expectedFinalNpSizes:   []int64{6, 7},
		},
		{
			name:                "Backoff_ResizeSingleNodePool_GetsStuckDueToMaxSkew",
			numOfPods:           20,
			initialMaxNodeCount: 5,
			// 5 loops: 2 to scale both pools to max (5 each), 1 to fail and annotate remaining pods as unhelpable,
			// 1 to register the controller in PTS backoff in Preprocess, and 1 to stabilize into steady-state backoff.
			initialLoopCount:    5,
			postResizeLoopCount: 2,
			resizeNodePools: func(npA, npB *gke_api_beta.NodePool) {
				npA.Autoscaling.MaxNodeCount = 10
			},
			// The same as in the previous case, but we did not increase the size of npB. So we have
			// 1st loop: npA - 5 -> 6, npB stays the same, 2nd loop: npB cannot scale up, so we are stuck.
			expectedInitialNpSizes: []int64{5, 5},
			expectedFinalNpSizes:   []int64{6, 5},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			customDomainKey := "my-custom-domain-key"
			pts := apiv1.TopologySpreadConstraint{
				MaxSkew:           1,
				TopologyKey:       customDomainKey,
				WhenUnsatisfiable: apiv1.DoNotSchedule,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "pts-app"},
				},
			}

			pods := make([]*apiv1.Pod, tc.numOfPods)
			for i := 0; i < tc.numOfPods; i++ {
				pods[i] = buildTestPTSPod(fmt.Sprintf("custom-pod-%d", i), pts, pod.WithOwnerReplicaSet("test-rs"))
			}

			testConfig := integration.NewTestConfig().
				WithCaVersion("35.140.0").
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithAutoprovisioningLocations("us-central1-a"),
				).
				WithNodePools(
					integration.EmptyNodePool("np-a").
						WithLocations("us-central1-a").
						WithLabels(map[string]string{customDomainKey: "domain-a"}).
						WithMax(tc.initialMaxNodeCount).
						Build(),
					integration.EmptyNodePool("np-b").
						WithLocations("us-central1-a").
						WithLabels(map[string]string{customDomainKey: "domain-b"}).
						WithMax(tc.initialMaxNodeCount).
						Build(),
				).
				WithExperiments("PodTopologySpreadNodeBased::MinCAVersion")

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)

				infra := integration.SetupInfrastructure(ctx, t)
				for _, pod := range pods {
					infra.Fakes.K8s.AddPod(pod)
				}
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)

				for i := 0; i < tc.initialLoopCount; i++ {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				}

				npA := infra.Fakes.GkeService.MustGetNodePool(t, "np-a")
				npB := infra.Fakes.GkeService.MustGetNodePool(t, "np-b")
				assert.ElementsMatch(t,
					tc.expectedInitialNpSizes,
					[]int64{
						infra.Fakes.GkeService.MustGetTargetSize(t, npA),
						infra.Fakes.GkeService.MustGetTargetSize(t, npB),
					},
				)

				if tc.resizeNodePools != nil {
					tc.resizeNodePools(npA, npB)
					for i := 0; i < tc.postResizeLoopCount; i++ {
						integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 2*time.Minute)
					}
				}
				// Use ElementsMatch because the evaluation order between identical node pools
				// during scale-up simulations is non-deterministic, so either pool may scale first.
				assert.ElementsMatch(t,
					tc.expectedFinalNpSizes,
					[]int64{
						infra.Fakes.GkeService.MustGetTargetSize(t, npA),
						infra.Fakes.GkeService.MustGetTargetSize(t, npB),
					},
				)
			})
		})
	}
}

func buildTestPTSPod(name string, pts apiv1.TopologySpreadConstraint, options ...func(*apiv1.Pod)) *apiv1.Pod {
	opts := append([]func(*apiv1.Pod){
		tu.MarkUnschedulable(),
		pod.WithLabels(map[string]string{"app": "pts-app"}),
		pod.WithTopologySpreadConstraints(pts),
	}, options...)
	return tu.BuildTestPod(name, 1500, 100, opts...)
}

func assertNodePoolWithLabels(t *testing.T, cluster *gke_api_beta.Cluster, expectedLabels map[string]string) {
	t.Helper()
	for _, np := range cluster.NodePools {
		if nodePoolHasAllMatchingLabels(np, expectedLabels) {
			return
		}
	}
	t.Errorf("Node pool with labels %v not found in cluster node pools: %v", expectedLabels, cluster.NodePools)
}

func nodePoolHasAllMatchingLabels(np *gke_api_beta.NodePool, labels map[string]string) bool {
	for k, v := range labels {
		if np.Config == nil || np.Config.Labels == nil || np.Config.Labels[k] != v {
			return false
		}
	}
	return true
}
