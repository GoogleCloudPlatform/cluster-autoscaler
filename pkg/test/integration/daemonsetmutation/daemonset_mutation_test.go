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

package daemonsetmutation

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/daemonset"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/reactors"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestDaemonSetMutationOverhead verifies that Cluster Autoscaler accounts for DaemonSet
// pod overhead injected via mutation during scale-up scheduling simulation when the feature
// is enabled, and ignores it when the feature is disabled.
func TestDaemonSetMutationOverhead(t *testing.T) {
	testCases := []struct {
		name                 string
		mutationEnabled      bool
		expectedNodePoolName string
		assertionMessage     string
	}{
		{
			name:                 "mutation enabled - accounts for overhead and scales up pool-large",
			mutationEnabled:      true,
			expectedNodePoolName: "pool-large",
			assertionMessage:     "Expected pod to be scheduled on pool-large due to DaemonSet overhead",
		},
		{
			name:                 "mutation disabled - ignores overhead and scales up pool-small",
			mutationEnabled:      false,
			expectedNodePoolName: "pool-small",
			assertionMessage:     "Expected pod to be scheduled on pool-small because mutation feature is disabled",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodePools := []*gke_api_beta.NodePool{
				integration.EmptyNodePool("pool-small").WithMachineType("n1-standard-2").Build(),
				integration.EmptyNodePool("pool-large").WithMachineType("n1-standard-4").Build(),
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithOverrides(
					integration.WithMaxMemoryTotal(140*1024*1024*1024),
					integration.WithDaemonSetMutationEnabled(tc.mutationEnabled),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				defer integration_synctest.TearDown(cancel)

				// Prepend Reactor to mock dry-run API server mutation.
				reactors.PrependMockDaemonSetMutationReactor(&infra.Fakes.KubeClient.Fake, "ds-overhead", "300m")

				ds := daemonset.BuildTestDaemonSet("ds-overhead", "kube-system", daemonset.WithResource(apiv1.ResourceCPU, "300m", "300m"))
				_, err := infra.Fakes.KubeClient.AppsV1().DaemonSets("kube-system").Create(ctx, ds, metav1.CreateOptions{})
				assert.NoError(t, err)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)

				// Wait for background controller to sync informer and populate cache.
				synctest.Wait()

				userPod := tu.BuildTestPod("user-pod", 1500, 100, tu.MarkUnschedulable())
				infra.Fakes.K8s.AddPod(userPod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				// Assertions:
				// If Mutation Enabled:
				//   Mutated DS (600m) + User Pod (1500m) = 2100m > 2000m (capacity of n1-standard-2)
				//   Should scale up pool-large.
				// If Mutation Disabled:
				//   DS (300m) + User Pod (1500m) = 1800m <= 1930m (allocatable of n1-standard-2)
				//   Should scale up pool-small.
				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "user-pod", metav1.GetOptions{})
				assert.NoError(t, err)
				assert.NotEmpty(t, updatedPod.Spec.NodeName)
				assert.Contains(t, updatedPod.Spec.NodeName, tc.expectedNodePoolName, tc.assertionMessage)
			})
		})
	}
}

// TestNAPWithDaemonSetMutationOverhead verifies that NAP accounts for DaemonSet
// pod overhead injected via mutation during scale-up scheduling simulation when the feature
// is enabled, and ignores it when the feature is disabled.
func TestNAPWithDaemonSetMutationOverhead(t *testing.T) {
	testCases := []struct {
		name                string
		mutationEnabled     bool
		expectedMachineType string
		assertionMessage    string
	}{
		{
			name:                "mutation enabled - accounts for overhead and autoprovisions pool with n1-standard-4",
			mutationEnabled:     true,
			expectedMachineType: "n1-standard-4",
			assertionMessage:    "Expected NAP to provision n1-standard-4 due to DaemonSet overhead",
		},
		{
			name:                "mutation disabled - ignores overhead and scales up default-pool (n1-standard-2)",
			mutationEnabled:     false,
			expectedMachineType: "n1-standard-2",
			assertionMessage:    "Expected NAP/Autoscaler to use n1-standard-2 because mutation feature is disabled",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodePools := []*gke_api_beta.NodePool{
				integration.DefaultNodePool(
					integration.WithNodePoolMachineType("n1-standard-2"),
					integration.WithNodePoolSize(0),
					integration.WithNodePoolLocations("us-central1-a"),
				),
			}

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithOverrides(
					integration.WithMaxMemoryTotal(140*1024*1024*1024),
					integration.WithDaemonSetMutationEnabled(tc.mutationEnabled),
					integration.WithAutoProvisioningEnabled(),
				).
				WithClusterOverrides(
					integration.WithClusterAutoProvisioningEnabled(),
					integration.WithAutoprovisioningLocations("us-central1-a"),
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				defer integration_synctest.TearDown(cancel)

				// Prepend Reactor to mock dry-run API server mutation.
				reactors.PrependMockDaemonSetMutationReactor(&infra.Fakes.KubeClient.Fake, "ds-overhead", "300m")

				ds := daemonset.BuildTestDaemonSet("ds-overhead", "kube-system", daemonset.WithResource(apiv1.ResourceCPU, "300m", "300m"))
				_, err := infra.Fakes.KubeClient.AppsV1().DaemonSets("kube-system").Create(ctx, ds, metav1.CreateOptions{})
				assert.NoError(t, err)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)

				// Wait for background controller to sync informer and populate cache.
				synctest.Wait()

				userPod := tu.BuildTestPod("user-pod", 1500, 4*1024*1024*1024, tu.MarkUnschedulable())
				infra.Fakes.K8s.AddPod(userPod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				// Assertions:
				// If Mutation Enabled:
				//   Mutated DS (600m) + User Pod (1500m) = 2100m > 1930m (allocatable of n1-standard-2)
				//   Should autoprovision a new node pool with n1-standard-4.
				// If Mutation Disabled:
				//   DS (300m) + User Pod (1500m) = 1800m <= 1930m (allocatable of n1-standard-2)
				//   Should scale up default-pool (n1-standard-2).
				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, "user-pod", metav1.GetOptions{})
				assert.NoError(t, err)
				assert.NotEmpty(t, updatedPod.Spec.NodeName)

				// Get the node details to verify machine type.
				node, err := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, updatedPod.Spec.NodeName, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedMachineType, node.Labels[apiv1.LabelInstanceTypeStable], tc.assertionMessage)
			})
		})
	}
}
