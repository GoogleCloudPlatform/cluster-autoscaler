/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backoff_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// Test, whether a single zone backoff does not result in back-ing off
// the whole priority
func TestCCCZonalBackoff(t *testing.T) {
	// cc has n1 as optimal (P1, index 0) and n2 as fallback (P2, index 1).
	cc := ccc.NewComputeClassBuilder("zonal-backoff-ccc").
		WithNapEnabled().
		AddPriority(v1.Priority{
			MachineType: ptr.To("n1-standard-4"),
		}).
		AddPriority(v1.Priority{
			MachineType: ptr.To("n2-standard-4"),
		}).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterZones("us-central1-a", "us-central1-b"),
		).
		WithRegionToZones(map[string][]string{"us-central1": {"us-central1-a", "us-central1-b"}}).
		WithCccCrds(cc)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.GceService.SetCreateInstanceForZoneError("us-central1-a", cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
			ErrorCode:    "STOCKOUT",
			ErrorMessage: "Simulated stockout in zone a",
		})

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		// Create a pod requesting the CCC and targeting us-central1-a.
		p1 := tu.BuildTestPod("pod-1", 3000, 4000, tu.MarkUnschedulable(), pod.WithCCC("zonal-backoff-ccc"), pod.WithNodeSelectorEntry("topology.kubernetes.io/zone", "us-central1-a"))
		infra.Fakes.K8s.AddPod(p1)

		// Run autoscaler loop once. It should try to scale up in us-central1-a and fail.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Verify no nodes are created yet (since the attempt failed).
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 0, "Expected no nodes to be created after first failed attempt")

		// Create a second pod targeting us-central1-b.
		p2 := tu.BuildTestPod("pod-2", 3000, 4000, tu.MarkUnschedulable(), pod.WithCCC("zonal-backoff-ccc"), pod.WithNodeSelectorEntry("topology.kubernetes.io/zone", "us-central1-b"))
		infra.Fakes.K8s.AddPod(p2)

		// Run autoscaler loop again.
		// Priority 0 (n1-standard-4) is in zonal backoff for us-central1-a, but NOT for us-central1-b.
		// Therefore, pod-2 targeting us-central1-b should successfully scale up with Priority 0 (n1-standard-4) in us-central1-b.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// We expect 1 node to be created in us-central1-b with Priority 0 (n1-standard-4).
		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)

		if !assert.Len(t, nodes.Items, 1, "Expected exactly 1 node to be created") {
			return
		}

		node := nodes.Items[0]
		assert.Equal(t, "us-central1-b", node.Labels["topology.kubernetes.io/zone"])
		assert.Equal(t, "n1-standard-4", node.Labels["node.kubernetes.io/instance-type"])
		assert.Equal(t, "zonal-backoff-ccc", node.Labels["cloud.google.com/compute-class"])
	})
}

// When priority gets the quota error, we should back-off the entire priority.
func TestCCCQuotaBackoff(t *testing.T) {
	// cc has n1 as optimal (P1, index 0) and n2 as fallback (P2, index 1).
	cc := ccc.NewComputeClassBuilder("quota-backoff-ccc").
		WithNapEnabled().
		AddPriority(v1.Priority{
			MachineType: ptr.To("n1-standard-4"),
		}).
		AddPriority(v1.Priority{
			MachineType: ptr.To("n2-standard-4"),
		}).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterZones("us-central1-a", "us-central1-b"),
		).
		WithRegionToZones(map[string][]string{"us-central1": {"us-central1-a", "us-central1-b"}}).
		WithCccCrds(cc)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		// Set quota error for zone us-central1-a.
		// A quota error triggers full/region backoff for the priority across all zones.
		infra.Fakes.GceService.SetCreateInstanceForZoneError("us-central1-a", cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
			ErrorCode:    gce.ErrorCodeQuotaExceeded,
			ErrorMessage: "Simulated quota error in zone a",
		})

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		// Create a pod requesting the CCC and targeting us-central1-a.
		p1 := tu.BuildTestPod("pod-1", 3000, 4000, tu.MarkUnschedulable(), pod.WithCCC("quota-backoff-ccc"), pod.WithNodeSelectorEntry("topology.kubernetes.io/zone", "us-central1-a"))
		infra.Fakes.K8s.AddPod(p1)

		// Run autoscaler loop once. It should try to scale up Priority 0 (n1-standard-4) in us-central1-a and fail with QUOTA_EXCEEDED.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

		// Verify no nodes are created yet.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Len(t, nodes.Items, 0, "Expected no nodes to be created after first failed attempt")

		// Create a second pod targeting us-central1-b.
		infra.Fakes.K8s.DeletePod(p1.Namespace, p1.Name)
		p2 := tu.BuildTestPod("pod-2", 3000, 4000, tu.MarkUnschedulable(), pod.WithCCC("quota-backoff-ccc"), pod.WithNodeSelectorEntry("topology.kubernetes.io/zone", "us-central1-b"))
		infra.Fakes.K8s.AddPod(p2)

		// Run autoscaler loop again.
		// Priority 0 was placed in full backoff region due to QUOTA error in zone a, so it cannot be used even for pod-2 in us-central1-b.
		// Autoscaler must fall back to Priority 1 (n2-standard-4).
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)

		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)

		if !assert.Len(t, nodes.Items, 1, "Expected exactly 1 node to be created") {
			return
		}

		node := nodes.Items[0]
		assert.Equal(t, "us-central1-b", node.Labels["topology.kubernetes.io/zone"])
		assert.Equal(t, "n2-standard-4", node.Labels["node.kubernetes.io/instance-type"], "Expected Priority 1 (n2-standard-4) due to full backoff of Priority 0 from quota error")
		assert.Equal(t, "quota-backoff-ccc", node.Labels["cloud.google.com/compute-class"])
	})
}

// The test tries to schedule multiple pods in a single iteration in few
// different zones. One of them fails - as a result, the second iteration of CA
// should retry scheduling a failed pod, on already succeeded zones.
// No back-off to the second priority expected.
func TestCCCMultiZoneScaleButSingleZoneBackoff(t *testing.T) {
	// cc has n1 as optimal (P1, index 0) and n2 as fallback (P2, index 1).
	cc := ccc.NewComputeClassBuilder("zonal-backoff-ccc").
		WithNapEnabled().
		AddPriority(v1.Priority{
			MachineType: ptr.To("n1-standard-4"),
		}).
		AddPriority(v1.Priority{
			MachineType: ptr.To("n2-standard-4"),
		}).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			integration.WithBalanceSimilarNodeGroups(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterZones("us-central1-a", "us-central1-b"),
		).
		WithRegionToZones(map[string][]string{"us-central1": {"us-central1-a", "us-central1-b"}}).
		WithCccCrds(cc)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)

		infra.Fakes.GceService.SetCreateInstanceForZoneError("us-central1-a", cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
			ErrorCode:    "STOCKOUT",
			ErrorMessage: "Simulated stockout in zone a",
		})

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		// Create two pods requesting full node.
		p1 := tu.BuildTestPod("pod-1", 3000, 4000, tu.MarkUnschedulable(), pod.WithCCC("zonal-backoff-ccc"))
		infra.Fakes.K8s.AddPod(p1)
		p2 := tu.BuildTestPod("pod-2", 3000, 4000, tu.MarkUnschedulable(), pod.WithCCC("zonal-backoff-ccc"))
		infra.Fakes.K8s.AddPod(p2)

		// Run autoscaler loop once. It should try to scale up in us-central1-a and us-central1-b.
		// Only us-central1-b scale up succeeds.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Verify only one node created
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		if assert.Len(t, nodes.Items, 1, "Expected one node to be created after first attempt") {
			assert.Equal(t, "us-central1-b", nodes.Items[0].Labels["topology.kubernetes.io/zone"])
		}

		// Run autoscaler loop again. We should scale in 1st priority again.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// We expect 2 node to be created in us-central1-b with Priority 0 (n1-standard-4).
		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		if assert.Len(t, nodes.Items, 2, "Expected two nodes to be created") {
			assert.Equal(t, "us-central1-b", nodes.Items[0].Labels["topology.kubernetes.io/zone"])
			assert.Equal(t, "us-central1-b", nodes.Items[1].Labels["topology.kubernetes.io/zone"])
		}
	})
}
