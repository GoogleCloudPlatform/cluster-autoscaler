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

package networking

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	netapi "github.com/GoogleCloudPlatform/gke-networking-api/apis/network/v1"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	netutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const multiNetworkName = "blue-net"

// TestMultiNetworkNapScaleUp verifies that NAP autoprovisions a node pool with multi-networking
// configuration when an unschedulable pod requests a multi-network resource. Node templates built
// from a NodePoolSpec must carry multi-network capacity, otherwise the pod never fits the NAP
// candidate and no node pool is created.
func TestMultiNetworkNapScaleUp(t *testing.T) {
	testConfig := newMultiNetworkTestConfig(multiNetworkName)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		infra.Fakes.K8s.AddPod(buildMultiNetworkPod("multi-net-pod", multiNetworkName))

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// NAP creates a single node pool carrying the additional pod network configuration.
		cluster := integration.GetTestCluster(t, infra, testConfig)
		assert.Len(t, cluster.NodePools, 1, "expected exactly one NAP-created node pool")
		np := cluster.NodePools[0]
		assert.NotNil(t, np.NetworkConfig, "expected NetworkConfig on the autoprovisioned node pool")
		assert.NotEmpty(t, np.NetworkConfig.AdditionalPodNetworkConfigs, "expected AdditionalPodNetworkConfigs to be populated")

		assert.Len(t, infra.Fakes.K8s.Nodes().Items, 1, "expected NAP to register exactly one node")
	})
}

// TestMultiNetworkNapNetdInitialization exercises the window where a freshly booted
// multi-networking node is already Ready according to kubelet but netd and the NetDevice device
// plugin have not registered their resources in Status.Allocatable yet. MultiNetworkingProcessor
// demotes such a node to ResourceUnready so that it keeps counting as upcoming capacity and the
// still-pending pod does not drive redundant scale-ups.
func TestMultiNetworkNapNetdInitialization(t *testing.T) {
	testConfig := newMultiNetworkTestConfig(multiNetworkName)
	resName := multiNetworkResourceName(multiNetworkName)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler := integration.MustSetupAutoscaler(ctx, t, testConfig, infra)

		pod := buildMultiNetworkPod("multi-net-pod", multiNetworkName)
		infra.Fakes.K8s.AddPod(pod)

		// The pending pod triggers the initial NAP scale-up.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Len(t, integration.GetTestCluster(t, infra, testConfig).NodePools, 1, "expected one NAP-created node pool")

		nodes := infra.Fakes.K8s.Nodes().Items
		assert.Len(t, nodes, 1, "expected exactly one node after the initial scale-up")
		node := nodes[0].DeepCopy()

		// The node boots Ready but netd has not registered the network resource yet.
		assert.NotContains(t, node.Status.Allocatable, resName, "expected the booting node to lack multi-network allocatable")

		// In production, CloudProvider.Refresh discovers the instance in GCE long before
		// the VM finishes booting and registers with Kubernetes. In this in-memory test
		// the fake registers the node synchronously during scale-up, so we refresh the
		// cloud provider cache to match production reality.
		assert.NoError(t, autoscaler.AutoscalingContext.CloudProvider.Refresh(ctx))

		// MultiNetworkingProcessor demotes the node to ResourceUnready while netd is missing.
		// CA counts it as NotStarted, so upcoming capacity satisfies the pod and
		// prevents duplicate scale-up.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Len(t, infra.Fakes.K8s.Nodes().Items, 1, "expected no extra node while netd is initializing")
		assert.Len(t, integration.GetTestCluster(t, infra, testConfig).NodePools, 1, "expected no extra node pool while netd is initializing")

		// netd finishes initializing and publishes the network resource.
		qty := resource.MustParse("1")
		node.Status.Capacity[resName] = qty
		node.Status.Allocatable[resName] = qty
		infra.Fakes.K8s.UpdateNode(node)

		// The pod now fits the initialized node, so the cluster stays as it is.
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		assert.Len(t, infra.Fakes.K8s.Nodes().Items, 1, "expected no extra node once netd is ready")
		assert.Len(t, integration.GetTestCluster(t, infra, testConfig).NodePools, 1, "expected no extra node pool once netd is ready")

		// Verify the pod actually schedules onto the initialized node.
		infra.Fakes.RunScheduler(ctx, t)
		scheduledPod, err := infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, node.Name, scheduledPod.Spec.NodeName, "expected pod to be scheduled on the initialized node")
	})
}

func newMultiNetworkTestConfig(networkName string) *integration.TestConfig {
	return integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			integration.WithMultiNetworkSupportEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
		).
		WithGkeNetworkParamSets(newGkeNetworkParamSet(networkName))
}

func newGkeNetworkParamSet(networkName string) *netapi.GKENetworkParamSet {
	return &netapi.GKENetworkParamSet{
		ObjectMeta: metav1.ObjectMeta{Name: networkName},
		Spec: netapi.GKENetworkParamSetSpec{
			VPC:           "test-vpc",
			VPCSubnet:     "test-subnet",
			PodIPv4Ranges: &netapi.SecondaryRanges{RangeNames: []string{"test-range"}},
		},
		Status: netapi.GKENetworkParamSetStatus{NetworkName: networkName},
	}
}

func multiNetworkResourceName(networkName string) apiv1.ResourceName {
	return apiv1.ResourceName(fmt.Sprintf(netutil.MultiNetworkingResourceNamePattern, networkName))
}

func buildMultiNetworkPod(name, networkName string) *apiv1.Pod {
	p := tu.BuildTestPod(name, 1000, 1000, tu.MarkUnschedulable())
	p.Annotations[netapi.InterfaceAnnotationKey] = fmt.Sprintf(`[{"interfaceName":"eth1","network":%q}]`, networkName)

	resName := multiNetworkResourceName(networkName)
	qty := resource.MustParse("1")
	p.Spec.Containers[0].Resources.Requests[resName] = qty
	p.Spec.Containers[0].Resources.Limits = apiv1.ResourceList{resName: qty}
	return p
}
