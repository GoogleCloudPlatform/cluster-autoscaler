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

	"github.com/stretchr/testify/assert"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	dra "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources/testing"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestDRAStartup verifies that Cluster Autoscaler, when started with DRA enabled, can correctly
// scale up a node pool based on pods requesting DRA resources.
// The test sets up a single node pool with DRA-enabled GPU nodes. It first deploys a pod
// that fits on the existing node. Then, it adds a second pod that, combined with the first,
// exceeds the capacity of a single node, triggering a scale-up.
func TestDRAStartup(t *testing.T) {
	testConfig := integration.NewTestConfig().
		WithNodePools(&gke_api_beta.NodePool{
			Name: "dra-nodepool",
			Config: &gke_api_beta.NodeConfig{
				MachineType: "g2-standard-96",
				Labels: map[string]string{
					labels.DraGpuNodeLabel: "true",
				},
				Accelerators: []*gke_api_beta.AcceleratorConfig{
					{
						AcceleratorCount: 8,
						AcceleratorType:  labels.NvidiaL4,
						GpuDriverInstallationConfig: &gke_api_beta.GPUDriverInstallationConfig{
							GpuDriverVersion: "disabled",
						},
					},
				},
			},
			InitialNodeCount: 1,
			Autoscaling: &gke_api_beta.NodePoolAutoscaling{
				Enabled:      true,
				MinNodeCount: 1,
				MaxNodeCount: 10,
			},
			Locations: []string{"us-central1-b"},
		}).
		WithOverrides(
			integration.WithDraEnabled(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		var healthyNodeName string
		for _, node := range infra.Fakes.K8s.Nodes().Items {
			healthyNodeName = node.Name
		}
		assert.NotEmpty(t, healthyNodeName, "Should find the auto-generated healthy node attached to pool")

		// NOTE: Kubernetes fake doesn't perform standard admission logic, i.e. it does't inject resource claims for extended resources.
		// Thus, the pod setup is a bit more extensive than what we would do in e2e tests.
		fillerClaim := dra.NewResourceClaim("filler-claim", dra.GpuDeviceExactReq(7))
		// Retrieve the natively generated ResourceSlice to get the correct Pool name
		slices, err := infra.Fakes.KubeClient.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		dra.MustAllocateClaim(t, ctx, slices.Items, fillerClaim, 7, healthyNodeName)
		_, err = infra.Fakes.KubeClient.ResourceV1().ResourceClaims("default").Create(ctx, fillerClaim, metav1.CreateOptions{})
		assert.NoError(t, err)
		infra.Fakes.K8s.AddPod(gpuPod("filler", "filler-claim", tu.WithNodeName(healthyNodeName), tu.WithCreationTimestamp(time.Now().Add(-1*time.Minute))))

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		// Verify that with just one pod that fits on the node (7 GPUs used out of 8), CA doesn't scale up
		assert.Equal(t, 1, len(infra.Fakes.K8s.Nodes().Items), "Should not scale up with only the filler pod")

		// Now add the 2nd pod which requires 4 GPUs (total 11 > 8), forcing a scale-up
		gpuClaim := dra.NewResourceClaim("gpu-claim", dra.GpuDeviceExactReq(4))
		_, err = infra.Fakes.KubeClient.ResourceV1().ResourceClaims("default").Create(ctx, gpuClaim, metav1.CreateOptions{})
		assert.NoError(t, err)
		gpuPodObj := gpuPod("gpu", "gpu-claim", tu.MarkUnschedulable(), tu.WithCreationTimestamp(time.Now().Add(-1*time.Minute)))
		infra.Fakes.K8s.AddPod(gpuPodObj)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		assert.Equal(t, 2, len(infra.Fakes.K8s.Nodes().Items), fmt.Sprintf("Got %d nodes, want 2 (scale-up from 1)", len(infra.Fakes.K8s.Nodes().Items)))
	})
}

func gpuPod(name string, claimName string, opts ...func(*apiv1.Pod)) *apiv1.Pod {
	opts = append(
		opts,
		tu.WithResourceClaim(claimName, claimName, ""),
		pod.WithNodeSelector(map[string]string{
			labels.GPULabel:           labels.NvidiaL4,
			labels.MachineFamilyLabel: "g2",
			labels.DraGpuNodeLabel:    "true",
		}),
	)
	return tu.BuildTestPod(name, 100, 400, opts...)
}
