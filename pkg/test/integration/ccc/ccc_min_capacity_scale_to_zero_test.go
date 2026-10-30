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

package ccc_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// TestCCCMinCapacityBlocksClusterScaleToZero verifies that the nodes kept for a
// ComputeClass min capacity keep cluster scale-to-zero from removing the node
// pool running the system pods, whether or not the min capacity fake pods stay
// in the snapshot for the whole loop.
func TestCCCMinCapacityBlocksClusterScaleToZero(t *testing.T) {
	const (
		cccName      = "my-ccc"
		minCapacity  = 2
		systemPool   = "system"
		unneededTime = time.Minute
		machineType  = "n1-standard-1"
		zone         = "us-central1-b"
	)
	testCases := []struct {
		name               string
		experimentFlags    map[string]bool
		wantNodeCount      int
		wantSystemNodeKept bool
	}{
		{
			name:               "fake pods placed only for scale-to-zero",
			experimentFlags:    map[string]bool{experiments.ComputeClassMinCapacityDefragOnlyFakePodsEnabledFlag: true},
			wantNodeCount:      minCapacity + 1,
			wantSystemNodeKept: true,
		},
		{
			name:               "fake pods placed for the whole loop",
			experimentFlags:    map[string]bool{experiments.ComputeClassMinCapacityDefragOnlyFakePodsEnabledFlag: false},
			wantNodeCount:      minCapacity + 1,
			wantSystemNodeKept: true,
		},
		{
			// Shows that scale-to-zero runs in this setup.
			name:            "min capacity disabled",
			experimentFlags: map[string]bool{experiments.ComputeClassMinCapacityEnabledFlag: false},
			wantNodeCount:   0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cc := ccc.NewComputeClassBuilder(cccName).
				WithTargetNodeCount(ptr.To(minCapacity)).
				AddPriority(v1.Priority{MachineType: ptr.To(machineType)}).
				Build()
			testConfig := integration.NewTestConfig().
				WithNodePools(
					integration.DefaultNodePool(
						integration.WithNodePoolName(systemPool),
						integration.WithNodePoolMachineType(machineType),
						integration.WithNodePoolSize(1),
						integration.WithNodePoolMin(0),
						integration.WithNodePoolLocations(zone),
						integration.WithNodePoolAutoscalingEnabled(true),
					),
					integration.DefaultNodePool(
						integration.WithNodePoolName("ccc-pool"),
						integration.WithNodePoolMachineType(machineType),
						integration.WithNodePoolSize(minCapacity),
						integration.WithNodePoolMin(0),
						integration.WithNodePoolMax(minCapacity),
						integration.WithNodePoolLocations(zone),
						integration.WithNodePoolLabels(map[string]string{gke_labels.ComputeClassLabel: cccName}),
						integration.WithNodePoolAutoscalingEnabled(true),
					),
				).
				WithCccCrds(cc).
				WithOverrides(
					integration.WithComputeClassMinCapacityEnabled(),
					integration.WithClusterScaleToZeroEnabled(0),
					integration.WithScaleDownUnneededTime(unneededTime),
				).
				WithExperimentOverrides(
					tc.experimentFlags,
					map[string]string{},
				)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer integration_synctest.TearDown(cancel)
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				if err != nil {
					t.Fatalf("SetupAutoscaler() failed: %v", err)
				}

				systemNode := mustGetPoolNode(ctx, t, infra, systemPool)
				systemPod := tu.BuildTestPod("kube-dns", 100, 100)
				systemPod.Namespace = "kube-system"
				tu.SetRSPodSpec(systemPod, "kube-dns")
				systemPod.Spec.NodeName = systemNode
				if _, err := infra.Fakes.KubeClient.CoreV1().Pods(systemPod.Namespace).Create(ctx, systemPod, metav1.CreateOptions{}); err != nil {
					t.Fatalf("Create(%q) failed: %v", systemPod.Name, err)
				}

				for range 3 {
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, unneededTime)
					time.Sleep(5 * time.Second)
				}

				if got := countNodes(ctx, t, infra); got != tc.wantNodeCount {
					t.Errorf("countNodes() = %d, want %d", got, tc.wantNodeCount)
				}
				_, err = infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, systemNode, metav1.GetOptions{})
				if gotKept := err == nil; gotKept != tc.wantSystemNodeKept {
					t.Errorf("system node %q kept = %t (Get() error: %v), want %t", systemNode, gotKept, err, tc.wantSystemNodeKept)
				}
			})
		})
	}
}

// mustGetPoolNode returns the name of the only node of the given node pool.
func mustGetPoolNode(ctx context.Context, t *testing.T, infra *integration.TestInfrastructure, pool string) string {
	t.Helper()
	nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Nodes().List() failed: %v", err)
	}
	var names []string
	for _, node := range nodes.Items {
		if node.Labels[gke_labels.GkeNodePoolLabel] == pool {
			names = append(names, node.Name)
		}
	}
	if len(names) != 1 {
		t.Fatalf("nodes of pool %q = %v, want exactly one", pool, names)
	}
	return names[0]
}
