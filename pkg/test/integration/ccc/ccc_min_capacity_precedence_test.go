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
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

func TestCCCPendingPodScaleUpPrecedenceOverMinCapacity(t *testing.T) {
	// CCC-1: For pod, no minimum capacity.
	ccc1Name := "ccc-1"
	ccc1 := ccc.NewComputeClassBuilder(ccc1Name).
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{MachineType: ptr.To("n1-standard-2")}).
		Build()

	// CCC-0: For minimum capacity, target node count 1.
	ccc0Name := "ccc-0"
	ccc0 := ccc.NewComputeClassBuilder(ccc0Name).
		WithNapEnabled().
		WithTargetNodeCount(ptr.To(1)).
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{MachineType: ptr.To("n1-standard-2")}).
		Build()

	// CCC-2: For pod, no minimum capacity.
	ccc2Name := "ccc-2"
	ccc2 := ccc.NewComputeClassBuilder(ccc2Name).
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{MachineType: ptr.To("n1-standard-2")}).
		Build()

	// CCC-3: For pod, no minimum capacity.
	ccc3Name := "ccc-3"
	ccc3 := ccc.NewComputeClassBuilder(ccc3Name).
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{MachineType: ptr.To("n1-standard-2")}).
		Build()

	// CCC-4: For pod, no minimum capacity.
	ccc4Name := "ccc-4"
	ccc4 := ccc.NewComputeClassBuilder(ccc4Name).
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{MachineType: ptr.To("n1-standard-2")}).
		Build()

	// Create a test pod targeting CCC-1
	p1 := tu.BuildTestPod("pod-ccc-1", 1000, 1000,
		tu.MarkUnschedulable(),
		pod.WithCCC(ccc1Name),
	)
	p2 := tu.BuildTestPod("pod-ccc-2", 1000, 1000,
		tu.MarkUnschedulable(),
		pod.WithCCC(ccc2Name),
	)
	p3 := tu.BuildTestPod("pod-ccc-3", 1000, 1000,
		tu.MarkUnschedulable(),
		pod.WithCCC(ccc3Name),
	)
	p4 := tu.BuildTestPod("pod-ccc-4", 1000, 1000,
		tu.MarkUnschedulable(),
		pod.WithCCC(ccc4Name),
	)

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			integration.WithComputeClassMinCapacityEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
		).
		WithCccCrds(ccc0, ccc1, ccc2, ccc3, ccc4)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Add pod targeting ccc-1, ccc-2, ccc-3, ccc-4
		infra.Fakes.K8s.AddPod(p1)
		infra.Fakes.K8s.AddPod(p2)
		infra.Fakes.K8s.AddPod(p3)
		infra.Fakes.K8s.AddPod(p4)

		// Run the autoscaler loop 4 times to satisfy the 4 pending pods
		for i := 1; i <= 4; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		}

		// Verify that exactly 4 nodes are created, one for each pending pod
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 4, len(nodes.Items), "Expected exactly 4 nodes after 4 loops (one for each pending pod)")

		createdCCCs := make(map[string]bool)
		for _, node := range nodes.Items {
			createdCCCs[node.Labels["cloud.google.com/compute-class"]] = true
		}

		assert.True(t, createdCCCs[ccc1Name], "ccc-1 node should be created")
		assert.True(t, createdCCCs[ccc2Name], "ccc-2 node should be created")
		assert.True(t, createdCCCs[ccc3Name], "ccc-3 node should be created")
		assert.True(t, createdCCCs[ccc4Name], "ccc-4 node should be created")
		assert.False(t, createdCCCs[ccc0Name], "ccc-0 minimum capacity node should NOT be created while pending pods remain")

		// Run the autoscaler loop one more time to process the minimum capacity
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Verify that now 5 nodes are present, including the minimum capacity node for ccc-0
		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 5, len(nodes.Items), "Expected 5 nodes after minimum capacity loop")

		createdCCCs = make(map[string]bool)
		for _, node := range nodes.Items {
			createdCCCs[node.Labels["cloud.google.com/compute-class"]] = true
		}
		assert.True(t, createdCCCs[ccc0Name], "ccc-0 minimum capacity node should be created after all pending pods are satisfied")
	})
}
