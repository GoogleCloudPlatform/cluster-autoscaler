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
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

func stockOutError() cloudprovider.InstanceErrorInfo {
	return cloudprovider.InstanceErrorInfo{
		ErrorClass:   cloudprovider.OutOfResourcesErrorClass,
		ErrorCode:    "ZONE_RESOURCE_POOL_EXHAUSTED",
		ErrorMessage: "GCE API error: stock out",
	}
}

func TestCCCMinCapacityActiveMigration(t *testing.T) {
	cccName := "active-migration-ccc"

	// CC has n2 as optimal (P1, index 0) and e2 as non-optimal (P2, index 1).
	cc := ccc.NewComputeClassBuilder(cccName).
		WithTargetNodeCount(ptr.To(1)).
		AddPriority(v1.Priority{MachineType: ptr.To("n2-standard-2")}).
		AddPriority(v1.Priority{MachineType: ptr.To("e2-standard-2")}).
		WithActiveMigration(true).
		Build()

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithComputeClassMinCapacityEnabled(),
			integration.WithDefragEnabled("high-priority-migration"),
			integration.WithDefragCandidateLimit(10),
			integration.WithMaxDrainParallelism(10),
			integration.WithScaleDownUnneededTime(time.Minute),
			integration.WithScaleDownDelayAfterAdd(time.Minute),
			integration.WithScaleDownUtilizationThreshold(0.5),
		).
		WithNodePools(
			// P0 (Optimal): n2, empty
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-n2"),
				integration.WithNodePoolMachineType("n2-standard-2"),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "n2",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
			// P1 (Fallback): e2, empty
			integration.DefaultNodePool(
				integration.WithNodePoolName("ng-e2"),
				integration.WithNodePoolMachineType("e2-standard-2"),
				integration.WithNodePoolSize(0),
				integration.WithNodePoolLocations("us-central1-a"),
				integration.WithNodePoolLabels(map[string]string{
					labels.ComputeClassLabel:  cccName,
					labels.MachineFamilyLabel: "e2",
				}),
				integration.WithNodePoolMin(0),
				integration.WithNodePoolMax(10),
				integration.WithNodePoolAutoscalingEnabled(true),
			),
		).
		WithCccCrds(cc)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer integration_synctest.TearDown(cancel)

		infra := integration.SetupInfrastructure(ctx, t)

		// 1. Mock GCE stockout error on ng-n2
		infra.Fakes.GceService.SetCreateInstanceForMigError("ng-n2", stockOutError())

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)

		for i := 0; i < 6; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 5*time.Second)
		}

		// Wait for ng-e2 node to be created.
		nodes, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		if !assert.Len(t, nodes.Items, 1, "Expected 1 node after fallback scale-up") {
			t.FailNow()
		}
		assert.Equal(t, "e2", nodes.Items[0].Labels[labels.MachineFamilyLabel], "Node should be from fallback e2 pool")

		// 3. Remove stockout on optimal pool
		infra.Fakes.GceService.ClearCreateInstanceForMigError("ng-n2")
		// we want the backoffs to expire, so we wait 5 minutes.
		time.Sleep(5 * time.Minute)

		// There are 4 iteration to ensure defrag fully happens - new node needs to get created
		// for the scale down to finish
		for i := 0; i < 4; i++ {
			integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, 30*time.Second)
		}

		nodes, err = infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)

		if !assert.Len(t, nodes.Items, 1, "Expected exactly 1 node after active migration") {
			t.FailNow()
		}
		assert.Equal(t, "n2", nodes.Items[0].Labels[labels.MachineFamilyLabel], "Node should be actively migrated to optimal n2 pool")
	})
}
