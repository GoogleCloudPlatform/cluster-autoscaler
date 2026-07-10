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

package ccc

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/utils/ptr"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

// TestCCCScaleUpPriorityScore verifies that workloads are scaled up on the highest priorityScore rules.
func TestCCCScaleUpPriorityScore(t *testing.T) {
	testCases := []struct {
		name             string
		ccc              *v1.ComputeClass
		wantFromFamilies []string
	}{
		{
			name: "Prefer_higher_priorityScore",
			ccc: NewComputeClassBuilder("scaleup-priorityscore-prefer-higher").
				WithNodePoolAutoCreation(true).
				WithPriorities(
					v1.Priority{
						MachineFamily: ptr.To("e2"),
						PriorityScore: ptr.To(5),
					},
					v1.Priority{
						MachineFamily: ptr.To("n2"),
						PriorityScore: ptr.To(10),
					},
				).
				Build(),
			wantFromFamilies: []string{"n2"},
		},
		{
			name: "Equal_highest_priorityScore_any_of_allowed",
			ccc: NewComputeClassBuilder("scaleup-priorityscore-equal").
				WithNodePoolAutoCreation(true).
				WithPriorities(
					v1.Priority{
						MachineFamily: ptr.To("e2"),
						PriorityScore: ptr.To(10),
					},
					v1.Priority{
						MachineFamily: ptr.To("n2"),
						PriorityScore: ptr.To(20),
					},
					v1.Priority{
						MachineFamily: ptr.To("n1"),
						PriorityScore: ptr.To(20),
					},
				).
				Build(),
			wantFromFamilies: []string{"n2", "n1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithOverrides(integration.WithAutoProvisioningEnabled()).
				WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
				WithCccCrds(tc.ccc)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// When: Add an unschedulable pod requesting this compute class
				p := tu.BuildTestPod("test-pod", 3000, 10000, tu.MarkUnschedulable(), pod.WithCCC(tc.ccc.Name))
				infra.Fakes.K8s.AddPod(p)

				// Run autoscaler loop
				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

				// Then: Verify a NAP node pool was created with the expected machine family
				cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
				assert.NoError(t, err)

				assert.Equal(t, 1, len(cluster.NodePools), "Expected exactly 1 NAP node pool to be created")
				np := cluster.NodePools[0]
				assert.NotNil(t, np.Config, "Expected node pool config to be non-nil")
				assert.NotEmpty(t, np.Config.MachineType, "Expected non-empty MachineType")

				prefix := strings.Split(np.Config.MachineType, "-")[0]
				assert.True(t, slices.Contains(tc.wantFromFamilies, prefix),
					"Scale-up got machine family: %s (from %s), want one of: %v", prefix, np.Config.MachineType, tc.wantFromFamilies)
			})
		})
	}
}

// TestCCCDefragPriorityScore tests High Priority Migration (Defrag) behavior with PriorityScore.
func TestCCCDefragPriorityScore(t *testing.T) {
	baseAdd := NewComputeClassBuilder("defrag-priorityscore-add").
		WithNodePoolAutoCreation(true).
		WithActiveMigration(true).
		WithPriorities(
			v1.Priority{
				MachineFamily: ptr.To("e2"),
				PriorityScore: ptr.To(10),
			},
		)

	baseChange := NewComputeClassBuilder("defrag-priorityscore-change").
		WithNodePoolAutoCreation(true).
		WithActiveMigration(true)

	testCases := []struct {
		name                     string
		initialCCC               *v1.ComputeClass
		updatedCCC               *v1.ComputeClass
		wantInitialMachineFamily string
		wantUpdatedMachineFamily string
	}{
		{
			name:       "Migrate_to_newly_added_higher_priorityScore",
			initialCCC: baseAdd.Build(),
			updatedCCC: baseAdd.Clone().AddPriority(
				v1.Priority{
					MachineFamily: ptr.To("n2"),
					PriorityScore: ptr.To(20), // New higher priorityScore.
				},
			).Build(),
			wantInitialMachineFamily: "e2",
			wantUpdatedMachineFamily: "n2",
		},
		{
			name: "Migrate_to_existing_but_previously_not_highest_priorityScore",
			initialCCC: baseChange.Clone().WithPriorities(
				v1.Priority{
					MachineFamily: ptr.To("e2"),
					PriorityScore: ptr.To(20),
				},
				v1.Priority{
					MachineFamily: ptr.To("n2"),
					PriorityScore: ptr.To(10),
				},
			).Build(),
			updatedCCC: baseChange.Clone().WithPriorities(
				v1.Priority{
					MachineFamily: ptr.To("e2"),
					PriorityScore: ptr.To(20),
				},
				v1.Priority{
					MachineFamily: ptr.To("n2"),
					PriorityScore: ptr.To(30), // n2 now higher score.
				},
			).Build(),
			wantInitialMachineFamily: "e2",
			wantUpdatedMachineFamily: "n2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithDefragEnabled("high-priority-migration"),
					integration.WithScaleDownUtilizationThreshold(1.0),
					integration.WithScaleDownUnneededTime(time.Nanosecond),
				).
				WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
				WithCccCrds(tc.initialCCC)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Given: An initial ComputeClass and unschedulable pods belonging to a single ReplicaSet.
				rsName := "test-rs"
				_, err = infra.Fakes.KubeClient.AppsV1().ReplicaSets("default").Create(ctx, &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{Name: rsName, Namespace: "default"},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr.To[int32](2),
					},
				}, metav1.CreateOptions{})
				assert.NoError(t, err)

				for i := 0; i < 2; i++ {
					podName := fmt.Sprintf("pod-%d", i)
					p := tu.BuildTestPod(podName, 3000, 10000,
						tu.MarkUnschedulable(),
						pod.WithCCC(tc.initialCCC.Name),
						pod.WithOwnerReplicaSet(rsName),
					)
					infra.Fakes.K8s.AddPod(p)
				}

				// When: The autoscaler runs, triggering initial scale-up.
				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

				// Then: Verify the initial scale-up created a node pool on wantInitialMachineFamily.
				cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
				assert.NoError(t, err)
				assert.Equal(t, 1, len(cluster.NodePools), "Initial scale-up: expected exactly 1 node pool")
				assert.True(t, strings.HasPrefix(cluster.NodePools[0].Config.MachineType, tc.wantInitialMachineFamily),
					"Expected initial pool on %s, got %s", tc.wantInitialMachineFamily, cluster.NodePools[0].Config.MachineType)

				// Advance time by more than DefragMaxDelay (5 minutes) so defrag considers the nodes stable enough to migrate.
				integration_synctest.MustRunOnceAfter(t, autoscaler, 5*time.Minute+time.Second)

				// Schedule pods onto the authentic registered nodes (nodes now automatically ready and versioned via GCE fake infra).
				infra.Fakes.RunScheduler(ctx, t)

				// When: The ComputeClass is updated to have a higher priorityScore rule.
				tc.updatedCCC.ResourceVersion = "2"
				_, err = infra.Fakes.CccClient.CloudV1().ComputeClasses().Update(ctx, tc.updatedCCC, metav1.UpdateOptions{})
				assert.NoError(t, err)

				// When: The autoscaler runs again, allowing defrag to evaluate migration to the new higher priority pool.
				integration_synctest.MustRunOnceAfter(t, autoscaler, 15*time.Minute)
				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

				// Then: Verify defrag triggered scale-up of a replacement pool on wantUpdatedMachineFamily.
				cluster, err = infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
				assert.NoError(t, err)

				var foundUpdated bool
				for _, np := range cluster.NodePools {
					if strings.HasPrefix(np.Config.MachineType, tc.wantUpdatedMachineFamily) {
						foundUpdated = true
						break
					}
				}
				assert.True(t, foundUpdated, "Defrag failed: expected to find a new pool on machine family %s", tc.wantUpdatedMachineFamily)
			})
		})
	}
}
