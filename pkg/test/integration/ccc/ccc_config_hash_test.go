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

	cccv1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	ccc_builder "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	integration_pod "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestComputeClassConfigHashLabelInjection(t *testing.T) {
	cccName := "hash-ccc"

	testCases := []struct {
		name               string
		disableHashFlag    bool
		disableVersionFlag bool
		disableFeature     bool
		expectHash         bool
	}{
		{
			name:       "Default (Experiments enabled, Feature enabled) - hash label injected",
			expectHash: true,
		},
		{
			name:            "ComputeClassConfigHashEnabledFlag disabled - hash label NOT injected",
			disableHashFlag: true,
			expectHash:      false,
		},
		{
			name:               "ComputeClassConfigHashMinCAVersionFlag disabled - hash label NOT injected",
			disableVersionFlag: true,
			expectHash:         false,
		},
		{
			name:           "WithComputeClassConfigHashEnabled not enabled - hash label NOT injected",
			disableFeature: true,
			expectHash:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithCccNodeAutoprovisioningEnabled(),
				)

			if !tc.disableFeature {
				testConfig = testConfig.WithOverrides(integration.WithComputeClassConfigHashEnabled())
			}

			testConfig = testConfig.WithClusterOverrides(
				integration.WithClusterAutoProvisioningEnabled(),
				integration.WithAutoprovisioningLocations("us-central1-a"),
			).
				WithCccCrds(
					ccc_builder.NewComputeClassBuilder(cccName).
						WithNapEnabled().
						WithNodePoolConfig(&cccv1.NodePoolConfig{
							ServiceAccount: "test-sa@example.com",
						}).
						WithPriorities(
							cccv1.Priority{
								MachineFamily: ptr.To("n2"),
							},
						).
						Build(),
				)

			boolFlags := make(map[string]bool)
			stringFlags := make(map[string]string)

			if tc.disableHashFlag {
				boolFlags[experiments.ComputeClassConfigHashEnabledFlag] = false
			}
			if tc.disableVersionFlag {
				stringFlags[experiments.ComputeClassConfigHashMinCAVersionFlag] = "999.999.999"
			}

			if len(boolFlags) > 0 || len(stringFlags) > 0 {
				testConfig = testConfig.WithExperimentOverrides(boolFlags, stringFlags)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Create a pod targeting the CCC
				pod := tu.BuildTestPod("my-pod", 1000, 1000, tu.MarkUnschedulable(), integration_pod.WithCCC(cccName))
				infra.Fakes.K8s.AddPod(pod)

				// Run autoscaler loop
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

				// Verify that a node pool was created and labeled with the hash
				nodeList, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
				assert.NoError(t, err)

				assert.NotEmpty(t, nodeList.Items, "No nodes found in the cluster")

				foundHash := false
				for _, node := range nodeList.Items {
					if hash, ok := node.Labels[gkelabels.ComputeClassConfigHashLabel]; ok {
						assert.NotEmpty(t, hash)
						foundHash = true
						break
					}
				}
				if tc.expectHash {
					assert.True(t, foundHash, "ComputeClassConfigHashLabel not found on any node")
				} else {
					assert.False(t, foundHash, "ComputeClassConfigHashLabel found on a node but should be disabled")
				}
			})
		})
	}
}

func TestComputeClassConfigHashStatusSurfacing(t *testing.T) {
	cccName := "hash-status-ccc"

	testCases := []struct {
		name               string
		disableHashFlag    bool
		disableVersionFlag bool
		disableFeature     bool
		expectHash         bool
	}{
		{
			name:       "Default (Experiments enabled, Feature enabled) - hash surfaced",
			expectHash: true,
		},
		{
			name:            "ComputeClassConfigHashEnabledFlag disabled - hash NOT surfaced",
			disableHashFlag: true,
			expectHash:      false,
		},
		{
			name:               "ComputeClassConfigHashMinCAVersionFlag disabled - hash NOT surfaced",
			disableVersionFlag: true,
			expectHash:         false,
		},
		{
			name:           "Feature not enabled - hash NOT surfaced",
			disableFeature: true,
			expectHash:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testConfig := integration.NewTestConfig().
				WithOverrides(
					integration.WithAutoProvisioningEnabled(),
					integration.WithCccNodeAutoprovisioningEnabled(),
					integration.WithEnhancedCrdStatusReportingEnabled(),
				)

			if !tc.disableFeature {
				testConfig = testConfig.WithOverrides(integration.WithComputeClassConfigHashEnabled())
			}

			testConfig = testConfig.WithClusterOverrides(
				integration.WithClusterAutoProvisioningEnabled(),
				integration.WithAutoprovisioningLocations("us-central1-a"),
			).
				WithCccCrds(
					ccc_builder.NewComputeClassBuilder(cccName).
						WithNapEnabled().
						WithNodePoolConfig(&cccv1.NodePoolConfig{
							ServiceAccount: "test-sa@example.com",
						}).
						WithPriorities(
							cccv1.Priority{
								MachineFamily: ptr.To("n2"),
							},
						).
						Build(),
				)

			boolFlags := make(map[string]bool)
			stringFlags := make(map[string]string)

			if tc.disableHashFlag {
				boolFlags[experiments.ComputeClassConfigHashEnabledFlag] = false
			}
			if tc.disableVersionFlag {
				stringFlags[experiments.ComputeClassConfigHashMinCAVersionFlag] = "999.999.999"
			}

			if len(boolFlags) > 0 || len(stringFlags) > 0 {
				testConfig = testConfig.WithExperimentOverrides(boolFlags, stringFlags)
			}

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				// Run autoscaler loop to trigger status population.
				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				// Wait for Validator (60s) and Aggregator to flush (30s)
				time.Sleep(100 * time.Second)

				// Fetch the ComputeClass from fake client
				cccObj, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
				assert.NoError(t, err)

				if tc.expectHash {
					assert.NotEmpty(t, cccObj.Status.PriorityStatuses, "PriorityStatuses should be populated")

					var initialHash string
					foundHash := false
					for _, ps := range cccObj.Status.PriorityStatuses {
						if ps.Identifier == "0" {
							assert.NotEmpty(t, ps.ConfigHash, "ConfigHash should be populated for priority 0")
							initialHash = ps.ConfigHash
							foundHash = true
							break
						}
					}
					assert.True(t, foundHash, "PriorityStatus for priority 0 not found")

					cccCopy := cccObj.DeepCopy()
					cccCopy.Spec.NodePoolConfig.ServiceAccount = "new-sa@example.com"
					cccCopy.ResourceVersion = "2" // Manually increment for fake client cache
					_, err = infra.Fakes.CccClient.CloudV1().ComputeClasses().Update(ctx, cccCopy, metav1.UpdateOptions{})
					assert.NoError(t, err)

					// Verify update in fake client immediately
					readBack, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
					assert.NoError(t, err)
					assert.Equal(t, "new-sa@example.com", readBack.Spec.NodePoolConfig.ServiceAccount, "Fake client should have updated SA")

					// Run autoscaler loop again
					integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
					// Wait for Validator and Aggregator to flush
					time.Sleep(100 * time.Second)

					// Fetch updated CCC
					updatedCccObj, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
					assert.NoError(t, err)

					// Verify hash changed
					foundUpdatedHash := false
					for _, ps := range updatedCccObj.Status.PriorityStatuses {
						if ps.Identifier == "0" {
							assert.NotEmpty(t, ps.ConfigHash, "ConfigHash should be populated for priority 0")
							assert.NotEqual(t, initialHash, ps.ConfigHash, "ConfigHash should have changed after spec update")
							foundUpdatedHash = true
							break
						}
					}
					assert.True(t, foundUpdatedHash, "PriorityStatus for priority 0 not found after update")
				} else {
					// Verify NO hash in status
					for _, ps := range cccObj.Status.PriorityStatuses {
						assert.Empty(t, ps.ConfigHash, "ConfigHash should be empty when feature is disabled")
					}
				}
			})
		})
	}
}

func TestComputeClassConfigHashMatching(t *testing.T) {
	cccName := "matching-hash-ccc"

	testConfig := integration.NewTestConfig().
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			integration.WithCccNodeAutoprovisioningEnabled(),
			integration.WithEnhancedCrdStatusReportingEnabled(),
			integration.WithComputeClassConfigHashEnabled(),
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithAutoprovisioningLocations("us-central1-a"),
		).
		WithCccCrds(
			ccc_builder.NewComputeClassBuilder(cccName).
				WithNapEnabled().
				WithNodePoolConfig(&cccv1.NodePoolConfig{
					ServiceAccount: "test-sa@example.com",
				}).
				WithPriorities(
					cccv1.Priority{
						MachineFamily: ptr.To("n2"),
					},
				).
				Build(),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		// Create a pod targeting the CCC
		pod := tu.BuildTestPod("my-pod", 1000, 1000, tu.MarkUnschedulable(), integration_pod.WithCCC(cccName))
		infra.Fakes.K8s.AddPod(pod)

		// Run autoscaler loop to create node
		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)

		// Verify node created and get its hash
		nodeList, err := infra.Fakes.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, nodeList.Items, "No nodes found in the cluster")

		var nodeHash string
		foundNode := false

		for _, node := range nodeList.Items {
			if hash, ok := node.Labels[gkelabels.ComputeClassConfigHashLabel]; ok {
				nodeHash = hash
				foundNode = true
				break
			}
		}

		assert.True(t, foundNode, "Node with ConfigHash label not found")
		assert.NotEmpty(t, nodeHash, "Node hash is empty")

		// Wait for Validator and Aggregator to flush to update CCC status
		time.Sleep(100 * time.Second)

		// Fetch the ComputeClass
		cccObj, err := infra.Fakes.CccClient.CloudV1().ComputeClasses().Get(ctx, cccName, metav1.GetOptions{})
		assert.NoError(t, err)

		// Find PriorityStatus for priority 0
		foundStatus := false
		var statusHash string
		for _, ps := range cccObj.Status.PriorityStatuses {
			if ps.Identifier == "0" {
				statusHash = ps.ConfigHash
				foundStatus = true
				break
			}
		}

		assert.True(t, foundStatus, "PriorityStatus for priority 0 not found in CCC")
		assert.NotEmpty(t, statusHash, "CCC status hash is empty")

		// Compare hashes
		assert.Equal(t, nodeHash, statusHash, "Node hash should match CCC status hash")
	})
}
