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
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

func newCCCNodePool(poolName, cccName string) *gke_api_beta.NodePool {
	return integration.DefaultNodePool(
		integration.WithNodePoolName(poolName),
		integration.WithNodePoolMachineType("n1-standard-2"),
		integration.WithNodePoolSize(0),
		integration.WithNodePoolMaxNodeCount(10),
		integration.WithNodePoolLabels(map[string]string{
			labels.ComputeClassLabel: cccName,
		}),
		integration.WithNodePoolTaints(&gke_api_beta.NodeTaint{
			Key:    labels.ComputeClassLabel,
			Value:  cccName,
			Effect: "NO_SCHEDULE",
		}),
	)
}

// TestCCCSchedulingWithoutNodeSelector verifies workload scheduling behaviors for Custom ComputeClasses
// corresponding to the scenarios in b/528305042#comment21.
func TestCCCSchedulingWithoutNodeSelector(t *testing.T) {
	const cccName = "my-ccc"

	computeClass := ccc.NewComputeClassBuilder(cccName).
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{Nodepools: []string{"ccc-pool"}}).
		Build()

	cccNodePool := newCCCNodePool("ccc-pool", cccName)
	nonCCCNodePool := integration.EmptyNodePool("non-ccc-pool").
		WithMachineType("n1-standard-2").
		WithMax(10).
		Build()

	testCases := []struct {
		name                        string
		pod                         *apiv1.Pod
		extraPools                  []*gke_api_beta.NodePool
		expectPodScheduledOnCCCNode bool
	}{
		{
			name: "Scenario 1a: Workload with broad CCC key toleration and no nodeSelector",
			pod: tu.BuildTestPod("pod-broad-ccc-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Key:      labels.ComputeClassLabel,
					Operator: apiv1.TolerationOpExists,
					Effect:   apiv1.TaintEffectNoSchedule,
				}),
			),
			expectPodScheduledOnCCCNode: true,
		},
		{
			name: "Scenario 1b: Workload with specific CCC toleration and no nodeSelector",
			pod: tu.BuildTestPod("pod-specific-ccc-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Key:      labels.ComputeClassLabel,
					Value:    cccName,
					Operator: apiv1.TolerationOpEqual,
					Effect:   apiv1.TaintEffectNoSchedule,
				}),
			),
			expectPodScheduledOnCCCNode: true,
		},
		{
			name: "Scenario 1c: Workload with wildcard toleration and no nodeSelector",
			pod: tu.BuildTestPod("pod-wildcard-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Operator: apiv1.TolerationOpExists,
				}),
			),
			expectPodScheduledOnCCCNode: true,
		},
		{
			name: "Scenario 2: Workload with CCC toleration and nodeSelector",
			pod: tu.BuildTestPod("pod-ccc-selector-and-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithCCC(cccName),
			),
			expectPodScheduledOnCCCNode: true,
		},
		{
			name: "Scenario 3: System pod without CCC toleration on cluster with non-CCC nodepool",
			pod: func() *apiv1.Pod {
				p := tu.BuildTestPod("system-pod-no-ccc", 1000, 1000, tu.MarkUnschedulable())
				p.Namespace = "kube-system"
				return p
			}(),
			extraPools:                  []*gke_api_beta.NodePool{nonCCCNodePool},
			expectPodScheduledOnCCCNode: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodePools := []*gke_api_beta.NodePool{cccNodePool}
			nodePools = append(nodePools, tc.extraPools...)

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithCccCrds(computeClass)

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				infra.Fakes.K8s.AddPod(tc.pod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods(tc.pod.Namespace).Get(ctx, tc.pod.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod %s to be scheduled", tc.pod.Name)

				node, err := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, updatedPod.Spec.NodeName, metav1.GetOptions{})
				assert.NoError(t, err)

				if tc.expectPodScheduledOnCCCNode {
					assert.Equal(t, cccName, node.Labels[labels.ComputeClassLabel],
						"Expected pod %s to be scheduled on a node with compute class %q", tc.pod.Name, cccName)
				} else {
					assert.NotEqual(t, cccName, node.Labels[labels.ComputeClassLabel],
						"Expected pod %s to be scheduled on a non-CCC node pool, got compute class %q", tc.pod.Name, node.Labels[labels.ComputeClassLabel])
				}
			})
		})
	}
}
