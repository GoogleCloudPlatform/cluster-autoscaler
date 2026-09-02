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
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

func newCCCNodePool(poolName, cccName string) integration.NodePoolBuilder {
	return integration.EmptyNodePool(poolName).
		WithCCCLabel(cccName).
		WithCCCTaint(cccName).
		WithMax(10)
}

// TestCCCSchedulingWithoutNodeSelector verifies workload scheduling behaviors for Custom ComputeClasses
// corresponding to the scenarios in b/528305042#comment21.
func TestCCCSchedulingWithoutNodeSelector(t *testing.T) {
	const cccName = "my-ccc-1"
	const missingCCC = "non-existing-ccc"

	computeClass := ccc.NewComputeClassBuilder(cccName).
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{Nodepools: []string{"ccc-pool-1"}}).
		Build()

	cccNodePool := newCCCNodePool("ccc-pool-1", cccName).
		WithLabel("role", "role-1").
		Build()
	cccNodePool2 := newCCCNodePool("ccc-pool-2", missingCCC).Build()
	nonCCCNodePool := integration.EmptyNodePool("non-ccc-pool").
		WithMax(10).
		Build()

	testCases := []struct {
		name                         string
		pod                          *apiv1.Pod
		extraPool                    *gke_api_beta.NodePool
		expectPodScheduledOnNodePool string
	}{
		{
			// we expect CA to NOT schedule
			name: "Workload without node-selector and toleration",
			pod: tu.BuildTestPod("pod", 1000, 1000,
				tu.MarkUnschedulable(),
			),
			expectPodScheduledOnNodePool: "",
		},
		{
			// we expect CA to NOT schedule
			name: "Workload without CCC node-selector and toleration, but with non-CCC node-selector",
			pod: tu.BuildTestPod("pod", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithNodeSelector(map[string]string{
					"role": "role-1",
				}),
			),
			expectPodScheduledOnNodePool: "",
		},
		{
			// we expect CA to schedule
			name: "Workload with CCC node-selector, but missing toleration",
			pod: tu.BuildTestPod("pod", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithNodeSelector(map[string]string{
					labels.ComputeClassLabel: cccName,
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			// we expect CA to schedule
			name: "Workload without CCC node-selector, but with non-CCC node-selector and broad CCC toleration",
			pod: tu.BuildTestPod("pod", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithNodeSelector(map[string]string{
					"role": "role-1",
				}),
				pod.WithTolerations(apiv1.Toleration{
					Key:      labels.ComputeClassLabel,
					Operator: apiv1.TolerationOpExists,
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			name: "Workload with broad CCC key toleration and no nodeSelector",
			pod: tu.BuildTestPod("pod-broad-ccc-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Key:      labels.ComputeClassLabel,
					Operator: apiv1.TolerationOpExists,
					Effect:   apiv1.TaintEffectNoSchedule,
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			name: "Workload with specific CCC toleration and no nodeSelector",
			pod: tu.BuildTestPod("pod-specific-ccc-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Key:      labels.ComputeClassLabel,
					Value:    cccName,
					Operator: apiv1.TolerationOpEqual,
					Effect:   apiv1.TaintEffectNoSchedule,
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			name: "Workload with wildcard toleration and no nodeSelector",
			pod: tu.BuildTestPod("pod-wildcard-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Operator: apiv1.TolerationOpExists,
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			// we expect CA to schedule
			name: "Workload with specific CCC toleration, but CCC does not exists",
			pod: tu.BuildTestPod("pod-specific-ccc-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Key:      labels.ComputeClassLabel,
					Value:    missingCCC,
					Operator: apiv1.TolerationOpEqual,
					Effect:   apiv1.TaintEffectNoSchedule,
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool2.Name,
			extraPool:                    cccNodePool2,
		},
		{
			// we expect CA to schedule
			name: "Workload without node-selector, but with missing operator in toleration",
			pod: tu.BuildTestPod("pod", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Key:    labels.ComputeClassLabel,
					Value:  cccName,
					Effect: apiv1.TaintEffectNoSchedule,
					// missing Operator should default to apiv1.TolerationOpEqual
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			// we expect CA to schedule
			name: "Workload without node-selector, but with missing effect in toleration",
			pod: tu.BuildTestPod("pod", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Key:      labels.ComputeClassLabel,
					Value:    cccName,
					Operator: apiv1.TolerationOpEqual,
					// missing effect matches all effects
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			// we expect CA to schedule
			name: "Workload without node-selector, but with unsupported operator in toleration",
			pod: tu.BuildTestPod("pod", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Key:      labels.ComputeClassLabel,
					Value:    cccName,
					Operator: apiv1.TolerationOpGt,
					// missing effect matches all effects
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			name: "Workload with broad CCC key toleration and no nodeSelector",
			pod: tu.BuildTestPod("pod-broad-ccc-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithTolerations(apiv1.Toleration{
					Key:      labels.ComputeClassLabel,
					Operator: apiv1.TolerationOpExists,
					// no effect should match all effects
				}),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			name: "Workload with CCC toleration and nodeSelector",
			pod: tu.BuildTestPod("pod-ccc-selector-and-toleration", 1000, 1000,
				tu.MarkUnschedulable(),
				pod.WithCCC(cccName),
			),
			expectPodScheduledOnNodePool: cccNodePool.Name,
		},
		{
			name: "System pod without CCC toleration on cluster with non-CCC nodepool",
			pod: func() *apiv1.Pod {
				p := tu.BuildTestPod("system-pod-no-ccc", 1000, 1000, tu.MarkUnschedulable())
				p.Namespace = "kube-system"
				return p
			}(),
			extraPool:                    nonCCCNodePool,
			expectPodScheduledOnNodePool: nonCCCNodePool.Name,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodePools := []*gke_api_beta.NodePool{cccNodePool}
			if tc.extraPool != nil {
				nodePools = append(nodePools, tc.extraPool)
			}

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
				if tc.expectPodScheduledOnNodePool != "" {
					assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod %s to be scheduled", tc.pod.Name)
					node, err := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, updatedPod.Spec.NodeName, metav1.GetOptions{})
					assert.NoError(t, err)
					assert.Contains(t, node.Name, tc.expectPodScheduledOnNodePool)
				} else {
					assert.Empty(t, updatedPod.Spec.NodeName)
				}
			})
		})
	}
}

// TestCCCNodePoolPreferNoSchedule verifies that workloads can schedule on
// CCC NodePools even without a corresponding toleration, provided the CCC taint
// has a PREFER_NO_SCHEDULE effect.
func TestCCCNodePoolPreferNoSchedule(t *testing.T) {
	pod := tu.BuildTestPod("pod", 1000, 1000,
		tu.MarkUnschedulable(),
		pod.WithNodeSelector(map[string]string{
			"role": "role-1",
		}),
	)
	const cccName = "my-ccc-1"

	computeClass := ccc.NewComputeClassBuilder(cccName).
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{Nodepools: []string{"ccc-pool-1"}}).
		Build()

	cccNodePool := integration.EmptyNodePool("ccc-pool-1").
		WithCCCLabel(cccName).
		WithLabel("role", "role-1").
		WithTaints(&gke_api_beta.NodeTaint{
			Key:    labels.ComputeClassLabel,
			Value:  cccName,
			Effect: "PREFER_NO_SCHEDULE",
		}).
		WithMax(10).
		Build()
	testConfig := integration.NewTestConfig().
		WithNodePools(cccNodePool).
		WithCccCrds(computeClass)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		infra.Fakes.K8s.AddPod(pod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod %s to be scheduled", pod.Name)
	})
}

// TestCCCNodePoolNoTaint verifies that a non-CCC workload without node-selector and toleration
// schedules on a CCC NodePool if the NodePool has no taint and there is nothing else.
func TestCCCNodePoolNoTaint(t *testing.T) {
	pod := tu.BuildTestPod("pod", 1000, 1000,
		tu.MarkUnschedulable(),
	)
	const cccName = "my-ccc-1"

	computeClass := ccc.NewComputeClassBuilder(cccName).
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{Nodepools: []string{"ccc-pool-1"}}).
		Build()

	cccNodePool := integration.EmptyNodePool("ccc-pool-1").
		WithCCCLabel(cccName).
		WithMax(10).
		Build()
	testConfig := integration.NewTestConfig().
		WithNodePools(cccNodePool).
		WithCccCrds(computeClass)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)
		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		assert.NoError(t, err)
		defer integration_synctest.TearDown(cancel)

		infra.Fakes.K8s.AddPod(pod)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		infra.Fakes.RunScheduler(ctx, t)
		updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod %s to be scheduled", pod.Name)
	})
}

// TestCCCDefaultFallbackWithCustomToleration verifies scheduling behavior when Default Compute Class is enabled
// and a workload tolerates a custom Compute Class without explicitly requesting it via compute-class nodeSelector.
// - When no default node pool exists, it falls back to the custom Compute Class node pool.
// - When a default node pool exists but does not match the workload's nodeSelector, it falls back to the custom Compute Class node pool.
// - When a matching default node pool exists, it schedules on the default node pool instead of the custom Compute Class node pool.
func TestCCCDefaultFallbackWithCustomToleration(t *testing.T) {
	const customCCCName = "my-custom-class"

	testCases := []struct {
		name             string
		defaultCC        *v1.ComputeClass
		defaultPool      *gke_api_beta.NodePool
		podNodeSelector  map[string]string
		podTolerations   []apiv1.Toleration
		expectedPoolName string
	}{
		{
			name: "without default node pools",
			defaultCC: ccc.NewComputeClassBuilder("default").
				WithNapEnabled().
				WithWhenUnsatisfiable("ScaleUpAnyway").
				Build(),
			defaultPool:     nil,
			podNodeSelector: map[string]string{"role": "custom-role"},
			podTolerations: []apiv1.Toleration{
				{
					Key:      labels.ComputeClassLabel,
					Value:    customCCCName,
					Operator: apiv1.TolerationOpEqual,
					Effect:   apiv1.TaintEffectNoSchedule,
				},
			},
			expectedPoolName: "custom-pool",
		},
		{
			name: "with non-matching (missing custom label) default node pool",
			defaultCC: ccc.NewComputeClassBuilder("default").
				WithNapEnabled().
				WithWhenUnsatisfiable("ScaleUpAnyway").
				WithPriorities(v1.Priority{Nodepools: []string{"default-pool"}}).
				Build(),
			defaultPool:     newCCCNodePool("default-pool", "default").Build(),
			podNodeSelector: map[string]string{"role": "custom-role"},
			podTolerations: []apiv1.Toleration{
				{
					Key:      labels.ComputeClassLabel,
					Value:    customCCCName,
					Operator: apiv1.TolerationOpEqual,
					Effect:   apiv1.TaintEffectNoSchedule,
				},
			},
			expectedPoolName: "custom-pool",
		},
		{
			name: "with matching default node pool",
			defaultCC: ccc.NewComputeClassBuilder("default").
				WithNapEnabled().
				WithWhenUnsatisfiable("ScaleUpAnyway").
				WithPriorities(v1.Priority{Nodepools: []string{"default-pool"}}).
				Build(),
			defaultPool:      integration.EmptyNodePool("default-pool").WithMax(10).Build(),
			podNodeSelector:  nil,
			podTolerations:   nil,
			expectedPoolName: "default-pool",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			customComputeClass := ccc.NewComputeClassBuilder(customCCCName).
				WithNapEnabled().
				WithWhenUnsatisfiable("ScaleUpAnyway").
				WithPriorities(v1.Priority{Nodepools: []string{"custom-pool"}}).
				Build()

			customNodePool := newCCCNodePool("custom-pool", customCCCName).
				WithLabel("role", "custom-role").
				Build()

			podOptions := []func(*apiv1.Pod){
				tu.MarkUnschedulable(),
			}
			if len(tc.podTolerations) > 0 {
				podOptions = append(podOptions, pod.WithTolerations(tc.podTolerations...))
			}
			if len(tc.podNodeSelector) > 0 {
				podOptions = append(podOptions, pod.WithNodeSelector(tc.podNodeSelector))
			}

			testPod := tu.BuildTestPod("pod-test", 1000, 1000, podOptions...)

			var nodePools []*gke_api_beta.NodePool
			if tc.defaultPool != nil {
				nodePools = append(nodePools, tc.defaultPool)
			}
			nodePools = append(nodePools, customNodePool)

			testConfig := integration.NewTestConfig().
				WithNodePools(nodePools...).
				WithCccCrds(tc.defaultCC, customComputeClass).
				WithDefaultComputeClassEnabled()

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				defer integration_synctest.TearDown(cancel)

				infra.Fakes.K8s.AddPod(testPod)

				integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
				infra.Fakes.RunScheduler(ctx, t)

				updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods(testPod.Namespace).Get(ctx, testPod.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod %s to be scheduled", testPod.Name)
				node, err := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, updatedPod.Spec.NodeName, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Contains(t, node.Name, tc.expectedPoolName)
			})
		})
	}
}
