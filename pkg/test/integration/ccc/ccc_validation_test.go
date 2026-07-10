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
	"testing"
	"testing/synctest"
	"time"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	google_api_container "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
	"k8s.io/utils/ptr"
)

func runCCCWorkloadTest(t *testing.T, name string, cc *v1.ComputeClass, podSpec *apiv1.Pod) {
	t.Run(name, func(t *testing.T) {
		testConfig := integration.NewTestConfig().
			WithOverrides(integration.WithAutoProvisioningEnabled()).
			WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
			WithCccCrds(cc)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			infra := integration.SetupInfrastructure(ctx, t)
			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			assert.NoError(t, err)
			defer integration_synctest.TearDown(cancel)

			infra.Fakes.K8s.AddPod(podSpec)

			integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, podSpec.Name, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.NotEmpty(t, updatedPod.Spec.NodeName, "Expected pod to be scheduled")
		})
	})
}

// TestCCCWorkloadValidation verifies that pods requesting a valid Custom Compute Class
// (across various toleration and node selection patterns) get successfully scheduled.
// Demonstrates that independent workload equivalence groups must be evaluated
// in separate bubbles to prevent scheduling competition during scale-up.
func TestCCCWorkloadValidation(t *testing.T) {
	cc := NewComputeClassBuilder("my-ccc").
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{MachineFamily: ptr.To("n1")}).
		Build()

	runCCCWorkloadTest(t, "No Additional Selectors", cc,
		tu.BuildTestPod("pod-no-selectors", 1000, 1000, tu.MarkUnschedulable(), pod.WithCCC("my-ccc")),
	)

	runCCCWorkloadTest(t, "Toleration for another CCC", cc,
		tu.BuildTestPod("pod-extra-toleration", 1000, 1000, tu.MarkUnschedulable(),
			pod.WithCCC("my-ccc"),
			pod.WithTolerations(apiv1.Toleration{
				Key:      "cloud.google.com/compute-class",
				Value:    "another-cc",
				Operator: apiv1.TolerationOpEqual,
				Effect:   apiv1.TaintEffectNoSchedule,
			}),
		),
	)

	runCCCWorkloadTest(t, "Wild Card Tolerations", cc,
		tu.BuildTestPod("pod-wildcard-toleration", 1000, 1000, tu.MarkUnschedulable(),
			pod.WithCCC("my-ccc"),
			pod.WithTolerations(apiv1.Toleration{
				Key:      "cloud.google.com/compute-class",
				Operator: apiv1.TolerationOpExists,
				Effect:   apiv1.TaintEffectNoSchedule,
			}),
		),
	)

	runCCCWorkloadTest(t, "Workload Separation", cc,
		tu.BuildTestPod("pod-workload-separation", 1000, 1000, tu.MarkUnschedulable(),
			pod.WithCCC("my-ccc"),
			pod.WithNodeSelectorEntry("foo", "bar"),
			pod.WithTolerations(apiv1.Toleration{
				Key:      "foo",
				Value:    "bar",
				Operator: apiv1.TolerationOpEqual,
				Effect:   apiv1.TaintEffectNoSchedule,
			}),
		),
	)
}

func runCCCConfigurationTest(t *testing.T, name string, cc *v1.ComputeClass, verify func(t *testing.T, cluster *google_api_container.Cluster)) {
	t.Run(name, func(t *testing.T) {
		testConfig := integration.NewTestConfig().
			WithOverrides(integration.WithAutoProvisioningEnabled()).
			WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
			WithCccCrds(cc)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			infra := integration.SetupInfrastructure(ctx, t)
			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			assert.NoError(t, err)
			defer integration_synctest.TearDown(cancel)

			infra.Fakes.K8s.AddPod(tu.BuildTestPod("test-pod", 1000, 1000, tu.MarkUnschedulable(), pod.WithCCC(cc.Name)))

			integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

			cluster, err := infra.Fakes.GkeService.GetCluster("projects/test-project/locations/us-central1/clusters/test-cluster")
			assert.NoError(t, err)
			verify(t, cluster)
		})
	})
}

// TestCCCConfigurations verifies that various accepted storage profiles
// (such as Local SSDs and Boot Disk Sizes) are correctly provisioned
// onto dynamic node pools within optimized sub-test evaluations.
func TestCCCConfigurations(t *testing.T) {
	cccSsd := NewComputeClassBuilder("ccc-ssd").
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{
			MachineType: ptr.To("n1-standard-4"),
			Storage: &v1.Storage{
				LocalSSDCount: ptr.To(2),
			},
		}).
		Build()

	cccDisk := NewComputeClassBuilder("ccc-disk").
		WithNapEnabled().
		WithWhenUnsatisfiable("ScaleUpAnyway").
		WithPriorities(v1.Priority{
			MachineType: ptr.To("n1-standard-4"),
			Storage: &v1.Storage{
				BootDiskSize: ptr.To(50),
			},
		}).
		Build()

	runCCCConfigurationTest(t, "Local SSD with machine type", cccSsd, func(t *testing.T, cluster *google_api_container.Cluster) {
		if assert.Equal(t, 1, len(cluster.NodePools)) {
			np := cluster.NodePools[0]
			if assert.NotNil(t, np.Config.EphemeralStorageConfig) {
				assert.Equal(t, int64(2), np.Config.EphemeralStorageConfig.LocalSsdCount)
			}
		}
	})

	runCCCConfigurationTest(t, "Boot disk size with machine type", cccDisk, func(t *testing.T, cluster *google_api_container.Cluster) {
		if assert.Equal(t, 1, len(cluster.NodePools)) {
			np := cluster.NodePools[0]
			assert.Equal(t, int64(50), np.Config.DiskSizeGb)
		}
	})
}

// TestCCCRejectedErrors verifies runtime error detection, confirming that malformed
// or unsupported rules (such as missing priority rules) correctly halt scale-up.
func TestCCCRejectedErrors(t *testing.T) {
	t.Run("Missing Priority Rules - Halt Scale-Up", func(t *testing.T) {
		cc := NewComputeClassBuilder("empty-ccc").
			WithNapEnabled().
			WithWhenUnsatisfiable("DoNotScaleUp").
			Build()

		testConfig := integration.NewTestConfig().
			WithOverrides(integration.WithAutoProvisioningEnabled()).
			WithClusterOverrides(integration.WithClusterAutoProvisioningEnabled()).
			WithCccCrds(cc)

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			infra := integration.SetupInfrastructure(ctx, t)
			autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
			assert.NoError(t, err)
			defer integration_synctest.TearDown(cancel)

			p := tu.BuildTestPod("test-pod", 1000, 1000, tu.MarkUnschedulable(), pod.WithCCC("empty-ccc"))
			infra.Fakes.K8s.AddPod(p)

			integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
			infra.Fakes.RunScheduler(ctx, t)

			updatedPod, err := infra.Fakes.KubeClient.CoreV1().Pods("default").Get(ctx, p.Name, metav1.GetOptions{})
			assert.NoError(t, err)
			assert.Empty(t, updatedPod.Spec.NodeName, "Expected pod to remain unscheduled")
		})
	})
}
