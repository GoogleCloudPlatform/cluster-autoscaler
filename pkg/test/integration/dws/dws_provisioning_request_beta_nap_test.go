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

package dws

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	gcev1 "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	prv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	config "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/reconciler"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/provreq"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

const (
	gpuTypeLabel   = "cloud.google.com/gke-accelerator"
	customGPUType  = "nvidia-tesla-t4"
	customGPUType2 = "nvidia-l4"
)

func buildProvReqWithPodTemplate(name, namespace string, podCount int, gpuCount int, gpuType, npName string) *provreqwrapper.ProvisioningRequest {
	opts := []func(*apiv1.Pod){
		pod.WithToleration("nvidia.com/gpu", apiv1.TolerationOpExists, apiv1.TaintEffectNoSchedule),
		pod.WithResource("cpu", "500m", "500m"),
	}

	if npName != "" {
		opts = append(opts, pod.WithNodeSelectorEntry("cloud.google.com/gke-nodepool", npName))
	}

	if gpuCount > 0 {
		opts = append(opts, pod.WithResource("nvidia.com/gpu", "1", "1"))
	}

	if gpuType != "" {
		opts = append(opts, pod.WithNodeSelectorEntry("cloud.google.com/gke-accelerator", gpuType))
	}

	testPod := tu.BuildTestPod("temp", 0, 0, opts...)
	return provreq.BuildTestProvisioningRequest(namespace, name, map[string]string{
		"cloud.google.com/gke-capacity-check-wait-time-seconds": "0",
	}, testPod.Spec, provreq.WithPodCount(int32(podCount)))
}

func validateNapNP(t *testing.T, np *gke_api_beta.NodePool) {
	t.Helper()
	if assert.NotNil(t, np.QueuedProvisioning, "Expected NAP NodePool to have QueuedProvisioning configured") {
		assert.True(t, np.QueuedProvisioning.Enabled, "Expected QueuedProvisioning.Enabled to be true")
	}

	if assert.NotNil(t, np.Autoscaling, "Expected NAP NodePool to have Autoscaling configured") {
		assert.True(t, np.Autoscaling.Enabled, "Expected Autoscaling.Enabled to be true")
		assert.Equal(t, "ANY", np.Autoscaling.LocationPolicy, "Expected LocationPolicy to be ANY")
	}

	if assert.NotNil(t, np.Config, "Expected NAP NodePool to have Config") && assert.NotNil(t, np.Config.ReservationAffinity, "Expected NAP NodePool to have ReservationAffinity configured") {
		assert.Equal(t, "NO_RESERVATION", np.Config.ReservationAffinity.ConsumeReservationType, "Expected ConsumeReservationType to be NO_RESERVATION")
	}
}

// TestProvReqNAPGPUWithLimit tests ProvReq requesting any GPU; with accelerator limit set on a custom GPU.
func TestProvReqNAPGPUWithLimit(t *testing.T) {
	prw := buildProvReqWithPodTemplate("prov-req-test-0-0", "default", 1, 1, "", "")

	testConfig := integration.NewTestConfig().
		WithExperiments().
		WithProvisioningRequests(prw).
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
				o.InternalOptions.ProvisioningLabelEnabled = true
				return o
			},
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: 1000},
				{ResourceType: "memory", Maximum: 10000000},
				{ResourceType: customGPUType, Maximum: 1},
			}),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if err != nil {
			t.Fatalf("SetupAutoscaler failed: %v", err)
		}

		infra.Fakes.GceService.WithAcceleratorTypes(&gcev1.AcceleratorTypeList{
			Items: []*gcev1.AcceleratorType{
				{
					Name:                    customGPUType,
					Zone:                    "us-central1-a",
					MaximumCardsPerInstance: 4,
				},
			},
		})
		defer integration_synctest.TearDown(cancel)

		p := tu.BuildTestPod("job-consuming-prov-req-test-0-0", 0, 0, tu.MarkUnschedulable(),
			pod.WithAnnotation("cluster-autoscaler.kubernetes.io/consume-provisioning-request", "prov-req-test-0-0"),
			pod.WithAnnotation("cluster-autoscaler.kubernetes.io/provisioning-class-name", "queued-provisioning.gke.io"),
			pod.WithNodeSelector(map[string]string{
				apiv1.LabelInstanceTypeStable:               "n1-standard-2",
				"cloud.google.com/gke-provisioning-request": "prov-req-test-0-0",
			}),
			pod.WithResource("cpu", "500m", "500m"),
			pod.WithResource("nvidia.com/gpu", "1", "1"),
			pod.WithToleration("nvidia.com/gpu", apiv1.TolerationOpExists, apiv1.TaintEffectNoSchedule),
		)
		infra.Fakes.K8s.AddPod(p)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		_ = integration_synctest.RunOnceAfter(ctx, t, autoscaler, reconciler.MaxObtainabilityStrategyUnreconciledPeriod)

		nps := infra.Fakes.GkeService.GetAutoprovisionedNodePools()
		assert.Len(t, nps, 1, "Expected exactly 1 autoprovisioned node pool")

		targetSize := infra.Fakes.GkeService.MustGetTargetSize(t, nps[0])
		assert.Equal(t, int64(1), targetSize, "Expected NAP node pool to have target size 1")

		if assert.NotNil(t, nps[0].Config, "Expected NAP NodePool to have Config") && assert.NotEmpty(t, nps[0].Config.Accelerators, "Expected NAP NodePool to have Accelerators configured") {
			assert.Equal(t, customGPUType, nps[0].Config.Accelerators[0].AcceleratorType)
			assert.Equal(t, int64(1), nps[0].Config.Accelerators[0].AcceleratorCount)
		}

		validateNapNP(t, nps[0])

		updatedPR, err := infra.Fakes.PRClientset.AutoscalingV1().ProvisioningRequests("default").Get(ctx, "prov-req-test-0-0", metav1.GetOptions{})
		assert.NoError(t, err)
		acceptedCond := findCondition(updatedPR, prv1.Accepted)
		if assert.NotNil(t, acceptedCond, "Expected PR to have Accepted condition") {
			assert.Equal(t, metav1.ConditionTrue, acceptedCond.Status)
		}
	})
}

// Test2ProvReqsGPUWithLimit1 tests 2 ProvReqs requesting a GPU; with accelerator limit set on that GPU only allowing 1 ProvReq to provision at a time.
func Test2ProvReqsGPUWithLimit1(t *testing.T) {
	prw1 := buildProvReqWithPodTemplate("prov-req-test-1-0", "default", 1, 1, customGPUType2, "")
	prw2 := buildProvReqWithPodTemplate("prov-req-test-1-1", "default", 1, 1, customGPUType2, "")

	testConfig := integration.NewTestConfig().
		WithExperiments().
		WithProvisioningRequests(prw1, prw2).
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
				o.InternalOptions.ProvisioningLabelEnabled = true
				return o
			},
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: 1000},
				{ResourceType: "memory", Maximum: 10000000},
				{ResourceType: customGPUType2, Maximum: 1}, // Limit to 1 GPU total
			}),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if err != nil {
			t.Fatalf("SetupAutoscaler failed: %v", err)
		}

		infra.Fakes.GceService.WithAcceleratorTypes(&gcev1.AcceleratorTypeList{
			Items: []*gcev1.AcceleratorType{
				{
					Name:                    customGPUType2,
					Zone:                    "us-central1-a",
					MaximumCardsPerInstance: 4,
				},
			},
		})
		defer integration_synctest.TearDown(cancel)

		p1 := tu.BuildTestPod("job-consuming-prov-req-test-1-0", 0, 0, tu.MarkUnschedulable(),
			pod.WithAnnotation("cluster-autoscaler.kubernetes.io/consume-provisioning-request", "prov-req-test-1-0"),
			pod.WithAnnotation("cluster-autoscaler.kubernetes.io/provisioning-class-name", "queued-provisioning.gke.io"),
			pod.WithNodeSelectorEntry("cloud.google.com/gke-provisioning-request", "prov-req-test-1-0"),
			pod.WithResource("cpu", "500m", "500m"),
			pod.WithResource("nvidia.com/gpu", "1", "1"),
			pod.WithToleration("nvidia.com/gpu", apiv1.TolerationOpExists, apiv1.TaintEffectNoSchedule),
			pod.WithCreationTimestamp(metav1.NewTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(-1*time.Hour))),
		)

		p2 := tu.BuildTestPod("job-consuming-prov-req-test-1-1", 0, 0, tu.MarkUnschedulable(),
			pod.WithAnnotation("cluster-autoscaler.kubernetes.io/consume-provisioning-request", "prov-req-test-1-1"),
			pod.WithAnnotation("cluster-autoscaler.kubernetes.io/provisioning-class-name", "queued-provisioning.gke.io"),
			pod.WithNodeSelectorEntry("cloud.google.com/gke-provisioning-request", "prov-req-test-1-1"),
			pod.WithResource("cpu", "500m", "500m"),
			pod.WithResource("nvidia.com/gpu", "1", "1"),
			pod.WithToleration("nvidia.com/gpu", apiv1.TolerationOpExists, apiv1.TaintEffectNoSchedule),
			pod.WithCreationTimestamp(metav1.NewTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(-1*time.Hour))),
		)

		infra.Fakes.K8s.AddPod(p1)
		infra.Fakes.K8s.AddPod(p2)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		_ = integration_synctest.RunOnceAfter(ctx, t, autoscaler, reconciler.MaxObtainabilityStrategyUnreconciledPeriod)

		nps := infra.Fakes.GkeService.GetAutoprovisionedNodePools()
		var totalNAPTargetSize int64
		for _, np := range nps {
			targetSize := infra.Fakes.GkeService.MustGetTargetSize(t, np)
			totalNAPTargetSize += targetSize
			if targetSize > 0 {
				if assert.NotNil(t, np.Config, "Expected NAP NodePool to have Config") && assert.NotEmpty(t, np.Config.Accelerators, "Expected NAP NodePool to have Accelerators configured") {
					assert.Equal(t, customGPUType2, np.Config.Accelerators[0].AcceleratorType)
					assert.Equal(t, int64(1), np.Config.Accelerators[0].AcceleratorCount)
				}
				validateNapNP(t, np)
			}
		}
		assert.Equal(t, int64(1), totalNAPTargetSize, "Expected exactly 1 node to be scaled up due to limit")

		updatedPR1, err := infra.Fakes.PRClientset.AutoscalingV1().ProvisioningRequests("default").Get(ctx, "prov-req-test-1-0", metav1.GetOptions{})
		assert.NoError(t, err)

		updatedPR2, err := infra.Fakes.PRClientset.AutoscalingV1().ProvisioningRequests("default").Get(ctx, "prov-req-test-1-1", metav1.GetOptions{})
		assert.NoError(t, err)

		var acceptedCount int
		var failedCount int

		for _, pr := range []*prv1.ProvisioningRequest{updatedPR1, updatedPR2} {
			if cond := findCondition(pr, prv1.Accepted); cond != nil && cond.Status == metav1.ConditionTrue {
				acceptedCount++
			}
			if cond := findCondition(pr, prv1.Failed); cond != nil && cond.Status == metav1.ConditionTrue && cond.Reason == "OutOfResources" {
				failedCount++
			}
		}

		assert.Equal(t, 1, acceptedCount, "Expected exactly 1 PR to be Accepted")
		assert.Equal(t, 1, failedCount, "Expected exactly 1 PR to be Failed due to OutOfResources")
	})
}

// TestProvReqGPUAndNodePool tests ProvReq requesting a GPU and a nodepool that won't fit all of the nodes.
func TestProvReqGPUAndNodePool(t *testing.T) {
	prw := buildProvReqWithPodTemplate("prov-req-test-2-0", "default", 4, 1, "", "existing-qp-nodepool")

	testConfig := integration.NewTestConfig().
		WithExperiments().
		WithProvisioningRequests(prw).
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
				o.InternalOptions.ProvisioningLabelEnabled = true
				return o
			},
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: 1000},
				{ResourceType: "memory", Maximum: 10000000},
				{ResourceType: customGPUType, Maximum: 10},
			}),
		).
		WithNodePools(integration.DefaultNodePool(
			integration.WithNodePoolName("existing-qp-nodepool"),
			integration.WithNodePoolMachineType("n1-standard-2"),
			integration.WithNodePoolInitialNodeCount(0),
			integration.WithNodePoolMinNodeCount(0),
			integration.WithNodePoolMaxNodeCount(2),
			integration.WithNodePoolLabels(map[string]string{
				"cloud.google.com/gke-nodepool": "existing-qp-nodepool",
				apiv1.LabelInstanceTypeStable:   "n1-standard-2",
			}),
			integration.WithNodePoolTaints(&gke_api_beta.NodeTaint{
				Key:    "nvidia.com/gpu",
				Value:  "present",
				Effect: "NO_SCHEDULE",
			}),
			integration.WithNodePoolAccelerators(&gke_api_beta.AcceleratorConfig{
				AcceleratorCount: 1,
				AcceleratorType:  customGPUType,
			}),
			integration.WithNodePoolQueuedProvisioning(true),
		))

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if err != nil {
			t.Fatalf("SetupAutoscaler failed: %v", err)
		}

		infra.Fakes.GceService.
			WithAcceleratorTypes(&gcev1.AcceleratorTypeList{
				Items: []*gcev1.AcceleratorType{
					{
						Name:                    customGPUType,
						Zone:                    "us-central1-a",
						MaximumCardsPerInstance: 4,
					},
				},
			})

		defer integration_synctest.TearDown(cancel)

		for i := 0; i < 4; i++ {
			p := tu.BuildTestPod(fmt.Sprintf("job-consuming-prov-req-test-2-0-%d", i), 0, 0, tu.MarkUnschedulable(),
				pod.WithAnnotation("cluster-autoscaler.kubernetes.io/consume-provisioning-request", "prov-req-test-2-0"),
				pod.WithAnnotation("cluster-autoscaler.kubernetes.io/provisioning-class-name", "queued-provisioning.gke.io"),
				pod.WithNodeSelectorEntry(apiv1.LabelInstanceTypeStable, "n1-standard-2"),
				pod.WithNodeSelectorEntry("cloud.google.com/gke-nodepool", "existing-qp-nodepool"),
				pod.WithNodeSelectorEntry("cloud.google.com/gke-provisioning-request", "prov-req-test-2-0"),
				pod.WithResource("cpu", "500m", "500m"),
				pod.WithResource("nvidia.com/gpu", "1", "1"),
				pod.WithToleration("nvidia.com/gpu", apiv1.TolerationOpExists, apiv1.TaintEffectNoSchedule),
				pod.WithCreationTimestamp(metav1.NewTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(-1*time.Hour))),
			)
			infra.Fakes.K8s.AddPod(p)
		}

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		_ = integration_synctest.RunOnceAfter(ctx, t, autoscaler, reconciler.MaxObtainabilityStrategyUnreconciledPeriod)

		napNodePools := infra.Fakes.GkeService.GetAutoprovisionedNodePools()
		assert.Empty(t, napNodePools, "Expected NO newly created NAP node group because NodePool is specified")

		// Verify existing NodePool target size
		np := infra.Fakes.GkeService.MustGetNodePool(t, "existing-qp-nodepool")
		targetSize := infra.Fakes.GkeService.MustGetTargetSize(t, np)
		assert.Equal(t, int64(0), targetSize, "Expected NodePool NOT to be scaled up because PR failed")

		updatedPR, err := infra.Fakes.PRClientset.AutoscalingV1().ProvisioningRequests("default").Get(ctx, "prov-req-test-2-0", metav1.GetOptions{})
		assert.NoError(t, err)
		failedCond := findCondition(updatedPR, prv1.Failed)
		if assert.NotNil(t, failedCond, "Expected PR to have Failed condition") {
			assert.Equal(t, "NodepoolSizeReached", failedCond.Reason)
		}
	})
}

// TestProvReqGPUWithNonExistingNodePool tests ProvReq requesting a GPU with a node selector targeting a non-existing nodepool.
func TestProvReqGPUWithNonExistingNodePool(t *testing.T) {
	prw := buildProvReqWithPodTemplate("prov-req-test-3-0", "default", 1, 1, "", "non-existing-nodepool")

	testConfig := integration.NewTestConfig().
		WithExperiments().
		WithProvisioningRequests(prw).
		WithOverrides(
			integration.WithAutoProvisioningEnabled(),
			func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
				o.InternalOptions.ProvisioningLabelEnabled = true
				return o
			},
		).
		WithClusterOverrides(
			integration.WithClusterAutoProvisioningEnabled(),
			integration.WithClusterResourceLimits([]*gke_api_beta.ResourceLimit{
				{ResourceType: "cpu", Maximum: 1000},
				{ResourceType: "memory", Maximum: 10000000},
				{ResourceType: customGPUType, Maximum: 1},
			}),
		)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		infra := integration.SetupInfrastructure(ctx, t)

		autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
		if err != nil {
			t.Fatalf("SetupAutoscaler failed: %v", err)
		}

		infra.Fakes.GceService.
			WithAcceleratorTypes(&gcev1.AcceleratorTypeList{
				Items: []*gcev1.AcceleratorType{
					{
						Name:                    customGPUType,
						Zone:                    "us-central1-a",
						MaximumCardsPerInstance: 4,
					},
				},
			})

		defer integration_synctest.TearDown(cancel)

		p := tu.BuildTestPod("job-consuming-prov-req-test-3-0", 0, 0, tu.MarkUnschedulable(),
			pod.WithAnnotation("cluster-autoscaler.kubernetes.io/consume-provisioning-request", "prov-req-test-3-0"),
			pod.WithAnnotation("cluster-autoscaler.kubernetes.io/provisioning-class-name", "queued-provisioning.gke.io"),
			pod.WithNodeSelectorEntry(apiv1.LabelInstanceTypeStable, "n1-standard-2"),
			pod.WithNodeSelectorEntry("cloud.google.com/gke-nodepool", "non-existing-nodepool"),
			pod.WithNodeSelectorEntry("cloud.google.com/gke-provisioning-request", "prov-req-test-3-0"),
			pod.WithResource("cpu", "500m", "500m"),
			pod.WithResource("nvidia.com/gpu", "1", "1"),
			pod.WithToleration("nvidia.com/gpu", apiv1.TolerationOpExists, apiv1.TaintEffectNoSchedule),
		)
		infra.Fakes.K8s.AddPod(p)

		integration_synctest.MustRunOnceAfter(ctx, t, autoscaler, time.Second)
		_ = integration_synctest.RunOnceAfter(ctx, t, autoscaler, reconciler.MaxObtainabilityStrategyUnreconciledPeriod)

		napNodePools := infra.Fakes.GkeService.GetAutoprovisionedNodePools()
		assert.Empty(t, napNodePools, "Expected NO newly created NAP node group")

		updatedPR, err := infra.Fakes.PRClientset.AutoscalingV1().ProvisioningRequests("default").Get(ctx, "prov-req-test-3-0", metav1.GetOptions{})
		assert.NoError(t, err)
		failedCond := findCondition(updatedPR, prv1.Failed)
		if assert.NotNil(t, failedCond, "Expected PR to have Failed condition") {
			assert.Equal(t, "NoQueuedNodepoolAvailable", failedCond.Reason)
		}
	})
}
