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
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/ccc"

	"github.com/stretchr/testify/assert"
	gcev1 "google.golang.org/api/compute/v1"
	"k8s.io/utils/ptr"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	tu "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	config "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/node"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/pod"
	integration_synctest "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/test/integration/synctest"
)

func TestNodeRecyclingCustomComputeClass(t *testing.T) {
	customGpuType := "nvidia-l4"
	customTpuType := "tpu-v6e-slice"
	setupTimeout := 25 * time.Minute
	capacityCheckWaitTime := 45 * time.Minute
	maxRunDuration := setupTimeout + setupTimeout // 50m

	testCases := []struct {
		name    string
		podOpts []func(*apiv1.Pod)
		ccc     *v1.ComputeClass
	}{
		{
			name: "GPU",
			podOpts: []func(*apiv1.Pod){
				pod.WithResource("cpu", "1000m", "1000m"),
				pod.WithResource("memory", "1000Mi", "1000Mi"),
				pod.WithResource("nvidia.com/gpu", "1", "1"),
				pod.WithToleration("nvidia.com/gpu", apiv1.TolerationOpExists, apiv1.TaintEffectNoSchedule),
				pod.WithTolerationValue("cloud.google.com/compute-class", apiv1.TolerationOpEqual, "gpu-ccc", apiv1.TaintEffectNoSchedule),
				pod.WithNodeSelectorEntry("cloud.google.com/compute-class", "gpu-ccc"),
				pod.WithNodeSelectorEntry("cloud.google.com/gke-accelerator", customGpuType),
			},
			ccc: ccc.NewComputeClassBuilder("gpu-ccc").
				WithNodePoolAutoCreation(true).
				WithPriorities(
					v1.Priority{
						MaxRunDurationSeconds: ptr.To(int(maxRunDuration.Seconds())),
						FlexStart: &v1.FlexStart{
							Enabled: true,
							NodeRecycling: &v1.NodeRecyclingConfig{
								LeadTimeSeconds: ptr.To(int(setupTimeout.Seconds())),
							},
						},
						CapacityCheckWaitTimeSeconds: ptr.To(int(capacityCheckWaitTime.Seconds())),
						Gpu: &v1.GPU{
							Type:          customGpuType,
							Count:         1,
							DriverVersion: "default",
						},
					},
				).
				Build(),
		},
		{
			name: "MultiHostTPU",
			podOpts: []func(*apiv1.Pod){
				pod.WithResource("cpu", "1000m", "1000m"),
				pod.WithResource("memory", "1000Mi", "1000Mi"),
				pod.WithResource("google.com/tpu", "4", "4"),
				pod.WithToleration("google.com/tpu", apiv1.TolerationOpExists, apiv1.TaintEffectNoSchedule),
				pod.WithTolerationValue("cloud.google.com/compute-class", apiv1.TolerationOpEqual, "tpu-ccc", apiv1.TaintEffectNoSchedule),
				pod.WithNodeSelectorEntry("cloud.google.com/compute-class", "tpu-ccc"),
				pod.WithNodeSelectorEntry("cloud.google.com/gke-tpu-accelerator", customTpuType),
				pod.WithNodeSelectorEntry("cloud.google.com/gke-tpu-topology", "2x4"),
				pod.WithNodeSelectorEntry("cloud.google.com/gke-accelerator-count", "4"),
			},
			ccc: ccc.NewComputeClassBuilder("tpu-ccc").
				WithNodePoolAutoCreation(true).
				WithPriorities(
					v1.Priority{
						MachineType:           ptr.To("ct6e-standard-4t"),
						MaxRunDurationSeconds: ptr.To(int(maxRunDuration.Seconds())),
						FlexStart: &v1.FlexStart{
							Enabled: true,
							NodeRecycling: &v1.NodeRecyclingConfig{
								LeadTimeSeconds: ptr.To(int(setupTimeout.Seconds())),
							},
						},
						CapacityCheckWaitTimeSeconds: ptr.To(int(capacityCheckWaitTime.Seconds())),
						Tpu: &v1.TPU{
							Type:     customTpuType,
							Count:    4,
							Topology: "2x4",
						},
					},
				).
				Build(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				infra := integration.SetupInfrastructure(ctx, t)
				// Setup Compute Class
				tc.ccc.Spec.WhenUnsatisfiable = "DoNotScaleUp"
				// tc.ccc is passed directly to WithCccCrds

				testConfig := integration.NewTestConfig().
					WithExperiments(
						experiments.FlexStartNonQueuedEnabledFlag,
						experiments.FlexStartNonQueuedNAPEnabledFlag,
					).
					WithOverrides(
						integration.WithAutoProvisioningEnabled(),
						func(o *config.AutoscalingOptions) *config.AutoscalingOptions {
							o.TpuAutoprovisioningEnabled = true
							o.DefragEnabled = true
							o.DefragPlugins = "recycling"
							o.DefragCandidateLimit = 10
							o.DefragCandidateNodeLimit = 10
							o.NodeGroupDefaults.ScaleDownUnneededTime = 0
							return o
						},
					).
					WithClusterOverrides(
						integration.WithClusterAutoProvisioningEnabled(),
					).
					WithCccCrds(tc.ccc)

				autoscaler, err := integration.SetupAutoscaler(ctx, t, testConfig, infra)
				assert.NoError(t, err)
				if err != nil {
					t.Fatalf("Failed to setup autoscaler: %v", err)
				}
				defer integration_synctest.TearDown(cancel)

				// Configure Accelerators manually after SetupAutoscaler
				diskTypes := make(map[string][]string)
				for _, zone := range []string{"us-central1-a", "us-central1-b", "us-central1-c"} {
					diskTypes[zone] = []string{"pd-standard", "pd-balanced", "pd-ssd", "boot", "hyperdisk-balanced"}
				}
				infra.Fakes.GceService.WithDiskTypes(diskTypes)

				infra.Fakes.GceService.WithAcceleratorTypes(&gcev1.AcceleratorTypeList{
					Items: []*gcev1.AcceleratorType{
						{
							Name:                    customGpuType,
							Zone:                    "us-central1-a",
							MaximumCardsPerInstance: 8,
						},
						{
							Name:                    customGpuType,
							Zone:                    "us-central1-b",
							MaximumCardsPerInstance: 8,
						},
						{
							Name:                    customGpuType,
							Zone:                    "us-central1-c",
							MaximumCardsPerInstance: 8,
						},
						{
							Name:                    customTpuType,
							Zone:                    "us-central1-a",
							MaximumCardsPerInstance: 4,
						},
						{
							Name:                    customTpuType,
							Zone:                    "us-central1-b",
							MaximumCardsPerInstance: 8,
						},
						{
							Name:                    customTpuType,
							Zone:                    "us-central1-c",
							MaximumCardsPerInstance: 8,
						},
					},
				})

				numPods := 1
				if tc.name == "MultiHostTPU" {
					numPods = 2
				}
				for i := 0; i < numPods; i++ {
					p := tu.BuildTestPod("test-pod-"+string(rune('a'+i)), 1000, 1000, append([]func(*apiv1.Pod){
						tu.MarkUnschedulable(),
						pod.WithCreationTimestamp(metav1.NewTime(time.Now().Add(-1 * time.Hour))),
						pod.WithAnnotation("cluster-autoscaler.kubernetes.io/safe-to-evict", "true"),
					}, tc.podOpts...)...)
					infra.Fakes.K8s.AddPod(p)
				}

				// First loop executes NAP logic to provision a new underlying NodeGroup.
				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)
				// Second loop evaluates target sizes and executes the actual CA Scale-Up.
				integration_synctest.MustRunOnceAfter(t, autoscaler, time.Second)

				nodeGroups := autoscaler.AutoscalingContext.CloudProvider.NodeGroups()
				initialNgId := validateInitialNodeGroupSize(t, nodeGroups, numPods)
				if initialNgId == "" {
					return
				}

				// Get the node instances to update their annotations.
				var instances []string
				for _, ng := range nodeGroups {
					if ng.Id() == initialNgId {
						insts, _ := ng.Nodes()
						for _, inst := range insts {
							instances = append(instances, inst.Id)
						}
						break
					}
				}

				assert.GreaterOrEqual(t, len(instances), numPods, "Expected at least %d instances, got %d", numPods, len(instances))
				if len(instances) < numPods {
					t.Fatalf("Expected at least %d instances, got %d", numPods, len(instances))
				}
				for i := 0; i < len(instances); i++ {
					parts := strings.Split(instances[i], "/")
					nodeName := parts[len(parts)-1]

					n, errGet := infra.Fakes.KubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
					assert.NoError(t, errGet, "Node wasn't automatically created by fake GCE integration")
					if errGet != nil {
						t.Fatalf("Node wasn't automatically created by fake GCE integration: %v", errGet)
					}

					n = node.ApplyOptions(n,
						node.WithLabel("cloud.google.com/gke-node-recycle-lead-time-seconds", fmt.Sprintf("%d", int(setupTimeout.Seconds()))),
						node.WithAnnotation("node.gke.io/machine-termination-datetime", time.Now().Add(maxRunDuration).Format(time.RFC3339)),
						node.WithAnnotation("node.gke.io/last-applied-node-labels", "true"),
					)
					_, errUpdate := infra.Fakes.KubeClient.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{})
					assert.NoError(t, errUpdate)
					if errUpdate != nil {
						t.Fatalf("Failed to update node: %v", errUpdate)
					}
				}

				synctest.Wait()
				infra.Fakes.RunScheduler(ctx, t)
				synctest.Wait()

				// Move time forward by MaxRunDuration - LeadTimeSeconds.
				// This simulates the nodes getting closer to their max duration.
				integration_synctest.MustRunOnceAfter(t, autoscaler, maxRunDuration-setupTimeout+1*time.Minute)

				nodeGroups = autoscaler.AutoscalingContext.CloudProvider.NodeGroups()
				validateReplacementNodeGroupCreated(t, nodeGroups, initialNgId, numPods)
			})
		})
	}
}

func validateInitialNodeGroupSize(t *testing.T, nodeGroups []cloudprovider.NodeGroup, numPods int) string {
	t.Helper()
	var initialNgId string
	for _, ng := range nodeGroups {
		if strings.Contains(ng.Id(), "nap") {
			size, _ := ng.TargetSize()
			// Ensure it cleanly hit the full number of bounds for the pod set
			if size == numPods {
				initialNgId = ng.Id()
				break
			}
		}
	}
	assert.NotEmpty(t, initialNgId, "Expected to find a NAP nodegroup cleanly scaled up to %d", numPods)
	if initialNgId == "" {
		t.Fatalf("Expected to find a NAP nodegroup cleanly scaled up to %d", numPods)
	}
	return initialNgId
}

func validateReplacementNodeGroupCreated(t *testing.T, nodeGroups []cloudprovider.NodeGroup, initialNgId string, numPods int) {
	t.Helper()
	foundReplacement := false
	for _, ng := range nodeGroups {
		if strings.Contains(ng.Id(), "nap") && ng.Id() != initialNgId {
			size, _ := ng.TargetSize()
			if size >= numPods {
				foundReplacement = true
			}
		}
	}
	if !foundReplacement {
		for _, ng := range nodeGroups {
			if ng.Id() == initialNgId {
				size, _ := ng.TargetSize()
				if size >= numPods*2 { // Wait for the full set of replacements instead of partial
					foundReplacement = true
				}
			}
		}
	}
	assert.True(t, foundReplacement, "Expected a replacement nodegroup to be scaled up due to node recycling")
	if !foundReplacement {
		t.Fatalf("Expected a replacement nodegroup to be scaled up due to node recycling")
	}
}
