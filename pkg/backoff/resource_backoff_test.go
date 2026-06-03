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

package backoff

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	base_backoff "k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	internal_customresources "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources"
)

var (
	rC8M16 = NodeResources{
		cpu("zone1", 8),
		memory("zone1", 16),
	}

	rC8M8 = NodeResources{
		cpu("zone1", 8),
		memory("zone1", 8),
	}
	rC4M16 = NodeResources{
		cpu("zone1", 4),
		memory("zone1", 16),
	}
	rC8M32 = NodeResources{
		cpu("zone1", 8),
		memory("zone1", 32),
	}
	rC16M16 = NodeResources{
		cpu("zone1", 16),
		memory("zone1", 16),
	}
	rC1 = NodeResources{
		cpu("zone1", 1),
	}
	rC2 = NodeResources{
		cpu("zone1", 2),
	}
	rC4 = NodeResources{
		cpu("zone1", 4),
	}
	rC8 = NodeResources{
		cpu("zone1", 8),
	}
	rC16 = NodeResources{
		cpu("zone1", 16),
	}
	rM16 = NodeResources{
		memory("zone1", 16),
	}

	notAutoprovisionedNodeGroup = testNodeGroup("notAutoprovisioned", false, false)
	autoprovisionedNodeGroup    = testNodeGroup("autoprovisioned", true, false)
	autoprovisionedNodeGroup2   = testNodeGroup("autoprovisioned2", true, false)
	flexNodeGroup               = testNodeGroup("flex", true, true)
	flexNodeGroup2              = testNodeGroup("flex2", false, true)
)

func TestResourceBackoffSimple(t *testing.T) {
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
	now := time.Time{}

	// resources not backed off initially
	backoffStatus, _ := backoff.areResourcesBackedOff(rC8M16, now)
	assert.Equal(t, noBackoff, backoffStatus)

	// backing off
	backoffUntil := backoff.backoffResources(rC8M16, quotaError, now)
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)

	// >= resource sets backed off
	backoffStatus, backoffUntil = backoff.areResourcesBackedOff(rC8M16, now)
	assert.Equal(t, backoffWithQuotaError, backoffStatus)
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)

	backoffStatus, backoffUntil = backoff.areResourcesBackedOff(rC16M16, now)
	assert.Equal(t, backoffWithQuotaError, backoffStatus)
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)

	backoffStatus, backoffUntil = backoff.areResourcesBackedOff(rC8M32, now)
	assert.Equal(t, backoffWithQuotaError, backoffStatus)
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)

	// not >= resource sets not backed off
	backoffStatus, _ = backoff.areResourcesBackedOff(rC8M8, now)
	assert.Equal(t, noBackoff, backoffStatus)

	backoffStatus, _ = backoff.areResourcesBackedOff(rC4M16, now)
	assert.Equal(t, noBackoff, backoffStatus)

	backoffStatus, _ = backoff.areResourcesBackedOff(rC4, now)
	assert.Equal(t, noBackoff, backoffStatus)

	backoffStatus, _ = backoff.areResourcesBackedOff(rC8, now)
	assert.Equal(t, noBackoff, backoffStatus)

	backoffStatus, _ = backoff.areResourcesBackedOff(rC16, now)
	assert.Equal(t, noBackoff, backoffStatus)

	// if we are testing after backoffUntil no longer backed off
	backoffStatus, _ = backoff.areResourcesBackedOff(rC8M16, now.Add(100*time.Second))
	assert.Equal(t, noBackoff, backoffStatus)

	// backing off all 16 core machines at t+10
	backoffUntil = backoff.backoffResources(rC16, ipSpaceExhaustedError("1.1.1.1/24"), now.Add(10*time.Second))
	assert.Equal(t, now.Add(110*time.Second), backoffUntil)

	// c8_m16 is not covered by 16 core backoff so old backoff duration applies
	backoffStatus, backoffUntil = backoff.areResourcesBackedOff(rC8M16, now)
	assert.Equal(t, backoffWithQuotaError, backoffStatus)
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)

	// no lets backoff all 8 core machines at t+20
	backoffUntil = backoff.backoffResources(rC8, gkePersistentOperationError, now.Add(20*time.Second))
	assert.Equal(t, now.Add(120*time.Second), backoffUntil)

	// old backoff times are overridden by new backoff
	backoffStatus, backoffUntil = backoff.areResourcesBackedOff(rC16, now)
	assert.Equal(t, backoffWithGkePersistentOperationError, backoffStatus)
	assert.Equal(t, now.Add(120*time.Second), backoffUntil)

	backoffStatus, backoffUntil = backoff.areResourcesBackedOff(rC8M16, now)
	assert.Equal(t, backoffWithGkePersistentOperationError, backoffStatus)
	assert.Equal(t, now.Add(120*time.Second), backoffUntil)

	// backoff something already covered by 8 cores at t+30 should not override backoff duration
	backoffUntil = backoff.backoffResources(rC8M16, quotaError, now.Add(30*time.Second))
	assert.Equal(t, now.Add(120*time.Second), backoffUntil)

	// delete backoff for 4 cores should not change anything
	backoff.removeBackoffForResources(rC4)
	backoffStatus, backoffUntil = backoff.areResourcesBackedOff(rC16, now)
	assert.Equal(t, backoffWithGkePersistentOperationError, backoffStatus)
	assert.Equal(t, now.Add(120*time.Second), backoffUntil)

	// deleting backoff for something which includes has >= 8 cores should clear everything
	backoff.removeBackoffForResources(rC16M16)
	backoffStatus, _ = backoff.areResourcesBackedOff(rC16, now)
	assert.Equal(t, noBackoff, backoffStatus)
}

func TestResourceBackoffSimpleIndependentMachineFamilies(t *testing.T) {
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
	now := time.Time{}

	// resources not backed off initially
	backoffStatus, _ := backoff.areResourcesBackedOff(machineFamilyResource(rC4, "n1"), now)
	assert.Equal(t, noBackoff, backoffStatus)
	backoffStatus, _ = backoff.areResourcesBackedOff(machineFamilyResource(rC1, "e2"), now)
	assert.Equal(t, noBackoff, backoffStatus)

	// backing off on n1
	backoffUntil := backoff.backoffResources(machineFamilyResource(rC4, "n1"), quotaError, now)
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)

	// >= n1 resource sets backed off
	backoffStatus, backoffUntil = backoff.areResourcesBackedOff(machineFamilyResource(rC4, "n1"), now)
	assert.Equal(t, backoffWithQuotaError, backoffStatus)
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)
	// c2 unaffected
	backoffStatus, _ = backoff.areResourcesBackedOff(machineFamilyResource(rC8M16, "e2"), now)
	assert.Equal(t, noBackoff, backoffStatus)

	// backing off on e2
	backoffUntil = backoff.backoffResources(machineFamilyResource(rC1, "e2"), quotaError, now)
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)

	// 2 CPU n1 should not be considered backed off. 2 CPU e2 should be backed off
	backoffStatus, _ = backoff.areResourcesBackedOff(machineFamilyResource(rC2, "n1"), now)
	assert.Equal(t, noBackoff, backoffStatus)
	backoffStatus, _ = backoff.areResourcesBackedOff(machineFamilyResource(rC2, "e2"), now)
	assert.Equal(t, backoffWithQuotaError, backoffStatus)

	// Remove e2 backoff
	backoff.removeBackoffForResources(machineFamilyResource(rC1, "e2"))

	// N1 2 CPU should not be backed off while N1 4 CPU is still backed off, e2 is no longer backed off.
	backoffStatus, _ = backoff.areResourcesBackedOff(machineFamilyResource(rC2, "n1"), now)
	assert.Equal(t, noBackoff, backoffStatus)
	backoffStatus, _ = backoff.areResourcesBackedOff(machineFamilyResource(rC4, "n1"), now)
	assert.Equal(t, backoffWithQuotaError, backoffStatus)
	backoffStatus, _ = backoff.areResourcesBackedOff(machineFamilyResource(rC2, "e2"), now)
	assert.Equal(t, noBackoff, backoffStatus)
}

func machineFamilyResource(resources NodeResources, machineFamily string) NodeResources {
	cpy := make(NodeResources, len(resources))
	copy(cpy, resources)
	cpy[0].MachineFamily = machineFamily
	return cpy
}

func TestResourceExponentialBackoff(t *testing.T) {
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
	now := time.Time{}

	backoffUntil := backoff.backoffResources(rC8, quotaError, now)
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)

	// If resource is already backed off we do not do anything
	backoff.RemoveStaleBackoffData(now.Add(50 * time.Second))
	backoffUntil = backoff.backoffResources(rC8, quotaError, now.Add(50*time.Second))
	assert.Equal(t, now.Add(100*time.Second), backoffUntil)

	// If we backoff after backoff completed but before it is reset we make duration twice as long
	backoff.RemoveStaleBackoffData(now.Add(110 * time.Second))
	backoffUntil = backoff.backoffResources(rC8, quotaError, now.Add(110*time.Second))
	assert.Equal(t, now.Add(310*time.Second), backoffUntil)

	// If resource is already backed off we do not do anything
	backoff.RemoveStaleBackoffData(now.Add(120 * time.Second))
	backoffUntil = backoff.backoffResources(rC8, quotaError, now.Add(120*time.Second))
	assert.Equal(t, now.Add(310*time.Second), backoffUntil)

	// Backing off after backoff completed makes duration twice as long again
	backoff.RemoveStaleBackoffData(now.Add(320 * time.Second))
	backoffUntil = backoff.backoffResources(rC8, quotaError, now.Add(320*time.Second))
	assert.Equal(t, now.Add(720*time.Second), backoffUntil)

	// Max duration is capped at 500 seconds
	backoff.RemoveStaleBackoffData(now.Add(1500 * time.Second))
	backoffUntil = backoff.backoffResources(rC8, quotaError, now.Add(1500*time.Second))
	assert.Equal(t, now.Add(2000*time.Second), backoffUntil)

	// Waiting backoffResetTimeout (1000 seconds) will reset exponential backoff counter and we start
	// with initial delay again
	backoff.RemoveStaleBackoffData(now.Add(3010 * time.Second))
	backoffUntil = backoff.backoffResources(rC8, quotaError, now.Add(3010*time.Second))
	assert.Equal(t, now.Add(3110*time.Second), backoffUntil)
}

func TestResourceBackoffUserInterfaceSimple(t *testing.T) {
	now := time.Time{}

	standardNode := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
	standardNode.Labels[v1.LabelTopologyZone] = "zone1"
	standardNode.Labels[v1.LabelTopologyRegion] = "region1"
	standardNodeInfo := framework.NewTestNodeInfo(standardNode)

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
	backoff.Backoff(autoprovisionedNodeGroup, standardNodeInfo, stockoutError, now)
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(autoprovisionedNodeGroup2, standardNodeInfo, now))
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(notAutoprovisionedNodeGroup, standardNodeInfo, now))

	backoff.RemoveBackoff(autoprovisionedNodeGroup, standardNodeInfo)
	assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))
}

func TestResourceBackoffNotAutoprovisioned(t *testing.T) {
	now := time.Time{}

	standardNode := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
	standardNode.Labels[v1.LabelTopologyZone] = "zone1"
	standardNodeInfo := framework.NewTestNodeInfo(standardNode)

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
	backoff.Backoff(notAutoprovisionedNodeGroup, standardNodeInfo, quotaError, now)
	assert.Equal(t, noBackoff, backoff.BackoffStatus(notAutoprovisionedNodeGroup, standardNodeInfo, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup2, standardNodeInfo, now))
}

func TestBackoffWithPreemptionNode(t *testing.T) {
	for _, preemptionLabel := range []string{labels.PreemptibleLabel, labels.SpotLabel} {
		t.Run(preemptionLabel, func(t *testing.T) {

			now := time.Time{}

			standardNode := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
			standardNode.Labels[v1.LabelTopologyZone] = "us-central1-f"
			standardNode.Labels[v1.LabelTopologyRegion] = "us-central1"
			standardNodeInfo := framework.NewTestNodeInfo(standardNode)

			preemptionNode := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
			preemptionNode.Labels[v1.LabelTopologyZone] = "us-central1-f"
			preemptionNode.Labels[v1.LabelTopologyRegion] = "us-central1"
			preemptionNode.Labels[preemptionLabel] = labels.PreemptionValue
			preemptionNodeInfo := framework.NewTestNodeInfo(preemptionNode)

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})

			{
				// backoff preemption node resources
				backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
				backoff.Backoff(autoprovisionedNodeGroup, preemptionNodeInfo, stockoutError, now)
				assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(autoprovisionedNodeGroup, preemptionNodeInfo, now))
				// standard node with same resources should not be backed off
				assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))

				// removing backoff for preemption node
				backoff.RemoveBackoff(autoprovisionedNodeGroup, preemptionNodeInfo)
				assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, preemptionNodeInfo, now))
				assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))
			}

			{
				// backoff preemption node resources
				backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
				backoff.Backoff(autoprovisionedNodeGroup, preemptionNodeInfo, stockoutError, now)
				assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(autoprovisionedNodeGroup, preemptionNodeInfo, now))
				// standard node with same resources should not be backed off
				assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))

				// backoff removal for standard node - preemption node is still backed off
				backoff.RemoveBackoff(autoprovisionedNodeGroup, standardNodeInfo)
				assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(autoprovisionedNodeGroup, preemptionNodeInfo, now))
				assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))
			}

			{
				// backoff standard node resources
				backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
				backoff.Backoff(autoprovisionedNodeGroup, standardNodeInfo, stockoutError, now)
				// both preemption and standard should be backed off
				assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(autoprovisionedNodeGroup, preemptionNodeInfo, now))
				assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))

				// backoff removal for standard node
				backoff.RemoveBackoff(autoprovisionedNodeGroup, standardNodeInfo)
				// both backoffs should be gone
				assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, preemptionNodeInfo, now))
				assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))
			}

			{
				// backoff standard node resources
				backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
				backoff.Backoff(autoprovisionedNodeGroup, standardNodeInfo, stockoutError, now)
				// both preemption and standard should be backed off
				assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(autoprovisionedNodeGroup, preemptionNodeInfo, now))
				assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))

				// backoff removal for preemption node
				backoff.RemoveBackoff(autoprovisionedNodeGroup, preemptionNodeInfo)
				// both backoffs should be gone
				assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, preemptionNodeInfo, now))
				assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))
			}
		})
	}
}

func TestResourceBackoffStandardVsExpensiveResources(t *testing.T) {
	now := time.Time{}

	standardNode := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
	standardNode.Labels[v1.LabelTopologyZone] = "zone1"
	standardNode.Labels[v1.LabelTopologyRegion] = "region1"
	standardNodeInfo := framework.NewTestNodeInfo(standardNode)

	// Same number of CPU and memory as standardNode but with 2 gpus
	gpuNode := test.BuildTestNode("n2", 2000, 8*1024*1024*1024)
	gpuNode.Labels[v1.LabelTopologyZone] = "zone1"
	gpuNode.Labels[v1.LabelTopologyRegion] = "region1"
	test.AddGpusToNode(gpuNode, 2)
	gpuNodeInfo := framework.NewTestNodeInfo(gpuNode)

	// less memory and CPU than gpuNode but same number of GPUs
	smallGpuNode := test.BuildTestNode("n3", 1000, 4*1024*1024*1024)
	smallGpuNode.Labels[v1.LabelTopologyZone] = "zone1"
	smallGpuNode.Labels[v1.LabelTopologyRegion] = "region1"
	test.AddGpusToNode(smallGpuNode, 2)
	smallGpuNodeInfo := framework.NewTestNodeInfo(smallGpuNode)

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})

	backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
	backoff.Backoff(autoprovisionedNodeGroup, standardNodeInfo, quotaError, now)
	assert.Equal(t, backoffWithQuotaError, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))

	backoff = NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
	backoff.Backoff(autoprovisionedNodeGroup, gpuNodeInfo, quotaError, now)
	assert.Equal(t, noBackoff, backoff.BackoffStatus(autoprovisionedNodeGroup, standardNodeInfo, now))
	assert.Equal(t, backoffWithQuotaError, backoff.BackoffStatus(autoprovisionedNodeGroup, gpuNodeInfo, now))
	assert.Equal(t, backoffWithQuotaError, backoff.BackoffStatus(autoprovisionedNodeGroup, smallGpuNodeInfo, now))
}

func TestResourceBackoffTwoMatchingKeys(t *testing.T) {
	type backoffCall struct {
		resources NodeResources
		delta     time.Duration
	}
	type test struct {
		description               string
		backoffCalls              []backoffCall
		testResources             NodeResources
		expectedBackoffUntilDelta time.Duration
	}

	tests := []test{
		{
			description: "0: CPU 8, 100: MEM 16",
			backoffCalls: []backoffCall{
				{
					resources: rC8,
					delta:     0,
				},
				{
					resources: rM16,
					delta:     100,
				},
			},
			testResources:             rC8M16,
			expectedBackoffUntilDelta: 200,
		},
		{
			description: "100: MEM 16, 0: CPU 8",
			backoffCalls: []backoffCall{
				{
					resources: rM16,
					delta:     100,
				},
				{
					resources: rC8,
					delta:     0,
				},
			},
			testResources:             rC8M16,
			expectedBackoffUntilDelta: 200,
		},
		{
			description: "0: MEM 16, 100: CPU 8",
			backoffCalls: []backoffCall{
				{
					resources: rM16,
					delta:     0,
				},
				{
					resources: rC8,
					delta:     100,
				},
			},
			testResources:             rC8M16,
			expectedBackoffUntilDelta: 200,
		},
	}

	for _, test := range tests {
		for i := 0; i < 10; i++ {
			t.Run(fmt.Sprintf("[%d] %s", i, test.description), func(t *testing.T) {
				// iterate a few times to rule out nondeterministic behavior
				provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
				processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
				processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
				backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)
				now := time.Time{}
				for _, backoffCall := range test.backoffCalls {
					backoff.backoffResources(backoffCall.resources, quotaError, now.Add(backoffCall.delta*time.Second))
				}

				backoffUntil := backoff.backoffResources(test.testResources, quotaError, now)
				assert.Equal(t, now.Add(test.expectedBackoffUntilDelta*time.Second), backoffUntil)
			})
		}
	}
}

func TestAddVirtualResources(t *testing.T) {
	standardNode := test.BuildTestNode("n1", 10, 20)
	standardNode.Labels[v1.LabelTopologyZone] = "us-central1-f"
	standardNodeInfo := framework.NewTestNodeInfo(standardNode)

	standardNodeWithPvmFalseLabel := test.BuildTestNode("n1", 10, 20)
	standardNodeWithPvmFalseLabel.Labels[v1.LabelTopologyZone] = "us-central1-f"
	standardNodeWithPvmFalseLabel.Labels[labels.PreemptibleLabel] = "false"
	standardNodeInfoWithPvmFalseLabel := framework.NewTestNodeInfo(standardNodeWithPvmFalseLabel)

	standardNodeWithSpotFalseLabel := test.BuildTestNode("n1", 10, 20)
	standardNodeWithSpotFalseLabel.Labels[v1.LabelTopologyZone] = "us-central1-f"
	standardNodeWithSpotFalseLabel.Labels[labels.SpotLabel] = "false"
	standardNodeInfoWithSpotFalseLabel := framework.NewTestNodeInfo(standardNodeWithPvmFalseLabel)

	pvmNode := test.BuildTestNode("n1", 10, 20)
	pvmNode.Labels[v1.LabelTopologyZone] = "us-central1-f"
	pvmNode.Labels[labels.PreemptibleLabel] = labels.PreemptionValue
	pvmNodeInfo := framework.NewTestNodeInfo(pvmNode)

	spotNode := test.BuildTestNode("n1", 10, 20)
	spotNode.Labels[v1.LabelTopologyZone] = "us-central1-f"
	spotNode.Labels[labels.SpotLabel] = labels.PreemptionValue
	spotNodeInfo := framework.NewTestNodeInfo(spotNode)

	flexNode := test.BuildTestNode("n1", 10, 20)
	flexNode.Labels[v1.LabelTopologyZone] = "us-central1-f"
	flexNode.Labels[labels.FlexStartLabel] = labels.FlexStartValue
	flexNodeInfo := framework.NewTestNodeInfo(flexNode)

	tests := []struct {
		description       string
		resources         NodeResources
		nodeInfo          *framework.NodeInfo
		machineType       string
		expectedResources NodeResources
	}{
		{
			"standard node",
			NodeResources{
				cpu("us-central1-f", 10),
			},
			standardNodeInfo,
			"n1-standard-2",
			NodeResources{
				cpu("us-central1-f", 10),
				vmPriority("us-central1-f", 1),
			},
		},
		{
			"standard node with pvm-false label",
			NodeResources{
				cpu("us-central1-f", 10),
			},
			standardNodeInfoWithPvmFalseLabel,
			"n1-standard-2",
			NodeResources{
				cpu("us-central1-f", 10),
				vmPriority("us-central1-f", 1),
			},
		},
		{
			"standard node with spot-false label",
			NodeResources{
				cpu("us-central1-f", 10),
			},
			standardNodeInfoWithSpotFalseLabel,
			"n1-standard-2",
			NodeResources{
				cpu("us-central1-f", 10),
				vmPriority("us-central1-f", 1),
			},
		},
		{
			"pvm node",
			NodeResources{
				cpu("us-central1-f", 10),
			},
			pvmNodeInfo,
			"n1-standard-2",
			NodeResources{
				cpu("us-central1-f", 10),
				vmPriority("us-central1-f", 2),
			},
		},
		{
			"spot node",
			NodeResources{
				cpu("us-central1-f", 10),
			},
			spotNodeInfo,
			"n1-standard-2",
			NodeResources{
				cpu("us-central1-f", 10),
				vmPriority("us-central1-f", 2),
			},
		},
		{
			"flex node",
			NodeResources{
				flex(cpu("us-central1-f", 10)),
			},
			flexNodeInfo,
			"n1-standard-2",
			NodeResources{
				flex(cpu("us-central1-f", 10)),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			nodeGroup := gke.NewTestGkeMigBuilder().SetMaxSize(1000).SetSpec(&gkeclient.NodePoolSpec{MachineType: test.machineType}).Build()
			resultResources := addVirtualResources(test.resources, test.nodeInfo, nodeGroup)
			assert.ElementsMatch(t, test.expectedResources, resultResources)
		})
	}
}
func TestZonalVsRegionalBackoff(t *testing.T) {
	now := time.Time{}

	n1 := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
	n1.Labels[v1.LabelTopologyZone] = "zone1"
	n1.Labels[v1.LabelTopologyRegion] = "region1"
	nodeInfoToBackoff := framework.NewTestNodeInfo(n1)

	n2 := test.BuildTestNode("n2", 2000, 8*1024*1024*1024)
	n2.Labels[v1.LabelTopologyZone] = "zone2"
	n2.Labels[v1.LabelTopologyRegion] = "region1"
	sameRegionNodeInfo := framework.NewTestNodeInfo(n2)

	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
	processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
	processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)

	tests := []struct {
		description           string
		nodeInfo              *framework.NodeInfo
		errorInfo             cloudprovider.InstanceErrorInfo
		expectedBackoffStatus base_backoff.Status
	}{
		{
			description:           "backoff zone after quota error",
			nodeInfo:              sameRegionNodeInfo,
			errorInfo:             stockoutError,
			expectedBackoffStatus: noBackoff,
		},
		{
			description:           "backoff region after quota error",
			nodeInfo:              sameRegionNodeInfo,
			errorInfo:             quotaError,
			expectedBackoffStatus: backoffWithQuotaError,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			backoff.Backoff(autoprovisionedNodeGroup, nodeInfoToBackoff, test.errorInfo, now)
			assert.Equal(t, test.expectedBackoffStatus, backoff.BackoffStatus(autoprovisionedNodeGroup, test.nodeInfo, now))
		})
	}
}

func TestFlexVsStandardBackoff(t *testing.T) {
	now := time.Time{}

	standardNode := test.BuildTestNode("n1", 10, 20)
	standardNode.Labels[v1.LabelTopologyZone] = "us-central1-f"
	test.AddGpusToNode(standardNode, 2)
	standardNodeInfo := framework.NewTestNodeInfo(standardNode)

	flexNode := test.BuildTestNode("n1", 10, 20)
	flexNode.Labels[v1.LabelTopologyZone] = "us-central1-f"
	flexNode.Labels[labels.FlexStartLabel] = labels.FlexStartValue
	test.AddGpusToNode(flexNode, 2)
	flexNodeInfo := framework.NewTestNodeInfo(flexNode)

	tests := []struct {
		description           string
		backoffNodeInfo       *framework.NodeInfo
		backoffNodeGroup      cloudprovider.NodeGroup
		statusNodeInfo        *framework.NodeInfo
		statusNodeGroup       cloudprovider.NodeGroup
		expectedBackoffStatus base_backoff.Status
	}{
		{
			description:           "standard does not backoff flex",
			backoffNodeInfo:       standardNodeInfo,
			backoffNodeGroup:      autoprovisionedNodeGroup,
			statusNodeInfo:        flexNodeInfo,
			statusNodeGroup:       flexNodeGroup,
			expectedBackoffStatus: noBackoff,
		},
		{
			description:           "flex does not backoff standard",
			backoffNodeInfo:       flexNodeInfo,
			backoffNodeGroup:      flexNodeGroup,
			statusNodeInfo:        standardNodeInfo,
			statusNodeGroup:       autoprovisionedNodeGroup,
			expectedBackoffStatus: noBackoff,
		},
		{
			description:           "flex backs off flex",
			backoffNodeInfo:       flexNodeInfo,
			backoffNodeGroup:      flexNodeGroup,
			statusNodeInfo:        flexNodeInfo,
			statusNodeGroup:       flexNodeGroup2,
			expectedBackoffStatus: backoffWithStockoutError,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
			backoff := NewResourceBackoff(processor, 100*time.Second, 500*time.Second, 1000*time.Second).(*ResourceBackoff)

			backoff.Backoff(test.backoffNodeGroup, test.backoffNodeInfo, stockoutError, now)
			assert.Equal(t, test.expectedBackoffStatus, backoff.BackoffStatus(test.statusNodeGroup, test.statusNodeInfo, now))
		})
	}
}

func cpu(zone string, value int64) NodeResource {
	return NodeResource{
		Type:          "cpu",
		Value:         value,
		CostClass:     StandardCostClass,
		Location:      zone,
		MachineFamily: "n1",
		CapacityClass: StandardCapacityClass,
	}
}

func memory(zone string, value int64) NodeResource {
	return NodeResource{
		Type:          "memory",
		Value:         value,
		CostClass:     StandardCostClass,
		Location:      zone,
		MachineFamily: "n1",
		CapacityClass: StandardCapacityClass,
	}
}

func vmPriority(zone string, value int64) NodeResource {
	return NodeResource{
		Type:          "vm_priority",
		Value:         value,
		CostClass:     StandardCostClass,
		Location:      zone,
		MachineFamily: "n1",
		CapacityClass: StandardCapacityClass,
	}
}

func flex(resource NodeResource) NodeResource {
	resource.CapacityClass = FlexStartCapacityClass
	return resource
}
