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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

var (
	extraResources = map[string]resource.Quantity{
		gpu.ResourceNvidiaGPU: resource.MustParse("1"),
	}
	taints = []v1.Taint{
		{
			Key:    gpu.ResourceNvidiaGPU,
			Value:  "present",
			Effect: v1.TaintEffectNoSchedule,
		},
	}

	migSpecOne = &gkeclient.NodePoolSpec{
		MachineType:      "n1-standard-2",
		Labels:           map[string]string{"l1": "l1v"},
		Taints:           taints,
		PodIpv4CidrBlock: "1.1.1.1/24",
	}
	migSpecTwo = &gkeclient.NodePoolSpec{
		MachineType:      "e2-standard-2",
		Labels:           map[string]string{"l1": "l1v"},
		Taints:           taints,
		PodIpv4CidrBlock: "2.2.2.2/24",
	}
	migSpecThree = &gkeclient.NodePoolSpec{
		MachineType:      "ct4p-hightpu-4t",
		Labels:           map[string]string{"l1": "l1v"},
		TpuType:          labels.TpuV4PodsliceValue,
		TpuTopology:      "2x2x1",
		TpuMultiHost:     false,
		PodIpv4CidrBlock: "3.3.3.3/24",
	}
	migSpecFour = &gkeclient.NodePoolSpec{
		MachineType:      "ct5lp-hightpu-4t",
		Labels:           map[string]string{"l1": "l1v"},
		TpuType:          labels.TpuV5LitePodsliceValue,
		TpuTopology:      "2x4",
		TpuMultiHost:     true,
		PodIpv4CidrBlock: "4.4.4.4/24",
	}

	refMars = gce.GceRef{
		Project: "chocolatebarsproject",
		Zone:    "us-central1-f",
		Name:    "mars",
	}
	refSnickers = gce.GceRef{
		Project: "chocolatebarsproject",
		Zone:    "us-central1-f",
		Name:    "snickers",
	}
	refPierrot = gce.GceRef{
		Project: "chocolatebarsproject",
		Zone:    "us-central1-f",
		Name:    "pierrot",
	}
	refBounty = gce.GceRef{
		Project: "chocolatebarsproject",
		Zone:    "us-central1-f",
		Name:    "bounty",
	}
)

func TestGkeNodeGroupBasedBackoff(t *testing.T) {
	nodeInfoN1 := framework.NewTestNodeInfo(test.BuildTestNode("n1", 2000, 8*1024*1024*1024))
	nodeInfoE2 := framework.NewTestNodeInfo(test.BuildTestNode("e2", 2000, 8*1024*1024*1024))

	gkeManager := &gke.GkeManagerMock{MockIsUpcoming: true}
	gkeManager.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&v1.Node{}), nil)

	// mars and snickers share spec
	builder := gke.NewTestGkeMigBuilder().
		SetMaxSize(10).
		SetAutoprovisioned(true).
		SetSpec(migSpecOne).
		SetExtraResources(extraResources).
		SetGkeManager(gkeManager)
	mars := builder.SetGceRef(refMars).SetNodePoolName("marsNodePool").Build()
	snickers := builder.SetGceRef(refSnickers).SetNodePoolName("snickersNodePool").Build()

	// node groups which are already existing and share refs
	builder = builder.SetExist(true)
	existingMars := builder.SetGceRef(refMars).SetNodePoolName("marsNodePool").Build()
	existingSnickers := builder.SetGceRef(refSnickers).SetNodePoolName("snickersNodePool").Build()

	// bounty has different spec
	bounty := builder.SetExist(false).SetSpec(migSpecTwo).
		SetGceRef(refBounty).SetNodePoolName("bountyNodePool").Build()

	// sanity check
	assert.NotEqual(t, mars.Id(), snickers.Id())
	assert.NotEqual(t, existingMars.Id(), existingSnickers.Id())

	now := time.Now()
	backoff := createGkeBackoff()

	// both mars and snickers should be backed off
	// cloudprovider.OtherErrorClass (e.g. randomError) does not trigger ResourceBackoff
	backoff.Backoff(mars, nodeInfoN1, randomError, now)
	assert.Equal(t, backoffWithRandomError, backoff.BackoffStatus(mars, nodeInfoN1, now))
	assert.Equal(t, backoffWithRandomError, backoff.BackoffStatus(snickers, nodeInfoN1, now))

	// bounty should not be backed off
	assert.Equal(t, noBackoff, backoff.BackoffStatus(bounty, nodeInfoE2, now))

	// existing node groups should not be backed off
	assert.Equal(t, noBackoff, backoff.BackoffStatus(existingMars, nodeInfoN1, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(existingSnickers, nodeInfoN1, now))

	// existing node groups are backed off using their id
	// cloudprovider.OtherErrorClass (e.g. randomError) does not trigger ResourceBackoff
	backoff.Backoff(existingMars, nodeInfoN1, randomError, now)
	assert.Equal(t, backoffWithRandomError, backoff.BackoffStatus(existingMars, nodeInfoN1, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(existingSnickers, nodeInfoN1, now))
}

func TestGkeSpaceExhaustionBasedBackoff(t *testing.T) {
	gkeManager := &gke.GkeManagerMock{MockIsUpcoming: true}
	gkeManager.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&v1.Node{}), nil)

	builder := gke.NewTestGkeMigBuilder().
		SetMaxSize(10).
		SetSpec(migSpecOne).
		SetExtraResources(extraResources).
		SetExist(true).
		SetGkeManager(gkeManager)

	// Mars, snickers and pierrot have the same cidr ip range (same spec)
	// Bounty has another ip range
	mars := builder.SetGceRef(refMars).SetNodePoolName("marsNodePool").Build()
	snickers := builder.SetGceRef(refSnickers).SetNodePoolName("snickersNodePool").Build()
	pierrot := builder.SetGceRef(refPierrot).SetNodePoolName("pierrotNodePool").Build()
	bounty := builder.SetSpec(migSpecTwo).SetGceRef(refBounty).SetNodePoolName("bountyNodePool").Build()

	nodeInfoN1 := framework.NewTestNodeInfo(test.BuildTestNode("n1", 2000, 8*1024*1024*1024))
	nodeInfoE2 := framework.NewTestNodeInfo(test.BuildTestNode("e2", 2000, 8*1024*1024*1024))

	now := time.Now()

	backoff := createGkeBackoff()
	backoffStatus := backoffWithIPSpaceExhaustedError(migSpecOne.PodIpv4CidrBlock)

	// This should have set up cidr ip backoff
	// Cidr backoff for migSpecOne.PodIpv4CidrBlock will be set
	backoff.Backoff(mars, nodeInfoN1, ipSpaceExhaustedError(migSpecOne.PodIpv4CidrBlock), now)

	// Should pass because of exponential backoff (it will return first)
	assert.Equal(t, backoffStatus, backoff.BackoffStatus(mars, nodeInfoN1, now))

	// Should pass because of cidr ip backoff
	assert.Equal(t, backoffStatus, backoff.BackoffStatus(pierrot, nodeInfoN1, now))
	// Should pass because of cidr ip backoff
	assert.Equal(t, backoffStatus, backoff.BackoffStatus(snickers, nodeInfoN1, now))
	// Should pass because there is no backoff for this node group and its cird ip range
	assert.Equal(t, noBackoff, backoff.BackoffStatus(bounty, nodeInfoE2, now))
}

func TestGkeResourceBasedBackoff(t *testing.T) {
	nodeN1 := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
	nodeN1.Labels[v1.LabelTopologyZone] = "zone1"
	nodeN1.Labels[v1.LabelTopologyRegion] = "region1"
	nodeInfoN1 := framework.NewTestNodeInfo(nodeN1)

	nodeN1Zone2 := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
	nodeN1Zone2.Labels[v1.LabelTopologyZone] = "zone2"
	nodeN1Zone2.Labels[v1.LabelTopologyRegion] = "region1"
	nodeInfoN1Zone2 := framework.NewTestNodeInfo(nodeN1Zone2)

	nodeE2 := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
	nodeE2.Labels[v1.LabelTopologyZone] = "zone1"
	nodeE2.Labels[v1.LabelTopologyRegion] = "region1"
	nodeInfoE2 := framework.NewTestNodeInfo(nodeE2)

	gkeManager := gke.NewFakeGkeManagerBuilder().
		WithDataplaneV2Enabled(true).
		Build()

	// mars, snickers and pierrot share the spec
	builder := gke.NewTestGkeMigBuilder().
		SetMaxSize(10).
		SetAutoprovisioned(true).
		SetSpec(migSpecOne).
		SetExtraResources(extraResources).
		SetGkeManager(gkeManager).
		SetDeploymentType(gke.DeploymentTypeNone)
	mars := builder.SetGceRef(refMars).SetNodePoolName("marsNodePool").Build()
	snickers := builder.SetGceRef(refSnickers).SetNodePoolName("snickersNodePool").Build()

	// node groups which are already existing and share refs
	builder = builder.SetExist(true)
	existingMars := builder.SetGceRef(refMars).SetNodePoolName("marsNodePool").Build()
	existingSnickers := builder.SetGceRef(refSnickers).SetNodePoolName("snickersNodePool").Build()

	// non autoprovisioned node group sharing the refs
	existingPierrot := builder.SetAutoprovisioned(false).SetGceRef(refPierrot).SetNodePoolName("pierrotNodePool").Build()

	// bounty has different spec
	bounty := gke.NewTestGkeMigBuilder().
		SetGceRef(refBounty).
		SetMaxSize(10).
		SetAutoprovisioned(true).
		SetNodePoolName("bountyNodePool").
		SetSpec(migSpecTwo).
		SetGkeManager(gkeManager).
		SetDeploymentType(gke.DeploymentTypeNone).
		Build()

	now := time.Now()
	backoff := createGkeBackoff()

	// non autoprovisioned node groups do not trigger ResourceBackoff
	backoff.Backoff(existingPierrot, nodeInfoN1, stockoutError, now)
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(existingPierrot, nodeInfoN1, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(existingMars, nodeInfoN1, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(existingSnickers, nodeInfoN1, now))

	// all node groups sharing spec should be backed off
	// cloudprovider.OutOfResourcesErrorClass (e.g. quotaError) triggers ResourceBackoff
	backoff.Backoff(existingMars, nodeInfoN1, stockoutError, now)
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(existingMars, nodeInfoN1, now))
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(existingSnickers, nodeInfoN1, now))
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(mars, nodeInfoN1, now))
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(snickers, nodeInfoN1, now))

	// bounty should not be backed off
	assert.Equal(t, noBackoff, backoff.BackoffStatus(bounty, nodeInfoE2, now))

	// only existingMars should be backed off in zone2 because of Node Group based backoff
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(existingMars, nodeInfoN1Zone2, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(existingSnickers, nodeInfoN1Zone2, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(mars, nodeInfoN1Zone2, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(snickers, nodeInfoN1Zone2, now))

	// backing off nonExisting node groups works across zones
	backoff.Backoff(mars, nodeInfoN1, stockoutError, now)
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(existingMars, nodeInfoN1Zone2, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(existingSnickers, nodeInfoN1Zone2, now))
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(mars, nodeInfoN1Zone2, now))
	assert.Equal(t, backoffWithStockoutError, backoff.BackoffStatus(snickers, nodeInfoN1Zone2, now))
}

func TestGkeReservationsBasedBackoff(t *testing.T) {
	nodeN1 := test.BuildTestNode("n1", 2000, 8*1024*1024*1024)
	nodeN1.Labels[v1.LabelInstanceTypeStable] = "ct5lp-hightpu-4t"
	nodeN1.Labels[labels.TPULabel] = labels.TpuV5LitePodsliceValue
	nodeN1.Labels[labels.TPUTopologyLabel] = "2x2"
	nodeInfoN1 := framework.NewTestNodeInfo(nodeN1)

	nodeN2 := test.BuildTestNode("n2", 2000, 8*1024*1024*1024)
	nodeN2.Labels[v1.LabelInstanceTypeStable] = "ct4p-hightpu-4t"
	nodeN2.Labels[labels.TPULabel] = labels.TpuV4PodsliceValue
	nodeN2.Labels[labels.TPUTopologyLabel] = "2x2x1"
	nodeInfoN2 := framework.NewTestNodeInfo(nodeN2)

	gkeManager := &gke.GkeManagerMock{MockIsUpcoming: true}
	gkeManager.On("GetMigTemplateNodeInfo", mock.Anything).Return(framework.NewTestNodeInfo(&v1.Node{}), nil)

	// mars, snickers are tpu node pools
	tpuv4MigBuilder := gke.NewTestGkeMigBuilder().
		SetMaxSize(2).
		SetAutoprovisioned(true).
		SetSpec(migSpecThree).
		SetExtraResources(extraResources).
		SetGkeManager(gkeManager)
	mars := tpuv4MigBuilder.SetGceRef(refMars).SetNodePoolName("marsNodePool").Build()

	tpuv5MigBuilder := gke.NewTestGkeMigBuilder().
		SetMaxSize(2).
		SetAutoprovisioned(true).
		SetSpec(migSpecFour).
		SetExtraResources(extraResources).
		SetGkeManager(gkeManager)
	snickers := tpuv5MigBuilder.SetGceRef(refMars).SetNodePoolName("snickersNodePool").Build()

	// bounty is not tpu node pool
	bounty := gke.NewTestGkeMigBuilder().
		SetGceRef(refBounty).
		SetMaxSize(10).
		SetAutoprovisioned(true).
		SetNodePoolName("bountyNodePool").
		SetSpec(migSpecTwo).
		SetGkeManager(gkeManager).
		Build()

	now := time.Now()
	backoff := createGkeBackoff()

	backoff.Backoff(mars, nodeInfoN1, quotaError, now)
	assert.Equal(t, backoffWithQuotaError, backoff.BackoffStatus(mars, nodeInfoN1, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(snickers, nodeInfoN2, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(bounty, nodeInfoN1, now))

	backoff.Backoff(snickers, nodeInfoN1, quotaError, now)
	assert.Equal(t, backoffWithQuotaError, backoff.BackoffStatus(mars, nodeInfoN1, now))
	assert.Equal(t, backoffWithQuotaError, backoff.BackoffStatus(snickers, nodeInfoN1, now))
	assert.Equal(t, noBackoff, backoff.BackoffStatus(bounty, nodeInfoN1, now))
}

type mockProvider struct {
	mock.Mock
}

func (m *mockProvider) GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily {
	args := m.Called()
	return args.Get(0).(machinetypes.MachineFamily)
}

func (m *mockProvider) IsAutopilotEnabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *mockProvider) MachineConfigProvider() *machinetypes.MachineConfigProvider {
	return machinetypes.NewMachineConfigProvider(nil)
}
