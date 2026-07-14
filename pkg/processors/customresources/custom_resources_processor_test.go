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

package customresources

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	gce_api "google.golang.org/api/compute/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"

	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clock "k8s.io/utils/clock/testing"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodeinfosprovider"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/dynamicresources"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
)

func TestWorstAllocatableBasic(t *testing.T) {
	tests := []struct {
		nodeMemory            int64
		nodeCpu               int64
		autoprovisioned       bool
		templateMemory        int64
		templateCpu           int64
		overEstimationMemory  float64
		underEstimationMemory float64
		overEstimationCpu     float64
		underEstimationCpu    float64
		hasError              bool
	}{
		{
			nodeMemory:           1000,
			nodeCpu:              2,
			templateMemory:       1100,
			templateCpu:          2,
			overEstimationMemory: 0.1,
		},
		{
			nodeMemory:           1000,
			nodeCpu:              2,
			autoprovisioned:      true,
			templateMemory:       1100,
			templateCpu:          2,
			overEstimationMemory: 0.1,
		},
		{
			nodeMemory:            1000,
			nodeCpu:               1,
			templateMemory:        900,
			templateCpu:           2,
			underEstimationMemory: 0.1,
			overEstimationCpu:     1,
		},
		{
			nodeMemory:         1000,
			nodeCpu:            2,
			templateMemory:     1000,
			underEstimationCpu: 1,
		},
		{
			nodeMemory:      1000,
			nodeCpu:         1,
			autoprovisioned: true,
		},
		{
			templateMemory: 900,
			templateCpu:    2,
			hasError:       true,
		},
	}

	gkeManagerMock := &gke.GkeManagerMock{}
	gkeManagerMock.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)
	provider, _ := buildGkeCloudProvider(gkeManagerMock)
	ctx := &context.AutoscalingContext{CloudProvider: provider}

	for i, test := range tests {
		machineType := "mT"
		imageType := "cos_containerd"
		diskSize := int64(0)
		spec := gkeclient.NodePoolSpec{
			MachineType: machineType,
			ImageType:   imageType,
			DiskSize:    diskSize,
		}

		ng1 := gke.NewTestGkeMigBuilder().
			SetGceRefName("pool-1").
			SetMaxSize(10).
			SetAutoprovisioned(test.autoprovisioned).
			SetExist(true).
			SetNodePoolName("pool-1").
			SetSpec(&spec).
			SetGkeManager(gkeManagerMock).
			Build()
		templateId := uint64(123)
		ng1Template := &gce_api.InstanceTemplate{Id: templateId}
		gkeManagerMock.On("GetMigForInstance", mock.AnythingOfType("gce.GceRef")).Return(ng1, nil)
		gkeManagerMock.On("GetMigInstanceTemplate", ng1).Return(ng1Template, nil)
		cache := nodetemplate.NewCache()
		p := NewProcessor(cache)
		p.SetContext(ctx)

		node := BuildTestNode("n1", test.nodeCpu*1000, test.nodeMemory)
		node.Spec.ProviderID = "gce://project1/us-central1-b/node-1"

		if test.templateCpu > 0 || test.templateMemory > 0 {
			nodeTemplate := BuildTestNode("n1", test.templateCpu*1000, test.templateMemory)
			cache.Add(nodetemplate.BuildKeyForCA(templateId), nodeTemplate, nodetemplate.LongTTL)

			if test.autoprovisioned {
				key := nodetemplate.BuildKeyForNAP(&spec, imageType, "", "")
				cache.Add(key, nodeTemplate, nodetemplate.LongTTL)
			}
		}

		if err := p.compareNodesWithNodeTemplates([]*apiv1.Node{node}); err != nil && !test.hasError {
			t.Errorf("compareNodesWithNodeTemplates() = %v, want nil", err)
		}

		wantOverestimation := make(map[waKey]float64)
		if test.overEstimationCpu > 0 || test.nodeCpu == test.templateCpu {
			wantOverestimation[waKey{"cpu", "cos", "mT"}] = test.overEstimationCpu
		}
		if test.overEstimationMemory > 0 || test.nodeMemory == test.templateMemory {
			wantOverestimation[waKey{"memory", "cos", "mT"}] = test.overEstimationMemory
		}
		wantUnderestimation := make(map[waKey]float64)
		if test.underEstimationCpu > 0 || test.nodeCpu == test.templateCpu {
			wantUnderestimation[waKey{"cpu", "cos", "mT"}] = test.underEstimationCpu
		}
		if test.underEstimationMemory > 0 || test.nodeMemory == test.templateMemory {
			wantUnderestimation[waKey{"memory", "cos", "mT"}] = test.underEstimationMemory
		}

		if diff := cmp.Diff(wantOverestimation, p.worstAllocatableOverestimation); diff != "" {
			t.Errorf("worstAllocatableOverestimation mismatch for test #%d (-want +got):\n%s", i+1, diff)
		}
		if diff := cmp.Diff(wantUnderestimation, p.worstAllocatableUnderestimation); diff != "" {
			t.Errorf("worstAllocatableUnderestimation mismatch for test #%d (-want +got):\n%s", i+1, diff)
		}
	}
}

func TestWorstAllocatableMultipleNodes(t *testing.T) {
	gkeManagerMock := &gke.GkeManagerMock{}
	gkeManagerMock.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)

	provider, _ := buildGkeCloudProvider(gkeManagerMock)
	cache := nodetemplate.NewCache()
	p := NewProcessor(cache)
	p.SetContext(&context.AutoscalingContext{CloudProvider: provider})

	ref1 := gce.GceRef{
		Project: "project1",
		Zone:    "us-central1-b",
		Name:    "node-1",
	}
	ref2 := gce.GceRef{
		Project: "project1",
		Zone:    "us-central1-b",
		Name:    "node-2",
	}
	refNAP := gce.GceRef{
		Project: "project1",
		Zone:    "us-central1-b",
		Name:    "node-3",
	}

	builder := gke.NewTestGkeMigBuilder().SetMaxSize(10).SetExist(true)
	ng1 := builder.
		SetGceRef(ref1).
		SetNodePoolName("pool-1").
		SetGkeManager(gkeManagerMock).
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "mT1",
			ImageType:   "cos_containerd",
			DiskSize:    int64(10),
		}).Build()
	ng2 := builder.
		SetGceRef(ref2).
		SetNodePoolName("pool-2").
		SetGkeManager(gkeManagerMock).
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "mT2",
			ImageType:   "ubuntu",
			DiskSize:    int64(5),
		}).Build()

	node1 := BuildTestNode("n1", 1000, 1000)
	node1.Spec.ProviderID = "gce://project1/us-central1-b/node-1"
	nodeTemplate1 := BuildTestNode("n1", 1000, 900)
	node2 := BuildTestNode("n2", 1000, 1000)
	node2.Spec.ProviderID = "gce://project1/us-central1-b/node-2"
	nodeTemplate2 := BuildTestNode("n2", 1000, 1100)

	ngNAPSpec := &gkeclient.NodePoolSpec{
		MachineType: "mT2",
		ImageType:   "cos_containerd",
		DiskSize:    int64(0),
	}
	ngNAP := builder.
		SetGceRef(refNAP).
		SetAutoprovisioned(true).
		SetNodePoolName("pool-3").
		SetSpec(ngNAPSpec).Build()
	key := nodetemplate.BuildKeyForNAP(ngNAPSpec, "cos", "1-2-3", "us-central1-b")

	nodeNAP := BuildTestNode("n3", 1000, 1000)
	nodeNAP.Spec.ProviderID = "gce://project1/us-central1-b/node-3"
	nodeNAP.Status.NodeInfo.KubeletVersion = "v1-2-3"
	nodeTemplateNAP := BuildTestNode("n3", 1000, 800)

	ng1InstanceTemplateId := uint64(1)
	ng2InstanceTemplateId := uint64(2)
	ngNAPInstanceTemplateId := uint64(3)
	gkeManagerMock.On("GetMigInstanceTemplate", ng1).Return(&gce_api.InstanceTemplate{Id: ng1InstanceTemplateId}, nil)
	gkeManagerMock.On("GetMigInstanceTemplate", ng2).Return(&gce_api.InstanceTemplate{Id: ng2InstanceTemplateId}, nil)
	gkeManagerMock.On("GetMigInstanceTemplate", ngNAP).Return(&gce_api.InstanceTemplate{Id: ngNAPInstanceTemplateId}, nil)
	cache.Add(nodetemplate.BuildKeyForCA(ng1InstanceTemplateId), nodeTemplate1, nodetemplate.LongTTL)
	cache.Add(nodetemplate.BuildKeyForCA(ng2InstanceTemplateId), nodeTemplate2, nodetemplate.LongTTL)
	cache.Add(key, nodeTemplateNAP, nodetemplate.LongTTL)

	gkeManagerMock.On("GetMigForInstance", ref1).Return(ng1, nil)
	gkeManagerMock.On("GetMigForInstance", ref2).Return(ng2, nil)
	gkeManagerMock.On("GetMigForInstance", refNAP).Return(ngNAP, nil)

	err := p.compareNodesWithNodeTemplates([]*apiv1.Node{node1, node2, nodeNAP})
	if err != nil {
		t.Errorf("compareNodesWithNodeTemplates() = %v, want nil", err)
	}

	wantOverestimation := map[waKey]float64{
		{"memory", "ubuntu", "mT2"}: 0.1,
		{"cpu", "cos", "mT1"}:       0,
		{"cpu", "cos", "mT2"}:       0,
		{"cpu", "ubuntu", "mT2"}:    0,
	}
	wantUnderestimation := map[waKey]float64{
		{"memory", "cos", "mT1"}: 0.1,
		{"memory", "cos", "mT2"}: 0.2,
		{"cpu", "cos", "mT1"}:    0,
		{"cpu", "cos", "mT2"}:    0,
		{"cpu", "ubuntu", "mT2"}: 0,
	}

	if diff := cmp.Diff(wantOverestimation, p.worstAllocatableOverestimation); diff != "" {
		t.Errorf("worstAllocatableOverestimation mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantUnderestimation, p.worstAllocatableUnderestimation); diff != "" {
		t.Errorf("worstAllocatableUnderestimation mismatch (-want +got):\n%s", diff)
	}
}

func TestNodeIsNotConsideredTwoTimes(t *testing.T) {
	gkeManagerMock := &gke.GkeManagerMock{}
	provider, _ := buildGkeCloudProvider(gkeManagerMock)
	cache := nodetemplate.NewCache()
	p := NewProcessor(cache)
	p.SetContext(&context.AutoscalingContext{CloudProvider: provider})
	fakeClock := clock.NewFakePassiveClock(time.Now())
	p.clock = fakeClock

	nodeRef := gce.GceRef{
		Project: "project1",
		Zone:    "us-central1-b",
		Name:    "node",
	}

	builder := gke.NewTestGkeMigBuilder().SetMaxSize(10).SetExist(true)
	nodeGroup := builder.
		SetGceRef(nodeRef).
		SetNodePoolName("pool-1").
		SetGkeManager(gkeManagerMock).
		SetSpec(&gkeclient.NodePoolSpec{
			MachineType: "mT1",
			ImageType:   "cos_containerd",
			DiskSize:    int64(10),
		}).Build()

	node := BuildTestNode("n1", 1000, 1000)
	node.CreationTimestamp = v1.Time{Time: fakeClock.Now().Add(-time.Hour)}
	node.Spec.ProviderID = "gce://project1/us-central1-b/node"
	nodeTemplateFirst := BuildTestNode("n1", 1000, 1000)
	nodeTemplateSecond := BuildTestNode("n1", 1000, 500)
	nodeTemplateThird := BuildTestNode("n1", 1000, 900)

	instanceTemplateId := uint64(123)
	gkeManagerMock.On("GetMigInstanceTemplate", nodeGroup).Return(&gce_api.InstanceTemplate{Id: instanceTemplateId}, nil)
	gkeManagerMock.On("GetMigForInstance", nodeRef).Return(nodeGroup, nil)

	cache.Add(nodetemplate.BuildKeyForCA(instanceTemplateId), nodeTemplateFirst, nodetemplate.LongTTL)
	err := p.compareNodesWithNodeTemplates([]*apiv1.Node{node})
	if err != nil {
		t.Errorf("compareNodesWithNodeTemplates() = %v, want nil", err)
	}

	cache.Cleanup()
	cache.Add(nodetemplate.BuildKeyForCA(instanceTemplateId), nodeTemplateSecond, nodetemplate.LongTTL)
	err = p.compareNodesWithNodeTemplates([]*apiv1.Node{node})
	if err != nil {
		t.Errorf("compareNodesWithNodeTemplates() = %v, want nil", err)
	}

	cache.Cleanup()
	cache.Add(nodetemplate.BuildKeyForCA(instanceTemplateId), nodeTemplateThird, nodetemplate.LongTTL)
	node.CreationTimestamp = v1.Time{Time: fakeClock.Now().Add(24 * time.Hour)}
	err = p.compareNodesWithNodeTemplates([]*apiv1.Node{node})
	if err != nil {
		t.Errorf("compareNodesWithNodeTemplates() = %v, want nil", err)
	}

	wantOverestimation := map[waKey]float64{
		{"cpu", "cos", "mT1"}:    0,
		{"memory", "cos", "mT1"}: 0,
	}
	wantUnderestimation := map[waKey]float64{
		{"memory", "cos", "mT1"}: 0.1,
		{"cpu", "cos", "mT1"}:    0,
	}

	if diff := cmp.Diff(wantOverestimation, p.worstAllocatableOverestimation); diff != "" {
		t.Errorf("worstAllocatableOverestimation mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantUnderestimation, p.worstAllocatableUnderestimation); diff != "" {
		t.Errorf("worstAllocatableUnderestimation mismatch (-want +got):\n%s", diff)
	}
}

func TestErrorCases(t *testing.T) {
	gkeManagerMock := &gke.GkeManagerMock{}
	gkeManagerMock.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)

	provider, _ := buildGkeCloudProvider(gkeManagerMock)
	cache := nodetemplate.NewCache()
	p := NewProcessor(cache)
	p.SetContext(&context.AutoscalingContext{CloudProvider: provider})

	// Node Group is not autoscaled.
	node1 := BuildTestNode("notAutoscalingNode", 1000, 1200)
	node1.Spec.ProviderID = "gce://project1/us-central1-b/node1"
	node1Ref := gce.GceRef{
		Project: "project1",
		Zone:    "us-central1-b",
		Name:    "node1",
	}
	gkeManagerMock.On("GetMigForInstance", node1Ref).Return(nil, nil)

	err := p.compareNodesWithNodeTemplates([]*apiv1.Node{node1})
	if err != nil {
		t.Errorf("compareNodesWithNodeTemplates() = %v, want nil", err)
	}

	// NodeGroupForNode() return error.
	node2 := BuildTestNode("notAutoscalingNode", 1000, 1200)
	node2.Spec.ProviderID = "gce://project1/us-central1-b/node2"
	node2Ref := gce.GceRef{
		Project: "project1",
		Zone:    "us-central1-b",
		Name:    "node2",
	}
	nodeGroupErr := fmt.Errorf("Error")
	resultErr := fmt.Sprintf("1 error(s) in total: Couldn't find NodeGroup for node: %s, error: %v;", node2.Name, nodeGroupErr)
	gkeManagerMock.On("GetMigForInstance", node2Ref).Return(nil, nodeGroupErr)

	err = p.compareNodesWithNodeTemplates([]*apiv1.Node{node2})
	if err.Error() != resultErr {
		t.Errorf("compareNodesWithNodeTemplates() = %v, want %v", err, resultErr)
	}
}

func testResourceSliceForNode(nodeName, sliceName, driverName string, devices ...string) *resourceapi.ResourceSlice {
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: v1.ObjectMeta{Name: sliceName},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   driverName,
			NodeName: &nodeName,
			Pool: resourceapi.ResourcePool{
				Name:               nodeName,
				ResourceSliceCount: 1,
			},
		},
	}
	for _, dev := range devices {
		slice.Spec.Devices = append(slice.Spec.Devices, resourceapi.Device{
			Name: dev,
		})
	}
	return slice
}

func countSlicesByDriver(slices []*resourceapi.ResourceSlice) map[string]int {
	result := map[string]int{}
	for _, slice := range slices {
		result[slice.Spec.Driver]++
	}
	return result
}

// TestDraProcessors tests if DRA-related processors are correctly plumbed into the common CustomResourcesProcessor.
func TestDraProcessors(t *testing.T) {
	now := time.Now()

	nonDraNode1 := BuildTestNode("nonDraNode1", 1, 1)
	SetNodeReadyState(nonDraNode1, true, now)
	nonDraNode1.Annotations = map[string]string{lastAppliedLabelsKey: "something"} // This annotation is needed for ready Nodes, otherwise the LabelsProcessor hacks the readiness.
	nonDraNode2 := BuildTestNode("nonDraNode2", 1, 1)
	SetNodeReadyState(nonDraNode2, false, now)
	draNode1 := BuildTestNode("draNode1", 1, 1)
	draNode1.Annotations = map[string]string{lastAppliedLabelsKey: "something"}
	SetNodeReadyState(draNode1, true, now)
	draNode2 := BuildTestNode("draNode2", 1, 1)
	draNode2.Annotations = map[string]string{lastAppliedLabelsKey: "something"}
	SetNodeReadyState(draNode2, true, now)
	draNode2UnreadyCopy := kubernetes.GetUnreadyNodeCopy(draNode2, kubernetes.ResourceUnready)

	draNode1Slice1 := testResourceSliceForNode("draNode1", "draNode1-slice1", "test-driver-1", "dev1", "dev2")
	draNode1Slice2 := testResourceSliceForNode("draNode1", "draNode1-slice2", "test-driver-2", "dev1", "dev2")
	draNode2Slice1 := testResourceSliceForNode("draNode2", "draNode2-slice1", "test-driver-1", "dev1", "dev2")
	draSlices := map[string][]*resourceapi.ResourceSlice{
		"draNode1": {draNode1Slice1, draNode1Slice2},
		"draNode2": {draNode2Slice1}, // draNode2Slice2 is missing, so draNode2 should have hacked readiness.
	}
	computeDomainDeviceClasses := map[string]*resourceapi.DeviceClass{
		dynamicresources.ComputeDomainChannelDeviceClassName: {
			ObjectMeta: v1.ObjectMeta{
				Name: dynamicresources.ComputeDomainChannelDeviceClassName,
			},
		},
		dynamicresources.ComputeDomainDaemonDeviceClassName: {
			ObjectMeta: v1.ObjectMeta{
				Name: dynamicresources.ComputeDomainDaemonDeviceClassName,
			},
		},
	}
	otherDeviceClasses := map[string]*resourceapi.DeviceClass{
		"abc": {
			ObjectMeta: v1.ObjectMeta{
				Name: "abc",
			},
		},
	}

	// Set up a test cloud provider - 2 node groups, the DRA one has 2 drivers with 2 devices each in TemplateNodeInfo().
	// One of the DRA Nodes doesn't have one of the slices in the DRA snapshot, so it should be hacked to unready.
	provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()
	provider.AddAutoprovisionedNodeGroup("nonDraPool", 0, 100, 2, "machine-type-non-dra")
	provider.AddAutoprovisionedNodeGroup("draPool", 0, 100, 2, "machine-type-dra")
	provider.AddNode("nonDraPool", nonDraNode1)
	provider.AddNode("nonDraPool", nonDraNode2)
	provider.AddNode("draPool", draNode1)
	provider.AddNode("draPool", draNode2)
	provider.SetMachineTemplates(map[string]*framework.NodeInfo{
		"machine-type-non-dra": framework.NewTestNodeInfo(BuildTestNode("nonDraPool-template-node", 1, 1)),
		"machine-type-dra": framework.NewNodeInfo(
			BuildTestNode("draPool-template-node", 1, 1),
			[]*resourceapi.ResourceSlice{
				testResourceSliceForNode("draPool-template-node", "draPool-template-node-slice1", "test-driver-1", "dev1", "dev2"),
				testResourceSliceForNode("draPool-template-node", "draPool-template-node-slice2", "test-driver-2", "dev1", "dev2"),
			},
		),
	})

	for _, tc := range []struct {
		testName         string
		draEnabled       bool
		allNodes         []*apiv1.Node
		readyNodes       []*apiv1.Node
		draDeviceClasses map[string]*resourceapi.DeviceClass

		wantAllNodes                []*apiv1.Node
		wantReadyNodes              []*apiv1.Node
		wantResourceSlicesPerDriver map[string]int
	}{
		{
			testName:         "DRA_enabled_compute_domain_classes_present",
			draEnabled:       true,
			draDeviceClasses: computeDomainDeviceClasses,
			allNodes:         []*apiv1.Node{nonDraNode1, nonDraNode2, draNode1, draNode2},
			readyNodes:       []*apiv1.Node{nonDraNode1, draNode1, draNode2},

			// Assert that the DRA readiness processor is executed as part of the common processor (draNode2 should be hacked to unready).
			wantAllNodes:   []*apiv1.Node{nonDraNode1, nonDraNode2, draNode1, draNode2UnreadyCopy},
			wantReadyNodes: []*apiv1.Node{nonDraNode1, draNode1},
			// Assert that the DRA ResourcePredictor correctly updates its DeviceClass state (it should predict that the ComputeDomain driver is enabled for A4X).
			wantResourceSlicesPerDriver: map[string]int{dynamicresources.ComputeDomainDriver: 1},
		},
		{
			testName:         "DRA_enabled_compute_domain_classes_missing",
			draEnabled:       true,
			draDeviceClasses: otherDeviceClasses,
			allNodes:         []*apiv1.Node{nonDraNode1, nonDraNode2, draNode1, draNode2},
			readyNodes:       []*apiv1.Node{nonDraNode1, draNode1, draNode2},

			// Assert that the DRA readiness processor is executed as part of the common processor (draNode2 should be hacked to unready).
			wantAllNodes:   []*apiv1.Node{nonDraNode1, nonDraNode2, draNode1, draNode2UnreadyCopy},
			wantReadyNodes: []*apiv1.Node{nonDraNode1, draNode1},
			// Assert that the DRA ResourcePredictor correctly updates its DeviceClass state (it should predict that the ComputeDomain driver is disabled for A4X).
			wantResourceSlicesPerDriver: map[string]int{},
		},
		{
			testName:   "DRA_disabled",
			draEnabled: false,
			allNodes:   []*apiv1.Node{nonDraNode1, nonDraNode2, draNode1, draNode2},
			readyNodes: []*apiv1.Node{nonDraNode1, draNode1, draNode2},
			// Assert that the DRA readiness processor is not executed as part of the common processor because DRA is disabled
			wantAllNodes:   []*apiv1.Node{nonDraNode1, nonDraNode2, draNode1, draNode2},
			wantReadyNodes: []*apiv1.Node{nonDraNode1, draNode1, draNode2},
			// Assert that the DRA ResourcePredictor correctly handles DRA disabled and a nil DraSnapshot
			wantResourceSlicesPerDriver: map[string]int{},
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			var draSnapshot *drasnapshot.Snapshot
			if tc.draEnabled {
				draSnapshot = drasnapshot.NewSnapshot(nil, draSlices, nil, tc.draDeviceClasses)
			}

			templateNodeInfoProvider := nodeinfosprovider.NewDefaultTemplateNodeInfoProvider(nil, false)
			templateNodeInfoRegistry := nodeinfosprovider.NewTemplateNodeInfoRegistry(templateNodeInfoProvider)
			autoscalingCtx := &context.AutoscalingContext{
				CloudProvider:            provider,
				AutoscalingOptions:       config.AutoscalingOptions{DynamicResourceAllocationEnabled: tc.draEnabled},
				TemplateNodeInfoRegistry: templateNodeInfoRegistry,
			}
			commonProcessor := NewProcessor(nodetemplate.NewCache())
			commonProcessor.SetContext(autoscalingCtx) // This is needed, otherwise the GPU partitioning processor panics.
			commonProcessor.SetCloudProvider(provider)

			// Assert that the ResourcePredictor is instantiated.
			assert.NotNil(t, commonProcessor.GetDraResourcePredictor())

			// Assert that the result Node lists are as expected.
			gotAllNodes, gotReadyNodes := commonProcessor.FilterOutNodesWithUnreadyResources(autoscalingCtx, tc.allNodes, tc.readyNodes, draSnapshot, csisnapshot.NewEmptySnapshot())
			assert.Equal(t, tc.wantAllNodes, gotAllNodes)
			assert.Equal(t, tc.wantReadyNodes, gotReadyNodes)

			// Assert that ResourcePredictor updates its state in FilterOutNodesWithUnreadyResources() and the result of ResourceSlicesForNode() for A4X is as expected.
			a4XNodeTemplate := BuildTestNode("a4xNodeTemplate", 1, 1)
			a4XNodeTemplate.Labels = map[string]string{labels.MachineFamilyLabel: machinetypes.A4X.Name(), labels.GPULabel: "gpu-type"}
			predictor := commonProcessor.GetDraResourcePredictor()
			resourceSlices, err := predictor.ResourceSlicesForNode(&gkeclient.NodePoolSpec{}, a4XNodeTemplate)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantResourceSlicesPerDriver, countSlicesByDriver(resourceSlices))
		})
	}
}

func TestGetNodeResourceTargets_DraGpuTpu(t *testing.T) {
	gkeManagerMock := &gke.GkeManagerMock{}
	gkeManagerMock.On("GetAutoprovisioningDefaultFamily").Return(machinetypes.N1)
	machineConfigProvider := machinetypes.NewMachineConfigProvider(nil)
	provider, _ := gke.BuildGkeCloudProvider(gkeManagerMock, nil, nil, false, "us-test1", nil, false, false, nil, "", nil, machineConfigProvider, nil, 1000)
	ctx := &context.AutoscalingContext{CloudProvider: provider}
	cache := nodetemplate.NewCache()
	processor := NewProcessor(cache) // Centralized Processor
	processor.SetContext(ctx)
	processor.SetCloudProvider(provider)

	nonDraGpuNode := BuildTestNode("non-dra-gpu", 1000, 1000)
	nonDraGpuNode.Labels[labels.GPULabel] = "nvidia-tesla-t4"
	nonDraGpuNode.Status.Allocatable[apiv1.ResourceName("nvidia.com/gpu")] = *resource.NewQuantity(1, resource.DecimalSI)

	draGpuNode := BuildTestNode("dra-gpu", 1000, 1000)
	draGpuNode.Labels[labels.DraGpuNodeLabel] = "true"
	draGpuNode.Labels[labels.GPULabel] = "nvidia-tesla-t4"

	nonDraTpuNode := BuildTestNode("non-dra-tpu", 1000, 1000)
	nonDraTpuNode.Labels[labels.TPULabel] = gkelabels.TpuV3SliceValue
	nonDraTpuNode.Status.Allocatable[apiv1.ResourceName("google.com/tpu")] = *resource.NewQuantity(8, resource.DecimalSI)

	draTpuNode := BuildTestNode("dra-tpu", 1000, 1000)
	draTpuNode.Labels[labels.DraTpuNodeLabel] = "true"
	draTpuNode.Labels[labels.TPULabel] = gkelabels.TpuV3SliceValue

	tests := []struct {
		name        string
		node        *apiv1.Node
		nodeGroup   cloudprovider.NodeGroup
		wantTargets []customresources.CustomResourceTarget
		wantErr     bool
	}{
		{
			name:      "NonDraGpuNode_ShouldReturnTargetsFromGpuProcessor",
			node:      nonDraGpuNode,
			nodeGroup: nil,
			wantTargets: []customresources.CustomResourceTarget{
				{
					ResourceType:  "nvidia-tesla-t4",
					ResourceCount: 1,
				},
			},
		},
		{
			name: "DraGpuNode_ShouldReturnTargetsFromDraProcessor",
			node: draGpuNode,
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "n1-standard-1",
					Accelerators: []*gke_api_beta.AcceleratorConfig{
						{
							AcceleratorType:  "nvidia-tesla-t4",
							AcceleratorCount: 1,
						},
					},
				}).
				Build(),
			wantTargets: []customresources.CustomResourceTarget{
				{
					ResourceType:  "nvidia-tesla-t4",
					ResourceCount: 1,
				},
			},
		},
		{
			name:      "NonDraTpuNode_ShouldReturnTargetsFromTpuProcessor",
			node:      nonDraTpuNode,
			nodeGroup: nil,
			wantTargets: []customresources.CustomResourceTarget{
				{
					ResourceType:  gkelabels.TpuV3SliceValue,
					ResourceCount: 8,
				},
			},
		},
		{
			name: "DraTpuNode_ShouldReturnTargetsFromDraProcessor",
			node: draTpuNode,
			nodeGroup: gke.NewTestGkeMigBuilder().
				SetSpec(&gkeclient.NodePoolSpec{
					MachineType: "ct5lp-hightpu-8t",
				}).
				Build(),
			wantTargets: []customresources.CustomResourceTarget{
				{
					ResourceType:  gkelabels.TpuV3SliceValue,
					ResourceCount: 8,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			targets, err := processor.GetNodeResourceTargets(ctx, tc.node, tc.nodeGroup)

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.ElementsMatch(t, tc.wantTargets, targets)
		})
	}
}

func buildGkeCloudProvider(gkeManager gke.GkeManager) (cloudprovider.CloudProvider, error) {
	return gke.BuildGkeCloudProvider(gkeManager, nil, nil, false, "us-test1", nil, false, false, nil, "", nil, nil, nil, 1000)
}
