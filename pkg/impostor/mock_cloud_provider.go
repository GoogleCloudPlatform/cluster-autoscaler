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

package impostor

import (
	"context"
	"fmt"
	"strconv"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gceprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	units "k8s.io/autoscaler/cluster-autoscaler/utils/units"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"

	klog "k8s.io/klog/v2"
)

const (
	// fluentd, prometheus-to-sd
	daemonSetPods = 2
	// 100m fluentd, 3m prometheus-to-sd
	daemonSetMilliCPU = 103
	// 200Mi fluentd, 20Mi prometheus-to-sd
	daemonSetMemory = 220 * units.MiB
	// Maximum number of pod per node
	maxPodPerNodeCount = 110
)

type machineTypeSize struct {
	millicpu int64
	mem      float64
}

var machineTypeSizes = map[string]machineTypeSize{
	"f1-micro":       {1000, 0.6 * units.GiB},
	"g1-small":       {1000, 1.7 * units.GiB},
	"n1-standard-1":  {1000, 3.75 * units.GiB},
	"n1-standard-2":  {2000, 7.5 * units.GiB},
	"n1-standard-4":  {4000, 15 * units.GiB},
	"n1-standard-8":  {8000, 30 * units.GiB},
	"n1-standard-16": {16000, 60 * units.GiB},
	"n1-standard-32": {32000, 120 * units.GiB},
	"n1-standard-64": {64000, 240 * units.GiB},
	"n1-highcpu-2":   {2000, 1.8 * units.GiB},
	"n1-highcpu-4":   {4000, 3.6 * units.GiB},
	"n1-highcpu-8":   {8000, 7.2 * units.GiB},
	"n1-highcpu-16":  {16000, 14.4 * units.GiB},
	"n1-highcpu-32":  {32000, 28.8 * units.GiB},
	"n1-highcpu-64":  {64000, 57.6 * units.GiB},
	"n1-highmem-2":   {2000, 13 * units.GiB},
	"n1-highmem-4":   {4000, 26 * units.GiB},
	"n1-highmem-8":   {8000, 52 * units.GiB},
	"n1-highmem-16":  {16000, 104 * units.GiB},
	"n1-highmem-32":  {32000, 208 * units.GiB},
	"n1-highmem-64":  {64000, 416 * units.GiB},
}

// BuildTestGcpNode creates test node based on machine type resources
func BuildTestGcpNode(nodeName, machineType string, gpusCount int64, maxPodPerNodeCount int64) *apiv1.Node {
	node := &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:     nodeName,
			SelfLink: fmt.Sprintf("/api/v1/nodes/%s", nodeName),
			Labels:   map[string]string{},
		},
		Spec: apiv1.NodeSpec{
			ProviderID: nodeName,
		},
		Status: apiv1.NodeStatus{
			Capacity:    apiv1.ResourceList{},
			Allocatable: apiv1.ResourceList{},
		},
	}

	node.Labels[apiv1.LabelInstanceType] = machineType

	sizeInfo, found := machineTypeSizes[machineType]
	if !found {
		panic(machineType)
	}
	millicpu := sizeInfo.millicpu
	mem := int64(sizeInfo.mem)
	evictionHard := gceprovider.ParseEvictionHardOrGetDefault(nil)

	node.Status.Capacity[apiv1.ResourcePods] = *resource.NewQuantity(maxPodPerNodeCount, resource.DecimalSI)
	node.Status.Allocatable[apiv1.ResourcePods] = *resource.NewQuantity(maxPodPerNodeCount-daemonSetPods, resource.DecimalSI)

	node.Status.Capacity[apiv1.ResourceCPU] = *resource.NewMilliQuantity(millicpu, resource.DecimalSI)
	allocatableMilliCPU := millicpu - gke.PredictKubeReservedCpuMillicores(millicpu, machineType, gkelabels.DefaultMaxPodsPerNode)
	allocatableMilliCPU -= daemonSetMilliCPU
	node.Status.Allocatable[apiv1.ResourceCPU] = *resource.NewMilliQuantity(allocatableMilliCPU, resource.DecimalSI)

	r := gceprovider.GceReserved{}
	migOsInfo := gceprovider.NewMigOsInfo(gceprovider.OperatingSystemLinux, gceprovider.OperatingSystemDistributionDefault, gceprovider.DefaultArch)
	capacityMemory := mem - r.CalculateKernelReserved(migOsInfo, mem)
	node.Status.Capacity[apiv1.ResourceMemory] = *resource.NewQuantity(capacityMemory, resource.DecimalSI)
	allocatableMemory := capacityMemory - gke.PredictKubeReservedMemory(mem, false) - gceprovider.GetKubeletEvictionHardForMemory(evictionHard)
	allocatableMemory -= daemonSetMemory
	node.Status.Allocatable[apiv1.ResourceMemory] = *resource.NewQuantity(allocatableMemory, resource.DecimalSI)

	if gpusCount > 0 {
		testutils.AddGpusToNode(node, gpusCount)
	}

	readyCondition := apiv1.NodeCondition{
		Type:   apiv1.NodeReady,
		Status: apiv1.ConditionTrue,
	}
	node.Status.Conditions = append(node.Status.Conditions, readyCondition)

	return node
}

// MockCloudProvider is a mock cloud provider simulating GKE cloud provider.
type MockCloudProvider struct {
	gke.TestAutoprovisioningCloudProvider
	machineTemplates       map[string]*framework.NodeInfo
	kubeClient             kube_client.Interface
	scaleDownUtilThreshold float64
	scaleDownUnneededTime  time.Duration
	autopilotEnabled       bool
}

// NewMockCloudProvider builds new MockCloudProvider with autoprovisioning support.
func NewMockCloudProvider(kubeClient kube_client.Interface, scaleDownUtilThreshold float64, scaleDownUnneededTime time.Duration, maxPodPerNodeCount int64, autopilotEnabled bool, autoprovisioningEnabled bool) *MockCloudProvider {
	machineTemplates := make(map[string]*framework.NodeInfo, len(machineTypeSizes))
	var machineTypes []string
	for machineType := range machineTypeSizes {
		node := BuildTestGcpNode(machineType+"-template", machineType, 0, maxPodPerNodeCount)
		nodeInfo := framework.NewTestNodeInfo(node)
		machineTemplates[machineType] = nodeInfo
		if machineType != "f1-micro" && machineType != "g1-small" {
			machineTypes = append(machineTypes, machineType)
		}
	}

	testAutoprovisioningCloudProvider := *gke.NewTestAutoprovisioningCloudProviderBuilder().
		WithMachineTypes(machineTypes...).
		WithMachineTemplates(machineTemplates).
		WithNodeLocations("us-central1-c").
		WithAutoprovisioningLocations("us-central1-c").
		WithAllZones("us-central1-a", "us-central1-b", "us-central1-c").
		WithMachineTypesPerZone(map[string][]string{
			"us-central1-a": machineTypes,
			"us-central1-b": machineTypes,
			"us-central1-c": machineTypes,
		}).
		WithPricingModel(gceprovider.NewGcePriceModel(gke.NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), false), localssdsize.NewSimpleLocalSSDProvider())).
		WithAutoprovisioningEnabled(autoprovisioningEnabled).
		WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
		Build()

	return &MockCloudProvider{
		TestAutoprovisioningCloudProvider: testAutoprovisioningCloudProvider,
		machineTemplates:                  machineTemplates,
		kubeClient:                        kubeClient,
		scaleDownUtilThreshold:            scaleDownUtilThreshold,
		scaleDownUnneededTime:             scaleDownUnneededTime,
		autopilotEnabled:                  autopilotEnabled,
	}
}

// GPULabel returns the label added to nodes with GPU resource.
func (mcp *MockCloudProvider) GPULabel() string {
	return gkelabels.GPULabel
}

// GetAvailableGPUTypes return all available GPU types cloud provider supports
func (mcp *MockCloudProvider) GetAvailableGPUTypes() map[string]struct{} {
	result := make(map[string]struct{})
	machineConfigProvider := machinetypes.NewMachineConfigProvider(nil)
	for _, gpuType := range machineConfigProvider.GetAllGpuTypes() {
		result[gpuType.Name()] = struct{}{}
	}
	return result
}

// NewNodeGroup builds a theoretical node group based on the node definition provided. The node group is not automatically
// created on the cloud provider side. The node group is not returned by NodeGroups() until it is created.
func (mcp *MockCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string,
	taints []apiv1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {

	nodePoolName := fmt.Sprintf("nap-%s-%s", machineType, gke.GenerateRandomId(8))

	targetLabels := make(map[string]string)

	for key, value := range systemLabels {
		targetLabels[key] = value
	}
	for key, value := range labels {
		targetLabels[key] = value
	}
	targetLabels[apiv1.LabelInstanceType] = machineType

	if gpuRequest, found := extraResources[gpu.ResourceNvidiaGPU]; found {
		gpuType, found := systemLabels[gkelabels.GPULabel]
		if !found {
			klog.V(5).Infof("GPULabel not found")
			return nil, cloudprovider.ErrIllegalConfiguration
		}
		gpuPartitionSize := systemLabels[gkelabels.GPUPartitionSizeLabel]
		gpuMaxSharedClients := systemLabels[gkelabels.GPUMaxSharedClientsLabel]
		sizeInfo, found := machineTypeSizes[machineType]
		if !found {
			klog.V(5).Infof("Couldn't find cpus and mem for machineType %s", machineType)
			return nil, cloudprovider.ErrIllegalConfiguration
		}
		gpuCount := gpuRequest.Value()
		err := mcp.MachineConfigProvider().ValidateGpuForMachineType(gpuType, gpuPartitionSize, gpuMaxSharedClients, machineType, machinetypes.AllocatableGpuCount(gpuCount), sizeInfo.millicpu/1000, int64(sizeInfo.mem))
		if err != nil {
			klog.V(5).Infof("Invalid acceleratorType configuration: %v", err)
			return nil, cloudprovider.ErrIllegalConfiguration
		}
		nodePoolName = fmt.Sprintf("nap-%s-gpu%d-%s", machineType, gpuCount, gke.GenerateRandomId(8))

		taint := apiv1.Taint{
			Effect: apiv1.TaintEffectNoSchedule,
			Key:    gpu.ResourceNvidiaGPU,
			Value:  "present",
		}
		taints = append(taints, taint)
	}

	return NewMockNodeGroup(mcp, nodePoolName, 1000, 0, 0, false, true, machineType,
		targetLabels, taints, extraResources, nil, mcp.kubeClient, mcp.scaleDownUtilThreshold, mcp.scaleDownUnneededTime), nil
}

// BuildNodeGroup returns a mock node group.
func (mcp *MockCloudProvider) BuildNodeGroup(id string, min, max, size int, autoprovisioned bool, machineType string) *MockNodeGroup {
	return NewMockNodeGroup(mcp, id, max, min, size, true, autoprovisioned, machineType, nil, nil, nil, nil, mcp.kubeClient, mcp.scaleDownUtilThreshold, mcp.scaleDownUnneededTime)
}

// AddNodeGroup adds node group to mock cloud provider.
func (mcp *MockCloudProvider) AddNodeGroup(id string, min int, max int, size int) {
	nodeGroup := mcp.BuildNodeGroup(id, min, max, size, false, "")
	mcp.InsertNodeGroup(nodeGroup)
}

// AddAutoprovisionedNodeGroup adds node group to mock cloud provider.
func (mcp *MockCloudProvider) AddAutoprovisionedNodeGroup(id string, min int, max int, size int, machineType string) *MockNodeGroup {
	nodeGroup := mcp.BuildNodeGroup(id, min, max, size, true, machineType)
	mcp.InsertNodeGroup(nodeGroup)
	return nodeGroup
}

// IsAutopilotEnabled returns true if autopilot is enabled
func (mcp *MockCloudProvider) IsAutopilotEnabled() bool {
	return mcp.autopilotEnabled
}

// MockNodeGroup is a node group used by MockCloudProvider.
type MockNodeGroup struct {
	testprovider.TestNodeGroup
	annotations    map[string]string
	cloudProvider  *MockCloudProvider
	machineType    string
	extraResources map[string]resource.Quantity
	kubeClient     kube_client.Interface
}

// NewMockNodeGroup build a MockNodeGroup.
func NewMockNodeGroup(cloudProvider *MockCloudProvider, id string, maxSize, minSize, targetSize int,
	exist bool, autoprovisioned bool, machineType string,
	labels map[string]string, taints []apiv1.Taint, extraResources map[string]resource.Quantity, annotations map[string]string,
	kubeClient kube_client.Interface, scaleDownUtilThreshold float64, scaleDownUnneededTime time.Duration) *MockNodeGroup {
	ng := &MockNodeGroup{
		TestNodeGroup:  *testprovider.NewTestNodeGroup(id, maxSize, minSize, targetSize, exist, autoprovisioned, machineType, labels, taints),
		annotations:    annotations,
		cloudProvider:  cloudProvider,
		machineType:    machineType,
		extraResources: extraResources,
		kubeClient:     kubeClient,
	}
	ng.TestNodeGroup.SetOptions(&config.NodeGroupAutoscalingOptions{ScaleDownUnneededTime: scaleDownUnneededTime, ScaleDownUtilizationThreshold: scaleDownUtilThreshold})
	return ng
}

// NodePoolName returns the name of the GKE node pool this Mig belongs to.
func (mng *MockNodeGroup) NodePoolName() string {
	return mng.Id()
}

// SetAnnotations set annotations of the node group.
func (mng *MockNodeGroup) SetAnnotations(annotations map[string]string) {
	mng.Lock()
	defer mng.Unlock()
	mng.annotations = annotations
}

// Annotations return annotations of the node group.
func (mng *MockNodeGroup) Annotations() map[string]string {
	return mng.annotations
}

// Create creates the node group on the cloud provider side.
func (mng *MockNodeGroup) Create() (cloudprovider.NodeGroup, error) {
	if mng.Exist() {
		return nil, fmt.Errorf("group already exist")
	}
	newNodeGroup := mng.cloudProvider.AddAutoprovisionedNodeGroup(mng.Id(), mng.MinSize(), mng.MaxSize(), 0, mng.machineType)
	return newNodeGroup, nil
}

// Delete deletes the node group on the cloud provider side.
// This will be executed only for autoprovisioned node groups, once their size drops to 0.
func (mng *MockNodeGroup) Delete() error {
	mng.cloudProvider.DeleteNodeGroup(mng.Id())
	return nil
}

// TemplateNodeInfo returns a node template for this node group.
func (mng *MockNodeGroup) TemplateNodeInfo() (*framework.NodeInfo, error) {
	if mng.cloudProvider.machineTemplates == nil {
		return nil, cloudprovider.ErrNotImplemented
	}
	template, found := mng.cloudProvider.machineTemplates[mng.machineType]
	if !found {
		return nil, fmt.Errorf("no template declared for %s", mng.machineType)
	}

	node := template.Node().DeepCopy()
	node.Spec.Taints = mng.Taints()
	node.Labels = mng.Labels()
	node.Annotations = mng.Annotations()
	if gpuRequest, found := mng.extraResources[gpu.ResourceNvidiaGPU]; found {
		node.Status.Capacity[gpu.ResourceNvidiaGPU] = gpuRequest.DeepCopy()
		node.Status.Allocatable[gpu.ResourceNvidiaGPU] = gpuRequest.DeepCopy()
	}
	if node.Annotations != nil {
		ssdSizeProvider := localssdsize.NewSimpleLocalSSDProvider()
		localSSDSize := ssdSizeProvider.SSDSizeInGiB(mng.machineType)
		buildEphemeralStorageCapacity(node, int64(localSSDSize))
	}

	newtemplate := framework.NewTestNodeInfo(node)
	for _, pod := range template.Pods() {
		newtemplate.AddPod(framework.NewPodInfo(pod.Pod, nil))
	}

	return newtemplate, nil
}

// DeleteNodes deletes provided mockNodes from node group.
func (mng *MockNodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	for _, node := range nodes {
		if err := mng.kubeClient.CoreV1().Nodes().Delete(context.Background(), node.Name, metav1.DeleteOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// TODO(b/517096955): Implement AtomicIncreaseSize
// ref: https://github.com/kubernetes/autoscaler/commit/9cdced4cfd7a4aef9bdf23d56ee430085d13b57f
func (mng *MockNodeGroup) AtomicIncreaseSize(delta int) error {
	return cloudprovider.ErrNotImplemented
}

func buildEphemeralStorageCapacity(node *apiv1.Node, localSSDSizeInGib int64) {
	ehpStrOnLocalSsd, _ := strconv.ParseBool(node.Annotations[gceprovider.EphemeralStorageLocalSsdAnnotation])
	if ehpStrOnLocalSsd {
		localSsdCount, err := strconv.ParseInt(node.Annotations[gceprovider.LocalSsdCountAnnotation], 10, 64)
		if err != nil {
			return
		}
		node.Status.Allocatable[apiv1.ResourceEphemeralStorage] = *resource.NewQuantity(localSsdCount*localSSDSizeInGib*units.GB, resource.DecimalSI)
	} else {
		diskSize, err := strconv.ParseInt(node.Annotations[gceprovider.BootDiskSizeAnnotation], 10, 64)
		if err != nil {
			return
		}
		node.Status.Allocatable[apiv1.ResourceEphemeralStorage] = *resource.NewQuantity(diskSize, resource.DecimalSI)
	}
}
