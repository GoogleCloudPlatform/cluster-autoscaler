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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	gceprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	kubernetesutils "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/klog/v2"
)

func init() {
	klog.InitFlags(nil)
}

var (
	defaultUtilizationThreshold  = 0.85
	defaultScaleDownUnneededTime = 1 * time.Minute
)

// Cluster wraps MockCloudProvider with helper methods and listers
type Cluster struct {
	pods                   *sync.Map
	nodes                  *sync.Map
	provider               *MockCloudProvider
	autoscaler             *TestAutoscaler
	location               string
	kubeClient             *MockKubeClient
	scaleDownUtilThreshold float64
	scaleDownUnneededTime  time.Duration
	lastNodeNum            int64
}

// ClusterOption can be used to customize customClusterParameters
type ClusterOption func(cluster *customClusterParameters)

type customClusterParameters struct {
	MaxPodPerNodeCount      int64
	AutopilotEnabled        bool
	AutoprovisioningEnabled bool
}

// NewParameterizedCluster constructs a Cluster instance based on provided arguments.
func NewParameterizedCluster(kubeClient *MockKubeClient, provider *MockCloudProvider,
	autoscaler *TestAutoscaler, pods *sync.Map, nodes *sync.Map,
	scaleDownUtilThreshold float64, scaleDownUnneededTime time.Duration) *Cluster {

	locations := provider.GetAutoprovisioningLocations()
	if len(locations) != 1 {
		panic(locations)
	}
	maxLimits := make(map[string]int64)
	maxLimits[machinetypes.DeprecatedDefaultGPU] = 1000000
	provider.SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, maxLimits))

	return &Cluster{
		pods:                   pods,
		nodes:                  nodes,
		provider:               provider,
		autoscaler:             autoscaler,
		location:               locations[0],
		kubeClient:             kubeClient,
		scaleDownUtilThreshold: scaleDownUtilThreshold,
		scaleDownUnneededTime:  scaleDownUnneededTime,
	}
}

// NewCluster constructs a Cluster instance
func NewCluster(opts ...ClusterOption) *Cluster {
	clusterParameters := &customClusterParameters{
		MaxPodPerNodeCount:      maxPodPerNodeCount,
		AutopilotEnabled:        false,
		AutoprovisioningEnabled: true,
	}
	for _, opt := range opts {
		opt(clusterParameters)
	}
	pods := &sync.Map{}
	nodes := &sync.Map{}
	kubeClient, _ := NewMockKubeClient(pods, nil)
	provider := NewMockCloudProvider(kubeClient, defaultUtilizationThreshold, defaultScaleDownUnneededTime, clusterParameters.MaxPodPerNodeCount, clusterParameters.AutopilotEnabled, clusterParameters.AutoprovisioningEnabled)
	autoscaler := DefaultTestAutoscaler(provider)
	return NewParameterizedCluster(kubeClient, provider, autoscaler, pods, nodes, defaultUtilizationThreshold, defaultScaleDownUnneededTime)
}

// WithCustomMaxPodPerNodeCount sets custom maxPodPerNodeCount to the customClusterParameters
func WithCustomMaxPodPerNodeCount(maxPodPerNodeCount int64) ClusterOption {
	return func(c *customClusterParameters) {
		c.MaxPodPerNodeCount = maxPodPerNodeCount
	}
}

// WithAutopilotEnabled set the AutopilotEnabled to true
func WithAutopilotEnabled(enabled bool) ClusterOption {
	return func(c *customClusterParameters) {
		c.AutopilotEnabled = enabled
	}
}

func WithAutoprovisioningEnabled(enabled bool) ClusterOption {
	return func(c *customClusterParameters) {
		c.AutoprovisioningEnabled = enabled
	}
}

// Provider returns internal cloud provider
func (c *Cluster) Provider() *MockCloudProvider {
	return c.provider
}

// Autoscaler returns internal test autoscaler
func (c *Cluster) Autoscaler() *TestAutoscaler {
	return c.autoscaler
}

// AddNodes adds node[s] directly skipping node groups
func (c *Cluster) AddNodes(newNodes ...*apiv1.Node) {
	for _, node := range newNodes {
		c.lastNodeNum++
		if _, found := c.nodes.Load(node.Name); found {
			panic(node)
		}
		c.nodes.Store(node.Name, node)
		c.kubeClient.AddNode(node)
		c.scheduleDaemonSetsOnNode(node)
	}
}

// AddPods adds pod[s] directly
func (c *Cluster) AddPods(newPods ...*apiv1.Pod) {
	for _, pod := range newPods {
		podID := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
		if _, found := c.pods.Load(pod.Name); found {
			panic(pod.Name)
		}
		if pod.Spec.NodeName != "" {
			if _, found := c.nodes.Load(pod.Spec.NodeName); !found {
				panic(pod)
			}
		}
		c.pods.Store(podID, pod)
	}
}

// AddPodsWithCustomEvictionTime adds new pods with specified time of eviction.
func (c *Cluster) AddPodsWithCustomEvictionTime(evictionTime time.Duration, newPods ...*apiv1.Pod) {
	c.AddPods(newPods...)
	c.kubeClient.AddPods(evictionTime, newPods)
}

// NodeLister returns NodeLister for that cluster
func (c *Cluster) NodeLister() kubernetesutils.NodeLister {
	return c.kubeClient.listers.nodeLister
}

// PodLister returns PodLister for that cluster
func (c *Cluster) PodLister() kubernetesutils.PodLister {
	return c.kubeClient.listers.podLister
}

// buildNodeGroup creates empty node group of given machine types
func (c *Cluster) buildNodeGroup(name string, machineType string, gpusCount int64, autoscaled bool, workload string, extraLabels map[string]string) *MockNodeGroup {
	maxLimit := 0
	if autoscaled {
		maxLimit = 100000
	}
	labels := map[string]string{
		apiv1.LabelZoneFailureDomain:       c.location,
		apiv1.LabelZoneFailureDomainStable: c.location,
		apiv1.LabelInstanceType:            machineType,
		apiv1.LabelInstanceTypeStable:      machineType,
	}
	for lKey, lVal := range extraLabels {
		labels[lKey] = lVal
	}
	var taints []apiv1.Taint
	var extraResources map[string]resource.Quantity

	if workload != "" {
		taints = append(taints, apiv1.Taint{
			Key:    "workload",
			Value:  workload,
			Effect: "NoSchedule",
		})
		labels["workload"] = workload
	}

	if gpusCount > 0 {
		taints = append(taints, apiv1.Taint{
			Key:    gpu.ResourceNvidiaGPU,
			Value:  "present",
			Effect: "NoSchedule",
		})
		if extraResources == nil {
			extraResources = make(map[string]resource.Quantity)
		}
		extraResources[gpu.ResourceNvidiaGPU] = *resource.NewQuantity(gpusCount, resource.DecimalSI)
		labels[gceprovider.GPULabel] = machinetypes.DeprecatedDefaultGPU
	}

	return NewMockNodeGroup(c.provider, name, maxLimit, 0, 0, true, false, machineType, labels, taints, extraResources, nil, c.kubeClient, c.scaleDownUtilThreshold, c.scaleDownUnneededTime)
}

// AddNodeGroup adds empty node group of given machine types
func (c *Cluster) AddNodeGroup(name string, machineType string, gpusCount int64, autoscaled bool) {
	c.addNodeGroupWithCustomLabels(name, machineType, gpusCount, autoscaled, nil)
}

func (c *Cluster) addNodeGroupWithCustomLabels(name, machineType string, gpusCount int64, autoscaled bool, labels map[string]string) {
	existing := c.provider.GetNodeGroup(name)
	if existing != nil {
		panic(name)
	}
	group := c.buildNodeGroup(name, machineType, gpusCount, autoscaled, "", labels)
	c.provider.InsertNodeGroup(group)
}

// AddDedicatedNodeGroup adds empty group marked as dedicated
func (c *Cluster) AddDedicatedNodeGroup(name string, machineType string, workload string, gpusCount int64) {
	existing := c.provider.GetNodeGroup(name)
	if existing != nil {
		panic(name)
	}
	group := c.buildNodeGroup(name, machineType, gpusCount, true, workload, nil)
	c.provider.InsertNodeGroup(group)
}

// GetNodeGroup return NodeGroup by its name
func (c *Cluster) GetNodeGroup(name string) cloudprovider.NodeGroup {
	pool := c.provider.GetNodeGroup(name)
	if pool == nil {
		panic(name)
	}
	return pool
}

// NodeGroups lists all node groups
func (c *Cluster) NodeGroups() []cloudprovider.NodeGroup {
	return c.provider.NodeGroups()
}

// BuildNodeForGroup creates a node for given node group template
func (c *Cluster) BuildNodeForGroup(nodeName, poolName string) *apiv1.Node {
	group := c.provider.GetNodeGroup(poolName)
	if group == nil {
		panic(poolName)
	}
	nodeInfo, err := group.TemplateNodeInfo()
	if err != nil {
		panic(err)
	}
	node := nodeInfo.Node().DeepCopy()
	node.Name = nodeName
	node.Labels[apiv1.LabelZoneFailureDomain] = c.location
	return node
}

// ScaleUpNodeGroup creates new nodes in given node group
func (c *Cluster) ScaleUpNodeGroup(name string, delta int) []*apiv1.Node {
	if delta < 0 {
		panic(delta)
	}
	group := c.provider.GetNodeGroup(name)
	if group == nil {
		panic(name)
	}
	testGroup := group.(*MockNodeGroup)
	targetSize, err := testGroup.TargetSize()
	if err != nil {
		panic(err)
	}
	testGroup.SetTargetSize(targetSize + delta)
	var nodes []*apiv1.Node
	for i := 0; i < delta; i++ {
		nodeName := fmt.Sprintf("%s-no%d", name, c.lastNodeNum)
		node := c.BuildNodeForGroup(nodeName, name)
		c.AddNodes(node)
		c.provider.AddNode(name, node)
		nodes = append(nodes, node)
	}
	return nodes
}

// AddOrScaleUpNodeGroup creates new node group if needed and nodes in it
func (c *Cluster) AddOrScaleUpNodeGroup(name, machineType string, size int, autoscaled bool) []*apiv1.Node {
	return c.AddOrScaleUpNodeGroupWithCustomLabels(name, machineType, size, autoscaled, nil)
}

// AddOrScaleUpNodeGroupWithCustomLabels creates new node group with custom labels if needed and nodes in it
func (c *Cluster) AddOrScaleUpNodeGroupWithCustomLabels(name, machineType string, size int, autoscaled bool, labels map[string]string) []*apiv1.Node {
	existing := c.provider.GetNodeGroup(name)
	if existing == nil {
		initialGpuCount := 0
		machineType := machineType
		if gpuIdx := strings.LastIndex(name, "gpu"); gpuIdx > 0 {
			var err error
			initialGpuCount, err = strconv.Atoi(name[len(name)-1:])
			if err != nil {
				panic(err)
			}
			machineType = machineType[0 : gpuIdx-1]
		}
		machineType = strings.Replace(machineType, "nap-", "", 1)
		if gpuIdx := strings.LastIndex(machineType, "gpu"); gpuIdx > 0 {
			panic(machineType)
		}
		c.addNodeGroupWithCustomLabels(name, machineType, int64(initialGpuCount), autoscaled, labels)
	}
	return c.ScaleUpNodeGroup(name, size)
}

// FillUpNodesPartially fills up mockNodes with numberOfPods pods up to nodeMemTargetUtilization and nodeCpuTargetUtilization percentage. It also sets podTerminationTime and controller for all created pods.
func (c *Cluster) FillUpNodesPartially(nodes []*apiv1.Node, nodeMemTargetUtilization, nodeCPUTargetUtilization int64,
	numberOfPods int, podTerminationTime time.Duration, controller string,
	numOfControllers int, namespace string, labels map[string]string) {
	var podsToAdd []*apiv1.Pod
	var controllers []string
	if controller == "DaemonSet" {
		c.AddDS(nodes, podTerminationTime, map[string]string{}, namespace, nodeCPUTargetUtilization, nodeMemTargetUtilization)
		return
	}
	for i := 0; i < numOfControllers; i++ {
		baseName := fmt.Sprintf("%v-%d", rand.String(10), i)
		c := c.SetUpController(baseName, namespace, controller)
		controllers = append(controllers, c)
	}
	for _, node := range nodes {
		nodeCPU := node.Status.Allocatable[apiv1.ResourceCPU]
		nodeMem := node.Status.Allocatable[apiv1.ResourceMemory]
		podsCPU := float64(nodeCPU.MilliValue()) * float64(nodeCPUTargetUtilization) / 100.0 / float64(numberOfPods)
		podsMem := float64(nodeMem.Value()) * float64(nodeMemTargetUtilization) / 100.0 / float64(numberOfPods)
		for i := 0; i < numberOfPods; i++ {
			cId := rand.Intn(numOfControllers)
			controllerName := controllers[cId]
			pod := constructPod(namespace, node.Name, controllerName, controller, labels, podTerminationTime, int64(podsMem), int64(podsCPU))
			podsToAdd = append(podsToAdd, pod)
			c.updateReplicasInController(controllerName, namespace, controller)
		}
	}
	c.AddPodsWithCustomEvictionTime(podTerminationTime, podsToAdd...)
}

// AddDS adds a DaemonSet with provided node selector labels and podTerminationTime.
// It will schedule ds pods on provided nodes if they match provided selector.
func (c *Cluster) AddDS(nodes []*apiv1.Node, podTerminationTime time.Duration, dsNodeSelectorLabels map[string]string, namespace string, cCpu, cMem int64) {
	ds := c.buildDS(dsNodeSelectorLabels, namespace, cCpu, cMem, podTerminationTime)
	c.kubeClient.listers.daemonSetLister.Add(ds)
	var podsToAdd []*apiv1.Pod
	selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
	if err != nil {
		return
	}
	for _, node := range nodes {
		if len(dsNodeSelectorLabels) == 0 || selector.Matches(labels.Set(node.Labels)) {
			pod := constructPod(namespace, node.Name, ds.Name, "DaemonSet", dsNodeSelectorLabels, podTerminationTime, cMem, cCpu)
			podsToAdd = append(podsToAdd, pod)
		}
	}
	c.AddPodsWithCustomEvictionTime(podTerminationTime, podsToAdd...)
}

func (c *Cluster) buildDS(dsNodeSelectorLabels map[string]string, namespace string, cCpu, cMem int64, podTerminationTime time.Duration) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("ds-%v", rand.String(10)),
			Namespace: namespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: dsNodeSelectorLabels,
			},
			Template: apiv1.PodTemplateSpec{Spec: apiv1.PodSpec{
				TerminationGracePeriodSeconds: proto.Int64(int64(podTerminationTime.Seconds())),
				Containers: []apiv1.Container{
					{
						Resources: apiv1.ResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceCPU:    *resource.NewMilliQuantity(cCpu, resource.DecimalSI),
								apiv1.ResourceMemory: *resource.NewQuantity(cMem, resource.DecimalSI),
							},
						},
					},
				},
			}},
		}}
}

func (c *Cluster) scheduleDaemonSetsOnNode(node *apiv1.Node) {
	dSets, err := c.kubeClient.listers.daemonSetLister.List(labels.NewSelector())
	if err != nil {
		panic("Could not list deamon sets")
	}
	for _, ds := range dSets {
		selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
		if err != nil {
			return
		}
		if len(ds.Spec.Selector.MatchLabels) == 0 || selector.Matches(labels.Set(node.Labels)) {
			if len(ds.Spec.Template.Spec.Containers) != 1 {
				return
			}
			container := ds.Spec.Template.Spec.Containers[0]
			cCpu := container.Resources.Requests[apiv1.ResourceCPU]
			cMem := container.Resources.Requests[apiv1.ResourceMemory]
			pod := constructPod(ds.Namespace, node.Name, ds.Name, "DaemonSet", ds.Spec.Selector.MatchLabels, 0*time.Second, cMem.Value(), cCpu.MilliValue())
			gracePeriod := *ds.Spec.Template.Spec.TerminationGracePeriodSeconds
			c.AddPodsWithCustomEvictionTime(time.Duration(gracePeriod/2 /*Actual eviction time is gracePeriod>Seconds/2*/)*time.Second, pod)
		}
	}
}

func constructPod(namespace, nodeName, controller, controllerType string, labels map[string]string, terminationTime time.Duration, mem, cpu int64) *apiv1.Pod {
	randomSuffix := rand.String(podSuffixLen)
	podName := fmt.Sprintf("filler-%s-%s", nodeName, randomSuffix)
	pod := testutils.BuildTestPod(podName, cpu, mem)
	pod.Namespace = namespace
	pod.Spec.NodeName = nodeName
	pod.UID = types.UID(fmt.Sprintf("%v/%v", pod.Namespace, pod.Name))
	pod.OwnerReferences = []metav1.OwnerReference{{Name: controller, Kind: controllerType, Controller: proto.Bool(true)}}
	pod.Spec.TerminationGracePeriodSeconds = proto.Int64(2 * int64(terminationTime.Seconds()))
	pod.Labels = labels
	return pod
}

func (c *Cluster) updateReplicasInController(controllerName, controllerNamespace, controllerType string) {
	switch controllerType {
	case "StatefulSet":
		s, err := c.kubeClient.listers.statefulSetLister.StatefulSets(controllerNamespace).Get(controllerName)
		if err != nil || s == nil {
			panic("Could not fetch stateful set")
		}
		replicas := *s.Spec.Replicas + 1
		s.Spec.Replicas = &replicas
		c.kubeClient.listers.statefulSetLister.Add(s)
	case "ReplicaSet":
		rs, err := c.kubeClient.listers.replicaSetLister.ReplicaSets(controllerNamespace).Get(controllerName)
		if err != nil || rs == nil {
			panic("Could not fetch replica set")
		}
		replicas := *rs.Spec.Replicas + 1
		rs.Spec.Replicas = &replicas
		c.kubeClient.listers.replicaSetLister.Add(rs)
	}
}

// SetUpController creates a new controller of controllerType with provided baseName and namespace.
func (c *Cluster) SetUpController(baseName, namespace, controllerType string) string {
	switch controllerType {
	case "StatefulSet":
		statefulSetName := fmt.Sprintf("%s-statefulset", baseName)
		replicas := int32(0)
		statefulSet := appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      statefulSetName,
				Namespace: namespace,
			},
			Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
		}
		c.kubeClient.listers.statefulSetLister.Add(&statefulSet)
		return statefulSetName
	// TODO: implement the rest of controllers
	case "ReplicaSet":
		rsName := fmt.Sprintf("%s-replicaset", baseName)
		replicas := int32(0)
		replicaSet := appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rsName,
				Namespace: namespace,
			},
			Spec: appsv1.ReplicaSetSpec{Replicas: &replicas},
		}
		c.kubeClient.listers.replicaSetLister.Add(&replicaSet)
		return rsName
	default:
		return ""
	}
}

// FillUpNodesCompletely creates pods that occupy all of free cpu and memory on nodes
func (c *Cluster) FillUpNodesCompletely(nodes []*apiv1.Node) {
	for _, node := range nodes {
		podName := fmt.Sprintf("filler-%s", node.Name)
		cpuResource := node.Status.Allocatable[apiv1.ResourceCPU]
		memoryResource := node.Status.Allocatable[apiv1.ResourceMemory]
		pod := testutils.BuildTestPod(podName, cpuResource.MilliValue(), memoryResource.Value())
		pod.Spec.NodeName = node.Name
		c.AddPods(pod)
	}
}

// FillUpNodesBasedOnRequest fills nodes with big pods being multiplicity of spec
func (c *Cluster) FillUpNodesBasedOnRequest(nodes []*apiv1.Node, cpuMilli int64, memMB int64) {
	if cpuMilli <= 0 && memMB <= 0 {
		panic("cpuMilli and memMB lower or equal zero")
	}
	// Mimic expander logic setting some small requests if missing
	if cpuMilli <= 0 {
		cpuMilli = 5
	}
	if memMB <= 0 {
		memMB = 5
	}
	memory := memMB * units.MiB
	for _, node := range nodes {
		cpuResource := node.Status.Allocatable[apiv1.ResourceCPU]
		memoryResource := node.Status.Allocatable[apiv1.ResourceMemory]
		cpuMultiplier := int64(cpuResource.MilliValue() / cpuMilli)
		memMultiplier := int64(memoryResource.Value() / memory)
		multiplier := cpuMultiplier
		if memMultiplier < multiplier {
			multiplier = memMultiplier
		}
		podName := fmt.Sprintf("filler-%s", node.Name)
		pod := testutils.BuildTestPod(podName, cpuMilli*multiplier, memory*multiplier)
		pod.Spec.NodeName = node.Name
		c.AddPods(pod)
	}
}

// FillUpNodesWithPods fills nodes with pods matching spec
func (c *Cluster) FillUpNodesWithPods(nodes []*apiv1.Node, cpuMilli int64, memMB int64) {
	if cpuMilli <= 0 && memMB <= 0 {
		panic("cpuMilli and memMB lower or equal zero")
	}
	for _, node := range nodes {
		cpuResource := node.Status.Allocatable[apiv1.ResourceCPU]
		memoryResource := node.Status.Allocatable[apiv1.ResourceMemory]
		cpuFree := cpuResource.MilliValue()
		memoryFree := memoryResource.Value() / units.MiB
		for i := 0; cpuFree >= cpuMilli && memoryFree >= memMB; i++ {
			podName := fmt.Sprintf("filler-%s-%v", node.Name, i)
			pod := testutils.BuildTestPod(podName, cpuMilli, memMB*units.MiB)
			pod.Spec.NodeName = node.Name
			c.AddPods(pod)
			cpuFree -= cpuMilli
			memoryFree -= memMB
		}
	}
}

// EnableExpanderEphemeralStorageSupport enable expander ephemeal storage support in cluster.
func (c *Cluster) EnableExpanderEphemeralStorageSupport() {
	c.provider.SetPricingModel(gceprovider.NewGcePriceModel(gke.NewGkePriceInfo(machinetypes.NewMachineConfigProvider(nil), false), localssdsize.NewSimpleLocalSSDProvider()))
}

// AddAndScaleUpNodeGroupWithEphemeralStorage adds 1-node node group with specific attached bootDisk.
func (c *Cluster) AddAndScaleUpNodeGroupWithEphemeralStorage(machineType, bootDiskType string, bootDiskSize, localSsdCount int64, ephemeralStorageLocalSsd bool) []*apiv1.Node {
	// From ephemeral storage perspective we don't care about local SSD if ephemeral storage isn't backed up with local SSD.
	if !ephemeralStorageLocalSsd {
		localSsdCount = 0
	}
	name := ConstructNodeGroupNameEphemeralStorageSupport(machineType, bootDiskType, bootDiskSize, localSsdCount)
	existing := c.provider.GetNodeGroup(name)
	if existing != nil {
		panic(name)
	}
	group := c.buildNodeGroup(name, machineType, 0, true, "", nil)

	annotations := make(map[string]string)
	if bootDiskType != "" {
		annotations[gceprovider.BootDiskTypeAnnotation] = bootDiskType
		annotations[gceprovider.BootDiskSizeAnnotation] = strconv.FormatInt(bootDiskSize, 10)
	}
	if ephemeralStorageLocalSsd {
		annotations[gceprovider.EphemeralStorageLocalSsdAnnotation] = "true"
		annotations[gceprovider.LocalSsdCountAnnotation] = strconv.FormatInt(localSsdCount, 10)
	}
	group.SetAnnotations(annotations)
	c.provider.InsertNodeGroup(group)
	return c.ScaleUpNodeGroup(name, 1)
}

// ConstructNodeGroupNameEphemeralStorageSupport construct name using boot disk type, size and local SSD count.
func ConstructNodeGroupNameEphemeralStorageSupport(machineType, bootDiskType string, bootDiskSize, localSsdCount int64) string {
	return strings.Join([]string{machineType, bootDiskType, strconv.FormatInt(bootDiskSize, 10), "localSsdCount", strconv.FormatInt(localSsdCount, 10)}, "_")
}
