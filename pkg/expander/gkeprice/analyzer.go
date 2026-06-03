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

package gkeprice

import (
	"fmt"
	"math"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

const (
	// reusability defines how much of unused resources on the new node could be later reclaimed.
	reusability = 0.5
	// Calculations of unused resources that could be reclaimed later are based on
	// predicted ratio of memory to cpu of future pods.
	// Such ratio is computed based on existing pods in the cluster and pending pods in expander option.
	// Pending pods should have bigger impact on such calculation so they requests are multiplied
	// by pendingResourcesRatioWeight constant for the purpose of ratio calculations.
	pendingResourcesRatioWeight = 2
	// Conversion ratio from CPU to memory based on highmem instances
	cpuToMemoryConversionRatio = (13 * units.GiB) / 2000

	daemonSetType = "DaemonSet"
)

// ClusterAnalyzer creates ClusterAnalysis object based on snapshot of the cluster state
type ClusterAnalyzer interface {
	Analyze(map[string]*framework.NodeInfo) (ClusterAnalysis, error)
	AnalyzeUserWorkloadUse() (UserWorkloadClusterAnalysis, error)
}

// ClusterAnalysis provides preferred node and ratio of reusable resources based on the cluster state
type ClusterAnalysis interface {
	GetPreferredCpuCount(expander.Option, *framework.NodeInfo) (int64, error)
	GetReusableResources(expander.Option, *framework.NodeInfo) (*apiv1.Pod, error)
}

// UserWorkloadClusterAnalysis provides information about approximate resource
// requests of pods based on a cluster state. The approximation is calculated
// as the weighted average of resource requests of pods already running (weight 1.0) on similar nodes
// and resource requests of pods present on provided node infos (weight 2.0).
// More details in go/dynamic-max-pods-per-node-design#bookmark=id.eqsww254o3qc.
type UserWorkloadClusterAnalysis interface {
	GetPodResourceRequestApproximation(nodeInfo []framework.NodeInfo) (Resource, error)
}

type groupingClusterAnalyzer struct {
	cloudProvider        cloudprovider.CloudProvider
	nodeLister           kube_util.NodeLister
	podLister            kube_util.PodLister
	systemPodsClassifier systempods.Classifier
}

// TODO(b/235788503): Extend resourceRatio struct with memToEph value.
type resourceRatio struct {
	memToCpu float64
	ephToCpu float64
}

// NewGroupingClusterAnalyzer returns groupingClusterAnalyzer handling dedicated workloads
func NewGroupingClusterAnalyzer(cloudProvider cloudprovider.CloudProvider, nodeLister kube_util.NodeLister, podLister kube_util.PodLister, classifier systempods.Classifier) ClusterAnalyzer {
	return &groupingClusterAnalyzer{
		cloudProvider:        cloudProvider,
		nodeLister:           nodeLister,
		podLister:            podLister,
		systemPodsClassifier: classifier,
	}
}

// Analyze creates ClusterAnalysis object based on snapshot of the cluster state
func (ca *groupingClusterAnalyzer) Analyze(nodeInfos map[string]*framework.NodeInfo) (ClusterAnalysis, error) {
	workloadCapacity := make(map[string]Resource)
	for _, nodeGroup := range ca.cloudProvider.NodeGroups() {
		targetSize, err := nodeGroup.TargetSize()
		if err != nil {
			klog.Errorf("Failed to get target size for %v: %v", nodeGroup.Id(), err)
			continue
		}
		nodeInfo, found := nodeInfos[nodeGroup.Id()]
		if !found {
			klog.Errorf("Failed to get template node info for %v", nodeGroup.Id())
			continue
		}
		capacity := Resource{}
		capacity.AddResourceList(nodeInfo.Node().Status.Capacity)
		groupId := podrequirements.ExtractWorkloadID(nodeInfo.Node())
		totalCapacity := workloadCapacity[groupId]
		totalCapacity.MilliCPU += capacity.MilliCPU * int64(targetSize)
		totalCapacity.Memory += capacity.Memory * int64(targetSize)
		workloadCapacity[groupId] = totalCapacity
	}

	allPods, err := ca.podLister.List()
	if err != nil {
		klog.Errorf("Failed to list pods: %v", err)
		return nil, err
	}
	pods := kube_util.ScheduledPods(allPods)
	nodes, err := ca.nodeLister.List()
	if err != nil {
		klog.Errorf("Failed to list nodes: %v", err)
		return nil, err
	}
	nodeNameToWorkloadIdMap := make(map[string]string)
	for _, node := range nodes {
		workloadId := podrequirements.ExtractWorkloadID(node)
		nodeNameToWorkloadIdMap[node.Name] = workloadId
	}

	workloadUse := make(map[string]Resource)
	for _, pod := range pods {
		workloadId, found := nodeNameToWorkloadIdMap[pod.Spec.NodeName]
		if !found {
			klog.Warningf("Node \"%v\" not found for pod \"%v\"", pod.Spec.NodeName, pod.Name)
			continue
		}
		used := workloadUse[workloadId]
		used.AddPodsRequests(pod)
		// TODO(b/517097612): filter out daemon sets
		workloadUse[workloadId] = used
	}

	return &groupingClusterAnalysis{
		workloadCapacity: workloadCapacity,
		workloadUse:      workloadUse,
	}, nil
}

func (ca *groupingClusterAnalyzer) AnalyzeUserWorkloadUse() (UserWorkloadClusterAnalysis, error) {
	allPods, err := ca.podLister.List()
	if err != nil {
		klog.Errorf("Failed to list pods: %v", err)
		return nil, err
	}
	pods := kube_util.ScheduledPods(allPods)
	nodes, err := ca.nodeLister.List()
	if err != nil {
		klog.Errorf("Failed to list nodes: %v", err)
		return nil, err
	}
	nodeNameToWorkloadIdMap := make(map[string]string)
	for _, node := range nodes {
		workloadId := podrequirements.ExtractWorkloadID(node)
		nodeNameToWorkloadIdMap[node.Name] = workloadId
	}
	userWorkloadUse := make(map[string]Resource)
	userWorkloadCount := make(map[string]int)
	for _, pod := range pods {
		workloadId, found := nodeNameToWorkloadIdMap[pod.Spec.NodeName]
		if !found {
			klog.Warningf("Node \"%v\" not found for pod \"%v\"", pod.Spec.NodeName, pod.Name)
			continue
		}
		if isNonIgnoredNonDSPod(pod, ca.systemPodsClassifier) {
			userUsed := userWorkloadUse[workloadId]
			userUsed.AddPodsRequests(pod)
			userWorkloadUse[workloadId] = userUsed
			userWorkloadCount[workloadId]++
		}
	}

	return &groupingClusterAnalysis{
		userWorkloadCount:    userWorkloadCount,
		userWorkloadUse:      userWorkloadUse,
		systemPodsClassifier: ca.systemPodsClassifier,
	}, nil
}

type groupingClusterAnalysis struct {
	workloadCapacity     map[string]Resource
	workloadUse          map[string]Resource
	userWorkloadUse      map[string]Resource
	userWorkloadCount    map[string]int
	systemPodsClassifier systempods.Classifier
}

// GetReusableResources returns amount of resources that could be reclaimed in the future based on cluster state
func (cs *groupingClusterAnalysis) GetReusableResources(option expander.Option, nodeInfo *framework.NodeInfo) (*apiv1.Pod, error) {
	id := podrequirements.ExtractWorkloadID(nodeInfo.Node())
	used := cs.workloadUse[id]
	newPodsRequests := cs.sumPodRequests(option)
	resourceRatio := cs.calculateTargetResourceRatio(used, newPodsRequests)
	// Compute free resources on new node
	daemonSetsResources := nodeInfo.GetRequested()
	nodeFreeResources := Resource{
		MilliCPU:         nodeInfo.GetAllocatable().GetMilliCPU() - daemonSetsResources.GetMilliCPU(),
		Memory:           nodeInfo.GetAllocatable().GetMemory() - daemonSetsResources.GetMemory(),
		EphemeralStorage: nodeInfo.GetAllocatable().GetEphemeralStorage() - daemonSetsResources.GetEphemeralStorage(),
	}
	// Compute wasted resources after scale up
	wastedResources := Resource{
		MilliCPU:         int64(option.NodeCount)*nodeFreeResources.MilliCPU - newPodsRequests.MilliCPU,
		Memory:           int64(option.NodeCount)*nodeFreeResources.Memory - newPodsRequests.Memory,
		EphemeralStorage: int64(option.NodeCount)*nodeFreeResources.EphemeralStorage - newPodsRequests.EphemeralStorage,
	}
	reusableResources := cs.calculateReusable(wastedResources, resourceRatio)
	reusableResources = cs.limitReusableResources(option.NodeCount, reusableResources, newPodsRequests, resourceRatio)

	return buildPod("reclaimed", reusableResources.MilliCPU, reusableResources.Memory, reusableResources.EphemeralStorage), nil
}

// GetPodResourceRequestApproximation calculates the approximate resource requests of pods that could schedule on provided nodes.
// The approximation is calculated as the weighted average of resource requests of pods already running (weight 1.0) on similar nodes
// and resource requests of pods present on provided node infos (weight 2.0).
func (cs *groupingClusterAnalysis) GetPodResourceRequestApproximation(nodeInfos []framework.NodeInfo) (Resource, error) {
	if len(nodeInfos) == 0 {
		return Resource{}, nil
	}
	id := podrequirements.ExtractWorkloadID(nodeInfos[0].Node())

	newPodsResource := Resource{}
	newPodsCount := 0
	for _, nodeInfo := range nodeInfos {
		nId := podrequirements.ExtractWorkloadID(nodeInfo.Node())
		if nId != id {
			return Resource{}, fmt.Errorf("got node infos with different workload ids: %s, %s", id, nId)
		}
		for _, podInfo := range nodeInfo.Pods() {
			// TODO(b/517095676): remove check for non empty NodeName of pod. This is used
			// to not take into account resource requests of "future daemon set pods".
			// Current logic injecting those doesn't set the owner - we should change that.
			// However, this pods can be distinguished from others by having non empty node name.
			if !isNonIgnoredNonDSPod(podInfo.Pod, cs.systemPodsClassifier) || podInfo.Pod.Spec.NodeName != "" {
				continue
			}
			newPodsResource.AddPodsRequests(podInfo.Pod)
			newPodsCount++
		}
	}
	existingResource := cs.userWorkloadUse[id]
	existingWorkloadCount := cs.userWorkloadCount[id]
	if existingWorkloadCount+newPodsCount == 0 {
		return Resource{}, nil
	}

	return Resource{
		MilliCPU: (existingResource.MilliCPU + pendingResourcesRatioWeight*newPodsResource.MilliCPU) /
			int64(existingWorkloadCount+pendingResourcesRatioWeight*newPodsCount),
		Memory: (existingResource.Memory + pendingResourcesRatioWeight*newPodsResource.Memory) /
			int64(existingWorkloadCount+pendingResourcesRatioWeight*newPodsCount),
		EphemeralStorage: (existingResource.EphemeralStorage + pendingResourcesRatioWeight*newPodsResource.EphemeralStorage) /
			int64(existingWorkloadCount+pendingResourcesRatioWeight*newPodsCount),
	}, nil
}

func isNonIgnoredNonDSPod(pod *apiv1.Pod, classifier systempods.Classifier) bool {
	if classifier.IsSystemPod(pod) {
		return false
	}
	return !isPodOwnerDaemonSet(pod)
}

func isPodOwnerDaemonSet(pod *apiv1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == daemonSetType {
			return true
		}
	}
	return false
}

// sumPodRequests sums pending pods containers requests
func (cs *groupingClusterAnalysis) sumPodRequests(option expander.Option) Resource {
	podRequests := Resource{}
	podRequests.AddPodsRequests(option.Pods...)
	return podRequests
}

// calculateTargetResourceRatio predicts future pod requests memory/cpu, ephemeralStorage/cpu ratio.
// Such ratio is computed based on existing pods in the cluster and pending pods in expander option.
// Pending pods should have bigger impact on such calculation so they requests are multiplied
// by pendingResourcesRatioWeight constant for the purpose of ratio calculations.
func (cs *groupingClusterAnalysis) calculateTargetResourceRatio(runningPodRequests, newPodsRequests Resource) resourceRatio {
	cpuMilliTotal := runningPodRequests.MilliCPU + pendingResourcesRatioWeight*newPodsRequests.MilliCPU
	memTotal := runningPodRequests.Memory + pendingResourcesRatioWeight*newPodsRequests.Memory
	memToCpu := float64(memTotal) / float64(cpuMilliTotal+1)
	ephTotal := runningPodRequests.EphemeralStorage + pendingResourcesRatioWeight*newPodsRequests.EphemeralStorage
	ephToCpu := float64(ephTotal) / float64(cpuMilliTotal+1)

	return resourceRatio{
		memToCpu: memToCpu,
		ephToCpu: ephToCpu,
	}
}

// calculateReusable is applying certain ratio to the free resources from the node to calculate how many of available resources
// may be reusable. The ratio is applying to each pair of resources: (memory, cpu), (ephemeralStorage, cpu), (memory, ephemeralStorage).
func (cs *groupingClusterAnalysis) calculateReusable(wastedResources Resource, ratio resourceRatio) Resource {
	// Compute resources that could be reclaimed given predicted targetRatio
	var reusableMilliCpu, reusableMilliCpu2 int64
	var reusableMemory int64
	var reusableEphemeralStorage int64
	reusableMemory, reusableMilliCpu = cropToRatio(ratio.memToCpu, wastedResources.Memory, wastedResources.MilliCPU+1)
	reusableEphemeralStorage, reusableMilliCpu2 = cropToRatio(ratio.ephToCpu, wastedResources.EphemeralStorage, wastedResources.MilliCPU+1)
	reusableMilliCpu = min(reusableMilliCpu2, reusableMilliCpu)
	if ratio.ephToCpu > 0 {
		memToEph := ratio.memToCpu / ratio.ephToCpu
		reusableMemory, reusableEphemeralStorage = cropToRatio(memToEph, reusableMemory, reusableEphemeralStorage)
	}
	// Discount computed resources as reclaiming is uncertain
	return Resource{
		MilliCPU:         int64(reusability * float64(reusableMilliCpu)),
		Memory:           int64(reusability * float64(reusableMemory)),
		EphemeralStorage: int64(reusability * float64(reusableEphemeralStorage)),
	}
}

// cropToRatio returns updated values that have specific ratio. One of the value remains the same,
// when the second value is equal or smaller than initial value.
func cropToRatio(targetRatio float64, resource1, resource2 int64) (int64, int64) {
	oldRatio := float64(resource1) / float64(resource2)
	if targetRatio == 0 {
		return 0, resource2
	}
	if oldRatio > targetRatio {
		resource1 = int64(float64(resource1) * targetRatio / oldRatio)
	} else {
		resource2 = int64(float64(resource2) * oldRatio / targetRatio)
	}
	return resource1, resource2
}

// limitReusableResources crops reusability value when considering scale up with many nodes that are underutilized.
// It should prefer smaller nodes when anti-affinity or other factor limits utilization.
// For small scale ups (i.e. 1-3 nodes), it should not limit reusability calculated in previous steps.
// For large scale ups, it will limit reusability to some multiplier of pending pod requests
// which will penalize large but mostly empty nodes.
// The reason behind limiting reusability with pending pods is following: the binpacking algorithm is implemented in a way
// that  N - 1 nodes are packed, so we assume that reusable resources located mostly in the last node.
func (cs *groupingClusterAnalysis) limitReusableResources(nodeCount int, reusableResources, newPodsRequests Resource, ratio resourceRatio) Resource {
	if nodeCount <= 1 {
		return reusableResources
	}
	limit := Resource{}

	// calculate limit for memory and cpu
	limit.Memory, limit.MilliCPU = applyLimit(newPodsRequests.Memory, newPodsRequests.MilliCPU+1, int64(nodeCount), ratio.memToCpu)

	if ratio.ephToCpu > 0 {
		// update ephemeral storage and cpu
		var cpu2 int64
		limit.EphemeralStorage, cpu2 = applyLimit(newPodsRequests.EphemeralStorage, newPodsRequests.MilliCPU+1, int64(nodeCount), ratio.ephToCpu)
		limit.MilliCPU = max(limit.MilliCPU, cpu2)

		// update memory and ephemeral storage
		memToEph := ratio.memToCpu / ratio.ephToCpu
		limit.Memory, limit.EphemeralStorage = applyLimit(newPodsRequests.Memory, newPodsRequests.EphemeralStorage, int64(nodeCount), memToEph)
	}

	return Resource{
		MilliCPU:         min(reusableResources.MilliCPU, limit.MilliCPU),
		Memory:           min(reusableResources.Memory, limit.Memory),
		EphemeralStorage: min(reusableResources.EphemeralStorage, limit.EphemeralStorage),
	}
}

// applyLimit limit resources by multiplicator that depends on nodeCount and keep the target ratio between resources.
func applyLimit(resource1, resource2 int64, nodeCount int64, targetRatio float64) (int64, int64) {
	ratio := float64(resource1) / float64(resource2)
	if ratio > targetRatio {
		resource1 = int64((float64(resource1*nodeCount) / float64(nodeCount-1)) * 0.5)
		resource2 = int64(float64(resource1) / targetRatio)
	} else {
		resource2 = int64((float64(resource2*nodeCount) / float64(nodeCount-1)) * 0.5)
		resource1 = int64(float64(resource2) * targetRatio)
	}
	return resource1, resource2
}

// GetPreferredCpuCount returns preferred node based on the cluster size
func (cs *groupingClusterAnalysis) GetPreferredCpuCount(option expander.Option, nodeInfo *framework.NodeInfo) (int64, error) {
	podsRequests := Resource{}
	podsRequests.AddPodsRequests(option.Pods...)
	cpuMilliTotal := podsRequests.MilliCPU
	cpuMilliEquivalent := calculateCpuMilliEquivalent(podsRequests.Memory)
	if cpuMilliEquivalent > cpuMilliTotal {
		cpuMilliTotal = cpuMilliEquivalent
	}

	workloadId := podrequirements.ExtractWorkloadID(nodeInfo.Node())
	capacity := cs.workloadCapacity[workloadId]
	cpuMilliTotal += capacity.MilliCPU

	cpuTotal := int64(math.Ceil(float64(cpuMilliTotal) / 1000))
	return preferredCpuCount(cpuTotal), nil
}

func calculateCpuMilliEquivalent(memory int64) int64 {
	return int64(math.Ceil(float64(memory) / cpuToMemoryConversionRatio))
}

// preferredCpuCount returns preferred CPU count of new node based on total CPU
func preferredCpuCount(cpuTotal int64) int64 {
	// Double node size with every time the cluster node count increases 3x.
	switch {
	case cpuTotal <= 3: // legacy <= 2 existing nodes
		return 1
	case cpuTotal <= 12: // 6 * 2, legacy <= 6 existing nodes
		return 2
	case cpuTotal <= 60: // 12 * 4 + 12, legacy <= 20 existing nodes
		return 4
	case cpuTotal <= 380: // 40 * 8 + 60, legacy <= 60 existing nodes
		return 8
	case cpuTotal <= 2300: // 120 * 16 + 380, legacy <= 200 existing nodes
		return 16
	case cpuTotal <= 18300: // 500 * 32 + 2300, probable legacy would be 200 * (≈3) ≈ 700
		return 32
	case cpuTotal <= 107900: // 1400 * 64 + 18300, probable legacy would be 700 * (≈3) ≈ 2100
		return 64
	}
	return 96
}
