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

package estimator

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/lookaheadbuffer"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/extendeddurationpods"
	schedulerframework "k8s.io/kube-scheduler/framework"
)

const maxSupportedPodsPerNode int = 256

var (
	// defaultResource defines default resource requests of a Pod in an Autopilot cluster.
	defaultResource = gkeprice.Resource{
		MilliCPU:         500,
		Memory:           2 * units.GiB,
		EphemeralStorage: 1 * units.GiB,
	}
)

// we don't want to calculate future pods for certain classes of nodes, as we
// want them to have 1 user pod per node. This so far includes nodes with GPU and TPU.
func shouldFuturePodsBeCalculated(info *framework.NodeInfo) bool {
	return !isOnePodPerNode(info.Node())
}

// TODO: move to global, autopilot specific util
// context: http://gkecl/905326/comment/8343ec4c_22146288/
func isOnePodPerNode(node *apiv1.Node) bool {
	// Performance ComputeClass uses slice of hardware model, which as of today is limited to ~one pod per node
	computeClass, found := node.Labels[labels.ComputeClassLabel]
	if found && computeClass == "Performance" {
		return true
	}

	return tpu.NodeHasTpu(node) || extendeddurationpods.EdpOnePodPerNode(node)
}

// getFuturePods calculated how many more pods could fit based on the approximate resources.
// we get this information by dividing available space by previously calculated approximate resources.
// In case passed approximate resource requests are "0" we will use the autopilot default resource requests.
func getFuturePodsWithDefaultResources(nodeInfo framework.NodeInfo, approximateResource gkeprice.Resource) int {
	// it is possible that all currently existing user workloads are "workload separated",
	// and we have scale up for system pods only, which can result in "0" resource
	// approximation. In such case we can use Autopilot default resource requests here.
	if isResourceApproximateZero(approximateResource) {
		approximateResource = defaultResource
	}
	return getFuturePods(nodeInfo, approximateResource)
}

// getFuturePods calculated how many more pods could fit based on the approximate resources.
// we get this information by dividing available space by previously calculated approximate resources.
func getFuturePods(nodeInfo framework.NodeInfo, approximateResource gkeprice.Resource) int {
	currentPodsCount := len(nodeInfo.Pods())
	if gpu.NodeHasGpu(labels.GPULabel, nodeInfo.Node()) {
		return getFutureGPUPods(currentPodsCount, nodeInfo.GetAllocatable(), nodeInfo.GetRequested())
	}
	cpuAvailablePods := maxSupportedPodsPerNode

	laPodsRequests := lookaheadbuffer.AllLookaheadPodsRequests(&nodeInfo)
	requestedMilliCPUWithouthLAPods := nodeInfo.GetRequested().GetMilliCPU() - laPodsRequests.Cpu().MilliValue()
	requestedMemoryWithoutLAPods := nodeInfo.GetRequested().GetMemory() - laPodsRequests.Memory().Value()
	if approximateResource.MilliCPU != 0 {
		cpuAvailablePods = int((nodeInfo.GetAllocatable().GetMilliCPU() - requestedMilliCPUWithouthLAPods) / approximateResource.MilliCPU)
	}
	futurePods := cpuAvailablePods
	if approximateResource.Memory != 0 {
		memAvailablePods := int((nodeInfo.GetAllocatable().GetMemory() - requestedMemoryWithoutLAPods) / approximateResource.Memory)

		if futurePods > memAvailablePods {
			futurePods = memAvailablePods
		}
	}

	if approximateResource.EphemeralStorage != 0 {
		ephStorageAvailablePods := int((nodeInfo.GetAllocatable().GetEphemeralStorage() - nodeInfo.GetRequested().GetEphemeralStorage()) / approximateResource.EphemeralStorage)
		if futurePods > ephStorageAvailablePods {
			futurePods = ephStorageAvailablePods
		}
	}

	if currentPodsCount+futurePods > maxSupportedPodsPerNode {
		futurePods = max(0, maxSupportedPodsPerNode-currentPodsCount)
	}
	// TODO(b/377688902): this shouldn't be necessary, however due to b/377688902
	// we can have requested >> allocatable.
	futurePods = max(0, futurePods)
	return futurePods
}

// getFutureGPUPods calculates how many more GPU pods could fit on the given GPU node.
// As GPU is an expensive resource, we assume that each remaining unit of GPU
// can be consumed by one pod, to not limit their consumption in any way.
func getFutureGPUPods(currentPodsCount int, allocatable, requests schedulerframework.Resource) int {
	remainingGPUs := int(allocatable.GetScalarResources()[gpu.ResourceNvidiaGPU] - requests.GetScalarResources()[gpu.ResourceNvidiaGPU])
	if remainingGPUs+currentPodsCount > maxSupportedPodsPerNode {
		return max(0, maxSupportedPodsPerNode-remainingGPUs)
	}
	return remainingGPUs
}

func isResourceApproximateZero(approximateResource gkeprice.Resource) bool {
	return approximateResource.MilliCPU == 0 && approximateResource.Memory == 0 && approximateResource.EphemeralStorage == 0
}
