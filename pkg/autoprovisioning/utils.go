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

package autoprovisioning

import (
	"context"
	"fmt"
	"strconv"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/tpu"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"

	gke_backoff "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/backoff"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/rules"
	podutils "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/pod"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

func getResourceBasedBackoff(compositeBackoff gke_backoff.CompositeBackoff) *gke_backoff.ResourceBackoff {
	for _, backoff := range compositeBackoff.GetBackoffs() {
		if resourceBasedBackoff, ok := backoff.(*gke_backoff.ResourceBackoff); ok {
			return resourceBasedBackoff
		}
	}
	return nil
}

func autoprovisionedNodeGroupsCount(nodeGroups []cloudprovider.NodeGroup) int {
	result := 0
	for _, group := range nodeGroups {
		if group.Autoprovisioned(context.TODO()) {
			result++
		}
	}
	return result
}

func virtualNodeInfos(nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo) map[string]*framework.NodeInfo {
	var virtualNodeInfos = make(map[string]*framework.NodeInfo)
	for _, nodeG := range nodeGroups {
		if !nodeG.Exist(context.TODO()) {
			nodeI := nodeInfos[nodeG.Id()]
			virtualNodeInfos[nodeG.Id()] = nodeI
		}
	}
	return virtualNodeInfos
}

// configuredMaxPodsPerNodeFromLabels returns the configured MaxPodsPerNode.
// If it's configured it returns MaxPodsPerNode as an int, otherwise returns 0.
func configuredMaxPodsPerNodeFromLabels(systemLabels map[string]string) (int, error) {
	if systemLabels == nil {
		return 0, nil
	}
	if strMPPN, exists := systemLabels[labels.MaxPodsPerNodeLabel]; exists {
		mppn, err := strconv.Atoi(strMPPN)
		if err != nil {
			return 0, err
		}
		if mppn < 0 {
			return 0, fmt.Errorf("Invalid MaxPodsPerNode found, expected an int >= 0, instead found: %v", mppn)
		}
		return mppn, nil
	}
	return 0, nil
}

// getEstimatedNumberOfPods returns an estimated number of pods that could fit given machineType based on node group
// requirements. It's meant to be a very simple approximation, not bullet proof mechanism.
func getEstimatedNumberOfPods(estimatedNumberOfPods int, requirements nodeGroupRequirements, machineTypeInfo machinetypes.MachineType) int {
	if len(requirements.pods) == 0 {
		return 0
	}

	cpuSum := resource.Quantity{}
	memorySum := resource.Quantity{}
	gpuSum := resource.Quantity{}
	tpuSum := resource.Quantity{}
	for _, pod := range requirements.pods {
		podRequests := podutils.PodRequests(pod)
		cpuSum.Add(podRequests[apiv1.ResourceCPU])
		memorySum.Add(podRequests[apiv1.ResourceMemory])
		gpuSum.Add(podRequests[gpu.ResourceNvidiaGPU])
		tpuSum.Add(podRequests[tpu.ResourceGoogleTPU])
	}

	averageCpuMilli := cpuSum.MilliValue() / int64(len(requirements.pods))
	averageMemory := memorySum.Value() / int64(len(requirements.pods))
	averageGpuSum := gpuSum.Value() / int64(len(requirements.pods))
	averageTpuSum := tpuSum.Value() / int64(len(requirements.pods))
	if averageCpuMilli != 0 && int((machineTypeInfo.CPU*1000)/averageCpuMilli) < estimatedNumberOfPods {
		estimatedNumberOfPods = int((machineTypeInfo.CPU * 1000) / averageCpuMilli)
	}
	if averageMemory != 0 && int(machineTypeInfo.Memory/averageMemory) < estimatedNumberOfPods {
		estimatedNumberOfPods = int(machineTypeInfo.Memory / averageMemory)
	}
	if averageGpuSum != 0 && requirements.gpuRequest.Count != 0 && int(int64(requirements.gpuRequest.Count)/averageGpuSum) < estimatedNumberOfPods {
		estimatedNumberOfPods = int(int64(requirements.gpuRequest.Count) / averageGpuSum)
	}
	if averageTpuSum != 0 && int(requirements.tpuRequest.ChipsPerNode/averageTpuSum) < estimatedNumberOfPods {
		estimatedNumberOfPods = int(requirements.tpuRequest.ChipsPerNode / averageTpuSum)
	}

	return estimatedNumberOfPods
}

func capacityCheckWaitTimeSecondsSignature(computeClassRule rules.Rule) string {
	ccwt := "nil"
	if computeClassRule != nil && computeClassRule.SelfServiceMetadata() != nil {
		ccwt = computeClassRule.SelfServiceMetadata()[labels.CapacityCheckWaitTimeSecondsLabel]
	}
	return ccwt
}

func placementGroupSpec(ngReq *nodeGroupRequirements, labelReq podrequirements.LabelRequirements) placement.Spec {
	pgSpec := placement.FromRequirements(labelReq)
	// if there is a placement policy inferred from compute class.
	if ngReq.computeClassRule != nil && ngReq.computeClassRule.PlacementPolicy() != "" && pgSpec.Policy == "" {
		pgSpec.Policy = ngReq.computeClassRule.PlacementPolicy()
	}
	return pgSpec
}

// defaultEphemeralStorageLSSDCount returns the default number of local SSDs
// for ephemeral storage, after subtracting swap dedicated local SSDs, data
// cache, and NVME block local SSDs from the available number of local SSDs.
func defaultEphemeralStorageLSSDCount(ngReq nodeGroupRequirements, total int) int {
	defaultCount := total
	if ngReq.linuxNodeConfig != nil && ngReq.linuxNodeConfig.SwapConfig != nil {
		swapConfig := ngReq.linuxNodeConfig.SwapConfig
		if swapConfig.Enabled && swapConfig.DedicatedLocalSsdProfile != nil {
			defaultCount -= int(swapConfig.DedicatedLocalSsdProfile.DiskCount)
		}
	}
	// TODO(go/ccc-static-local-ssd): Add data cache and NVME block count.
	return defaultCount
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

func derefInt32(i *int32) int32 {
	if i == nil {
		return 0
	}
	return *i
}

func derefBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}
