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
	"sort"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
)

// NewSortedNodeGroupListProcessor builds new sortedNodeGroupListProcessor.
func NewSortedNodeGroupListProcessor(listProcessor nodegroups.NodeGroupListProcessor) nodegroups.NodeGroupListProcessor {
	return &sortedNodeGroupListProcessor{listProcessor: listProcessor}
}

// sortedNodeGroupListProcessor is a wrapper processor making NodeGroupListProcessor return nodes sorted descending by allocatable CPU.
type sortedNodeGroupListProcessor struct {
	listProcessor nodegroups.NodeGroupListProcessor
}

// Process returns a list of sorted node groups along with node infos.
func (p *sortedNodeGroupListProcessor) Process(context *context.AutoscalingContext,
	nodeGroups []cloudprovider.NodeGroup,
	nodeInfos map[string]*framework.NodeInfo,
	unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	nodeGroups, infos, err := p.listProcessor.Process(context, nodeGroups, nodeInfos, unschedulablePods)
	if err != nil {
		return nil, nil, err
	}
	sortedNodeGroups := sortNodeGroupsByAllocCPU(nodeGroups)
	return sortedNodeGroups, infos, nil
}

// CleanUp cleans up wrapped list processor.
func (p *sortedNodeGroupListProcessor) CleanUp() {
	p.listProcessor.CleanUp()
}

func sortNodeGroupsByAllocCPU(ngs []cloudprovider.NodeGroup) []cloudprovider.NodeGroup {
	// A cache to call TemplateNodeInfo only once per node group
	nodeGroupIdToAllocCPUMap := map[string]int64{}
	for _, ng := range ngs {
		ngAllocCPU := int64(0)
		template, err := ng.TemplateNodeInfo()
		if err == nil {
			ngAllocCPU = template.GetAllocatable().GetMilliCPU()
		}
		nodeGroupIdToAllocCPUMap[ng.Id()] = ngAllocCPU
	}

	cpuCounters := make(map[int64]int, 10)
	for _, ng := range ngs {
		cpuCounters[nodeGroupIdToAllocCPUMap[ng.Id()]] = cpuCounters[nodeGroupIdToAllocCPUMap[ng.Id()]] + 1
	}

	cpus := make([]int64, len(cpuCounters))
	cpuIndex := 0
	for cpu := range cpuCounters {
		cpus[cpuIndex] = cpu
		cpuIndex += 1
	}

	if len(cpus) == 0 {
		return ngs
	}
	sort.Slice(cpus, func(i, j int) bool {
		return cpus[i] > cpus[j]
	})

	indexes := make(map[int64]int, len(cpus))
	indexes[cpus[0]] = 0
	for i := range len(cpus) - 1 {
		indexes[cpus[i+1]] = indexes[cpus[i]] + cpuCounters[cpus[i]]
	}

	result := make([]cloudprovider.NodeGroup, len(ngs))
	for _, ng := range ngs {
		ngCPU := nodeGroupIdToAllocCPUMap[ng.Id()]
		result[indexes[ngCPU]] = ng
		indexes[ngCPU] = indexes[ngCPU] + 1
	}
	return result
}
