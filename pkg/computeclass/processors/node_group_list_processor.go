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

package processors

import (
	"math/rand"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/klog/v2"
)

type nodeGroupListMetrics interface {
	ObserveInvalidNpcScaleUpOrder()
}

type processorCloudProvider interface {
	IsResizableVmEnabledInAutopilot(machineFamily string) bool
	IsResizableVmWithinPodFamilyEnabled(machineFamily string) bool
	IsExtendedFallbacksEnabled() bool
	IsAutopilotEnabled() bool
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// nodeGroupListProcessor implements NodeGroupListProcessor and BinpackingLimiter.
type nodeGroupListProcessor struct {
	nodeGroupListProcessor       nodegroups.NodeGroupListProcessor
	organiser                    computeclass.Organizer
	lister                       lister.Lister
	metrics                      nodeGroupListMetrics
	nodegroupBucket              map[string]int
	nodegroupsCountPerBucket     map[int]int
	nodegroupsProcessedPerBucket map[int]int
	lastProcessedNodegroup       string
}

// NewNodeGroupListProcessor creates an instance of nodeGroupListProcessor.
func NewNodeGroupListProcessor(lister lister.Lister, ngListProcessor nodegroups.NodeGroupListProcessor, metrics nodeGroupListMetrics, provider processorCloudProvider) *nodeGroupListProcessor {
	organiser := computeclass.NewOrganizer(lister, provider)
	return &nodeGroupListProcessor{
		nodeGroupListProcessor:       ngListProcessor,
		organiser:                    organiser,
		lister:                       lister,
		metrics:                      metrics,
		nodegroupBucket:              make(map[string]int),
		nodegroupsCountPerBucket:     make(map[int]int),
		nodegroupsProcessedPerBucket: make(map[int]int),
	}
}

// Process processes the nodegroups and order them on the basis of crd priorities.
func (p *nodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {

	// Reset to avoid accumulating data over multiple runs
	p.nodegroupBucket = make(map[string]int)
	p.nodegroupsCountPerBucket = make(map[int]int)

	nodeGroups, nodeInfos, err := p.nodeGroupListProcessor.Process(ctx, nodeGroups, nodeInfos, unschedulablePods)
	if err != nil {
		klog.Errorf("Cannot process nodegroups from NAP, error: %v", err)
		return nodeGroups, nodeInfos, err
	}

	crds, err := p.lister.ListCrds()
	if err != nil {
		klog.Errorf("Cannot list npc crds while processing nodegroups, error: %v", err)
		return nodeGroups, nodeInfos, err
	}

	nodeGroupBucketsByCrd := p.organiser.OrganizeByCrds(nodeGroups, crds)

	// Randomize the order of bucket groups so that no crd starves.
	rand.Shuffle(len(nodeGroupBucketsByCrd), func(i, j int) {
		nodeGroupBucketsByCrd[i], nodeGroupBucketsByCrd[j] = nodeGroupBucketsByCrd[j], nodeGroupBucketsByCrd[i]
	})
	var nodeGroupBuckets [][]cloudprovider.NodeGroup
	for _, buckets := range nodeGroupBucketsByCrd {
		nodeGroupBuckets = append(nodeGroupBuckets, buckets...)
	}

	nodeGroups = p.flattenNodeGroupsByBucket(nodeGroupBuckets)
	return nodeGroups, nodeInfos, nil
}

// CleanUp cleans up the processor's internal structures. Just here to satisfy the NodeGroupListProcessor interface.
func (p *nodeGroupListProcessor) CleanUp() {
	p.nodeGroupListProcessor.CleanUp()
}

// InitBinpacking initialises the BinpackingLimiter.
func (p *nodeGroupListProcessor) InitBinpacking(context *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup) {
	// p.nodegroupsCountPerBucket & p.nodegroupBucket are set in
	// p.Process() method
	p.nodegroupsProcessedPerBucket = make(map[int]int)
}

// StopBinpacking is used to make decisions on the evaluated expansion options.
// Binpacking would be stopped if we have evaluated all nodegroups from Nth Bucket.
func (p *nodeGroupListProcessor) StopBinpacking(context *context.AutoscalingContext, evaluatedOptions []expander.Option) bool {
	bucket := p.nodegroupBucket[p.lastProcessedNodegroup]
	// Check if all the previous buckets are already filled
	for previous := 0; previous < bucket; previous++ {
		if p.nodegroupsProcessedPerBucket[previous] < p.nodegroupsCountPerBucket[previous] {
			klog.Errorf("scale-up option from bucket %d is processed after only %d out of %d options from bucket %d were processed", bucket, p.nodegroupsProcessedPerBucket[previous], p.nodegroupsCountPerBucket[previous], previous)
			p.metrics.ObserveInvalidNpcScaleUpOrder()
			break
		}
	}
	if p.nodegroupsProcessedPerBucket[bucket] == p.nodegroupsCountPerBucket[bucket] {
		klog.V(2).Infof("processed all node groups from bucket %d, got %d scale-up options.", bucket, len(evaluatedOptions))
		return len(evaluatedOptions) > 0
	}
	return false
}

// MarkProcessed marks the nodegroup as processed.
func (p *nodeGroupListProcessor) MarkProcessed(context *context.AutoscalingContext, nodegroupId string) {
	bucket := p.nodegroupBucket[nodegroupId]
	p.nodegroupsProcessedPerBucket[bucket]++
	p.lastProcessedNodegroup = nodegroupId
	klog.V(2).Infof("processed %d out of %d node groups from bucket %d", p.nodegroupsProcessedPerBucket[bucket], p.nodegroupsCountPerBucket[bucket], bucket)
}

// FinalizeBinpacking is only here to satisfy the interface.
func (p *nodeGroupListProcessor) FinalizeBinpacking(context *context.AutoscalingContext, finalOptions []expander.Option) {
}

// flattenNodeGroupsByBucket converts 2-d slice to 1-d and stores information for binpacking limiter.
func (p *nodeGroupListProcessor) flattenNodeGroupsByBucket(pool [][]cloudprovider.NodeGroup) []cloudprovider.NodeGroup {
	var result []cloudprovider.NodeGroup
	for pr, nodegroups := range pool {
		if len(nodegroups) > 0 {
			// store nodegroup bucket.
			for _, ng := range nodegroups {
				p.nodegroupBucket[ng.Id()] = pr
			}
			// store count of nodegroups per bucket.
			p.nodegroupsCountPerBucket[pr] = len(nodegroups)
			result = append(result, nodegroups...)
		}
	}
	klog.V(2).Infof("created %d buckets with a total of %d node groups", len(pool), len(result))
	return result
}
