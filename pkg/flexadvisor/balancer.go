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

package flexadvisor

import (
	"container/heap"
	"errors"
	"fmt"
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	auto_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
)

// ScaleUpBalancer tries to keep the new sizes of similar NodeGroups balanced, when scaling up respecting the guidance from Flex Advisor.
type ScaleUpBalancer struct {
	nodegroupset.NodeGroupSetProcessor
	provider           instanceavailability.Provider
	cccLister          lister.Lister
	ignoreRLA          bool
	experimentsManager experiments.Manager
}

// balancingInfo contains balancing information about planned scale-up of a single NodeGroup.
type balancingInfo struct {
	scaleUpInfo        nodegroupset.ScaleUpInfo
	snapshot           *instanceavailability.Snapshot
	instanceRef        *InstanceReference
	capacityLimit      int // capacityLimit is the max limit satisfying NodeGroup.MaxSize and capacity guidance from Flex Advisor
	gcePreferenceScore float64
}

// String is used for printing ScaleUpInfo for logging, etc
func (b balancingInfo) String() string {
	return fmt.Sprintf("{%v %v->%v (maxSize=%v, capacityLimit=%v)}", b.scaleUpInfo.Group.Id(), b.scaleUpInfo.CurrentSize, b.scaleUpInfo.NewSize, b.scaleUpInfo.MaxSize, b.capacityLimit)
}

// NewScaleUpBalancer returns an instance of Flex Advisor based ScaleUpBalancer.
func NewScaleUpBalancer(nodeGroupSetProcessor nodegroupset.NodeGroupSetProcessor, provider instanceavailability.Provider, cccLister lister.Lister, experimentsManager experiments.Manager, ignoreRLA bool) *ScaleUpBalancer {
	return &ScaleUpBalancer{
		NodeGroupSetProcessor: nodeGroupSetProcessor,
		provider:              provider,
		cccLister:             cccLister,
		experimentsManager:    experimentsManager,
		ignoreRLA:             ignoreRLA,
	}
}

func (b *ScaleUpBalancer) FindSimilarNodeGroups(context *context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, nodeInfosForGroups map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, auto_errors.AutoscalerError) {
	return b.NodeGroupSetProcessor.FindSimilarNodeGroups(context, nodeGroup, nodeInfosForGroups)
}

// BalanceScaleUpBetweenGroups balances the scale up based on Flex Advisor guidance and notify Flex Advisor about the scale up decision.
func (b *ScaleUpBalancer) BalanceScaleUpBetweenGroups(context *context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, auto_errors.AutoscalerError) {
	if !IsFlexAdvisorProcessingEnabled(b.experimentsManager) {
		klog.Info("FlexAdvisor: balancer processing is disabled by FlexAdvisorProcessing experiment, falling back to balancing logic")
		return b.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}
	sampleGkeNodeGroup, err := getSampleGkeNodeGroup(groups)
	if err != nil {
		klog.V(4).Infof("FlexAdvisor: Falling back to balancing logic, due to an error extracting sample gke.NodeGroup err: %v", err)
		return b.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	locationPolicy := sampleGkeNodeGroup.LocationPolicy()

	if sampleGkeNodeGroup.IsTpuMig() && !isFlexAdvisorTPUEnabled(b.experimentsManager) {
		klog.V(4).Infof("FlexAdvisor: Falling back to balancing logic, TPU scale ups are not supported by Flex Advisor")
		return b.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}
	if sampleGkeNodeGroup.FlexStart() && !isFlexAdvisorDWSEnabled(b.experimentsManager) {
		klog.V(4).Infof("FlexAdvisor: Falling back to balancing logic, Flex start scale ups are not supported by Flex Advisor")
		return b.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	// For Location policy ANY we still rely on RLA.
	// If RLA based balancing failed we re-attempt with Flex Advisor.
	if locationPolicy == gke.LocationPolicyAny && !b.ignoreRLA {
		return b.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	scaleUpInfos, maxNewNodesFromNGs, err := maxScaleUpsByCloudProvider(groups)
	if err != nil {
		klog.V(4).Infof("FlexAdvisor: Falling back to balancing logic, due to an error extracting data from cloud provider err: %v", err)
		return b.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	balancingInfos, maxNewNodesFromFA, err := BalancingInfoFromFlexAdvisor(b.provider, b.cccLister, scaleUpInfos, b.experimentsManager, newNodes)
	if err != nil {
		klog.V(4).Infof("FlexAdvisor: Falling back to balancing logic, due to an error extracting balancing info from Flex Advisor err: %v", err)
		return b.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	initialNewNodes := newNodes
	newNodes = min(newNodes, maxNewNodesFromNGs, maxNewNodesFromFA)

	if newNodes < initialNewNodes {
		klog.V(2).Infof("FlexAdvisor: Requested scale-up (%v) is being capped. Constraints: NodeGroup MaxSize limit: %v, Flex Advisor limit: %v. Final node count: %v", initialNewNodes, maxNewNodesFromNGs, maxNewNodesFromFA, newNodes)
	}

	balancingInfos = distributeNewNodes(balancingInfos, newNodes)
	MarkUsed(balancingInfos)
	return b.balancingInfosToScaleUpInfos(balancingInfos), nil
}

func (b *ScaleUpBalancer) balancingInfosToScaleUpInfos(balancingInfos []*balancingInfo) []nodegroupset.ScaleUpInfo {
	var scaleUpInfos []nodegroupset.ScaleUpInfo
	for _, balancingInfo := range balancingInfos {
		scaleUpInfos = append(scaleUpInfos, balancingInfo.scaleUpInfo)
	}
	return scaleUpInfos
}

func maxScaleUpsByCloudProvider(groups []cloudprovider.NodeGroup) ([]nodegroupset.ScaleUpInfo, int, error) {
	scaleUpInfos := make([]nodegroupset.ScaleUpInfo, 0)
	possibleMaxScaleUpSize := 0
	for _, ng := range groups {
		currentSize, err := ng.TargetSize()
		if err != nil {
			return []nodegroupset.ScaleUpInfo{}, 0, err
		}
		maxSize := ng.MaxSize()
		if currentSize >= maxSize {
			// group already maxed, ignore it
			continue
		}
		// we still have capacity to expand
		possibleMaxScaleUpSize += (maxSize - currentSize)

		scaleUpInfos = append(scaleUpInfos, nodegroupset.ScaleUpInfo{
			Group:       ng,
			CurrentSize: currentSize,
			NewSize:     currentSize,
			MaxSize:     maxSize,
		})
	}
	return scaleUpInfos, possibleMaxScaleUpSize, nil
}

// BalancingInfoFromFlexAdvisor attaches FA data to scale up plan:
// - doesn't modify order of elements in scaleUpInfos
// - doesn't cross out elements from scaleUpInfos
// - in general sending decision to GCE is best-effort. Ie if node pool selected for scale up doesnt use CCC we will not generate data here used for notifying
// attaches data from FA and returns wrapped elements
// returns error if CCC or machine config was not found
// TODO(b/504971103): move this to separate notifier.go, keeping here to make backport review easier
func BalancingInfoFromFlexAdvisor(provider instanceavailability.Provider, cccLister lister.Lister, scaleUpInfos []nodegroupset.ScaleUpInfo, experimentsManager experiments.Manager, newNodes int) ([]*balancingInfo, int, error) {
	var balancingInfos []*balancingInfo
	snapshots := make(map[string]map[string]*instanceavailability.Snapshot)
	possibleMaxScaleUpSize := 0
	guidanceIdsUsed := make(map[string]bool)
	for _, scaleUpInfo := range scaleUpInfos {
		instanceRef, err := ConstructInstanceReference(scaleUpInfo.Group, cccLister, experimentsManager)
		// Currently we will not be able to generate instanceRef if pool:
		// - doesnt use CCC
		// - uses DWS (unless FlexAdvisorDWS::Enabled)
		// - uses TPU
		if err != nil {
			return []*balancingInfo{}, 0, err
		}

		var snapshot *instanceavailability.Snapshot
		found := false
		scopeCache, scopeFound := snapshots[instanceRef.FlexibilityScopeKey]
		if scopeFound {
			snapshot, found = scopeCache[instanceRef.InstanceConfigKey]
		} else {
			snapshots[instanceRef.FlexibilityScopeKey] = make(map[string]*instanceavailability.Snapshot)
		}

		if !found {
			snapshot, err = provider.AwaitInstanceAvailability(instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
			if err != nil {
				return []*balancingInfo{}, 0, err
			}
			snapshots[instanceRef.FlexibilityScopeKey][instanceRef.InstanceConfigKey] = snapshot
		}

		maxAvailability, found := snapshot.MaxAvailableInstances(instanceRef.Zone)
		if !found {
			provider.IncrementFlexAdvisorCacheQueryCount(metrics.FACacheMissNoZone, instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
			return []*balancingInfo{}, 0, fmt.Errorf("zone: %v not found in flex advisor snaphot for flexibility scope: %s, instance config: %s", instanceRef.Zone, instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
		} else {
			provider.IncrementFlexAdvisorCacheQueryCount(metrics.FACacheHit, instanceRef.FlexibilityScopeKey, instanceRef.InstanceConfigKey)
		}
		maxAvailability = min(maxAvailability, scaleUpInfo.MaxSize-scaleUpInfo.CurrentSize)

		// make sure we don't go below 0
		// Negative capacity represents over-provisioning and, we cannot help that now.
		maxAvailability = max(maxAvailability, 0)

		possibleMaxScaleUpSize += maxAvailability
		guidanceIdsUsed[snapshot.GuidanceId()] = true

		gcePreferenceScore, found := snapshot.GcePreferenceScore(instanceRef.Zone)
		if !found {
			klog.Warningf("FlexAdvisor: GCE Preference Score not present for scope %s and zone %s. Score value 0.0 will be used.", instanceRef.FlexibilityScopeKey, instanceRef.Zone)
		}
		balancingInfos = append(balancingInfos, &balancingInfo{
			scaleUpInfo:        scaleUpInfo,
			snapshot:           snapshot,
			instanceRef:        instanceRef,
			capacityLimit:      maxAvailability,
			gcePreferenceScore: gcePreferenceScore,
		})
	}
	klog.V(4).Infof("FlexAdvisor: Max scaleup size based on FlexAdvisor: %v (looking for: %v), guidanceIds used: %v, balancingInfos: %v", possibleMaxScaleUpSize, newNodes, slices.Collect(maps.Keys(guidanceIdsUsed)), balancingInfos)
	return balancingInfos, possibleMaxScaleUpSize, nil
}

func distributeNewNodes(balancingInfos []*balancingInfo, newNodes int) []*balancingInfo {
	pq := make(balancingInfoPriorityQueue, 0)
	heap.Init(&pq)
	for _, info := range balancingInfos {
		heap.Push(&pq, info)
	}

	for pq.Len() > 0 && newNodes > 0 {
		info := heap.Pop(&pq).(*balancingInfo)
		if info.capacityLimit <= 0 {
			continue
		}
		info.scaleUpInfo.NewSize += 1
		info.capacityLimit -= 1
		newNodes -= 1

		heap.Push(&pq, info)
	}

	// remove noop items
	// TODO(b/504971103): test whether removing of noop items is needed. In RLA, we don't filter them out(so ie if GCE returns such elements, they will get returned from balancers)
	balancingInfos = slices.DeleteFunc(balancingInfos, func(balancingInfo *balancingInfo) bool {
		return balancingInfo.scaleUpInfo.NewSize == balancingInfo.scaleUpInfo.CurrentSize
	})

	return balancingInfos
}

// NotifyDecision sends decision we made to GCE. GCE tries to reserve capacity for us
// Sending decision notification is best effort. If the scale up is intended for node pool without CCC,
// BalancingInfoFromFlexAdvisor will not be able to generate balancingInfos for NotifyDecision.
// TODO(b/504971103): move this to separate notifier.go, keeping here to make backport review easier
func MarkUsed(balancingInfos []*balancingInfo) {
	provisioningPlan := make(map[*instanceavailability.Snapshot]map[string]int)
	countUsed := 0
	for _, balancingInfo := range balancingInfos {

		// don't report noop items. in FA balancer we filter these out on earlier stage,
		// if this function is called from RLA they may not have been filtered out
		if balancingInfo.scaleUpInfo.NewSize == balancingInfo.scaleUpInfo.CurrentSize {
			continue
		}

		zonalCounts, found := provisioningPlan[balancingInfo.snapshot]
		if !found {
			zonalCounts = make(map[string]int)
		}

		used := balancingInfo.scaleUpInfo.NewSize - balancingInfo.scaleUpInfo.CurrentSize
		zonalCounts[balancingInfo.instanceRef.Zone] += used
		provisioningPlan[balancingInfo.snapshot] = zonalCounts
		countUsed += used
	}

	decisionId := string(uuid.NewUUID())

	klog.Infof("FlexAdvisor: marking %d capacity as used, decisionId=%s, balancingInfos after the decision: %v", countUsed, decisionId, balancingInfos)
	for snapshot, zonalCount := range provisioningPlan {
		_ = snapshot.MarkUsed(zonalCount, decisionId)
	}
}
func getSampleGkeNodeGroup(groups []cloudprovider.NodeGroup) (gke.NodeGroup, error) {
	if len(groups) == 0 {
		return nil, errors.New("got empty groups slice")
	}

	gkeNodeGroup, ok := groups[0].(gke.NodeGroup)
	if !ok {
		return nil, fmt.Errorf("got a NodeGroup that is not castable to gke.NodeGroup: %v", groups[0])
	}
	return gkeNodeGroup, nil
}

// balancingInfoPriorityQueue implements heap.Interface and holds balancingInfo pointers.
type balancingInfoPriorityQueue []*balancingInfo

// Len is the number of elements in the collection.
func (pq *balancingInfoPriorityQueue) Len() int {
	return len(*pq)
}

// Less reports whether the element with index i
// must sort before the element with index j.
// The priority is given to the node group that:
// 1. Has a smaller number of nodes (to keep groups balanced).
// 2. As a tie-breaker, has a higher GCE preference score.
func (pq *balancingInfoPriorityQueue) Less(i, j int) bool {
	// If scaleUpInfo.NewSize are different, the one with fewer nodes has higher priority.
	if (*pq)[i].scaleUpInfo.NewSize != (*pq)[j].scaleUpInfo.NewSize {
		return (*pq)[i].scaleUpInfo.NewSize < (*pq)[j].scaleUpInfo.NewSize
	}
	// If scaleUpInfo.NewSize are the same, the one with the higher gce preference score has higher priority.
	return (*pq)[i].gcePreferenceScore > (*pq)[j].gcePreferenceScore
}

// Swap swaps the elements with indexes i and j.
func (pq *balancingInfoPriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

// Push adds an item to the queue.
func (pq *balancingInfoPriorityQueue) Push(x any) {
	item := x.(*balancingInfo)
	*pq = append(*pq, item)
}

// Pop removes and returns the highest-priority item from the queue.
func (pq *balancingInfoPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}
