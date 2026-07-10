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

package reservations

import (
	"container/heap"
	"fmt"
	"reflect"

	gce_api "google.golang.org/api/compute/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	autoscaling_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	auto_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
)

type ReservationBalancingProcessor struct {
	nodegroupset.NodeGroupSetProcessor
	puller                   *gceclient.ReservationsPuller
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider
	provider                 machineConfigProvider
}

// NewReservationBalancingProcessor creates a new ScaleUpProcessor.
func NewReservationBalancingProcessor(p nodegroupset.NodeGroupSetProcessor, puller *gceclient.ReservationsPuller, localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider, provider machineConfigProvider) *ReservationBalancingProcessor {
	return &ReservationBalancingProcessor{
		NodeGroupSetProcessor:    p,
		puller:                   puller,
		localSSDDiskSizeProvider: localSSDDiskSizeProvider,
		provider:                 provider,
	}
}

// FindSimilarNodeGroups returns a list of NodeGroups similar to the one provided in parameter.
func (p *ReservationBalancingProcessor) FindSimilarNodeGroups(context *autoscaling_context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, nodeInfosForGroups map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, auto_errors.AutoscalerError) {
	return p.NodeGroupSetProcessor.FindSimilarNodeGroups(context, nodeGroup, nodeInfosForGroups)
}

// BalanceScaleUpBetweenGroups adds new nodes to the node groups to match unused reservations.
// New nodes are added to node groups attempting to balance the new sizes of node groups.
// Any remaining nodes are passed to the underlying NodeGroupSetProcessor for further balancing.
func (p *ReservationBalancingProcessor) BalanceScaleUpBetweenGroups(context *autoscaling_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, auto_errors.AutoscalerError) {

	var scaleUpInfos []nodegroupset.ScaleUpInfo

	pullerReservations := p.puller.GetReservations()
	// Subtract resources from reservations that can be used by upcoming nodes.
	reservations := p.consumeUpcomingScaleUps(context, pullerReservations)

	balancingInfos, err := p.createBalancingInfos(groups, reservations)
	if err != nil {
		return nil, err
	}

	balancingInfos, addedNodes := distributeNewNodes(balancingInfos, newNodes)

	wrappedNodeGroups, wrapErr := wrapNodeGroups(balancingInfos)
	if wrapErr != nil {
		klog.Infof("Falling back to balancing logic, due to an error in reservation based balancer Err: %v", wrapErr)
		// Ignore whatever balancing we've done so far, balance using original nodeGroups
		return p.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	if newNodes-addedNodes > 0 {
		scaleUpInfos, err = p.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, wrappedNodeGroups, newNodes-addedNodes)
		if err != nil {
			return nil, err
		}
	}

	var unWrapErr error
	scaleUpInfos, unWrapErr = unwrapNodeGroups(scaleUpInfos)
	if unWrapErr != nil {
		return nil, auto_errors.NewAutoscalerError(auto_errors.InternalError, fmt.Sprintf("Error when unWrapping node grpus. err: %v", unWrapErr.Error()))
	}

	scaleUpInfos = addMissingScaleUpInfo(scaleUpInfos, balancingInfos)
	scaleUpInfos = filterEmptyScaleUpInfos(scaleUpInfos)

	// Subtract resources from reservations that are used by the current scale up and update the puller.
	// The puller is not updated for reservations that could be consumed by upcoming node groups.
	p.puller.SetReservations(p.updateReservations(scaleUpInfos, reservations, pullerReservations))

	return scaleUpInfos, nil
}

func (p *ReservationBalancingProcessor) createBalancingInfos(groups []cloudprovider.NodeGroup, reservations []*gce_api.Reservation) ([]*balancingInfo, auto_errors.AutoscalerError) {
	var balancingInfos []*balancingInfo
	for _, group := range groups {
		currentSize, err := group.TargetSize()
		if err != nil {
			return nil, auto_errors.NewAutoscalerErrorf(auto_errors.CloudProviderError, "failed to get node group size: %v", err)
		}

		scaleUpInfo := nodegroupset.ScaleUpInfo{
			Group:       group,
			CurrentSize: currentSize,
			NewSize:     currentSize,
			MaxSize:     group.MaxSize(),
		}
		matchingReservations := MatchingUnusedReservations(p.provider, group, reservations, p.localSSDDiskSizeProvider)
		balancingInfos = append(balancingInfos, &balancingInfo{
			scaleUpInfo,
			matchingReservations,
		})
	}

	return balancingInfos, nil
}

func distributeNewNodes(balancingInfos []*balancingInfo, newNodes int) ([]*balancingInfo, int) {
	initialNodes := newNodes
	pq := make(balancingInfoPriorityQueue, 0)
	heap.Init(&pq)
	for _, info := range balancingInfos {
		if info.unUsedReservationCount <= 0 {
			continue
		}
		if info.scaleUpInfo.NewSize >= info.scaleUpInfo.MaxSize {
			continue
		}
		heap.Push(&pq, info)
	}

	for pq.Len() > 0 && newNodes > 0 {
		info := heap.Pop(&pq).(*balancingInfo)
		info.scaleUpInfo.NewSize += 1
		info.unUsedReservationCount -= 1
		newNodes -= 1

		if info.unUsedReservationCount <= 0 {
			continue
		}
		if info.scaleUpInfo.NewSize >= info.scaleUpInfo.MaxSize {
			continue
		}
		heap.Push(&pq, info)
	}

	return balancingInfos, initialNodes - newNodes
}

func addMissingScaleUpInfo(scaleUpInfos []nodegroupset.ScaleUpInfo, balancingInfos []*balancingInfo) []nodegroupset.ScaleUpInfo {
	scaleUpInfoFound := make(map[string]bool)
	for _, scaleUpInfo := range scaleUpInfos {
		scaleUpInfoFound[scaleUpInfo.Group.Id()] = true
	}

	for _, info := range balancingInfos {
		if scaleUpInfoFound[info.scaleUpInfo.Group.Id()] {
			continue
		}
		scaleUpInfoFound[info.scaleUpInfo.Group.Id()] = true

		scaleUpInfos = append(scaleUpInfos, info.scaleUpInfo)
	}
	return scaleUpInfos
}

// wrapNodeGroups wraps cloudprovider.NodeGroup in *nodeGroupWrapper to hide newly added nodes as current nodes.
func wrapNodeGroups(balancingInfos []*balancingInfo) ([]cloudprovider.NodeGroup, error) {
	var wrapped []cloudprovider.NodeGroup
	for _, info := range balancingInfos {
		mig, ok := info.scaleUpInfo.Group.(*gke.GkeMig)
		if !ok {
			return nil, fmt.Errorf("got a NodeGroup that is not castable to GkeMig: %v", info.scaleUpInfo.Group)
		}
		wrapped = append(wrapped, &nodeGroupWrapper{
			GkeMig:     mig,
			addedNodes: info.scaleUpInfo.NewSize - info.scaleUpInfo.CurrentSize,
		})
	}
	return wrapped, nil
}

// unwrapNodeGroups unwraps *nodeGroupWrapper to cloudprovider.NodeGroup.
func unwrapNodeGroups(scaleUpInfos []nodegroupset.ScaleUpInfo) ([]nodegroupset.ScaleUpInfo, error) {
	var unwrapped []nodegroupset.ScaleUpInfo
	for _, scaleUpInfo := range scaleUpInfos {
		ng, ok := scaleUpInfo.Group.(*nodeGroupWrapper)
		if !ok {
			return nil, fmt.Errorf("unexpected cloudprovider.NodeGroup type, got: %s, want: *nodeGroupWrapper", reflect.TypeOf(scaleUpInfo.Group))
		}

		targetSize, err := ng.GkeMig.TargetSize()
		if err != nil {
			return nil, err
		}

		unwrapped = append(unwrapped, nodegroupset.ScaleUpInfo{
			Group:       ng.GkeMig,
			CurrentSize: targetSize,
			NewSize:     scaleUpInfo.NewSize,
			MaxSize:     ng.GkeMig.MaxSize(),
		})
	}
	return unwrapped, nil
}

// consumeUpcomingScaleUps subtract resources from matching reservation for upcoming node groups.
func (p *ReservationBalancingProcessor) consumeUpcomingScaleUps(context *autoscaling_context.AutoscalingContext, reservations []*gce_api.Reservation) []*gce_api.Reservation {
	if context == nil || context.CloudProvider == nil {
		return reservations
	}
	consumedReservations := p.reservationsForUpcomingNodes(context, reservations)
	if len(consumedReservations) == 0 {
		return reservations
	}
	adjusted := subtractReservations(reservations, consumedReservations)
	return adjusted
}

func (p *ReservationBalancingProcessor) reservationsForUpcomingNodes(context *autoscaling_context.AutoscalingContext, reservations []*gce_api.Reservation) map[uint64]int {
	consumedReservations := make(map[uint64]int)
	for _, ng := range context.CloudProvider.NodeGroups() {
		gkeMig, ok := ng.(*gke.GkeMig)
		if !ok || !gkeMig.IsUpcoming() {
			continue
		}
		size, err := ng.TargetSize()
		if err != nil && size <= 0 {
			continue
		}
		for _, rsv := range reservations {
			if reservationMatch(p.provider, ng, rsv, p.localSSDDiskSizeProvider) {
				unused := int(rsv.SpecificReservation.Count) - int(rsv.SpecificReservation.InUseCount) - consumedReservations[rsv.Id]
				if size > unused {
					consumedReservations[rsv.Id] += unused
					size = size - unused
				} else {
					consumedReservations[rsv.Id] += size
					size = 0
				}
			}
		}
	}
	return consumedReservations
}

func subtractReservations(reservations []*gce_api.Reservation, consumedReservations map[uint64]int) []*gce_api.Reservation {
	var subtracted []*gce_api.Reservation
	for _, rsv := range reservations {
		consumed := consumedReservations[rsv.Id]
		if consumed == 0 {
			subtracted = append(subtracted, rsv)
			continue
		}
		specRsvCopy := *rsv.SpecificReservation
		specRsvCopy.InUseCount += int64(consumed)
		rsvCopy := *rsv
		rsvCopy.SpecificReservation = &specRsvCopy
		subtracted = append(subtracted, &rsvCopy)
	}
	return subtracted
}

func (p *ReservationBalancingProcessor) updateReservations(scaleUpInfos []nodegroupset.ScaleUpInfo, reservations, pulledReservations []*gce_api.Reservation) []*gce_api.Reservation {
	usedReservations := map[gceclient.MetricLabels]int{}
	pulledReservationMap := map[uint64]*gce_api.Reservation{}

	for _, pulledRsv := range pulledReservations {
		pulledReservationMap[pulledRsv.Id] = pulledRsv
	}

	for _, scaleUpInfo := range scaleUpInfos {
		reservedNodes := MatchingUnusedReservations(p.provider, scaleUpInfo.Group, reservations, p.localSSDDiskSizeProvider)
		newNodes := scaleUpInfo.NewSize - scaleUpInfo.CurrentSize
		newReservedNodes := min(newNodes, reservedNodes)

		for _, rsv := range reservations {
			if reservationMatch(p.provider, scaleUpInfo.Group, rsv, p.localSSDDiskSizeProvider) {
				unusedNodes := int(rsv.SpecificReservation.Count - rsv.SpecificReservation.InUseCount)

				if unusedNodes > 0 {
					pulledRsv, found := pulledReservationMap[rsv.Id]
					if !found {
						klog.Errorf("Reservation not fround. Self link: %v. This is unexpected", rsv.SelfLink)
						continue
					}
					usedNodes := min(unusedNodes, newReservedNodes)
					pulledRsv.SpecificReservation.InUseCount += int64(usedNodes)
					newReservedNodes -= usedNodes
					usedReservations[gceclient.ReservationMetricLabels(pulledRsv)] += usedNodes
				}
			}
		}
	}

	for labels, count := range usedReservations {
		metrics.Metrics.IncreaseReservationsUsed(labels.ToMap(), count)
	}

	return pulledReservations
}

// filterEmptyScaleUpInfos removes empty scale-up infos
func filterEmptyScaleUpInfos(scaleUpInfos []nodegroupset.ScaleUpInfo) []nodegroupset.ScaleUpInfo {
	var result []nodegroupset.ScaleUpInfo
	for _, scaleUpInfo := range scaleUpInfos {
		if scaleUpInfo.CurrentSize < scaleUpInfo.NewSize {
			result = append(result, scaleUpInfo)
		}
	}

	return result
}

// balancingInfo contains balancing information about planned scale-up of a single NodeGroup.
type balancingInfo struct {
	scaleUpInfo            nodegroupset.ScaleUpInfo
	unUsedReservationCount int
}

// balancingInfoPriorityQueue implements heap.Interface and holds balancingInfo pointers.
type balancingInfoPriorityQueue []*balancingInfo

// Len is the number of elements in the collection.
func (pq *balancingInfoPriorityQueue) Len() int {
	return len(*pq)
}

// Less reports whether the element with index i must sort before the element with index j.
// The priority is given to the node group that has a smaller number of nodes (to keep groups balanced).
func (pq *balancingInfoPriorityQueue) Less(i, j int) bool {
	return (*pq)[i].scaleUpInfo.NewSize < (*pq)[j].scaleUpInfo.NewSize
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

// nodeGroupWrapper wraps cloudprovider.NodeGroup to hide added nodes as current nodes,
// so that the next balancers will see as current nodes, with cloudprovider.NodeGroup interface
type nodeGroupWrapper struct {
	*gke.GkeMig
	addedNodes int
}

func (n *nodeGroupWrapper) TargetSize() (int, error) {
	size, err := n.GkeMig.TargetSize()
	return size + n.addedNodes, err
}
