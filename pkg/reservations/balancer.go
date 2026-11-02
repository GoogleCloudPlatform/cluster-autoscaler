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
	"context"
	"fmt"
	"reflect"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gce/localssdsize"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/reservation"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/priorityqueue"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	autoscaling_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupset"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	auto_errors "sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

type ReservationBalancingProcessor struct {
	nodegroupset.NodeGroupSetProcessor
	puller                   *gceclient.ReservationsPuller
	localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider
	provider                 machineConfigProvider

	// upcomingDeductions tracks, by reservation ID, the slots that upcoming node groups claimed in
	// updateReservations and that are still subtracted from the reservation stored in the puller.
	// consumeUpcomingScaleUps skips those nodes, so that they are not subtracted twice.
	//
	// BalanceScaleUpBetweenGroups is only called from the main autoscaling loop, so access to this
	// map is not synchronized.
	upcomingDeductions map[uint64]*upcomingDeduction
}

// upcomingDeduction describes the slots of a single reservation claimed by upcoming node groups.
type upcomingDeduction struct {
	// snapshot is the reservation object stored in the puller that the slots were subtracted
	// from. The puller replaces the object when it refreshes the reservation from GCE, which
	// invalidates the deduction.
	snapshot *reservation.Reservation
	// nodes is the number of subtracted slots by upcoming node group ID.
	nodes map[string]int
}

// NewReservationBalancingProcessor creates a new ScaleUpProcessor.
func NewReservationBalancingProcessor(p nodegroupset.NodeGroupSetProcessor, puller *gceclient.ReservationsPuller, localSSDDiskSizeProvider localssdsize.LocalSSDSizeProvider, provider machineConfigProvider) *ReservationBalancingProcessor {
	return &ReservationBalancingProcessor{
		NodeGroupSetProcessor:    p,
		puller:                   puller,
		localSSDDiskSizeProvider: localSSDDiskSizeProvider,
		provider:                 provider,
		upcomingDeductions:       make(map[uint64]*upcomingDeduction),
	}
}

// FindSimilarNodeGroups returns a list of NodeGroups similar to the one provided in parameter.
func (p *ReservationBalancingProcessor) FindSimilarNodeGroups(ctx context.Context, autoscalingCtx *autoscaling_context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, nodeInfosForGroups map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, auto_errors.AutoscalerError) {
	return p.NodeGroupSetProcessor.FindSimilarNodeGroups(ctx, autoscalingCtx, nodeGroup, nodeInfosForGroups)
}

// BalanceScaleUpBetweenGroups adds new nodes to the node groups to match unused reservations.
// New nodes are added to node groups attempting to balance the new sizes of node groups.
// Any remaining nodes are passed to the underlying NodeGroupSetProcessor for further balancing.
func (p *ReservationBalancingProcessor) BalanceScaleUpBetweenGroups(ctx context.Context, autoscalingCtx *autoscaling_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, auto_errors.AutoscalerError) {

	var scaleUpInfos []nodegroupset.ScaleUpInfo

	pullerReservations := p.puller.GetReservations()
	p.dropStaleUpcomingDeductions(pullerReservations)
	// Subtract resources from reservations that can be used by upcoming nodes.
	reservations := p.consumeUpcomingScaleUps(autoscalingCtx, pullerReservations)

	balancingInfos, err := p.createBalancingInfos(groups, reservations)
	if err != nil {
		return nil, err
	}

	balancingInfos, addedNodes := distributeNewNodes(balancingInfos, newNodes)

	wrappedNodeGroups, wrapErr := wrapNodeGroups(balancingInfos)
	if wrapErr != nil {
		klog.Infof("Falling back to balancing logic, due to an error in reservation based balancer Err: %v", wrapErr)
		// Ignore whatever balancing we've done so far, balance using original nodeGroups
		return p.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(ctx, autoscalingCtx, groups, newNodes)
	}

	if newNodes-addedNodes > 0 {
		scaleUpInfos, err = p.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(ctx, autoscalingCtx, wrappedNodeGroups, newNodes-addedNodes)
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
	p.puller.SetReservations(p.updateReservations(scaleUpInfos, reservations, pullerReservations))

	return scaleUpInfos, nil
}

func (p *ReservationBalancingProcessor) createBalancingInfos(groups []cloudprovider.NodeGroup, reservations []*reservation.Reservation) ([]*balancingInfo, auto_errors.AutoscalerError) {
	var balancingInfos []*balancingInfo
	for _, group := range groups {
		currentSize, err := group.TargetSize(context.TODO())
		if err != nil {
			return nil, auto_errors.NewAutoscalerErrorf(auto_errors.CloudProviderError, "failed to get node group size: %v", err)
		}

		scaleUpInfo := nodegroupset.ScaleUpInfo{
			Group:       group,
			CurrentSize: currentSize,
			NewSize:     currentSize,
			MaxSize:     group.MaxSize(context.TODO()),
		}
		matchingReservations, _ := matchingUnusedReservations(p.provider, group, reservations, p.localSSDDiskSizeProvider, false)
		balancingInfos = append(balancingInfos, &balancingInfo{
			scaleUpInfo:          scaleUpInfo,
			matchingReservations: matchingReservations,
		})
	}

	return balancingInfos, nil
}

func distributeNewNodes(balancingInfos []*balancingInfo, newNodes int) ([]*balancingInfo, int) {
	initialNodes := newNodes
	claimed := make(map[uint64]int)

	hasUnusedReservation := func(info *balancingInfo) bool {
		for _, rsv := range info.matchingReservations {
			if rsv.SpecificReservation != nil && claimed[rsv.ID] < int(rsv.SpecificReservation.AvailableCount) {
				return true
			}
		}
		return false
	}

	claimReservationSlot := func(info *balancingInfo) bool {
		for _, rsv := range info.matchingReservations {
			if rsv.SpecificReservation != nil && claimed[rsv.ID] < int(rsv.SpecificReservation.AvailableCount) {
				claimed[rsv.ID]++
				return true
			}
		}
		return false
	}

	pq := priorityqueue.NewWithLessComparator(func(a, b *balancingInfo) bool {
		return a.scaleUpInfo.NewSize < b.scaleUpInfo.NewSize
	})
	for _, info := range balancingInfos {
		if !hasUnusedReservation(info) {
			continue
		}
		if info.scaleUpInfo.NewSize >= info.scaleUpInfo.MaxSize {
			continue
		}
		pq.Push(info)
	}

	for newNodes > 0 {
		info, ok := pq.Pop()
		if !ok {
			break
		}
		if !claimReservationSlot(info) {
			continue
		}
		info.scaleUpInfo.NewSize++
		newNodes--

		if !hasUnusedReservation(info) {
			continue
		}
		if info.scaleUpInfo.NewSize >= info.scaleUpInfo.MaxSize {
			continue
		}
		pq.Push(info)
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

		targetSize, err := ng.GkeMig.TargetSize(context.TODO())
		if err != nil {
			return nil, err
		}

		unwrapped = append(unwrapped, nodegroupset.ScaleUpInfo{
			Group:       ng.GkeMig,
			CurrentSize: targetSize,
			NewSize:     scaleUpInfo.NewSize,
			MaxSize:     ng.GkeMig.MaxSize(context.TODO()),
		})
	}
	return unwrapped, nil
}

// consumeUpcomingScaleUps subtract resources from matching reservation for upcoming node groups.
func (p *ReservationBalancingProcessor) consumeUpcomingScaleUps(context *autoscaling_context.AutoscalingContext, reservations []*reservation.Reservation) []*reservation.Reservation {
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

func isUpcomingNodeGroup(ng cloudprovider.NodeGroup) bool {
	gkeMig, ok := ng.(*gke.GkeMig)
	return ok && gkeMig.IsUpcoming()
}

func (p *ReservationBalancingProcessor) reservationsForUpcomingNodes(autoscalingCtx *autoscaling_context.AutoscalingContext, reservations []*reservation.Reservation) map[uint64]int {
	consumedReservations := make(map[uint64]int)
	upcomingIDs := make(map[string]bool)
	for _, ng := range autoscalingCtx.CloudProvider.NodeGroups(context.TODO()) {
		if !isUpcomingNodeGroup(ng) {
			continue
		}
		upcomingIDs[ng.Id()] = true
		size, err := ng.TargetSize(context.TODO())
		if err != nil {
			continue
		}
		// Nodes that are already subtracted from the reservations stored in the puller must not be
		// subtracted again.
		size -= p.deductedUpcomingNodes(ng.Id())
		if size <= 0 {
			continue
		}
		for _, rsv := range reservations {
			if ok, _, _ := reservationMatch(p.provider, ng, rsv, p.localSSDDiskSizeProvider, false); ok {
				unused := int(rsv.SpecificReservation.AvailableCount) - consumedReservations[rsv.ID]
				if size > unused {
					consumedReservations[rsv.ID] += unused
					size = size - unused
				} else {
					consumedReservations[rsv.ID] += size
					size = 0
				}
			}
		}
	}
	p.dropDeductionsOfNotUpcomingNodeGroups(upcomingIDs)
	return consumedReservations
}

// dropStaleUpcomingDeductions drops deductions for reservations that the puller refreshed (or no
// longer returns) since the deductions were recorded. Refreshed reservations don't include the
// deductions, so consumeUpcomingScaleUps has to subtract the upcoming nodes again.
func (p *ReservationBalancingProcessor) dropStaleUpcomingDeductions(pulledReservations []*reservation.Reservation) {
	if len(p.upcomingDeductions) == 0 {
		return
	}
	pulled := make(map[uint64]*reservation.Reservation, len(pulledReservations))
	for _, rsv := range pulledReservations {
		pulled[rsv.ID] = rsv
	}
	for id, deduction := range p.upcomingDeductions {
		if pulled[id] != deduction.snapshot {
			delete(p.upcomingDeductions, id)
		}
	}
}

// dropDeductionsOfNotUpcomingNodeGroups drops deductions of node groups that are no longer
// upcoming. Their slots stay subtracted from the reservations stored in the puller until the
// puller refreshes them, the same as for any other node group.
func (p *ReservationBalancingProcessor) dropDeductionsOfNotUpcomingNodeGroups(upcomingIDs map[string]bool) {
	for id, deduction := range p.upcomingDeductions {
		for ngID := range deduction.nodes {
			if !upcomingIDs[ngID] {
				delete(deduction.nodes, ngID)
			}
		}
		if len(deduction.nodes) == 0 {
			delete(p.upcomingDeductions, id)
		}
	}
}

// deductedUpcomingNodes returns the number of nodes of the upcoming node group that are already
// subtracted from the reservations stored in the puller.
func (p *ReservationBalancingProcessor) deductedUpcomingNodes(nodeGroupID string) int {
	deducted := 0
	for _, deduction := range p.upcomingDeductions {
		deducted += deduction.nodes[nodeGroupID]
	}
	return deducted
}

// recordUpcomingDeductions records slots of a reservation claimed by upcoming node groups, after
// the reservation object stored in the puller changes from oldRsv to newRsv. Deductions recorded
// against oldRsv are carried over, as newRsv is derived from it.
func (p *ReservationBalancingProcessor) recordUpcomingDeductions(oldRsv, newRsv *reservation.Reservation, claimedByUpcoming map[string]int) {
	deduction := p.upcomingDeductions[newRsv.ID]
	if deduction == nil || deduction.snapshot != oldRsv {
		if len(claimedByUpcoming) == 0 {
			delete(p.upcomingDeductions, newRsv.ID)
			return
		}
		deduction = &upcomingDeduction{nodes: make(map[string]int, len(claimedByUpcoming))}
		if p.upcomingDeductions == nil {
			p.upcomingDeductions = make(map[uint64]*upcomingDeduction)
		}
		p.upcomingDeductions[newRsv.ID] = deduction
	}
	deduction.snapshot = newRsv
	for ngID, nodes := range claimedByUpcoming {
		deduction.nodes[ngID] += nodes
	}
}

func subtractReservations(reservations []*reservation.Reservation, consumedReservations map[uint64]int) []*reservation.Reservation {
	var subtracted []*reservation.Reservation
	for _, rsv := range reservations {
		consumed := consumedReservations[rsv.ID]
		if consumed == 0 {
			subtracted = append(subtracted, rsv)
			continue
		}
		rsvCopy := rsv.Clone()
		rsvCopy.SpecificReservation.AvailableCount -= int64(consumed)
		subtracted = append(subtracted, rsvCopy)
	}
	return subtracted
}

// updateReservations returns pulledReservations with the slots used by scaleUpInfos subtracted.
// Slots are subtracted for upcoming node groups as well, so that everything that reads the puller
// sees them as used. They are recorded in upcomingDeductions, so that consumeUpcomingScaleUps
// doesn't subtract them for the second time once the node groups report their new target size.
func (p *ReservationBalancingProcessor) updateReservations(scaleUpInfos []nodegroupset.ScaleUpInfo, reservations, pulledReservations []*reservation.Reservation) []*reservation.Reservation {
	usedReservations := map[gceclient.MetricLabels]int{}
	pulledReservationMap := map[uint64]*reservation.Reservation{}

	for _, pulledRsv := range pulledReservations {
		pulledReservationMap[pulledRsv.ID] = pulledRsv
	}

	// claimed counts the slots of each reservation used by earlier node groups in this call.
	claimed := map[uint64]int{}
	// claimedByUpcoming counts the slots of each reservation used by upcoming node groups, by node group ID.
	claimedByUpcoming := map[uint64]map[string]int{}
	for _, scaleUpInfo := range scaleUpInfos {
		newReservedNodes := scaleUpInfo.NewSize - scaleUpInfo.CurrentSize
		upcoming := isUpcomingNodeGroup(scaleUpInfo.Group)

		for _, rsv := range reservations {
			if newReservedNodes <= 0 {
				break
			}
			if ok, _, _ := reservationMatch(p.provider, scaleUpInfo.Group, rsv, p.localSSDDiskSizeProvider, false); ok {
				unusedNodes := int(rsv.SpecificReservation.AvailableCount) - claimed[rsv.ID]

				if unusedNodes > 0 {
					pulledRsv, found := pulledReservationMap[rsv.ID]
					if !found {
						klog.Errorf("Reservation not found. Self link: %v. This is unexpected", rsv.SelfLink)
						continue
					}
					usedNodes := min(unusedNodes, newReservedNodes)
					claimed[rsv.ID] += usedNodes
					if upcoming {
						if claimedByUpcoming[rsv.ID] == nil {
							claimedByUpcoming[rsv.ID] = map[string]int{}
						}
						claimedByUpcoming[rsv.ID][scaleUpInfo.Group.Id()] += usedNodes
					}
					newReservedNodes -= usedNodes
					usedReservations[gceclient.ReservationMetricLabels(pulledRsv)] += usedNodes
				}
			}
		}
	}

	for labels, count := range usedReservations {
		metrics.Metrics.IncreaseReservationsUsed(labels.ToMap(), count)
	}

	if len(claimed) == 0 {
		return pulledReservations
	}

	updatedReservations := make([]*reservation.Reservation, 0, len(pulledReservations))
	for _, pulledRsv := range pulledReservations {
		used := claimed[pulledRsv.ID]
		if used == 0 || pulledRsv.SpecificReservation == nil {
			updatedReservations = append(updatedReservations, pulledRsv)
			continue
		}
		rsvCopy := pulledRsv.Clone()
		rsvCopy.SpecificReservation.AvailableCount = max(0, rsvCopy.SpecificReservation.AvailableCount-int64(used))
		p.recordUpcomingDeductions(pulledRsv, rsvCopy, claimedByUpcoming[pulledRsv.ID])
		updatedReservations = append(updatedReservations, rsvCopy)
	}

	return updatedReservations
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
	scaleUpInfo          nodegroupset.ScaleUpInfo
	matchingReservations []*reservation.Reservation
}

// nodeGroupWrapper wraps cloudprovider.NodeGroup to hide added nodes as current nodes,
// so that the next balancers will see as current nodes, with cloudprovider.NodeGroup interface
type nodeGroupWrapper struct {
	*gke.GkeMig
	addedNodes int
}

func (n *nodeGroupWrapper) TargetSize(ctx context.Context) (int, error) {
	size, err := n.GkeMig.TargetSize(ctx)
	return size + n.addedNodes, err
}
