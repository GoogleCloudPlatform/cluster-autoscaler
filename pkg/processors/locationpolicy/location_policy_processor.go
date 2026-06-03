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

package locationpolicy

import (
	"errors"
	"fmt"

	"google.golang.org/api/googleapi"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	autoscaling_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	auto_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	internal_processors "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors"
	klog "k8s.io/klog/v2"
)

// Balancer defines a location policy specific balancing strategy.
type Balancer interface {
	Balance(gkeNodeGroups []gke.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, error)
}

// Processor implements NodeGroupSetProcessor. If enabled it enforces the node-pool
// location policy during scale-up. If the algorithm fails it defaults to the
// open-source implementation of BalancingProcessor.
type Processor struct {
	nodegroupset.NodeGroupSetProcessor

	provider             internal_processors.ProcessorsCloudProvider
	balancers            map[gke.LocationPolicyEnum]Balancer
	experimentsManager   experiments.Manager
	isFlexAdvisorEnabled bool
	iaProvider           instanceavailability.Provider
	cccLister            lister.Lister
}

// NewProcessor creates a new NodeGroupSetProcessor.
func NewProcessor(p nodegroupset.NodeGroupSetProcessor, provider internal_processors.ProcessorsCloudProvider, balancers map[gke.LocationPolicyEnum]Balancer, experimentsManager experiments.Manager, isFlexAdvisorEnabled bool, iaProvider instanceavailability.Provider, cccLister lister.Lister) *Processor {
	return &Processor{
		NodeGroupSetProcessor: p,
		provider:              provider,
		balancers:             balancers,
		experimentsManager:    experimentsManager,
		isFlexAdvisorEnabled:  isFlexAdvisorEnabled,
		iaProvider:            iaProvider,
		cccLister:             cccLister,
	}
}

// FindSimilarNodeGroups returns a list of NodeGroups similar to the one provided in parameter.
func (s *Processor) FindSimilarNodeGroups(context *autoscaling_context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, nodeInfosForGroups map[string]*framework.NodeInfo) ([]cloudprovider.NodeGroup, auto_errors.AutoscalerError) {
	return s.NodeGroupSetProcessor.FindSimilarNodeGroups(context, nodeGroup, nodeInfosForGroups)
}

// BalanceScaleUpBetweenGroups splits the scale-up between the node groups.
// When the processor is enabled the location_policy is taken into account.
// In case of any errors the processor fallbacks to the balancing logic.
func (s *Processor) BalanceScaleUpBetweenGroups(context *autoscaling_context.AutoscalingContext, groups []cloudprovider.NodeGroup, newNodes int) ([]nodegroupset.ScaleUpInfo, auto_errors.AutoscalerError) {
	if len(groups) < 2 && !context.ProvisioningRequestScaleUpMode {
		klog.V(2).Infof("Falling back to balancing logic, less than 2 node groups to balance")
		return s.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	gkeNodeGroup, err := getGkeNodeGroups(groups)
	if err != nil {
		klog.Warningf("Falling back to balancing logic, due to an error for casting groups to *gke.NodeGroup: %v", err)
		return s.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	// Since all gkeNodeGroup should have the same nodepool we just pick value from the first one.
	locationPolicy := gkeNodeGroup[0].LocationPolicy()
	klog.V(2).Infof("Using location policy: %s", locationPolicy)

	capacity, err := getGkeNodeGroupsCapacity(gkeNodeGroup)
	if err != nil {
		klog.Warningf("Falling back to balancing logic, due to an error for groups node capacity: %v", err)
		return s.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}
	if newNodes >= capacity {
		klog.V(2).Infof("Requested scale-up (%v) is greater or equal to node group set capacity, using balancing logic", newNodes)
		return s.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}
	if locationPolicy == gke.LocationPolicyAny && gkeNodeGroup[0].IsTpuMig() && gkeNodeGroup[0].FlexStartNonQueued() && s.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.RecommendLocationsDisabledForTPUFlag, false) {
		klog.V(2).Infof("Falling back to balancing logic, RLA disabled for TPU FSNQ")
		return s.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	balancer, found := s.balancers[locationPolicy]
	if !found {
		// The default location policy is BALANCED and it doesn't have a dedicated balancer object. Falling back is perfectly fine in that case.
		// This case can even occur for healthy scale ups
		klog.Infof("Balancer not found for location policy: %s", locationPolicy)
		return s.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
	}

	scaleUpInfos, err := balancer.Balance(gkeNodeGroup, newNodes)
	if err != nil {
		if context.ProvisioningRequestScaleUpMode && locationPolicy == gke.LocationPolicyAny && noCapacityErr(err) {
			return nil, auto_errors.NewAutoscalerErrorf(auto_errors.CloudProviderError, "failed to find capacity: %v", err)
		} else {
			klog.Infof("Falling back to balancing logic, due to an error for location policy %s: %v", locationPolicy, err)
			return s.NodeGroupSetProcessor.BalanceScaleUpBetweenGroups(context, groups, newNodes)
		}
	}
	// TODO(b/504971103): move notifying logic directly to location_policy_any_balancer.go, so directly to RLA service.
	if s.isFlexAdvisorEnabled && locationPolicy == gke.LocationPolicyAny {
		balancingInfos, _, faErr := flexadvisor.BalancingInfoFromFlexAdvisor(s.iaProvider, s.cccLister, scaleUpInfos, s.experimentsManager, newNodes)
		if faErr == nil {
			flexadvisor.MarkUsed(balancingInfos)
		} else {
			klog.Infof("FlexAdvisor: not notifying GCE about scale up decision in RLA balancer, got error while fetching FA data: %v", faErr)
		}
	}
	return scaleUpInfos, nil
}

func getGkeNodeGroups(groups []cloudprovider.NodeGroup) ([]gke.NodeGroup, error) {
	if len(groups) == 0 {
		return nil, errors.New("got empty groups slice")
	}
	var gkeNodeGroups []gke.NodeGroup
	for _, group := range groups {
		gkeNodeGroup, ok := group.(gke.NodeGroup)
		if !ok {
			return nil, fmt.Errorf("got a NodeGroup that is not castable to Gke.NodeGroup: %v", group)
		}
		gkeNodeGroups = append(gkeNodeGroups, gkeNodeGroup)
	}
	return gkeNodeGroups, nil
}

func getGkeNodeGroupsCapacity(gkeNodeGroups []gke.NodeGroup) (int, error) {
	capacity := 0
	for _, group := range gkeNodeGroups {
		targetSize, err := group.TargetSize()
		if err != nil {
			return 0, err
		}
		if group.MaxSize() > targetSize {
			capacity += group.MaxSize() - targetSize
		}
	}
	return capacity, nil
}

func noCapacityErr(err error) bool {
	if apierr, ok := err.(*googleapi.Error); ok && apierr.Code == 403 {
		return true
	}
	return false
}
