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

package scaleup

import (
	"context"
	"fmt"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	provreqv1 "k8s.io/autoscaler/cluster-autoscaler/apis/provisioningrequest/autoscaling.x-k8s.io/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	cactx "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/equivalence"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	ca_processors "k8s.io/autoscaler/cluster-autoscaler/processors"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqwrapper"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/klog/v2"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podsharding"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/locationpolicy"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/provreqstate"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/scaleup/reasons"
)

const (
	// For this period since creation, pending Provisioning Requests failing before attempting creation of Resize Request (e.g. because of lack of QueuedProvisioning nodepool, which might still be registering to k8s) will be retried in a next CA loop.
	pendingProvisioningRequestRetryPeriod = 2 * time.Minute

	// Threshold for the number of ProvisioningRequest-derived Pods we process when computing expansion options.
	// When we cross this threshold, we stop considering any new ProvisioningRequests.
	expansionOptionPodsCountThreshold = 2000

	// Cap for the number of ProvisioningRequest to consider in a single expansion option.
	provReqInExpansionOptionCountCap = 5

	// Limit for the number of autoscaler errors to return in the ScaleUp func
	autoscalerErrorReturnLimit = 3

	// Limit for the number of similar node groups to log
	similarGroupsLogLimit = 10
)

var (
	provisioningRequestStillRetriableErr = fmt.Errorf("Provisioning Request cannot be failed because it's still retriable")
)

type GkeCloudProvider interface {
	GetMigInstanceTemplateSelfLink(*gke.GkeMig) (string, error)
	RecommendLocations(ctx context.Context, request gceclient.RecommendLocationsRequest) (*gceclient.RecommendLocationsResponse, error)
	GetAllZones() ([]string, error)
}

// Orchestrator implements scaleup.Orchestrator interface for Provisioning Request.
// It allows GKE to handle the scale ups atomically.
type Orchestrator struct {
	wrappedOrchestrator *orchestrator.ScaleUpOrchestrator
	prCache             *provreqcache.QueuedProvisioningCache
	prClient            *provreqclient.ProvisioningRequestClient

	autoscalingContext           *cactx.AutoscalingContext
	processors                   *ca_processors.AutoscalingProcessors
	clusterStateRegistry         *clusterstate.ClusterStateRegistry
	scaleUpExecutor              *scaleUpExecutor
	provider                     GkeCloudProvider
	taintConfig                  taints.TaintConfig
	initialized                  bool
	now                          func() time.Time
	experimentsManager           experiments.Manager
	maxProvReqBinpackingDuration time.Duration
	fastpathBinpackingEnabled    bool
	napResourceAnalyzerFunc      estimator.EstimationAnalyserFunc
	quotasTrackerFactory         *resourcequotas.TrackerFactory
}

type applyDelta bool

const (
	checkAndApply applyDelta = true
	checkOnly     applyDelta = false
)

// NewOrchestrator returns new instance of scale up orchestrator.
func NewOrchestrator(
	o *orchestrator.ScaleUpOrchestrator,
	provider GkeCloudProvider,
	prClient *provreqclient.ProvisioningRequestClient,
	prCache *provreqcache.QueuedProvisioningCache,
	maxProvReqBinpackingDuration time.Duration,
	fastpathBinpackingEnabled bool,
	experimentsManager experiments.Manager,
	napResourceAnalyzerFunc estimator.EstimationAnalyserFunc,
) scaleup.Orchestrator {
	return &Orchestrator{
		wrappedOrchestrator:          o,
		prCache:                      prCache,
		prClient:                     prClient,
		initialized:                  false,
		now:                          time.Now,
		provider:                     provider,
		experimentsManager:           experimentsManager,
		maxProvReqBinpackingDuration: maxProvReqBinpackingDuration,
		fastpathBinpackingEnabled:    fastpathBinpackingEnabled,
		napResourceAnalyzerFunc:      napResourceAnalyzerFunc,
	}
}

// Initialize initializes the orchestrator object with required fields.
func (o *Orchestrator) Initialize(
	autoscalingContext *cactx.AutoscalingContext,
	processors *ca_processors.AutoscalingProcessors,
	clusterStateRegistry *clusterstate.ClusterStateRegistry,
	estimatorBuilder estimator.EstimatorBuilder,
	taintConfig taints.TaintConfig,
	quotasTrackerFactory *resourcequotas.TrackerFactory,
) {
	o.wrappedOrchestrator.Initialize(autoscalingContext, processors, clusterStateRegistry, estimatorBuilder, taintConfig, quotasTrackerFactory)
	o.autoscalingContext = autoscalingContext
	o.processors = processors
	o.clusterStateRegistry = clusterStateRegistry
	o.taintConfig = taintConfig
	o.scaleUpExecutor = newScaleUpExecutor(autoscalingContext, processors.ScaleStateNotifier, o.processors.AsyncNodeGroupStateChecker)
	o.quotasTrackerFactory = quotasTrackerFactory
	o.initialized = true
}

// ScaleUp tries to scale the cluster up. Returns appropriate status or error if
// an unexpected error occurred. Assumes that all nodes in the cluster are ready
// and in sync with instance groups.
func (o *Orchestrator) ScaleUp(
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
	allOrNothing bool, // Either request enough capacity for all unschedulablePods, or don't request it at all.
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	var err error
	if !o.initialized {
		return scaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerError(errors.InternalError, "provisioningrequest.Orchestrator is not initialized"))
	}

	// If Provisioning Request shard was not picked, fall-back to OSS logic.
	podShard := podsharding.GetSelectedPodShard(o.autoscalingContext)
	if podShard == nil || podShard.NodeGroupDescriptor.ProvisioningClassName != queuedwrapper.QueuedProvisioningClassName {
		return o.wrappedOrchestrator.ScaleUp(unschedulablePods, nodes, daemonSets, nodeInfos, allOrNothing)
	}
	isParallel := podShard.NodeGroupDescriptor.ProvisioningCapacitySearchStrategy == queuedwrapper.CapacitySearchStrategyObtainability

	nodeGroups := o.autoscalingContext.CloudProvider.NodeGroups()
	podEquivalenceGroups := equivalence.BuildPodGroups(unschedulablePods)
	prGroups := buildProvReqGroups(podEquivalenceGroups)
	if len(prGroups) == 0 {
		// This should not happen as it would mean there are no ProvReq objects
		// considered during this iteration but we rely on the fact that
		// prGroups is not empty later on, so we explicitly assert it here.
		return scaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerError(errors.InternalError, "no ProvReq groups to consider"))
	}

	if o.processors != nil && o.processors.NodeGroupListProcessor != nil {
		nodeGroups, nodeInfos, err = o.processors.NodeGroupListProcessor.Process(o.autoscalingContext, nodeGroups, nodeInfos, unschedulablePods)
		if err != nil {
			return scaleUpError(&status.ScaleUpStatus{}, errors.ToAutoscalerError(errors.InternalError, err))
		}
	}

	tracker, err := o.quotasTrackerFactory.NewQuotasTracker(o.autoscalingContext, nodes)
	if err != nil {
		return scaleUpError(&status.ScaleUpStatus{}, errors.ToAutoscalerError(errors.InternalError, err).AddPrefix("could not create quotas tracker: "))
	}

	now := o.now()

	// Filter out invalid node groups
	validNodeGroups, skippedNodeGroups := o.filterValidScaleUpNodeGroups(nodeGroups, nodeInfos, tracker, now)
	schedulablePods := map[string][]estimator.PodEquivalenceGroup{}
	for _, nodeGroup := range validNodeGroups {
		schedulablePods[nodeGroup.Id()] = o.wrappedOrchestrator.SchedulablePodGroups(podEquivalenceGroups, nodeGroup, nodeInfos[nodeGroup.Id()])
	}

	expansionOptions, compositeOptionsMap, skippedNodeGroups, aErr := o.computeExpansionOptions(validNodeGroups, skippedNodeGroups, nodes, nodeInfos, prGroups, tracker, now, isParallel)
	if aErr != nil {
		return scaleUpError(&status.ScaleUpStatus{}, aErr)
	}

	// Pick some expansion option.
	initialOption := o.autoscalingContext.ExpanderStrategy.BestOption(expansionOptions, nodeInfos)
	if initialOption == nil || initialOption.NodeCount <= 0 {
		// We only fail the first ProvReq here - empty best option means that
		// no NodeGroup scale up is possible to host the Pods corresponding to
		// the first ProvisioningRequest in the Pod shard.
		prNamespace := prGroups[0].ID.Namespace
		prName := prGroups[0].ID.Name
		klog.V(1).Infof("No expansion options for ProvisioningRequest %s/%s", prNamespace, prName)
		if err := o.failProvisioningRequest(prNamespace, prName, skippedNodeGroups, now); err != nil && err != provisioningRequestStillRetriableErr {
			return scaleUpError(&status.ScaleUpStatus{}, errors.ToAutoscalerError(errors.InternalError, err))
		}
		return &status.ScaleUpStatus{
			Result:                  status.ScaleUpNoOptionsAvailable,
			PodsRemainUnschedulable: o.getRemainingPods(podEquivalenceGroups, nodeGroups, reasons.TranslateKeysToNames(skippedNodeGroups), nodeInfos),
			ConsideredNodeGroups:    nodeGroups,
		}, nil
	}

	var createNodeGroupResults []nodegroups.CreateNodeGroupResult
	if !initialOption.NodeGroup.Exist() && !o.processors.AsyncNodeGroupStateChecker.IsUpcoming(initialOption.NodeGroup) {
		oldId := initialOption.NodeGroup.Id()
		var scaleUpStatus *status.ScaleUpStatus
		if o.autoscalingContext.AsyncNodeGroupsEnabled {
			initializer := NewAsyncDWSNodeGroupInitializer(
				nodeInfos[oldId],
				initialOption.Pods,
				o.scaleUpExecutor,
				o.taintConfig,
				daemonSets,
				o.processors.ScaleUpStatusProcessor,
				o.autoscalingContext,
				o.prCache,
			)
			createNodeGroupResults, scaleUpStatus, aErr = o.wrappedOrchestrator.CreateNodeGroupAsync(initialOption, nodeInfos, schedulablePods, podEquivalenceGroups, daemonSets, initializer)
		} else {
			createNodeGroupResults, scaleUpStatus, aErr = o.wrappedOrchestrator.CreateNodeGroup(initialOption, nodeInfos, schedulablePods, podEquivalenceGroups, daemonSets)
		}

		if aErr != nil {
			return scaleUpStatus, aErr
		}
		if len(createNodeGroupResults) < 1 {
			return scaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "Failed to find results of node group creation for node group id: %s", oldId))
		}
		createNodeGroupResult := createNodeGroupResults[0]
		newId := createNodeGroupResult.MainCreatedNodeGroup.Id()
		// The just-created node group might have a different ID. In that case, we update the entry.
		if oldId != newId {
			createdNodeGroups := []cloudprovider.NodeGroup{createNodeGroupResult.MainCreatedNodeGroup}
			createdNodeGroups = append(createdNodeGroups, createNodeGroupResult.ExtraCreatedNodeGroups...)

			// The node templates before and after NG creation might be slightly different, specifically wrt nodepool names and zone selectors, we need to recalculate the options
			expansionOptions, compositeOptionsMap, skippedNodeGroups, aErr = o.computeExpansionOptions(createdNodeGroups, skippedNodeGroups, nodes, nodeInfos, prGroups, tracker, now, isParallel)
			if aErr != nil {
				return scaleUpError(&status.ScaleUpStatus{}, aErr)
			}

			recomputedOption, found := compositeOptionsMap[newId]
			if !found {
				return scaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "ProvReqs became unschedulable after nodegroup creation: %s.", newId))
			}
			initialOption = &recomputedOption.Option
			klog.V(1).Infof("NAP for queued ProvReq: recomputed option from %s to %s", oldId, newId)
		}
	}
	klog.V(1).Infof("Best option: estimated %d nodes needed in %s", initialOption.NodeCount, initialOption.NodeGroup.Id())
	if len(initialOption.Debug) > 0 {
		klog.V(1).Info(initialOption.Debug)
	}

	initialCompositeOption := compositeOptionsMap[initialOption.NodeGroup.Id()]

	optionsToExecute := o.getOptionsToExecute(
		initialCompositeOption,
		compositeOptionsMap,
		nodeInfos,
		now,
		schedulablePods,
		prGroups,
		isParallel,
	)

	if isParallel {
		err := o.setParallelQueueingDetails(initialCompositeOption, optionsToExecute)
		if err != nil {
			return scaleUpError(&status.ScaleUpStatus{}, errors.ToAutoscalerError(errors.InternalError, err))
		}
	}

	var wg sync.WaitGroup
	scaleUpState := newScaleUpState()
	for optionIdx, optionToExecute := range optionsToExecute {
		wg.Add(1)
		go func(optionIdx int, optionToExecute *CompositeOption) {
			defer wg.Done()
			for poIdx, partialOption := range optionToExecute.partialOptions {
				shouldUpdateProvReqDetails := manager.UpdateProvReqDetails
				if isParallel {
					shouldUpdateProvReqDetails = manager.DoNotUpdateProvReqDetails
				}
				additionalInfo := fmt.Sprintf("Executed option: %d/%d. Partial option: %d/%d.", optionIdx+1, len(optionsToExecute), poIdx+1, len(optionToExecute.partialOptions))
				o.scaleUpExecutor.executeScaleUpForOption(optionToExecute, &partialOption, scaleUpState, nodeInfos, now, podEquivalenceGroups, additionalInfo, shouldUpdateProvReqDetails)
			}
		}(optionIdx, optionToExecute)
	}
	wg.Wait()
	o.clusterStateRegistry.Recalculate()

	if len(scaleUpState.autoscalerErrors) > 0 {
		klog.V(1).Infof("Got %d autoscaler errors during scale up.", len(scaleUpState.autoscalerErrors))
		return &status.ScaleUpStatus{
			Result:                 status.ScaleUpError,
			FailedResizeNodeGroups: scaleUpState.failedResizeNodeGroups,
			CreateNodeGroupResults: createNodeGroupResults,
		}, aggregateAutoscalerErrors(scaleUpState.autoscalerErrors)
	}
	return &status.ScaleUpStatus{
		Result:                  status.ScaleUpSuccessful,
		ScaleUpInfos:            scaleUpState.scaleUpInfos,
		PodsRemainUnschedulable: o.getRemainingPods(podEquivalenceGroups, nodeGroups, reasons.TranslateKeysToNames(skippedNodeGroups), nodeInfos),
		ConsideredNodeGroups:    nodeGroups,
		PodsAwaitEvaluation:     scaleUpState.podsAwaitEvaluation,
		CreateNodeGroupResults:  createNodeGroupResults,
	}, nil
}

// ScaleUpToNodeGroupMinSize calls wrapped ScaleUpToNodeGroupMinSize.
func (o *Orchestrator) ScaleUpToNodeGroupMinSize(
	nodes []*apiv1.Node,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	return o.wrappedOrchestrator.ScaleUpToNodeGroupMinSize(nodes, nodeInfos)
}

func (o *Orchestrator) intersectAllCompositeOptionsForSimilarNodeGroups(
	compositeOptions map[string]*CompositeOption,
	nodeInfos map[string]*framework.NodeInfo,
	now time.Time,
) (map[string]*CompositeOption, errors.AutoscalerError) {
	for _, option := range compositeOptions {
		compositeOption, aErr := o.intersectCompositeOptionsForSimilarNodeGroups(option, compositeOptions, nodeInfos, now)
		if aErr != nil {
			return nil, aErr
		}
		if compositeOption.NodeCount <= 0 {
			klog.V(1).Infof("%v filtered out to an empty scaleup after parallel intersection", compositeOption.NodeGroup.Id())
			delete(compositeOptions, compositeOption.NodeGroup.Id())
		} else {
			compositeOptions[compositeOption.NodeGroup.Id()] = compositeOption
		}
	}
	return compositeOptions, nil
}

// Note that the result should be the same here for all nodegroups in the same nodepool.
// Consider caching if this ever becomes a performance problem.
func (o *Orchestrator) intersectCompositeOptionsForSimilarNodeGroups(
	initialOption *CompositeOption,
	compositeOptions map[string]*CompositeOption,
	nodeInfos map[string]*framework.NodeInfo,
	now time.Time,
) (*CompositeOption, errors.AutoscalerError) {
	similarGroups, aErr := o.findSimilarNodeGroupsForParallelQueueing(initialOption.NodeGroup, nodeInfos, now)
	if aErr != nil {
		return nil, aErr
	}

	filteredOption := expander.Option{
		NodeGroup:         initialOption.NodeGroup,
		Pods:              make([]*apiv1.Pod, 0),
		SimilarNodeGroups: similarGroups,
	}
	provReqSet := optionToProvReqSet(initialOption)
	klog.V(1).Infof("Parallel queueing, intersecting options for %s, initial provreqs: %v", initialOption.NodeGroup.Id(), provReqSet)
	for _, ng := range similarGroups {
		similarGroupProvReqSet := optionToProvReqSet(compositeOptions[ng.Id()])
		klog.V(4).Infof("Parallel queueing, intersecting options for %s with options for %s: %v", initialOption.NodeGroup.Id(), ng.Id(), similarGroupProvReqSet)
		provReqSet = provReqSet.Intersection(similarGroupProvReqSet)
	}

	filteredPartialOptions := []PartialOption{}
	for _, partialOption := range initialOption.partialOptions {
		if provReqSet.Has(partialOption.ProvReqID) {
			filteredPartialOptions = append(filteredPartialOptions, partialOption)
			filteredOption.NodeCount += partialOption.NodeCount
			filteredOption.Pods = append(filteredOption.Pods, partialOption.Pods...)
		}
	}
	klog.V(1).Infof("Parallel queueing, intersected options for %s, final provreqs: %v", initialOption.NodeGroup.Id(), provReqSet)
	return &CompositeOption{Option: filteredOption, partialOptions: filteredPartialOptions}, nil
}

func (o *Orchestrator) findSimilarNodeGroupsForParallelQueueing(
	nodeGroup cloudprovider.NodeGroup,
	nodeInfos map[string]*framework.NodeInfo,
	now time.Time,
) ([]cloudprovider.NodeGroup, errors.AutoscalerError) {
	result := []cloudprovider.NodeGroup{}
	similarGroups, aErr := o.processors.NodeGroupSetProcessor.FindSimilarNodeGroups(o.autoscalingContext, nodeGroup, nodeInfos)
	if aErr != nil {
		klog.Errorf("Failed to find similar node groups: %v", aErr)
		return nil, aErr
	}

	klog.V(1).Infof("Parallel queueing in %s: considering %d similar nodeGroups", nodeGroup.Id(), len(similarGroups))
	for _, ng := range similarGroups {
		if ng.Exist() && !o.clusterStateRegistry.NodeGroupScaleUpSafety(ng, now).SafeToScale {
			klog.V(4).Infof("Skipping checking %v for parallelization, not ready for scaleup", ng.Id())
			continue
		}
		result = append(result, ng)
	}
	klog.V(1).Infof("Parallel queueing in %s: found %d similar nodeGroups", nodeGroup.Id(), len(result))

	return result, nil
}

// computeExpansionOptions computes list of possible scale up options in the cluster.
// It also returns a map of why particular nodegroup could not be extended.
func (o *Orchestrator) computeExpansionOptions(
	validNodeGroups []cloudprovider.NodeGroup,
	skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons,
	nodes []*apiv1.Node,
	nodeInfos map[string]*framework.NodeInfo,
	provReqGroups []*ProvReqGroup,
	tracker *resourcequotas.Tracker,
	now time.Time,
	isParallel bool,
) ([]expander.Option, map[string]*CompositeOption, map[cloudprovider.NodeGroup]status.Reasons, errors.AutoscalerError) {
	// Map from NodeGroup ID to composite options.
	compositeOptionsMap := map[string]*CompositeOption{}
	// Calculate expansion options
	var expansionOptions []expander.Option
	for _, nodeGroup := range validNodeGroups {
		// Populate required fields
		currentTargetSize, err := nodeGroup.TargetSize()
		if err != nil {
			klog.Errorf("Failed to get node group size: %v", err)
			skippedNodeGroups[nodeGroup] = orchestrator.NotReadyReason
			continue
		}
		nodeInfo, found := nodeInfos[nodeGroup.Id()]
		if !found {
			klog.Errorf("No node info for: %s", nodeGroup.Id())
			skippedNodeGroups[nodeGroup] = orchestrator.NotReadyReason
			continue
		}

		// Try to build a scale up option as it should be possible (at least partially).
		compositeOption, skipReason := o.computeExpansionOption(provReqGroups, nodeGroup, nodeInfo, currentTargetSize, now)
		if skipReason != nil {
			skippedNodeGroups[nodeGroup] = skipReason
			continue
		}
		if compositeOption == nil || len(compositeOption.Pods) <= 0 || compositeOption.NodeCount <= 0 {
			klog.V(4).Infof("No pod can fit to %s", nodeGroup.Id())
			continue
		}

		// Check if the option can be fulfilled fully.
		skipReason, aErr := o.isWithinResourceLimits(nodeInfos[nodeGroup.Id()], compositeOption.Option, tracker, currentTargetSize, len(nodes), checkOnly)
		if aErr != nil {
			return nil, nil, nil, aErr
		}
		if skipReason != nil {
			skippedNodeGroups[nodeGroup] = skipReason
			continue
		}

		expansionOptions = append(expansionOptions, compositeOption.Option)
		compositeOptionsMap[nodeGroup.Id()] = compositeOption
	}

	// If we need to parallalize the scaleup, we need to confirm that multiple copies of the same scaleup will be possible
	// within a nodepool. We do it by computing the intersection of ProvReqs that are schedulable in multiple parallel nodegroups,
	// and confirming that the multiplied scaleup will still fit within cluster-wide and nodepool wide limits.
	if isParallel {
		var aErr errors.AutoscalerError
		compositeOptionsMap, aErr = o.intersectAllCompositeOptionsForSimilarNodeGroups(compositeOptionsMap, nodeInfos, now)
		if aErr != nil {
			return nil, nil, nil, aErr
		}
		expansionOptions, compositeOptionsMap, skippedNodeGroups, aErr = o.filterParallelOptionsOutsideResourceLimits(skippedNodeGroups, compositeOptionsMap, nodes, nodeInfos)
		if aErr != nil {
			return nil, nil, nil, aErr
		}
	}

	return expansionOptions, compositeOptionsMap, skippedNodeGroups, nil
}

// filterValidScaleUpNodeGroups filters the node groups that are valid for scale-up
// TODO(b/282134696): Figure out a way to share more code with OSS.
func (o *Orchestrator) filterValidScaleUpNodeGroups(
	nodeGroups []cloudprovider.NodeGroup,
	nodeInfos map[string]*framework.NodeInfo,
	tracker *resourcequotas.Tracker,
	now time.Time,
) ([]cloudprovider.NodeGroup, map[cloudprovider.NodeGroup]status.Reasons) {
	var validNodeGroups []cloudprovider.NodeGroup
	skippedNodeGroups := map[cloudprovider.NodeGroup]status.Reasons{}

	for _, nodeGroup := range nodeGroups {
		if !reasons.IsNodeGroupQueued(nodeGroup) {
			skippedNodeGroups[nodeGroup] = reasons.NotQueuedNodeGroupSkippedReason
			continue
		}

		if skipReason := o.wrappedOrchestrator.IsNodeGroupReadyToScaleUp(nodeGroup, now); skipReason != nil {
			skippedNodeGroups[nodeGroup] = skipReason
			continue
		}

		currentTargetSize, err := nodeGroup.TargetSize()
		if err != nil {
			klog.Errorf("Failed to get node group size: %v", err)
			skippedNodeGroups[nodeGroup] = orchestrator.NotReadyReason
			continue
		}
		if currentTargetSize >= nodeGroup.MaxSize() {
			klog.V(4).Infof("Skipping node group %s - max size (%v) reached ", nodeGroup.Id(), nodeGroup.MaxSize())
			skippedNodeGroups[nodeGroup] = orchestrator.MaxLimitReachedReason
			continue
		}

		nodeInfo, found := nodeInfos[nodeGroup.Id()]
		if !found {
			klog.Errorf("No node info for: %s", nodeGroup.Id())
			skippedNodeGroups[nodeGroup] = orchestrator.NotReadyReason
			continue
		}

		if skipReason := o.wrappedOrchestrator.IsNodeGroupResourceExceeded(tracker, nodeGroup, nodeInfo, 1); skipReason != nil {
			skippedNodeGroups[nodeGroup] = skipReason
			continue
		}

		validNodeGroups = append(validNodeGroups, nodeGroup)
	}
	return validNodeGroups, skippedNodeGroups
}

// computeExpansionOption computes expansion option based on pending pods and cluster state.
func (o *Orchestrator) computeExpansionOption(
	provReqGroups []*ProvReqGroup,
	nodeGroup cloudprovider.NodeGroup,
	nodeInfo *framework.NodeInfo,
	currentTargetSize int,
	now time.Time,
) (*CompositeOption, status.Reasons) {
	option := expander.Option{
		NodeGroup: nodeGroup,
		Pods:      make([]*apiv1.Pod, 0),
	}
	partialOptions := make([]PartialOption, 0)

	// Limit the number of nodes we estimate to the max scale up possible.
	nodeEstimationCap := nodeGroup.MaxSize() - currentTargetSize
	if mig, ok := nodeGroup.(*gke.GkeMig); ok {
		nodeEstimationCap = min(nodeEstimationCap, mig.GetInstanceLimit()-currentTargetSize)
	}
	nodeEstimationCap = max(nodeEstimationCap, 0)

	processedPodsCount := 0
	timeout := o.maxProvReqBinpackingDuration
	startTs := o.now()
	isFirstProvReq := true
	for i, provReqGroup := range provReqGroups {
		// If ProvReq has multiple PodSets (i.e. Pods from multiple PodTemplates), we expect the ProvReq to get fully enqueued in a single node group.
		// Thus it's required that:
		// - all Pods from all PodSets will end up in a single pod shard
		// - all those pods in a single pod shard are able to get scheduled in a single node group.
		// Otherwise the ProvReq won't find any expansion options and fail after some time.

		schedulablePodGroups := o.wrappedOrchestrator.SchedulablePodGroups(provReqGroup.PodGroups, nodeGroup, nodeInfo)
		// Check whether all ProvReq's PodEquivalenceGroups fit in the same single node group
		if len(schedulablePodGroups) != len(provReqGroup.PodGroups) {
			if i == 0 {
				var err status.Reasons
				for _, pg := range provReqGroup.PodGroups {
					if err = pg.SchedulingErrors[nodeGroup.Id()]; err != nil {
						break
					}
				}
				if schedErr, ok := err.(clustersnapshot.SchedulingError); ok && schedErr.Type() == clustersnapshot.FailingPredicateError {
					return nil, reasons.NewCouldNotScheduleAnyPodsInNodePool(schedErr.Reasons())
				}
				return nil, reasons.NewCouldNotScheduleAnyPodsInNodePool([]string{"not schedulable"})
			}
			break
		}

		schedulablePodCount := totalPodCount(schedulablePodGroups)
		canSchedule, failReason := o.canScheduleWholeProvReqOnMig(provReqGroup, schedulablePodCount)
		if !canSchedule {
			if i == 0 {
				return nil, failReason
			}
			klog.Infof("Couldn't schedule whole ProvisioningRequest %v on node group %q, reason: %+v", provReqGroup.ID, nodeGroup.Id(), failReason)
			break
		}
		if !o.canScheduleProvReqOnMigBasedOnMRDValue(provReqGroup, nodeGroup) {
			if i == 0 {
				return nil, reasons.NewCouldNotScheduleAnyPodsInNodePool([]string{"unschedulable due to MaxRunDuration mismatch"})
			}
			break
		}
		thresholds := []estimator.Threshold{
			estimator.NewStaticThreshold(nodeEstimationCap, timeout),
			estimator.NewSngCapacityThreshold(),
			estimator.NewClusterCapacityThreshold(),
		}
		limiter := estimator.NewThresholdBasedEstimationLimiter(thresholds)
		estimator := estimator.NewBinpackingNodeEstimator(
			o.autoscalingContext.ClusterSnapshot,
			limiter, estimator.NewDecreasingPodOrderer(),
			estimator.NewEstimationContext(o.autoscalingContext.MaxNodesTotal, nil, currentTargetSize),
			o.napResourceAnalyzerFunc,
			o.fastpathBinpackingEnabled,
		)
		estimatedNodeCount, estimatedPods := estimator.Estimate(schedulablePodGroups, nodeInfo, nodeGroup)

		if o.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ProvisioningRequestsZeroOrMaxRoundUpEnabledFlag, false) {
			// Special handling for groups that only scale from zero to max.
			if mig, ok := nodeGroup.(*gke.GkeMig); ok {
				if opts, err := mig.GetOptions(o.autoscalingContext.NodeGroupDefaults); err == nil {
					if opts != nil && opts.ZeroOrMaxNodeScaling {
						// For zero-or-max scaling groups, the only valid value of node count is node group's max size.
						if estimatedNodeCount > nodeGroup.MaxSize() {
							// We would have to cap the node count, which means not all pods will be
							// accommodated. This violates the principle of all-or-nothing strategy.
							estimatedPods = nil
							estimatedNodeCount = 0
						}
						if estimatedNodeCount > 0 {
							// Cap or increase the number of nodes to the only valid value - node group's max size.
							estimatedNodeCount = nodeGroup.MaxSize()
						}
					}
				}
			}
		}

		if schedulablePodCount != len(estimatedPods) {
			pr := o.prCache.PendingProvReq(provReqGroup.ID.Namespace, provReqGroup.ID.Name)
			if pr == nil {
				klog.Errorf("Pending Provisioning Request %s/%s not found in the cache when calculating expansion option, skipping", provReqGroup.ID.Namespace, provReqGroup.ID.Name)
				continue
			}
			if isRetriable(pr, now) {
				continue
			}
			if isFirstProvReq {
				if estimatedNodeCount >= nodeEstimationCap {
					return nil, orchestrator.MaxLimitReachedReason
				}
				return nil, reasons.CouldNotScheduleAllPodsInSingleZone
			}
			break
		}

		option.NodeCount += estimatedNodeCount
		option.Pods = append(option.Pods, estimatedPods...)
		partialOptions = append(partialOptions, PartialOption{
			NodeCount: estimatedNodeCount,
			ProvReqID: provReqGroup.ID,
			Pods:      estimatedPods,
		})
		nodeEstimationCap -= estimatedNodeCount
		processedPodsCount += len(estimatedPods)
		currentTargetSize += estimatedNodeCount
		isFirstProvReq = false
		// Stop if either:
		// 1. NodeGroup is already saturated,
		// 2. Cap for the number of handled ProvReqs is achieved,
		// 3. Cap for the number of Pods is achieved,
		// 4. We run computations for too long already (i.e. we hit soft timeout).
		if nodeEstimationCap <= 0 || i+1 >= provReqInExpansionOptionCountCap || processedPodsCount >= expansionOptionPodsCountThreshold {
			break
		}
		timeout = o.maxProvReqBinpackingDuration - (o.now().Sub(startTs))
		if timeout <= 0 {
			klog.Infof("Hit timeout when computing expansion options for NodeGroup %s (handled %d ProvReqs)", nodeGroup.Id(), i+1)
			break
		}
	}
	return &CompositeOption{
		Option:         option,
		partialOptions: partialOptions,
	}, nil
}

func (o *Orchestrator) canScheduleWholeProvReqOnMig(provReqGroup *ProvReqGroup, schedulablePodCount int) (bool, status.Reasons) {
	// podCount denotes `how many Pods from all PodSets in this ProvReq are there overall?`
	podCount, err := o.provReqPodCount(provReqGroup.ID)
	if err != nil {
		klog.Errorf("Couldn't get expected Pods count for Provisioning Request %s/%s: %v, skipping", provReqGroup.ID.Namespace, provReqGroup.ID.Name, err)
		return false, reasons.PodCountNotFoundReason
	}
	// Check whether the Pods that managed to get scheduled in the same single node group were actually **all ProvReq's Pods** from all PodSets
	// or whether some of the Pods didn't even make it to this PodShard
	if schedulablePodCount != podCount {
		return false, reasons.NewCouldNotScheduleAnyPodsInNodePool(
			[]string{fmt.Sprintf("only %d/%d Pods from all PodSets were schedulable" /* other Pods weren't even present in this PodShard */, schedulablePodCount, podCount)})
	}
	return true, nil
}

func (o *Orchestrator) provReqPodCount(provReqID prpods.ProvReqID) (int, error) {
	prNamespace, prName := provReqID.Namespace, provReqID.Name
	pr := o.prCache.PendingProvReq(prNamespace, prName)
	if pr == nil {
		return 0, fmt.Errorf("Provisioning Request %s/%s was not found in cache", prNamespace, prName)
	}

	expectedTotalProvReqPods := 0
	podSets, err := pr.PodSets()
	if err != nil {
		return 0, fmt.Errorf("couldn't retrieve PodSets of Provisioning Request %s/%s: %v", prNamespace, prName, err)
	}
	for _, podSet := range podSets {
		expectedTotalProvReqPods += int(podSet.Count)
	}
	return expectedTotalProvReqPods, nil
}

/*
 * canScheduleProvReqOnMigBasedOnMRDValue
 *
 * Returns true if ProvReq's MRD is compatible with Mig's.
 * - runs only for MIGs with BulkProvisioning enabled
 * - blocks scheduling if ProvReq has no MRD specified
 * - currently behind ProvisioningRequestBulkMigsFlag flag
 */
func (o *Orchestrator) canScheduleProvReqOnMigBasedOnMRDValue(provReqGroup *ProvReqGroup, nodeGroup cloudprovider.NodeGroup) bool {

	if !o.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ProvisioningRequestBulkMigsFlag, false) {
		klog.V(4).Infof("ProvisioningRequestBulkMigsFlag inactive, not running MaxRunDuration checks")
		return true
	}

	mig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return true
	}
	if !mig.UsesBulkProvisioning() {
		klog.V(4).Infof("Mig's %s UsesBulkProvisioning() == false, not running MaxRunDuration checks for %s/%s",
			mig.NodePoolName(),
			provReqGroup.ID.Namespace,
			provReqGroup.ID.Name)
		return true
	}

	samplePod := provReqGroup.PodGroups[0].Pods[0]
	pr := o.prCache.PendingProvReq(provReqGroup.ID.Namespace, provReqGroup.ID.Name)
	if pr == nil {
		klog.Warningf("Pending Provisioning Request %s/%s not found in the cache when calculating expansion option, not scheduling %s/%s on %s",
			provReqGroup.ID.Namespace,
			provReqGroup.ID.Name,
			samplePod.Namespace,
			samplePod.Name,
			nodeGroup.Id())
		return false
	}

	// For MIG we do not default to 7 days. This is in contrast to requests we send for flex non-queued MIGs where we >do< default to 7 days
	migMaxDuration := mig.Spec().MaxRunDurationInSeconds
	migMaxDurationParsed, err := queuedwrapper.MaxRunDurationFromString(migMaxDuration)
	if err != nil {
		klog.Warningf("Invalid MaxRunDuration %v on mig %s: %v, not scheduling %s/%s on %s",
			migMaxDuration,
			mig.NodePoolName(), err,
			samplePod.Namespace,
			samplePod.Name,
			nodeGroup.Id())
		// Unreadable MaxRunDuration, not scheduling the pods
		return false
	}

	// For ProvReq we default to 7 days if MRD is not specified
	prMaxRunDurationParsed, err := queuedwrapper.ToQueuedProvisioningRequest(*pr).MaxRunDurationOrDefaultWithWarning()
	if err != nil {
		klog.Warningf("Invalid MaxRunDuration %v on ProvisiongRequest %s/%s: %v, not scheduling %s/%s on %s",
			string(queuedwrapper.ToQueuedProvisioningRequest(*pr).Spec.Parameters[queuedwrapper.MaxRunDurationSecondsKey]),
			pr.Namespace, pr.Name, err,
			samplePod.Namespace,
			samplePod.Name,
			nodeGroup.Id())
		// Unreadable MaxRunDuration, not scheduling the pods
		return false
	}

	// Check PR's MRD is compatible with our pool
	if *prMaxRunDurationParsed > *migMaxDurationParsed {
		klog.V(2).Infof("Mig's %s MaxRunDuration is incompatible with ProvisioningRequest's MaxRunDuration, not scheduling %s/%s on %s",
			mig.NodePoolName(),
			samplePod.Namespace,
			samplePod.Name,
			nodeGroup.Id())
		return false
	}
	return true
}

func (o *Orchestrator) getOptionsToExecute(
	initialCompositeOption *CompositeOption,
	compositeOptionsMap map[string]*CompositeOption,
	nodeInfos map[string]*framework.NodeInfo,
	now time.Time,
	schedulablePods map[string][]estimator.PodEquivalenceGroup,
	prGroups []*ProvReqGroup,
	isParallel bool,
) []*CompositeOption {
	if isParallel {
		return o.getParallelizedOptions(initialCompositeOption, compositeOptionsMap)
	} else {
		return o.getDistributedOptions(initialCompositeOption, compositeOptionsMap, nodeInfos, now, schedulablePods, prGroups)
	}
}

func (o *Orchestrator) getParallelizedOptions(
	initialCompositeOption *CompositeOption,
	compositeOptionsMap map[string]*CompositeOption,
) []*CompositeOption {
	parallelizedOptions := []*CompositeOption{initialCompositeOption}
	for _, ng := range initialCompositeOption.SimilarNodeGroups {
		compositeOption := compositeOptionsMap[ng.Id()]
		parallelizedOptions = append(parallelizedOptions, compositeOption)
	}

	return parallelizedOptions
}

func (o *Orchestrator) getDistributedOptions(
	initialCompositeOption *CompositeOption,
	compositeOptionsMap map[string]*CompositeOption,
	nodeInfos map[string]*framework.NodeInfo,
	now time.Time,
	schedulablePods map[string][]estimator.PodEquivalenceGroup,
	prGroups []*ProvReqGroup,
) []*CompositeOption {
	distributedOptions := []*CompositeOption{initialCompositeOption}
	initialMig, ok := initialCompositeOption.NodeGroup.(*gke.GkeMig)
	if !ok {
		klog.Errorf("non-GkeMig node group detected: %s", initialCompositeOption.NodeGroup.Id())
		return distributedOptions
	}
	if initialMig.IsTpuMig() && o.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.RecommendLocationsDisabledForTPUFlag, false) {
		klog.V(1).Infof("Skipping RLA for TPU MIG: %s for ProvisinoingRequest %s/%s and %d others", initialMig.Id(), prGroups[0].ID.Namespace, prGroups[0].ID.Name, len(prGroups)-1)
		return distributedOptions
	}
	if o.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ProvisioningRequestsRLAEnabledFlag, false) {
		adjustedOption, err := o.reassignOptionToRecommendedLocation(initialCompositeOption, compositeOptionsMap, schedulablePods, nodeInfos, now)
		if err != nil {
			klog.Errorf("Failed to determine optimal zone for ProvisioningRequest %s/%s and %d others, falling back to the initially selected zone, received error: %s", prGroups[0].ID.Namespace, prGroups[0].ID.Name, len(prGroups)-1, err)
		} else {
			klog.V(1).Infof("RLA adjusted option: estimated %d nodes needed in %s.", adjustedOption.NodeCount, adjustedOption.NodeGroup.Id())
			distributedOptions = []*CompositeOption{adjustedOption}
		}
		return distributedOptions
	}
	return distributedOptions
}

func (o *Orchestrator) reassignOptionToRecommendedLocation(
	option *CompositeOption,
	compositeOptions map[string]*CompositeOption,
	schedulablePods map[string][]estimator.PodEquivalenceGroup,
	nodeInfos map[string]*framework.NodeInfo,
	now time.Time,
) (*CompositeOption, error) {
	// Recompute similar node groups in case they need to be updated
	option.SimilarNodeGroups = o.wrappedOrchestrator.ComputeSimilarNodeGroups(option.NodeGroup, nodeInfos, schedulablePods, now)
	klog.V(2).Infof("Found %d similar node groups", len(option.SimilarNodeGroups))
	if !o.autoscalingContext.BalanceSimilarNodeGroups || len(option.SimilarNodeGroups) < 1 {
		return option, nil
	}

	selectedMig, ok := option.NodeGroup.(*gke.GkeMig)
	if !ok {
		return nil, fmt.Errorf("non-GkeMig node group detected: %s", option.NodeGroup.Id())
	}

	targetMigs := []*gke.GkeMig{selectedMig}
	// Each mig has an equivalent instance template, so we pick the first one.
	for _, ng := range option.SimilarNodeGroups {
		targetSize, err := ng.TargetSize()
		if err != nil {
			return nil, fmt.Errorf("couldn't obtain target size for node group %s: %s", ng.Id(), err)
		}
		ngRoomLeft := ng.MaxSize() - targetSize
		if _, ok := compositeOptions[ng.Id()]; !ok {
			klog.V(4).Infof("Ignoring Mig %s for resize request of size at least %v, it can fit only %v nodes", ng.Id(), option.NodeCount, ngRoomLeft)
		} else {
			mig, ok := ng.(*gke.GkeMig)
			if !ok {
				return nil, fmt.Errorf("non-GkeMig node group detected: %s", ng.Id())
			}
			targetMigs = append(targetMigs, mig)
		}
	}
	if len(targetMigs) < 2 {
		klog.V(1).Infof("None of similar node groups could be considered for scale-up, not calling RLA")
		return option, nil
	}
	klog.V(1).Infof("Looking for zone recommendation between %v similar node groups: {%v}", len(targetMigs), aggregateMigIds(targetMigs, similarGroupsLogLimit))
	maxRunDuration := o.highestMaxRunDuration(option)
	recZone, err := locationpolicy.RecommendZoneForQueueing(o.provider, maxRunDuration, option.NodeCount, selectedMig, targetMigs)
	if err != nil {
		return nil, fmt.Errorf("couldn't obtain zone recommendation: %s", err)
	}
	klog.Infof("Zone %s recommended for scale-up", recZone)
	for _, mig := range targetMigs {
		if mig.GceRef().Zone == recZone {
			return compositeOptions[mig.Id()], nil
		}
	}
	return nil, fmt.Errorf("RLA recommended zone %s where there are no similar node groups", recZone)
}

func (o *Orchestrator) filterParallelOptionsOutsideResourceLimits(
	skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons,
	compositeOptionsMap map[string]*CompositeOption,
	nodes []*apiv1.Node,
	nodeInfos map[string]*framework.NodeInfo,
) ([]expander.Option, map[string]*CompositeOption, map[cloudprovider.NodeGroup]status.Reasons, errors.AutoscalerError) {
	expansionOptions := []expander.Option{}
	filteredCompositeOptionsMap := make(map[string]*CompositeOption)
	for key, option := range compositeOptionsMap {
		reason, aErr := o.isParallelScaleupWithinResourceLimits(option, nodes, nodeInfos, compositeOptionsMap)
		if aErr != nil {
			klog.Errorf("Could not check resource limits for %v: %v", option.NodeGroup.Id(), aErr)
			return nil, nil, nil, aErr
		}
		if reason != nil {
			skippedNodeGroups[option.NodeGroup] = reasons.NewCouldNotParallelizeScaleup(reason)
			continue
		}
		expansionOptions = append(expansionOptions, option.Option)
		filteredCompositeOptionsMap[key] = option
	}
	return expansionOptions, filteredCompositeOptionsMap, skippedNodeGroups, nil
}

// isParallelScaleupWithinLimits checks if a parallelized composite options are within cluster-wide limits.
func (o *Orchestrator) isParallelScaleupWithinResourceLimits(
	initialCompositeOption *CompositeOption,
	nodes []*apiv1.Node,
	nodeInfos map[string]*framework.NodeInfo,
	compositeOptionsMap map[string]*CompositeOption,
) (status.Reasons, errors.AutoscalerError) {
	parallelizedOptions := o.getParallelizedOptions(initialCompositeOption, compositeOptionsMap)
	totalCount := optionsTotalCount(parallelizedOptions)
	klog.V(1).Infof("Checking limits for parallel scaleup in %s, total count: %d", initialCompositeOption.NodeGroup.Id(), totalCount)

	// Check total cluster size limits
	cappedNodeCount, aErr := o.wrappedOrchestrator.GetCappedNewNodeCount(totalCount, len(nodes))
	if aErr != nil {
		return nil, aErr
	}
	if totalCount != cappedNodeCount {
		klog.V(4).Infof("%s excluded due to total cluster size limit", initialCompositeOption.NodeGroup.Id())
		return reasons.ClusterSizeReachedSkippedReason, nil
	}

	// Check nodepool size limits
	mig, ok := initialCompositeOption.NodeGroup.(*gke.GkeMig)
	if !ok {
		return nil, errors.NewAutoscalerErrorf(errors.InternalError, "non-GkeMig node group detected: %v", initialCompositeOption.NodeGroup.Id())
	}
	npTargetSize, err := mig.NodePoolTargetSize()
	if err != nil {
		return nil, errors.ToAutoscalerError(errors.InternalError, err).AddPrefix("failed to get nodepool target size: ")
	}
	if mig.TotalSizeLimitEnabled() && npTargetSize+totalCount > mig.TotalMaxSize() {
		klog.V(4).Infof("%s excluded due to total nodepool size limit", initialCompositeOption.NodeGroup.Id())
		return orchestrator.MaxLimitReachedReason, nil
	}

	// Check per-mig limits and total resources
	tracker, err := o.quotasTrackerFactory.NewQuotasTracker(o.autoscalingContext, nodes)
	if err != nil {
		return nil, errors.ToAutoscalerError(errors.InternalError, err).AddPrefix("could not create quotas tracker: ")
	}
	for _, option := range parallelizedOptions {
		currentTargetSize, err := option.Option.NodeGroup.TargetSize()
		if err != nil {
			return nil, errors.ToAutoscalerError(errors.InternalError, err).AddPrefix("failed to get group target size: ")
		}
		reason, aErr := o.isWithinResourceLimits(nodeInfos[option.NodeGroup.Id()], option.Option, tracker, currentTargetSize, len(nodes), checkAndApply)
		if reason != nil || aErr != nil {
			klog.V(4).Infof("%s excluded due to resource limits", initialCompositeOption.NodeGroup.Id())
			return reason, aErr
		}
	}
	return nil, nil
}

// isWithinResourceLimits checks if the scale up option will be within cluster wide limits.
func (o *Orchestrator) isWithinResourceLimits(
	nodeInfo *framework.NodeInfo,
	option expander.Option,
	tracker *resourcequotas.Tracker,
	currentTargetSize int,
	allNodesCount int,
	applyDelta applyDelta,
) (status.Reasons, errors.AutoscalerError) {
	// Check cluster wide node count limit.
	cappedNodeCount, aErr := o.wrappedOrchestrator.GetCappedNewNodeCount(option.NodeCount, allNodesCount)
	if aErr != nil {
		return nil, aErr
	}
	if option.NodeCount != cappedNodeCount {
		return reasons.ClusterSizeReachedSkippedReason, nil
	}

	// Check if a max size will not be breached.
	if currentTargetSize+option.NodeCount > option.NodeGroup.MaxSize() {
		return orchestrator.MaxLimitReachedReason, nil
	}

	// Check if scale up fits fully within resource limits.
	checkResult, err := tracker.CheckDelta(o.autoscalingContext, option.NodeGroup, nodeInfo.Node(), option.NodeCount)
	if err != nil {
		return nil, errors.ToAutoscalerError(errors.InternalError, err).AddPrefix("failed to check resource quotas: ")
	}
	if checkResult.Exceeded() {
		return orchestrator.NewMaxResourceLimitReached(checkResult.ExceededQuotas), nil
	}
	if applyDelta {
		tracker.ApplyDelta(o.autoscalingContext, option.NodeGroup, nodeInfo.Node(), option.NodeCount)
	}
	return nil, nil
}

func (o *Orchestrator) highestMaxRunDuration(option *CompositeOption) *time.Duration {
	setHighest := func(highest **time.Duration, current *time.Duration) {
		if *highest == nil || *current > **highest {
			*highest = current
		}
	}

	var highestMaxRunDuration *time.Duration
	for _, po := range option.partialOptions {
		pr := o.prCache.PendingProvReq(po.ProvReqID.Namespace, po.ProvReqID.Name)
		if pr == nil {
			klog.Errorf("Failed to find Provisioning Request %s/%s when searching for MaxRunDuration. Defaulting to: %s.", po.ProvReqID.Namespace, po.ProvReqID.Name, queuedwrapper.DefaultMaxRunDuration)
			setHighest(&highestMaxRunDuration, &queuedwrapper.DefaultMaxRunDuration)
			continue
		}
		qpr := queuedwrapper.ToQueuedProvisioningRequest(*pr)
		duration, err := qpr.MaxRunDuration()
		if err != nil {
			klog.Errorf("Error when reading MaxRunDuration from Provisioning Request %s/%s. Defaulting to: %s. Error: %v", po.ProvReqID.Namespace, po.ProvReqID.Name, queuedwrapper.DefaultMaxRunDuration, err)
			setHighest(&highestMaxRunDuration, &queuedwrapper.DefaultMaxRunDuration)
			continue
		}
		if duration == nil {
			klog.Infof("Failed to find MaxRunDuration for Provisioning Request %s/%s. Defaulting to: %s.", po.ProvReqID.Namespace, po.ProvReqID.Name, queuedwrapper.DefaultMaxRunDuration)
			setHighest(&highestMaxRunDuration, &queuedwrapper.DefaultMaxRunDuration)
			continue
		}

		setHighest(&highestMaxRunDuration, duration)
	}
	return highestMaxRunDuration
}

func (o *Orchestrator) setParallelQueueingDetails(initialOption *CompositeOption, parallelOptions []*CompositeOption) error {
	migs := []*gke.GkeMig{}
	for _, option := range parallelOptions {
		mig, ok := option.NodeGroup.(*gke.GkeMig)
		if !ok {
			return fmt.Errorf("Non-mig nodegroup in parallel options: %s", option.NodeGroup.Id())
		}
		migs = append(migs, mig)
	}
	for _, pr := range initialOption.partialOptions {
		if err := o.setParallelQueueingDetailsForPR(pr.ProvReqID.Namespace, pr.ProvReqID.Name, migs); err != nil {
			return err
		}
	}
	return nil
}

func (o *Orchestrator) setParallelQueueingDetailsForPR(prNamespace, prName string, migs []*gke.GkeMig) error {
	pr := o.prCache.PendingProvReq(prNamespace, prName)
	if pr == nil {
		return fmt.Errorf("Provisioning Request %s/%s not found", prNamespace, prName)
	}
	if len(migs) < 1 {
		return fmt.Errorf("Trying to commit to zero migs")
	}
	mig := migs[0]

	details := &queuedwrapper.ProvisioningClassDetails{
		NodePoolName:            mig.NodePoolName(),
		AcceleratorType:         mig.Accelerators(),
		NodePoolAutoProvisioned: queuedwrapper.AutoprovisionedStatusFromBool(mig.Autoprovisioned()),
		PodTemplateName:         manager.PodTemplateNames(pr.PodTemplates),
		ProvisioningMode:        queuedwrapper.ProvisioningModeResizeRequest,
		ResizeRequestName:       resizerequestclient.ResizeRequestName(pr.Namespace, pr.Name),

		CommittedNodeGroups: committedNodeGroups(migs),
		CommittedZones:      committedZones(migs),
	}

	err := provreqstate.SetProvisioningClassDetails(pr, details)
	if err != nil {
		return fmt.Errorf("couldn't update Provisioning Request %s/%s with parallel details: %w", prNamespace, prName, err)
	}

	if _, err := o.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
		return fmt.Errorf("while updating Provisioning Request %s/%s got error: %w", prNamespace, prName, err)
	}

	return nil
}

// failProvisioningRequest marks the Provisioning Request as failed and sets appropriate reason and message.
func (o *Orchestrator) failProvisioningRequest(prNamespace, prName string, skippedNodeGroups map[cloudprovider.NodeGroup]status.Reasons, now time.Time) error {
	pr := o.prCache.PendingProvReq(prNamespace, prName)
	if pr == nil {
		return fmt.Errorf("Provisioning Request %s/%s not found", prNamespace, prName)
	}

	var baseError error
	reason, message := reasons.GetReasonAndMessage(skippedNodeGroups)
	if isRetriable(pr, now) {
		if changed := provreqstate.UpdateOrSetProvisioningRequestCondition(pr, provreqv1.Accepted, v1.ConditionFalse, reason, message, v1.NewTime(now)); !changed {
			return provisioningRequestStillRetriableErr
		}
		klog.Warningf("Recently created pending Provisioning Request %s/%s couldn't get Accepted, because %q: %s", prNamespace, prName, reason, message)
		baseError = provisioningRequestStillRetriableErr
	} else {
		err := provreqstate.SetStateCustomReasonMessage(pr, provreqstate.FailedState, reason, message, v1.NewTime(now))
		if err != nil {
			return fmt.Errorf("while setting Provisioning Request %s/%s got error: %w", prNamespace, prName, err)
		}
	}

	if _, err := o.prClient.UpdateProvisioningRequest(pr.ProvisioningRequest); err != nil {
		return fmt.Errorf("while updating Provisioning Request %s/%s got error: %w", prNamespace, prName, err)
	}
	return baseError
}

// If Provisioning Request was created less than pendingProvisioningRequestRetryPeriod ago, it won't be marked as failed yet and it can be retried in the next CA loop instead.
func isRetriable(pr *provreqwrapper.ProvisioningRequest, now time.Time) bool {
	return now.Before(pr.CreationTimestamp.Add(pendingProvisioningRequestRetryPeriod))
}

func (o *Orchestrator) getRemainingPods(egs []*equivalence.PodGroup, nodeGroups []cloudprovider.NodeGroup, skipped map[string]status.Reasons, nodeInfos map[string]*framework.NodeInfo) []status.NoScaleUpInfo {
	infos := o.wrappedOrchestrator.GetRemainingPods(egs, nodeGroups, skipped, nodeInfos)
	filtered := make([]status.NoScaleUpInfo, 0, len(infos))
	for _, info := range infos {
		_, consumingProvReq := pods.ProvisioningRequestName(info.Pod)
		_, isInjected := pods.InjectedPodProvReqRef(info.Pod)
		if !consumingProvReq || isInjected {
			filtered = append(filtered, info)
		}
	}
	return filtered
}

func scaleUpError(s *status.ScaleUpStatus, err errors.AutoscalerError) (*status.ScaleUpStatus, errors.AutoscalerError) {
	s.ScaleUpError = &err
	s.Result = status.ScaleUpError
	return s, err
}

func totalPodCount(groups []estimator.PodEquivalenceGroup) int {
	podCount := 0
	for _, group := range groups {
		podCount += len(group.Pods)
	}

	return podCount
}
