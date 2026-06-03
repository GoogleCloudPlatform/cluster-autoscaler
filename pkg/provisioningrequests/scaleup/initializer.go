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
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/manager"
	prpods "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/klog/v2"
)

// AsyncDWSNodeGroupInitializer is a component of the Orchestrator responsible for initial
// scale up of asynchronously created node groups.
type AsyncDWSNodeGroupInitializer struct {
	// guards allTargetSizes and provisioningRequests
	mutex                  sync.Mutex
	allTargetSizes         map[string]int64
	provisioningRequests   map[string][]provReqScaleUp
	nodeInfo               *framework.NodeInfo
	triggeringPods         []*apiv1.Pod
	scaleUpExecutor        *scaleUpExecutor
	taintConfig            taints.TaintConfig
	daemonSets             []*appsv1.DaemonSet
	scaleUpStatusProcessor status.ScaleUpStatusProcessor
	context                *context.AutoscalingContext
	prCache                *provreqcache.QueuedProvisioningCache
}

type provReqScaleUp struct {
	pr                         prpods.ProvReqID
	resize                     int64
	shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails
}

type ngScaleUp struct {
	prScaleUp   provReqScaleUp
	scaleUpInfo nodegroupset.ScaleUpInfo
}

// NewAsyncDWSNodeGroupInitializer creates a new instance of AsyncDWSNodeGroupInitializer.
func NewAsyncDWSNodeGroupInitializer(
	nodeInfo *framework.NodeInfo,
	triggeringPods []*apiv1.Pod,
	scaleUpExecutor *scaleUpExecutor,
	taintConfig taints.TaintConfig,
	daemonSets []*appsv1.DaemonSet,
	scaleUpStatusProcessor status.ScaleUpStatusProcessor,
	autoscalingContext *context.AutoscalingContext,
	prCache *provreqcache.QueuedProvisioningCache,
) *AsyncDWSNodeGroupInitializer {
	return &AsyncDWSNodeGroupInitializer{
		allTargetSizes:         map[string]int64{},
		provisioningRequests:   map[string][]provReqScaleUp{},
		nodeInfo:               nodeInfo,
		triggeringPods:         triggeringPods,
		scaleUpExecutor:        scaleUpExecutor,
		taintConfig:            taintConfig,
		daemonSets:             daemonSets,
		scaleUpStatusProcessor: scaleUpStatusProcessor,
		context:                autoscalingContext,
		prCache:                prCache,
	}
}

// GetTargetSize returns a target size of an upcoming node group.
func (s *AsyncDWSNodeGroupInitializer) GetTargetSize(nodeGroup string) int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.allTargetSizes[nodeGroup]
}

// SetTargetSize sets a target size of an upcoming node group.
func (s *AsyncDWSNodeGroupInitializer) SetTargetSize(nodeGroup string, size int64) {
	klog.Fatalf("SetTargetSize called on upcoming DWS node pool, this should never happen!")
}

// ChangeTargetSize changes by delta a target size of an upcoming node group.
func (s *AsyncDWSNodeGroupInitializer) ChangeTargetSize(nodeGroup string, delta int64) {
	klog.Fatalf("ChangeTargetSize called on upcoming DWS node pool, this should never happen!")
}

func (s *AsyncDWSNodeGroupInitializer) ScheduleProvReqResize(pr prpods.ProvReqID, ng string, delta int64, shouldUpdateProvReqDetails manager.ShouldUpdateProvReqDetails) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.allTargetSizes[ng] += delta
	s.provisioningRequests[ng] = append(s.provisioningRequests[ng], provReqScaleUp{pr, delta, shouldUpdateProvReqDetails})
	s.prCache.RegisterUpcomingProvReq(pr)
}

func (s *AsyncDWSNodeGroupInitializer) InitializeNodeGroup(result nodegroups.AsyncNodeGroupCreationResult) {
	if result.Error != nil {
		klog.Errorf("Async node group creation failed. Async scale-up is cancelled. %v", result.Error)
		s.emitScaleUpStatus(&status.ScaleUpStatus{}, errors.ToAutoscalerError(errors.InternalError, result.Error))
		return
	}
	mainCreatedNodeGroup := result.CreationResult.MainCreatedNodeGroup
	// If possible replace candidate node-info with node info based on created node group. The latter
	// one should be more in line with nodes which will be created by node group.
	nodeInfo, aErr := simulator.SanitizedTemplateNodeInfoFromNodeGroup(mainCreatedNodeGroup, s.daemonSets, s.taintConfig)
	if aErr != nil {
		klog.Warningf("Cannot build node info for newly created main node group %s. Using fallback. Error: %v", mainCreatedNodeGroup.Id(), aErr)
		nodeInfo = s.nodeInfo
	}

	nodeInfos := make(map[string]*framework.NodeInfo)
	var scaleUpInfos []nodegroupset.ScaleUpInfo
	var perGroupPlan = map[string][]ngScaleUp{}
	for _, nodeGroup := range result.CreationResult.AllCreatedNodeGroups() {
		upcomingId, ok := result.CreatedToUpcomingMapping[nodeGroup.Id()]
		if !ok {
			klog.Errorf("Couldn't retrieve initialization data for new node group %v. It won't get initialized. Available created to upcoming node group mapping: %v", nodeGroup.Id(), result.CreatedToUpcomingMapping)
			continue
		}
		klog.Infof("Mapping upcoming node group %v to the actually created one %v", upcomingId, nodeGroup.Id())
		var currentTargetSize = 0
		nodeInfos[nodeGroup.Id()] = nodeInfo
		for _, plannedPR := range s.provisioningRequests[upcomingId] {
			scaleUp := ngScaleUp{
				prScaleUp: plannedPR,
				scaleUpInfo: nodegroupset.ScaleUpInfo{
					Group:       nodeGroup,
					CurrentSize: currentTargetSize,
					NewSize:     currentTargetSize + int(plannedPR.resize),
					MaxSize:     nodeGroup.MaxSize(),
				},
			}
			perGroupPlan[nodeGroup.Id()] = append(perGroupPlan[nodeGroup.Id()], scaleUp)
			scaleUpInfos = append(scaleUpInfos, scaleUp.scaleUpInfo)
			currentTargetSize += int(plannedPR.resize)
		}
		wantTargetSize := int(s.GetTargetSize(upcomingId))
		if currentTargetSize != wantTargetSize {
			klog.Errorf("Error initializing DWS node group %v: want target size %d, but the ProvisioningRequests sum up to %d", nodeGroup.Id(), wantTargetSize, currentTargetSize)
		}
	}
	klog.Infof("[DWS] Starting initial scale-up for async created node groups. Scale ups: %v", scaleUpInfos)
	err, failedNodeGroups := s.executeInitializationPlans(perGroupPlan, nodeInfo, time.Now())
	if err != nil {
		var failedNodeGroupIds []string
		for _, failedNodeGroup := range failedNodeGroups {
			failedNodeGroupIds = append(failedNodeGroupIds, failedNodeGroup.Id())
		}
		klog.Errorf("Async scale-up for asynchronously created node group failed: %v (node groups: %v)", err, failedNodeGroupIds)
		s.emitScaleUpStatus(&status.ScaleUpStatus{
			CreateNodeGroupResults: []nodegroups.CreateNodeGroupResult{result.CreationResult},
			FailedResizeNodeGroups: failedNodeGroups,
			PodsTriggeredScaleUp:   s.triggeringPods,
		}, err)
		return
	}
	klog.Infof("Initial scale-up succeeded. Scale ups: %v", scaleUpInfos)
	scaleUpStatus := &status.ScaleUpStatus{
		Result:                 status.ScaleUpSuccessful,
		ScaleUpInfos:           scaleUpInfos,
		CreateNodeGroupResults: []nodegroups.CreateNodeGroupResult{result.CreationResult},
		PodsTriggeredScaleUp:   s.triggeringPods,
	}
	s.scaleUpStatusProcessor.Process(s.context, scaleUpStatus)
}

func (s *AsyncDWSNodeGroupInitializer) executeInitializationPlans(plans map[string][]ngScaleUp, nodeInfo *framework.NodeInfo, now time.Time) (errors.AutoscalerError, []cloudprovider.NodeGroup) {
	type errResult struct {
		err  errors.AutoscalerError
		info *nodegroupset.ScaleUpInfo
	}
	// Calculate the exact maximum number of possible errors
	maxPossibleErrors := 0
	for _, plan := range plans {
		maxPossibleErrors += len(plan)
	}
	errResults := make(chan errResult, maxPossibleErrors)
	var wg sync.WaitGroup
	for ngId, plan := range plans {
		wg.Add(1)
		go func(ngId string, plan []ngScaleUp) {
			defer wg.Done()
			nodeInfos := map[string]*framework.NodeInfo{ngId: nodeInfo}
			for _, ngScaleUp := range plan {
				klog.Infof("[HTNAP] Executing initialization scale-up by %d for node group %s, ProvReq %s/%s", ngScaleUp.prScaleUp.resize, ngId, ngScaleUp.prScaleUp.pr.Namespace, ngScaleUp.prScaleUp.pr.Name)
				aErr := s.scaleUpExecutor.executeScaleUp(ngScaleUp.scaleUpInfo, nodeInfos, ngScaleUp.prScaleUp.pr, now, ngScaleUp.prScaleUp.shouldUpdateProvReqDetails)
				s.prCache.UnregisterUpcomingProvReq(ngScaleUp.prScaleUp.pr)
				if aErr != nil {
					errResults <- errResult{err: aErr, info: &ngScaleUp.scaleUpInfo}
				}
			}
		}(ngId, plan)
	}
	wg.Wait()
	close(errResults)
	var results []errResult
	for err := range errResults {
		results = append(results, err)
	}
	if len(results) > 0 {
		failedNodeGroups := make([]cloudprovider.NodeGroup, len(results))
		scaleUpErrors := make([]errors.AutoscalerError, len(results))
		for i, result := range results {
			failedNodeGroups[i] = result.info.Group
			scaleUpErrors[i] = result.err
		}
		return errors.Combine(scaleUpErrors), failedNodeGroups
	}
	return nil, nil
}

func (s *AsyncDWSNodeGroupInitializer) emitScaleUpStatus(scaleUpStatus *status.ScaleUpStatus, err errors.AutoscalerError) {
	status.UpdateScaleUpError(scaleUpStatus, err)
	s.scaleUpStatusProcessor.Process(s.context, scaleUpStatus)
}
