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

package impostor

import (
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/actuation"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	ca_processors "k8s.io/autoscaler/cluster-autoscaler/processors"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/klog/v2"
)

// NewScaleDown returns new instance of ScaleDown.
func NewScaleDown(ctx *context.AutoscalingContext, planner scaledown.Planner, actuator scaledown.Actuator, processors *ca_processors.AutoscalingProcessors) *ScaleDown {
	return &ScaleDown{
		scaleDownPlanner:   planner,
		scaleDownActuator:  actuator,
		processors:         processors,
		AutoscalingContext: ctx,
		processorCallbacks: newStaticAutoscalerProcessorCallbacks(),
		taintConfig:        taints.NewTaintConfig(ctx.AutoscalingOptions),
	}
}

// ScaleDown is a structure used to  hold utils relevant for scale down.
type ScaleDown struct {
	*context.AutoscalingContext
	lastScaleUpTime         time.Time
	lastScaleDownDeleteTime time.Time
	lastScaleDownFailTime   time.Time
	scaleDownPlanner        scaledown.Planner
	scaleDownActuator       scaledown.Actuator
	processors              *ca_processors.AutoscalingProcessors
	taintConfig             taints.TaintConfig
	processorCallbacks      *staticAutoscalerProcessorCallbacks
}

// Run tries to perform scale down. It is borrowed from oss/cluster-autoscaler/core/static_autoscaler.go.
func (sd *ScaleDown) Run(currentTime time.Time) error {
	autoscalingContext := sd.AutoscalingContext
	scaleDownStatus := &status.ScaleDownStatus{Result: status.ScaleDownNotTried}
	// Get nodes and pods currently living on cluster
	allNodes, _, typedErr := sd.obtainNodeLists(sd.CloudProvider)
	if typedErr != nil {
		klog.Errorf("Failed to get node list: %v", typedErr)
		return typedErr
	}

	pods, err := sd.AllPodLister().List()
	if err != nil {
		klog.Errorf("Failed to list pods: %v", err)
		return errors.ToAutoscalerError(errors.ApiCallError, err)
	}
	scheduledPods := kube_util.ScheduledPods(pods)

	unneededStart := time.Now()
	// Initialize cluster state to ClusterSnapshot
	if err := sd.ClusterSnapshot.SetClusterState(allNodes, scheduledPods, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()); typedErr != nil {
		return errors.ToAutoscalerError(errors.InternalError, fmt.Errorf("ClusterSnapshot.SetClusterState: %v", err))
	}
	// Initialize remaining PDBs
	if typedErr := sd.initializeRemainingPdbTracker(); typedErr != nil {
		return typedErr.AddPrefix("Initialize RemainingPDBTracker")
	}

	klog.V(4).Infof("Calculating unneeded nodes")

	var scaleDownCandidates []*apiv1.Node
	var podDestinations []*apiv1.Node

	// podDestinations and scaleDownCandidates are initialized based on allNodes variable, which contains only
	// registered nodes in cluster.
	// It does not include any upcoming nodes which can be part of clusterSnapshot. As an alternative to using
	// allNodes here, we could use nodes from clusterSnapshot and explicitly filter out upcoming nodes here but it
	// is of little (if any) benefit.

	if sd.processors == nil || sd.processors.ScaleDownNodeProcessor == nil {
		scaleDownCandidates = allNodes
		podDestinations = allNodes
	} else {
		var err errors.AutoscalerError
		scaleDownCandidates, err = sd.processors.ScaleDownNodeProcessor.GetScaleDownCandidates(
			autoscalingContext, allNodes)
		if err != nil {
			klog.Error(err)
			return err
		}
		podDestinations, err = sd.processors.ScaleDownNodeProcessor.GetPodDestinationCandidates(autoscalingContext, allNodes)
		if err != nil {
			klog.Error(err)
			return err
		}
	}
	actuationStatus := sd.scaleDownActuator.CheckStatus()
	if typedErr := sd.scaleDownPlanner.UpdateClusterState(podDestinations, scaleDownCandidates, actuationStatus, currentTime); typedErr != nil {
		scaleDownStatus.Result = status.ScaleDownError
		klog.Errorf("Failed to scale down: %v", typedErr)
		return typedErr
	}

	unneededNodes := sd.scaleDownPlanner.UnneededNodes()
	sd.processors.ScaleDownCandidatesNotifier.Update(unneededNodes, currentTime)

	metrics.UpdateDurationFromStart(metrics.FindUnneeded, unneededStart)

	scaleDownInCooldown := sd.processorCallbacks.disableScaleDownForLoop ||
		sd.lastScaleUpTime.Add(sd.ScaleDownDelayAfterAdd).After(currentTime) ||
		sd.lastScaleDownFailTime.Add(sd.ScaleDownDelayAfterFailure).After(currentTime) ||
		sd.lastScaleDownDeleteTime.Add(sd.ScaleDownDelayAfterDelete).After(currentTime)

	klog.V(4).Infof("Scale down status: lastScaleUpTime=%s lastScaleDownDeleteTime=%v "+
		"lastScaleDownFailTime=%s scaleDownForbidden=%v scaleDownInCooldown=%v",
		sd.lastScaleUpTime, sd.lastScaleDownDeleteTime, sd.lastScaleDownFailTime,
		sd.processorCallbacks.disableScaleDownForLoop, scaleDownInCooldown)
	metrics.UpdateScaleDownInCooldown(scaleDownInCooldown)

	if scaleDownInCooldown {
		scaleDownStatus.Result = status.ScaleDownInCooldown
	} else {
		klog.V(4).Infof("Starting scale down")

		// We want to delete unneeded Node Groups only if there was no recent scale up,
		// and there is no current delete in progress and there was no recent errors.
		_, drained := actuationStatus.DeletionsInProgress()
		var removedNodeGroups []cloudprovider.NodeGroup
		if len(drained) == 0 {
			var err error
			removedNodeGroups, err = sd.processors.NodeGroupManager.RemoveUnneededNodeGroups(autoscalingContext)
			if err != nil {
				klog.Errorf("Error while removing unneeded node groups: %v", err)
			}
		}

		scaleDownStart := time.Now()
		metrics.UpdateLastTime(metrics.ScaleDown, scaleDownStart)
		empty, needDrain := sd.scaleDownPlanner.NodesToDelete(scaleDownStart)

		scaleDownResult, scaledDownNodes, typedErr := sd.scaleDownActuator.StartDeletion(empty, needDrain)
		nodeDeleteResults, nodeDeleteResultsAsOf := sd.scaleDownActuator.DeletionResults()
		sd.scaleDownActuator.ClearResultsNotNewerThan(scaleDownStatus.NodeDeleteResultsAsOf)
		scaleDownStatus := &status.ScaleDownStatus{
			Result:                scaleDownResult,
			ScaledDownNodes:       scaledDownNodes,
			NodeDeleteResults:     nodeDeleteResults,
			NodeDeleteResultsAsOf: nodeDeleteResultsAsOf,
			RemovedNodeGroups:     removedNodeGroups,
		}

		metrics.UpdateDurationFromStart(metrics.ScaleDown, scaleDownStart)
		metrics.UpdateUnremovableNodesCount(countsByReason(sd.scaleDownPlanner.UnremovableNodes()))

		if scaleDownStatus.Result == status.ScaleDownNodeDeleteStarted {
			sd.lastScaleDownDeleteTime = currentTime
		}

		if scaleDownStatus.Result == status.ScaleDownNoNodeDeleted &&
			sd.AutoscalingContext.AutoscalingOptions.MaxBulkSoftTaintCount != 0 {
			taintableUnneededNodes := sd.scaleDownPlanner.UnneededNodes()
			taintableNodes := retrieveNodes(taintableUnneededNodes)
			untaintableNodes := subtractNodes(allNodes, taintableNodes)
			actuation.UpdateSoftDeletionTaints(sd.AutoscalingContext, taintableNodes, untaintableNodes)
		}

		if sd.processors != nil && sd.processors.ScaleDownStatusProcessor != nil {
			scaleDownStatus.SetUnremovableNodesInfo(sd.scaleDownPlanner.UnremovableNodes(), sd.scaleDownPlanner.NodeUtilizationMap(), sd.CloudProvider)
			sd.processors.ScaleDownStatusProcessor.Process(autoscalingContext, scaleDownStatus)
		}

		if typedErr != nil {
			klog.Errorf("Failed to scale down: %v", typedErr)
			sd.lastScaleDownFailTime = currentTime
			return typedErr
		}
	}
	return nil
}

func (sd *ScaleDown) obtainNodeLists(cp cloudprovider.CloudProvider) ([]*apiv1.Node, []*apiv1.Node, errors.AutoscalerError) {
	allNodes, err := sd.AllNodeLister().List()
	if err != nil {
		klog.Errorf("Failed to list all nodes: %v", err)
		return nil, nil, errors.ToAutoscalerError(errors.ApiCallError, err)
	}
	readyNodes, err := sd.ReadyNodeLister().List()
	if err != nil {
		klog.Errorf("Failed to list ready nodes: %v", err)
		return nil, nil, errors.ToAutoscalerError(errors.ApiCallError, err)
	}

	// Handle GPU case - allocatable GPU may be equal to 0 up to 15 minutes after
	// node registers as ready. See https://github.com/kubernetes/kubernetes/issues/54959
	// Treat those nodes as unready until GPU actually becomes available and let
	// our normal handling for booting up nodes deal with this.
	// TODO: Update to use proper mocked Snapshot
	allNodes, readyNodes = sd.processors.CustomResourcesProcessor.FilterOutNodesWithUnreadyResources(sd.AutoscalingContext, allNodes, readyNodes, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot())
	allNodes, readyNodes = taints.FilterOutNodesWithStartupTaints(sd.taintConfig, allNodes, readyNodes)
	return allNodes, readyNodes, nil
}

type staticAutoscalerProcessorCallbacks struct {
	disableScaleDownForLoop bool
	extraValues             map[string]any
	scaleDownPlanner        scaledown.Planner
}

func (callbacks *staticAutoscalerProcessorCallbacks) ResetUnneededNodes() {
	callbacks.scaleDownPlanner.CleanUpUnneededNodes()
}

func newStaticAutoscalerProcessorCallbacks() *staticAutoscalerProcessorCallbacks {
	callbacks := &staticAutoscalerProcessorCallbacks{}
	callbacks.reset()
	return callbacks
}

func (callbacks *staticAutoscalerProcessorCallbacks) DisableScaleDownForLoop() {
	callbacks.disableScaleDownForLoop = true
}

func (callbacks *staticAutoscalerProcessorCallbacks) SetExtraValue(key string, value any) {
	callbacks.extraValues[key] = value
}

func (callbacks *staticAutoscalerProcessorCallbacks) GetExtraValue(key string) (value any, found bool) {
	value, found = callbacks.extraValues[key]
	return
}

func (callbacks *staticAutoscalerProcessorCallbacks) reset() {
	callbacks.disableScaleDownForLoop = false
	callbacks.extraValues = make(map[string]any)
}

func countsByReason(nodes []*simulator.UnremovableNode) map[simulator.UnremovableReason]int {
	counts := make(map[simulator.UnremovableReason]int)

	for _, node := range nodes {
		counts[node.Reason]++
	}

	return counts
}

func subtractNodes(a []*apiv1.Node, b []*apiv1.Node) []*apiv1.Node {
	var c []*apiv1.Node
	namesToDrop := make(map[string]bool)
	for _, n := range b {
		namesToDrop[n.Name] = true
	}
	for _, n := range a {
		if namesToDrop[n.Name] {
			continue
		}
		c = append(c, n)
	}
	return c
}

func (sd *ScaleDown) initializeRemainingPdbTracker() errors.AutoscalerError {
	sd.RemainingPdbTracker.Clear()

	pdbs, err := sd.PodDisruptionBudgetLister().List()
	if err != nil {
		klog.Errorf("Failed to list pod disruption budgets: %v", err)
		return errors.NewAutoscalerError(errors.ApiCallError, err.Error())
	}
	err = sd.RemainingPdbTracker.SetPdbs(pdbs)
	if err != nil {
		return errors.NewAutoscalerError(errors.InternalError, err.Error())
	}
	return nil
}

func retrieveNodes(unneededNodes []*scaledown.UnneededNode) []*apiv1.Node {
	nodes := make([]*apiv1.Node, 0, len(unneededNodes))
	for _, c := range unneededNodes {
		nodes = append(nodes, c.Node)
	}
	return nodes
}
