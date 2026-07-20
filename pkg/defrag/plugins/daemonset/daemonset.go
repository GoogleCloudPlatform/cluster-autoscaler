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

package daemonset

import (
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/utils/clock"
)

type nodeStatus int

const (
	NodeHealthy nodeStatus = iota
	NodeUnfit
	NodeNeedsGrace
)

const (
	PluginName         = "daemonset"
	defaultGracePeriod = 3 * time.Minute
)

type plugin struct {
	config                config.PluginsConfig
	latestUnfitNodesCount int
	pendingUnfitNodes     map[string]time.Time
	clock                 clock.PassiveClock
	gracePeriod           time.Duration
}

func NewPlugin(config config.PluginsConfig) defrag.Plugin {
	return &plugin{
		config:            config,
		pendingUnfitNodes: make(map[string]time.Time),
		clock:             clock.RealClock{},
		gracePeriod:       defaultGracePeriod,
	}
}

func (p *plugin) String() string {
	return PluginName
}

func (p *plugin) NewCandidate(ctx *context.AutoscalingContext, nodeNames []string) *defrag.Candidate {
	targetNodes := unschedulableDSPodsTargetNodes(ctx)

	now := p.clock.Now()
	newPending := make(map[string]time.Time)
	var unfitNodes []string

	for _, nodeName := range nodeNames {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Errorf("Failed to get node info for node %v: %v", nodeName, err)
			continue
		}

		status := p.checkNodeStatus(ctx, nodeInfo, targetNodes.Has(nodeName))
		switch status {
		case NodeUnfit:
			unfitNodes = append(unfitNodes, nodeName)
		case NodeNeedsGrace:
			discoveryTime, pending := p.pendingUnfitNodes[nodeName]
			switch {
			case !pending:
				klog.V(5).Infof("Defrag/daemonset: EK node %s newly unfit — starting grace period.", nodeName)
				newPending[nodeName] = now
			case p.clock.Since(discoveryTime) >= p.gracePeriod:
				klog.V(5).Infof("Defrag/daemonset: EK node %s grace period expired — marking candidate.", nodeName)
				unfitNodes = append(unfitNodes, nodeName)
			default:
				klog.V(5).Infof("Defrag/daemonset: EK node %s still within grace period.", nodeName)
				newPending[nodeName] = discoveryTime
			}
		}
	}

	p.pendingUnfitNodes = newPending
	p.latestUnfitNodesCount = len(unfitNodes)

	if len(unfitNodes) == 0 {
		return nil
	}
	return defrag.NewCandidateWithLimit(unfitNodes, defrag.Partial, p.config.MaxCandidateNodeCount)
}

func (p *plugin) ValidCandidateNodes(ctx *context.AutoscalingContext, nodeNames []string) []string {
	targetNodes := unschedulableDSPodsTargetNodes(ctx)

	var candidateNodes []string
	for _, nodeName := range nodeNames {
		if targetNodes.Has(nodeName) {
			candidateNodes = append(candidateNodes, nodeName)
		}
	}
	return candidateNodes
}

func (p *plugin) IsExpansionOptionValid(_ *context.AutoscalingContext, _ *defrag.Candidate, _ expander.Option) bool {
	// All expansion options should schedule all DaemonSet pods, assuming --force-ds option is enabled
	return true
}

func (p *plugin) BackoffDuration(_ *context.AutoscalingContext, _ *defrag.Candidate) time.Duration {
	return 5 * time.Minute
}

func (p *plugin) Type() defrag.PluginType {
	return defrag.StandardPluginType
}

// checkNodeStatus checks a single node and returns its status.
// It determines if the node is unfit and if it's subject to the grace period.
func (p *plugin) checkNodeStatus(
	ctx *context.AutoscalingContext,
	nodeInfo *framework.NodeInfo,
	hasUnschedulableDSPod bool,
) nodeStatus {
	node := nodeInfo.Node()
	if !p.config.Autopilot {
		enabled, err := p.enabledViaCCC(ctx, node)
		if err != nil {
			klog.Error(err)
		}
		if !enabled {
			return NodeHealthy
		}
	}

	if !hasUnschedulableDSPod {
		return NodeHealthy
	}
	machineFamily, hasLabel := node.Labels[gkelabels.MachineFamilyLabel]
	if !hasLabel {
		klog.Errorf("Defrag/daemonset: node %q missing machine family label", node.Name)
		return NodeUnfit
	}

	// EK nodes require grace period to give them a chance to be upsized; others are immediately unfit.
	if machineFamily == machinetypes.EK.Name() {
		return NodeNeedsGrace
	}
	klog.V(5).Infof("Defrag/daemonset: Non-EK node %s unfit, marking candidate immediately", node.Name)
	return NodeUnfit
}

func unschedulableDSPodsTargetNodes(autoscalingCtx *context.AutoscalingContext) sets.Set[string] {
	targetNodes := make(sets.Set[string])
	if autoscalingCtx.ListerRegistry == nil || autoscalingCtx.ListerRegistry.AllPodLister() == nil {
		klog.Errorf("AllPodLister is not set in AutoscalingContext")
		return targetNodes
	}

	allPods, err := autoscalingCtx.ListerRegistry.AllPodLister().List()
	if err != nil {
		klog.Errorf("Failed to list pods: %v", err)
		return targetNodes
	}

	for _, pod := range allPods {
		if !isPodUnschedulable(pod) {
			continue
		}
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef == nil || controllerRef.Kind != "DaemonSet" {
			continue
		}
		targetNode := getNodeTargetedByDaemonSetPod(pod)
		if targetNode != "" {
			targetNodes.Insert(targetNode)
		}
	}
	return targetNodes
}

func isPodUnschedulable(pod *apiv1.Pod) bool {
	if pod.Spec.NodeName != "" {
		return false
	}
	_, cond := podutil.GetPodCondition(&pod.Status, apiv1.PodScheduled)
	if cond != nil && cond.Status == apiv1.ConditionFalse && cond.Reason == apiv1.PodReasonUnschedulable {
		return true
	}
	return false
}

func getNodeTargetedByDaemonSetPod(pod *apiv1.Pod) string {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, matchField := range term.MatchFields {
			if matchField.Key == metav1.ObjectNameField && matchField.Operator == apiv1.NodeSelectorOpIn && len(matchField.Values) == 1 {
				return matchField.Values[0]
			}
		}
	}
	return ""
}

func (p *plugin) enabledViaCCC(ctx *context.AutoscalingContext, node *apiv1.Node) (bool, error) {
	nodeGroup, err := ctx.CloudProvider.NodeGroupForNode(node)
	if err != nil {
		return false, fmt.Errorf("failed to get node group for node %s: %v", node.Name, err)
	}
	if nodeGroup == nil {
		return false, nil
	}

	crd, crdName, err := p.config.NPCLister.NodeGroupCrd(nodeGroup)
	if err != nil {
		return false, fmt.Errorf("failed to get CCC %s: %v", crdName, err)
	}

	if crd == nil {
		return false, nil
	}

	return crd.EnsureAllDaemonSetPodsRunning(), nil
}

func (p *plugin) LatestUnfitNodesCount() int {
	return p.latestUnfitNodesCount
}
