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
	gocontext "context"
	"fmt"
	"time"

	v1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apilabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/defrag/plugins/config"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/daemon"
	"k8s.io/kubernetes/pkg/util/taints"
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
	daemonSets, err := ctx.ListerRegistry.DaemonSetLister().List(apilabels.Everything())
	if err != nil {
		klog.Errorf("Failed to list daemon sets: %v", err)
		return nil
	}

	now := p.clock.Now()
	newPending := make(map[string]time.Time)
	var unfitNodes []string

	for _, nodeName := range nodeNames {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Errorf("Failed to get node info for node %v: %v", nodeName, err)
			continue
		}

		status := p.checkNodeStatus(ctx, nodeInfo, daemonSets)
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
	daemonSets, err := ctx.ListerRegistry.DaemonSetLister().List(apilabels.Everything())
	if err != nil {
		klog.Errorf("Failed to list daemon sets: %v", err)
		return nil
	}

	var candidateNodes []string
	for _, nodeName := range nodeNames {
		nodeInfo, err := ctx.ClusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Errorf("Failed to get node info for node %v: %v", nodeName, err)
			continue
		}

		// Remove HardTaint from the candidate node. We are using the scheduling
		// logic to determine if the DS pod should be running on the node. After
		// the candidate nodes are created they are tainted with the defrag taint,
		// for which the missing DS likely doesn't have a toleration. Because of
		// that here we just ignore this taint for previously picked nodes.
		node := nodeInfo.Node().DeepCopy()
		node.Spec.Taints, _ = taints.DeleteTaint(node.Spec.Taints, &apiv1.Taint{
			Key:    defrag.HardTaint,
			Effect: apiv1.TaintEffectNoSchedule,
		})

		if !allDaemonSetsSchedulable(ctx.ClusterSnapshot, node, nodeInfo.Pods(), daemonSets) {
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
	daemonSets []*v1.DaemonSet,
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

	if allDaemonSetsSchedulable(ctx.ClusterSnapshot, node, nodeInfo.Pods(), daemonSets) {
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

func allDaemonSetsSchedulable(snapshot clustersnapshot.ClusterSnapshot, node *apiv1.Node, podInfos []*framework.PodInfo, daemonSets []*v1.DaemonSet) bool {
	logger := klog.FromContext(gocontext.Background())
	snapshot.Fork()
	defer snapshot.Revert()

	runningDS := make(map[types.UID]bool)
	for _, podInfo := range podInfos {
		controllerRef := metav1.GetControllerOf(podInfo.Pod)
		if controllerRef != nil && controllerRef.Kind == "DaemonSet" {
			runningDS[controllerRef.UID] = true
		}
	}

	for _, ds := range daemonSets {
		if shouldRun, _ := daemon.NodeShouldRunDaemonPod(logger, node, ds); shouldRun && !runningDS[ds.UID] {
			dsPod := daemon.NewPod(ds, node.Name)
			if err := snapshot.SchedulePod(dsPod, node.Name); err != nil && err.Type() == clustersnapshot.SchedulingInternalError {
				// Unexpected error.
				klog.Errorf("Error while scheduling pod in snapshot: %v", err)
				return false
			} else if err != nil {
				// dsPod can't be scheduled on node because of scheduling predicates.
				klog.V(4).Infof("Node %v cannot schedule expected DS pod %v: %q", node.Name, ds.Name, err)
				return false
			}
			// dsPod was scheduled on node.
		}
	}

	return true
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
