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
	"fmt"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/nodecontroller"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	internalmetrics "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/utils/set"
)

const (
	nodeReconciliationMetricLabel = "scaleUp:CSNNodeReconciliationProcessor"
	bufferAssignmentUnknown       = "unknown"
	nodeReconciliationLogPrefix   = "CSN Node Reconciliation processor:"
)

type csnNodeController interface {
	List(filters ...nodecontroller.CSNFilter) ([]nodecontroller.CSNNode, error)
	Consume(nodes []string) set.Set[string]
	MarkAsSuspendable(nodeInfos []*framework.NodeInfo) set.Set[string]
	ProcessBufferAssignment(nodeNameToBuffer map[string]*v1beta1.CapacityBuffer)
	Reconcile()
}

type cloudProvider interface {
	GkeMigForNode(node *apiv1.Node) (*gke.GkeMig, error)
}

// NodeReconciliationProcessor is a processor that reconciles the state of CSN nodes in the Cluster Autoscaler's ClusterSnapshot
// with the desired state from the CSN node controller. go/csn-in-ca
type NodeReconcilationProcessor struct {
	nodeController     csnNodeController
	cloudProvider      cloudProvider
	experimentsManager experiments.Manager
	metrics            csnMetrics
}

func NewNodeReconciliationProcessor(nodeController csnNodeController, cloudProvider cloudProvider, experimentsManager experiments.Manager) *NodeReconcilationProcessor {
	return &NodeReconcilationProcessor{
		nodeController:     nodeController,
		cloudProvider:      cloudProvider,
		experimentsManager: experimentsManager,
		metrics:            internalmetrics.Metrics,
	}
}

func (p *NodeReconcilationProcessor) Preprocess(ctx *context.AutoscalingContext) error {
	defer metrics.UpdateDurationFromStart(nodeReconciliationMetricLabel, time.Now())

	snapshot := ctx.ClusterSnapshot
	snapshot.Fork()

	err := p.preprocess(snapshot)
	if err != nil {
		snapshot.Revert()
		klog.Errorf("%s error during preprocess: %v", nodeReconciliationLogPrefix, err)
		p.metrics.SetCSNInvalidCondition(internalmetrics.CSNNodeReconciliationProcessorError)
		return err
	}

	if err := snapshot.Commit(); err != nil {
		klog.Errorf("%s error while commiting the snapshot: %v", nodeReconciliationLogPrefix, err)
		p.metrics.SetCSNInvalidCondition(internalmetrics.CommitSnapshotError)
		return fmt.Errorf("error while commiting the snapshot: %v", err)
	}
	return nil
}

// proprocess should be executed under fork.
func (p *NodeReconcilationProcessor) preprocess(snapshot clustersnapshot.ClusterSnapshot) error {
	go func() {
		p.nodeController.Reconcile()
	}()
	csnNodes, err := p.nodeController.List()
	if err != nil {
		return fmt.Errorf("error listing CSN nodes: %v", err)
	}

	csnNodesByName := make(map[string]nodecontroller.CSNNode, len(csnNodes))
	for _, csnNode := range csnNodes {
		csnNodesByName[csnNode.Name] = csnNode
	}

	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		return fmt.Errorf("error getting node infos: %v", err)
	}

	processTemplateNodesEnabled := p.experimentsManager.DirectLaunchBoolFlag(experiments.ColdStandbyNodesProcessTemplateNodeInfosFlag)

	for _, ni := range nodeInfos {
		if !processTemplateNodesEnabled {
			// If this experiments flag is enabled, then the template generating upcoming nodes will contain the fix no need to execute this function.
			p.markUpcomingCSNNode(ni)
		}

		node := ni.Node()
		assignedBuffer := csn.GetBufferIdFromNode(node)
		csnNode, csnNodeFound := csnNodesByName[node.Name]
		if csnNodeFound {
			csnBuffer := csnNode.Buffer.Id()
			if assignedBuffer != "" && csnBuffer != "" && assignedBuffer != csnBuffer {
				klog.Warningf("%s node %q has assigned buffer %q different from the one from CSN node controller %q. Assuming the controller as source of truth.", nodeReconciliationLogPrefix, node.Name, assignedBuffer, csnBuffer)
			}
			if csnBuffer != "" {
				assignedBuffer = csnBuffer
			}
		} else if assignedBuffer != "" {
			klog.Warningf("%s node %q has assigned buffer, but this node doesn't exist in CSN node controller", nodeReconciliationLogPrefix, node.Name)
		}

		if assignedBuffer == "" && csn.ClassifyNode(node) == csn.NodeStateChilling {
			assignedBuffer = bufferAssignmentUnknown
		}

		if assignedBuffer != "" {
			var err error
			node, err = assignNodeToBufferForProcessors(node, assignedBuffer)
			if err != nil {
				return fmt.Errorf("error assigning node %q to buffer %q: %v", node.Name, assignedBuffer, err)
			}
			ni.SetNode(node)
		}

		if !csnNodeFound {
			continue
		}

		if csnNode.DesiredState == csn.NodeStateUnknown {
			continue
		}
		node, err = setNodeAsForProcessors(node, csnNode.DesiredState)
		if err != nil {
			return fmt.Errorf("error marking node %q as %s: %v", csnNode.Name, csnNode.DesiredState, err)
		}
		if csnNode.DesiredState == csn.NodeStateConsumed {
			// We do this because this taint takes some time to be removed, so we need to remove it in CA simulation to avoid unnecessary scale-up.
			// Node Controller delete consumed nodes very shortly after they are consumed, so this change should be safe (i.e. won't remove unreachable taints when the node wasn't suspended shortly before).
			node.Spec.Taints, _ = taints.DeleteTaintsByKey(node.Spec.Taints, apiv1.TaintNodeUnreachable)
		}
		ni.SetNode(node)
	}
	return nil
}

func (p *NodeReconcilationProcessor) markUpcomingCSNNode(ni *framework.NodeInfo) {
	node := ni.Node()
	// Node is already a CSN node, no need to modify it
	if csn.IsCSNNode(node) {
		return
	}
	// Node is not upcoming, it shouldn't be marked
	if !isNodeUpcoming(node) {
		return
	}

	// Node is not in a CSN node group, it shouldn't be marked
	if !NodeInCSNNodeGroup(node, p.cloudProvider) {
		return
	}

	// This only happens for upcoming nodes.
	// New nodes will be `chilling` at first.
	node, err := csn.SetNodeAs(node, csn.NodeStateChilling)
	if err != nil {
		klog.Errorf("%s error while adding CSN label to node %q: %v", nodeReconciliationLogPrefix, node.Name, err)
		return
	}
	ni.SetNode(node)
	return
}

// NodeInCSNNodeGroup checks if the node belongs to a CSN node group.
func NodeInCSNNodeGroup(node *apiv1.Node, cp cloudProvider) bool {
	mig, err := cp.GkeMigForNode(node)
	if err != nil {
		return false
	}
	// nil mig means the node should not be managed by CA.
	if mig == nil {
		return false
	}
	spec := mig.Spec()
	if spec == nil {
		return false
	}
	if spec.Labels == nil {
		// no labels == no CSN label
		return false
	}
	if spec.Labels[csn.SoftWorkloadSeparationKey] != csn.SoftWorkloadSeparationValue {
		return false
	}
	return true
}

func isNodeUpcoming(node *apiv1.Node) bool {
	if node.Annotations == nil {
		return false
	}
	val, ok := node.Annotations[annotations.NodeUpcomingAnnotation]
	if !ok {
		return false
	}
	return val == "true"
}
