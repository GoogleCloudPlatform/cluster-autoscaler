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
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	pod_util "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	"k8s.io/klog/v2"
)

const (
	nodePoolLabel    = "cloud.google.com/gke-nodepool"
	quickRemoveTaint = "cloud.google.com/gke-quick-remove"
)

// ScaleToZeroPodListProcessor is a PodListProcessor that is used to allow
// scaling an entire cluster to 0 nodes if all the non-daemonset, scheduled
// and pending pods are in ignoredNamespaces. This is achieved by filtering
// out any such pods (if the conditions are met) so that they no longer trigger
// scale-up or block scale-down.
// Cluster scale-to-0 will only happen if the conditions are met continuously
// for the duration of gracePeriod.
type ScaleToZeroPodListProcessor struct {
	metricsFilter        filter.MetricsFilter
	emptySince           time.Time
	gracePeriod          time.Duration
	systemPodsClassifier systempods.Classifier
	podsFilteredOut      bool
	ignoreNodeFn         filter.IgnoreNodeFilter
}

// NewScaleToZeroPodListProcessor creates a new ScaleToZeroPodListProcessor.
func NewScaleToZeroPodListProcessor(metricsFilter filter.MetricsFilter, gracePeriod time.Duration, podsClassifier systempods.Classifier) *ScaleToZeroPodListProcessor {
	return newScaleToZeroPodListProcessorWithNodeFilter(metricsFilter, gracePeriod, podsClassifier, nil)
}

// newScaleToZeroPodListProcessor creates a new ScaleToZeroPodListProcessor.
func newScaleToZeroPodListProcessorWithNodeFilter(metricsFilter filter.MetricsFilter, gracePeriod time.Duration, podsClassifier systempods.Classifier, ignoreNodeFn filter.IgnoreNodeFilter) *ScaleToZeroPodListProcessor {
	return &ScaleToZeroPodListProcessor{
		metricsFilter:        metricsFilter,
		gracePeriod:          gracePeriod,
		systemPodsClassifier: podsClassifier,
		ignoreNodeFn:         ignoreNodeFn,
	}
}

// Process verifies if the only pods in cluster (scheduled and pending) are
// daemonset or system pods. If this is true and has been true for the duration
// of grace period it filters out non-ds pods both from list of unschedulable
// pods and ClusterSnapshot.
func (p *ScaleToZeroPodListProcessor) Process(context *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	nodeInfos, err := context.ClusterSnapshot.ListNodeInfos()
	if err != nil {
		// This is the safe direction to fail (if in doubt don't take
		// any action).
		// Also - current implementation of snapshot never returns an
		// error, it's in the function signature only to satisfy the
		// interface.
		klog.Warningf("Error when trying to retrieve nodeInfos from snapshot: %v. Assuming user pods are present in cluster", err)
		return unschedulablePods, nil
	}
	p.metricsFilter.ObserveScaleToZero(unschedulablePods, nodeInfos, p.ignoreNodeFn, false)

	if p.blockingPodsPresent(nodeInfos, unschedulablePods) {
		p.resetStatus()
		klog.Info("User pods present")
		return unschedulablePods, nil
	}
	if p.emptySince.IsZero() {
		p.emptySince = time.Now()
	}
	klog.Infof("No user pods observed since %v", p.emptySince)

	if p.alreadyAtZero(nodeInfos) || p.allNodesQuickRemoveCandidates(nodeInfos) || p.emptySince.Add(p.gracePeriod).Before(time.Now()) {
		p.metricsFilter.ObserveScaleToZero(unschedulablePods, nodeInfos, p.ignoreNodeFn, true)
		klog.Info("Filtering out system pods to allow cluster scale-to-0")
		err := p.filterOutSystemPods(context.ClusterSnapshot)
		if err != nil {
			return []*apiv1.Pod{}, errors.ToAutoscalerError(errors.InternalError, err).AddPrefix("Failed when filtering system pods for clusters scale-to-0")
		}
		// Filtering out all pods from pending pods list.
		// It's important that we remove unschedulable user DS from this
		// list. Otherwise there is a large chance they would be scheduled
		// in filterOutSchedulable (since we've filtered out system pods
		// that are running on the node and thus freed resources for DS)
		// which would block scale-down (via DisableScaleDownForLoop call
		// in filterOutSchedulable).
		return []*apiv1.Pod{}, nil
	}
	return unschedulablePods, nil
}

func (p *ScaleToZeroPodListProcessor) blockingPodsPresent(nodeInfos []*framework.NodeInfo, unschedulable []*apiv1.Pod) bool {
	for _, pod := range unschedulable {
		if p.podBlocksScaleToZero(pod) {
			return true
		}
	}
	for _, ni := range nodeInfos {
		if p.ignoreNode(ni) {
			continue
		}
		for _, podInfo := range ni.Pods() {
			if p.podBlocksScaleToZero(podInfo.Pod) {
				return true
			}
		}
	}
	return false
}

func (p *ScaleToZeroPodListProcessor) podBlocksScaleToZero(pod *apiv1.Pod) bool {
	// DS and system pods do not block.
	return !pod_util.IsDaemonSetPod(pod) && !p.systemPodsClassifier.IsSystemPod(pod)
}

// This function only returns error in case of snapshot commit/revert errors.
// In this case the snapshot is in inconsistent state and there is probably
// no way to recover the CA loop.
func (p *ScaleToZeroPodListProcessor) filterOutSystemPods(snapshot clustersnapshot.ClusterSnapshot) error {
	// Technically fork/commit is not really needed to modify snapshot here.
	// The reasoning is to revert on failure, so in case of error CA can
	// continue the loop with a consistent snapshot.
	// Fork/commit can be somewhat expensive in very large cluster, but a
	// scenario where we have a huge cluster with no user pods seems unlikely.
	snapshot.Fork()
	nodeInfos, err := snapshot.ListNodeInfos()
	if err != nil {
		klog.Errorf("Aborting cluster scale-to-0. Failed to list nodeinfos: %v", err)
		snapshot.Revert()
		return nil
	}

	// Avoid issues with modifying snapshot during iteration
	nodeInfosCopy := make([]*framework.NodeInfo, len(nodeInfos))
	copy(nodeInfosCopy, nodeInfos)
	nodeInfos = nodeInfosCopy

	for _, ni := range nodeInfos {
		if p.ignoreNode(ni) {
			continue
		}
		for _, podInfo := range ni.Pods() {
			if pod_util.IsDaemonSetPod(podInfo.Pod) {
				continue
			}

			// This copies nodeInfo every time pod is removed. Filtering all pods here
			// and doing RemoveNode / AddNodeWithPods could be used if this ever needs
			// to be optimized.
			ns := podInfo.Pod.Namespace
			name := podInfo.Pod.Name
			if !p.systemPodsClassifier.IsSystemPod(podInfo.Pod) {
				klog.Errorf("Aborting cluster scale-to-0. Encountered user pod %s/%s when filtering out pods", ns, name)
				snapshot.Revert()
				return nil
			}
			err := snapshot.ForceRemovePod(ns, name, ni.Node().Name)
			if err != nil {
				klog.Errorf("Aborting cluster scale-to-0. Failed to remove pod %s/%s: %s", ns, name, err)
				snapshot.Revert()
				return nil
			}
		}
	}

	p.podsFilteredOut = true
	return snapshot.Commit()
}

func (p *ScaleToZeroPodListProcessor) resetStatus() {
	p.emptySince = time.Time{}
	p.podsFilteredOut = false
}

func (p *ScaleToZeroPodListProcessor) ignoreNode(ni *framework.NodeInfo) bool {
	return p.ignoreNodeFn != nil && p.ignoreNodeFn(ni)
}

func (p *ScaleToZeroPodListProcessor) alreadyAtZero(nodeInfos []*framework.NodeInfo) bool {
	// For non-MT clusters there is no extra penalty here since:
	// 1. if nodeInfos is already at 0 then the loop won't be executed
	// 2. if nodeInfos is not at 0 then the very first node itself will cause the loop
	// to break since we don't ignore any nodes in non-MT clusters
	for _, ni := range nodeInfos {
		if !p.ignoreNode(ni) {
			return false
		}
	}
	return true
}

func (p *ScaleToZeroPodListProcessor) Name() string {
	return "ScaleToZero"
}

// Drainable drains pods when scale to zero is triggered.
func (p *ScaleToZeroPodListProcessor) Drainable(_ *drainability.DrainContext, _ *apiv1.Pod, _ *framework.NodeInfo) drainability.Status {
	if p.podsFilteredOut {
		return drainability.Status{
			Outcome: drainability.SkipDrain,
		}
	}
	return drainability.NewUndefinedStatus()
}

func (p *ScaleToZeroPodListProcessor) allNodesQuickRemoveCandidates(nodeInfos []*framework.NodeInfo) bool {
	klog.Infof("Checking all nodes for quick removal")
	for _, ni := range nodeInfos {
		if p.ignoreNode(ni) {
			continue
		}
		defaultPool := ni.Node().Labels[nodePoolLabel] == "default-pool"
		quickRemove := containsTaint(ni.Node(), quickRemoveTaint)
		if !defaultPool || !quickRemove {
			klog.Infof("Node %q is not a candidate for quick removal", ni.Node().Name)
			return false
		}
	}
	klog.Infof("All nodes are candidates for quick removal")
	return true
}

func containsTaint(node *apiv1.Node, taint string) bool {
	for _, t := range node.Spec.Taints {
		if t.Key == quickRemoveTaint {
			return true
		}
	}
	return false
}
