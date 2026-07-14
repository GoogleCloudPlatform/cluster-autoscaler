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

package controller

import (
	"context"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"

	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/processors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

const minimumCapacityControllerName = "MinimumCapacityController"

// minCapacityController tracks minimum capacity fulfillment.
type minCapacityController struct {
	checkInterval      time.Duration
	ccLister           cc_lister.Lister
	nodeLister         kube_util.NodeLister
	cloudProvider      cloudprovider.CloudProvider
	matcher            computeclass.Matcher
	observer           processors.MinCapacityObserver
	experimentsManager experiments.Manager
}

// NewMinCapacityController creates a new minCapacityController.
func NewMinCapacityController(
	checkInterval time.Duration,
	ccLister cc_lister.Lister,
	nodeLister kube_util.NodeLister,
	cloudProvider cloudprovider.CloudProvider,
	matcher computeclass.Matcher,
	observer processors.MinCapacityObserver,
	experimentsManager experiments.Manager,
) Controller {
	if observer == nil {
		klog.Warning("minCapacityController initialized with nil observer; metrics will not be emitted.")
	}
	return &minCapacityController{
		checkInterval:      checkInterval,
		ccLister:           ccLister,
		nodeLister:         nodeLister,
		cloudProvider:      cloudProvider,
		matcher:            matcher,
		observer:           observer,
		experimentsManager: experimentsManager,
	}
}

// Start starts the controller loop in the background.
func (c *minCapacityController) Start(ctx context.Context) error {
	klog.Infof("Starting %s with interval %v", minimumCapacityControllerName, c.checkInterval)
	go wait.UntilWithContext(ctx, c.runOnce, c.checkInterval)
	return nil
}

func (c *minCapacityController) runOnce(ctx context.Context) {
	now := time.Now()
	if err := c.reconcile(now); err != nil {
		klog.Errorf("%s: Failed to reconcile: %v", minimumCapacityControllerName, err)
	}
}

// reconcile checks if minimum capacity is fulfilled for the given CC.
func (c *minCapacityController) reconcile(now time.Time) error {
	if !computeclass.IsComputeClassMinCapacityEnabled(c.experimentsManager) {
		return nil
	}

	if c.observer != nil {
		c.observer.CheckLongUnprovisioned(now)
	}

	ccs, err := c.ccLister.ListCrds()
	if err != nil {
		klog.Errorf("%s: Failed to list CCs: %v", minimumCapacityControllerName, err)
		return err
	}

	nodes, err := c.nodeLister.List()
	if err != nil {
		klog.Errorf("%s: Failed to list nodes: %v", minimumCapacityControllerName, err)
		return err
	}

	// Pre-process and group ready nodes by ComputeClass
	readyNodesByCC := make(map[string][]*apiv1.Node)
	for _, node := range nodes {
		ccName, hasLabel := node.Labels[labels.ComputeClassLabel]
		if !hasLabel {
			continue
		}
		if _, isUpcoming := node.Annotations[annotations.NodeUpcomingAnnotation]; isUpcoming {
			continue
		}

		if kube_util.IsNodeReadyAndSchedulable(node) {
			readyNodesByCC[ccName] = append(readyNodesByCC[ccName], node)
		}
	}

	for _, cc := range ccs {
		ccName := cc.Name()
		realNodes := readyNodesByCC[ccName]

		specLevelCount := 0
		priorityRuleCounts := make(map[int]int)
		nodeGroupRuleCache := make(map[string]int)

		for _, node := range realNodes {
			nodeGroup, err := c.cloudProvider.NodeGroupForNode(node)
			if err != nil {
				klog.Warningf("%s: Failed to get node group for node %s: %v", minimumCapacityControllerName, node.Name, err)
				continue
			}
			if nodeGroup == nil {
				continue
			}

			specLevelCount++

			ngId := nodeGroup.Id()
			if ruleIdx, exists := nodeGroupRuleCache[ngId]; exists {
				if ruleIdx >= 0 {
					priorityRuleCounts[ruleIdx]++
				}
				continue
			}

			ruleFound, ruleIdx, _ := c.matcher.FirstMatchedRule(nodeGroup, cc)
			if ruleFound {
				nodeGroupRuleCache[ngId] = ruleIdx
				priorityRuleCounts[ruleIdx]++
			} else {
				nodeGroupRuleCache[ngId] = -1 // Cache negative results too
			}
		}

		// Emit metrics for spec level.
		if cc.TargetNodeCount() != nil {
			if specLevelCount >= *cc.TargetNodeCount() {
				klog.V(5).Infof("%s: CC %s spec level capacity fulfilled (%d >= %d)", minimumCapacityControllerName, ccName, specLevelCount, *cc.TargetNodeCount())
				if c.observer != nil {
					c.observer.OnProvisioningComplete(ccName, -1, now)
				}
			} else if c.observer != nil {
				c.observer.OnShortfallDetected(ccName, -1, now)
			}
		}

		// Emit metrics for priority levels.
		for ruleIdx, rule := range cc.Rules() {
			if rule.TargetNodeCount() != nil {
				count := priorityRuleCounts[ruleIdx]
				if count >= *rule.TargetNodeCount() {
					klog.V(5).Infof("%s: CC %s priority rule %d capacity fulfilled (%d >= %d)", minimumCapacityControllerName, ccName, ruleIdx, count, *rule.TargetNodeCount())
					if c.observer != nil {
						c.observer.OnProvisioningComplete(ccName, ruleIdx, now)
					}
				} else if c.observer != nil {
					c.observer.OnShortfallDetected(ccName, ruleIdx, now)
				}
			}
		}
	}

	return nil
}
