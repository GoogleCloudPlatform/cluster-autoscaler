/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourcequota

import (
	"fmt"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

// TargetNodeCountProvider provides quotas based on targetNodeCount in ComputeClass CRDs.
type TargetNodeCountProvider struct {
	ccLister           cc_lister.Lister
	excludeTopLevel    bool
	experimentsManager experiments.Manager
}

// NewTargetNodeCountProvider creates a new TargetNodeCountProvider.
func NewTargetNodeCountProvider(ccLister cc_lister.Lister, excludeTopLevel bool, experimentsManager experiments.Manager) *TargetNodeCountProvider {
	return &TargetNodeCountProvider{
		ccLister:           ccLister,
		excludeTopLevel:    excludeTopLevel,
		experimentsManager: experimentsManager,
	}
}

// Quotas generates TargetNodeCountQuotas for valid non-negative targets in all ComputeClass CRDs.
func (p *TargetNodeCountProvider) Quotas() ([]resourcequotas.Quota, error) {
	if !computeclass.IsComputeClassMinCapacityEnabled(p.experimentsManager) {
		return nil, nil
	}

	crds, err := p.ccLister.ListCrds()
	if err != nil {
		return nil, err
	}
	var quotas []resourcequotas.Quota
	for _, c := range crds {
		crdName := c.Name()

		// Top-level quota
		if !p.excludeTopLevel && c.TargetNodeCount() != nil {
			if target := *c.TargetNodeCount(); target >= 0 {
				quotas = append(quotas, &TargetNodeCountQuota{
					id:              fmt.Sprintf("cc-min-nodes-%s", crdName),
					crdName:         crdName,
					targetNodeCount: int64(target),
					ruleIdxStr:      "", // Empty means all nodes for this CC
				})
			} else {
				klog.Warningf("Ignoring invalid TargetNodeCount %d for CCC %s", target, crdName)
			}
		}

		// Rule-level quotas
		for ruleIdx, r := range c.Rules() {
			if r.TargetNodeCount() != nil {
				if target := *r.TargetNodeCount(); target >= 0 {
					quotas = append(quotas, &TargetNodeCountQuota{
						id:              fmt.Sprintf("cc-min-nodes-%s-rule-%d", crdName, ruleIdx),
						crdName:         crdName,
						targetNodeCount: int64(target),
						ruleIdxStr:      strconv.Itoa(ruleIdx),
					})
				} else {
					klog.Warningf("Ignoring invalid TargetNodeCount %d for rule %d of CCC %s", target, ruleIdx, crdName)
				}
			}
		}
	}
	return quotas, nil
}

// TargetNodeCountQuota implements resourcequotas.Quota.
type TargetNodeCountQuota struct {
	id              string
	crdName         string
	targetNodeCount int64
	ruleIdxStr      string
}

// ID returns a unique quota identifier (CC name + optional rule index).
func (q *TargetNodeCountQuota) ID() string {
	return q.id
}

// AppliesTo checks if a node matches the quota's ComputeClass and optional priority rule labels.
func (q *TargetNodeCountQuota) AppliesTo(node *apiv1.Node) bool {
	if node == nil {
		return false
	}
	if node.Labels[labels.ComputeClassLabel] != q.crdName {
		return false
	}
	if q.ruleIdxStr != "" {
		return node.Annotations[labels.CCCPriorityIndexAnnotationKey] == q.ruleIdxStr
	}
	return true
}

// Limits returns the target node count as a 'nodes' resource limit for MinQuotasTracker.
func (q *TargetNodeCountQuota) Limits() map[string]int64 {
	return map[string]int64{
		resourcequotas.ResourceNodes: q.targetNodeCount,
	}
}
