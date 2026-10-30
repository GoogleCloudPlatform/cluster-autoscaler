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
	"context"
	"fmt"
	"strconv"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	cc_lister "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
)

// RuleMinCapacityQuotaProvider provides quotas based on the rule-level min capacity
// (minimumCapacity.targetNodeCount) in ComputeClass CRDs.
type RuleMinCapacityQuotaProvider struct {
	ccLister           cc_lister.Lister
	experimentsManager experiments.Manager
}

// NewRuleMinCapacityQuotaProvider creates a new RuleMinCapacityQuotaProvider.
func NewRuleMinCapacityQuotaProvider(ccLister cc_lister.Lister, experimentsManager experiments.Manager) *RuleMinCapacityQuotaProvider {
	return &RuleMinCapacityQuotaProvider{
		ccLister:           ccLister,
		experimentsManager: experimentsManager,
	}
}

// Quotas generates RuleMinCapacityQuotas for valid non-negative rule-level min
// capacities in all ComputeClass CRDs.
func (p *RuleMinCapacityQuotaProvider) Quotas(ctx context.Context) ([]resourcequotas.Quota, error) {
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
		for ruleIdx, r := range c.Rules() {
			if r.TargetNodeCount() != nil {
				if target := *r.TargetNodeCount(); target >= 0 {
					quotas = append(quotas, &RuleMinCapacityQuota{
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

// RuleMinCapacityQuota implements resourcequotas.Quota.
type RuleMinCapacityQuota struct {
	id              string
	crdName         string
	targetNodeCount int64
	ruleIdxStr      string
}

// ID returns a unique quota identifier (CC name + rule index).
func (q *RuleMinCapacityQuota) ID() string {
	return q.id
}

// AppliesTo checks if a node matches the quota's ComputeClass and priority rule.
func (q *RuleMinCapacityQuota) AppliesTo(node *apiv1.Node) bool {
	if node == nil {
		return false
	}
	if node.Labels[labels.ComputeClassLabel] != q.crdName {
		return false
	}
	return node.Annotations[labels.CCCPriorityIndexAnnotationKey] == q.ruleIdxStr
}

// Limits returns the target node count as a 'nodes' resource limit for
// MinQuotasTracker.
func (q *RuleMinCapacityQuota) Limits() map[string]int64 {
	return map[string]int64{
		resourcequotas.ResourceNodes: q.targetNodeCount,
	}
}
