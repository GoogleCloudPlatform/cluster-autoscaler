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

package estimator

import (
	"context"
	"time"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	oss_metrics "sigs.k8s.io/cluster-autoscaler/pkg/metrics"
)

// EstimationLimiterWrapper wraps an OSS estimator.EstimationLimiter to allow running
// custom GKE logic around estimation lifecycle methods (e.g. before StartEstimation starts
// the per-node-group estimation timer).
type EstimationLimiterWrapper struct {
	inner                        estimator.EstimationLimiter
	instanceAvailabilityProvider instanceavailability.Provider
	cccLister                    lister.Lister
	binpackerExpanderAwaitBudget *flexadvisor.BinpackerExpanderAwaitBudget
}

// NewEstimationLimiterWrapper creates a new EstimationLimiterWrapper around inner.
func NewEstimationLimiterWrapper(
	inner estimator.EstimationLimiter,
	instanceAvailabilityProvider instanceavailability.Provider,
	cccLister lister.Lister,
	binpackerExpanderAwaitBudget *flexadvisor.BinpackerExpanderAwaitBudget,
) *EstimationLimiterWrapper {
	return &EstimationLimiterWrapper{
		inner:                        inner,
		instanceAvailabilityProvider: instanceAvailabilityProvider,
		cccLister:                    cccLister,
		binpackerExpanderAwaitBudget: binpackerExpanderAwaitBudget,
	}
}

// StartEstimation runs pre-estimation logic before delegating to the inner EstimationLimiter.
func (w *EstimationLimiterWrapper) StartEstimation(ctx context.Context, podEquivalenceGroups []estimator.PodEquivalenceGroup, nodeGroup cloudprovider.NodeGroup, estimationContext estimator.EstimationContext) {
	// If nodeGroup references a CCC, consume the shared
	// binpackerExpanderAwaitBudget (default: 3s, shared with
	// FleetEfficiencyFilter) to await initial availability data at most
	// once per scale-up pass; subsequent estimations in the pass do not
	// wait. Wait time contributes only to the main binpacking time limit
	// (~5m) and not to the per-nodeGroup limit (7s).
	w.awaitScopeAvailabilityIfNeeded(ctx, nodeGroup)
	w.inner.StartEstimation(ctx, podEquivalenceGroups, nodeGroup, estimationContext)
}

func (w *EstimationLimiterWrapper) awaitScopeAvailabilityIfNeeded(ctx context.Context, nodeGroup cloudprovider.NodeGroup) {
	if nodeGroup == nil || w.instanceAvailabilityProvider == nil || w.cccLister == nil || !w.binpackerExpanderAwaitBudget.CanConsume() {
		return
	}
	// NodeGroupCrd only resolves CCC-backed scopes and returns "" for Pay-Per-Pod Autopilot
	// node groups. This is by design: Pay-Per-Pod scopes are only used for Fleet Efficiency
	// and we don't use FA recommendations for them in FlexAdvisor. This potentially could be improved in future
	// iterations of FA
	// TODO(b/579502607): Avoid calling NodeGroupCrd(nodeGroup) which does some allocations and could be replaced with cccLister.PodCrd(eg.Exemplar()) instead
	_, flexibilityScopeKey, err := w.cccLister.NodeGroupCrd(nodeGroup)
	if err != nil || flexibilityScopeKey == "" {
		return
	}
	timeout, ok := w.binpackerExpanderAwaitBudget.Consume()
	if !ok {
		return
	}
	if timeout <= 0 {
		klog.Warningf("FlexAdvisor: timeout=%v, not awaiting in bin packer, flexibilityScopeKey=%v", timeout, flexibilityScopeKey)
		return
	}

	waitStart := time.Now()
	err = w.instanceAvailabilityProvider.AwaitFlexibilityScopeWithTimeout(flexibilityScopeKey, timeout)
	if waitDuration := time.Since(waitStart); waitDuration > 100*time.Millisecond {
		oss_metrics.UpdateDuration(ctx, "FlexAdvisor:BinpackingAwaitInstanceAvailability", waitDuration)
	}
	if err != nil {
		klog.Warningf("FlexAdvisor: got err=%v when awaiting in binpacker flexibilityScopeKey=%v", err, flexibilityScopeKey)
	}
}

// EndEstimation delegates to the inner EstimationLimiter.
func (w *EstimationLimiterWrapper) EndEstimation() {
	w.inner.EndEstimation()
}

// PermissionToAddNode delegates to the inner EstimationLimiter.
func (w *EstimationLimiterWrapper) PermissionToAddNode(ctx context.Context) bool {
	return w.inner.PermissionToAddNode(ctx)
}
