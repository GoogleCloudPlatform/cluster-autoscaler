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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/flexadvisor"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/instanceavailability"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	oss_metrics "sigs.k8s.io/cluster-autoscaler/pkg/metrics"
)

const defaultBinpackerBlockTimeBudget = 3 * time.Second

// EstimationLimiterWrapper wraps an OSS estimator.EstimationLimiter to allow running
// custom GKE logic around estimation lifecycle methods (e.g. before StartEstimation starts
// the per-node-group estimation timer).
type EstimationLimiterWrapper struct {
	inner                        estimator.EstimationLimiter
	instanceAvailabilityProvider instanceavailability.Provider
	cccLister                    lister.Lister
	experimentsManager           experiments.Manager
	scopeAwaitTimeoutUsed        bool
}

// NewEstimationLimiterWrapper creates a new EstimationLimiterWrapper around inner.
func NewEstimationLimiterWrapper(
	inner estimator.EstimationLimiter,
	instanceAvailabilityProvider instanceavailability.Provider,
	cccLister lister.Lister,
	experimentsManager experiments.Manager,
) *EstimationLimiterWrapper {
	return &EstimationLimiterWrapper{
		inner:                        inner,
		instanceAvailabilityProvider: instanceAvailabilityProvider,
		cccLister:                    cccLister,
		experimentsManager:           experimentsManager,
	}
}

// StartEstimation runs pre-estimation logic before delegating to the inner EstimationLimiter.
func (w *EstimationLimiterWrapper) StartEstimation(ctx context.Context, podEquivalenceGroups []estimator.PodEquivalenceGroup, nodeGroup cloudprovider.NodeGroup, estimationContext estimator.EstimationContext) {
	// If nodeGroup references CCC we wait binpackerBlockTimeout (default: 3sec) amount of time for recommendation to be fetched if it haven't been fetched yet
	// This wait is a best-effort and is applied only once per whole loop (estimations for subsequent node groups will not wait even if data is not available)
	// Wait time contributes only to main binpacking time limit and not to that specific nodeGroup limit (binpacking uses two limits, former is ~5 minutes, latter is 7 seconds per node group)
	w.awaitScopeAvailabilityIfNeeded(ctx, nodeGroup)
	w.inner.StartEstimation(ctx, podEquivalenceGroups, nodeGroup, estimationContext)
}

func (w *EstimationLimiterWrapper) awaitScopeAvailabilityIfNeeded(ctx context.Context, nodeGroup cloudprovider.NodeGroup) {
	if w.scopeAwaitTimeoutUsed || nodeGroup == nil || w.instanceAvailabilityProvider == nil || w.cccLister == nil || !flexadvisor.IsFlexAdvisorBinpackerBlockEnabled(w.experimentsManager) {
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
	w.scopeAwaitTimeoutUsed = true
	timeout := w.binpackerBlockTimeout()
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

func (w *EstimationLimiterWrapper) binpackerBlockTimeout() time.Duration {
	if w.experimentsManager == nil {
		return defaultBinpackerBlockTimeBudget
	}
	timeout := w.experimentsManager.EvaluateDurationSecondsFlagOrFailsafe(
		experiments.FlexAdvisorBinpackerBlockTimeBudgetFlag,
		defaultBinpackerBlockTimeBudget,
	)
	if timeout < 0 {
		return defaultBinpackerBlockTimeBudget
	}
	return timeout
}

// OnScaleUpEnd resets the per-scale-up-pass await budget.
func (w *EstimationLimiterWrapper) OnScaleUpEnd() {
	w.scopeAwaitTimeoutUsed = false
}

// EndEstimation delegates to the inner EstimationLimiter.
func (w *EstimationLimiterWrapper) EndEstimation() {
	w.inner.EndEstimation()
}

// PermissionToAddNode delegates to the inner EstimationLimiter.
func (w *EstimationLimiterWrapper) PermissionToAddNode(ctx context.Context) bool {
	return w.inner.PermissionToAddNode(ctx)
}
