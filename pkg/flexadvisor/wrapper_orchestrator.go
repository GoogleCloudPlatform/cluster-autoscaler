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

package flexadvisor

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

type wrapperOrchestratorMetrics interface {
	RegisterFlexAdvisorBypassAttempt()
}

// WrapperOrchestrator wraps a scaleup.Orchestrator to manage FlexAdvisor per-pass
// limiter tracking and recommendations bypass lifecycle within a single execution layer.
type WrapperOrchestrator struct {
	wrappedOrchestrator scaleup.Orchestrator
	cccLister           lister.Lister
	bypassTracker       RecommendationsBypassTracker
	limiterTracker      ScaleUpLimiterTracker
	experimentsManager  experiments.Manager
	metrics             wrapperOrchestratorMetrics
}

type wrapperOrchestratorOption func(*WrapperOrchestrator)

func withWrapperOrchestratorMetrics(m wrapperOrchestratorMetrics) wrapperOrchestratorOption {
	return func(o *WrapperOrchestrator) {
		o.metrics = m
	}
}

// NewWrapperOrchestrator returns a new WrapperOrchestrator for FlexAdvisor.
func NewWrapperOrchestrator(
	wrappedOrchestrator scaleup.Orchestrator,
	cccLister lister.Lister,
	bypassTracker RecommendationsBypassTracker,
	limiterTracker ScaleUpLimiterTracker,
	experimentsManager experiments.Manager,
	opts ...wrapperOrchestratorOption,
) *WrapperOrchestrator {
	o := &WrapperOrchestrator{
		wrappedOrchestrator: wrappedOrchestrator,
		cccLister:           cccLister,
		bypassTracker:       bypassTracker,
		limiterTracker:      limiterTracker,
		experimentsManager:  experimentsManager,
		metrics:             metrics.Metrics,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Initialize initializes the wrapped orchestrator object with required fields.
func (o *WrapperOrchestrator) Initialize(
	autoscalingCtx *ca_context.AutoscalingContext,
	processors *ca_processors.AutoscalingProcessors,
	clusterStateRegistry *clusterstate.ClusterStateRegistry,
	estimatorBuilder estimator.EstimatorBuilder,
	taintConfig taints.TaintConfig,
	quotasTrackerFactory *resourcequotas.TrackerFactory,
) {
	o.wrappedOrchestrator.Initialize(autoscalingCtx, processors, clusterStateRegistry, estimatorBuilder, taintConfig, quotasTrackerFactory)
}

// ScaleUp resets the per-pass limiter tracker, installs recommendations bypass if the active scope
// has reached the blocked threshold, executes the wrapped ScaleUp pass, and updates bypass counters
// based on the final ScaleUpStatus.
func (o *WrapperOrchestrator) ScaleUp(
	ctx context.Context,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
	allOrNothing bool,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	if o.limiterTracker != nil {
		o.limiterTracker.Reset()
	}
	if o.bypassTracker != nil {
		o.bypassTracker.RemoveBypasses()
	}

	if !isFlexAdvisorRecommendationsBypassEnabled(o.experimentsManager) || o.bypassTracker == nil || o.cccLister == nil {
		return o.wrappedOrchestrator.ScaleUp(ctx, unschedulablePods, nodes, daemonSets, nodeInfos, allOrNothing)
	}

	var flexibilityScopeKey string
	if len(unschedulablePods) > 0 {
		// PodCrd defaults to GetDefaultCrd() if pod doesn't have CCC
		_, flexibilityScopeKey, _ = o.cccLister.PodCrd(unschedulablePods[0])
	}

	if flexibilityScopeKey != "" && o.bypassTracker.HasReachedBypassThreshold(flexibilityScopeKey) {
		klog.Infof("FlexAdvisor: flexibilityScopeKey=%v has reached bypass threshold, enabling recommendations bypass", flexibilityScopeKey)
		o.bypassTracker.PauseRecommendationsEnforcement(flexibilityScopeKey)
		if o.metrics != nil {
			o.metrics.RegisterFlexAdvisorBypassAttempt()
		}
	}

	scaleUpStatus, err := o.wrappedOrchestrator.ScaleUp(ctx, unschedulablePods, nodes, daemonSets, nodeInfos, allOrNothing)
	if err != nil {
		return scaleUpStatus, err
	}

	if scaleUpStatus == nil {
		return nil, nil
	}

	switch scaleUpStatus.Result {
	case status.ScaleUpSuccessful:
		if flexibilityScopeKey != "" {
			o.bypassTracker.ResetCounters([]string{flexibilityScopeKey})
		}
	case status.ScaleUpNoOptionsAvailable:
		if o.limiterTracker != nil {
			o.bypassTracker.MarkBlocked(o.limiterTracker.GetFlexibilityScopesConstrainedByFlexAdvisor())
		}
	}

	return scaleUpStatus, nil
}

// ScaleUpToNodeGroupMinSize delegates to the wrapped orchestrator.
func (o *WrapperOrchestrator) ScaleUpToNodeGroupMinSize(
	ctx context.Context,
	nodes []*apiv1.Node,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	return o.wrappedOrchestrator.ScaleUpToNodeGroupMinSize(ctx, nodes, nodeInfos)
}
