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

package karpenter

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	cactx "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

// Orchestrator implements scaleup.Orchestrator by wrapping another scaleup.Orchestrator
// and routing to Karpenter-based scale-up simulation when enabled.
type Orchestrator struct {
	wrappedOrchestrator scaleup.Orchestrator
}

// NewOrchestrator returns a new Karpenter wrapper scale-up orchestrator.
func NewOrchestrator(wrappedOrchestrator scaleup.Orchestrator) *Orchestrator {
	return &Orchestrator{
		wrappedOrchestrator: wrappedOrchestrator,
	}
}

// Initialize initializes the wrapped orchestrator.
func (o *Orchestrator) Initialize(
	autoscalingContext *cactx.AutoscalingContext,
	processors *processors.AutoscalingProcessors,
	clusterStateRegistry *clusterstate.ClusterStateRegistry,
	estimatorBuilder estimator.EstimatorBuilder,
	taintConfig taints.TaintConfig,
	quotasTrackerFactory *resourcequotas.TrackerFactory,
) {
	o.wrappedOrchestrator.Initialize(autoscalingContext, processors, clusterStateRegistry, estimatorBuilder, taintConfig, quotasTrackerFactory)
}

// ScaleUp tries to scale the cluster up.
func (o *Orchestrator) ScaleUp(
	ctx context.Context,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
	allOrNothing bool,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	klog.V(1).Info("Karpenter is enabled")
	// TODO: Implement Karpenter-based scale-up logic here.

	return o.wrappedOrchestrator.ScaleUp(ctx, unschedulablePods, nodes, daemonSets, nodeInfos, allOrNothing)
}

// ScaleUpToNodeGroupMinSize delegates to the wrapped orchestrator's ScaleUpToNodeGroupMinSize.
func (o *Orchestrator) ScaleUpToNodeGroupMinSize(
	ctx context.Context,
	nodes []*apiv1.Node,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	return o.wrappedOrchestrator.ScaleUpToNodeGroupMinSize(ctx, nodes, nodeInfos)
}
