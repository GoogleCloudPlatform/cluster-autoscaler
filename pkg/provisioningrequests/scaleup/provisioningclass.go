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

package scaleup

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate"
	cactx "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/scaleup/orchestrator"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/status"
	"sigs.k8s.io/cluster-autoscaler/pkg/provisioningrequest/provreqclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/resourcequotas"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

// QueuedProvisioningClass wraps orchestrator to implement OSS ProvisioningClass interface.
type QueuedProvisioningClass struct {
	orchestrator *Orchestrator
}

func (q *QueuedProvisioningClass) Initialize(
	ctx *cactx.AutoscalingContext,
	processors *ca_processors.AutoscalingProcessors,
	clusterStateRegistry *clusterstate.ClusterStateRegistry,
	estimatorBuilder estimator.EstimatorBuilder,
	taintConfig taints.TaintConfig,
	injector *scheduling.HintingSimulator,
	quotasTrackerFactory *resourcequotas.TrackerFactory,
) {
	q.orchestrator.Initialize(ctx, processors, clusterStateRegistry, estimatorBuilder, taintConfig, quotasTrackerFactory)
}

func (q *QueuedProvisioningClass) Provision(
	ctx context.Context,
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	// Since all pods come from one shard, it's enough to check Provisioning Class for first pod.
	provReq, err := q.orchestrator.prClient.ProvisioningRequest(context.TODO(), unschedulablePods[0].Namespace, unschedulablePods[0].OwnerReferences[0].Name)
	if err != nil {
		return scaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "Failed to get ProvisiningRequest owner from pod %s", unschedulablePods[0].Name))
	}
	if provReq.Spec.ProvisioningClassName != queuedwrapper.QueuedProvisioningClassName {
		return nil, nil
	}
	return q.orchestrator.ScaleUp(context.TODO(), unschedulablePods, nodes, daemonSets, nodeInfos, false)
}

func NewQueuedProvisioningClass(
	provider GkeCloudProvider,
	prClient *provreqclient.ProvisioningRequestClient,
	prCache *provreqcache.QueuedProvisioningCache,
	maxProvReqBinpackingDuration time.Duration,
	fastpathBinpackingEnabled bool,
	experimentsManager experiments.Manager,
	napResourceAnalyzerFunc estimator.EstimationAnalyserFunc,
) *QueuedProvisioningClass {
	scaleUpOrchestrator := orchestrator.New()
	orchestrator, _ := NewOrchestrator(scaleUpOrchestrator, provider, prClient, prCache, maxProvReqBinpackingDuration, fastpathBinpackingEnabled, experimentsManager, napResourceAnalyzerFunc).(*Orchestrator)
	return &QueuedProvisioningClass{orchestrator}
}
