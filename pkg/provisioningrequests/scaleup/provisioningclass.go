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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	cactx "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/orchestrator"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	ca_processors "k8s.io/autoscaler/cluster-autoscaler/processors"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/provisioningrequest/provreqclient"
	"k8s.io/autoscaler/cluster-autoscaler/resourcequotas"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	provreqcache "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/cache"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/queuedwrapper"
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
	unschedulablePods []*apiv1.Pod,
	nodes []*apiv1.Node,
	daemonSets []*appsv1.DaemonSet,
	nodeInfos map[string]*framework.NodeInfo,
) (*status.ScaleUpStatus, errors.AutoscalerError) {
	// Since all pods come from one shard, it's enough to check Provisioning Class for first pod.
	provReq, err := q.orchestrator.prClient.ProvisioningRequest(unschedulablePods[0].Namespace, unschedulablePods[0].OwnerReferences[0].Name)
	if err != nil {
		return scaleUpError(&status.ScaleUpStatus{}, errors.NewAutoscalerErrorf(errors.InternalError, "Failed to get ProvisiningRequest owner from pod %s", unschedulablePods[0].Name))
	}
	if provReq.Spec.ProvisioningClassName != queuedwrapper.QueuedProvisioningClassName {
		return nil, nil
	}
	return q.orchestrator.ScaleUp(unschedulablePods, nodes, daemonSets, nodeInfos, false)
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
