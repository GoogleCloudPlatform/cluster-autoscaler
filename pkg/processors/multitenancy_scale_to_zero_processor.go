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
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/metrics/filter"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/utils/systempods"
	klog "k8s.io/klog/v2"
)

// MultitenantScaleToZeroPodListProcessor is a PodListProcessor that is used to allow
// scaling a tenant or supervisor to 0 nodes if all the non-daemonset, scheduled
// and pending pods are in ignoredNamespaces of that tenant or supervisor.
// This is achieved by filtering out any such pods (if the conditions are met)
// so that they no longer trigger scale-up or block scale-down.
// Per tenant/supervisor scale-to-0 will only happen if the conditions are met continuously
// for the duration of gracePeriod.
type MultitenantScaleToZeroPodListProcessor struct {
	metricsFilter                     filter.MetricsFilter
	gracePeriod                       time.Duration
	systemPodsClassifier              systempods.Classifier
	tenantProcessors                  map[string]*ScaleToZeroPodListProcessor
	supervisorProcessor               *ScaleToZeroPodListProcessor
	experimentsManager                experiments.Manager
	backupScaleToZeroPodListProcessor *ScaleToZeroPodListProcessor
	clusterHash                       string
}

func NewMultitenantScaleToZeroPodListProcessor(metricsFilter filter.MetricsFilter, gracePeriod time.Duration, podsClassifier systempods.Classifier, experimentsManager experiments.Manager, clusterHash string) *MultitenantScaleToZeroPodListProcessor {
	return &MultitenantScaleToZeroPodListProcessor{
		metricsFilter:                     metricsFilter,
		gracePeriod:                       gracePeriod,
		systemPodsClassifier:              podsClassifier,
		tenantProcessors:                  make(map[string]*ScaleToZeroPodListProcessor),
		experimentsManager:                experimentsManager,
		backupScaleToZeroPodListProcessor: NewScaleToZeroPodListProcessor(metricsFilter, gracePeriod, podsClassifier),
		clusterHash:                       clusterHash,
	}
}

// Process verifies if the only pods in a tenant/supervisor (scheduled and pending) are
// daemonset or system pods. If this is true and has been true for the duration
// of grace period it filters out non-ds pods both from list of unschedulable
// pods and ClusterSnapshot for that tenant/supervisor
func (p *MultitenantScaleToZeroPodListProcessor) Process(context *context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	if p.isMultitenantScaleToZeroExpDisabled() {
		return p.backupScaleToZeroPodListProcessor.Process(context, unschedulablePods)
	}
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

	// construct a map of all tenants CA cares about for scale down to 0 by looking at pods and nodes
	// For each tenant also store its respective unschedulable pods and store the remaining pods as
	// supervisor unschedulable pods
	tenantToUnschedulablePods := tenantsFromNodes(nodeInfos)
	supervisorUnschedulablePods := groupUnschedulablePods(unschedulablePods, tenantToUnschedulablePods)
	klog.Infof("Found %d tenants", len(tenantToUnschedulablePods))
	var leftoverUnschedulablePods []*apiv1.Pod
	for tenantUID, pods := range tenantToUnschedulablePods {
		tenantProcessor := p.tenantScaleToZeroProcessor(tenantUID)
		klog.Infof("Checking for scale down to zero in tenant %s", tenantUID)
		tenantLeftoverUnschedulablePods, err := tenantProcessor.Process(context, pods)
		if err != nil {
			klog.Errorf("Encountered scale-to-0 error in tenant %s: %v", tenantUID, err)
		}
		leftoverUnschedulablePods = append(leftoverUnschedulablePods, tenantLeftoverUnschedulablePods...)
		multitenancy.ObserveTenantScaleToZero(tenantUID, tenantProcessor.podsFilteredOut)
	}

	supervisorProcessor := p.supervisorScaleToZeroProcessor()
	supervisorLeftoverUnschedulablePods, err := supervisorProcessor.Process(context, supervisorUnschedulablePods)
	if err != nil {
		klog.Errorf("Encountered scale-to-0 error in supervisor: %v", err)
	}
	leftoverUnschedulablePods = append(leftoverUnschedulablePods, supervisorLeftoverUnschedulablePods...)
	multitenancy.ObserveTenantScaleToZero(p.clusterHash, supervisorProcessor.podsFilteredOut)

	// remove any tenant processors that CA didn't see to avoid memory leaks.
	// it means that the tenant was deleted since a tenant at the very least
	// would have an unschedulable event-exporter pod (single replica pod)
	for existingTenant := range p.tenantProcessors {
		_, tenantExists := tenantToUnschedulablePods[existingTenant]
		if !tenantExists {
			delete(p.tenantProcessors, existingTenant)
		}
	}
	return leftoverUnschedulablePods, nil
}

func (p *MultitenantScaleToZeroPodListProcessor) Name() string {
	if p.isMultitenantScaleToZeroExpDisabled() {
		return p.backupScaleToZeroPodListProcessor.Name()
	}
	return "MultitenantScaleToZero"
}

// Drainable drains pods when scale to zero is triggered.
func (p *MultitenantScaleToZeroPodListProcessor) Drainable(ctx *drainability.DrainContext, pod *apiv1.Pod, node *framework.NodeInfo) drainability.Status {
	if p.isMultitenantScaleToZeroExpDisabled() {
		return p.backupScaleToZeroPodListProcessor.Drainable(ctx, pod, node)
	}
	if multitenancy.IsSupervisorPod(pod) {
		if p.supervisorProcessor == nil {
			return drainability.NewUndefinedStatus()
		}
		return p.supervisorProcessor.Drainable(ctx, pod, node)
	}

	tenantUID := pod.Labels[multitenancy.TenantUIDLabel]
	tenantProcessor, ok := p.tenantProcessors[tenantUID]
	if !ok {
		return drainability.NewUndefinedStatus()
	}
	return tenantProcessor.Drainable(ctx, pod, node)
}

func (p *MultitenantScaleToZeroPodListProcessor) isMultitenantScaleToZeroExpDisabled() bool {
	return !p.experimentsManager.EvaluateBoolFlagOrFailsafe(experiments.MultitenancyScaleToZeroProcessorFlag, false)
}

// tenantScaleToZeroProcessor creates a scale to zero processor for a tenant.
func (p *MultitenantScaleToZeroPodListProcessor) tenantScaleToZeroProcessor(tenantUID string) *ScaleToZeroPodListProcessor {
	processor, ok := p.tenantProcessors[tenantUID]
	if ok {
		return processor
	}

	filter := func(node *framework.NodeInfo) bool {
		return !multitenancy.NodeBelongsToTenant(node, tenantUID)
	}

	processor = newScaleToZeroPodListProcessorWithNodeFilter(
		p.metricsFilter, p.gracePeriod, p.systemPodsClassifier, filter,
	)
	p.tenantProcessors[tenantUID] = processor
	return processor
}

// supervisorScaleToZeroProcessor creates a scale to zero processor for a tenant.
func (p *MultitenantScaleToZeroPodListProcessor) supervisorScaleToZeroProcessor() *ScaleToZeroPodListProcessor {
	if p.supervisorProcessor != nil {
		return p.supervisorProcessor
	}
	p.supervisorProcessor = newScaleToZeroPodListProcessorWithNodeFilter(
		p.metricsFilter, p.gracePeriod, p.systemPodsClassifier, multitenancy.IsNonSupervisorNode,
	)
	return p.supervisorProcessor
}

func tenantsFromNodes(nodeInfos []*framework.NodeInfo) map[string][]*apiv1.Pod {
	tenantToUnschedulablePods := make(map[string][]*apiv1.Pod)
	for _, node := range nodeInfos {
		uid, ok := node.Node().Labels[multitenancy.TenantUIDLabel]
		if ok {
			tenantToUnschedulablePods[uid] = []*apiv1.Pod{}
		}
	}
	return tenantToUnschedulablePods
}

func groupUnschedulablePods(pods []*apiv1.Pod, tenantToUnschedulablePod map[string][]*apiv1.Pod) []*apiv1.Pod {
	var supervisorPods []*apiv1.Pod
	for _, pod := range pods {
		if multitenancy.IsSupervisorPod(pod) {
			supervisorPods = append(supervisorPods, pod)
			continue
		}
		uid := pod.Labels[multitenancy.TenantUIDLabel]
		tenantToUnschedulablePod[uid] = append(tenantToUnschedulablePod[uid], pod)
	}
	return supervisorPods
}
