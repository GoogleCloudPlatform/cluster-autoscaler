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
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/pods"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/provisioningrequests/scaleup/reasons"
	klog "k8s.io/klog/v2"
)

type filterQueuedNodeGroupListProcessor struct {
	nodeGroupListProcessor nodegroups.NodeGroupListProcessor
}

// NewFilterQueuedNodeGroupListProcessor creates an instance of nodeGroupListProcessor and filters out all nodegroups that are queued.
func NewFilterQueuedNodeGroupListProcessor(nodeGroupListProcessor nodegroups.NodeGroupListProcessor) *filterQueuedNodeGroupListProcessor {
	return &filterQueuedNodeGroupListProcessor{
		nodeGroupListProcessor: nodeGroupListProcessor,
	}
}

// Process first calls nodeGroupListProcessor and then filters out all nodegroups that are queued.
func (p *filterQueuedNodeGroupListProcessor) Process(ctx *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	nodeGroups, nodeInfos, err := p.nodeGroupListProcessor.Process(ctx, nodeGroups, nodeInfos, unschedulablePods)
	if err != nil {
		klog.Errorf("Cannot filter DWS nodegroups, error: %v", err)
		return nodeGroups, nodeInfos, err
	}
	if len(unschedulablePods) == 0 {
		return nodeGroups, nodeInfos, nil
	}
	// This check relies on the fact that all pods come from the same shard and we can infer
	// if the whole shard comes from the Provisioning Request or no based on the first pod.
	if _, found := pods.ProvisioningRequestName(unschedulablePods[0]); found {
		klog.V(4).Infof("Pods are consuming provisioning requests not filtering out queued nodepools")
		return nodeGroups, nodeInfos, nil
	}

	filteredNodeGroups := make([]cloudprovider.NodeGroup, 0, len(nodeGroups))
	for _, nodeGroup := range nodeGroups {
		if reasons.IsNodeGroupQueued(nodeGroup) {
			continue
		}
		filteredNodeGroups = append(filteredNodeGroups, nodeGroup)
	}
	return filteredNodeGroups, nodeInfos, nil
}

// CleanUp cleans up the processor's internal structures.
func (p *filterQueuedNodeGroupListProcessor) CleanUp() {
	p.nodeGroupListProcessor.CleanUp()
}
