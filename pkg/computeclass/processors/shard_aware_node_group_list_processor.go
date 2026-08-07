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
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroups"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd/ccc"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/klog/v2"
)

type shardAwareNodeGroupListProcessor struct {
	nodeGroupListProcessor nodegroups.NodeGroupListProcessor
	lister                 lister.Lister
}

func NewShardAwareNodeGroupListProcessor(nodeGroupListProcessor nodegroups.NodeGroupListProcessor, l lister.Lister) *shardAwareNodeGroupListProcessor {
	return &shardAwareNodeGroupListProcessor{
		nodeGroupListProcessor: nodeGroupListProcessor,
		lister:                 l,
	}
}

func (p *shardAwareNodeGroupListProcessor) Process(autoscalingContext *context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo, unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	nodeGroups, nodeInfos, err := p.nodeGroupListProcessor.Process(autoscalingContext, nodeGroups, nodeInfos, unschedulablePods)
	if err != nil {
		return nodeGroups, nodeInfos, err
	}
	filteredNodeGroups := p.filterNodeGroupsByCccShardHomogeneity(nodeGroups, unschedulablePods)
	return filteredNodeGroups, nodeInfos, nil
}

func (p *shardAwareNodeGroupListProcessor) CleanUp() {
	p.nodeGroupListProcessor.CleanUp()
}

func (p *shardAwareNodeGroupListProcessor) filterNodeGroupsByCccShardHomogeneity(nodeGroups []cloudprovider.NodeGroup, unschedulablePods []*apiv1.Pod) []cloudprovider.NodeGroup {
	if len(unschedulablePods) == 0 {
		return nodeGroups
	}

	nodeSelectorTargetCCCName := p.getPodCccName(unschedulablePods[0])
	isHomogeneous := true

	for _, pod := range unschedulablePods[1:] {
		if p.getPodCccName(pod) != nodeSelectorTargetCCCName {
			isHomogeneous = false
			break
		}
	}

	if !isHomogeneous {
		return nodeGroups
	}

	toleratedCccs, toleratesAllCccs := p.getToleratedCCCNodesMetadata(unschedulablePods)
	if nodeSelectorTargetCCCName == "" && toleratesAllCccs {
		return nodeGroups
	}
	klog.V(4).Infof("Filtering node groups to match homogeneous shard requirement, selected compute-class: %q", nodeSelectorTargetCCCName)
	var filteredNodeGroups []cloudprovider.NodeGroup
	for _, ng := range nodeGroups {
		ngCrd, ngCrdName, err := p.lister.NodeGroupCrd(ng)
		if err != nil {
			klog.Warningf("Cannot resolve CRD for node group %s due to error, not pruning: %v", ng.Id(), err)
			filteredNodeGroups = append(filteredNodeGroups, ng)
		}
		// node selector defined
		if nodeSelectorTargetCCCName != "" {
			if ngCrdName == nodeSelectorTargetCCCName {
				filteredNodeGroups = append(filteredNodeGroups, ng)
			}
		} else if p.cccNodeGroupMatchesPod(ng, ngCrd, ngCrdName, nodeSelectorTargetCCCName, toleratedCccs) {
			filteredNodeGroups = append(filteredNodeGroups, ng)
		}
	}
	return filteredNodeGroups
}

// cccNodeGroupMatchesPod does quick filtering based on taints and tolerations.
// Returns true when the CCC nodeGroup might match the pod.
// Returns false when the nodeGroup surely does not match the pod.
func (p *shardAwareNodeGroupListProcessor) cccNodeGroupMatchesPod(ng cloudprovider.NodeGroup, ngCrd crd.CRD, ngCrdName string, nodeSelectorTargetCCCName string, toleratedCccs map[string]bool) bool {
	isNodeGroupCCC := ngCrd != nil && ngCrd.CrdType() == ccc.CrdType
	if !isNodeGroupCCC {
		return true // missing node-selector, and non-ccc ng
	}
	// we do not check exact toleration's effect, as this is only pre-filter
	if toleratedCccs[ngCrdName] {
		return true
	}
	cccTaint := p.getCCCTaint(ng)
	if cccTaint == nil || cccTaint.Effect == apiv1.TaintEffectPreferNoSchedule {
		return true
	}
	klog.V(4).Infof("Filtering out the nodegroup %v from further processing, targetCCC for pods = %v", ng.Id(), nodeSelectorTargetCCCName)
	return false
}

func (p *shardAwareNodeGroupListProcessor) getPodCccName(pod *apiv1.Pod) string {
	c, name, err := p.lister.PodCrd(pod)
	if err != nil || c == nil || c.CrdType() != ccc.CrdType {
		return ""
	}
	return name
}

func (p *shardAwareNodeGroupListProcessor) getToleratedCCCNodesMetadata(pods []*apiv1.Pod) (tolerations map[string]bool, toleratedAllCccs bool) {
	crdLabels := p.getCrdLabelsMap()
	if len(crdLabels) == 0 {
		return nil, false
	}
	tolerations = make(map[string]bool)

	for _, pod := range pods {
		for _, toleration := range pod.Spec.Tolerations {
			if toleration.Key == "" && toleration.Operator == apiv1.TolerationOpExists {
				toleratedAllCccs = true
				continue
			}
			if !crdLabels[toleration.Key] {
				continue
			}
			if toleration.Operator == apiv1.TolerationOpExists {
				toleratedAllCccs = true
			} else if toleration.Operator == apiv1.TolerationOpEqual || toleration.Operator == "" {
				tolerations[toleration.Value] = true
			} else {
				klog.Warningf("Unsupported toleration operator %v for key %v", toleration.Operator, toleration.Key)
				tolerations[toleration.Value] = true
			}
		}
	}
	return
}

func (p *shardAwareNodeGroupListProcessor) getCrdLabelsMap() map[string]bool {
	labelsMap := make(map[string]bool)
	for _, label := range p.lister.Labels() {
		labelsMap[label] = true
	}
	return labelsMap
}

func (p *shardAwareNodeGroupListProcessor) getCCCTaint(nodeGroup cloudprovider.NodeGroup) *apiv1.Taint {
	crdLabels := p.getCrdLabelsMap()
	if len(crdLabels) == 0 {
		return nil
	}
	gkeMig, ok := nodeGroup.(*gke.GkeMig)
	if !ok {
		return nil
	}
	if gkeMig.Spec() == nil {
		return nil
	}
	for _, taint := range gkeMig.Spec().Taints {

		for crdLabels := range crdLabels {
			if taint.Key == crdLabels {
				return &taint
			}
		}
	}
	return nil
}
