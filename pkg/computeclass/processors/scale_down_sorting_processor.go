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
	"fmt"
	"math"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	klog "k8s.io/klog/v2"
)

// cloudProvider specifies the subset of cloudprovider.CloudProvider used by the processor.
type cloudProvider interface {
	NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error)
	GetAutoprovisioningDefaultFamily() machinetypes.MachineFamily
	IsAutopilotEnabled() bool
}

// crdScaleDownSortingProcessor ensures nodes with highest priority are scaled down last after nodes with lower priority
type crdScaleDownSortingProcessor struct {
	lister             lister.Lister
	cloudProvider      cloudProvider
	matcher            computeclass.Matcher
	priorityIndexCache map[string]int
}

func NewCrdScaleDownSortingProcessor(crdLister lister.Lister, cloudProvider cloudProvider) *crdScaleDownSortingProcessor {
	return &crdScaleDownSortingProcessor{
		lister:             crdLister,
		cloudProvider:      cloudProvider,
		matcher:            computeclass.NewMatcher(crdLister, cloudProvider),
		priorityIndexCache: make(map[string]int),
	}
}

// ScaleDownEarlierThan determines whether node1 should be scaled down before node2 & vice versa
// Also tries to keep the sorting process stable by making node2 only be scaled first
// Iff node1 doesn't have an associated Crd but node2 does or priority index of node2 is lower than that of node1
// This is done to not interfere with other sorters unnecessarily
func (p *crdScaleDownSortingProcessor) ScaleDownEarlierThan(node1, node2 *apiv1.Node) bool {

	crd1, nodeGroup1, err := p.getCrdForNode(node1)
	if err != nil {
		klog.Warningf("error when retrieving crd for node %v: %v", node1.Name, err.Error())
	}
	crd2, nodeGroup2, err := p.getCrdForNode(node2)
	if err != nil {
		klog.Warningf("error when retrieving crd for node %v: %v", node2.Name, err.Error())
	}

	if crd1 == nil && crd2 != nil {
		return true
	}
	if crd1 != nil && crd2 == nil || crd1 == nil && crd2 == nil {
		return false
	}

	return p.priorityIndex(nodeGroup1, crd1) > p.priorityIndex(nodeGroup2, crd2)
}

func (p *crdScaleDownSortingProcessor) priorityIndex(group cloudprovider.NodeGroup, crd crd.CRD) int {
	if idx, ok := p.priorityIndexCache[group.Id()]; ok {
		return idx
	}
	idx := p.priorityIndexNoCache(group, crd)
	p.priorityIndexCache[group.Id()] = idx
	return idx
}

func (p *crdScaleDownSortingProcessor) priorityIndexNoCache(group cloudprovider.NodeGroup, crd crd.CRD) int {
	if len(crd.Rules()) == 0 {
		return 0
	}
	if found, idx, _ := p.matcher.FirstMatchedRule(group, crd); found {
		return idx
	}
	return math.MaxInt
}

// ResetState resets internal state before every sorting.
func (p *crdScaleDownSortingProcessor) ResetState() {
	p.priorityIndexCache = make(map[string]int)
}

func (p *crdScaleDownSortingProcessor) getCrdForNode(node *apiv1.Node) (crd.CRD, cloudprovider.NodeGroup, error) {
	if node == nil {
		return nil, nil, fmt.Errorf("expected node; got %v", node)
	}
	nodeGroup, err := p.cloudProvider.NodeGroupForNode(node)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to retrieve node group for node %s: %v", node.Name, err)
	}
	if nodeGroup == nil {
		return nil, nil, fmt.Errorf("Node group for node %s not found", node.Name)
	}

	crd, crdName, err := p.lister.NodeGroupCrd(nodeGroup)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to retrieve Crd for node %s: %v", node.Name, err)
	}

	if crd == nil || crdName == "" {
		return nil, nodeGroup, nil
	}

	return crd, nodeGroup, nil
}
