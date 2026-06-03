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

package gke

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/core/utils"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	kube_utils "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	klog "k8s.io/klog/v2"

	kube "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/updateinfos/apis/nodemanagement.gke.io/v1alpha1"
)

// NodeGroupFromNode gets the nodegroup that node belongs to.
type NodeGroupFromNode func(node *apiv1.Node) (cloudprovider.NodeGroup, error)

// SurgeUpgradeResourceTracker tracks the resources consumed by surge nodes during
// an upgrade
type SurgeUpgradeResourceTracker struct {
	processor              customresources.CustomResourcesProcessor
	allNodeLister          kube_utils.NodeLister
	fetcher                kube.UpdateInfoFetcher
	allNodesSnapshotByName map[string]*apiv1.Node
	surgeNodes             sets.Set[string]
}

// NewSurgeUpgradeResourceTracker returns a new SurgeResourceTracker
func NewSurgeUpgradeResourceTracker(processor customresources.CustomResourcesProcessor, allNodeLister kube_utils.NodeLister, fetcher kube.UpdateInfoFetcher) *SurgeUpgradeResourceTracker {
	return &SurgeUpgradeResourceTracker{
		processor:     processor,
		allNodeLister: allNodeLister,
		fetcher:       fetcher,
		surgeNodes:    make(sets.Set[string]),
	}
}

// SurgeNodesInMIG gets the number of surge nodes in the node group
func (s *SurgeUpgradeResourceTracker) SurgeNodesInMIG(mig *GkeMig) (int, error) {
	migUpdateInfos, err := s.fetcher.GetUpdateInfosForMig(mig.Id())
	if err != nil {
		return 0, fmt.Errorf("error computing upgrade nodes: %v", err)
	}

	migSurgeNodes := s.getSurgeNodesForUpdateInfos(migUpdateInfos)
	if len(migSurgeNodes) > 0 {
		klog.V(4).Infof("Returning %d surge nodes in node group %s", len(migSurgeNodes), mig.Id())
	}
	return len(migSurgeNodes), nil
}

// GetSurgeResources gets resources consumed by surge nodes.
func (s *SurgeUpgradeResourceTracker) GetSurgeResources(n NodeGroupFromNode) (map[string]int64, error) {
	surgeNodeNames := s.surgeNodes
	var surgeNodes []*apiv1.Node
	for _, name := range surgeNodeNames.UnsortedList() {
		if node, ok := s.allNodesSnapshotByName[name]; ok {
			surgeNodes = append(surgeNodes, node)
		}
	}

	cores, mem := calculateCoresAndMemForNodes(surgeNodes)
	customResources, err := calculateCustomResourcesForNodes(s.processor, surgeNodes, n)
	if err != nil {
		klog.Error(err)
		return nil, err
	}
	resourcesConsumed := map[string]int64{}
	resourcesConsumed[cloudprovider.ResourceNameCores] = cores
	resourcesConsumed[cloudprovider.ResourceNameMemory] = mem
	for resource, quantity := range customResources {
		if resource == "" {
			continue
		}
		resourcesConsumed[resource] = quantity
	}
	return resourcesConsumed, nil
}

// Refresh snapshots nodes and updateInfo when its called
func (s *SurgeUpgradeResourceTracker) Refresh() error {
	allNodes, err := s.allNodeLister.List()
	if err != nil {
		return fmt.Errorf("refreshing all nodes failed; %v", err)
	}

	s.allNodesSnapshotByName = make(map[string]*apiv1.Node)
	for _, node := range allNodes {
		s.allNodesSnapshotByName[node.Name] = node
	}

	err = s.fetcher.Refresh()
	if err != nil {
		return fmt.Errorf("refreshing updateinfos failed; %v", err)
	}
	updateInfos, err := s.fetcher.GetUpdateInfos()
	if err != nil {
		return fmt.Errorf("error getting update infos: %v", err)
	}

	surgeNodes := s.getSurgeNodesForUpdateInfos(updateInfos)
	s.surgeNodes.Clear()
	for _, node := range surgeNodes {
		s.surgeNodes.Insert(node.Name)
	}
	return nil
}

func (s *SurgeUpgradeResourceTracker) ExcludeFromTracking(node *apiv1.Node) bool {
	return s.surgeNodes.Has(node.Name)
}

func (s *SurgeUpgradeResourceTracker) getSurgeNodesForUpdateInfos(updateInfos []*v1alpha1.UpdateInfo) []*apiv1.Node {
	var result []*apiv1.Node

	for _, updateInfo := range updateInfos {
		specHasSurge := updateInfo.Spec.SurgeNode != ""
		targetAndSurgeDifferent := updateInfo.Spec.SurgeNode != updateInfo.Spec.TargetNode
		surgeNode, surgeNodeExists := s.allNodesSnapshotByName[updateInfo.Spec.SurgeNode]
		_, targetNodeExists := s.allNodesSnapshotByName[updateInfo.Spec.TargetNode]

		if specHasSurge && targetNodeExists && surgeNodeExists && targetAndSurgeDifferent {
			result = append(result, surgeNode)
		}
	}

	return result
}

func calculateCoresAndMemForNodes(nodes []*apiv1.Node) (int64, int64) {
	cores, mem := int64(0), int64(0)
	for _, node := range nodes {
		nodeCores, nodeMem := utils.GetNodeCoresAndMemory(node)
		cores += nodeCores
		mem += nodeMem
	}
	return cores, mem
}

func calculateCustomResourcesForNodes(processor customresources.CustomResourcesProcessor,
	nodes []*apiv1.Node, ngFromNode NodeGroupFromNode) (map[string]int64, error) {
	resources := map[string]int64{}
	for _, node := range nodes {
		ng, err := ngFromNode(node)
		if err != nil {
			klog.Warningf("couldn't get node group from node; skipping node %s"+
				" for surge resource calculation; %v", node.Name, err)
			continue
		}

		resourceTargets, err := processor.GetNodeResourceTargets(nil, node, ng)
		if err != nil {
			// Can happen in cases where gpu is committed to a node from a
			// non autoscaled node group which is:
			// 1. The only gpu node in the node group and
			// 2. GPU label is missing on it since GPU drivers are installing.
			klog.Warningf("failed to get custom resource targets for node;"+
				" skipping node %s for surge resource calculation; %v", node.Name, err)
			continue
		}
		for _, resourceTarget := range resourceTargets {
			resources[resourceTarget.ResourceType] += resourceTarget.ResourceCount
		}
	}
	return resources, nil
}
