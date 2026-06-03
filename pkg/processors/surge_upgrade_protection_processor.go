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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	kube "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/kubernetes"
	klog "k8s.io/klog/v2"
)

// SurgeUpgradeScaleDownNodeProcessor prevents scale down of nodes undergoing upgrade
// by filtering them out from candidate nodes and returning surge nodes as temporary nodes. It does
// this by listing UpgradeInfo CRD.
type SurgeUpgradeScaleDownNodeProcessor struct {
	fetcher kube.UpdateInfoFetcher
}

const (
	// UpgradeType represents a upgrade type of UpdateInfo
	UpgradeType = "Upgrade"
	// RepairType represents a repair type of UpdateInfo
	RepairType = "Repair"
)

// NewSurgeUpgradeScaleDownNodeProcessor configures and returns a SurgeUpgradeScaleDownNodeProcessor.
func NewSurgeUpgradeScaleDownNodeProcessor(fetcher kube.UpdateInfoFetcher) *SurgeUpgradeScaleDownNodeProcessor {
	return &SurgeUpgradeScaleDownNodeProcessor{fetcher: fetcher}
}

func (sup *SurgeUpgradeScaleDownNodeProcessor) filterSurgeNodes(nodes []*apiv1.Node, msg string) ([]*apiv1.Node, errors.AutoscalerError) {
	updateInfos, err := sup.fetcher.GetUpdateInfos()
	if err != nil {
		err = fmt.Errorf("error computing upgrade nodes: %v", err)
		return nil, errors.ToAutoscalerError(errors.ApiCallError, err)
	}

	nodesToFilterOut := make(map[string]bool)
	for _, updateInfo := range updateInfos {
		nodesToFilterOut[updateInfo.Spec.TargetNode] = true
		if updateInfo.Spec.SurgeNode != "" {
			nodesToFilterOut[updateInfo.Spec.SurgeNode] = true
		}
	}

	remainingNodes := []*apiv1.Node{}
	for _, node := range nodes {
		if _, found := nodesToFilterOut[node.Name]; !found {
			remainingNodes = append(remainingNodes, node)
		} else {
			klog.V(4).Infof("node %s filtered out from %s", node.Name, msg)
		}
	}

	return remainingNodes, nil
}

// GetPodDestinationCandidates returns nodes that potentially could harbor the pods that would become
// unscheduled after a scale down.
func (sup *SurgeUpgradeScaleDownNodeProcessor) GetPodDestinationCandidates(ctx *context.AutoscalingContext,
	nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return sup.filterSurgeNodes(nodes, "pod destination node candidates")
}

// GetScaleDownCandidates returns nodes that potentially could be scaled down.
func (sup *SurgeUpgradeScaleDownNodeProcessor) GetScaleDownCandidates(ctx *context.AutoscalingContext,
	nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return sup.filterSurgeNodes(nodes, "scale down node candidates")
}

// CleanUp is called at CA termination
func (sup *SurgeUpgradeScaleDownNodeProcessor) CleanUp() {
}
