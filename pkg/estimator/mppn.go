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

package estimator

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
	"k8s.io/klog/v2"
)

// mppnAnalysisFunc analyzes estimation result and based on it and cluster state
// adjusts max pods per node config of a node pool if it has not yet been created.
func (nrt *NapResourceTrimmer) mppnAnalysisFunc(clusterSnapshot clustersnapshot.ClusterSnapshot, nodeGroup cloudprovider.NodeGroup, newNodesWithPods map[string]bool, workloadAnalysisResult gkeprice.UserWorkloadClusterAnalysis) {
	mig := nodeGroup.(*gke.GkeMig)
	spec := mig.Spec()

	if spec == nil {
		return
	}

	var newNodeInfos []framework.NodeInfo
	for nodeName := range newNodesWithPods {
		nodeInfo, err := clusterSnapshot.GetNodeInfo(nodeName)
		if err != nil {
			klog.Errorf("Failed to get node info for node %v: %v", nodeName, err)
			continue
		}
		newNodeInfos = append(newNodeInfos, *nodeInfo)
	}
	resourceApprox, err := workloadAnalysisResult.GetPodResourceRequestApproximation(newNodeInfos)
	if err != nil {
		klog.Errorf("Could not get approximate resource requests: %v", err)
		return
	}

	estimatedMaxPodsPerNode := 0
	for _, nodeInfo := range newNodeInfos {
		podsPerNode := len(nodeInfo.Pods())
		approximateMaxFuturePodsPerNode := 0
		if shouldFuturePodsBeCalculated(&nodeInfo) {
			approximateMaxFuturePodsPerNode = getFuturePodsWithDefaultResources(nodeInfo, resourceApprox)
		}

		currentEstimatedMaxPodsPerNode := podsPerNode + approximateMaxFuturePodsPerNode
		if currentEstimatedMaxPodsPerNode > estimatedMaxPodsPerNode {
			estimatedMaxPodsPerNode = currentEstimatedMaxPodsPerNode
		}
	}

	mppn := spec.MaxPodsPerNode
	err = mig.SetMaxPodsPerNode(toValidMppnValue(estimatedMaxPodsPerNode, mppn))
	if err != nil {
		klog.Errorf("Can't overwrite node group max pods per node: %v", err)
	}
}

// toValidMppnValue returns  max pods per node value to be used for a new node pool, rounding up predicted pods
// (calculated based on the binpacking results and leftover space on nodes) to valid max pods per node value, not larger
// than initial max pods per node setting of a node pool.
func toValidMppnValue(predictedPods int, initialMppn int64) int64 {
	if predictedPods <= 32 {
		return 32
	}
	if predictedPods <= 64 {
		return 64
	}
	if predictedPods <= 110 || initialMppn == 110 {
		return 110
	}
	if predictedPods <= 128 {
		return 128
	}
	return 256
}
