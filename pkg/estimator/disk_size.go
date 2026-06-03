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
	"fmt"
	"strconv"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
	"k8s.io/klog/v2"
)

// SetNodeInfosMap
// TODO(b/322314237): temporary solution, to be removed
func (nrt *NapResourceTrimmer) SetNodeInfosMap(nodeInfos map[string]*framework.NodeInfo) {
	nrt.nodeInfos = nodeInfos
}

// diskSizeAnalysisFunc trims ephemeral storage to a value that fits all pending pods, taking into account approximate future pods
// Details: go/ap-dynamic-eph-storage
func (nrt *NapResourceTrimmer) diskSizeAnalysisFunc(clusterSnapshot clustersnapshot.ClusterSnapshot, nodeGroup cloudprovider.NodeGroup, newNodesWithPods map[string]bool, workloadAnalysisResult gkeprice.UserWorkloadClusterAnalysis) {
	mig := nodeGroup.(*gke.GkeMig)

	if _, exist := mig.Spec().Labels[gkelabels.BootDiskSizeLabelKey]; exist {
		return
	}

	if mig.Spec().LocalSSDConfig.EphemeralStorageOnLocalSsd(mig.Spec().MachineType) {
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

	var estimatedDiskSizeGiB int64 = 0
	for _, nodeInfo := range newNodeInfos {

		// requested storage for currently allocated pods, including daemonSets
		requestedPodsSize := nodeInfo.GetRequested().GetEphemeralStorage()

		// estimate storage required for future pods
		debugFuturePodsCalculation := ""
		if shouldFuturePodsBeCalculated(&nodeInfo) {
			futurePodsCount := int64(getFuturePodsWithDefaultResources(nodeInfo, resourceApprox))
			estimatedFuturePodsSize := futurePodsCount * resourceApprox.EphemeralStorage
			debugFuturePodsCalculation = fmt.Sprintf("futurePodsCount=%d, resourceApprox.EphemeralStorage=%v", futurePodsCount, resourceApprox.EphemeralStorage)
			requestedPodsSize += estimatedFuturePodsSize
		}

		// calculate physical size, based on allocatable
		physicalSizeGiB := nrt.storageCalculator.CalculatePhysicalEphemeralStorageGiB(mig, requestedPodsSize)

		klog.V(5).Infof("Dynamic storage estimation: requestedPodsSize: %v, physicalSizeGiB: %d, %s, migId: %s, NodeInfo: %v", requestedPodsSize, physicalSizeGiB, debugFuturePodsCalculation, mig.Id(), nodeInfo)

		if physicalSizeGiB > estimatedDiskSizeGiB {
			estimatedDiskSizeGiB = physicalSizeGiB
		}
	}

	// add buffor: min(5%, 100Gb) - account for slight underestimations of the heuristic above
	estimatedDiskSizeGiB += min(int64(float64(estimatedDiskSizeGiB)*0.05), 100)

	klog.V(5).Infof("estimated ephemeral storage: %s diskSize=%d", mig.Id(), estimatedDiskSizeGiB)

	if estimatedDiskSizeGiB < machinetypes.MinBootDiskSizeGBForNAP {
		estimatedDiskSizeGiB = machinetypes.MinBootDiskSizeGBForNAP
	}
	if estimatedDiskSizeGiB < mig.Spec().DiskSize {
		err := mig.SetDiskSize(estimatedDiskSizeGiB)
		if err != nil {
			klog.Errorf("Failed to set %s disk size: %v", mig.Id(), err)
			return
		}

		// TODO(b/322314237): to be replaced with: ea.ctx.TemplateNodesForGroups[nodeGroup.Id()].Node()...
		// the "gce/boot-disk-size" annotation is used for gce_price_model.NodePrice
		if nodeInfo, found := nrt.nodeInfos[nodeGroup.Id()]; found {
			if nodeInfo.Node().Annotations == nil {
				nodeInfo.Node().Annotations = make(map[string]string)
			}
			nodeInfo.Node().Annotations[gce.BootDiskSizeAnnotation] = strconv.FormatInt(estimatedDiskSizeGiB, 10)
		}
	}
}
