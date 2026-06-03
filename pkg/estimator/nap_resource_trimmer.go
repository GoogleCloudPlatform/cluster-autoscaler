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
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
	"k8s.io/klog/v2"
)

// subset of gce_cloudprovider.OsReservedCalculator needed for DiskSize analysis.
type storageCalculator interface {
	CalculatePhysicalEphemeralStorageGiB(mig *gke.GkeMig, allocatableBytes int64) int64
}

// NapResourceTrimmer analyses CA estimation results and based on them
// overrides max pods per node and/or disk size for not yet created node pools.
type NapResourceTrimmer struct {
	clusterAnalyser   gkeprice.ClusterAnalyzer
	storageCalculator storageCalculator
	autopilotEnabled  bool

	// TODO(b/322314237): !!! HACK !!! TO BE FIXED IN NEAR FUTURE !!!
	nodeInfos map[string]*framework.NodeInfo
}

func NewNapResourceTrimmer(a gkeprice.ClusterAnalyzer,
	storageCalculator storageCalculator,
	autopilotEnabled bool) *NapResourceTrimmer {
	return &NapResourceTrimmer{
		clusterAnalyser:   a,
		storageCalculator: storageCalculator,
		autopilotEnabled:  autopilotEnabled,
	}
}

// NapResourceAnalyzerFunc returns a function used for analysis of estimation result.
func (nrt *NapResourceTrimmer) NapResourceAnalyzerFunc() estimator.EstimationAnalyserFunc {
	return nrt.analysisChainFunc
}

func (nrt *NapResourceTrimmer) analysisChainFunc(clusterSnapshot clustersnapshot.ClusterSnapshot, nodeGroup cloudprovider.NodeGroup, newNodesWithPods map[string]bool) {
	if nodeGroup.Exist() || !nodeGroup.Autoprovisioned() {
		return
	}

	mig := nodeGroup.(*gke.GkeMig)

	// If either autopilot cluster or standard cluster but
	// the respective dynamic feature is enabled (only possible for managed node groups)
	_, nodeGroupDynamicBootDiskSizeEnabled := mig.Spec().Labels[gkelabels.NodeGroupDynamicBootDiskSizeEnabledLabelKey]
	shouldEstimateDiskSize := nrt.autopilotEnabled || nodeGroupDynamicBootDiskSizeEnabled
	_, nodeGroupDynamicMaxPodsPerNodeEnabled := mig.Spec().Labels[gkelabels.NodeGroupDynamicMaxPodsPerNodeEnabledLabelKey]
	shouldEstimateMppn := nodeGroupDynamicMaxPodsPerNodeEnabled

	if !shouldEstimateDiskSize && !shouldEstimateMppn {
		return
	}

	workloadAnalysisResult, err := nrt.clusterAnalyser.AnalyzeUserWorkloadUse()
	if err != nil {
		klog.Errorf("Could not perform cluster analysis: %v", err)
		return
	}

	if shouldEstimateDiskSize {
		nrt.diskSizeAnalysisFunc(clusterSnapshot, nodeGroup, newNodesWithPods, workloadAnalysisResult)
	}
	if shouldEstimateMppn {
		nrt.mppnAnalysisFunc(clusterSnapshot, nodeGroup, newNodesWithPods, workloadAnalysisResult)
	}
}
