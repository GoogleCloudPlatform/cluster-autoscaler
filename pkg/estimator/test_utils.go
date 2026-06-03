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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	test_util "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	volume "k8s.io/cloud-provider/volume/helpers"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
)

type staticClusterAnalyzer struct {
	analysis *staticClusterAnalysis
}

func (sa *staticClusterAnalyzer) Analyze(map[string]*framework.NodeInfo) (gkeprice.ClusterAnalysis, error) {
	return nil, nil
}

func (sa *staticClusterAnalyzer) AnalyzeUserWorkloadUse() (gkeprice.UserWorkloadClusterAnalysis, error) {
	return sa.analysis, nil
}

type staticStorageCalculator struct {
}

func (sc *staticStorageCalculator) CalculatePhysicalEphemeralStorageGiB(mig *gke.GkeMig, allocatableBytes int64) int64 {
	// physical ~= 60 * reserved , based on: {osDistribution: cos, architecture: amd64, nodeVersion: 1.28.2-gke.1157000}
	return int64(1.66 * float64(allocatableBytes/volume.GiB))
}

type staticClusterAnalysis struct {
	resourceApprox gkeprice.Resource
}

func (sa *staticClusterAnalysis) GetPodResourceRequestApproximation(nodeInfo []framework.NodeInfo) (gkeprice.Resource, error) {
	return sa.resourceApprox, nil
}

func createPods(numPods int, cpuMilli, mem, ephStorage int64) []*apiv1.Pod {
	var pods []*apiv1.Pod
	for i := 0; i < numPods; i++ {
		pods = append(pods, testPod(fmt.Sprintf("pod-%d", i), cpuMilli, mem, ephStorage, 0))
	}
	return pods
}

func createPodsWithGPU(numPods int, cpuMilli, mem, ephStorage, gpuCount int64) []*apiv1.Pod {
	var pods []*apiv1.Pod
	for i := 0; i < numPods; i++ {
		pods = append(pods, testPod(fmt.Sprintf("pod-%d", i), cpuMilli, mem, ephStorage, gpuCount))
	}
	return pods
}

func testPod(name string, cpuMilli, mem, ephStorage, gpuCount int64) *apiv1.Pod {
	pod := test_util.BuildTestPodWithEphemeralStorage(name, cpuMilli, mem, ephStorage)
	if gpuCount > 0 {
		test_util.RequestGpuForPod(pod, gpuCount)
	}
	return pod
}
