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

package test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	priceexpander "k8s.io/autoscaler/cluster-autoscaler/expander/price"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/impostor"
)

type priceExpanderWrapper struct {
	priceExpander expander.Filter
}

func (p *priceExpanderWrapper) BestOption(expansionOptions []expander.Option, nodeInfos map[string]*framework.NodeInfo) *expander.Option {
	opts := p.priceExpander.BestOptions(expansionOptions, nodeInfos)
	if len(opts) == 0 {
		return nil
	}
	return &opts[0]
}

func buildStrategy(cluster *impostor.Cluster) expander.Strategy {
	return &priceExpanderWrapper{
		priceExpander: priceexpander.NewFilter(
			cluster.Provider(),
			priceexpander.NewSimplePreferredNodeProvider(cluster.NodeLister()),
			priceexpander.SimpleNodeUnfitness,
		),
	}
}

func TestExpanderPriceOSSBasicScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}
	type testCase struct {
		clusterSize      int
		pods             int
		millicpu         int64
		mem              int64
		expectedNodePool string
	}
	testCases := []testCase{
		{2, 1, 100, 100, "nap-n1-standard-1"},
		{2, 8, 100, 100, "nap-n1-standard-1"},
		{2, 1, 800, 2400, "nap-n1-standard-1"},
		{2, 8, 800, 2400, "nap-n1-standard-1"}, // cpu and mem under std1
		{2, 1, 900, 1000, "nap-n1-highcpu-2"},  // cpu over std1
		{2, 1, 900, 2600, "nap-n1-standard-2"}, // cpu over std1, mem over cpu2
		{2, 1, 900, 5000, "nap-n1-standard-2"},
		{2, 1, 900, 5500, "nap-n1-highmem-2"},   // mem over standard-2
		{2, 10, 900, 5000, "nap-n1-highmem-2"},  // 2 pods per node
		{2, 10, 900, 5400, "nap-n1-standard-2"}, // 1 pod per node, 1x mem under std2, 2x mem over mem2

		{3, 1, 100, 100, "nap-n1-highcpu-2"},
		{3, 8, 100, 100, "nap-n1-highcpu-2"},
		{3, 1, 800, 2400, "nap-n1-standard-2"},
		{3, 8, 800, 2400, "nap-n1-standard-2"},

		{7, 1, 900, 1000, "nap-n1-highcpu-4"},
		{7, 1, 900, 2600, "nap-n1-standard-4"},
		{7, 1, 900, 5500, "nap-n1-standard-4"},
		{7, 10, 900, 5500, "nap-n1-highmem-4"},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			initialNodes := cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
			cluster.FillUpNodesCompletely(initialNodes)
			strategy := buildStrategy(cluster)
			pods := Pods(tc.pods).WithName("test").WithCPUMilli(tc.millicpu).WithMemMiB(tc.mem).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if err != nil {
				t.Errorf("Expected nil error, got: %v", err)
			}
			bestOption := getBestOptionGroupName(option)
			if diff := cmp.Diff(tc.expectedNodePool, bestOption); diff != "" {
				debugInfo := fmt.Sprintf("cluster size: %v, pods: %v, milliCpu: %v, mem: %v\n", tc.clusterSize, tc.pods, tc.millicpu, tc.mem)
				t.Errorf("Mismatch for case #%d, diff (-want +got):\n%sDebug info:\n%s", idx, diff, debugInfo)
			}
		})
	}
}

func TestExpanderPriceOSSReserved(t *testing.T) {
	type testCase struct {
		clusterSize      int
		millicpu         int64
		mem              int64
		expectedNodePool string
	}
	testCases := []testCase{
		{2, 100, 2404, "nap-n1-standard-1"}, // mem just under std1 limit
		{2, 100, 2425, "nap-n1-standard-2"}, // mem over std1
		{2, 835, 1000, "nap-n1-standard-1"}, // cpu just under std1 limit
		{2, 840, 1000, "nap-n1-highcpu-2"},  // cpu over std1
		{2, 840, 1025, "nap-n1-standard-2"}, // cpu over std1, mem over cpu2
		{2, 840, 5402, "nap-n1-standard-2"}, // mem just under std2 limit
		{2, 840, 5430, "nap-n1-highmem-2"},  // mem over std2 limit
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
			strategy := buildStrategy(cluster)
			pods := Pods(1).WithName("test").WithCPUMilli(tc.millicpu).WithMemMiB(tc.mem).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			assert.NoError(t, err)
			bestOption := getBestOptionGroupName(option)
			assert.Equal(t, tc.expectedNodePool, bestOption,
				"cluster size %v, millicpu %v, mem %v",
				tc.clusterSize, tc.millicpu, tc.mem)
		})
	}
}

func TestExpanderPriceOSSGpuValidation(t *testing.T) {
	gpu := machinetypes.NvidiaTeslaP100

	testCases := []struct {
		clusterSize      int
		pods             int
		millicpu         int64
		mem              int64
		gpu              int64
		expectedNodePool string
	}{
		{2, 1, 100, 100, 1, "nap-n1-standard-1-gpu1"},       // 1 GPU needed
		{2, 1, 100, 100, 2, "nap-n1-standard-1-gpu2"},       // 2 GPU needed
		{2, 1, 100, 100, 3, "nap-n1-standard-1-gpu4"},       // 3 GPU needed, bumped to 4 (supported counts: 1,2,4)
		{2, 1, 100, 100, 4, "nap-n1-standard-1-gpu4"},       // 4 GPU needed
		{2, 1, 100, 100, 9, "-"},                            // invalid GPU count (>4)
		{2, 1, 100, 100, 16, "-"},                           // invalid GPU count (>4)
		{2, 1, 18 * 1000, 100, 1, "nap-n1-highcpu-32-gpu2"}, // 2 GPU needed. NvidiaTeslaP100 allows up to 16 CPUs per GPU.
		{2, 2, 18 * 1000, 100, 1, "nap-n1-highcpu-32-gpu2"}, // 2 GPU needed. NvidiaTeslaP100 allows up to 16 CPUs per GPU.
	}

	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("tc_%d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
			cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
			strategy := buildStrategy(cluster)
			pods := Pods(tc.pods).WithName("test").WithCPUMilli(tc.millicpu).WithMemMiB(tc.mem).WithGPU(tc.gpu).WithGpuType(gpu.Name()).Get()
			option, _, _ := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			bestOption := getBestOptionGroupName(option)
			assert.Equal(t, tc.expectedNodePool, bestOption)
		})
	}
}

func TestExpanderPriceOSSGpuMini(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}

	gpu := machinetypes.NvidiaTeslaP100

	testCases := []struct {
		clusterSize      int
		pods             int
		gpu              int64
		expectedNodePool string
	}{
		{2, 1, 0, "nap-n1-standard-1"},
		{2, 2, 0, "nap-n1-standard-1"},
		{3, 4, 0, "nap-n1-standard-2"},
		{7, 8, 0, "nap-n1-standard-4"},
		{2, 1, 1, "nap-n1-standard-1-gpu1"},
		{2, 2, 1, "nap-n1-standard-1-gpu1"},
		{3, 4, 1, "nap-n1-standard-1-gpu1"},
		{7, 8, 1, "nap-n1-standard-1-gpu1"},
		{2, 1, 2, "nap-n1-standard-1-gpu2"},
		{2, 2, 2, "nap-n1-standard-1-gpu2"},
		{3, 4, 2, "nap-n1-standard-1-gpu2"},
		{7, 8, 2, "nap-n1-standard-1-gpu2"},
		{2, 1, 3, "nap-n1-standard-1-gpu4"},
		{2, 2, 3, "nap-n1-standard-1-gpu4"},
		{3, 4, 3, "nap-n1-standard-1-gpu4"},
		{7, 8, 3, "nap-n1-standard-1-gpu4"},
		{2, 1, 4, "nap-n1-standard-1-gpu4"},
		{2, 2, 4, "nap-n1-standard-1-gpu4"},
		{3, 4, 4, "nap-n1-standard-1-gpu4"},
		{7, 8, 4, "nap-n1-standard-1-gpu4"},
	}

	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("tc_%d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
			cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
			strategy := buildStrategy(cluster)
			pods := Pods(tc.pods).WithName("gpu").WithCPUMilli(100).WithMemMiB(300).WithGPU(tc.gpu).WithGpuType(gpu.Name()).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption)
			}
		})
	}
}

func TestExpanderPriceOSSGpu2Cores(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}

	gpu := machinetypes.NvidiaTeslaP100

	testCases := []struct {
		clusterSize      int
		pods             int
		gpu              int64
		expectedNodePool string
	}{
		{2, 1, 1, "nap-n1-highcpu-2-gpu1"},
		{2, 2, 1, "nap-n1-highcpu-2-gpu1"},
		{3, 4, 1, "nap-n1-highcpu-2-gpu1"},
		{7, 8, 1, "nap-n1-highcpu-2-gpu1"},
		{2, 1, 2, "nap-n1-highcpu-2-gpu2"},
		{7, 8, 2, "nap-n1-highcpu-2-gpu2"},
		{2, 1, 4, "nap-n1-highcpu-2-gpu4"},
		{7, 8, 4, "nap-n1-highcpu-2-gpu4"},
	}

	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("tc_%d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
			cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
			strategy := buildStrategy(cluster)
			pods := Pods(tc.pods).WithName("gpu").WithCPUMilli(1500).WithMemMiB(1000).WithGPU(tc.gpu).WithGpuType(gpu.Name()).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption)
			}
		})
	}
}

func TestExpanderPriceOSSGpuMixed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}

	gpu := machinetypes.NvidiaTeslaP100

	testCases := []struct {
		clusterSize      int
		pods             int
		gpu              int64
		expectedNodePool string
	}{
		{2, 1, 1, "nap-n1-standard-1"}, // preferred 1 CPU, requested 1x1 GPUs
		{2, 2, 1, "nap-n1-standard-1"}, // preferred 1 CPU, requested 2x1 GPUs
		{3, 4, 1, "nap-n1-standard-2"}, // preferred 2 CPU, requested 4x1 GPUs
		{7, 8, 1, "nap-n1-standard-4"}, // preferred 4 CPU, requested 8x1 GPUs
		{2, 1, 4, "nap-n1-standard-1-gpu1"},
		{2, 2, 4, "nap-n1-standard-1-gpu1"},
		{3, 4, 4, "nap-n1-standard-1-gpu1"},
		{7, 1, 4, "nap-n1-standard-1-gpu1"},
		{7, 2, 4, "nap-n1-standard-1-gpu1"},
		{7, 4, 4, "nap-n1-standard-1-gpu1"},
		{7, 8, 4, "nap-n1-standard-1-gpu1"},
		{21, 1, 4, "nap-n1-standard-1-gpu1"},
		{21, 2, 4, "nap-n1-standard-1-gpu1"},
		{21, 4, 4, "nap-n1-standard-1-gpu1"},
		{21, 8, 4, "nap-n1-standard-1-gpu1"},
		{2, 1, 8, "nap-n1-standard-1-gpu1"},
		{2, 2, 8, "nap-n1-standard-1-gpu1"},
		{3, 2, 8, "nap-n1-standard-1-gpu1"},
		{3, 4, 8, "nap-n1-standard-1-gpu1"},
		{3, 8, 8, "nap-n1-standard-1-gpu1"},
		{7, 8, 8, "nap-n1-standard-1-gpu1"},
	}

	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("tc_%d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
			cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
			strategy := buildStrategy(cluster)
			pods1 := Pods(tc.pods).WithName("gpu").WithCPUMilli(500).WithMemMiB(1000).WithGPU(1).WithGpuType(gpu.Name()).Get()
			pods2 := Pods(tc.pods).WithName("gpu").WithCPUMilli(500).WithMemMiB(1000).WithGPU(tc.gpu - 1).WithGpuType(gpu.Name()).Get()
			pods := append(pods1, pods2...)
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption)
			}
		})
	}
}

func TestExpanderPriceOSSGpuExisting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}

	gpu := machinetypes.NvidiaTeslaP100

	testCases := []struct {
		clusterSize      int
		pods             int
		gpu              int64
		expectedNodePool string
	}{
		{2, 1, 1, "nap-n1-standard-1-gpu1"},
		{2, 2, 1, "nap-n1-standard-1-gpu1"},
		{2, 4, 1, "nap-n1-standard-1-gpu1"},
		{2, 8, 1, "nap-n1-standard-1-gpu1"},
		{2, 1, 2, "nap-n1-standard-1-gpu2"},
		{2, 2, 2, "nap-n1-standard-1-gpu2"},
		{2, 4, 2, "nap-n1-standard-1-gpu2"},
		{2, 8, 2, "nap-n1-standard-1-gpu2"},
	}

	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("tc_%d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
			cluster.AddOrScaleUpNodeGroup("n1-standard-1-gpu4", "n1-standard-1-gpu4", tc.clusterSize, true)
			strategy := buildStrategy(cluster)
			pods := Pods(tc.pods).WithName("gpu").WithCPUMilli(100).WithMemMiB(100).WithGPU(tc.gpu).WithGpuType(gpu.Name()).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption)
			}
		})
	}
}

func TestExpanderPriceOSSPodsLimitsMicro(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}
	type testCase struct {
		clusterSize      int
		pods             int
		expectedNodePool string
	}
	testCases := []testCase{
		{2, 50, "nap-n1-standard-1"},
		{2, 200, "nap-n1-standard-1"},
		{3, 50, "nap-n1-highcpu-2"},
		{3, 100, "nap-n1-highcpu-2"},
		{3, 200, "nap-n1-highcpu-2"},
		{7, 50, "nap-n1-highcpu-4"},
		{7, 100, "nap-n1-highcpu-4"},
		{7, 200, "nap-n1-highcpu-4"},
		{7, 400, "nap-n1-highcpu-4"}, // pod limit + tanh
		{21, 50, "nap-n1-highcpu-8"},
		{21, 100, "nap-n1-highcpu-8"},
		{21, 200, "nap-n1-highcpu-8"}, // pod limit + tanh
		{21, 400, "nap-n1-highcpu-4"}, // pod limit + tanh
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
			strategy := buildStrategy(cluster)
			pods := Pods(tc.pods).WithName("micro").WithCPUMilli(1).WithMemMiB(1).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption,
					"test #%v: cluster size %v, pods %v",
					idx, tc.clusterSize, tc.pods)
			}
		})
	}
}

func TestExpanderPriceOSSPodsLimitsMidi(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}
	type testCase struct {
		clusterSize      int
		pods             int
		expectedNodePool string
	}
	testCases := []testCase{
		{21, 50, "nap-n1-highcpu-8"},
		{21, 100, "nap-n1-highcpu-8"},
		{21, 200, "nap-n1-highcpu-8"},
		{21, 400, "nap-n1-highcpu-8"},
		{61, 50, "nap-n1-highcpu-16"},
		{61, 100, "nap-n1-highcpu-16"},
		{61, 300, "nap-n1-highcpu-8"}, // pod limit + tanh
		{61, 400, "nap-n1-highcpu-8"}, // pod limit + tanh
		{201, 50, "nap-n1-highcpu-32"},
		{201, 100, "nap-n1-highcpu-32"},
		{201, 200, "nap-n1-highcpu-16"}, // pod limit + tanh
		// {201, 400, "nap-n1-standard-32"}, // nap-n1-standard-1 x 25; pod limit + tanh
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
			strategy := buildStrategy(cluster)
			pods := Pods(tc.pods).WithName("midi").WithCPUMilli(50).WithMemMiB(50).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption,
					"test #%v: cluster size %v, pods %v",
					idx, tc.clusterSize, tc.pods)
			}
		})
	}
}

func TestExpanderPriceOSSAntiAffinity(t *testing.T) {
	type testCase struct {
		clusterSize       int
		existingNodeGroup string
		pods              int
		millicpu          int64
		mem               int64
		expectedNodePool  string
	}
	testCases := []testCase{
		{0, "n1-standard-1", 10, 100, 100, "nap-n1-standard-1"},
		{3, "n1-standard-1", 10, 100, 100, "nap-n1-highcpu-2"},
		{7, "n1-standard-1", 10, 100, 100, "nap-n1-highcpu-2"},
		{21, "n1-standard-1", 10, 100, 100, "nap-n1-highcpu-2"},
		{61, "n1-standard-1", 10, 100, 100, "nap-n1-highcpu-2"},
		{201, "n1-standard-1", 10, 100, 100, "nap-n1-highcpu-4"},
		{0, "n1-standard-1", 20, 100, 100, "nap-n1-standard-1"},
		{3, "n1-standard-1", 20, 100, 100, "nap-n1-standard-1"},
		{7, "n1-standard-1", 20, 100, 100, "nap-n1-standard-1"},
		{21, "n1-standard-1", 20, 100, 100, "nap-n1-highcpu-2"},
		{61, "n1-standard-1", 20, 100, 100, "nap-n1-highcpu-2"},
		{201, "n1-standard-1", 20, 100, 100, "nap-n1-highcpu-2"},
		{201, "n1-standard-1", 30, 100, 100, "nap-n1-highcpu-2"},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.AddOrScaleUpNodeGroup(tc.existingNodeGroup, tc.existingNodeGroup, tc.clusterSize, false)
			strategy := buildStrategy(cluster)
			pods := Pods(tc.pods).WithName("test").WithCPUMilli(tc.millicpu).WithMemMiB(tc.mem).Get()
			for _, pod := range pods {
				pod.Labels = map[string]string{
					"affinity_key": "a",
				}
				pod.Spec.Affinity = &apiv1.Affinity{
					PodAntiAffinity: &apiv1.PodAntiAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []apiv1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{"affinity_key": "a"},
								},
								TopologyKey: "kubernetes.io/hostname",
							},
						},
					},
				}
			}
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			assert.NoError(t, err)
			bestOption := getBestOptionGroupName(option)
			assert.Equal(t, tc.expectedNodePool, bestOption,
				"test #%v: cluster size %v, pods: %v, millicpu %v, mem %v, nodeCount: %v",
				idx, tc.clusterSize, tc.pods, tc.millicpu, tc.mem, option.NodeCount)
			assert.Equal(t, tc.pods, option.NodeCount,
				"test #%v: cluster size %v, pods: %v, millicpu %v, mem %v, nodeCount: %v",
				idx, tc.clusterSize, tc.pods, tc.millicpu, tc.mem, option.NodeCount)
		})
	}
}

func TestExpanderPriceOSSScalability2000Nodes30Pods(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}
	type testCase struct {
		pods             int
		expectedNodePool string
	}
	memRequest := float64(0.82 * 2638 / 30)
	cluster := impostor.NewCluster()
	strategy := buildStrategy(cluster)
	cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", 201, true)
	testCases := []testCase{
		{1, "nap-n1-highcpu-32"},
		{20, "nap-n1-highcpu-32"},
		{30, "nap-n1-highcpu-32"},
		{40, "nap-n1-highcpu-32"},
		{100, "nap-n1-highcpu-32"},
		{200, "nap-n1-highcpu-16"},
		{300, "nap-n1-highcpu-16"},
		{400, "nap-n1-highcpu-2"},
		{600, "n1-standard-1"}, // stays stable above that
		// {10000, "n1-standard-1"}, // disabled for performance reasons
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			pods := Pods(tc.pods).WithName("extra-pod").WithCPUMilli(0).WithMemMiB(int64(memRequest)).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption,
					"existing n1-standard-1 pool; test #%v: pods %v, new nodes %v", idx, tc.pods, option.NodeCount)
			}
		})
	}
	cluster.AddOrScaleUpNodeGroup("n1-highcpu-32", "n1-highcpu-32", 0, true)
	testCases2 := []testCase{
		{1, "n1-highcpu-32"},
		{20, "n1-highcpu-32"},
		{50, "n1-highcpu-32"},
		{100, "n1-highcpu-32"},
		{200, "n1-highcpu-32"},
		{500, "n1-highcpu-32"},
		{800, "n1-standard-1"}, // stays stable above that
		// {10000, "n1-standard-1"}, // disabled for performance reasons
	}
	for idx, tc := range testCases2 {
		t.Run(fmt.Sprintf("testcase2 %d", idx), func(t *testing.T) {
			pods := Pods(tc.pods).WithName("extra-pod").WithCPUMilli(0).WithMemMiB(int64(memRequest)).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption,
					"existing n1-highcpu-32 pool; test #%v: pods %v, new nodes %v", idx, tc.pods, option.NodeCount)
			}
		})
	}
}

func TestExpanderPriceOSSScalability30Cpu50Mem(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}
	type testCase struct {
		pods             int
		expectedNodePool string
	}
	cpuRequest := int64(30)
	memRequest := int64(50)
	cluster := impostor.NewCluster()
	strategy := buildStrategy(cluster)
	testCases := []testCase{
		{1, "nap-n1-standard-1"},
		{20, "nap-n1-standard-1"},
		{100, "nap-n1-standard-1"},
		{200, "nap-n1-standard-1"},
		{350, "nap-n1-standard-1"},
		{500, "nap-n1-standard-1"},
		// {1000, "nap-n1-standard-1"}, // pod limits hit; disabled for performance reasons
		// {5000, "nap-n1-standard-2"}, // pod limits hit; disabled for performance reasons
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			pods := Pods(tc.pods).WithName("extra-pod").WithCPUMilli(cpuRequest).WithMemMiB(memRequest).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption,
					"test #%v: pods %v, new nodes %v", idx, tc.pods, option.NodeCount)
			}
		})
	}
}

func TestExpanderPriceOSSMultiScalability(t *testing.T) {
	t.Skip("skipping scalability tests as they do not validate anything")
	type testCase struct {
		clusterSize           int
		defaultGroup          string
		autoscaleDefaultGroup bool
		batches               int
		pods                  int
		cpu                   int64
		mem                   int64
		expectedNodePool      string
	}
	testCases := []testCase{
		{3, "n1-standard-1", true, 30, 1, 100, 100, ""},
		{3, "n1-standard-1", false, 30, 1, 100, 100, ""},
		{3, "n1-standard-1", true, 30, 10, 100, 100, ""},
		{3, "n1-standard-1", false, 30, 10, 100, 100, ""},
		{3, "n1-standard-1", true, 30, 5, 0, 200, ""},
		{3, "n1-standard-1", false, 30, 5, 0, 200, ""},
		{3, "n1-standard-1", true, 30, 10, 0, 200, ""},
		{3, "n1-standard-1", false, 30, 10, 0, 200, ""},
		{3, "n1-standard-1", true, 30, 10, 500, 0, ""},
		{3, "n1-standard-1", true, 30, 50, 500, 0, ""},
		{3, "n1-standard-1", false, 30, 50, 500, 0, ""},
		{3, "n1-standard-1", true, 30, 10, 0, 500, ""},
		{3, "n1-standard-1", true, 30, 50, 0, 500, ""},
		{3, "n1-standard-1", true, 200, 1, 0, 100, ""},
		{3, "n1-standard-1", false, 200, 1, 0, 100, ""},
		{3, "n1-standard-1", true, 200, 1, 0, 1000, ""},
		{3, "n1-standard-1", false, 200, 1, 0, 1000, ""},
		{3, "n1-standard-1", true, 200, 5, 0, 1000, ""},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			cluster.AddOrScaleUpNodeGroup(tc.defaultGroup, tc.defaultGroup, tc.clusterSize, tc.autoscaleDefaultGroup)
			strategy := buildStrategy(cluster)
			pods := Pods(tc.pods).WithName("replicas").WithCPUMilli(tc.cpu).WithMemMiB(tc.mem).Get()
			groups := make(map[string]int)
			for i := 0; i < tc.batches; i++ {
				option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
				if !assert.NoError(t, err) {
					break
				}
				bestOption := getBestOptionGroupName(option)
				cluster.AddOrScaleUpNodeGroup(bestOption, bestOption, option.NodeCount, true)
				groupSize := groups[bestOption]
				groups[bestOption] = groupSize + option.NodeCount
			}
		})
	}
}

func TestExpanderPriceOSSDedicated(t *testing.T) {
	t.Skip("skipping performance tests as they do not validate anything")
	testCases := []struct {
		workloadsCount   int
		machineTypeCount int
		podsPerWorkload  int
		cpu              int64
		mem              int64
		gpu              int64
	}{
		{50, 1, 400, 30, 50, 0}, // groups to consider: 69, valid options: 69
		{1, 1, 4000, 30, 50, 0}, // groups to consider: 20, valid options: 20
		{1, 1, 1000, 800, 2000, 0},
		// {1, 1, 1000, 800, 2000, 1}, // disabled as it takes ~20s
		{10, 1, 100, 800, 2000, 0},
		{10, 1, 100, 800, 2000, 1},
		{50, 1, 100, 800, 2000, 0},
		{50, 1, 100, 800, 2000, 1},
		{100, 1, 100, 800, 2000, 0},
		// {100, 1, 100, 800, 2000, 1}, // disabled as it takes ~6s
	}

	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			strategy := buildStrategy(cluster)
			for i := 0; i < tc.workloadsCount; i++ {
				workload := fmt.Sprintf("workload%d", i)
				for j := 0; j < tc.machineTypeCount; j++ {
					cpuCount := 1 << uint(j)
					machineType := fmt.Sprintf("n1-standard-%d", cpuCount)
					name := fmt.Sprintf("%s-%s", workload, machineType)
					cluster.AddDedicatedNodeGroup(name, machineType, workload, tc.gpu)
				}
			}
			var allPods []*apiv1.Pod
			for i := 0; i < tc.workloadsCount; i++ {
				workload := fmt.Sprintf("workload%d", i)
				pods := Pods(tc.podsPerWorkload).WithName(workload + "-pod").WithCPUMilli(tc.cpu).WithMemMiB(tc.mem).WithGPU(tc.gpu).WithGpuType(machinetypes.DeprecatedDefaultGPU).DedicateForWorkload(workload).Get()
				allPods = append(allPods, pods...)
			}
			start := time.Now()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, allPods)
			if assert.NoError(t, err) {
				assert.Equal(t, tc.podsPerWorkload, len(option.Pods),
					"test #%v: workloads %v, machineTypeCounts %v, podsPerWorkload %v, new nodes %v",
					idx, tc.workloadsCount, tc.machineTypeCount, tc.podsPerWorkload, option.NodeCount)
			}
			assert.NotNil(t, option)
			elapsed := time.Since(start)
			log.Printf("test %d took %s", idx, elapsed)
		})
	}
}
