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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce/localssdsize"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	testutils "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	gkepriceexpander "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/expander/gkeprice"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/impostor"
	testoptions "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/testutils"
)

func buildGKEStrategy(t *testing.T, cluster *impostor.Cluster) (expander.Strategy, gkepriceexpander.ClusterAnalyzer) {
	return buildGKEStrategyWithUpcomingChecker(t, cluster, &asyncnodegroups.MockAsyncNodeGroupStateChecker{IsUpcomingNodeGroup: map[string]bool{}})
}

func buildGKEStrategyWithUpcomingChecker(t *testing.T, cluster *impostor.Cluster, upcomingChecker asyncnodegroups.AsyncNodeGroupStateChecker) (expander.Strategy, gkepriceexpander.ClusterAnalyzer) {
	clusterAnalyzer := gkepriceexpander.NewGroupingClusterAnalyzer(cluster.Provider(), cluster.NodeLister(), cluster.PodLister(), nil)
	groupPenaltyChecker := gkepriceexpander.NewStaticRelaxedGroupPenaltyChecker(cluster.Provider().IsAutopilotEnabled())
	expanderStrategy, err := gkepriceexpander.NewStrategy(cluster.Provider(), cluster.NodeLister(), cluster.PodLister(), nil, groupPenaltyChecker, true, localssdsize.NewSimpleLocalSSDProvider(), upcomingChecker)
	assert.NoError(t, err)

	return expanderStrategy, clusterAnalyzer
}

func getBestOptionWithDetails(t *testing.T, cluster *impostor.Cluster, strategy expander.Strategy, analyzer gkepriceexpander.ClusterAnalyzer,
	pods []*apiv1.Pod) (*expander.Option, string, int64) {
	option, nodeInfos, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
	assert.NoError(t, err)
	preferredCpuCount := getPreferredCpuCount(t, analyzer, *option, nodeInfos)
	optionName := getBestOptionGroupName(option)
	return option, optionName, preferredCpuCount
}

func getPreferredCpuCount(t *testing.T, analyzer gkepriceexpander.ClusterAnalyzer, option expander.Option, nodeInfos map[string]*framework.NodeInfo) int64 {
	analysis, err := analyzer.Analyze(nodeInfos)
	assert.NoError(t, err)
	nodeInfo := nodeInfos[option.NodeGroup.Id()]
	assert.NotNil(t, nodeInfo)
	preferredCpuCount, err := analysis.GetPreferredCpuCount(option, nodeInfo)
	assert.NoError(t, err)
	return preferredCpuCount
}

func TestExpanderPriceGKEBasicScenarios(t *testing.T) {
	type testCase struct {
		clusterSize       int
		autoscaleExisting bool
		pods              int
		millicpu          int64
		mem               int64
		expectedNodePool  string
		enableAutopilot   bool
	}
	testCases := []testCase{
		{1, false, 1, 100, 100, "nap-n1-standard-1", false},
		{1, false, 8, 100, 100, "nap-n1-standard-1", false},
		{1, false, 1, 800, 2400, "nap-n1-standard-1", false},
		{1, false, 8, 800, 2400, "nap-n1-standard-2", false},
		{1, false, 1, 900, 1000, "nap-n1-standard-2", false}, // cpu over standard-1
		{1, false, 1, 900, 2600, "nap-n1-standard-2", false}, // cpu over standard-1, mem over highcpu-2
		{1, false, 1, 900, 5000, "nap-n1-highmem-2", false},
		{1, false, 10, 900, 5000, "nap-n1-highmem-2", false}, // 2 pods per node
		{1, false, 10, 900, 5400, "nap-n1-highmem-2", false}, // 1 pod per node, 2 pods mem over highmem-2
		{1, false, 1, 900, 5500, "nap-n1-highmem-2", false},  // mem over standard-2

		{4, true, 1, 100, 100, "nap-n1-standard-2", false},  // n1-standard-1 on reusability 0.5
		{4, false, 8, 100, 100, "nap-n1-standard-2", false}, // n1-standard-1 on reusability 0.5
		// {4, true, 8, 100, 100, "nap-n1-standard-2"},  // n1-standard-1
		{4, true, 1, 800, 2400, "n1-standard-1", false}, // n1-standard-1 on reusability 0.5

		{10, true, 8, 800, 2400, "nap-n1-standard-4", false},
		{10, true, 1, 900, 1000, "nap-n1-standard-2", false},
		{10, true, 1, 900, 2600, "nap-n1-standard-2", false},
		{10, true, 1, 900, 5000, "nap-n1-highmem-2", false},
		{10, true, 10, 900, 5000, "nap-n1-highmem-4", false},
		{10, true, 10, 900, 5400, "nap-n1-highmem-4", false},
		{10, true, 1, 900, 5500, "nap-n1-highmem-2", false},

		{1, false, 1, 100, 100, "nap-n1-standard-1", true},
		{1, false, 8, 100, 100, "nap-n1-standard-1", true},
		{1, false, 1, 800, 2400, "nap-n1-standard-1", true},
		{1, false, 8, 800, 2400, "nap-n1-standard-2", true},
		{1, false, 1, 900, 1000, "nap-n1-standard-2", true}, // cpu over standard-1
		{1, false, 1, 900, 2600, "nap-n1-standard-2", true}, // cpu over standard-1, mem over highcpu-2
		{1, false, 1, 900, 5000, "nap-n1-highmem-2", true},
		{1, false, 10, 900, 5000, "nap-n1-highmem-2", true},  // 2 pods per node
		{1, false, 10, 900, 5400, "nap-n1-standard-2", true}, // 1 pod per node, 2 pods mem over highmem-2
		{1, false, 1, 900, 5500, "nap-n1-highmem-2", true},   // mem over standard-2

		{4, true, 1, 100, 100, "nap-n1-standard-2", true},  // n1-standard-1 on reusability 0.5
		{4, false, 8, 100, 100, "nap-n1-standard-2", true}, // n1-standard-1 on reusability 0.5
		{4, true, 8, 100, 100, "nap-n1-standard-2", true},  // n1-standard-1
		{4, true, 1, 800, 2400, "nap-n1-standard-2", true}, // n1-standard-1 on reusability 0.5

		{10, true, 8, 800, 2400, "nap-n1-standard-4", true},
		{10, true, 1, 900, 1000, "nap-n1-standard-2", true},
		{10, true, 1, 900, 2600, "nap-n1-standard-2", true},
		{10, true, 1, 900, 5000, "nap-n1-highmem-2", true},
		{10, true, 10, 900, 5000, "nap-n1-highmem-4", true},
		{10, true, 10, 900, 5400, "nap-n1-highmem-4", true},
		{10, true, 1, 900, 5500, "nap-n1-highmem-2", true},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(tc.enableAutopilot))
			initialNodes := cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, tc.autoscaleExisting)
			cluster.FillUpNodesCompletely(initialNodes)
			strategy, analyzer := buildGKEStrategy(t, cluster)
			pods := Pods(tc.pods).WithName("test").WithCPUMilli(tc.millicpu).WithMemMiB(tc.mem).Get()
			option, optionName, preferredCpuCount := getBestOptionWithDetails(t, cluster, strategy, analyzer, pods)
			if diff := cmp.Diff(tc.expectedNodePool, optionName); diff != "" {
				debugInfo := fmt.Sprintf("cluster size: %v, pods: %v, prefferedCpuCount: %v, optionName: %v, optionNodeCount: %v\n", tc.clusterSize, tc.pods, preferredCpuCount, optionName, option.NodeCount)
				t.Errorf("Mismatch for case #%d, diff (-want +got):\n%sDebug info:\n%s", idx, diff, debugInfo)
			}
		})
	}
}

func TestExpanderPriceGKEReserved(t *testing.T) {
	type testCase struct {
		clusterSize       int
		existingNodeGroup string
		millicpu          int64
		mem               int64
		expectedNodePool  string
		enableAutopilot   bool
	}
	testCases := []testCase{
		{0, "n1-standard-1", 100, 2404, "n1-standard-1", false},     // mem just under std1 limit
		{0, "n1-standard-1", 100, 2425, "nap-n1-highmem-2", false},  // mem over std1
		{0, "n1-standard-1", 835, 1000, "n1-standard-1", false},     // cpu just under std1 limit
		{0, "n1-standard-1", 840, 1000, "nap-n1-standard-2", false}, // cpu over std1
		{0, "n1-highcpu-2", 1800, 1000, "n1-highcpu-2", false},      // cpu over std1, mem just under cpu2 limits
		{0, "n1-highcpu-2", 1800, 1025, "nap-n1-standard-2", false}, // cpu over std1, mem over cpu2
		{0, "n1-standard-2", 840, 5402, "n1-standard-2", false},     // mem just under std2 limit
		{0, "n1-standard-2", 840, 5430, "nap-n1-highmem-2", false},  // mem over std2 limit

		{0, "n1-standard-1", 100, 2404, "n1-standard-1", true},     // mem just under std1 limit
		{0, "n1-standard-1", 100, 2425, "nap-n1-highmem-2", true},  // mem over std1
		{0, "n1-standard-1", 835, 1000, "n1-standard-1", true},     // cpu just under std1 limit
		{0, "n1-standard-1", 840, 1000, "nap-n1-standard-2", true}, // cpu over std1
		{0, "n1-highcpu-2", 1800, 1000, "n1-highcpu-2", true},      // cpu over std1, mem just under cpu2 limits
		{0, "n1-highcpu-2", 1800, 1025, "nap-n1-standard-2", true}, // cpu over std1, mem over cpu2
		{0, "n1-standard-2", 840, 5402, "nap-n1-highmem-2", true},  // mem just under std2 limit but node group creation penalty is relaxed in AP
		{0, "n1-standard-2", 840, 5430, "nap-n1-highmem-2", true},  // mem over std2 limit
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(tc.enableAutopilot))
			initialNodes := cluster.AddOrScaleUpNodeGroup(tc.existingNodeGroup, tc.existingNodeGroup, tc.clusterSize, true)
			cluster.FillUpNodesCompletely(initialNodes)
			strategy, analyzer := buildGKEStrategy(t, cluster)
			pods := Pods(1).WithName("test").WithCPUMilli(tc.millicpu).WithMemMiB(tc.mem).Get()
			option, optionName, preferredCpuCount := getBestOptionWithDetails(t, cluster, strategy, analyzer, pods)
			assert.Contains(t, optionName, tc.expectedNodePool,
				"test #%v: cluster size %v, millicpu %v, mem %v, preferred cpus %v, nodeCount %v",
				idx, tc.clusterSize, tc.millicpu, tc.mem, preferredCpuCount, option.NodeCount)
		})
	}
}

func TestExpanderPriceGKEGpuValidation(t *testing.T) {
	gpu := machinetypes.NvidiaTeslaP100

	testCases := []struct {
		clusterSize      int
		pods             int
		millicpu         int64
		mem              int64
		gpu              machinetypes.PhysicalGpuCount
		expectedNodePool string
	}{
		{2, 1, 0.3 * 1000, 100, 1, "nap-n1-standard-1-gpu1"}, // 1 GPU needed
		{2, 1, 0.3 * 1000, 100, 2, "nap-n1-standard-1-gpu2"}, // 2 GPU needed
		{2, 1, 0.3 * 1000, 100, 3, "nap-n1-standard-1-gpu4"}, // 3 GPU needed, bumped to 4 (supported counts: 1,2,4)
		{2, 1, 0.3 * 1000, 100, 4, "nap-n1-standard-1-gpu4"}, // 4 GPU needed
		{2, 1, 0.3 * 1000, 100, 5, "-"},                      // invalid GPU count (>4)
		{2, 1, 0.3 * 1000, 100, 8, "-"},                      // invalid GPU count (>4)
		{2, 1, 18 * 1000, 100, 1, "nap-n1-highcpu-32-gpu2"},  // 2 GPU needed. NvidiaTeslaP100 allows up to 16 CPUs per GPU.
		{2, 2, 18 * 1000, 100, 1, "nap-n1-highcpu-32-gpu2"},  // 2 GPU needed. NvidiaTeslaP100 allows up to 16 CPUs per GPU.
	}

	for clusterType, isAutopilot := range map[string]bool{"standard": false, "autopilot": true} {
		for idx, tc := range testCases {
			t.Run(fmt.Sprintf("tc_%d_%v", idx, clusterType), func(t *testing.T) {
				cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(isAutopilot))
				cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
				cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
				strategy, _ := buildGKEStrategy(t, cluster)
				pods := Pods(tc.pods).WithName("gpu").WithCPUMilli(tc.millicpu).WithMemMiB(tc.mem).WithGPU(int64(tc.gpu)).WithGpuType(gpu.Name()).Get()
				option, _, _ := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption)
			})
		}
	}
}

func TestExpanderPriceGKEGpu2Cores(t *testing.T) {
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
		{4, 4, 1, "nap-n1-highcpu-2-gpu1"},
		{13, 8, 1, "nap-n1-highcpu-2-gpu1"},
		{2, 1, 2, "nap-n1-highcpu-2-gpu2"},
		{13, 8, 2, "nap-n1-highcpu-2-gpu2"},
		{2, 1, 4, "nap-n1-highcpu-2-gpu4"},
		{13, 8, 4, "nap-n1-highcpu-2-gpu4"},
	}

	for clusterType, isAutopilot := range map[string]bool{"standard": false, "autopilot": true} {
		for idx, tc := range testCases {
			t.Run(fmt.Sprintf("tc_%d_%v", idx, clusterType), func(t *testing.T) {
				cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(isAutopilot))
				initialNodes := cluster.AddOrScaleUpNodeGroup("n1-standard-1-gpu1", "n1-standard-1-gpu1", tc.clusterSize, false)
				cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
				cluster.FillUpNodesCompletely(initialNodes)
				strategy, _ := buildGKEStrategy(t, cluster)
				pods := Pods(tc.pods).WithName("gpu").WithCPUMilli(1500).WithMemMiB(1000).WithGPU(tc.gpu).WithGpuType(gpu.Name()).Get()
				option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
				if assert.NoError(t, err) {
					bestOption := getBestOptionGroupName(option)
					assert.Equal(t, tc.expectedNodePool, bestOption)
				}
			})
		}
	}
}

func TestExpanderPriceGKEGpuMixed(t *testing.T) {
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
		{2, 1, 1, "nap-n1-standard-1"},
		{2, 2, 1, "nap-n1-standard-1"},
		{4, 4, 1, "nap-n1-standard-2"},
		{13, 8, 1, "nap-n1-standard-4"},
		{2, 1, 4, "nap-n1-standard-1-gpu1"},
		{2, 2, 4, "nap-n1-standard-1-gpu1"},
		{4, 4, 4, "nap-n1-standard-1-gpu1"},
		{13, 1, 4, "nap-n1-standard-1-gpu1"},
		{13, 2, 4, "nap-n1-standard-1-gpu1"},
		{13, 4, 4, "nap-n1-standard-1-gpu1"},
		{13, 8, 4, "nap-n1-standard-1-gpu1"},
		{61, 1, 4, "nap-n1-standard-1-gpu1"},
		{61, 2, 4, "nap-n1-standard-1-gpu1"},
		{61, 4, 4, "nap-n1-standard-1-gpu1"},
		{61, 8, 4, "nap-n1-standard-1-gpu1"},
	}

	for clusterType, isAutopilot := range map[string]bool{"standard": false, "autopilot": true} {
		for idx, tc := range testCases {
			t.Run(fmt.Sprintf("tc_%d_%v", idx, clusterType), func(t *testing.T) {
				cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(isAutopilot))
				cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
				initialNodes := cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", tc.clusterSize, false)
				cluster.FillUpNodesCompletely(initialNodes)
				cluster.AddOrScaleUpNodeGroup("n1-standard-1-gpu1", "n1-standard-1-gpu1", 0, false)
				strategy, _ := buildGKEStrategy(t, cluster)
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
}

func TestExpanderPriceGKEGpuExisting(t *testing.T) {
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

	for clusterType, isAutopilot := range map[string]bool{"standard": false, "autopilot": true} {
		for idx, tc := range testCases {
			t.Run(fmt.Sprintf("tc_%d_%v", idx, clusterType), func(t *testing.T) {
				cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(isAutopilot))
				cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
				cluster.AddOrScaleUpNodeGroup("n1-standard-1-gpu4", "n1-standard-1-gpu4", tc.clusterSize, true)
				strategy, _ := buildGKEStrategy(t, cluster)
				pods := Pods(tc.pods).WithName("gpu").WithCPUMilli(100).WithMemMiB(100).WithGPU(tc.gpu).WithGpuType(gpu.Name()).Get()
				option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
				if assert.NoError(t, err) {
					bestOption := getBestOptionGroupName(option)
					assert.Equal(t, tc.expectedNodePool, bestOption)
				}
			})
		}
	}
}

// TestExpanderPriceGKEPodsLimitsMicro tests adding tens of very small pods into a small cluster.
//
// Original cluster size: <5 nodes, <140 cores
// New pod size: 1 mCPU, 1MiB
// New pods count: 50-400
//
// With a max_pods_per_node setting of 110 pods, we would ideally use n1-standard-1 machine.
// (110 * 1 mCPU = 110 mCPU, 110 * 1MiB = 100MiB)
//
// Expander is choosing machines bigger than n1-standard-1, which is sub-optimal.
// It happens because:
// * expander is not aware of the max_pods_per_node limit
// * we try to choose future-proof (bigger) machines in bigger clusters.
func TestExpanderPriceGKEPodsLimitsMicro(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}
	type testCase struct {
		clusterSize      int
		machineCores     int
		pods             int
		expectedNodePool string
		enableAutopilot  bool
	}
	testCases := []testCase{
		{1, 2, 50, "nap-n1-standard-1", false},
		{1, 2, 200, "nap-n1-standard-1", false},
		{2, 4, 50, "nap-n1-standard-2", false},
		{2, 4, 100, "nap-n1-standard-2", false},
		{2, 4, 200, "nap-n1-standard-2", false},
		{2, 16, 50, "nap-n1-standard-4", false},
		{2, 16, 100, "nap-n1-standard-4", false},
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{2, 16, 200, "nap-n1-standard-2", false},
		{2, 16, 400, "nap-n1-standard-2", false},

		{4, 32, 50, "nap-n1-standard-8", false},
		{4, 32, 100, "nap-n1-standard-8", false},
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{4, 32, 300, "nap-n1-standard-2", false},
		{4, 32, 400, "nap-n1-standard-2", false},

		{1, 2, 50, "nap-n1-standard-1", true},
		{1, 2, 200, "nap-n1-standard-1", true},
		{2, 4, 50, "nap-n1-standard-2", true},
		{2, 4, 100, "nap-n1-standard-2", true},
		{2, 4, 200, "nap-n1-standard-2", true},
		{2, 16, 50, "nap-n1-standard-4", true},
		{2, 16, 100, "nap-n1-standard-4", true},
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{2, 16, 200, "nap-n1-standard-2", true},
		{2, 16, 400, "nap-n1-standard-2", true},

		{4, 32, 50, "nap-n1-standard-8", true},
		{4, 32, 100, "nap-n1-standard-8", true},
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{4, 32, 300, "nap-n1-standard-2", true},
		{4, 32, 400, "nap-n1-standard-2", true},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			groupName := fmt.Sprintf("n1-standard-%d", tc.machineCores)
			initialNodes := cluster.AddOrScaleUpNodeGroup(groupName, groupName, tc.clusterSize, false)
			cluster.FillUpNodesCompletely(initialNodes)
			strategy, analyzer := buildGKEStrategy(t, cluster)
			pods := Pods(tc.pods).WithName("micro").WithCPUMilli(1).WithMemMiB(1).Get()
			option, optionName, _ := getBestOptionWithDetails(t, cluster, strategy, analyzer, pods)
			assert.Equal(t, tc.expectedNodePool, optionName,
				"test #%v: cluster size %v x %v cores, pods %v, nodes %v",
				idx, tc.clusterSize, tc.machineCores, tc.pods, option.NodeCount)
		})
	}
}

// TestExpanderPriceGKEPodsLimitsMidi tests adding tens of medium-sized pods into
// a medium-sized cluster.
//
// Original cluster size: <60 nodes, <4k cores
// New pod size: 50 mCPU, 50MiB
// New pods count: 50-400
//
// With a max_pods_per_node setting of 110 pods, we would ideally use n1-highcpu-8 machines.
// (110 * 50 mCPU = 5.5 CPU, 50 * 50MiB = 2.44GiB, n1-highcpu-8: 8 CPU, 7.2 GB)
//
// Expander is choosing machines bigger than n1-standard-8, which is sub-optimal.
// It happens because:
// * expander is not aware of the max_pods_per_node limit
// * we try to choose future-proof (bigger) machines in bigger clusters.
func TestExpanderPriceGKEPodsLimitsMidi(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}
	type testCase struct {
		clusterSize      int
		machineCores     int
		pods             int
		expectedNodePool string
		enableAutpilot   bool
	}
	testCases := []testCase{
		{4, 32, 50, "nap-n1-standard-8", false},
		{4, 32, 100, "nap-n1-standard-8", false}, // nap-n1-standard-8 for reusability 0.75
		{20, 64, 50, "nap-n1-standard-16", false},
		{20, 64, 100, "nap-n1-standard-16", false},
		{20, 64, 200, "nap-n1-standard-16", false},
		{20, 64, 400, "nap-n1-standard-16", false},
		{60, 64, 50, "nap-n1-standard-32", false},
		{60, 64, 100, "nap-n1-standard-32", false},
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{60, 64, 200, "nap-n1-standard-16", false},
		{60, 64, 600, "nap-n1-standard-1", false}, // nap-n1-standard-1 x 25; pod limit + tanh

		{4, 32, 50, "nap-n1-standard-8", true},
		{4, 32, 100, "nap-n1-standard-8", true}, // nap-n1-standard-8 for reusability 0.75
		{20, 64, 50, "nap-n1-standard-16", true},
		{20, 64, 100, "nap-n1-standard-16", true},
		{20, 64, 200, "nap-n1-standard-16", true},
		{20, 64, 400, "nap-n1-standard-8", true},
		{60, 64, 50, "nap-n1-standard-32", true},
		{60, 64, 100, "nap-n1-standard-32", true},
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{60, 64, 200, "nap-n1-standard-16", true},
		{60, 64, 400, "nap-n1-standard-8", true},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster()
			if tc.enableAutpilot {
				cluster = impostor.NewCluster(impostor.WithAutopilotEnabled(true))
			}
			groupName := fmt.Sprintf("n1-standard-%d", tc.machineCores)
			initialNodes := cluster.AddOrScaleUpNodeGroup(groupName, groupName, tc.clusterSize, false)
			cluster.FillUpNodesCompletely(initialNodes)
			strategy, analyzer := buildGKEStrategy(t, cluster)
			pods := Pods(tc.pods).WithName("midi").WithCPUMilli(50).WithMemMiB(50).Get()
			option, optionName, _ := getBestOptionWithDetails(t, cluster, strategy, analyzer, pods)
			assert.Equal(t, tc.expectedNodePool, optionName,
				"test #%v: cluster size %v x %v cores, pods %v, nodes %v",
				idx, tc.clusterSize, tc.machineCores, tc.pods, option.NodeCount)
		})
	}
}

func TestExpanderPriceGKEGpuMini(t *testing.T) {
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
		{2, 1, 0, "nap-n1-standard-1"},       // preferred 1 CPU, requested 1x0 GPUs
		{2, 2, 0, "nap-n1-standard-1"},       // preferred 1 CPU, requested 2x0 GPUs
		{4, 4, 0, "nap-n1-standard-2"},       // preferred 2 CPU, requested 4x0 GPUs
		{13, 8, 0, "nap-n1-standard-4"},      // preferred 4 CPU, requested 8x0 GPUs
		{2, 1, 1, "nap-n1-standard-1-gpu1"},  // preferred 1 CPU, requested 1x1 GPUs
		{2, 2, 1, "nap-n1-standard-1-gpu1"},  // preferred 1 CPU, requested 2x1 GPUs
		{4, 4, 1, "nap-n1-standard-1-gpu1"},  // preferred 2 CPU, requested 4x1 GPUs
		{13, 8, 1, "nap-n1-standard-1-gpu1"}, // preferred 4 CPU, requested 8x1 GPUs
		{2, 1, 2, "nap-n1-standard-1-gpu2"},  // preferred 1 CPU, requested 1x2 GPUs
		{2, 2, 2, "nap-n1-standard-1-gpu2"},  // preferred 1 CPU, requested 2x2 GPUs
		{4, 4, 2, "nap-n1-standard-1-gpu2"},  // preferred 2 CPU, requested 4x2 GPUs
		{13, 8, 2, "nap-n1-standard-1-gpu2"}, // preferred 4 CPU, requested 8x2 GPUs
		{2, 1, 3, "nap-n1-standard-1-gpu4"},  // preferred 1 CPU, requested 1x3 GPUs
		{2, 2, 3, "nap-n1-standard-1-gpu4"},  // preferred 1 CPU, requested 2x3 GPUs
		{4, 4, 3, "nap-n1-standard-1-gpu4"},  // preferred 2 CPU, requested 4x3 GPUs
		{13, 8, 3, "nap-n1-standard-1-gpu4"}, // preferred 4 CPU, requested 8x3 GPUs
		{2, 1, 4, "nap-n1-standard-1-gpu4"},  // preferred 1 CPU, requested 1x4 GPUs
		{2, 2, 4, "nap-n1-standard-1-gpu4"},  // preferred 1 CPU, requested 2x4 GPUs
		{4, 4, 4, "nap-n1-standard-1-gpu4"},  // preferred 2 CPU, requested 4x4 GPUs
		{13, 8, 4, "nap-n1-standard-1-gpu4"}, // preferred 4 CPU, requested 8x4 GPUs
	}

	for clusterType, isAutopilot := range map[string]bool{"standard": false, "autopilot": true} {
		for idx, tc := range testCases {
			t.Run(fmt.Sprintf("tc_%d_%v", idx, clusterType), func(t *testing.T) {
				cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(isAutopilot))
				cluster.Provider().SetResourceLimiter(cloudprovider.NewResourceLimiter(nil, map[string]int64{gpu.Name(): 9999}))
				groupName := "n1-standard-1"
				if tc.gpu > 0 {
					groupName = "n1-standard-1-gpu1"
				}
				initialNodes := cluster.AddOrScaleUpNodeGroup(groupName, groupName, tc.clusterSize, false)
				cluster.FillUpNodesCompletely(initialNodes)
				strategy, analyzer := buildGKEStrategy(t, cluster)
				pods := Pods(tc.pods).WithName("gpu").WithCPUMilli(100).WithMemMiB(300).WithGPU(tc.gpu).WithGpuType(gpu.Name()).Get()
				_, optionName, _ := getBestOptionWithDetails(t, cluster, strategy, analyzer, pods)
				assert.Equal(t, tc.expectedNodePool, optionName)
			})
		}
	}
}

// TestExpanderPriceGKEAntiAffinity tests adding medium sized pods into various sized clusters.
// The pods use anti-affinity to enforce a one-pod-per-node scenario.
//
// Original cluster size: variable
// New pod size: 100 mCPU, 100MiB
// New pods count: 50-400
//
// With a 1 pod per node, we use n1-standard-1 machines in small clusters (<100 cores).
// (1 * 100 mCpu = 100 mCpu, 1 * 100MiB = 100MiB)
// Arguably, for bigger clusters, bigger machines are acceptable as a future-proof solution.
// (Anti affinity does not block us from scheduling other pods on these nodes in the future).
func TestExpanderPriceGKEAntiAffinity(t *testing.T) {
	type testCase struct {
		clusterSize       int
		existingNodeGroup string
		pods              int
		expectedNodePool  string
		enableAutopilot   bool
	}
	testCases := []testCase{
		{0, "n1-standard-1", 10, "nap-n1-standard-1", false},
		{4, "n1-standard-1", 10, "nap-n1-standard-1", false},
		{12, "n1-standard-1", 10, "nap-n1-highcpu-2", false},
		{30, "n1-standard-1", 10, "nap-n1-highcpu-2", false},
		{70, "n1-standard-1", 10, "nap-n1-highcpu-2", false},
		// The cluster is big enough to trigger adding bigger machines even though
		// they're going to be underutilized for now.
		{400, "n1-standard-1", 10, "nap-n1-highcpu-2", false},

		{0, "n1-standard-1", 20, "nap-n1-standard-1", false},
		{4, "n1-standard-1", 20, "nap-n1-standard-1", false},
		{12, "n1-standard-1", 20, "nap-n1-standard-1", false},
		{30, "n1-standard-1", 20, "nap-n1-standard-1", false},
		{70, "n1-standard-1", 20, "nap-n1-standard-1", false},
		// The cluster is big enough to trigger adding bigger machines even though
		// they're going to be underutilized for now, but we're not doing it.
		// The scale-up has enough nodes for the suppress unfitness function to effectively
		// ignore preferred machine size.
		// Works as implemented, not necessarily as intended.
		{400, "n1-standard-1", 20, "nap-n1-standard-1", false},
		{400, "n1-standard-1", 30, "nap-n1-standard-1", false},

		{0, "n1-standard-1", 10, "nap-n1-standard-1", true},
		{4, "n1-standard-1", 10, "nap-n1-standard-1", true},
		{12, "n1-standard-1", 10, "nap-n1-standard-1", true},
		{30, "n1-standard-1", 10, "nap-n1-standard-1", true},
		{70, "n1-standard-1", 10, "nap-n1-standard-1", true},
		{400, "n1-standard-1", 10, "nap-n1-standard-1", true},

		{0, "n1-standard-1", 20, "nap-n1-standard-1", true},
		{4, "n1-standard-1", 20, "nap-n1-standard-1", true},
		{12, "n1-standard-1", 20, "nap-n1-standard-1", true},
		{30, "n1-standard-1", 20, "nap-n1-standard-1", true},
		{70, "n1-standard-1", 20, "nap-n1-standard-1", true},
		// The cluster is big enough to trigger adding bigger machines even though
		// they're going to be underutilized for now, but we're not doing it.
		// The scale-up has enough nodes for the suppress unfitness function to effectively
		// ignore preferred machine size.
		// Works as implemented, not necessarily as intended.
		{400, "n1-standard-1", 20, "nap-n1-standard-1", true},
		{400, "n1-standard-1", 30, "nap-n1-standard-1", true},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(tc.enableAutopilot))
			cluster.AddOrScaleUpNodeGroup(tc.existingNodeGroup, tc.existingNodeGroup, tc.clusterSize, false)
			strategy, _ := buildGKEStrategy(t, cluster)
			pods := Pods(tc.pods).WithName("test").WithCPUMilli(100).WithMemMiB(100).Get()
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
				"test #%v: cluster size %v, pods: %v, nodeCount: %v",
				idx, tc.clusterSize, tc.pods, option.NodeCount)
			assert.Equal(t, tc.pods, option.NodeCount,
				"test #%v: cluster size %v, pods: %v, nodeCount: %v",
				idx, tc.clusterSize, tc.pods, option.NodeCount)
		})
	}
}

// n1Standard1AllocatableMiB is the amount of allocatable memory in MiB on n1-standard-1 machine
const n1Standard1AllocatableMiB = float64(2638)

// nodeAlmostFullFraction is a reasonable fullness multipler when we don't want to fill up the node
// completely.
const nodeAlmostFullFraction = 0.82

// TestExpanderPriceGKEScalabilityMem tests Cluster Autoscaler in a scalability-like scenario, with
// pod shape skewed towards memory.
//
// Initial cluster:
//   - 1400 cores
//   - existing nodes are filled up with memory requests
//   - multiple preexisting autoscaled node pools
//
// Pod size:
//   - memory request tailored so that 30 pods would easily fit on a n1-standard-1 machine (72 MiB)
//   - 0 CPU request
//
// Pod count: variable
//
// The scenario tests that expander chooses:
//   - a decently big machine (since the cluster is already big)
//   - an existing node pool instead of creating a new one
//   - a node pool that decently matches the pods' shape (i.e. chooses standard over highcpu)
//
// With max_pods_per_node = 110, the biggest machine that we can optimally use is n1-standard-4
// (choosing from the existing node pools).
// (110 * 72 MiB = 7.55GB, n1-standard-4 capacity is 15GB, n1-standard-2 capacity is 7.5GB)
//
// Expander is choosing machines bigger than n1-standard-4, which is sub-optimal.
// It happens because:
//   - expander is not aware of the max_pods_per_node limit
//   - we try to choose future-proof (bigger) machines in bigger clusters.
//
// Expander sometimes chooses high mem machine for AP because the node pool creation penalty is relaxed for AP
func TestExpanderPriceGKEScalabilityMem(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}

	initialClusterCores := 1400
	podsPerNode := 30.
	memRequest := int64(nodeAlmostFullFraction * n1Standard1AllocatableMiB / podsPerNode)

	testCases := []struct {
		pods             int
		expectedNodePool string
		enableAutopilot  bool
	}{
		{1, "n1-standard-16", false},
		{20, "n1-standard-16", false},
		{100, "n1-standard-16", false},
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{500, "n1-standard-8", false}, // pod count limits hit
		// {1000, "n1-standard-1", false}, // disabled for performance reasons
		// {5000, "n1-standard-4", false},  // pod count limits hit; disabled for performance reasons

		{1, "nap-n1-highmem-16", true},
		{20, "nap-n1-highmem-16", true},
		{100, "nap-n1-highmem-16", true}, // node pool creation penalty is relaxed for AP
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{500, "nap-n1-highmem-4", true}, // pod count limits hit
		// {1000, "nap-n1-highmem-2", true}, // disabled for performance reasons
		// {5000, "nap-n1-highmem-2", true}, // pod count limits hit; disabled for performance reasons
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(tc.enableAutopilot))
			strategy, _ := buildGKEStrategy(t, cluster)
			existingPools := "std1;std2;std4;std8;cpu16;std16"
			cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", 0, true)
			cluster.AddOrScaleUpNodeGroup("n1-standard-2", "n1-standard-2", 0, true)
			cluster.AddOrScaleUpNodeGroup("n1-standard-4", "n1-standard-4", 0, true)
			cluster.AddOrScaleUpNodeGroup("n1-standard-8", "n1-standard-8", 0, true)
			cluster.AddOrScaleUpNodeGroup("n1-highcpu-16", "n1-highcpu-16", 0, true)
			cluster.AddOrScaleUpNodeGroup("n1-standard-16", "n1-standard-16", 0, true)
			newNodes := cluster.AddOrScaleUpNodeGroup("n1-standard-8", "n1-standard-8", initialClusterCores/8, false)
			cluster.FillUpNodesBasedOnRequest(newNodes, 0, memRequest)

			pods := Pods(tc.pods).WithName("extra-pod").WithCPUMilli(0).WithMemMiB(memRequest).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption,
					"existing %v pool; test #%v: pods %v, new nodes %v", existingPools, idx, tc.pods, option.NodeCount)
			}
		})
	}
}

// TestExpanderPriceGKEScalabilityMemNAP tests Node Autoprovisioning in a scalability-like scenario
// with pod shape skewed towards memory.
//
// Initial cluster:
//   - 1400 cores
//   - existing nodes are filled up with memory requests
//   - multiple preexisting autoscaled node pools
//
// Pod size:
//   - memory request tailored so that 30 pods would easily fit on a n1-standard-1 machine (72 MiB)
//   - 0 CPU request
//
// Pod count: variable
//
// The scenario tests that expander chooses:
//   - a decently big machine (since the cluster is big)
//   - an existing node pool instead of creating a new one
//   - a node pool that matches the pods' shape (i.e. chooses highmem over standard and highcpu)
//
// With max_pods_per_node = 110, the biggest machine we can optimally use is n1-highmem-2.
// (110 * 72 MiB = 7.55GB, n1-highmem-2 capacity is 13GB)
//
// Expander is choosing machines bigger than n1-highmem-2, which is sub-optimal.
// It happens because:
//   - expander is not aware of the max_pods_per_node limit
//   - we try to choose future-proof (bigger) machines in bigger clusters.
//
// Expander sometimes chooses high mem machine for AP because the node pool creation penalty is relaxed for AP
func TestExpanderPriceGKEScalabilityNAP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}

	initialClusterCores := 1400
	podsPerNode := 30.
	memRequest := int64(nodeAlmostFullFraction * n1Standard1AllocatableMiB / podsPerNode)

	testCases := []struct {
		pods             int
		expectedNodePool string
		enableAutopilot  bool
	}{
		{1, "nap-n1-highmem-16", false},
		{20, "nap-n1-highmem-16", false},
		{100, "nap-n1-highmem-16", false},
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{500, "n1-standard-8", false}, // pod count limits hit
		// {1000, "n1-standard-1", false}, // disabled for performance reasons
		// {5000, "n1-standard-4", false}, // pod limits hit; disabled for performance reasons

		{1, "nap-n1-highmem-16", true},
		{20, "nap-n1-highmem-16", true},
		{100, "nap-n1-highmem-16", true},
		// Machine size getting smaller with growing number of pods added is an artifact of
		// how GKE price expander's suppress unfitness function works. The more nodes we add,
		// the less we take into account the preferred machine size.
		{500, "nap-n1-highmem-4", true}, // pod count limits hit
		// {1000, "nap-n1-highmem-2", true}, // disabled for performance reasons
		// {5000, "nap-n1-highmem-2", true}, // pod limits hit; disabled for performance reasons
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(tc.enableAutopilot))
			strategy, _ := buildGKEStrategy(t, cluster)
			existingPools := "std1;std2;std4;std8;cpu16"
			cluster.AddOrScaleUpNodeGroup("n1-standard-1", "n1-standard-1", 0, true)
			cluster.AddOrScaleUpNodeGroup("n1-standard-2", "n1-standard-2", 0, true)
			cluster.AddOrScaleUpNodeGroup("n1-standard-4", "n1-standard-4", 0, true)
			cluster.AddOrScaleUpNodeGroup("n1-standard-8", "n1-standard-8", 0, true)
			cluster.AddOrScaleUpNodeGroup("n1-highcpu-16", "n1-highcpu-16", 0, true)
			newNodes := cluster.AddOrScaleUpNodeGroup("n1-standard-8", "n1-standard-8", initialClusterCores/8, false)
			cluster.FillUpNodesBasedOnRequest(newNodes, 0, memRequest)

			pods := Pods(tc.pods).WithName("extra-pod").WithCPUMilli(0).WithMemMiB(memRequest).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption,
					"existing %v pool; test #%v: pods %v, new nodes %v", existingPools, idx, tc.pods, option.NodeCount)
			}
		})
	}
}

// TestExpanderPriceGKEScalability30CPU50Mem tests Node Autoprovisioning in a scalability-like
// scenario with pod shape 30 mCPU and 50 MiB.
//
// Initial cluster: 0 nodes
//
// Pod size: 30 mCPU, 50 MiB
//
// Pod count: variable
//
// With max_pods_per_node = 110, the biggest machine we can optimally use is n1-standard-4.
// (110 * 30 mCPU = 3.3 CPU, 110 * 50 MiB = 5.8 GB)
func TestExpanderPriceGKEScalability30Cpu50Mem(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}

	testCases := []struct {
		pods             int
		expectedNodePool string
		enableAutopilot  bool
	}{
		{1, "nap-n1-standard-1", false},
		{20, "nap-n1-standard-1", false},
		{100, "nap-n1-standard-1", false},
		{200, "nap-n1-standard-2", false},
		{350, "nap-n1-standard-2", false},
		{500, "nap-n1-standard-4", false},
		// {1000, "nap-n1-standard-4", false}, // pod limits hit; disabled for performance reasons
		// {5000, "nap-n1-standard-2", false}, // pod limits hit; disabled for performance reasons

		{1, "nap-n1-standard-1", true},
		{20, "nap-n1-standard-1", true},
		{100, "nap-n1-standard-1", true},
		{200, "nap-n1-standard-2", true},
		{350, "nap-n1-standard-2", true},
		{500, "nap-n1-standard-4", true},
		// {1000, "nap-n1-standard-2", true}, // pod limits hit; disabled for performance reasons
		// {5000, "nap-n1-standard-2", true}, // pod limits hit; disabled for performance reasons
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(tc.enableAutopilot))
			strategy, _ := buildGKEStrategy(t, cluster)
			pods := Pods(tc.pods).WithName("extra-pod").WithCPUMilli(30).WithMemMiB(50).Get()
			option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
			if assert.NoError(t, err) {
				bestOption := getBestOptionGroupName(option)
				assert.Equal(t, tc.expectedNodePool, bestOption,
					"test #%v: pods %v, new nodes %v", idx, tc.pods, option.NodeCount)
			}
		})
	}
}

func TestExpanderPriceGKEMultiScalability(t *testing.T) {
	t.Skip("skipping scalability tests as they do not validate anything")
	testCases := []struct {
		clusterSize           int
		defaultGroup          string
		autoscaleDefaultGroup bool
		batches               int
		pods                  int
		cpu                   int64
		mem                   int64
		expectedNodePool      string
		enableAutopilot       bool
	}{
		{3, "n1-standard-1", true, 40, 1, 100, 100, "", false},
		{3, "n1-standard-1", false, 40, 1, 100, 100, "", false},
		{3, "n1-standard-1", true, 40, 10, 100, 100, "", false},
		{3, "n1-standard-1", false, 40, 10, 100, 100, "", false},
		{3, "n1-standard-1", true, 40, 5, 0, 200, "", false},
		{3, "n1-standard-1", false, 40, 5, 0, 200, "", false},
		{3, "n1-standard-1", true, 40, 10, 0, 200, "", false},
		{3, "n1-standard-1", false, 40, 10, 0, 200, "", false},
		{3, "n1-standard-1", true, 40, 1, 100, 0, "", false},
		{3, "n1-standard-1", false, 40, 1, 100, 0, "", false},
		{3, "n1-standard-1", true, 40, 10, 100, 0, "", false},
		{3, "n1-standard-1", false, 40, 10, 100, 0, "", false},
		{3, "n1-standard-1", true, 40, 10, 500, 0, "", false},
		{3, "n1-standard-1", true, 40, 50, 500, 0, "", false},
		{3, "n1-standard-1", false, 40, 50, 500, 0, "", false},
		{3, "n1-standard-1", true, 40, 10, 0, 500, "", false},
		{3, "n1-standard-1", true, 40, 50, 0, 500, "", false},
		{3, "n1-standard-1", true, 200, 1, 0, 100, "", false},
		{3, "n1-standard-1", false, 200, 1, 0, 100, "", false},
		{3, "n1-standard-1", true, 200, 1, 0, 1000, "", false},
		{3, "n1-standard-1", false, 200, 1, 0, 1000, "", false},
		{3, "n1-standard-1", true, 200, 5, 0, 1000, "", false},

		{3, "n1-highmem-2", true, 40, 1, 100, 100, "", false},
		{3, "n1-highmem-2", true, 40, 10, 100, 100, "", false},
		{3, "n1-highmem-2", true, 40, 1, 100, 0, "", false},
		{3, "n1-highmem-2", true, 40, 10, 100, 0, "", false},
		{3, "n1-highmem-2", true, 40, 10, 500, 0, "", false},
		{3, "n1-highmem-2", true, 40, 50, 500, 0, "", false},

		{3, "n1-highcpu-2", true, 40, 1, 100, 100, "", false},
		{3, "n1-highcpu-2", true, 40, 10, 100, 100, "", false},
		{3, "n1-highcpu-2", true, 40, 5, 0, 200, "", false},
		{3, "n1-highcpu-2", true, 40, 10, 0, 200, "", false},
		{3, "n1-highcpu-2", true, 40, 10, 0, 500, "", false},
		{3, "n1-highcpu-2", true, 40, 50, 0, 500, "", false},
		{3, "n1-highcpu-2", true, 200, 1, 0, 100, "", false},
		{3, "n1-highcpu-2", true, 200, 1, 0, 1000, "", false},
		{3, "n1-highcpu-2", true, 200, 5, 0, 1000, "", false},

		{3, "n1-standard-1", true, 40, 1, 100, 100, "", true},
		{3, "n1-standard-1", false, 40, 1, 100, 100, "", true},
		{3, "n1-standard-1", true, 40, 10, 100, 100, "", true},
		{3, "n1-standard-1", false, 40, 10, 100, 100, "", true},
		{3, "n1-standard-1", true, 40, 5, 0, 200, "", true},
		{3, "n1-standard-1", false, 40, 5, 0, 200, "", true},
		{3, "n1-standard-1", true, 40, 10, 0, 200, "", true},
		{3, "n1-standard-1", false, 40, 10, 0, 200, "", true},
		{3, "n1-standard-1", true, 40, 1, 100, 0, "", true},
		{3, "n1-standard-1", false, 40, 1, 100, 0, "", true},
		{3, "n1-standard-1", true, 40, 10, 100, 0, "", true},
		{3, "n1-standard-1", false, 40, 10, 100, 0, "", true},
		{3, "n1-standard-1", true, 40, 10, 500, 0, "", true},
		{3, "n1-standard-1", true, 40, 50, 500, 0, "", true},
		{3, "n1-standard-1", false, 40, 50, 500, 0, "", true},
		{3, "n1-standard-1", true, 40, 10, 0, 500, "", true},
		{3, "n1-standard-1", true, 40, 50, 0, 500, "", true},
		{3, "n1-standard-1", true, 200, 1, 0, 100, "", true},
		{3, "n1-standard-1", false, 200, 1, 0, 100, "", true},
		{3, "n1-standard-1", true, 200, 1, 0, 1000, "", true},
		{3, "n1-standard-1", false, 200, 1, 0, 1000, "", true},
		{3, "n1-standard-1", true, 200, 5, 0, 1000, "", true},

		{3, "n1-highmem-2", true, 40, 1, 100, 100, "", true},
		{3, "n1-highmem-2", true, 40, 10, 100, 100, "", true},
		{3, "n1-highmem-2", true, 40, 1, 100, 0, "", true},
		{3, "n1-highmem-2", true, 40, 10, 100, 0, "", true},
		{3, "n1-highmem-2", true, 40, 10, 500, 0, "", true},
		{3, "n1-highmem-2", true, 40, 50, 500, 0, "", true},

		{3, "n1-highcpu-2", true, 40, 1, 100, 100, "", true},
		{3, "n1-highcpu-2", true, 40, 10, 100, 100, "", true},
		{3, "n1-highcpu-2", true, 40, 5, 0, 200, "", true},
		{3, "n1-highcpu-2", true, 40, 10, 0, 200, "", true},
		{3, "n1-highcpu-2", true, 40, 10, 0, 500, "", true},
		{3, "n1-highcpu-2", true, 40, 50, 0, 500, "", true},
		{3, "n1-highcpu-2", true, 200, 1, 0, 100, "", true},
		{3, "n1-highcpu-2", true, 200, 1, 0, 1000, "", true},
		{3, "n1-highcpu-2", true, 200, 5, 0, 1000, "", true},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster(impostor.WithAutopilotEnabled(tc.enableAutopilot))
			initialNodes := cluster.AddOrScaleUpNodeGroup(tc.defaultGroup, tc.defaultGroup, tc.clusterSize, tc.autoscaleDefaultGroup)
			cluster.FillUpNodesCompletely(initialNodes)
			strategy, _ := buildGKEStrategy(t, cluster)
			groups := make(map[string]int)
			for i := 0; i < tc.batches; i++ {
				pods := Pods(tc.pods).WithName("replicas").WithCPUMilli(tc.cpu).WithMemMiB(tc.mem).Get()
				option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
				if !assert.NoError(t, err) {
					break
				}
				bestOption := getBestOptionGroupName(option)
				newNodes := cluster.AddOrScaleUpNodeGroup(bestOption, bestOption, option.NodeCount, true)
				cluster.FillUpNodesBasedOnRequest(newNodes, tc.cpu, tc.mem)
				groups[bestOption] += option.NodeCount
			}
		})
	}
}

func TestPodWithEphemeralStorageRequest(t *testing.T) {
	// TODO: test has broken assumptions, needs fixing
	t.Skip()
	// test expect to select the second option
	testCases := []struct {
		name              string
		podEph            int64
		diskSize1         int64 // allocatable
		diskType1         string
		localSsdCount1    int64
		ephStrOnLocalSsd1 bool
		// selected option
		diskSize2         int64 // allocatable
		diskType2         string
		localSsdCount2    int64
		ephStrOnLocalSsd2 bool
		err               bool
	}{
		{
			name:      "Pod ephemeral storage doesn't fit to one option",
			podEph:    150,
			diskSize1: 100,
			diskType1: "pd-ssd",
			// selected option
			diskSize2: 250,
			diskType2: "pd-ssd",
		},
		{
			name:      "Pod ephemeral storage fit to both options, the smallest is chosen",
			podEph:    50,
			diskSize1: 200,
			diskType1: "pd-ssd",
			// selected option
			diskSize2: 100,
			diskType2: "pd-ssd",
		},
		{
			name:      "Same disk size, the cheapest disk type is chosen",
			podEph:    50,
			diskSize1: 100,
			diskType1: "pd-ssd",
			// selected option
			diskSize2: 100,
			diskType2: "pd-standard",
		},
		{
			name:      "Pod ephemeral storage doesn't fit to any option",
			podEph:    200,
			diskSize1: 150,
			diskType1: "pd-ssd",
			// selected option
			diskSize2: 100,
			diskType2: "pd-ssd",
			err:       true,
		},
		{
			name:           "Same boot disk options, one option with local SSD, the option without local SSD is chosen",
			podEph:         50,
			diskSize1:      100,
			diskType1:      "pd-standard",
			localSsdCount1: 1,
			// selected option
			diskSize2: 100,
			diskType2: "pd-standard",
		},
		{
			name:              "Same boot disk options, one option with ephOnLocalSsd=true, the option without local SSD is chosen",
			podEph:            50,
			diskSize1:         100,
			diskType1:         "pd-standard",
			localSsdCount1:    1,
			ephStrOnLocalSsd1: true,
			// selected option
			diskSize2: 100,
			diskType2: "pd-standard",
		},
		{
			name:      "Pod ephemerals strorage request fit on local SSD only",
			podEph:    300,
			diskSize1: 100,
			diskType1: "pd-standard",
			// selected option
			diskSize2:         100,
			diskType2:         "pd-standard",
			localSsdCount2:    1,
			ephStrOnLocalSsd2: true,
		},
		{
			name:              "Ephemeral storage on local SSD, the cheapest local SSD option is chosen",
			podEph:            50,
			diskSize1:         50,
			diskType1:         "pd-standard",
			localSsdCount1:    2,
			ephStrOnLocalSsd1: true,
			// selected option
			diskSize2:         100,
			diskType2:         "pd-standard",
			localSsdCount2:    1,
			ephStrOnLocalSsd2: true,
		},
		{
			name:              "Ephemeral storage on local SSD, same local SSD options, the cheapest boot disk option is chosen",
			podEph:            50,
			diskSize1:         100,
			diskType1:         "pd-standard",
			localSsdCount1:    1,
			ephStrOnLocalSsd1: true,
			// selected option
			diskSize2:         50,
			diskType2:         "pd-standard",
			localSsdCount2:    1,
			ephStrOnLocalSsd2: true,
		},
		{
			name:      "Option with ephemeral storage on local SSD is cheaper",
			podEph:    300,
			diskSize1: 300,
			diskType1: "pd-ssd", // node price is 57
			// selected option
			diskSize2:         50,
			diskType2:         "pd-standard",
			localSsdCount2:    1, // node price is 32
			ephStrOnLocalSsd2: true,
		},
		{
			name:              "Option with ephemeral storage on boot disk is cheaper",
			podEph:            300,
			diskSize1:         50,
			diskType1:         "pd-ssd",
			localSsdCount1:    1, // node price is 38.5
			ephStrOnLocalSsd1: true,
			// selected option
			diskSize2: 300,
			diskType2: "pd-standard", // node price is 12
		},
	}
	machineType := "n1-standard-1"
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, enableAutopilot := range []bool{false, true} {
				cluster := impostor.NewCluster()
				if enableAutopilot {
					cluster = impostor.NewCluster(impostor.WithAutopilotEnabled(true))
				}
				cluster.EnableExpanderEphemeralStorageSupport()
				nodes := cluster.AddAndScaleUpNodeGroupWithEphemeralStorage(machineType, tc.diskType1, tc.diskSize1*units.GiB, tc.localSsdCount1, tc.ephStrOnLocalSsd1)
				nodes = append(nodes, cluster.AddAndScaleUpNodeGroupWithEphemeralStorage(machineType, tc.diskType2, tc.diskSize2*units.GiB, tc.localSsdCount2, tc.ephStrOnLocalSsd2)[0])
				cluster.FillUpNodesCompletely(nodes)
				strategy, _ := buildGKEStrategy(t, cluster)

				pod := testutils.BuildTestPodWithEphemeralStorage("test", 0, 0, tc.podEph*units.GiB)
				pods := []*apiv1.Pod{pod}

				option, _, err := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
				if err != nil && !tc.err {
					t.Errorf("Got err %e for %s, expected nil", err, tc.name)
				} else if tc.err {
					return
				}
				expectedName := impostor.ConstructNodeGroupNameEphemeralStorageSupport(machineType, tc.diskType2, tc.diskSize2*units.GiB, tc.localSsdCount2)
				optionName := getBestOptionGroupName(option)
				if diff := cmp.Diff(expectedName, optionName); diff != "" {
					t.Errorf("Mismatch for case #%s, diff (-want +got):\n%s", tc.name, diff)
				}
			}
		})
	}
}

func TestLargeScaleUpRequest(t *testing.T) {
	testoptions.MarkTestManual(t)
	type testCase struct {
		clusterSize       int
		autoscaleExisting bool
		pods              int
		milliCpu          int64
		mem               int64
		expectedNodePool  string
		enableAutopilot   bool
	}
	testCases := []testCase{
		{1, true, 1000, 250, 512, "nap-n1-standard-8", false},
		{1, true, 2000, 250, 512, "nap-n1-standard-16", false},
		{1, true, 3000, 250, 512, "nap-n1-standard-16", false},
		{1, true, 5000, 250, 512, "nap-n1-standard-16", false},
		{1, true, 10000, 250, 512, "nap-n1-standard-16", false},
		{1, true, 12000, 250, 512, "nap-n1-standard-32", false},
		{1, true, 15000, 250, 512, "nap-n1-standard-32", false},
		{1, true, 25000, 250, 512, "nap-n1-standard-32", false},

		{1, true, 1000, 250, 512, "nap-n1-standard-8", true},
		{1, true, 2000, 250, 512, "nap-n1-standard-32", true},
		{1, true, 3000, 250, 512, "nap-n1-standard-32", true},
		{1, true, 5000, 250, 512, "nap-n1-standard-32", true},
		{1, true, 10000, 250, 512, "nap-n1-standard-32", true},
		{1, true, 12000, 250, 512, "nap-n1-standard-32", true},
		{1, true, 15000, 250, 512, "nap-n1-standard-32", true},
		{1, true, 25000, 250, 512, "nap-n1-standard-32", true},
	}
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("testcase %d", idx), func(t *testing.T) {
			cluster := impostor.NewCluster(impostor.WithCustomMaxPodPerNodeCount(128), impostor.WithAutopilotEnabled(tc.enableAutopilot))
			var initialNodes []*apiv1.Node
			cluster.FillUpNodesCompletely(initialNodes)
			strategy, analyzer := buildGKEStrategy(t, cluster)
			pods := Pods(tc.pods).WithName("test").WithCPUMilli(tc.milliCpu).WithMemMiB(tc.mem).Get()

			nodeGroups, nodeInfos, err := cluster.Autoscaler().GetNodeGroupsForScaleUp(pods)
			assert.NoError(t, err)
			context := cluster.Autoscaler().GetContext()
			expansionOptions := make([]expander.Option, 0)

			for _, nodeGroup := range nodeGroups {
				nodeInfo, found := nodeInfos[nodeGroup.Id()]
				assert.Equal(t, true, found)

				option := expander.Option{
					NodeGroup: nodeGroup,
				}

				context.ClusterSnapshot.Fork()

				nodeIndex := 1
				newNodeInfo, err := simulator.SanitizedNodeInfo(nodeInfo, fmt.Sprintf("e-%d", nodeIndex))
				assert.NoError(t, err)
				err = context.ClusterSnapshot.AddNodeInfo(newNodeInfo)
				assert.NoError(t, err)
				var scheduledPods []*apiv1.Pod
				ignore := false

				for _, pod := range pods {
					if schedErr := context.ClusterSnapshot.CheckPredicates(pod, newNodeInfo.Node().Name); schedErr != nil {
						nodeIndex += 1
						newNodeInfo, err = simulator.SanitizedNodeInfo(nodeInfo, fmt.Sprintf("e-%d", nodeIndex))
						assert.NoError(t, err)
						err = context.ClusterSnapshot.AddNodeInfo(newNodeInfo)
						assert.NoError(t, err)
					}

					if err = context.ClusterSnapshot.SchedulePod(pod, newNodeInfo.Node().Name); err != nil {
						ignore = true
						break
					}

					scheduledPods = append(scheduledPods, pod)
				}

				context.ClusterSnapshot.Revert()

				if ignore {
					continue
				}

				if len(scheduledPods) > 0 {
					option.NodeCount = nodeIndex
					option.Pods = scheduledPods
					expansionOptions = append(expansionOptions, option)
				}
			}

			bestOption := strategy.BestOption(expansionOptions, nodeInfos)
			assert.NotNil(t, bestOption)
			preferredCpuCount := getPreferredCpuCount(t, analyzer, *bestOption, nodeInfos)
			optionName := getBestOptionGroupName(bestOption)
			if diff := cmp.Diff(tc.expectedNodePool, optionName); diff != "" {
				debugInfo := fmt.Sprintf("cluster size: %v, pods: %v, prefferedCpuCount: %v, optionName: %v, optionNodeCount: %v\n", tc.clusterSize, tc.pods, preferredCpuCount, optionName, bestOption.NodeCount)
				t.Errorf("Mismatch for case #%d, diff (-want +got):\n%sDebug info:\n%s", idx, diff, debugInfo)
			}
		})
	}
}
