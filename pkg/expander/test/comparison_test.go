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
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/impostor"
)

func TestExpanderComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping simulations in short mode")
	}

	type testCase struct {
		poolCount        int
		nodeCount        int
		nodeCpus         int64
		pods             int
		millicpu         int64
		mem              int64
		gpu              int64
		existingNodePool string
		expectedNodePool string
		description      string
	}
	type testGroup struct {
		title     string
		testCases []testCase
	}
	testCases := []testGroup{
		{"Resource prediction", []testCase{
			{2, 1, 1, 1, 800, 2415, 0, "", "nap-n1-standard-1", ""},
			{2, 1, 1, 1, 800, 2425, 0, "", "nap-n1-standard-2", ""},
			{2, 1, 1, 1, 835, 1000, 0, "", "nap-n1-standard-1", ""},
			{2, 1, 1, 1, 840, 200, 0, "", "nap-n1-highcpu-2", ""},
			{2, 1, 1, 1, 840, 1025, 0, "", "nap-n1-standard-2", ""},
			{2, 1, 1, 1, 1820, 5420, 0, "", "nap-n1-standard-2", ""},
			{2, 1, 1, 1, 1820, 5430, 0, "", "nap-n1-highmem-2", ""},
		}},
		{"Basic scenarios", []testCase{
			// Basic
			{2, 1, 1, 1, 100, 100, 0, "", "nap-n1-standard-1", ""},
			{2, 1, 1, 8, 100, 100, 0, "", "nap-n1-standard-1", ""},
			{2, 0, 1, 1, 100, 2415, 0, "", "nap-n1-highmem-2", ""},
			{2, 1, 1, 1, 800, 2400, 0, "", "nap-n1-standard-1", ""},
			{2, 1, 1, 8, 800, 2400, 0, "", "nap-n1-standard-2", "needed 8 cores, 20 GiB"},
			{2, 0, 1, 1, 900, 1000, 0, "", "nap-n1-standard-2", "cpu over standard-1, empty cluster"},
			{2, 3, 2, 1, 900, 1000, 0, "", "nap-n1-standard-2", "cpu over standard-1, 3 nodes cluster"},
			{2, 1, 1, 1, 900, 2600, 0, "", "nap-n1-standard-2", "cpu over standard-1, mem over highcpu-2"},
			{2, 1, 1, 1, 900, 5000, 0, "", "nap-n1-highmem-2", "max mem for std-2"},
			{2, 1, 1, 10, 900, 5000, 0, "", "nap-n1-highmem-2", "2 pods per node"},
			{2, 1, 1, 10, 900, 5400, 0, "", "nap-n1-highmem-2", "1 pod per node, 2 pods mem over highmem-2, needed 10 cores, 54 GiB"},
			{2, 1, 1, 1, 900, 5500, 0, "", "nap-n1-highmem-2", "mem over standard-2"},
		}},
		{"GPU validation", []testCase{
			{2, 1, 1, 1, 100, 100, 1, "", "nap-n1-standard-1-gpu1", ""},
			{2, 1, 1, 1, 100, 100, 2, "", "nap-n1-standard-1-gpu2", ""},
			{2, 1, 1, 1, 100, 100, 3, "", "nap-n1-standard-1-gpu4", "bumped from 3 to 4 GPUs"},
			{2, 1, 1, 1, 100, 100, 8, "", "nap-n1-standard-1-gpu8", ""},
			{2, 1, 1, 1, 100, 100, 9, "", "-", "too many GPUs"},
			{2, 1, 1, 1, 100, 100, 16, "", "-", "too many GPUs"},
			{2, 1, 1, 1, 10000, 100, 1, "", "-", "at least cpu16-gpu2 needed"},
			{2, 1, 1, 2, 10000, 100, 1, "", "-", "at least cpu16-gpu2 or cpu32-gpu4"},
		}},
		{"Pods number limits", []testCase{
			{2, 1, 1, 50, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 1, 1, 200, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 3, 2, 50, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 3, 2, 108, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 3, 2, 200, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 7, 4, 50, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 7, 4, 108, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 7, 4, 200, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 7, 4, 400, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 21, 8, 50, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 21, 8, 108, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 21, 8, 200, 5, 5, 0, "", "nap-n1-standard-1", ""},
			{2, 21, 8, 400, 5, 5, 0, "", "nap-n1-standard-1", ""},
		}},
		{"Pick existing more expensive node pool", []testCase{
			{2, 3, 2, 1, 1500, 3000, 0, "", "nap-n1-standard-2", "make sure that std2 is selected by default"},
			{2, 3, 2, 1, 1000, 3000, 0, "n1-highmem-2", "n1-highmem-2", "check if mem2 is reused instead of std2"},
			{2, 3, 2, 1, 1000, 3000, 0, "n1-standard-4", "nap-n1-standard-2", "check if std4 is reused instead of std2"},
			{2, 3, 2, 1, 1000, 3000, 0, "n1-highmem-4", "nap-n1-standard-2", "check if more expensive mem4 is reused"},
			{2, 3, 2, 1, 100, 1000, 0, "", "nap-n1-standard-2", "make sure that std2 is selected by default"},
			{2, 3, 2, 1, 1500, 6000, 0, "n1-standard-4", "nap-n1-highmem-2", "check if std4 is reused instead of mem2"},
			{50, 3, 2, 1, 1500, 6000, 0, "n1-standard-4", "n1-standard-4", "check if std4 is reused instead of mem2"},
		}},
		{"Pick existing more expensive GPU node pool", []testCase{
			{2, 1, 1, 1, 1100, 2000, 2, "", "nap-n1-standard-2-gpu2", "std2 default"},
			{2, 1, 1, 1, 1100, 1000, 2, "n1-highmem-2-gpu2", "n1-highmem-2-gpu2", "existing mem2, 1 pod"},
			{2, 1, 1, 2, 1100, 1000, 2, "n1-highmem-2-gpu2", "n1-highmem-2-gpu2", "existing mem2, 2 pods"},
			{2, 1, 1, 1, 1100, 2000, 2, "n1-highmem-2-gpu2", "n1-highmem-2-gpu2", "existing mem2"},
			{2, 1, 1, 1, 1100, 2000, 2, "n1-highcpu-4-gpu2", "n1-highcpu-4-gpu2", "existing cpu4"},
			{2, 1, 1, 1, 1100, 2000, 2, "n1-standard-4-gpu2", "nap-n1-standard-2-gpu2", "existing std4"},
			{2, 1, 1, 1, 1100, 2000, 2, "n1-highmem-4-gpu2", "nap-n1-standard-2-gpu2", "existing mem4"},
			{2, 1, 1, 1, 100, 100, 2, "n1-highmem-2-gpu2", "n1-highmem-2-gpu2", "existing mem2, 1 pod"},
			{2, 1, 1, 2, 100, 100, 2, "n1-highmem-2-gpu2", "n1-highmem-2-gpu2", "existing mem2, 2 pods"},
		}},
		{"Create GPU node pool", []testCase{
			{2, 1, 1, 1, 100, 100, 1, "", "nap-n1-standard-1-gpu1", ""},
			{2, 1, 1, 2, 100, 100, 1, "", "nap-n1-standard-1-gpu1", ""},
			{2, 3, 2, 4, 100, 100, 1, "", "nap-n1-standard-1-gpu1", ""},
			{2, 7, 4, 8, 100, 100, 1, "", "nap-n1-standard-1-gpu1", ""},
			{2, 1, 1, 1, 100, 100, 2, "", "nap-n1-standard-1-gpu2", ""},
			{2, 1, 1, 2, 100, 100, 2, "", "nap-n1-standard-1-gpu2", ""},
			{2, 3, 2, 4, 100, 100, 2, "", "nap-n1-standard-1-gpu2", ""},
			{2, 7, 4, 8, 100, 100, 2, "", "nap-n1-standard-1-gpu2", ""},
			{2, 1, 1, 1, 100, 100, 3, "", "nap-n1-standard-1-gpu4", ""},
			{2, 1, 1, 2, 100, 100, 3, "", "nap-n1-standard-1-gpu4", ""},
			{2, 3, 2, 4, 100, 100, 3, "", "nap-n1-standard-1-gpu4", ""},
			{2, 7, 4, 8, 100, 100, 3, "", "nap-n1-standard-1-gpu4", ""},
			{2, 1, 1, 1, 100, 100, 4, "", "nap-n1-standard-1-gpu4", ""},
			{2, 1, 1, 2, 100, 100, 4, "", "nap-n1-standard-1-gpu4", ""},
			{2, 3, 2, 4, 100, 100, 4, "", "nap-n1-standard-1-gpu4", ""},
			{2, 7, 4, 8, 100, 100, 4, "", "nap-n1-standard-1-gpu4", ""},
			{2, 1, 1, 1, 100, 100, 8, "", "nap-n1-standard-1-gpu8", ""},
			{2, 3, 2, 4, 100, 100, 8, "", "nap-n1-standard-1-gpu8", ""},
			{2, 7, 4, 8, 100, 100, 8, "", "nap-n1-standard-1-gpu8", ""},
			{2, 1, 1, 1, 1100, 1000, 1, "", "nap-n1-highcpu-2-gpu1", ""},
			{2, 1, 1, 2, 1100, 1000, 1, "", "nap-n1-highcpu-2-gpu1", ""},
			{2, 1, 1, 5, 1100, 1000, 1, "", "nap-n1-highcpu-2-gpu1", ""},
		}},
		{"Do not share GPUs", []testCase{
			{2, 1, 1, 1, 100, 100, 1, "n1-standard-1-gpu2", "nap-n1-standard-1-gpu1", ""},
			{2, 1, 1, 2, 100, 100, 1, "n1-standard-1-gpu2", "nap-n1-standard-1-gpu1", ""},
			{2, 3, 2, 4, 100, 100, 1, "n1-standard-1-gpu2", "nap-n1-standard-1-gpu1", ""},
			{2, 7, 4, 8, 100, 100, 1, "n1-standard-1-gpu2", "nap-n1-standard-1-gpu1", ""},
			{2, 1, 1, 1, 100, 100, 2, "n1-standard-1-gpu4", "nap-n1-standard-1-gpu2", ""},
			{2, 1, 1, 2, 100, 100, 2, "n1-standard-1-gpu4", "nap-n1-standard-1-gpu2", ""},
			{2, 3, 2, 4, 100, 100, 2, "n1-standard-1-gpu4", "nap-n1-standard-1-gpu2", ""},
			{2, 7, 4, 8, 100, 100, 2, "n1-standard-1-gpu4", "nap-n1-standard-1-gpu2", ""},
			{2, 1, 1, 1, 100, 100, 3, "n1-standard-1-gpu8", "nap-n1-standard-1-gpu4", ""},
			{2, 1, 1, 2, 100, 100, 3, "n1-standard-1-gpu8", "nap-n1-standard-1-gpu4", ""},
			{2, 3, 2, 4, 100, 100, 3, "n1-standard-1-gpu8", "nap-n1-standard-1-gpu4", ""},
			{2, 7, 4, 8, 100, 100, 3, "n1-standard-1-gpu8", "nap-n1-standard-1-gpu4", ""},
			{2, 1, 1, 1, 100, 100, 4, "n1-standard-1-gpu8", "nap-n1-standard-1-gpu4", ""},
			{2, 1, 1, 2, 100, 100, 4, "n1-standard-1-gpu8", "nap-n1-standard-1-gpu4", ""},
			{2, 3, 2, 4, 100, 100, 4, "n1-standard-1-gpu8", "nap-n1-standard-1-gpu4", ""},
			{2, 7, 4, 8, 100, 100, 4, "n1-standard-1-gpu8", "nap-n1-standard-1-gpu4", ""},
		}},
	}
	for grpIdx, group := range testCases {
		t.Run(group.title, func(t *testing.T) {
			for tcIdx, tc := range group.testCases {
				t.Run(fmt.Sprintf("testcase %d", tcIdx), func(t *testing.T) {
					cluster := impostor.NewCluster()
					initialGroupName := tc.existingNodePool
					if tc.existingNodePool != "" {
						// Create initial node group that could be scaled up
						initialGpuCount := 0
						var err error
						machineType := tc.existingNodePool
						if gpuIdx := strings.LastIndex(tc.existingNodePool, "gpu"); gpuIdx > 0 {
							initialGpuCount, err = strconv.Atoi(tc.existingNodePool[len(tc.existingNodePool)-1:])
							assert.NoError(t, err)
							machineType = tc.existingNodePool[0 : gpuIdx-1]
						}
						cluster.AddNodeGroup(tc.existingNodePool, machineType, int64(initialGpuCount), true)
					}
					for len(cluster.NodeGroups()) < tc.poolCount {
						// Create node groups that could not be scaled up to test pool count reducer
						randomGroupName := fmt.Sprintf("n1-standard-%d-%v", tc.nodeCpus, gke.GenerateRandomId(8))
						cluster.AddNodeGroup(randomGroupName, fmt.Sprintf("n1-standard-%d", tc.nodeCpus), 0, false)
						initialGroupName = randomGroupName
					}
					cluster.ScaleUpNodeGroup(initialGroupName, tc.nodeCount)
					strategy := buildStrategy(cluster)
					GKEstrategy, analyzer := buildGKEStrategy(t, cluster)
					pods := Pods(tc.pods).WithName("test").WithCPUMilli(tc.millicpu).WithMemMiB(tc.mem).WithGPU(tc.gpu).Get()
					option, _, _ := cluster.Autoscaler().BestScaleUpOption(strategy, pods)
					bestOption, _, _ := parseBestOption(option)
					option2, nodeInfos2, _ := cluster.Autoscaler().BestScaleUpOption(GKEstrategy, pods)
					bestOption2, _, _ := parseBestOption(option2)
					preferredShape := getPreferredMachineType(t, cluster)
					if option2 != nil && tc.gpu == 0 {
						// Make sure that number of nodes and cpus in existing cluster
						// results in similar prediction for both expanders
						// GPUs are excluded as new expander would prefer minimal node size
						// New expander takes upcoming pods into consideration and could prefer bigger nodes
						parts := strings.Split(preferredShape, "-")
						preferredCpuCount, err := strconv.Atoi(parts[len(parts)-1])
						assert.NoError(t, err)
						preferredCpuCount2 := getPreferredCpuCount(t, analyzer, *option2, nodeInfos2)
						assert.True(t, int64(preferredCpuCount) <= preferredCpuCount2,
							"test %d.%02d: node count %v x %v cpus, pods %v x %v milli",
							grpIdx, tcIdx, tc.nodeCount, tc.nodeCpus, tc.pods, tc.millicpu)
					}
					point := 0
					if bestOption == tc.expectedNodePool {
						point += 1
					}
					point2 := 0
					if bestOption2 == tc.expectedNodePool {
						point2 += 1
					}
					diff := point2 - point
					if bestOption == bestOption2 && bestOption == tc.expectedNodePool {
						return
					}

					if diff < 0 {
						assert.Equal(t, tc.expectedNodePool, bestOption2,
							"test %d.%02d: node count %v, pods %v, millicpu %v, mem %v, nodeCount: %v",
							grpIdx, tcIdx, tc.nodeCount, tc.pods, tc.millicpu, tc.mem, option.NodeCount)
						panic(1)
					}
				})
			}
		})
	}
}
