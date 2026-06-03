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

package backoff

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/nodetemplate"
	internal_customresources "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/customresources"
)

func TestGetNodeResources(t *testing.T) {
	tcs := []struct {
		name        string
		zone        string
		cpu         int64
		mem         int64
		gpu         int64
		machineType string
		flexStart   bool
		expected    NodeResources
	}{
		{
			"standard node",
			"zone1",
			2000,
			8 * 1024 * 1024 * 1024,
			0,
			"n1-standard-2",
			false,
			NodeResources{
				{
					"zone1",
					"cpu",
					"n1",
					2,
					StandardCostClass,
					StandardCapacityClass,
				},
				{
					"zone1",
					"memory",
					"n1",
					8,
					StandardCostClass,
					StandardCapacityClass,
				},
			},
		},
		{
			"gpu node",
			"zone2",
			2000,
			8 * 1024 * 1024 * 1024,
			16,
			"n1-standard-2",
			false,
			NodeResources{
				{
					"zone2",
					"cpu",
					"n1",
					2,
					StandardCostClass,
					StandardCapacityClass,
				},
				{
					"zone2",
					"memory",
					"n1",
					8,
					StandardCostClass,
					StandardCapacityClass,
				},
				{
					"zone2",
					"nvidia-tesla-k80",
					"n1",
					16,
					ExpensiveCostClass,
					StandardCapacityClass,
				},
			},
		},
		{
			"gpu flex node",
			"zone2",
			2000,
			8 * 1024 * 1024 * 1024,
			16,
			"n1-standard-2",
			true,
			NodeResources{
				{
					"zone2",
					"cpu",
					"n1",
					2,
					StandardCostClass,
					FlexStartCapacityClass,
				},
				{
					"zone2",
					"memory",
					"n1",
					8,
					StandardCostClass,
					FlexStartCapacityClass,
				},
				{
					"zone2",
					"nvidia-tesla-k80",
					"n1",
					16,
					ExpensiveCostClass,
					FlexStartCapacityClass,
				},
			},
		},
	}
	for _, testCase := range tcs {
		t.Run(testCase.name, func(t *testing.T) {
			node := test.BuildTestNode("n", testCase.cpu, testCase.mem)
			if testCase.gpu > 0 {
				test.AddGpusToNode(node, testCase.gpu)
			}
			nodeInfo := framework.NewTestNodeInfo(node)
			nodeGroup := gke.NewTestGkeMigBuilder().SetMaxSize(1000).SetSpec(&gkeclient.NodePoolSpec{MachineType: testCase.machineType, FlexStart: testCase.flexStart}).Build()

			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().Build()
			processor := internal_customresources.NewProcessor(nodetemplate.NewCache())
			processor.SetContext(&context.AutoscalingContext{CloudProvider: provider})
			actual, err := GetNodeResources(processor, nodeInfo, nodeGroup, testCase.zone)
			assert.NoError(t, err)
			assert.ElementsMatch(t, testCase.expected, actual)
		})
	}
}
