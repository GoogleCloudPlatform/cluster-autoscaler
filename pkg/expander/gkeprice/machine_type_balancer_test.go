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

package gkeprice

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
)

func TestComputeClassBalancer(t *testing.T) {
	type nodeGroupConfig struct {
		machineType string
		targetSize  int
		cpu         int64
	}

	testCases := []struct {
		name           string
		machineType    string
		nodeGroups     []nodeGroupConfig
		expectedFactor float64
	}{
		{
			name:           "Invalid machine family, balancing factor 1.0",
			machineType:    "invalid",
			expectedFactor: 1.0,
		},
		{
			name:           "Unknown machine family, balancing factor 1.0",
			machineType:    "m8-standard-8",
			expectedFactor: 1.0,
		},
		{
			name:           "Machine family not in compute class, balancing factor 1.0",
			machineType:    "e2-standard-4",
			expectedFactor: 1.0,
		},
		{
			name:           "n2 machine family, no other node groups, balancing factor 1.0",
			machineType:    "n2-standard-4",
			expectedFactor: 1.0,
		},
		{
			name:        "n2 machine family, one n2 node group, balancing factor 1.0",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2-standard-1", 4, 1},
			},
			expectedFactor: 1.0,
		},
		{
			name:        "n2 machine family, many n2 node group, balancing factor 1.0",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2-standard-1", 4, 1},
				{"n2-standard-1", 5, 1},
				{"n2-standard-1", 10, 1},
			},
			expectedFactor: 1.0,
		},
		{
			name:        "n2 machine family, many n2 node group, diverse sizes, balancing factor 1.0",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2-standard-1", 3, 1},
				{"n2-standard-2", 3, 2},
				{"n2-standard-4", 10, 4},
			},
			expectedFactor: 1.0,
		},
		{
			name:        "n2 machine family, one n2d node group, balancing factor 1/(N2D+1)",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2d-standard-1", 4, 1},
			},
			expectedFactor: 0.2,
		},
		{
			name:        "n2 machine family, many n2d node group, balancing factor 1/(N2D+1)",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2d-standard-1", 4, 1},
				{"n2d-standard-1", 5, 1},
				{"n2d-standard-1", 10, 1},
			},
			expectedFactor: 0.05,
		},
		{
			name:        "n2 machine family, many n2d node group, diverse sizes, balancing factor 1/(N2D+1)",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2d-standard-1", 3, 1},
				{"n2d-standard-2", 3, 2},
				{"n2d-standard-4", 10, 4},
			},
			expectedFactor: 0.02,
		},
		{
			name:        "n2 machine family, equal number of n2 and n2d, balancing factor 1.0",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2-standard-1", 1, 1},
				{"n2-standard-4", 12, 4},
				{"n2d-standard-1", 3, 1},
				{"n2d-standard-2", 3, 2},
				{"n2d-standard-4", 10, 4},
			},
			expectedFactor: 1.0,
		},
		{
			name:        "n2 machine family, more n2 than n2d, balancing factor 1.0",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2-standard-1", 1, 1},
				{"n2-standard-4", 12, 4},
				{"n2d-standard-1", 3, 1},
				{"n2d-standard-2", 3, 2},
			},
			expectedFactor: 1.0,
		},
		{
			name:        "n2 machine family, less n2 than n2d, balancing factor (N2+1)/(N2D+1)",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2-standard-1", 1, 1},
				{"n2d-standard-1", 3, 1},
				{"n2d-standard-2", 3, 2},
				{"n2d-standard-4", 10, 4},
			},
			expectedFactor: 0.04,
		},
		{
			name:        "n2 machine family, other machine types no in compute class, balancing factor 1.0",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2-standard-1", 7, 1},
				{"n1-standard-1", 4, 1},
				{"n1-standard-2", 5, 2},
				{"n1-standard-4", 10, 4},
				{"t2d-standard-1", 4, 1},
				{"t2d-standard-2", 5, 2},
				{"t2d-standard-4", 10, 4},
			},
			expectedFactor: 1.0,
		},
		{
			name:        "n2 machine family, other machine types no in compute class, balancing factor (N2+1)/(N2D+1)",
			machineType: "n2-standard-4",
			nodeGroups: []nodeGroupConfig{
				{"n2-standard-1", 1, 1},
				{"n2d-standard-1", 3, 1},
				{"n2d-standard-2", 3, 2},
				{"n2d-standard-4", 10, 4},
				{"n1-standard-1", 4, 1},
				{"n1-standard-2", 5, 2},
				{"n1-standard-4", 10, 4},
				{"t2d-standard-1", 4, 1},
				{"t2d-standard-2", 5, 2},
				{"t2d-standard-4", 10, 4},
			},
			expectedFactor: 0.04,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := gke.NewTestAutoprovisioningCloudProviderBuilder().
				WithAutopilotEnabled(true).
				WithMachineConfigProvider(machinetypes.NewMachineConfigProvider(nil)).
				Build()

			nodeInfos := make(map[string]*framework.NodeInfo)
			for i, config := range tc.nodeGroups {
				nodeGroupName := fmt.Sprintf("%s-%d", config.machineType, i)
				provider.AddAutoprovisionedGkeNodeGroup(nodeGroupName, nodeGroupName, config.targetSize, true, false, config.machineType, true, true)
				node := test.BuildTestNode(nodeGroupName, config.cpu*1000, 0)
				nodeInfo := framework.NewTestNodeInfo(node)
				nodeInfos[nodeGroupName] = nodeInfo
			}

			balancer := NewComputeClassBalancer(provider)
			assert.Equal(t, balancer.MachineTypeBalancingFactor(tc.machineType, nodeInfos), tc.expectedFactor)
		})
	}
}
