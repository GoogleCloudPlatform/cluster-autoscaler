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

package gkeclient

import (
	"errors"
)

type autoscalingGkeClientMock struct {
	getCluster       func() (Cluster, error)
	createPool       func(string, *NodePoolSpec) error
	updatePoolLabels map[string]error
	deletePool       func(string) error
}

// NewAutoscalingGkeClientMock returns a mock gke client.
func NewAutoscalingGkeClientMock(getClusterFunc func() (Cluster, error), createPoolFunc func(string, *NodePoolSpec) error, deletePoolFunc func(string) error) AutoscalingGkeClient {
	return &autoscalingGkeClientMock{
		getCluster:       getClusterFunc,
		createPool:       createPoolFunc,
		updatePoolLabels: make(map[string]error),
		deletePool:       deletePoolFunc,
	}
}

func (c *autoscalingGkeClientMock) GetCluster() (Cluster, error) {
	if c.getCluster == nil {
		return Cluster{}, errors.New("Unexpected GetCluster call")
	}
	return c.getCluster()
}

func (c *autoscalingGkeClientMock) CreateNodePool(name string, spec *NodePoolSpec) error {
	if c.createPool == nil {
		return errors.New("Unexpected CreateNodePool call")
	}
	return c.createPool(name, spec)
}

func (c *autoscalingGkeClientMock) DeleteNodePool(pool string) error {
	if c.deletePool == nil {
		return errors.New("Unexpected DeleteNodePool call")
	}
	return c.deletePool(pool)
}

func (c *autoscalingGkeClientMock) UpdateNodePoolLabels(pool string, labels map[string]string) error {
	if err, found := c.updatePoolLabels[pool]; found {
		return err
	}
	return nil
}

// TestAdditionalNetworkConfig creates instance of AdditionalNetworkConfig fot test purposes.
func TestAdditionalNetworkConfig(net, subnet, podRange string, mppn int64) AdditionalNetworkConfig {
	return AdditionalNetworkConfig{
		VPCNetName:     net,
		VPCSubnetName:  subnet,
		SubRange:       podRange,
		MaxPodsPerNode: mppn,
	}
}

func TestAdditionalNetworkConfigWithAttachment(mppn int64, networkAttachment string) AdditionalNetworkConfig {
	return AdditionalNetworkConfig{
		MaxPodsPerNode:    mppn,
		NetworkAttachment: networkAttachment,
	}
}
