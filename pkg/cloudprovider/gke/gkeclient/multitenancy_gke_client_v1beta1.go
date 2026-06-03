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
	"fmt"
	"strings"
	"sync"

	gkeapi "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient/api"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
	"k8s.io/klog/v2"
)

type MultitenancyGKEClient interface {
	AutoscalingGkeClient
	multitenancy.ProviderConfigEventHandler
}

type multitenancyGkeClientV1beta1 struct {
	autoscalingGkeClient          AutoscalingGkeClient
	providerConfigToNetworkConfig map[string]*multitenancy.ProviderNetworkConfig
	mutex                         sync.Mutex
}

var _ MultitenancyGKEClient = &multitenancyGkeClientV1beta1{}

func NewMultitenancyGkeClientV1beta1(client gkeapi.Client, nodePoolTranslator NodePoolTranslator, projectId, location, clusterName string, machineConfigProvider *machinetypes.MachineConfigProvider, napMaxNodes int) (*multitenancyGkeClientV1beta1, error) {
	autoscalingGkeClient, err := NewAutoscalingGkeClientV1beta1(client, nodePoolTranslator, projectId, location, clusterName, machineConfigProvider, napMaxNodes)
	if err != nil {
		return nil, err
	}
	return &multitenancyGkeClientV1beta1{
		autoscalingGkeClient:          autoscalingGkeClient,
		providerConfigToNetworkConfig: map[string]*multitenancy.ProviderNetworkConfig{},
		mutex:                         sync.Mutex{},
	}, nil
}

func (m *multitenancyGkeClientV1beta1) AddProviderConfig(providerConfig *multitenancy.ProviderConfig) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.providerConfigToNetworkConfig[providerConfig.Name] = providerConfig.NetworkConfig
	return nil
}

func (m *multitenancyGkeClientV1beta1) DeleteProviderConfig(providerConfig *multitenancy.ProviderConfig) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.providerConfigToNetworkConfig, providerConfig.Name)
	return nil
}

func (m *multitenancyGkeClientV1beta1) GetCluster() (Cluster, error) {
	return m.autoscalingGkeClient.GetCluster()
}

func (m *multitenancyGkeClientV1beta1) DeleteNodePool(pool string) error {
	return m.autoscalingGkeClient.DeleteNodePool(pool)
}

func (m *multitenancyGkeClientV1beta1) UpdateNodePoolLabels(pool string, labels map[string]string) error {
	return m.autoscalingGkeClient.UpdateNodePoolLabels(pool, labels)
}

// CreateNodePool is a GKE MT specific implementation that adds
// relevant networking information for the nodepool in the node
// labels via a side-channel. It also sets the PodRange and
// PodIpv4CidrBlock fields in the nodepool proto.
//
// All nodes in a MT cluster should have a /provider-config label
// which is used to lookup the networking information.
func (m *multitenancyGkeClientV1beta1) CreateNodePool(name string, spec *NodePoolSpec) error {
	if spec.Labels == nil {
		spec.Labels = map[string]string{}
	}
	providerConfigName, exists := spec.Labels[multitenancy.ProviderConfigLabel]
	if !exists {
		// TODO(b/391376228): Once MT resource model is implemented this should return an error.
		klog.V(4).Infof("CreateNodePool call for nodepool %s has no provider config label", name)
		return m.autoscalingGkeClient.CreateNodePool(name, spec)
	}
	networkConfig, exists := m.providerConfigToNetworkConfig[providerConfigName]
	if !exists {
		return fmt.Errorf("unable to find network config for provider config %s", providerConfigName)
	}
	// TODO(b/391807976): Switch to setting fields in nodepool proto when new fields are available.
	spec.Labels[multitenancy.VPCLabel] = resourceNameFromFullPath(networkConfig.Network)
	spec.Labels[multitenancy.SubnetLabel] = resourceNameFromFullPath(networkConfig.Subnetwork)
	spec.PodRange = networkConfig.PodRange
	return m.autoscalingGkeClient.CreateNodePool(name, spec)
}

// resourceNameFromFullPath takes a GCP resource path like
// "projects/gketenancy-e2e-testing/global/networks/custom-network" and returns
// the resource name e.g. custom-network for the above path.
//
// It assumes that the resource name is the final element after splitting the
// path based on "/". If the path contains no "/", it returns the entire input
// string as the resource name.
func resourceNameFromFullPath(networkResourcePath string) string {
	splitPath := strings.Split(networkResourcePath, "/")
	return splitPath[len(splitPath)-1]
}
