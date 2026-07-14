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
	"sync"
	"testing"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/multitenancy"
)

func TestMTGKEClient(t *testing.T) {
	testCases := []struct {
		name                      string
		providerConfigsToAdd      []*multitenancy.ProviderConfig
		providerConfigsToDelete   []*multitenancy.ProviderConfig
		createNodepoolSpec        *NodePoolSpec
		expectCreateNodepoolError bool
		verificationFunc          func(spec *NodePoolSpec) error
		experimentsManager        experiments.Manager
	}{
		{
			name: "happy_case",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name: "t1234-tenant1",
					NetworkConfig: &multitenancy.ProviderNetworkConfig{
						Network:    "projects/gketenancy-e2e-testing/global/networks/test-network",
						Subnetwork: "projects/gketenancy-e2e-testing/regions/us-central1/subnetworks/test-subnetwork",
						PodRange:   "test-pod-range",
					},
				},
			},
			createNodepoolSpec: &NodePoolSpec{
				Labels: map[string]string{
					multitenancy.ProviderConfigLabel: "t1234-tenant1",
				},
			},
			verificationFunc: ensureMTNodepoolSpec(t, "projects/gketenancy-e2e-testing/global/networks/test-network", "projects/gketenancy-e2e-testing/regions/us-central1/subnetworks/test-subnetwork", "test-pod-range"),
		},
		{
			// At the moment, we don't necessarily require the full resource
			// paths in Network and Subnet, so allow things to be flexible and
			// ensure resource names without complete path also work.
			name: "happy_case_having_subnetwork_without_complete_path",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name: "t1234-tenant1",
					NetworkConfig: &multitenancy.ProviderNetworkConfig{
						Network:    "projects/gketenancy-e2e-testing/global/networks/test-network",
						Subnetwork: "test-subnetwork", // Subnetwork does not specify the complete path.
						PodRange:   "test-pod-range",
					},
				},
			},
			createNodepoolSpec: &NodePoolSpec{
				Labels: map[string]string{
					multitenancy.ProviderConfigLabel: "t1234-tenant1",
				},
			},
			verificationFunc: ensureMTNodepoolSpec(t, "projects/gketenancy-e2e-testing/global/networks/test-network", "test-subnetwork", "test-pod-range"),
		},
		{
			name: "add_delete_same_provider_config",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name: "t1234-tenant1",
					NetworkConfig: &multitenancy.ProviderNetworkConfig{
						Network:    "projects/gketenancy-e2e-testing/global/networks/test-network",
						Subnetwork: "test-subnetwork",
						PodRange:   "test-pod-range",
					},
				},
			},
			providerConfigsToDelete: []*multitenancy.ProviderConfig{
				{
					Name: "t1234-tenant1",
					NetworkConfig: &multitenancy.ProviderNetworkConfig{
						Network:    "projects/gketenancy-e2e-testing/global/networks/test-network",
						Subnetwork: "test-subnetwork",
					},
				},
			},
			createNodepoolSpec: &NodePoolSpec{
				Labels: map[string]string{
					multitenancy.ProviderConfigLabel: "t1234-tenant1",
				},
			},
			expectCreateNodepoolError: true,
		},
		{
			name: "add_same_twice",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name: "t1234-tenant1",
					NetworkConfig: &multitenancy.ProviderNetworkConfig{
						Network:    "projects/gketenancy-e2e-testing/global/networks/test-network",
						Subnetwork: "test-subnetwork",
						PodRange:   "test-pod-range",
					},
				},
				{
					Name: "t1234-tenant1",
					NetworkConfig: &multitenancy.ProviderNetworkConfig{
						Network:    "projects/gketenancy-e2e-testing/global/networks/test-network2",
						Subnetwork: "test-subnetwork2",
						PodRange:   "test-pod-range2",
					},
				},
			},
			createNodepoolSpec: &NodePoolSpec{
				Labels: map[string]string{
					multitenancy.ProviderConfigLabel: "t1234-tenant1",
				},
			},
			verificationFunc: ensureMTNodepoolSpec(t, "projects/gketenancy-e2e-testing/global/networks/test-network2", "test-subnetwork2", "test-pod-range2"),
		},
		{
			name: "create_nodepool_no_providerconfig",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name: "t1234-tenant1",
					NetworkConfig: &multitenancy.ProviderNetworkConfig{
						Network:    "projects/gketenancy-e2e-testing/global/networks/test-network",
						Subnetwork: "test-subnetwork",
						PodRange:   "test-pod-range",
					},
				},
			},
			createNodepoolSpec: &NodePoolSpec{},
			verificationFunc:   ensureNoMTLabels(t),
		},
		{
			name: "flag_false_network_subnet_exist",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name: "t1234-tenant1",
					NetworkConfig: &multitenancy.ProviderNetworkConfig{
						Network:    "projects/gketenancy-e2e-testing/global/networks/test-network",
						Subnetwork: "projects/gketenancy-e2e-testing/regions/us-central1/subnetworks/test-subnetwork",
						PodRange:   "test-pod-range",
					},
				},
			},
			createNodepoolSpec: &NodePoolSpec{
				Labels: map[string]string{
					multitenancy.ProviderConfigLabel: "t1234-tenant1",
				},
			},
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{experiments.MultitenancyKCPEnableServerAcceptNetworkApiFieldFlag: false}, map[string]string{}),
			verificationFunc:   ensureLegacyMTNodepoolSpec(t, "projects/gketenancy-e2e-testing/global/networks/test-network", "projects/gketenancy-e2e-testing/regions/us-central1/subnetworks/test-subnetwork", "test-pod-range"),
		},
		{
			name: "flag_true_network_subnet_exist",
			providerConfigsToAdd: []*multitenancy.ProviderConfig{
				{
					Name: "t1234-tenant1",
					NetworkConfig: &multitenancy.ProviderNetworkConfig{
						Network:    "projects/gketenancy-e2e-testing/global/networks/test-network",
						Subnetwork: "projects/gketenancy-e2e-testing/regions/us-central1/subnetworks/test-subnetwork",
						PodRange:   "test-pod-range",
					},
				},
			},
			createNodepoolSpec: &NodePoolSpec{
				Labels: map[string]string{
					multitenancy.ProviderConfigLabel: "t1234-tenant1",
				},
			},
			experimentsManager: experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{experiments.MultitenancyKCPEnableServerAcceptNetworkApiFieldFlag: true}, map[string]string{}),
			verificationFunc:   ensureMTNodepoolSpec(t, "projects/gketenancy-e2e-testing/global/networks/test-network", "projects/gketenancy-e2e-testing/regions/us-central1/subnetworks/test-subnetwork", "test-pod-range"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expManager := tc.experimentsManager
			if expManager == nil {
				expManager = experiments.NewMockManager()
			}
			mtGKEClient := &multitenancyGkeClientV1beta1{
				autoscalingGkeClient: &fakeGKEClient{
					createNodePoolVerificationFunc: tc.verificationFunc,
				},
				providerConfigToNetworkConfig: map[string]*multitenancy.ProviderNetworkConfig{},
				experimentsManager:            expManager,
				mutex:                         sync.Mutex{},
			}
			for _, providerConfig := range tc.providerConfigsToAdd {
				mtGKEClient.AddProviderConfig(providerConfig)
			}
			for _, providerConfig := range tc.providerConfigsToDelete {
				mtGKEClient.DeleteProviderConfig(providerConfig)
			}
			err := mtGKEClient.CreateNodePool("test", tc.createNodepoolSpec)
			if tc.expectCreateNodepoolError != (err != nil) {
				t.Error(err.Error())
			}
		})
	}
}

func ensureMTNodepoolSpec(t *testing.T, network, subnetwork, podRange string) func(*NodePoolSpec) error {
	t.Helper()
	return func(spec *NodePoolSpec) error {
		if spec.Network != resourceNameFromFullPath(network) {
			return fmt.Errorf("got network: %v, want: %v", spec.Network, resourceNameFromFullPath(network))
		}
		if spec.Subnetwork != resourceNameFromFullPath(subnetwork) {
			return fmt.Errorf("got subnetwork: %v, want: %v", spec.Subnetwork, resourceNameFromFullPath(subnetwork))
		}
		if spec.PodRange != podRange {
			return fmt.Errorf("got pod range: %v, want: %v", spec.PodRange, podRange)
		}
		vpcLabel := spec.Labels[multitenancy.VPCLabel]
		if vpcLabel != resourceNameFromFullPath(network) {
			return fmt.Errorf("got VPC label: %v, want: %v", vpcLabel, resourceNameFromFullPath(network))
		}
		subnetLabel := spec.Labels[multitenancy.SubnetLabel]
		if subnetLabel != resourceNameFromFullPath(subnetwork) {
			return fmt.Errorf("got Subnet label: %v, want: %v", subnetLabel, resourceNameFromFullPath(subnetwork))
		}
		return nil
	}
}

func ensureNoMTLabels(t *testing.T) func(*NodePoolSpec) error {
	t.Helper()
	return func(spec *NodePoolSpec) error {
		t.Helper()
		for label := range spec.Labels {
			if label == multitenancy.VPCLabel || label == multitenancy.SubnetLabel {
				return fmt.Errorf("got label: %v, want: no MT labels", label)
			}
		}
		if spec.PodIpv4CidrBlock != "" {
			return fmt.Errorf("got: %v, want: empty", spec.PodIpv4CidrBlock)
		}
		if spec.PodRange != "" {
			return fmt.Errorf("got: %v, want: empty", spec.PodRange)
		}
		return nil
	}
}

type fakeGKEClient struct {
	createNodePoolVerificationFunc func(spec *NodePoolSpec) error
}

var _ AutoscalingGkeClient = &fakeGKEClient{}

func (f *fakeGKEClient) GetCluster() (Cluster, error) {
	return Cluster{}, nil
}

func (f *fakeGKEClient) DeleteNodePool(pool string) error {
	return nil
}

func (f *fakeGKEClient) CreateNodePool(name string, spec *NodePoolSpec) error {
	return f.createNodePoolVerificationFunc(spec)
}

func (f *fakeGKEClient) UpdateNodePoolLabels(name string, labels map[string]string) error {
	return nil
}

func ensureLegacyMTNodepoolSpec(t *testing.T, network, subnetwork, podRange string) func(*NodePoolSpec) error {
	t.Helper()
	return func(spec *NodePoolSpec) error {
		t.Helper()
		if spec.Network != "" {
			return fmt.Errorf("got network: %v, want: empty", spec.Network)
		}
		if spec.Subnetwork != "" {
			return fmt.Errorf("got subnetwork: %v, want: empty", spec.Subnetwork)
		}
		if spec.PodRange != podRange {
			return fmt.Errorf("got pod range: %v, want: %v", spec.PodRange, podRange)
		}
		vpcLabel := spec.Labels[multitenancy.VPCLabel]
		if vpcLabel != resourceNameFromFullPath(network) {
			return fmt.Errorf("got VPC label: %v, want: %v", vpcLabel, resourceNameFromFullPath(network))
		}
		subnetLabel := spec.Labels[multitenancy.SubnetLabel]
		if subnetLabel != resourceNameFromFullPath(subnetwork) {
			return fmt.Errorf("got Subnet label: %v, want: %v", subnetLabel, resourceNameFromFullPath(subnetwork))
		}
		return nil
	}
}
