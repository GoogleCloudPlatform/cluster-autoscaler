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

package selfservice

import (
	"testing"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	"github.com/stretchr/testify/assert"
	container "google.golang.org/api/container/v1beta1"
	netutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

func TestAdditionalNetworks(t *testing.T) {
	feature := newAdditionalNetworks()

	t.Run("FromCccSpec - empty", func(t *testing.T) {
		spec := v1.ComputeClassSpec{}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - empty additional networks", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				AdditionalNetworks: []v1.AdditionalNetwork{},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - additional network with empty subnet priorities", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				AdditionalNetworks: []v1.AdditionalNetwork{
					{
						Name:             "net-1",
						SubnetPriorities: []v1.SubnetPriority{},
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - additional network with empty network name or subnet name", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				AdditionalNetworks: []v1.AdditionalNetwork{
					{
						Name: "",
						SubnetPriorities: []v1.SubnetPriority{
							{
								Name: "subnet-1",
							},
						},
					},
					{
						Name: "net-1",
						SubnetPriorities: []v1.SubnetPriority{
							{
								Name: "",
							},
						},
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - with network, subnet, and podRange", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				AdditionalNetworks: []v1.AdditionalNetwork{
					{
						Name: "net-1",
						SubnetPriorities: []v1.SubnetPriority{
							{
								Name:     "subnet-1",
								PodRange: "pod-range-1",
							},
						},
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1","podRange":"pod-range-1"}]`,
		}, metadata)
	})

	t.Run("FromCccSpec - node-only network without podRange", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				AdditionalNetworks: []v1.AdditionalNetwork{
					{
						Name: "net-1",
						SubnetPriorities: []v1.SubnetPriority{
							{
								Name: "subnet-1",
							},
						},
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1"}]`,
		}, metadata)
	})

	t.Run("FromCccSpec - multiple mixed node+pod and node-only networks", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				AdditionalNetworks: []v1.AdditionalNetwork{
					{
						Name: "net-1",
						SubnetPriorities: []v1.SubnetPriority{
							{
								Name:     "subnet-1",
								PodRange: "pod-range-1",
							},
							{
								Name:     "subnet-1-fallback",
								PodRange: "pod-range-1-fallback",
							},
						},
					},
					{
						Name: "net-2",
						SubnetPriorities: []v1.SubnetPriority{
							{
								Name: "subnet-2",
							},
						},
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1","podRange":"pod-range-1"},{"network":"net-2","subnetwork":"subnet-2"}]`,
		}, metadata)
	})

	t.Run("FromNodepool - nil np", func(t *testing.T) {
		metadata := feature.FromNodepool(nil)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - empty NetworkConfig", func(t *testing.T) {
		np := &container.NodePool{}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - empty AdditionalNodeNetworkConfigs", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{},
		}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - short names with podRange", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				AdditionalNodeNetworkConfigs: []*container.AdditionalNodeNetworkConfig{
					{
						Network:    "net-1",
						Subnetwork: "subnet-1",
					},
				},
				AdditionalPodNetworkConfigs: []*container.AdditionalPodNetworkConfig{
					{
						Subnetwork:        "subnet-1",
						SecondaryPodRange: "pod-range-1",
					},
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1","podRange":"pod-range-1"}]`,
		}, metadata)
	})

	t.Run("FromNodepool - full GCP resource URIs and mixed node+pod / node-only networks", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				AdditionalNodeNetworkConfigs: []*container.AdditionalNodeNetworkConfig{
					{
						Network:    "projects/my-project/global/networks/net-1",
						Subnetwork: "projects/my-project/regions/us-central1/subnetworks/subnet-1",
					},
					{
						Network:    "projects/my-project/global/networks/net-2",
						Subnetwork: "projects/my-project/regions/us-central1/subnetworks/subnet-2",
					},
				},
				AdditionalPodNetworkConfigs: []*container.AdditionalPodNetworkConfig{
					{
						Subnetwork:        "projects/my-project/regions/us-central1/subnetworks/subnet-1",
						SecondaryPodRange: "pod-range-1",
					},
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1","podRange":"pod-range-1"},{"network":"net-2","subnetwork":"subnet-2"}]`,
		}, metadata)
	})

	t.Run("ToNodepool - nil np", func(t *testing.T) {
		metadata := Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1","podRange":"pod-range-1"}]`,
		}
		assert.NotPanics(t, func() {
			feature.ToNodepool(nil, metadata)
		})
	})

	t.Run("ToNodepool - empty metadata", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, Metadata{})
		assert.Nil(t, np.NetworkConfig)
	})

	t.Run("ToNodepool - invalid JSON metadata is ignored", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, Metadata{additionalNetworksKey: "not-valid-json"})
		assert.Nil(t, np.NetworkConfig)
	})

	t.Run("ToNodepool - sets node-only network without AdditionalPodNetworkConfigs", func(t *testing.T) {
		np := &container.NodePool{}
		metadata := Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1"}]`,
		}
		feature.ToNodepool(np, metadata)
		assert.NotNil(t, np.NetworkConfig)
		assert.Equal(t, []*container.AdditionalNodeNetworkConfig{
			{
				Network:    "net-1",
				Subnetwork: "subnet-1",
			},
		}, np.NetworkConfig.AdditionalNodeNetworkConfigs)
		assert.Empty(t, np.NetworkConfig.AdditionalPodNetworkConfigs)
		assert.Equal(t, []string{"AdditionalNodeNetworkConfigs"}, np.NetworkConfig.ForceSendFields)
	})

	t.Run("ToNodepool - sets mixed node+pod and node-only networks", func(t *testing.T) {
		np := &container.NodePool{}
		metadata := Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1","podRange":"pod-range-1"},{"network":"net-2","subnetwork":"subnet-2"}]`,
		}
		feature.ToNodepool(np, metadata)
		assert.NotNil(t, np.NetworkConfig)
		assert.Equal(t, []*container.AdditionalNodeNetworkConfig{
			{
				Network:    "net-1",
				Subnetwork: "subnet-1",
			},
			{
				Network:    "net-2",
				Subnetwork: "subnet-2",
			},
		}, np.NetworkConfig.AdditionalNodeNetworkConfigs)
		assert.Equal(t, []*container.AdditionalPodNetworkConfig{
			{
				Subnetwork:        "subnet-1",
				SecondaryPodRange: "pod-range-1",
				MaxPodsPerNode: &container.MaxPodsConstraint{
					MaxPodsPerNode: netutil.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode,
				},
			},
		}, np.NetworkConfig.AdditionalPodNetworkConfigs)
		assert.Equal(t, []string{"AdditionalNodeNetworkConfigs", "AdditionalPodNetworkConfigs"}, np.NetworkConfig.ForceSendFields)
	})

	t.Run("ToNodepool - appends to existing ForceSendFields without duplicating", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				ForceSendFields: []string{"AdditionalNodeNetworkConfigs", "OtherField"},
			},
		}
		metadata := Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1","podRange":"pod-range-1"}]`,
		}
		feature.ToNodepool(np, metadata)
		assert.Equal(t, []string{"AdditionalNodeNetworkConfigs", "OtherField", "AdditionalPodNetworkConfigs"}, np.NetworkConfig.ForceSendFields)
	})

	t.Run("ToNodepool - overwrites existing network and subnetwork configs in-place and appends new ones", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				AdditionalNodeNetworkConfigs: []*container.AdditionalNodeNetworkConfig{
					{
						Network:    "net-other",
						Subnetwork: "subnet-other",
					},
					{
						Network:    "net-1",
						Subnetwork: "subnet-1",
					},
				},
				AdditionalPodNetworkConfigs: []*container.AdditionalPodNetworkConfig{
					{
						Subnetwork:        "subnet-other",
						SecondaryPodRange: "pod-range-other",
						MaxPodsPerNode: &container.MaxPodsConstraint{
							MaxPodsPerNode: netutil.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode,
						},
					},
					{
						Subnetwork:        "subnet-1",
						SecondaryPodRange: "random-pod-range-from-gnp",
						MaxPodsPerNode: &container.MaxPodsConstraint{
							MaxPodsPerNode: netutil.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode,
						},
					},
				},
			},
		}
		metadata := Metadata{
			additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1","podRange":"ccc-pod-range-1"},{"network":"net-2","subnetwork":"subnet-2","podRange":"ccc-pod-range-2"}]`,
		}
		feature.ToNodepool(np, metadata)
		assert.Equal(t, []*container.AdditionalNodeNetworkConfig{
			{
				Network:    "net-other",
				Subnetwork: "subnet-other",
			},
			{
				Network:    "net-1",
				Subnetwork: "subnet-1",
			},
			{
				Network:    "net-2",
				Subnetwork: "subnet-2",
			},
		}, np.NetworkConfig.AdditionalNodeNetworkConfigs)
		assert.Equal(t, []*container.AdditionalPodNetworkConfig{
			{
				Subnetwork:        "subnet-other",
				SecondaryPodRange: "pod-range-other",
				MaxPodsPerNode: &container.MaxPodsConstraint{
					MaxPodsPerNode: netutil.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode,
				},
			},
			{
				Subnetwork:        "subnet-1",
				SecondaryPodRange: "ccc-pod-range-1",
				MaxPodsPerNode: &container.MaxPodsConstraint{
					MaxPodsPerNode: netutil.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode,
				},
			},
			{
				Subnetwork:        "subnet-2",
				SecondaryPodRange: "ccc-pod-range-2",
				MaxPodsPerNode: &container.MaxPodsConstraint{
					MaxPodsPerNode: netutil.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode,
				},
			},
		}, np.NetworkConfig.AdditionalPodNetworkConfigs)
	})

	t.Run("FromPriority - returns nil", func(t *testing.T) {
		assert.Nil(t, feature.FromPriority(v1.Priority{}))
	})

	t.Run("FromLabelRequirements - returns nil", func(t *testing.T) {
		assert.Nil(t, feature.FromLabelRequirements(podrequirements.LabelRequirements{}))
	})

	t.Run("ToNodePoolLabels - no op", func(t *testing.T) {
		labels := map[string]string{"foo": "bar"}
		feature.ToNodePoolLabels(labels, Metadata{additionalNetworksKey: `[{"network":"net-1","subnetwork":"subnet-1"}]`})
		assert.Equal(t, map[string]string{"foo": "bar"}, labels)
	})
}
