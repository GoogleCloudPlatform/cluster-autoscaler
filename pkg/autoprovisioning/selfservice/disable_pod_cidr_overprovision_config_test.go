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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/utils/ptr"
)

func TestDisablePodCidrOverprovisionConfig(t *testing.T) {
	feature := newDisablePodCidrOverprovisionConfig()

	t.Run("FromCccSpec - empty spec", func(t *testing.T) {
		spec := v1.ComputeClassSpec{}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - nil NetworkConfig", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: nil,
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - nil DisablePodCidrOverprovisionConfig", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				DisablePodCidrOverprovisionConfig: nil,
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - DisablePodCidrOverprovisionConfig is true", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				DisablePodCidrOverprovisionConfig: ptr.To(true),
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			disablePodCidrOverprovisionConfigMetadataKey: "true",
		}, metadata)
	})

	t.Run("FromCccSpec - DisablePodCidrOverprovisionConfig is false", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				DisablePodCidrOverprovisionConfig: ptr.To(false),
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			disablePodCidrOverprovisionConfigMetadataKey: "false",
		}, metadata)
	})

	t.Run("FromNodepool - nil np", func(t *testing.T) {
		metadata := feature.FromNodepool(nil)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - nil NetworkConfig", func(t *testing.T) {
		np := &container.NodePool{}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - nil PodCidrOverprovisionConfig", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				PodCidrOverprovisionConfig: nil,
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - Disable is true", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				PodCidrOverprovisionConfig: &container.PodCIDROverprovisionConfig{
					Disable: true,
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			disablePodCidrOverprovisionConfigMetadataKey: "true",
		}, metadata)
	})

	t.Run("FromNodepool - Disable is false", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				PodCidrOverprovisionConfig: &container.PodCIDROverprovisionConfig{
					Disable: false,
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			disablePodCidrOverprovisionConfigMetadataKey: "false",
		}, metadata)
	})

	t.Run("FromPriority - returns nil", func(t *testing.T) {
		metadata := feature.FromPriority(v1.Priority{})
		assert.Nil(t, metadata)
	})

	t.Run("FromLabelRequirements - returns nil", func(t *testing.T) {
		metadata := feature.FromLabelRequirements(podrequirements.LabelRequirements{})
		assert.Nil(t, metadata)
	})

	t.Run("ToNodePoolLabels - no-op", func(t *testing.T) {
		labels := map[string]string{"foo": "bar"}
		feature.ToNodePoolLabels(labels, Metadata{
			disablePodCidrOverprovisionConfigMetadataKey: "true",
		})
		assert.Equal(t, map[string]string{"foo": "bar"}, labels)
	})

	t.Run("ToNodepool - nil np", func(t *testing.T) {
		assert.NotPanics(t, func() {
			feature.ToNodepool(nil, Metadata{
				disablePodCidrOverprovisionConfigMetadataKey: "true",
			})
		})
	})

	t.Run("ToNodepool - empty metadata", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{},
		}
		feature.ToNodepool(np, Metadata{})
		assert.Nil(t, np.NetworkConfig.PodCidrOverprovisionConfig)
	})

	t.Run("ToNodepool - unrelated metadata keys ignored", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{},
		}
		feature.ToNodepool(np, Metadata{
			"unrelated-key": "value",
		})
		assert.Nil(t, np.NetworkConfig.PodCidrOverprovisionConfig)
	})

	t.Run("ToNodepool - sets Disable=true and ForceSendFields on empty NetworkConfig", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, Metadata{
			disablePodCidrOverprovisionConfigMetadataKey: "true",
		})
		assert.NotNil(t, np.NetworkConfig)
		assert.NotNil(t, np.NetworkConfig.PodCidrOverprovisionConfig)
		assert.True(t, np.NetworkConfig.PodCidrOverprovisionConfig.Disable)
		assert.Contains(t, np.NetworkConfig.PodCidrOverprovisionConfig.ForceSendFields, "Disable")
	})

	t.Run("ToNodepool - sets Disable=false and ForceSendFields", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{},
		}
		feature.ToNodepool(np, Metadata{
			disablePodCidrOverprovisionConfigMetadataKey: "false",
		})
		assert.NotNil(t, np.NetworkConfig.PodCidrOverprovisionConfig)
		assert.False(t, np.NetworkConfig.PodCidrOverprovisionConfig.Disable)
		assert.Contains(t, np.NetworkConfig.PodCidrOverprovisionConfig.ForceSendFields, "Disable")
	})

	t.Run("ToNodepool - does not duplicate ForceSendFields", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				PodCidrOverprovisionConfig: &container.PodCIDROverprovisionConfig{
					ForceSendFields: []string{"Disable"},
				},
			},
		}
		feature.ToNodepool(np, Metadata{
			disablePodCidrOverprovisionConfigMetadataKey: "true",
		})
		assert.Equal(t, []string{"Disable"}, np.NetworkConfig.PodCidrOverprovisionConfig.ForceSendFields)
	})

	t.Run("ToNodepool - malformed metadata string ignored", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{},
		}
		feature.ToNodepool(np, Metadata{
			disablePodCidrOverprovisionConfigMetadataKey: "not-a-bool",
		})
		assert.Nil(t, np.NetworkConfig.PodCidrOverprovisionConfig)
	})
}
