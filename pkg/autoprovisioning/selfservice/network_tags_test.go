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
)

func TestNetworkTags(t *testing.T) {
	feature := newNetworkTags()

	t.Run("FromCccSpec - empty spec", func(t *testing.T) {
		spec := v1.ComputeClassSpec{}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - nil NodePoolConfig", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: nil,
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - empty NetworkTags", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				NetworkTags: []string{},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - with NetworkTags", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				NetworkTags: []string{
					"allow-ssh",
					"secure-firewall",
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			"network-tags.cloud.google.com/allow-ssh":       "true",
			"network-tags.cloud.google.com/secure-firewall": "true",
		}, metadata)
	})

	t.Run("FromCccSpec - with empty tag string", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				NetworkTags: []string{""},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - nil np", func(t *testing.T) {
		metadata := feature.FromNodepool(nil)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - nil Config", func(t *testing.T) {
		np := &container.NodePool{}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - empty Tags", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				Tags: []string{},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - with Tags", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				Tags: []string{
					"allow-ssh",
					"secure-firewall",
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			"network-tags.cloud.google.com/allow-ssh":       "true",
			"network-tags.cloud.google.com/secure-firewall": "true",
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
			"network-tags.cloud.google.com/tag": "true",
		})
		assert.Equal(t, map[string]string{"foo": "bar"}, labels)
	})

	t.Run("ToNodepool - nil np", func(t *testing.T) {
		assert.NotPanics(t, func() {
			feature.ToNodepool(nil, Metadata{
				"network-tags.cloud.google.com/tag": "true",
			})
		})
	})

	t.Run("ToNodepool - empty metadata", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		feature.ToNodepool(np, Metadata{})
		assert.Nil(t, np.Config.Tags)
	})

	t.Run("ToNodepool - unrelated metadata keys ignored", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		feature.ToNodepool(np, Metadata{
			"unrelated-key": "value",
		})
		assert.Nil(t, np.Config.Tags)
	})

	t.Run("ToNodepool - non-true values ignored", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		feature.ToNodepool(np, Metadata{
			"network-tags.cloud.google.com/tag1": "false",
			"network-tags.cloud.google.com/tag2": "other",
		})
		assert.Nil(t, np.Config.Tags)
	})

	t.Run("ToNodepool - sets sorted Tags on empty config", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, Metadata{
			"network-tags.cloud.google.com/tag-z": "true",
			"network-tags.cloud.google.com/tag-a": "true",
			"network-tags.cloud.google.com/tag-m": "true",
		})
		assert.NotNil(t, np.Config)
		assert.Equal(t, []string{"tag-a", "tag-m", "tag-z"}, np.Config.Tags)
	})

	t.Run("ToNodepool - empty tag prefix ignored", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		feature.ToNodepool(np, Metadata{
			"network-tags.cloud.google.com/": "true",
		})
		assert.Nil(t, np.Config.Tags)
	})

	t.Run("Unordered tags between CCC spec and NodePool produce identical metadata and reconstruct sorted", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				NetworkTags: []string{"hello", "lessgo"},
			},
		}
		np := &container.NodePool{
			Config: &container.NodeConfig{
				Tags: []string{"lessgo", "hello"},
			},
		}
		cccMetadata := feature.FromCccSpec(spec)
		npMetadata := feature.FromNodepool(np)
		assert.Equal(t, cccMetadata, npMetadata)

		targetNp := &container.NodePool{}
		feature.ToNodepool(targetNp, cccMetadata)
		assert.Equal(t, []string{"hello", "lessgo"}, targetNp.Config.Tags)
	})
}
