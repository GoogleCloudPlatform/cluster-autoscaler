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

func TestResourceLabels(t *testing.T) {
	feature := newResourceLabels()

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

	t.Run("FromCccSpec - empty ResourceLabels", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ResourceLabels: map[string]v1.ResourceLabelValue{},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - with ResourceLabels", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ResourceLabels: map[string]v1.ResourceLabelValue{
					"env":  "production",
					"team": "analytics",
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			"resource-labels.cloud.google.com/env":  "production",
			"resource-labels.cloud.google.com/team": "analytics",
		}, metadata)
	})

	t.Run("FromCccSpec - with empty value", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NodePoolConfig: &v1.NodePoolConfig{
				ResourceLabels: map[string]v1.ResourceLabelValue{
					"empty-val": "",
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			"resource-labels.cloud.google.com/empty-val": "",
		}, metadata)
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

	t.Run("FromNodepool - empty ResourceLabels", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				ResourceLabels: map[string]string{},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - with ResourceLabels", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				ResourceLabels: map[string]string{
					"billing-id": "12345",
					"dept":       "engineering",
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			"resource-labels.cloud.google.com/billing-id": "12345",
			"resource-labels.cloud.google.com/dept":       "engineering",
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
			"resource-labels.cloud.google.com/env": "production",
		})
		assert.Equal(t, map[string]string{"foo": "bar"}, labels)
	})

	t.Run("ToNodepool - nil np", func(t *testing.T) {
		assert.NotPanics(t, func() {
			feature.ToNodepool(nil, Metadata{
				"resource-labels.cloud.google.com/env": "production",
			})
		})
	})

	t.Run("ToNodepool - empty metadata", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		feature.ToNodepool(np, Metadata{})
		assert.Nil(t, np.Config.ResourceLabels)
	})

	t.Run("ToNodepool - unrelated metadata keys ignored", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		feature.ToNodepool(np, Metadata{
			"unrelated-key": "value",
		})
		assert.Nil(t, np.Config.ResourceLabels)
	})

	t.Run("ToNodepool - sets ResourceLabels on empty config", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, Metadata{
			"resource-labels.cloud.google.com/env":  "prod",
			"resource-labels.cloud.google.com/tier": "frontend",
		})
		assert.NotNil(t, np.Config)
		assert.Equal(t, map[string]string{
			"env":  "prod",
			"tier": "frontend",
		}, np.Config.ResourceLabels)
	})

	t.Run("ToNodepool - merges with existing ResourceLabels", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				ResourceLabels: map[string]string{
					"existing": "keep-me",
				},
			},
		}
		feature.ToNodepool(np, Metadata{
			"resource-labels.cloud.google.com/env": "prod",
		})
		assert.Equal(t, map[string]string{
			"existing": "keep-me",
			"env":      "prod",
		}, np.Config.ResourceLabels)
	})

	t.Run("ToNodepool - empty key prefix ignored", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		feature.ToNodepool(np, Metadata{
			"resource-labels.cloud.google.com/": "value",
		})
		assert.Nil(t, np.Config.ResourceLabels)
	})
}
