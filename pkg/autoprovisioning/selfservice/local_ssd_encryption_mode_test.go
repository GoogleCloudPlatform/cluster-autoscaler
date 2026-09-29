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

func TestLocalSSDEncryptionMode(t *testing.T) {
	feature := newLocalSSDEncryptionMode()

	t.Run("FromPriority - nil storage", func(t *testing.T) {
		p := v1.Priority{}
		metadata := feature.FromPriority(p)
		assert.Nil(t, metadata)
	})

	t.Run("FromPriority - nil LocalSSDEncryptionMode", func(t *testing.T) {
		p := v1.Priority{
			Storage: &v1.Storage{},
		}
		metadata := feature.FromPriority(p)
		assert.Nil(t, metadata)
	})

	t.Run("FromPriority - empty LocalSSDEncryptionMode", func(t *testing.T) {
		p := v1.Priority{
			Storage: &v1.Storage{
				LocalSSDEncryptionMode: ptr.To(""),
			},
		}
		metadata := feature.FromPriority(p)
		assert.Nil(t, metadata)
	})

	t.Run("FromPriority - STANDARD_ENCRYPTION", func(t *testing.T) {
		p := v1.Priority{
			Storage: &v1.Storage{
				LocalSSDEncryptionMode: ptr.To("STANDARD_ENCRYPTION"),
			},
		}
		metadata := feature.FromPriority(p)
		assert.Equal(t, Metadata{
			LocalSSDEncryptionModeMetadataKey: "STANDARD_ENCRYPTION",
		}, metadata)
	})

	t.Run("FromPriority - EPHEMERAL_KEY_ENCRYPTION", func(t *testing.T) {
		p := v1.Priority{
			Storage: &v1.Storage{
				LocalSSDEncryptionMode: ptr.To("EPHEMERAL_KEY_ENCRYPTION"),
			},
		}
		metadata := feature.FromPriority(p)
		assert.Equal(t, Metadata{
			LocalSSDEncryptionModeMetadataKey: "EPHEMERAL_KEY_ENCRYPTION",
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

	t.Run("FromNodepool - empty LocalSsdEncryptionMode", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - STANDARD_ENCRYPTION", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				LocalSsdEncryptionMode: "STANDARD_ENCRYPTION",
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			LocalSSDEncryptionModeMetadataKey: "STANDARD_ENCRYPTION",
		}, metadata)
	})

	t.Run("FromNodepool - EPHEMERAL_KEY_ENCRYPTION", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				LocalSsdEncryptionMode: "EPHEMERAL_KEY_ENCRYPTION",
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			LocalSSDEncryptionModeMetadataKey: "EPHEMERAL_KEY_ENCRYPTION",
		}, metadata)
	})

	t.Run("FromCccSpec - returns nil", func(t *testing.T) {
		metadata := feature.FromCccSpec(v1.ComputeClassSpec{})
		assert.Nil(t, metadata)
	})

	t.Run("FromLabelRequirements - returns nil", func(t *testing.T) {
		metadata := feature.FromLabelRequirements(podrequirements.LabelRequirements{})
		assert.Nil(t, metadata)
	})

	t.Run("ToNodePoolLabels - no op", func(t *testing.T) {
		labels := make(map[string]string)
		feature.ToNodePoolLabels(labels, Metadata{LocalSSDEncryptionModeMetadataKey: "STANDARD_ENCRYPTION"})
		assert.Empty(t, labels)
	})

	t.Run("ToNodepool - nil np", func(t *testing.T) {
		// Should not panic on empty metadata
		feature.ToNodepool(nil, Metadata{})
	})

	t.Run("ToNodepool - empty metadata", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		feature.ToNodepool(np, Metadata{})
		assert.Empty(t, np.Config.LocalSsdEncryptionMode)
	})

	t.Run("ToNodepool - sets LocalSsdEncryptionMode", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, Metadata{
			LocalSSDEncryptionModeMetadataKey: "EPHEMERAL_KEY_ENCRYPTION",
		})
		assert.NotNil(t, np.Config)
		assert.Equal(t, "EPHEMERAL_KEY_ENCRYPTION", np.Config.LocalSsdEncryptionMode)
	})
}
