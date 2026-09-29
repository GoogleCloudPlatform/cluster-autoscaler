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

func TestPerformanceMonitoringUnit(t *testing.T) {
	feature := newPerformanceMonitoringUnit()

	t.Run("FromPriority - nil PerformanceMonitoringUnit", func(t *testing.T) {
		p := v1.Priority{}
		metadata := feature.FromPriority(p)
		assert.Nil(t, metadata)
	})

	t.Run("FromPriority - empty PerformanceMonitoringUnit", func(t *testing.T) {
		p := v1.Priority{
			PerformanceMonitoringUnit: ptr.To(""),
		}
		metadata := feature.FromPriority(p)
		assert.Nil(t, metadata)
	})

	t.Run("FromPriority - ARCHITECTURAL", func(t *testing.T) {
		p := v1.Priority{
			PerformanceMonitoringUnit: ptr.To("ARCHITECTURAL"),
		}
		metadata := feature.FromPriority(p)
		assert.Equal(t, Metadata{
			PerformanceMonitoringUnitMetadataKey: "ARCHITECTURAL",
		}, metadata)
	})

	t.Run("FromPriority - STANDARD", func(t *testing.T) {
		p := v1.Priority{
			PerformanceMonitoringUnit: ptr.To("STANDARD"),
		}
		metadata := feature.FromPriority(p)
		assert.Equal(t, Metadata{
			PerformanceMonitoringUnitMetadataKey: "STANDARD",
		}, metadata)
	})

	t.Run("FromPriority - ENHANCED", func(t *testing.T) {
		p := v1.Priority{
			PerformanceMonitoringUnit: ptr.To("ENHANCED"),
		}
		metadata := feature.FromPriority(p)
		assert.Equal(t, Metadata{
			PerformanceMonitoringUnitMetadataKey: "ENHANCED",
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

	t.Run("FromNodepool - nil AdvancedMachineFeatures", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - empty PerformanceMonitoringUnit", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				AdvancedMachineFeatures: &container.AdvancedMachineFeatures{},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Nil(t, metadata)
	})

	t.Run("FromNodepool - ARCHITECTURAL", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				AdvancedMachineFeatures: &container.AdvancedMachineFeatures{
					PerformanceMonitoringUnit: "ARCHITECTURAL",
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			PerformanceMonitoringUnitMetadataKey: "ARCHITECTURAL",
		}, metadata)
	})

	t.Run("FromNodepool - STANDARD", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				AdvancedMachineFeatures: &container.AdvancedMachineFeatures{
					PerformanceMonitoringUnit: "STANDARD",
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			PerformanceMonitoringUnitMetadataKey: "STANDARD",
		}, metadata)
	})

	t.Run("FromNodepool - ENHANCED", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				AdvancedMachineFeatures: &container.AdvancedMachineFeatures{
					PerformanceMonitoringUnit: "ENHANCED",
				},
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			PerformanceMonitoringUnitMetadataKey: "ENHANCED",
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
		feature.ToNodePoolLabels(labels, Metadata{PerformanceMonitoringUnitMetadataKey: "STANDARD"})
		assert.Empty(t, labels)
	})

	t.Run("ToNodepool - nil np", func(t *testing.T) {
		// Should not panic on empty or present metadata
		feature.ToNodepool(nil, Metadata{})
		feature.ToNodepool(nil, Metadata{PerformanceMonitoringUnitMetadataKey: "ARCHITECTURAL"})
	})

	t.Run("ToNodepool - empty metadata", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{},
		}
		feature.ToNodepool(np, Metadata{})
		assert.Nil(t, np.Config.AdvancedMachineFeatures)
	})

	t.Run("ToNodepool - sets PerformanceMonitoringUnit and ForceSendFields", func(t *testing.T) {
		np := &container.NodePool{}
		feature.ToNodepool(np, Metadata{
			PerformanceMonitoringUnitMetadataKey: "ARCHITECTURAL",
		})
		assert.NotNil(t, np.Config)
		assert.NotNil(t, np.Config.AdvancedMachineFeatures)
		assert.Equal(t, "ARCHITECTURAL", np.Config.AdvancedMachineFeatures.PerformanceMonitoringUnit)
		assert.Equal(t, []string{"PerformanceMonitoringUnit"}, np.Config.AdvancedMachineFeatures.ForceSendFields)
	})

	t.Run("ToNodepool - appends to ForceSendFields without duplicate", func(t *testing.T) {
		np := &container.NodePool{
			Config: &container.NodeConfig{
				AdvancedMachineFeatures: &container.AdvancedMachineFeatures{
					ForceSendFields: []string{"EnableNestedVirtualization"},
				},
			},
		}
		feature.ToNodepool(np, Metadata{
			PerformanceMonitoringUnitMetadataKey: "ENHANCED",
		})
		assert.Equal(t, "ENHANCED", np.Config.AdvancedMachineFeatures.PerformanceMonitoringUnit)
		assert.Equal(t, []string{"EnableNestedVirtualization", "PerformanceMonitoringUnit"}, np.Config.AdvancedMachineFeatures.ForceSendFields)

		// Calling again shouldn't duplicate
		feature.ToNodepool(np, Metadata{
			PerformanceMonitoringUnitMetadataKey: "ENHANCED",
		})
		assert.Equal(t, []string{"EnableNestedVirtualization", "PerformanceMonitoringUnit"}, np.Config.AdvancedMachineFeatures.ForceSendFields)
	})
}
