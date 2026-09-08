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

func TestSubnetPriorities(t *testing.T) {
	feature := newSubnetPriorities()

	t.Run("FromCccSpec - empty", func(t *testing.T) {
		spec := v1.ComputeClassSpec{}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - empty subnet priorities", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				SubnetPriorities: []v1.SubnetPriority{},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Nil(t, metadata)
	})

	t.Run("FromCccSpec - with subnet and podRange", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				SubnetPriorities: []v1.SubnetPriority{
					{
						Name:     "subnet-a",
						PodRange: "pod-range-a",
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			subnetPriorityKey: "subnet-a",
			podRangeKey:       "pod-range-a",
		}, metadata)
	})

	t.Run("FromCccSpec - with only subnet", func(t *testing.T) {
		spec := v1.ComputeClassSpec{
			NetworkConfig: &v1.NetworkConfig{
				SubnetPriorities: []v1.SubnetPriority{
					{
						Name: "subnet-a",
					},
				},
			},
		}
		metadata := feature.FromCccSpec(spec)
		assert.Equal(t, Metadata{
			subnetPriorityKey: "subnet-a",
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

	t.Run("FromNodepool - short name and podRange", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				Subnetwork: "test-subnet",
				PodRange:   "test-pod-range",
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			subnetPriorityKey: "test-subnet",
			podRangeKey:       "test-pod-range",
		}, metadata)
	})

	t.Run("FromNodepool - full URI and podRange", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				Subnetwork: "projects/my-project/regions/us-central1/subnetworks/my-subnet",
				PodRange:   "my-pod-range",
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			subnetPriorityKey: "my-subnet",
			podRangeKey:       "my-pod-range",
		}, metadata)
	})

	t.Run("FromNodepool - only podRange", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				PodRange: "my-pod-range",
			},
		}
		metadata := feature.FromNodepool(np)
		assert.Equal(t, Metadata{
			podRangeKey: "my-pod-range",
		}, metadata)
	})

	t.Run("ToNodepool - nil np", func(t *testing.T) {
		metadata := Metadata{
			subnetPriorityKey: "test-subnet",
			podRangeKey:       "test-pod-range",
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

	t.Run("ToNodepool - sets subnet and podRange", func(t *testing.T) {
		np := &container.NodePool{}
		metadata := Metadata{
			subnetPriorityKey: "test-subnet",
			podRangeKey:       "test-pod-range",
		}
		feature.ToNodepool(np, metadata)
		assert.NotNil(t, np.NetworkConfig)
		assert.Equal(t, "test-subnet", np.NetworkConfig.Subnetwork)
		assert.Equal(t, "test-pod-range", np.NetworkConfig.PodRange)
		assert.Contains(t, np.NetworkConfig.ForceSendFields, "Subnetwork")
		assert.Contains(t, np.NetworkConfig.ForceSendFields, "PodRange")
	})

	t.Run("ToNodepool - appends to existing ForceSendFields without duplicating", func(t *testing.T) {
		np := &container.NodePool{
			NetworkConfig: &container.NodeNetworkConfig{
				ForceSendFields: []string{"Subnetwork", "OtherField"},
			},
		}
		metadata := Metadata{
			subnetPriorityKey: "test-subnet",
			podRangeKey:       "test-pod-range",
		}
		feature.ToNodepool(np, metadata)
		assert.Equal(t, "test-subnet", np.NetworkConfig.Subnetwork)
		assert.Equal(t, "test-pod-range", np.NetworkConfig.PodRange)
		assert.Equal(t, []string{"Subnetwork", "OtherField", "PodRange"}, np.NetworkConfig.ForceSendFields)
	})

	t.Run("FromPriority - returns nil", func(t *testing.T) {
		assert.Nil(t, feature.FromPriority(v1.Priority{}))
	})

	t.Run("FromLabelRequirements - returns nil", func(t *testing.T) {
		assert.Nil(t, feature.FromLabelRequirements(podrequirements.LabelRequirements{}))
	})

	t.Run("ToNodePoolLabels - no op", func(t *testing.T) {
		labels := map[string]string{"foo": "bar"}
		feature.ToNodePoolLabels(labels, Metadata{subnetPriorityKey: "test"})
		assert.Equal(t, map[string]string{"foo": "bar"}, labels)
	})
}
