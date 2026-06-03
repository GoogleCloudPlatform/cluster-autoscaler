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
	"strings"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const instanceMetadataPrefix = "instance-metadata.cloud.google.com/"

func newInstanceMetadata() feature {
	return &instanceMetadata{}
}

type instanceMetadata struct {
	internalFeatureDefaultImplementation
}

// We use a flat metadata approach with a prefix (`instance-metadata.cloud.google.com/`)
// instead of serializing the metadata map into a JSON string under a single key.
// This is necessary because CA's matching logic needs to verify if an existing NodePool
// satisfies a ComputeClass's requirements. The underlying NodePool metadata map contains
// additional system values injected by GKE. By keeping the self-service metadata flat,
// we can natively rely on CA's existing superset matching logic
// (where the NodePool's metadata must contain a superset of the CCC's required metadata).
func prefixInstanceMetadata(instanceMetadata map[string]string) Metadata {
	m := make(Metadata)
	if instanceMetadata != nil {
		for k, v := range instanceMetadata {
			m[instanceMetadataPrefix+k] = v
		}
	}
	if len(m) > 0 {
		return m
	}
	return nil
}

func (im *instanceMetadata) FromNodepool(np *container.NodePool) Metadata {
	if np != nil && np.Config != nil {
		return prefixInstanceMetadata(np.Config.Metadata)
	}
	return nil
}

func (im *instanceMetadata) FromLabelRequirements(req podrequirements.LabelRequirements) Metadata {
	return nil
}

func (im *instanceMetadata) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig != nil {
		return prefixInstanceMetadata(spec.NodePoolConfig.InstanceMetadata)
	}
	return nil
}

func (im *instanceMetadata) FromPriority(p v1.Priority) Metadata {
	return prefixInstanceMetadata(p.InstanceMetadata)
}

func (im *instanceMetadata) ToNodePoolLabels(labels map[string]string, m Metadata) {}

func (im *instanceMetadata) ToNodepool(np *container.NodePool, m Metadata) {
	for k, v := range m {
		if strings.HasPrefix(k, instanceMetadataPrefix) {
			strippedKey := strings.TrimPrefix(k, instanceMetadataPrefix)
			if np.Config == nil {
				np.Config = &container.NodeConfig{}
			}
			if np.Config.Metadata == nil {
				np.Config.Metadata = make(map[string]string)
			}
			np.Config.Metadata[strippedKey] = v
		}
	}
}
