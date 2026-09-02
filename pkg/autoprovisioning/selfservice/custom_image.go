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
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const (
	// CustomImageTypeMetadataKey stores the node pool image type in metadata map.
	CustomImageTypeMetadataKey = "gke.custom-image-type"

	// CustomImageNameMetadataKey stores the custom container image name in metadata map.
	CustomImageNameMetadataKey = "gke.custom-image-name"

	// CustomImageProjectMetadataKey stores the custom container image project in metadata map.
	CustomImageProjectMetadataKey = "gke.custom-image-project"

	// ImageTypeCustomContainerd is the enum value enabling custom container images.
	ImageTypeCustomContainerd = "custom_containerd"
)

// customImage is a self-service feature that allows setting custom containerd operating system images in GKE.
type customImage struct {
	internalFeatureDefaultImplementation
}

// newCustomImage creates and returns the customImage self-service feature.
func newCustomImage() feature {
	return &customImage{}
}

// FromNodepool extracts custom image type and config from an existing GKE NodePool.
func (c *customImage) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil {
		return nil
	}

	m := make(Metadata)
	if pool.Config.ImageType == ImageTypeCustomContainerd {
		m[CustomImageTypeMetadataKey] = ImageTypeCustomContainerd
		if pool.Config.NodeImageConfig != nil {
			if pool.Config.NodeImageConfig.Image != "" {
				m[CustomImageNameMetadataKey] = pool.Config.NodeImageConfig.Image
			}
			if pool.Config.NodeImageConfig.ImageProject != "" {
				m[CustomImageProjectMetadataKey] = pool.Config.NodeImageConfig.ImageProject
			}
		}
	}

	if len(m) == 0 {
		return nil
	}
	return m
}

// FromLabelRequirements is not used because custom images are defined at the ComputeClass resource level, not pod labels.
func (c *customImage) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

// FromCccSpec extracts custom image parameters from the ComputeClassSpec NodePoolConfig.
func (c *customImage) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil {
		return nil
	}

	m := make(Metadata)
	if spec.NodePoolConfig.ImageType == ImageTypeCustomContainerd {
		m[CustomImageTypeMetadataKey] = ImageTypeCustomContainerd
		if spec.NodePoolConfig.CustomImageConfig != nil {
			config := spec.NodePoolConfig.CustomImageConfig
			if config.ImageName != "" {
				m[CustomImageNameMetadataKey] = config.ImageName
			}
			if config.ImageProjectId != "" {
				m[CustomImageProjectMetadataKey] = config.ImageProjectId
			}
		}
	}

	if len(m) == 0 {
		return nil
	}
	return m
}

// FromPriority is a no-op as custom images are defined globally on the class, not per priority.
func (c *customImage) FromPriority(_ v1.Priority) Metadata {
	return nil
}

// ToNodePoolLabels does not need to apply labels for custom image.
func (c *customImage) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

// ToNodepool configures GKE NodePool with custom containerd image settings during scale-up.
func (c *customImage) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if metadata[CustomImageTypeMetadataKey] != ImageTypeCustomContainerd {
		return
	}

	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}

	// 1. Configure GKE NodePool image type
	pool.Config.ImageType = ImageTypeCustomContainerd

	// 2. Setup the native CustomImageConfig (NodeImageConfig) defensively
	imageName := metadata[CustomImageNameMetadataKey]
	imageProject := metadata[CustomImageProjectMetadataKey]
	if imageName != "" || imageProject != "" {
		if pool.Config.NodeImageConfig == nil {
			pool.Config.NodeImageConfig = &gke_api_beta.CustomImageConfig{}
		}
		pool.Config.NodeImageConfig.Image = imageName
		pool.Config.NodeImageConfig.ImageProject = imageProject
	}
}
