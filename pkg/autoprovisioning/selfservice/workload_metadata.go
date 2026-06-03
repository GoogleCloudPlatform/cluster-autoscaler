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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

type workloadMetadata struct {
	internalFeatureDefaultImplementation
}

func newWorkloadMetadata() feature {
	return &workloadMetadata{}
}

func (w *workloadMetadata) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil || pool.Config.WorkloadMetadataConfig == nil || pool.Config.WorkloadMetadataConfig.Mode == "" || pool.Config.WorkloadMetadataConfig.Mode == "MODE_UNSPECIFIED" {
		return nil
	}
	return Metadata{labels.WorkloadMetadataLabelKey: pool.Config.WorkloadMetadataConfig.Mode}
}

func (w *workloadMetadata) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || spec.NodePoolConfig.WorkloadMetadata == nil || *spec.NodePoolConfig.WorkloadMetadata == "" {
		return nil
	}
	return Metadata{labels.WorkloadMetadataLabelKey: *spec.NodePoolConfig.WorkloadMetadata}
}

func (w *workloadMetadata) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	mode, ok := metadata[labels.WorkloadMetadataLabelKey]
	if !ok || mode == "" {
		return
	}

	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}
	if pool.Config.WorkloadMetadataConfig == nil {
		pool.Config.WorkloadMetadataConfig = &gke_api_beta.WorkloadMetadataConfig{}
	}
	pool.Config.WorkloadMetadataConfig.Mode = mode
}

func (w *workloadMetadata) FromPriority(p v1.Priority) Metadata {
	return nil
}

func (w *workloadMetadata) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

func (w *workloadMetadata) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}
