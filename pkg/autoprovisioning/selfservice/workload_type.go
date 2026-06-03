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
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// workloadType is a self-service feature that allows setting workload type
// label for matching CCCs to be passed to node pools by NAP.
type workloadType struct {
	internalFeatureDefaultImplementation
}

func newWorkloadType() feature {
	return &workloadType{}
}

func (w *workloadType) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil {
		return nil
	}

	m := make(Metadata)
	if v, found := pool.Config.Labels[gkelabels.WorkloadTypeLabel]; found {
		m[gkelabels.WorkloadTypeLabel] = v
	}
	return m
}

func (w *workloadType) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (w *workloadType) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	m := make(Metadata)
	if spec.NodePoolConfig != nil && spec.NodePoolConfig.WorkloadType != "" {
		m[gkelabels.WorkloadTypeLabel] = spec.NodePoolConfig.WorkloadType
	}
	return m
}

func (w *workloadType) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (w *workloadType) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
	if v, found := metadata[gkelabels.WorkloadTypeLabel]; found {
		labels[gkelabels.WorkloadTypeLabel] = v
	}
}

func (w *workloadType) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if v, found := metadata[gkelabels.WorkloadTypeLabel]; found {
		if pool.Config == nil {
			pool.Config = &gke_api_beta.NodeConfig{}
		}
		if pool.Config.Labels == nil {
			pool.Config.Labels = make(map[string]string)
		}
		pool.Config.Labels[gkelabels.WorkloadTypeLabel] = v
	}
}
