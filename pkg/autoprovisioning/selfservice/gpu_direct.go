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

type gpuDirect struct {
	internalFeatureDefaultImplementation
}

func newGpuDirect() feature {
	return &gpuDirect{}
}

func (g *gpuDirect) FromPriority(p v1.Priority) Metadata {
	if p.GpuDirect == "" {
		return nil
	}
	m := make(Metadata)
	m[gkelabels.GpuDirectLabel] = p.GpuDirect
	return m
}

func (g *gpuDirect) FromLabelRequirements(reqs podrequirements.LabelRequirements) Metadata {
	val, found := reqs.GetValues(gkelabels.GpuDirectLabel)
	if found {
		valuesMap := val.Get()
		for v := range valuesMap {
			m := make(Metadata)
			m[gkelabels.GpuDirectLabel] = v
			return m
		}
	}
	return nil
}

// GpuDirect is a priority-specific feature, and is not expected at the general ComputeClassSpec level.
func (g *gpuDirect) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	return nil
}

func (n *gpuDirect) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
	if v, found := metadata[gkelabels.GpuDirectLabel]; found {
		labels[gkelabels.GpuDirectLabel] = v
	}
}

func (g *gpuDirect) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	v, found := metadata[gkelabels.GpuDirectLabel]
	if !found || v != "rdma" {
		return
	}
	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}
	if pool.Config.GpuDirectConfig == nil {
		pool.Config.GpuDirectConfig = &gke_api_beta.GPUDirectConfig{}
	}
	pool.Config.GpuDirectConfig.GpuDirectStrategy = "RDMA"
}

func (g *gpuDirect) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil {
		return nil
	}
	var m Metadata
	if pool.Config != nil &&
		pool.Config.GpuDirectConfig != nil &&
		pool.Config.GpuDirectConfig.GpuDirectStrategy == "RDMA" {
		if m == nil {
			m = make(Metadata)
		}
		m[gkelabels.GpuDirectLabel] = "rdma"
	}
	if pool.Config != nil && pool.Config.Labels != nil {
		if val, found := pool.Config.Labels[gkelabels.GpuDirectLabel]; found {
			if m == nil {
				m = make(Metadata)
			}
			m[gkelabels.GpuDirectLabel] = val
		}
	}

	return m
}
