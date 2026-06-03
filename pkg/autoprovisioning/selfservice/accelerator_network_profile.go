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

type acceleratorNetworkProfile struct {
	internalFeatureDefaultImplementation
}

func newAcceleratorNetworkProfile() feature {
	return &acceleratorNetworkProfile{}
}

func (n *acceleratorNetworkProfile) FromPriority(p v1.Priority) Metadata {
	if p.AcceleratorNetworkProfile == nil || *p.AcceleratorNetworkProfile == "" {
		return nil
	}
	m := make(Metadata)
	m[gkelabels.AcceleratorNetworkProfileLabel] = *p.AcceleratorNetworkProfile
	return m
}

func (n *acceleratorNetworkProfile) FromLabelRequirements(reqs podrequirements.LabelRequirements) Metadata {
	val, found := reqs.GetValues(gkelabels.AcceleratorNetworkProfileLabel)
	if found {
		valuesMap := val.Get()
		for v := range valuesMap {
			m := make(Metadata)
			m[gkelabels.AcceleratorNetworkProfileLabel] = v
			return m
		}
	}
	return nil
}

// ANP is a priority-specific feature, and is not expected at the general ComputeClassSpec level.
func (n *acceleratorNetworkProfile) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	return nil
}

func (n *acceleratorNetworkProfile) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
	if v, found := metadata[gkelabels.AcceleratorNetworkProfileLabel]; found {
		labels[gkelabels.AcceleratorNetworkProfileLabel] = v
	}
}

func (n *acceleratorNetworkProfile) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	v, found := metadata[gkelabels.AcceleratorNetworkProfileLabel]
	if !found {
		return
	}
	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}
	if pool.Config.Labels == nil {
		pool.Config.Labels = make(map[string]string)
	}
	pool.Config.Labels[gkelabels.AcceleratorNetworkProfileLabel] = v
}

func (n *acceleratorNetworkProfile) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil {
		return nil
	}
	if pool.Config != nil && pool.Config.Labels != nil {
		if val, found := pool.Config.Labels[gkelabels.AcceleratorNetworkProfileLabel]; found {
			m := make(Metadata)
			m[gkelabels.AcceleratorNetworkProfileLabel] = val
			return m
		}
	}
	return nil
}
