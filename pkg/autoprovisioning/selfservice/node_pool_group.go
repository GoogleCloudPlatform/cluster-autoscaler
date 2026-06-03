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

// nodePoolGroupName is a self-service feature that allows setting node pool
// group name label for matching CCCs to be passed to node pools by NAP and thus
// creation of a Multi-Mig
type nodePoolGroupName struct {
	internalFeatureDefaultImplementation
}

func newNodePoolGroupName() feature {
	return &nodePoolGroupName{}
}

func (n *nodePoolGroupName) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil {
		return nil
	}

	m := make(Metadata)
	if v, found := pool.Config.Labels[gkelabels.NodePoolGroupNameLabel]; found {
		m[gkelabels.NodePoolGroupNameLabel] = v
	}
	return m
}

func (n *nodePoolGroupName) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (n *nodePoolGroupName) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	m := make(Metadata)
	if spec.NodePoolGroup != nil && spec.NodePoolGroup.Name != "" {
		m[gkelabels.NodePoolGroupNameLabel] = spec.NodePoolGroup.Name
	}
	return m
}

func (n *nodePoolGroupName) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (n *nodePoolGroupName) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
	if v, found := metadata[gkelabels.NodePoolGroupNameLabel]; found {
		labels[gkelabels.NodePoolGroupNameLabel] = v
	}
}

func (n *nodePoolGroupName) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if v, found := metadata[gkelabels.NodePoolGroupNameLabel]; found {
		if pool.Config == nil {
			pool.Config = &gke_api_beta.NodeConfig{}
		}
		if pool.Config.Labels == nil {
			pool.Config.Labels = make(map[string]string)
		}
		pool.Config.Labels[gkelabels.NodePoolGroupNameLabel] = v
	}
}
