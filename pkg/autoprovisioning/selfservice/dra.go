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

type draFeature struct {
	internalFeatureDefaultImplementation
}

func newDraFeature() *draFeature {
	return &draFeature{}
}

func (n *draFeature) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil {
		return nil
	}

	m := make(Metadata)
	if v, found := pool.Config.Labels[gkelabels.DraNetNodeLabel]; found {
		m[gkelabels.DraNetNodeLabel] = v
	}
	return m
}

func (n *draFeature) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (n *draFeature) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	m := make(Metadata)
	if spec.NodePoolConfig != nil && spec.NodePoolConfig.Dra.Networking.Enabled {
		m[gkelabels.DraNetNodeLabel] = "true"
	}
	return m
}

func (n *draFeature) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (n *draFeature) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
	if v, found := metadata[gkelabels.DraNetNodeLabel]; found {
		labels[gkelabels.DraNetNodeLabel] = v
	}
}

func (n *draFeature) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
}
