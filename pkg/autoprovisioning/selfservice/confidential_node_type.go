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

// confidentialNodeType is a self-service feature that allows
// setting Confidential Nodes instance type label for matching CCCs to be passed
// to node pools by NAP.
type confidentialNodeType struct {
	internalFeatureDefaultImplementation
}

func newConfidentialNodeType() feature {
	return &confidentialNodeType{}
}

func (w *confidentialNodeType) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil || pool.Config.ConfidentialNodes == nil ||
		pool.Config.ConfidentialNodes.ConfidentialInstanceType == "" {
		return nil
	}

	m := make(Metadata)
	m[gkelabels.GkeConfidentialNodeType] = pool.Config.ConfidentialNodes.ConfidentialInstanceType
	return m
}

func (w *confidentialNodeType) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (w *confidentialNodeType) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	m := make(Metadata)
	if spec.NodePoolConfig != nil && spec.NodePoolConfig.ConfidentialNodeType != "" {
		m[gkelabels.GkeConfidentialNodeType] = spec.NodePoolConfig.ConfidentialNodeType
	}
	return m
}

func (w *confidentialNodeType) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (w *confidentialNodeType) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
	if v, found := metadata[gkelabels.GkeConfidentialNodeType]; found {
		labels[gkelabels.GkeConfidentialNodeType] = v
	}
}

func (w *confidentialNodeType) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if v, found := metadata[gkelabels.GkeConfidentialNodeType]; found {
		if pool.Config == nil {
			pool.Config = &gke_api_beta.NodeConfig{}
		}
		if pool.Config.Labels == nil {
			pool.Config.Labels = make(map[string]string)
		}
		pool.Config.Labels[gkelabels.GkeConfidentialNodeType] = v
		if pool.Config.ConfidentialNodes == nil {
			pool.Config.ConfidentialNodes = &gke_api_beta.ConfidentialNodes{}
		}
		pool.Config.ConfidentialNodes.ConfidentialInstanceType = v
	}
}
