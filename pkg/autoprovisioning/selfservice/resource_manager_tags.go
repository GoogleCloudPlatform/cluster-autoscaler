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
	"encoding/json"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

type resourceManagerTags struct {
	internalFeatureDefaultImplementation
}

type Tags map[string]string

// resourceManagerTags is a self-service feature defining resource manager tags
// to bind to all node pools managed by NAP.
func newResourceManagerTags() feature {
	return &resourceManagerTags{}
}

func (r *resourceManagerTags) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil || pool.Config.ResourceManagerTags == nil || len(pool.Config.ResourceManagerTags.Tags) == 0 {
		return nil
	}
	tagBytes, err := json.Marshal(pool.Config.ResourceManagerTags.Tags)
	if err != nil {
		klog.Errorf("Error marshalling resource manager tags from NodePool: %v", err)
		return nil
	}
	m := make(Metadata)
	m[gkelabels.TagsLabelKey] = string(tagBytes)
	return m
}

func (r *resourceManagerTags) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (r *resourceManagerTags) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || len(spec.NodePoolConfig.ResourceManagerTags) == 0 {
		return nil
	}
	tags := make(Tags)
	for _, cccTag := range spec.NodePoolConfig.ResourceManagerTags {
		tags[cccTag.Key] = cccTag.Value
	}
	tagBytes, err := json.Marshal(tags)
	if err != nil {
		klog.Errorf("Error marshalling resource manager tags from CCC: %v", err)
		return nil
	}
	m := make(Metadata)
	m[gkelabels.TagsLabelKey] = string(tagBytes)
	return m
}

func (r *resourceManagerTags) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (r *resourceManagerTags) ToNodePoolLabels(_ map[string]string, _ Metadata) {
}

func (r *resourceManagerTags) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	metadataTags, found := metadata[gkelabels.TagsLabelKey]
	if !found {
		return
	}
	var npTags Tags
	if err := json.Unmarshal([]byte(metadataTags), &npTags); err != nil {
		klog.Errorf("Error unmarshalling resource manager tags: %v", err)
		return
	}
	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}
	if pool.Config.ResourceManagerTags == nil {
		pool.Config.ResourceManagerTags = &gke_api_beta.ResourceManagerTags{}
	}
	pool.Config.ResourceManagerTags.Tags = npTags
}
