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

// imageStreaming is a self-service feature which lets users enable container image streaming in CCC definitions.
// NAP-created node pools will have image streaming enabled via GcfsConfig field.
// https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/GcfsConfig
type imageStreaming struct {
	internalFeatureDefaultImplementation
}

func newImageStreaming() feature {
	return &imageStreaming{}
}

// FromNodepool returns metadata with image streaming label, if image streaming is enabled in the node pool. Otherwise,
// it returns nil.
func (w *imageStreaming) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil || pool.Config.GcfsConfig == nil || !pool.Config.GcfsConfig.Enabled {
		return nil
	}
	return Metadata{gkelabels.ImageStreamingLabelKey: "true"}
}

// FromCccSpec returns medatada with image streaming label, if it is enabled in the CCC definition. Otherwise, it
// returns empty metadata.
func (w *imageStreaming) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || spec.NodePoolConfig.ImageStreaming == nil || !spec.NodePoolConfig.ImageStreaming.Enabled {
		return nil
	}
	return Metadata{gkelabels.ImageStreamingLabelKey: "true"}
}

// ToNodepool enables image streaming in the node-pool, if metadata contains image-streaming label.
func (w *imageStreaming) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if metadata[gkelabels.ImageStreamingLabelKey] != "true" {
		return
	}
	// Image streaming label is present, so we enable it in the nodepool.
	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}
	if pool.Config.GcfsConfig == nil {
		pool.Config.GcfsConfig = &gke_api_beta.GcfsConfig{}
	}
	pool.Config.GcfsConfig.Enabled = true
}

// FromPriority Not implemented, because ImageStreaming is set at the level of NodePoolConfig, not at the level of priorities.
func (w *imageStreaming) FromPriority(p v1.Priority) Metadata {
	return nil
}

// ToNodePoolLabels Labels are not needed for image streaming.
func (w *imageStreaming) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

// FromLabelRequirements Labels are not needed for image streaming.
func (w *imageStreaming) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}
