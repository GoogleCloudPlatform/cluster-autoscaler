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
	"strings"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// sandbox is a self-service feature which lets users configure sandbox type in CCC definitions.
type sandbox struct {
	internalFeatureDefaultImplementation
}

func newSandbox() feature {
	return &sandbox{}
}

// FromNodepool returns metadata with sandbox label, if sandbox is configured in the node pool. Otherwise, it returns nil.
func (w *sandbox) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil || pool.Config.SandboxConfig == nil || pool.Config.SandboxConfig.Type == "" {
		return nil
	}
	return Metadata{gkelabels.SandboxLabelKey: strings.ToLower(pool.Config.SandboxConfig.Type)}
}

// FromCccSpec returns medatada with sandbox label, if it is configured in the CCC definition. Otherwise, it
// returns empty metadata.
func (w *sandbox) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || spec.NodePoolConfig.Sandbox == nil {
		return nil
	}
	return Metadata{gkelabels.SandboxLabelKey: strings.ToLower(spec.NodePoolConfig.Sandbox.Type)}
}

// ToNodepool configures sandbox in the node-pool, if metadata contains sandbox label.
func (w *sandbox) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	sandboxType, ok := metadata[gkelabels.SandboxLabelKey]
	if !ok || sandboxType == "" {
		return
	}
	// Sandbox label is present, so we configure it in the nodepool.
	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}
	if pool.Config.SandboxConfig == nil {
		pool.Config.SandboxConfig = &gke_api_beta.SandboxConfig{}
	}
	pool.Config.SandboxConfig.Type = sandboxType
}

// FromPriority is not implemented, because sandbox is set at the level of NodePoolConfig, not at the level of priorities.
func (w *sandbox) FromPriority(p v1.Priority) Metadata {
	return nil
}

// ToNodePoolLabels is not implemented, as labels are not needed for sandbox.
func (w *sandbox) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

// FromLabelRequirements is not implemented, as labels are not needed for sandbox.
func (w *sandbox) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}
