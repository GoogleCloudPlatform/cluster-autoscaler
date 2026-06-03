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
	"fmt"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// gvnic is a self-service feature which lets users enable Google Virtual NIC in CCC definitions.
type gvnic struct {
	internalFeatureDefaultImplementation
}

func newGvnic() feature {
	return &gvnic{}
}

// FromNodepool returns metadata with gvnic label, if gvnic is enabled or
// disabled in the node pool. Otherwise, it returns nil.
func (w *gvnic) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	gvnicEnabled := false
	if pool == nil || pool.Config == nil || (pool.Config.Gvnic == nil && pool.Config.MachineType == "") {
		return nil
	} else if pool.Config.Gvnic == nil {
		// gvnic is enabled by default on machine for gen 3+
		gvnicEnabled = machinetypes.GetMachineGeneration(pool.Config.MachineType) >= 3
	} else {
		gvnicEnabled = pool.Config.Gvnic.Enabled
	}
	return Metadata{gkelabels.GvnicLabelKey: fmt.Sprintf("%v", gvnicEnabled)}
}

// FromCccSpec returns medatada with gvnic label, if it is enabled in the CCC definition. Otherwise, it
// returns empty metadata.
func (w *gvnic) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || spec.NodePoolConfig.Gvnic == nil {
		return nil
	}
	return Metadata{gkelabels.GvnicLabelKey: fmt.Sprintf("%v", spec.NodePoolConfig.Gvnic.Enabled)}
}

// ToNodepool enables gvnic in the node-pool, if metadata contains gvnic label.
func (w *gvnic) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if metadata[gkelabels.GvnicLabelKey] != "true" && metadata[gkelabels.GvnicLabelKey] != "false" {
		return
	}
	// Gvnic label is present, so we enable it in the nodepool.
	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}
	if pool.Config.Gvnic == nil {
		pool.Config.Gvnic = &gke_api_beta.VirtualNIC{}
	}
	pool.Config.Gvnic.Enabled = metadata[gkelabels.GvnicLabelKey] == "true"
}

// FromPriority is not implemented, because Gvnic is set at the level of NodePoolConfig, not at the level of priorities.
func (w *gvnic) FromPriority(p v1.Priority) Metadata {
	return nil
}

// ToNodePoolLabels is not implemented, as labels are not needed for gvnic.
func (w *gvnic) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

// FromLabelRequirements is not implemented, as labels are not needed for gvnic.
func (w *gvnic) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}
