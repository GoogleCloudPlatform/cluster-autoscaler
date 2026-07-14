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
	"k8s.io/klog/v2"
)

type maintenanceExclusion struct {
	internalFeatureDefaultImplementation
}

func newMaintenanceExclusion() feature {
	return &maintenanceExclusion{}
}

func (w *maintenanceExclusion) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil {
		return nil
	}

	// Read from GKE native NodePool.MaintenancePolicy field
	if pool.MaintenancePolicy != nil &&
		pool.MaintenancePolicy.ExclusionUntilEndOfSupport != nil &&
		pool.MaintenancePolicy.ExclusionUntilEndOfSupport.Enabled {
		return Metadata{
			gkelabels.MaintenanceExclusionLabelKey: string(v1.MaintenanceExclusionUntilEndOfSupport),
		}
	}
	return nil
}

func (n *maintenanceExclusion) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (w *maintenanceExclusion) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || spec.NodePoolConfig.MaintenanceExclusion == nil {
		return nil
	}
	if *spec.NodePoolConfig.MaintenanceExclusion == v1.MaintenanceExclusionUntilEndOfSupport {
		m := make(Metadata)
		m[gkelabels.MaintenanceExclusionLabelKey] = string(v1.MaintenanceExclusionUntilEndOfSupport)
		return m
	}
	klog.Warningf("Unsupported MaintenanceExclusion type: %s", *spec.NodePoolConfig.MaintenanceExclusion)
	return nil
}

func (w *maintenanceExclusion) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (n *maintenanceExclusion) ToNodePoolLabels(_ map[string]string, _ Metadata) {
}

func (w *maintenanceExclusion) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if val, found := metadata[gkelabels.MaintenanceExclusionLabelKey]; found {
		if val == string(v1.MaintenanceExclusionUntilEndOfSupport) {
			if pool.MaintenancePolicy == nil {
				pool.MaintenancePolicy = &gke_api_beta.NodePoolMaintenancePolicy{}
			}
			if pool.MaintenancePolicy.ExclusionUntilEndOfSupport == nil {
				pool.MaintenancePolicy.ExclusionUntilEndOfSupport = &gke_api_beta.ExclusionUntilEndOfSupport{}
			}
			pool.MaintenancePolicy.ExclusionUntilEndOfSupport.Enabled = true
		}
	}
}

func boolPtr(b bool) *bool {
	return &b
}
