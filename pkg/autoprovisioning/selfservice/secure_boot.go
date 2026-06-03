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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const (
	secureBootMetadataKey          = "ShieldedInstanceConfigEnableSecureBoot"
	integrityMonitoringMetadataKey = "ShieldedInstanceConfigEnableIntegrityMonitoring"
)

// secureBootFeature is a self-service feature that enables ShieldedInstanceConfig
// for NAP-created node pools.
type secureBootFeature struct {
	internalFeatureDefaultImplementation
}

func newSecureBootFeature() feature {
	return &secureBootFeature{}
}

func (w *secureBootFeature) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil || pool.Config.ShieldedInstanceConfig == nil {
		return nil
	}
	m := make(Metadata)
	if pool.Config.ShieldedInstanceConfig.EnableSecureBoot {
		m[secureBootMetadataKey] = "true"
	}
	if pool.Config.ShieldedInstanceConfig.EnableIntegrityMonitoring {
		m[integrityMonitoringMetadataKey] = "true"
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func (w *secureBootFeature) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolAutoCreation == nil || spec.NodePoolAutoCreation.ShieldedInstanceConfig == nil {
		return nil
	}
	sic := spec.NodePoolAutoCreation.ShieldedInstanceConfig
	m := make(Metadata)
	if sic.EnableSecureBoot != nil && *sic.EnableSecureBoot {
		m[secureBootMetadataKey] = "true"
	}
	if sic.EnableIntegrityMonitoring != nil && *sic.EnableIntegrityMonitoring {
		m[integrityMonitoringMetadataKey] = "true"
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func (w *secureBootFeature) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	enableSB := metadata[secureBootMetadataKey] == "true"
	enableIM := metadata[integrityMonitoringMetadataKey] == "true"

	if !enableSB && !enableIM {
		return
	}

	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}
	if pool.Config.ShieldedInstanceConfig == nil {
		pool.Config.ShieldedInstanceConfig = &gke_api_beta.ShieldedInstanceConfig{}
	}
	if enableSB {
		pool.Config.ShieldedInstanceConfig.EnableSecureBoot = true
	}
	if enableIM {
		pool.Config.ShieldedInstanceConfig.EnableIntegrityMonitoring = true
	}
}

func (w *secureBootFeature) FromPriority(p v1.Priority) Metadata {
	return nil
}

func (w *secureBootFeature) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

func (w *secureBootFeature) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}
