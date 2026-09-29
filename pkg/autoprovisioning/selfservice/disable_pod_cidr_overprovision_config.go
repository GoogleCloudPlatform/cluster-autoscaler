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
	"slices"
	"strconv"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const (
	disablePodCidrOverprovisionConfigMetadataKey = "PodCIDROverprovisionConfigDisable"
)

type disablePodCidrOverprovisionConfig struct {
	internalFeatureDefaultImplementation
}

func newDisablePodCidrOverprovisionConfig() feature {
	return &disablePodCidrOverprovisionConfig{}
}

func (d *disablePodCidrOverprovisionConfig) FromNodepool(np *container.NodePool) Metadata {
	if np == nil || np.NetworkConfig == nil || np.NetworkConfig.PodCidrOverprovisionConfig == nil {
		return nil
	}
	m := make(Metadata)
	m[disablePodCidrOverprovisionConfigMetadataKey] = strconv.FormatBool(np.NetworkConfig.PodCidrOverprovisionConfig.Disable)
	return m
}

func (d *disablePodCidrOverprovisionConfig) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (d *disablePodCidrOverprovisionConfig) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NetworkConfig == nil || spec.NetworkConfig.DisablePodCidrOverprovisionConfig == nil {
		return nil
	}
	m := make(Metadata)
	m[disablePodCidrOverprovisionConfigMetadataKey] = strconv.FormatBool(*spec.NetworkConfig.DisablePodCidrOverprovisionConfig)
	return m
}

func (d *disablePodCidrOverprovisionConfig) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (d *disablePodCidrOverprovisionConfig) ToNodePoolLabels(_ map[string]string, _ Metadata) {}

func (d *disablePodCidrOverprovisionConfig) ToNodepool(np *container.NodePool, m Metadata) {
	if np == nil {
		return
	}
	val, found := m[disablePodCidrOverprovisionConfigMetadataKey]
	if !found {
		return
	}
	disable, err := strconv.ParseBool(val)
	if err != nil {
		return
	}
	if np.NetworkConfig == nil {
		np.NetworkConfig = &container.NodeNetworkConfig{}
	}
	if np.NetworkConfig.PodCidrOverprovisionConfig == nil {
		np.NetworkConfig.PodCidrOverprovisionConfig = &container.PodCIDROverprovisionConfig{}
	}
	np.NetworkConfig.PodCidrOverprovisionConfig.Disable = disable
	if !slices.Contains(np.NetworkConfig.PodCidrOverprovisionConfig.ForceSendFields, "Disable") {
		np.NetworkConfig.PodCidrOverprovisionConfig.ForceSendFields = append(np.NetworkConfig.PodCidrOverprovisionConfig.ForceSendFields, "Disable")
	}
}
