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
	"slices"
	"strings"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	netutil "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/networking/util"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

const (
	additionalNetworksKey = "internal.additional-networks"
)

type additionalNetworkEntry struct {
	Network    string `json:"network"`
	Subnetwork string `json:"subnetwork"`
	PodRange   string `json:"podRange,omitempty"`
}

type additionalNetworks struct {
	internalFeatureDefaultImplementation
}

func newAdditionalNetworks() feature {
	return &additionalNetworks{}
}

func normalizeResourceName(name string) string {
	if idx := strings.LastIndex(name, "/"); idx != -1 {
		return name[idx+1:]
	}
	return name
}

func (f *additionalNetworks) FromNodepool(np *container.NodePool) Metadata {
	if np == nil || np.NetworkConfig == nil || len(np.NetworkConfig.AdditionalNodeNetworkConfigs) == 0 {
		return nil
	}

	podRangeBySubnetwork := make(map[string]string, len(np.NetworkConfig.AdditionalPodNetworkConfigs))
	for _, podCfg := range np.NetworkConfig.AdditionalPodNetworkConfigs {
		if podCfg == nil || podCfg.Subnetwork == "" {
			continue
		}
		subnet := normalizeResourceName(podCfg.Subnetwork)
		if _, exists := podRangeBySubnetwork[subnet]; !exists {
			podRangeBySubnetwork[subnet] = podCfg.SecondaryPodRange
		}
	}

	var entries []additionalNetworkEntry
	for _, nodeCfg := range np.NetworkConfig.AdditionalNodeNetworkConfigs {
		if nodeCfg == nil {
			continue
		}
		network := normalizeResourceName(nodeCfg.Network)
		subnetwork := normalizeResourceName(nodeCfg.Subnetwork)
		entries = append(entries, additionalNetworkEntry{
			Network:    network,
			Subnetwork: subnetwork,
			PodRange:   podRangeBySubnetwork[subnetwork],
		})
	}
	if len(entries) == 0 {
		return nil
	}

	bytes, err := json.Marshal(entries)
	if err != nil {
		klog.Errorf("Error marshalling additional networks from NodePool: %v", err)
		return nil
	}
	return Metadata{
		additionalNetworksKey: string(bytes),
	}
}

func (f *additionalNetworks) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (f *additionalNetworks) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NetworkConfig == nil || len(spec.NetworkConfig.AdditionalNetworks) == 0 {
		return nil
	}

	var entries []additionalNetworkEntry
	for _, net := range spec.NetworkConfig.AdditionalNetworks {
		if net.Name == "" || len(net.SubnetPriorities) == 0 || net.SubnetPriorities[0].Name == "" {
			continue
		}
		entries = append(entries, additionalNetworkEntry{
			Network:    net.Name,
			Subnetwork: net.SubnetPriorities[0].Name,
			PodRange:   net.SubnetPriorities[0].PodRange,
		})
	}
	if len(entries) == 0 {
		return nil
	}

	bytes, err := json.Marshal(entries)
	if err != nil {
		klog.Errorf("Error marshalling additional networks from CCC: %v", err)
		return nil
	}
	return Metadata{
		additionalNetworksKey: string(bytes),
	}
}

func (f *additionalNetworks) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (f *additionalNetworks) ToNodePoolLabels(_ map[string]string, _ Metadata) {}

func (f *additionalNetworks) ToNodepool(np *container.NodePool, m Metadata) {
	if np == nil || len(m) == 0 {
		return
	}

	raw, found := m[additionalNetworksKey]
	if !found || raw == "" {
		return
	}

	var entries []additionalNetworkEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		klog.Errorf("Error unmarshalling additional networks: %v", err)
		return
	}
	if len(entries) == 0 {
		return
	}

	if np.NetworkConfig == nil {
		np.NetworkConfig = &container.NodeNetworkConfig{}
	}

	for _, entry := range entries {
		nodeCfg := &container.AdditionalNodeNetworkConfig{
			Network:    entry.Network,
			Subnetwork: entry.Subnetwork,
		}
		if idx := slices.IndexFunc(np.NetworkConfig.AdditionalNodeNetworkConfigs, func(cfg *container.AdditionalNodeNetworkConfig) bool {
			return cfg != nil && normalizeResourceName(cfg.Network) == entry.Network
		}); idx != -1 {
			np.NetworkConfig.AdditionalNodeNetworkConfigs[idx] = nodeCfg
		} else {
			np.NetworkConfig.AdditionalNodeNetworkConfigs = append(np.NetworkConfig.AdditionalNodeNetworkConfigs, nodeCfg)
		}

		if entry.PodRange != "" {
			podCfg := &container.AdditionalPodNetworkConfig{
				Subnetwork:        entry.Subnetwork,
				SecondaryPodRange: entry.PodRange,
				MaxPodsPerNode: &container.MaxPodsConstraint{
					MaxPodsPerNode: netutil.DefaultAdditionalMultiNetworkConfigMaxPodsPerNode,
				},
			}
			if idx := slices.IndexFunc(np.NetworkConfig.AdditionalPodNetworkConfigs, func(cfg *container.AdditionalPodNetworkConfig) bool {
				return cfg != nil && normalizeResourceName(cfg.Subnetwork) == entry.Subnetwork
			}); idx != -1 {
				np.NetworkConfig.AdditionalPodNetworkConfigs[idx] = podCfg
			} else {
				np.NetworkConfig.AdditionalPodNetworkConfigs = append(np.NetworkConfig.AdditionalPodNetworkConfigs, podCfg)
			}
		}
	}

	if !slices.Contains(np.NetworkConfig.ForceSendFields, "AdditionalNodeNetworkConfigs") {
		np.NetworkConfig.ForceSendFields = append(np.NetworkConfig.ForceSendFields, "AdditionalNodeNetworkConfigs")
	}
	if len(np.NetworkConfig.AdditionalPodNetworkConfigs) > 0 && !slices.Contains(np.NetworkConfig.ForceSendFields, "AdditionalPodNetworkConfigs") {
		np.NetworkConfig.ForceSendFields = append(np.NetworkConfig.ForceSendFields, "AdditionalPodNetworkConfigs")
	}
}
