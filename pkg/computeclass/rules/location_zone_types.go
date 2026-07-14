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

package rules

import (
	"maps"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/zonetypes"
	"k8s.io/klog/v2"
)

const (
	ZoneTypeClusterDefault = "CLUSTER_DEFAULT"
	ZoneTypeStandard       = "STANDARD"
	ZoneTypeAI             = "AI"
)

type zoneDataProvider interface {
	GetAIZones() ([]string, error)
	GetStandardZones() ([]string, error)
	GetAutoprovisioningLocations() []string
	TrimLocationsForMachineConfig(locations []string, machineType string, acceleratorConfig *gke_api_beta.AcceleratorConfig, minCpuPlatform string, diskType string) []string
}

// LocationZoneTypesRule is an interface for rules with location zone types.
type LocationZoneTypesRule interface {
	BaseRule
	ZoneTypes() []v1.ZoneType
	Provider() zoneDataProvider
	GetZoneTypesZones() ([]string, caerrors.AutoscalerError)
}

type locationZoneTypesRule struct {
	zoneTypes []v1.ZoneType
	provider  zoneDataProvider
}

// Matches returns true if the node group's zone type is one of the allowed types.
func (r *locationZoneTypesRule) Matches(nodeGroup cloudprovider.NodeGroup) bool {
	if len(r.zoneTypes) == 0 {
		return true
	}

	mig, ok := nodeGroup.(gkeNodeGroup)
	if !ok {
		klog.Errorf("expected GkeMig; got %v", nodeGroup)
		return false
	}

	if mig.Spec() == nil || len(mig.Spec().Locations) == 0 {
		return false
	}

	nodePoolLocationsMap := make(map[string]bool)
	for _, loc := range mig.Spec().Locations {
		nodePoolLocationsMap[loc] = true
	}
	ruleZones, err := r.GetZoneTypesZones()
	if err != nil {
		klog.Errorf("zoneTypes: failed to get zones from zone types: %v", err)
		return false
	}

	var acceleratorConfig *gke_api_beta.AcceleratorConfig
	if len(mig.Spec().Accelerators) > 0 {
		acceleratorConfig = mig.Spec().Accelerators[0]
	}

	// RulesZones must be trimmed to only contain locations that support given machine config
	ruleZones = r.provider.TrimLocationsForMachineConfig(ruleZones, mig.MachineType(), acceleratorConfig, mig.Spec().MinCpuPlatform, mig.Spec().DiskType)
	ruleZonesMap := make(map[string]bool)
	for _, zone := range ruleZones {
		ruleZonesMap[zone] = true
	}

	if mig.Spec().PlacementGroup.UsesPlacement() || mig.Spec().TpuMultiHost {
		if len(mig.Spec().Locations) != 1 {
			klog.Errorf("compact node group %q has more than one zone, this should never happen", nodeGroup.Id())
			return false
		}
		_, matches := ruleZonesMap[mig.Spec().Locations[0]]
		return matches
	}

	return maps.Equal(nodePoolLocationsMap, ruleZonesMap)
}

// ZoneTypes returns the allowed zone types for this rule.
func (r *locationZoneTypesRule) ZoneTypes() []v1.ZoneType {
	return r.zoneTypes
}

// Provider returns the zone data provider for this rule.
func (r *locationZoneTypesRule) Provider() zoneDataProvider {
	return r.provider
}

func (r *locationZoneTypesRule) GetZoneTypesZones() ([]string, caerrors.AutoscalerError) {
	zones := []string{}
	for _, zoneType := range r.zoneTypes {
		switch zoneType {
		case ZoneTypeClusterDefault:
			zones = append(zones, r.provider.GetAutoprovisioningLocations()...)
		case ZoneTypeStandard:
			sZones, err := r.provider.GetStandardZones()
			if err != nil {
				return nil, caerrors.NewAutoscalerErrorf(caerrors.CloudProviderError, "zoneTypes: failed to get standard zones, err: %s", err.Error())
			}
			zones = append(zones, sZones...)
		case ZoneTypeAI:
			aiZones, err := r.provider.GetAIZones()
			if err != nil {
				return nil, caerrors.NewAutoscalerErrorf(caerrors.CloudProviderError, "zoneTypes: failed to get AI zones, err: %s", err.Error())
			}
			if len(aiZones) == 0 {
				return nil, zonetypes.NewErrNoAIZones()
			}
			zones = append(zones, aiZones...)
		default:
			klog.Errorf("zoneTypes: unknown zone type: %s", zoneType)
		}
	}
	return dedupeZones(zones), nil
}

func dedupeZones(zones []string) []string {
	uniqueZones := make(map[string]bool)
	var dedupedZones []string
	for _, z := range zones {
		if !uniqueZones[z] {
			uniqueZones[z] = true
			dedupedZones = append(dedupedZones, z)
		}
	}
	return dedupedZones
}

// WithLocationZoneTypesRule returns a RuleOption adding LocationZoneTypesRule.
func WithLocationZoneTypesRule(zoneTypes []v1.ZoneType, provider zoneDataProvider) RuleOption {
	return func(r *rule) {
		r.locationZoneTypesRule = locationZoneTypesRule{zoneTypes: zoneTypes, provider: provider}
	}
}
