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

package podtopologyspread

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
)

const (
	zonalDomainDiscoveryName = "zonal-domain-discovery"
	zonalExperimentName      = "PodTopologySpreadZonal::MinCAVersion"
)

type zonalCloudProvider interface {
	GetAutoprovisioningLocations() []string
}

type zonalDomainDiscovery struct {
	experimentsManager experiments.Manager
	cp                 zonalCloudProvider
}

func NewZonalDomainDiscovery(experimentsManager experiments.Manager, cp zonalCloudProvider) *zonalDomainDiscovery {
	return &zonalDomainDiscovery{
		experimentsManager: experimentsManager,
		cp:                 cp,
	}
}

func (dd *zonalDomainDiscovery) EligiblePTSPods(pods []*apiv1.Pod) []PTSConfig {
	if !dd.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(zonalExperimentName, false) {
		return nil
	}

	zones := dd.cp.GetAutoprovisioningLocations()
	var configs []PTSConfig
	for _, pod := range pods {
		var zonalConstraint *apiv1.TopologySpreadConstraint
		for _, constraint := range pod.Spec.TopologySpreadConstraints {
			if constraint.TopologyKey != apiv1.LabelTopologyZone {
				continue
			}
			if zonalConstraint == nil || zonalConstraint.WhenUnsatisfiable == apiv1.ScheduleAnyway {
				zonalConstraint = &constraint
			}
		}
		if zonalConstraint == nil {
			continue
		}
		if zonalConstraint.MinDomains != nil && len(zones) < int(*zonalConstraint.MinDomains) {
			continue
		}
		configs = append(configs, PTSConfig{
			pod:                 pod,
			domainNames:         zones,
			constraint:          zonalConstraint,
			domainDiscoveryName: zonalDomainDiscoveryName,
		})
	}
	return configs
}
