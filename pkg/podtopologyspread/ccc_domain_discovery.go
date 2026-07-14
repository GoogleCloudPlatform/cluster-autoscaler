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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/crd"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/computeclass/lister"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
)

const (
	cccDomainDiscoveryName = "ccc-domain-discovery"
	cccExperimentName      = "PodTopologySpreadCCC::MinCAVersion"
)

type cccDomainDiscovery struct {
	experimentsManager experiments.Manager
	cccLister          lister.Lister
}

func NewCCCDomainDiscovery(experimentsManager experiments.Manager, cccLister lister.Lister) *cccDomainDiscovery {
	return &cccDomainDiscovery{
		experimentsManager: experimentsManager,
		cccLister:          cccLister,
	}
}

type domainsCacheKey struct {
	cccName   string
	domainKey string
}

// EligiblePTSPods returns pods that are eligible for PTS support with CCC.
// To be eligible PTS pod's topologyKey has to exist in nodeLabels of all priorities in CCC.
func (dd *cccDomainDiscovery) EligiblePTSPods(pods []*apiv1.Pod) []PTSConfig {
	if !dd.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(cccExperimentName, false) {
		return nil
	}

	if len(pods) == 0 {
		return nil
	}

	var configs []PTSConfig
	domainsCache := make(map[domainsCacheKey][]string)

	for _, pod := range pods {
		if len(pod.Spec.TopologySpreadConstraints) == 0 {
			continue
		}

		cccCrd, _, err := dd.cccLister.PodCrd(pod)
		if err != nil {
			klog.Warningf("Retrieving crd for pod %q failed: %v", pod.Name, err)
			continue
		}
		if cccCrd == nil {
			continue
		}

		var matchingConstraint *apiv1.TopologySpreadConstraint
		var domains []string
		for _, constraint := range pod.Spec.TopologySpreadConstraints {
			domains = availableDomainsWithCache(domainsCache, cccCrd, constraint.TopologyKey)
			if len(domains) == 0 {
				continue
			}
			if constraint.MinDomains != nil && len(domains) < int(*constraint.MinDomains) {
				continue
			}
			if matchingConstraint == nil || matchingConstraint.WhenUnsatisfiable == apiv1.ScheduleAnyway {
				matchingConstraint = &constraint
			}
		}
		if matchingConstraint == nil {
			continue
		}

		configs = append(configs, PTSConfig{
			pod:                 pod,
			domainNames:         domains,
			constraint:          matchingConstraint,
			domainDiscoveryName: cccDomainDiscoveryName,
		})
	}
	return configs
}

func availableDomainsWithCache(domainsCache map[domainsCacheKey][]string, ccc crd.CRD, domainKey string) []string {
	cacheKey := domainsCacheKey{cccName: ccc.Name(), domainKey: domainKey}
	if domains, found := domainsCache[cacheKey]; found {
		return domains
	}

	domains := availableDomains(ccc, domainKey)
	domainsCache[cacheKey] = domains
	return domains
}

func availableDomains(ccc crd.CRD, domainKey string) []string {
	var domains []string
	for rIdx, rule := range ccc.Rules() {
		nodeLabels := rule.UserDefinedLabels()
		domainValue := nodeLabels[domainKey]
		if domainValue == "" {
			klog.V(5).Infof("Domain key %q not found in node labels of rule %d (1-indexed) in CRD %q", domainKey, rIdx+1, ccc.Name())
			return nil
		}
		domains = append(domains, domainValue)
	}
	return domains
}
