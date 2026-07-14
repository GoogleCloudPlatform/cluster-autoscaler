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
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/klog/v2"
	schedulerframework "k8s.io/kube-scheduler/framework"
)

const (
	nodeBasedDomainDiscoveryName        = "node-based-domain-discovery"
	nodeBasedDomainDiscoveryMetricLabel = "EligiblePTSPods:NodeBasedDomainDiscovery"
	nodeBasedExperimentName             = experiments.PodTopologySpreadNodeBasedMinCAVersionFlag
)

type nodeBasedDomainDiscovery struct {
	experimentsManager experiments.Manager
	snapshot           clustersnapshot.ClusterSnapshot
	cp                 cloudprovider.CloudProvider
}

// NewNodeBasedDomainDiscovery returns a new instance of nodeBasedDomainDiscovery.
func NewNodeBasedDomainDiscovery(experimentsManager experiments.Manager, snapshot clustersnapshot.ClusterSnapshot, cp cloudprovider.CloudProvider) *nodeBasedDomainDiscovery {
	return &nodeBasedDomainDiscovery{
		experimentsManager: experimentsManager,
		snapshot:           snapshot,
		cp:                 cp,
	}
}

func (dd *nodeBasedDomainDiscovery) EligiblePTSPods(pods []*apiv1.Pod) []PTSConfig {
	if !dd.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(nodeBasedExperimentName, false) {
		return nil
	}
	defer metrics.UpdateDurationFromStart(nodeBasedDomainDiscoveryMetricLabel, time.Now())
	if len(pods) == 0 {
		return nil
	}

	// 1. Retrieve domains over which pods want to be spreaded.
	ptsDomainKeys := domainKeysFromPods(pods)
	if len(ptsDomainKeys) == 0 {
		return nil
	}

	// 2. Track all the labels present in nodes for the domains.
	// Note: if nodes' labels were changed manually, they can differ from the corresponding nodepool's labels.
	// To ensure that we consider all available domains, it is safer to iterate over both nodes and node pools.
	nodeInfos, err := dd.snapshot.NodeInfos().List()
	if err != nil {
		klog.Warningf("Failed to list nodes from snapshot in node-based domain discovery: %v", err)
		return nil
	}
	nodeLabelsPerPodPtsDomain := make(map[string]sets.Set[string])
	labelsFromNodes(nodeLabelsPerPodPtsDomain, nodeInfos, ptsDomainKeys)

	// 3. Track all the labels from nodepools (consider both empty and not empty to cover the case of manual label change).
	nodePoolsNodeInfos := []schedulerframework.NodeInfo{}
	for _, ng := range dd.cp.NodeGroups() {
		nodeInfo, err := ng.TemplateNodeInfo()
		if err != nil {
			klog.Warningf("Failed to get template node info for node group %q: %v", ng.Id(), err)
			continue
		}
		nodePoolsNodeInfos = append(nodePoolsNodeInfos, nodeInfo)
	}
	labelsFromNodes(nodeLabelsPerPodPtsDomain, nodePoolsNodeInfos, ptsDomainKeys)

	// 4. Here we have both pods' PTS domains and nodes' labels satisfying these pod domains, so matching can be done.
	var configs []PTSConfig
	matchedDomainsCount := make(map[string]int)
	for _, pod := range pods {
		var matchedConstraint *apiv1.TopologySpreadConstraint
		var domains []string

		for _, constraint := range pod.Spec.TopologySpreadConstraints {
			key := constraint.TopologyKey
			availDomains := nodeLabelsPerPodPtsDomain[key]

			if len(availDomains) == 0 {
				continue
			}

			if constraint.MinDomains != nil && len(availDomains) < int(*constraint.MinDomains) {
				continue
			}
			// Prefer DoNotSchedule (hard) constraints over ScheduleAnyway (soft) constraints.
			if matchedConstraint == nil || matchedConstraint.WhenUnsatisfiable == apiv1.ScheduleAnyway {
				matchedConstraint = &constraint
				// To ensure determinism and that elements in the domains are the same.
				domains = sets.List(availDomains)
			}
		}
		if matchedConstraint == nil {
			continue
		}
		configs = append(configs, PTSConfig{
			pod:                 pod,
			domainNames:         domains,
			constraint:          matchedConstraint,
			domainDiscoveryName: nodeBasedDomainDiscoveryName,
		})
		matchedDomainsCount[matchedConstraint.TopologyKey] = len(domains)
	}

	if len(configs) > 0 {
		klog.V(4).Infof("Node-based domain discovery: %d pods' PTS requirements can be satisfied", len(configs))
		klog.V(4).Infof("Node-based domain discovery: %d matched domain keys were found: %+v", len(matchedDomainsCount), matchedDomainsCount)
	}

	return configs
}

func domainKeysFromPods(pods []*apiv1.Pod) sets.Set[string] {
	ptsDomainKeys := make(sets.Set[string])
	for _, pod := range pods {
		for _, constraint := range pod.Spec.TopologySpreadConstraints {
			// Zonal PTS is handled in its own processor
			// Note: CCC PTS pods will also not be evaluated here as those pods will selected and processed by CCC DD processor (which is defined earlier).
			if constraint.TopologyKey == apiv1.LabelTopologyZone {
				continue
			}
			// Hostname is not working with Node Based DD, as we do not know what will be the name of the upcoming node (if we need a scale up),
			// so we cannot properly set the node selector.
			if constraint.TopologyKey == apiv1.LabelHostname {
				continue
			}
			ptsDomainKeys.Insert(constraint.TopologyKey)
		}
	}
	return ptsDomainKeys
}

func labelsFromNodes(labelsFromNodes map[string]sets.Set[string], nodeInfos []schedulerframework.NodeInfo, podPtsDomains sets.Set[string]) {
	podPtsDomainsList := sets.List(podPtsDomains)
	for _, nodeInfo := range nodeInfos {
		node := nodeInfo.Node()
		if node == nil {
			continue
		}
		for _, label := range podPtsDomainsList {
			if value, ok := node.Labels[label]; ok {
				if _, ok := labelsFromNodes[label]; !ok {
					labelsFromNodes[label] = sets.New[string]()
				}
				labelsFromNodes[label].Insert(value)
			}
		}
	}
}
