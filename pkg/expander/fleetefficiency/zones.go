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

package fleetefficiency

import (
	"sort"

	apiv1 "k8s.io/api/core/v1"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
)

// sanitizeZones returns a new slice with duplicate and empty zone names removed
// while preserving the original order.
func sanitizeZones(raw []string) []string {
	if len(raw) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(raw))
	res := make([]string, 0, len(raw))
	for _, z := range raw {
		if z == "" {
			continue
		}
		if _, ok := seen[z]; !ok {
			seen[z] = struct{}{}
			res = append(res, z)
		}
	}
	return res
}

// nodeInfoInZone returns a copy of nodeInfo whose node is labeled with the given zone. Pods and resource
// slices are not copied, as they are not needed for backoff checks. Returns nil for a nil nodeInfo.
func nodeInfoInZone(nodeInfo *framework.NodeInfo, zone string) *framework.NodeInfo {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return nil
	}
	node := nodeInfo.Node().DeepCopy()
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	node.Labels[apiv1.LabelTopologyZone] = zone
	return framework.NewNodeInfo(node, nil)
}

// sortedZones returns the zones present in the given set, sorted alphabetically.
func sortedZones(zoneSet map[string]bool) []string {
	zones := make([]string, 0, len(zoneSet))
	for z := range zoneSet {
		zones = append(zones, z)
	}
	sort.Strings(zones)
	return zones
}
