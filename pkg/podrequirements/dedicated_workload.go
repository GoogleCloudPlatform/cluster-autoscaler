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

package podrequirements

import (
	"fmt"
	"sort"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

// ExtractWorkloadID produces string describing dedicated workload from a node
func ExtractWorkloadID(node *apiv1.Node) string {
	widTaints := ExtractWorkloadIDTaints(node)
	return taintsToWorkloadID(widTaints)
}

// ExtractWorkloadIDTaints extracts taints describing dedicated workload from a node
func ExtractWorkloadIDTaints(node *apiv1.Node) []apiv1.Taint {
	widTaints := []apiv1.Taint{}
	if node == nil {
		return widTaints
	}
	for _, taint := range node.Spec.Taints {
		if gkelabels.IsSystemLabel(taint.Key) && !allowedSystemLabel(taint.Key) {
			continue
		}
		if taint.Effect != apiv1.TaintEffectNoSchedule && taint.Effect != apiv1.TaintEffectNoExecute {
			continue
		}
		// Check for matching label
		labelValue, found := node.Labels[taint.Key]
		if !found || taint.Value != labelValue {
			// Check for matching resource
			_, found = node.Status.Capacity[apiv1.ResourceName(taint.Key)]
			if !found {
				continue
			}
		}
		widTaints = append(widTaints, taint)
	}
	return widTaints
}

// taintsToWorkloadID constructs a workload ID from a set of workload separation taints.
// It is the caller's responsibility to only enter taints belonging to the workload ID.
func taintsToWorkloadID(taints []apiv1.Taint) string {
	var keyValuePairs []string
	for _, taint := range taints {
		pair := fmt.Sprintf("%s:%s:%s", taint.Effect, taint.Key, taint.Value)
		keyValuePairs = append(keyValuePairs, pair)
	}
	sort.Strings(keyValuePairs)
	return strings.Join(keyValuePairs, ",")
}

// WorkloadIDToTolerations constructs a slice of tolerations from the workload ID.
func WorkloadIDToTolerations(id string) []apiv1.Toleration {
	if id == "" {
		return []apiv1.Toleration{}
	}
	taintStrings := strings.Split(id, ",")
	tolerations := []apiv1.Toleration{}
	for _, ts := range taintStrings {
		t := strings.Split(ts, ":")
		if len(t) != 3 {
			continue
		}
		tolerations = append(tolerations, apiv1.Toleration{
			Key:      t[1],
			Operator: apiv1.TolerationOpEqual,
			Value:    t[2],
			Effect:   apiv1.TaintEffect(t[0]),
		})
	}
	return tolerations
}

// allowedSystemLabel checks if the system label is allowed to be considered for workload separation.
func allowedSystemLabel(key string) bool {
	return key == gkelabels.ComputeClassLabel || key == gkelabels.ManagedNodeLabel
}
