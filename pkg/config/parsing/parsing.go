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

package parsing

import (
	"fmt"
	"strconv"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"

	internalopts "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/config/options"
)

// ParseResourceList parses the given configuration map into an API
// ResourceList or returns an error.
// https://github.com/kubernetes/kubernetes/blob/5ec31e84d6c525c173906b1497ee6f075c1926e9/cmd/kubelet/app/server.go#L1337
func ParseResourceList(m map[string]string) (apiv1.ResourceList, error) {
	if len(m) == 0 {
		return nil, nil
	}
	rl := make(apiv1.ResourceList)
	for k, v := range m {
		switch apiv1.ResourceName(k) {
		// CPU and memory resources are supported.
		case apiv1.ResourceCPU, apiv1.ResourceMemory:
			q, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse quantity %q for %q resource: %w", v, k, err)
			}
			if q.Sign() == -1 {
				return nil, fmt.Errorf("resource quantity for %q cannot be negative: %v", k, v)
			}
			rl[apiv1.ResourceName(k)] = q
		default:
			return nil, fmt.Errorf("cannot prase %q resource", k)
		}
	}
	return rl, nil
}

func ResourceListString(m map[string]string) string {
	return fmt.Sprintf("\"cpu=%s,memory=%s\"", m["cpu"], m["memory"])
}

func GetSystemNamespaces(systemNamespaces string) []string {
	s := strings.Split(systemNamespaces, ",")
	klog.Infof("System namespaces: %v", s)
	return s
}

// ParseMultipleNodeLimits allows for extracting mapping of node family names and their maximum nodes capacity.
// This information is passed by a flag in a format of f1:n1,f2:n2,f3:n3 where f1, f2, f3 are family names and
// n1, n2, n3 are maximum node limit values.
func ParseMultipleNodeLimits(flags string) (map[string]int64, error) {
	var parsedNodeLimits = map[string]int64{}
	if len(flags) > 0 {
		for _, nodeLimit := range strings.Split(flags, ",") {
			family, maxNodes, err := parseSingleNodeLimit(nodeLimit)
			if err != nil {
				return nil, err
			}
			parsedNodeLimits[family] = maxNodes
		}
	}
	return parsedNodeLimits, nil
}

// parseSingleNodeLimit is used to parse and validate a maximum node count for a single machine family.
func parseSingleNodeLimit(limits string) (string, int64, error) {
	parts := strings.Split(limits, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("incorrect max compact placement nodes specification: %v", limits)
	}
	maxNodes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("incorrect compact placement value - max node count is not an integer: %v", limits)
	}
	if maxNodes <= 0 {
		return "", 0, fmt.Errorf("incorrect compact placement value - max node count is a negative number: %v", limits)
	}
	return parts[0], maxNodes, nil
}

func ValidateMppnFlags(expanderNames string) error {
	dynamicMppnExpanderEnabled := strings.Contains(expanderNames, internalopts.MaxPodsPerNodeExpanderName)
	if !dynamicMppnExpanderEnabled {
		return fmt.Errorf("mppn expander is not present")
	}
	return nil
}
