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
	"strings"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const (
	subnetPriorityKey = "internal.subnet-priority-0"
	podRangeKey       = "internal.pod-range-0"
)

type subnetPriorities struct {
	internalFeatureDefaultImplementation
}

func newSubnetPriorities() feature {
	return &subnetPriorities{}
}

func (f *subnetPriorities) FromNodepool(np *container.NodePool) Metadata {
	if np == nil || np.NetworkConfig == nil {
		return nil
	}

	m := make(Metadata)
	if np.NetworkConfig.Subnetwork != "" {
		subnet := np.NetworkConfig.Subnetwork
		if idx := strings.LastIndex(subnet, "/"); idx != -1 {
			subnet = subnet[idx+1:]
		}
		m[subnetPriorityKey] = subnet
	}

	if np.NetworkConfig.PodRange != "" {
		m[podRangeKey] = np.NetworkConfig.PodRange
	}

	if len(m) == 0 {
		return nil
	}
	return m
}

func (f *subnetPriorities) FromLabelRequirements(req podrequirements.LabelRequirements) Metadata {
	return nil
}

func (f *subnetPriorities) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NetworkConfig == nil || len(spec.NetworkConfig.SubnetPriorities) == 0 {
		return nil
	}

	m := make(Metadata)
	if name := spec.NetworkConfig.SubnetPriorities[0].Name; name != "" {
		m[subnetPriorityKey] = name
	}
	if podRange := spec.NetworkConfig.SubnetPriorities[0].PodRange; podRange != "" {
		m[podRangeKey] = podRange
	}

	if len(m) == 0 {
		return nil
	}
	return m
}

func (f *subnetPriorities) FromPriority(p v1.Priority) Metadata {
	return nil
}

func (f *subnetPriorities) ToNodePoolLabels(labels map[string]string, m Metadata) {}

func (f *subnetPriorities) ToNodepool(np *container.NodePool, m Metadata) {
	if np == nil || len(m) == 0 {
		return
	}

	subnet := m[subnetPriorityKey]
	podRange := m[podRangeKey]
	if subnet == "" && podRange == "" {
		return
	}

	if np.NetworkConfig == nil {
		np.NetworkConfig = &container.NodeNetworkConfig{}
	}

	if subnet != "" {
		np.NetworkConfig.Subnetwork = subnet
		if !slices.Contains(np.NetworkConfig.ForceSendFields, "Subnetwork") {
			np.NetworkConfig.ForceSendFields = append(np.NetworkConfig.ForceSendFields, "Subnetwork")
		}
	}
	if podRange != "" {
		np.NetworkConfig.PodRange = podRange
		if !slices.Contains(np.NetworkConfig.ForceSendFields, "PodRange") {
			np.NetworkConfig.ForceSendFields = append(np.NetworkConfig.ForceSendFields, "PodRange")
		}
	}
}
