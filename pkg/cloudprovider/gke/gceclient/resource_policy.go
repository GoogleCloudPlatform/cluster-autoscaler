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

package gceclient

import (
	"fmt"

	gce_api_beta "google.golang.org/api/compute/v0.beta"
)

// GceResourcePolicy is a GKE cluster autoscaler domain object abstracting GCE resource policy.
// Only data used in cluster autoscaler are defined and populated from GCE API.
type GceResourcePolicy struct {
	Id              uint64
	Name            string
	Status          string
	PlacementPolicy PlacementPolicy
	WorkloadPolicy  WorkloadPolicy
}

type PlacementPolicy struct {
	VmCount                 int64
	AvailabilityDomainCount int64
	Collocation             string
	GpuTopology             string
	TpuTopology             string
	MaxDistance             int64
	SliceCount              int64
}

type WorkloadPolicy struct {
	AcceleratorTopology string
	MaxTopologyDistance string
	Type                string
}

func toGceResourcePolicy(item *gce_api_beta.ResourcePolicy) (*GceResourcePolicy, error) {
	if item == nil {
		return nil, fmt.Errorf("GCE resource policy is nil")
	}

	return &GceResourcePolicy{
		Id:              item.Id,
		Name:            item.Name,
		Status:          item.Status,
		PlacementPolicy: toPlacementPolicy(item.GroupPlacementPolicy),
		WorkloadPolicy:  toWorkloadPolicy(item.WorkloadPolicy),
	}, nil
}

func toPlacementPolicy(p *gce_api_beta.ResourcePolicyGroupPlacementPolicy) PlacementPolicy {
	if p != nil {
		return PlacementPolicy{
			VmCount:                 p.VmCount,
			AvailabilityDomainCount: p.AvailabilityDomainCount,
			Collocation:             p.Collocation,
			GpuTopology:             p.GpuTopology,
			TpuTopology:             p.TpuTopology,
			MaxDistance:             p.MaxDistance,
			SliceCount:              p.SliceCount,
		}
	}
	return PlacementPolicy{}
}

func toWorkloadPolicy(wp *gce_api_beta.ResourcePolicyWorkloadPolicy) WorkloadPolicy {
	if wp != nil {
		return WorkloadPolicy{
			AcceleratorTopology: wp.AcceleratorTopology,
			MaxTopologyDistance: wp.MaxTopologyDistance,
			Type:                wp.Type,
		}
	}
	return WorkloadPolicy{}
}
