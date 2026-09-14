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

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const (
	PerformanceMonitoringUnitMetadataKey = "AdvancedMachineFeaturesPerformanceMonitoringUnit"
)

// performanceMonitoringUnit is a self-service priority feature used to define
// PerformanceMonitoringUnit for the created nodepool.
type performanceMonitoringUnit struct {
	internalFeatureDefaultImplementation
}

func newPerformanceMonitoringUnit() feature {
	return &performanceMonitoringUnit{}
}

func (p *performanceMonitoringUnit) FromNodepool(pool *container.NodePool) Metadata {
	if pool == nil || pool.Config == nil || pool.Config.AdvancedMachineFeatures == nil || pool.Config.AdvancedMachineFeatures.PerformanceMonitoringUnit == "" {
		return nil
	}
	m := make(Metadata)
	m[PerformanceMonitoringUnitMetadataKey] = pool.Config.AdvancedMachineFeatures.PerformanceMonitoringUnit
	return m
}

func (p *performanceMonitoringUnit) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (p *performanceMonitoringUnit) FromCccSpec(_ v1.ComputeClassSpec) Metadata {
	return nil
}

func (p *performanceMonitoringUnit) FromPriority(pr v1.Priority) Metadata {
	if pr.PerformanceMonitoringUnit == nil || *pr.PerformanceMonitoringUnit == "" {
		return nil
	}
	m := make(Metadata)
	m[PerformanceMonitoringUnitMetadataKey] = *pr.PerformanceMonitoringUnit
	return m
}

func (p *performanceMonitoringUnit) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

func (p *performanceMonitoringUnit) ToNodepool(pool *container.NodePool, metadata Metadata) {
	val, found := metadata[PerformanceMonitoringUnitMetadataKey]
	if !found {
		return
	}
	if pool == nil {
		return
	}
	if pool.Config == nil {
		pool.Config = &container.NodeConfig{}
	}
	if pool.Config.AdvancedMachineFeatures == nil {
		pool.Config.AdvancedMachineFeatures = &container.AdvancedMachineFeatures{}
	}
	pool.Config.AdvancedMachineFeatures.PerformanceMonitoringUnit = val
	if amf := pool.Config.AdvancedMachineFeatures; !slices.Contains(amf.ForceSendFields, "PerformanceMonitoringUnit") {
		amf.ForceSendFields = append(amf.ForceSendFields, "PerformanceMonitoringUnit")
	}
}
