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
	"strconv"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const (
	nestedVirtualizationMetadataKey = "AdvancedMachineFeaturesEnableNestedVirtualization"
)

// nestedVirtualization is a self-service priority feature used to define
// whether nested virtualization should be enabled for the created nodepool.
type nestedVirtualization struct {
	internalFeatureDefaultImplementation
	// TODO(b/539947856): Remove once Giraffe experiment is concluded
	experimentsManager experiments.Manager
}

func newNestedVirtualization(experimentsManager experiments.Manager) feature {
	return &nestedVirtualization{
		experimentsManager: experimentsManager,
	}
}

func (w *nestedVirtualization) experimentEnabled() bool {
	if w == nil || w.experimentsManager == nil {
		return false
	}
	return w.experimentsManager.DirectLaunchBoolFlag(experiments.EnableNestedVirtualizationEnabledFlag) &&
		w.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.EnableNestedVirtualizationMinCAVersionFlag, true)
}

func (w *nestedVirtualization) FromNodepool(pool *container.NodePool) Metadata {
	if !w.experimentEnabled() {
		return nil
	}

	if pool == nil || pool.Config == nil || pool.Config.AdvancedMachineFeatures == nil {
		return nil
	}

	m := make(Metadata)
	m[nestedVirtualizationMetadataKey] = strconv.FormatBool(pool.Config.AdvancedMachineFeatures.EnableNestedVirtualization)
	return m
}

func (w *nestedVirtualization) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (w *nestedVirtualization) FromCccSpec(_ v1.ComputeClassSpec) Metadata {
	return nil
}

func (w *nestedVirtualization) FromPriority(p v1.Priority) Metadata {
	if !w.experimentEnabled() {
		return nil
	}

	if p.EnableNestedVirtualization == nil {
		return nil
	}
	m := make(Metadata)
	m[nestedVirtualizationMetadataKey] = strconv.FormatBool(*p.EnableNestedVirtualization)
	return m
}

func (w *nestedVirtualization) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

func (w *nestedVirtualization) ToNodepool(pool *container.NodePool, metadata Metadata) {
	if !w.experimentEnabled() {
		return
	}

	val, found := metadata[nestedVirtualizationMetadataKey]
	if !found {
		return
	}

	if pool.Config == nil {
		pool.Config = &container.NodeConfig{}
	}
	if pool.Config.AdvancedMachineFeatures == nil {
		pool.Config.AdvancedMachineFeatures = &container.AdvancedMachineFeatures{}
	}

	pool.Config.AdvancedMachineFeatures.EnableNestedVirtualization = (val == "true")
	if amf := pool.Config.AdvancedMachineFeatures; !slices.Contains(amf.ForceSendFields, "EnableNestedVirtualization") {
		amf.ForceSendFields = append(amf.ForceSendFields, "EnableNestedVirtualization")
	}
}
