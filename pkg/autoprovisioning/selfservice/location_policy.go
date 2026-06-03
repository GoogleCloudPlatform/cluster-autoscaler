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
	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
	"k8s.io/klog/v2"
)

const (
	LocationPolicyAny      = "ANY"
	LocationPolicyBalanced = "BALANCED"
)

// LocationPolicy self-service feature.
type locationPolicy struct{}

func newLocationPolicy() *locationPolicy {
	return &locationPolicy{}
}

// Creates metadata with LocationPolicy extracted from the node pool.
func (w *locationPolicy) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Autoscaling == nil || pool.Autoscaling.LocationPolicy == "" {
		return nil
	}
	m := make(Metadata)
	m[gkelabels.LocationPolicyLabelKey] = pool.Autoscaling.LocationPolicy
	return m
}

// LocationPolicy is defined at the level of priorities, as opposed to ComputeClass.Spec.NodePoolConfig.
func (w *locationPolicy) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	return nil
}

// Only if we let define location Policy at node pool config level.
func (w *locationPolicy) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if locationPolicy, found := metadata[gkelabels.LocationPolicyLabelKey]; found && locationPolicy != "" {
		if pool.Autoscaling == nil {
			pool.Autoscaling = &gke_api_beta.NodePoolAutoscaling{}
		}
		if locationPolicy != LocationPolicyAny && locationPolicy != LocationPolicyBalanced {
			klog.Warningf("Invalid LocationPolicy: %q; Expected values: %q, %q", locationPolicy, LocationPolicyAny, LocationPolicyBalanced)
		}
		pool.Autoscaling.LocationPolicy = locationPolicy
	}
}

func (w *locationPolicy) FromPriority(p v1.Priority) Metadata {
	if p.Location == nil || p.Location.LocationPolicy == nil || *p.Location.LocationPolicy == "" {
		return nil
	}
	return Metadata{gkelabels.LocationPolicyLabelKey: *p.Location.LocationPolicy}
}

func (w *locationPolicy) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

func (w *locationPolicy) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (w *locationPolicy) UpdateMig(mig GkeMigSetter, metadata Metadata) {
	if locationPolicy, exist := metadata[gkelabels.LocationPolicyLabelKey]; exist {
		mig.SetLocationPolicy(locationPolicy)
	}
}
