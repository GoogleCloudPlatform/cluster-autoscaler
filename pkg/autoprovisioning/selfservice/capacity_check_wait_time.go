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
	"fmt"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	gke_api_beta "google.golang.org/api/container/v1beta1"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

// capacityCheckWaitTimeSeconds is a self-service priority feature used to define
// for how many seconds will the given CCC priority be attempted to scale up before falling back to the next priority.
type capacityCheckWaitTimeSeconds struct {
	internalFeatureDefaultImplementation
}

func newCapacityCheckWaitTimeSeconds() feature {
	return &capacityCheckWaitTimeSeconds{}
}

func (w *capacityCheckWaitTimeSeconds) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil || pool.Config == nil {
		return nil
	}

	m := make(Metadata)
	if v, found := pool.Config.Labels[gkelabels.CapacityCheckWaitTimeSecondsLabel]; found {
		m[gkelabels.CapacityCheckWaitTimeSecondsLabel] = v
	}
	return m
}

func (w *capacityCheckWaitTimeSeconds) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (w *capacityCheckWaitTimeSeconds) FromCccSpec(_ v1.ComputeClassSpec) Metadata {
	return nil
}

func (w *capacityCheckWaitTimeSeconds) FromPriority(p v1.Priority) Metadata {
	m := make(Metadata)
	if p.CapacityCheckWaitTimeSeconds != nil {
		m[gkelabels.CapacityCheckWaitTimeSecondsLabel] = fmt.Sprintf("%d", *p.CapacityCheckWaitTimeSeconds)
	}
	return m
}

func (w *capacityCheckWaitTimeSeconds) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
	if v, found := metadata[gkelabels.CapacityCheckWaitTimeSecondsLabel]; found {
		labels[gkelabels.CapacityCheckWaitTimeSecondsLabel] = v
	}
}

func (w *capacityCheckWaitTimeSeconds) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	if v, found := metadata[gkelabels.CapacityCheckWaitTimeSecondsLabel]; found {
		if pool.Config == nil {
			pool.Config = &gke_api_beta.NodeConfig{}
		}
		if pool.Config.Labels == nil {
			pool.Config.Labels = make(map[string]string)
		}
		pool.Config.Labels[gkelabels.CapacityCheckWaitTimeSecondsLabel] = v
	}
}
