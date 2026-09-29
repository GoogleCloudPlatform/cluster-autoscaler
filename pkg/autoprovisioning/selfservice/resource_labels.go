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
	"strings"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const resourceLabelsPrefix = "resource-labels.cloud.google.com/"

func newResourceLabels() feature {
	return &resourceLabels{}
}

type resourceLabels struct {
	internalFeatureDefaultImplementation
}

// prefixResourceLabels maps each resource label to a flat metadata key.
// This allows CA's native superset matching logic to match node pools that
// have all required resource labels.
func prefixResourceLabels(resourceLabels map[string]string) Metadata {
	if len(resourceLabels) == 0 {
		return nil
	}
	m := make(Metadata, len(resourceLabels))
	for k, v := range resourceLabels {
		if k != "" {
			m[resourceLabelsPrefix+k] = v
		}
	}
	if len(m) > 0 {
		return m
	}
	return nil
}

func (rl *resourceLabels) FromNodepool(np *container.NodePool) Metadata {
	if np != nil && np.Config != nil {
		return prefixResourceLabels(np.Config.ResourceLabels)
	}
	return nil
}

func (rl *resourceLabels) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (rl *resourceLabels) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig != nil && len(spec.NodePoolConfig.ResourceLabels) > 0 {
		m := make(Metadata, len(spec.NodePoolConfig.ResourceLabels))
		for k, v := range spec.NodePoolConfig.ResourceLabels {
			if k != "" {
				m[resourceLabelsPrefix+k] = string(v)
			}
		}
		if len(m) > 0 {
			return m
		}
	}
	return nil
}

func (rl *resourceLabels) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (rl *resourceLabels) ToNodePoolLabels(_ map[string]string, _ Metadata) {}

func (rl *resourceLabels) ToNodepool(np *container.NodePool, m Metadata) {
	if np == nil {
		return
	}
	var labels map[string]string
	for k, v := range m {
		if strings.HasPrefix(k, resourceLabelsPrefix) {
			key := strings.TrimPrefix(k, resourceLabelsPrefix)
			if key == "" {
				continue
			}
			if labels == nil {
				labels = make(map[string]string)
			}
			labels[key] = v
		}
	}
	if len(labels) == 0 {
		return
	}
	if np.Config == nil {
		np.Config = &container.NodeConfig{}
	}
	if np.Config.ResourceLabels == nil {
		np.Config.ResourceLabels = make(map[string]string)
	}
	for k, v := range labels {
		np.Config.ResourceLabels[k] = v
	}
}
