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
	"sort"
	"strings"

	v1 "github.com/googlecloudplatform/compute-class-api/api/cloud.google.com/v1"
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const networkTagsPrefix = "network-tags.cloud.google.com/"

func newNetworkTags() feature {
	return &networkTags{}
}

type networkTags struct {
	internalFeatureDefaultImplementation
}

// prefixNetworkTags maps each tag in networkTags to a flat metadata key.
// This allows CA's native superset matching logic to match node pools that
// have all required network tags.
func prefixNetworkTags(networkTags []string) Metadata {
	if len(networkTags) == 0 {
		return nil
	}
	m := make(Metadata, len(networkTags))
	for _, tag := range networkTags {
		if tag != "" {
			m[networkTagsPrefix+tag] = "true"
		}
	}
	if len(m) > 0 {
		return m
	}
	return nil
}

func (nt *networkTags) FromNodepool(np *container.NodePool) Metadata {
	if np != nil && np.Config != nil {
		return prefixNetworkTags(np.Config.Tags)
	}
	return nil
}

func (nt *networkTags) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (nt *networkTags) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig != nil {
		return prefixNetworkTags(spec.NodePoolConfig.NetworkTags)
	}
	return nil
}

func (nt *networkTags) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (nt *networkTags) ToNodePoolLabels(_ map[string]string, _ Metadata) {}

func (nt *networkTags) ToNodepool(np *container.NodePool, m Metadata) {
	if np == nil {
		return
	}
	var tags []string
	for k, v := range m {
		if strings.HasPrefix(k, networkTagsPrefix) && v == "true" {
			tag := strings.TrimPrefix(k, networkTagsPrefix)
			if tag != "" {
				tags = append(tags, tag)
			}
		}
	}
	if len(tags) == 0 {
		return
	}
	sort.Strings(tags)
	if np.Config == nil {
		np.Config = &container.NodeConfig{}
	}
	np.Config.Tags = tags
}
