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

package metrics

import (
	k8smetrics "k8s.io/component-base/metrics"
)

// FeatureName represents a name of a feature used in metric labels.
type FeatureName string

const (
	// DraFeatureName is the name of the DynamicResourceAllocation feature.
	DraFeatureName FeatureName = "dra"
	// CapacityBufferFeatureName is the name of the CapacityBuffer feature.
	CapacityBufferFeatureName FeatureName = "capacity_buffer"
	// FastpathBinpackingFeatureName is the name of the Fastpath Binpacking feature.
	FastpathBinpackingFeatureName = "fastpath_binpacking"
	// HTNAPFeatureName is the name of the High Throughput NAP feature.
	HTNAPFeatureName = "htnap"
	// IncreasedMaxNodesPerScaleUpFeatureName is the name of the IncreasedMaxNodesPerScaleUp feature.
	IncreasedMaxNodesPerScaleUpFeatureName = "increased_max_nodes_per_scale_up"
)

var featureEnabled = k8smetrics.NewGaugeVec(
	&k8smetrics.GaugeOpts{
		Namespace: caNamespace,
		Name:      "feature_enabled",
		Help:      "Whether or not a given feature is enabled. 1 if it is, 0 otherwise.",
	},
	[]string{"feature_name"},
)

// UpdateFeatureEnabled records if a given feature is enabled
func (*prometheusMetrics) UpdateFeatureEnabled(featureName FeatureName, enabled bool) {
	if enabled {
		featureEnabled.WithLabelValues(string(featureName)).Set(1)
	} else {
		featureEnabled.WithLabelValues(string(featureName)).Set(0)
	}
}
