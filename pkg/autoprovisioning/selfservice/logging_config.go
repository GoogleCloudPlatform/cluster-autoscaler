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
)

// loggingConfig is a self-service feature which lets users configure node pool logging in CCC definitions.
// It maps the following setting in cluster service API:
// http://google3/google/container/v1beta1/cluster_service.proto;l=7145;rcl=814061873
type loggingConfig struct {
	internalFeatureDefaultImplementation
}

func newLoggingConfig() feature {
	return &loggingConfig{}
}

// FromNodepool returns metadata with LoggingConfigVariant label, if logging variant is configured in the node pool.
// Otherwise, it returns nil.
func (w *loggingConfig) FromNodepool(pool *gke_api_beta.NodePool) Metadata {
	if pool == nil ||
		pool.Config == nil ||
		pool.Config.LoggingConfig == nil ||
		pool.Config.LoggingConfig.VariantConfig == nil ||
		pool.Config.LoggingConfig.VariantConfig.Variant == "" {
		return nil
	}
	return Metadata{gkelabels.LoggingConfigVariant: pool.Config.LoggingConfig.VariantConfig.Variant}
}

// FromCccSpec returns medatada with LoggingConfigVariant label, if it is enabled in the CCC definition.
// Otherwise, it returns empty metadata.
func (w *loggingConfig) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil ||
		spec.NodePoolConfig.LoggingConfig == nil ||
		spec.NodePoolConfig.LoggingConfig.LoggingVariantConfig == nil ||
		spec.NodePoolConfig.LoggingConfig.LoggingVariantConfig.Variant == "" {
		return nil
	}
	return Metadata{gkelabels.LoggingConfigVariant: spec.NodePoolConfig.LoggingConfig.LoggingVariantConfig.Variant}
}

// ToNodepool sets node pool's logging config in the node pool, if metadata contains LoggingConfigVariant label.
func (w *loggingConfig) ToNodepool(pool *gke_api_beta.NodePool, metadata Metadata) {
	loggingVariant := metadata[gkelabels.LoggingConfigVariant]
	if loggingVariant == "" {
		return
	}

	if pool.Config == nil {
		pool.Config = &gke_api_beta.NodeConfig{}
	}
	if pool.Config.LoggingConfig == nil {
		pool.Config.LoggingConfig = &gke_api_beta.NodePoolLoggingConfig{}
	}
	if pool.Config.LoggingConfig.VariantConfig == nil {
		pool.Config.LoggingConfig.VariantConfig = &gke_api_beta.LoggingVariantConfig{}
	}

	pool.Config.LoggingConfig.VariantConfig.Variant = loggingVariant
}

// FromPriority is not implemented, because node pool's logging config is set at the level of NodePoolConfig,
// not at the level of priorities.
func (w *loggingConfig) FromPriority(p v1.Priority) Metadata {
	return nil
}

// ToNodePoolLabels sets "cloud.google.com/gke-logging-variant" label on the node pool as it's needed for internal
// CA scheduler simulation to properly estimate the size of pod (MAX_THROUGHPUT requires a different variant of
// fluentbit logging DeamonSet).
func (w *loggingConfig) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
	loggingVariant := metadata[gkelabels.LoggingConfigVariant]
	if loggingVariant == "" {
		return
	}

	labels[gkelabels.LoggingConfigVariant] = loggingVariant
}

// FromLabelRequirements is not implemented, as labels are not needed for node pool's logging config.
func (w *loggingConfig) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}
