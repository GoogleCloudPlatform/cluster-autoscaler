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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const (
	pdbTimeoutDurationMetadataKey               = "NodeDrainConfigPdbTimeoutDuration"
	graceTerminationDurationMetadataKey         = "NodeDrainConfigGraceTerminationDuration"
	respectPdbDuringNodePoolDeletionMetadataKey = "NodeDrainConfigRespectPdbDuringNodePoolDeletion"
)

// nodeDrainConfig is a self-service feature that enables NodeDrainConfig
// for NAP-created node pools.
type nodeDrainConfig struct {
	internalFeatureDefaultImplementation
}

func newNodeDrainConfig() feature {
	return &nodeDrainConfig{}
}

func (n *nodeDrainConfig) FromNodepool(pool *container.NodePool) Metadata {
	if pool == nil || pool.NodeDrainConfig == nil {
		return nil
	}
	m := make(Metadata)
	if pool.NodeDrainConfig.PdbTimeoutDuration != "" {
		m[pdbTimeoutDurationMetadataKey] = pool.NodeDrainConfig.PdbTimeoutDuration
	}
	if pool.NodeDrainConfig.GraceTerminationDuration != "" {
		m[graceTerminationDurationMetadataKey] = pool.NodeDrainConfig.GraceTerminationDuration
	}
	m[respectPdbDuringNodePoolDeletionMetadataKey] = strconv.FormatBool(pool.NodeDrainConfig.RespectPdbDuringNodePoolDeletion)
	return m
}

func (n *nodeDrainConfig) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (n *nodeDrainConfig) FromCccSpec(spec v1.ComputeClassSpec) Metadata {
	if spec.NodePoolConfig == nil || spec.NodePoolConfig.NodeDrainConfig == nil {
		return nil
	}
	ndc := spec.NodePoolConfig.NodeDrainConfig
	m := make(Metadata)
	if ndc.PdbTimeoutDuration != nil && *ndc.PdbTimeoutDuration != "" {
		m[pdbTimeoutDurationMetadataKey] = *ndc.PdbTimeoutDuration
	}
	if ndc.GraceTerminationDuration != nil && *ndc.GraceTerminationDuration != "" {
		m[graceTerminationDurationMetadataKey] = *ndc.GraceTerminationDuration
	}
	if ndc.RespectPdbDuringNodePoolDeletion != nil {
		m[respectPdbDuringNodePoolDeletionMetadataKey] = strconv.FormatBool(*ndc.RespectPdbDuringNodePoolDeletion)
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func (n *nodeDrainConfig) FromPriority(_ v1.Priority) Metadata {
	return nil
}

func (n *nodeDrainConfig) ToNodePoolLabels(_ map[string]string, _ Metadata) {
}

func (n *nodeDrainConfig) ToNodepool(pool *container.NodePool, metadata Metadata) {
	pdbTimeout, hasPdbTimeout := metadata[pdbTimeoutDurationMetadataKey]
	graceTermination, hasGraceTermination := metadata[graceTerminationDurationMetadataKey]
	respectPdb, hasRespectPdb := metadata[respectPdbDuringNodePoolDeletionMetadataKey]

	if !hasPdbTimeout && !hasGraceTermination && !hasRespectPdb {
		return
	}

	if pool.NodeDrainConfig == nil {
		pool.NodeDrainConfig = &container.NodeDrainConfig{}
	}

	if hasPdbTimeout {
		pool.NodeDrainConfig.PdbTimeoutDuration = pdbTimeout
		if !slices.Contains(pool.NodeDrainConfig.ForceSendFields, "PdbTimeoutDuration") {
			pool.NodeDrainConfig.ForceSendFields = append(pool.NodeDrainConfig.ForceSendFields, "PdbTimeoutDuration")
		}
	}
	if hasGraceTermination {
		pool.NodeDrainConfig.GraceTerminationDuration = graceTermination
		if !slices.Contains(pool.NodeDrainConfig.ForceSendFields, "GraceTerminationDuration") {
			pool.NodeDrainConfig.ForceSendFields = append(pool.NodeDrainConfig.ForceSendFields, "GraceTerminationDuration")
		}
	}
	if hasRespectPdb {
		pool.NodeDrainConfig.RespectPdbDuringNodePoolDeletion = (respectPdb == "true")
		if !slices.Contains(pool.NodeDrainConfig.ForceSendFields, "RespectPdbDuringNodePoolDeletion") {
			pool.NodeDrainConfig.ForceSendFields = append(pool.NodeDrainConfig.ForceSendFields, "RespectPdbDuringNodePoolDeletion")
		}
	}
}
