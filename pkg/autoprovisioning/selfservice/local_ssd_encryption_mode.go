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
	container "google.golang.org/api/container/v1beta1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/podrequirements"
)

const (
	LocalSSDEncryptionModeMetadataKey = "LocalSSDEncryptionMode"
)

// localSSDEncryptionMode is a self-service feature that enables LocalSSDEncryptionMode
// for NAP-created node pools.
type localSSDEncryptionMode struct {
	internalFeatureDefaultImplementation
}

func newLocalSSDEncryptionMode() feature {
	return &localSSDEncryptionMode{}
}

func (l *localSSDEncryptionMode) FromNodepool(pool *container.NodePool) Metadata {
	if pool == nil || pool.Config == nil || pool.Config.LocalSsdEncryptionMode == "" {
		return nil
	}
	m := make(Metadata)
	m[LocalSSDEncryptionModeMetadataKey] = pool.Config.LocalSsdEncryptionMode
	return m
}

func (l *localSSDEncryptionMode) FromLabelRequirements(_ podrequirements.LabelRequirements) Metadata {
	return nil
}

func (l *localSSDEncryptionMode) FromCccSpec(_ v1.ComputeClassSpec) Metadata {
	return nil
}

func (l *localSSDEncryptionMode) FromPriority(p v1.Priority) Metadata {
	if p.Storage == nil || p.Storage.LocalSSDEncryptionMode == nil || *p.Storage.LocalSSDEncryptionMode == "" {
		return nil
	}
	m := make(Metadata)
	m[LocalSSDEncryptionModeMetadataKey] = *p.Storage.LocalSSDEncryptionMode
	return m
}

func (l *localSSDEncryptionMode) ToNodePoolLabels(labels map[string]string, metadata Metadata) {
}

func (l *localSSDEncryptionMode) ToNodepool(pool *container.NodePool, metadata Metadata) {
	v, found := metadata[LocalSSDEncryptionModeMetadataKey]
	if !found {
		return
	}
	if pool == nil || pool.Config == nil {
		pool.Config = &container.NodeConfig{}
	}
	pool.Config.LocalSsdEncryptionMode = v
}
