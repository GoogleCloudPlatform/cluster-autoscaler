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

package nodesnowflake

import (
	"context"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
)

const (
	// BlockedMigSnowflaked - the MIG has scaling blocked because its node pool is snowflaked.
	BlockedMigSnowflaked scaleblocking.BlockedMigReason = "blocked.mig.snowflaked"
)

// CloudProvider is the subset of GkeCloudProvider needed for nodesnowflake.BlockedMigsSource.
type CloudProvider interface {
	GetGkeMigs() []*gke.GkeMig
}

// BlockedMigsSource provides ids of MIGs which should have scaling blocked because their node pools are snowflaked.
type BlockedMigsSource struct {
	cloudProvider CloudProvider
	sw            Watcher
}

// NewBlockedMigsSource returns a BlockedMigsSource which provides blocked MIGs based on information from the snowflake watcher.
func NewBlockedMigsSource(cloudProvider CloudProvider, sw Watcher) *BlockedMigsSource {
	return &BlockedMigsSource{cloudProvider: cloudProvider, sw: sw}
}

// BlockedMigs returns MIGs which should have scaling blocked because their node pools are snowflaked.
func (s *BlockedMigsSource) BlockedMigs() scaleblocking.BlockedMigs {
	result := scaleblocking.BlockedMigs{
		NoScaleUpMigs:   map[string]scaleblocking.BlockedMigReasonSet{},
		NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{},
	}
	noScaleUpNodePools := s.sw.NoScaleUpNodePools()
	for _, mig := range s.cloudProvider.GetGkeMigs() {
		if noScaleUpNodePools.Has(mig.NodePoolName()) {
			result.NoScaleUpMigs[mig.Id()] = scaleblocking.BlockedMigReasonSet{BlockedMigSnowflaked: true}
		}
	}
	noScaleDownNodePools := s.sw.NoScaleDownNodePools()
	for _, mig := range s.cloudProvider.GetGkeMigs() {
		if noScaleDownNodePools.Has(mig.NodePoolName()) {
			result.NoScaleDownMigs[mig.Id()] = scaleblocking.BlockedMigReasonSet{BlockedMigSnowflaked: true}
		}
	}
	return result
}

// Run starts the watcher goroutine
func (s *BlockedMigsSource) Run(ctx context.Context) {
	go s.sw.Run(ctx)
}

// CleanUp is a no-op as the watcher goroutine is stopped by context cancellation.
func (s *BlockedMigsSource) CleanUp() {
}
