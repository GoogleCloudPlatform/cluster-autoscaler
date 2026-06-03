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

package tpu

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/machinetypes"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
	klog "k8s.io/klog/v2"
)

const (
	// BlockedMigTPUReconciling - the TPU MIG has scaling blocked because its node pool is reconciling.
	BlockedMigTPUReconciling scaleblocking.BlockedMigReason = "blocked.mig.tpu.reconciling"
)

// CloudProvider is the subset of GkeCloudProvider needed for ..BlockedMigsSource.
type CloudProvider interface {
	GetGkeMigs() []*gke.GkeMig
	MachineConfigProvider() *machinetypes.MachineConfigProvider
}

// NodeGroup is the subset of GkeMig need for ..BlockedMigSource.
type NodeGroup interface {
	cloudprovider.NodeGroup

	Status() string
}

// BlockedMigsSource provides ids of MIGs which should have scaling blocked because their node pools are snowflaked.
type BlockedMigsSource struct {
	cloudProvider CloudProvider
}

// NewBlockedMigsSource returns a BlockedMigsSource which provides blocked MIGs based on information from the snowflake watcher.
func NewBlockedMigsSource(cloudProvider CloudProvider) *BlockedMigsSource {
	return &BlockedMigsSource{cloudProvider: cloudProvider}
}

// BlockedMigs returns TPU MIGs which should have scaling blocked because their node pools are reconciling.
func (s *BlockedMigsSource) BlockedMigs() scaleblocking.BlockedMigs {
	migs := s.cloudProvider.GetGkeMigs()
	blocked := s.blockedMigs(migsToNodeGroups(migs))
	return scaleblocking.BlockedMigs{
		NoScaleUpMigs:   blocked,
		NoScaleDownMigs: blocked,
	}
}

// helper function extracted to allow for use of interface for node group (for testing).
func (s *BlockedMigsSource) blockedMigs(ngs []NodeGroup) map[string]scaleblocking.BlockedMigReasonSet {
	noScaleMigs := map[string]scaleblocking.BlockedMigReasonSet{}
	reason := scaleblocking.BlockedMigReasonSet{BlockedMigTPUReconciling: true}
	for _, ng := range ngs {
		if multihost, err := s.isMultiHostTpuMig(ng); err != nil {
			// Fail open and allow scaling if TPU configuration can't be determined.
			klog.Errorf("Failed to get TPU configuration node group %v: %v", ng.Id(), err)
		} else if multihost && ng.Status() == "RECONCILING" {
			noScaleMigs[ng.Id()] = reason
		}
	}
	return noScaleMigs
}

func migsToNodeGroups(migs []*gke.GkeMig) []NodeGroup {
	var ngs []NodeGroup
	for _, m := range migs {
		ngs = append(ngs, m)
	}
	return ngs
}

func (s *BlockedMigsSource) isMultiHostTpuMig(ng NodeGroup) (bool, error) {
	tr, found, err := getTpuFromTemplate(ng)
	if !found || err != nil {
		return false, err
	}

	return s.cloudProvider.MachineConfigProvider().IsMultiHostTpuPodslice(tr.TpuType, tr.Topology, tr.Count)
}

// CleanUp cleans up source's internal structures.
func (s *BlockedMigsSource) CleanUp() {
}
