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

package bulkmig

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
	"k8s.io/klog/v2"
)

const (
	FlexStartNonQueuedBulkMigsExpDisabled scaleblocking.BlockedMigReason = "bulkmig.fsnq.disabled"
	FlexStartQueuedBulkMigsExpDisabled    scaleblocking.BlockedMigReason = "bulkmig.fsq.disabled"
)

// CloudProvider is the subset of GkeCloudProvider.
type CloudProvider interface {
	GetGkeMigs() []*gke.GkeMig
}

type BlockedMigsSource struct {
	cloudProvider      CloudProvider
	experimentsManager experiments.Manager
}

// NewBlockedMigsSource returns a new instance of BlockedMigsSource.
func NewBlockedMigsSource(cloudProvider CloudProvider, experimentsManager experiments.Manager) BlockedMigsSource {
	return BlockedMigsSource{
		cloudProvider:      cloudProvider,
		experimentsManager: experimentsManager,
	}
}

// BlockedMigs returns MIGs which should have scale-up blocked due to disabled Flex Start Bulk Migs node pools handling.
func (b BlockedMigsSource) BlockedMigs() scaleblocking.BlockedMigs {
	bulkFSQEnabled := b.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.ProvisioningRequestBulkMigsFlag, false)
	bulkFSNQEnabled := b.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexStartNonQueuedBulkMigsFlag, false)

	// Don't block any migs if experiments are enabled
	if bulkFSQEnabled && bulkFSNQEnabled {
		return scaleblocking.BlockedMigs{}
	}

	blocked := make(map[string]scaleblocking.BlockedMigReasonSet)
	blockedMigRefs := []gce.GceRef{}
	for _, mig := range b.cloudProvider.GetGkeMigs() {
		if !mig.FlexStart() || !mig.UsesBulkProvisioning() {
			continue
		}

		if !bulkFSNQEnabled && !mig.QueuedProvisioning() {
			blocked[mig.Id()] = scaleblocking.BlockedMigReasonSet{FlexStartNonQueuedBulkMigsExpDisabled: true}
			blockedMigRefs = append(blockedMigRefs, mig.GceRef())
			continue
		}

		if !bulkFSQEnabled && mig.QueuedProvisioning() {
			blocked[mig.Id()] = scaleblocking.BlockedMigReasonSet{FlexStartQueuedBulkMigsExpDisabled: true}
			blockedMigRefs = append(blockedMigRefs, mig.GceRef())
			continue
		}
	}

	if len(blockedMigRefs) > 0 {
		klog.Infof("Blocking scale up for FlexStart Bulk MIGs (project/zone/migName): %+v", blockedMigRefs)
	}
	return scaleblocking.BlockedMigs{
		NoScaleUpMigs: blocked,
	}
}

// CleanUp satisfies the interface.
func (b BlockedMigsSource) CleanUp() {}
