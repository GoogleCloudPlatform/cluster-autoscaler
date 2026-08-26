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

package flexstart

import (
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
	"k8s.io/klog/v2"
)

const (
	// FlexStartNonQueuedExpDisabled - the Flex Start Non-Queued node pools handling was disabled by experiment, their MIGs shouldn't get scaled up.
	FlexStartNonQueuedExpDisabled scaleblocking.BlockedMigReason = "flexstart.nonqueued.disabled"
)

// CloudProvider is the subset of GkeCloudProvider.
type CloudProvider interface {
	GetGkeMigs() []*gke.GkeMig
}

// BlockedMigsSource blocks scale ups only when `FlexStartNonQueuedEnabledFlag` experiment is disabled
// to safely disable broken/incomplete scale up path for Flex Start Non-Queued (FSNQ) node pools.
// FSQN cannot be scaled using regular scale ups, thus scaling them up should be disabled.
// The scale down code path is the same as for regular scale ups, so the nodes are permitted to scale down.
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

// BlockedMigs returns MIGs which should have scale-up blocked due to disabled Flex Start Non-Queued (FSNQ) node pools handling.
func (b BlockedMigsSource) BlockedMigs() scaleblocking.BlockedMigs {
	flexStartNonQueuedExpEnabled := b.experimentsManager.EvaluateMinimumVersionFlagOrFailsafe(experiments.FlexStartNonQueuedEnabledFlag, false)

	if flexStartNonQueuedExpEnabled {
		return scaleblocking.BlockedMigs{}
	}

	// Experiment is disabled, so block scale up for FlexStartNonQueued MIGs
	blocked := make(map[string]scaleblocking.BlockedMigReasonSet)
	blockedMigRefs := []gce.GceRef{}
	for _, mig := range b.cloudProvider.GetGkeMigs() {
		if mig.FlexStartNonQueued() {
			blocked[mig.Id()] = scaleblocking.BlockedMigReasonSet{FlexStartNonQueuedExpDisabled: true}
			blockedMigRefs = append(blockedMigRefs, mig.GceRef())
		}
	}

	if len(blockedMigRefs) > 0 {
		klog.Infof("Experiment %q is disabled, blocking scale up for Flex Start Non-Queued MIGs (project/zone/migName): %+v", experiments.FlexStartNonQueuedEnabledFlag, blockedMigRefs)
	}
	return scaleblocking.BlockedMigs{
		NoScaleUpMigs: blocked,
	}
}

// CleanUp satisfies the interface.
func (b BlockedMigsSource) CleanUp() {}
