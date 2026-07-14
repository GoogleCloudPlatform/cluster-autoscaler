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

package bluegreen

import (
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
)

const (
	// BlockedMigBlueGreen - the MIG has scaling blocked because of an ongoing Blue/Green update.
	BlockedMigBlueGreen scaleblocking.BlockedMigReason = "blocked.mig.blue.green"
)

// CloudProvider is the subset of GkeCloudProvider needed for bluegreen.BlockedMigsSource.
type CloudProvider interface {
	GetGkeMigs() []*gke.GkeMig
}

// BlockedMigsSource provides the ids of MIGs which should have scaling blocked because of an ongoing Blue/Green update.
// More details: go/gke-ca-with-bg-update-dd.
type BlockedMigsSource struct {
	cloudProvider CloudProvider
}

// NewBlockedMigsSource returns a new instance of bluegreen.BlockedMigsSource.
func NewBlockedMigsSource(cloudProvider CloudProvider) BlockedMigsSource {
	return BlockedMigsSource{cloudProvider: cloudProvider}
}

// BlockedMigs returns the ids of MIGs which should have scaling blocked because of an ongoing Blue/Green update.
func (b BlockedMigsSource) BlockedMigs() scaleblocking.BlockedMigs {
	result := scaleblocking.BlockedMigs{
		NoScaleUpMigs:   map[string]scaleblocking.BlockedMigReasonSet{},
		NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{},
	}
	for _, mig := range b.cloudProvider.GetGkeMigs() {
		shouldBlockScaleUp, shouldBlockScaleDown := ShouldBlockScaling(mig.BlueGreenInfo())
		if shouldBlockScaleUp {
			result.NoScaleUpMigs[mig.Id()] = scaleblocking.BlockedMigReasonSet{BlockedMigBlueGreen: true}
		}
		if shouldBlockScaleDown {
			result.NoScaleDownMigs[mig.Id()] = scaleblocking.BlockedMigReasonSet{BlockedMigBlueGreen: true}
		}
	}
	return result
}

// ShouldBlockScaling returns whether a given MIG should have scaling blocked because of an ongoing Blue/Green update, based
// on its MigBlueGreenInfo.
func ShouldBlockScaling(migBgi *gke.MigBlueGreenInfo) (shouldBlockScaleUp, shouldBlockScaleDown bool) {
	if migBgi == nil {
		// The MIG is not taking part in an ongoing B/G update.
		return false, false
	}
	if migBgi.IsAutoScaled {
		return shouldBlockScalingForAutoscaledBG(migBgi)
	}
	return shouldBlockScalingForStandardBG(migBgi)
}

// shouldBlockScalingForAutoscaledBlueGreen returns whether a given MIG should have scaling blocked because of an ongoing
// Autoscaled Blue/Green update operation, based on its MigBlueGreenInfo.
// Details of how scaling should be blocked: go/autoscaled-bluegreen-upgrades-public-preview.
func shouldBlockScalingForAutoscaledBG(migBgi *gke.MigBlueGreenInfo) (shouldBlockScaleUp, shouldBlockScaleDown bool) {
	if migBgi.Color == gke.BlueMig {
		// Blue MIG can only scale down in the WAITING_TO_DRAIN_BLUE_POOL phase.
		if migBgi.Phase == gkeclient.PhaseWaitingToDrainBluePool {
			return true, false
		}
		// Blue MIG will operate in regular mode in ROLLBACK_STARTED phase.
		if migBgi.Phase == gkeclient.PhaseRollbackStarted {
			return false, false
		}
		// Blue MIG scaling is completely disabled in all other ABG update phases.
		return true, true
	}
	if migBgi.Color == gke.GreenMig {
		// Green MIG is disabled to scale-up/scale-down in ROLLBACK_STARTED phase.
		if migBgi.Phase == gkeclient.PhaseRollbackStarted {
			return true, true
		}
		// Green MIG will operate in regular mode for rest of the ABG update phases.
		return false, false
	}
	return false, false
}

// shouldBlockScalingForStandardBG returns whether a given MIG should have scaling blocked because of an ongoing
// standard Blue/Green update operation, based on its MigBlueGreenInfo.
// Details of how scaling should be blocked: go/ca-bg-scale-blocking.
func shouldBlockScalingForStandardBG(migBgi *gke.MigBlueGreenInfo) (shouldBlockScaleUp, shouldBlockScaleDown bool) {
	if migBgi.Color == gke.BlueMig {
		// Blue MIG will operate in scale-up-only mode in ROLLBACK_STARTED phase.
		if migBgi.Phase == gkeclient.PhaseRollbackStarted {
			return false, true
		}
		// Blue MIG scaling is completely disabled in all other B/G phases.
		return true, true
	}
	if migBgi.Color == gke.GreenMig {
		// Green MIG will operate in regular mode in NODE_POOL_SOAKING and DELETING_BLUE_POOL phases.
		if migBgi.Phase == gkeclient.PhaseNodePoolSoaking || migBgi.Phase == gkeclient.PhaseDeletingBluePool {
			return false, false
		}
		// Green MIG should have scaling completely blocked in ROLLBACK_STARTED phase.
		if migBgi.Phase == gkeclient.PhaseRollbackStarted {
			return true, true
		}
		// Green MIG should operate in scale-up-only mode in all other B/G phases.
		return false, true
	}
	return false, false
}

// CleanUp is a no-op.
func (b BlockedMigsSource) CleanUp() {
}
