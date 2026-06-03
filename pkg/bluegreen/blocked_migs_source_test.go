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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
)

func TestShouldBlockScaling(t *testing.T) {
	for tn, tc := range map[string]struct {
		migBgi                   *gke.MigBlueGreenInfo
		wantShouldBlockScaleUp   bool
		wantShouldBlockScaleDown bool
	}{
		"no ongoing B/G update (nil *MigBlueGreenInfo) -> regular mode": {
			migBgi:                   nil,
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: false,
		},
		"Regular B/G UPDATE_STARTED -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseUpdateStarted},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G CREATING_GREEN_POOL -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseCreatingGreenPool},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G CORDONING_BLUE_POOL -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseCordoningBluePool},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G DRAINING_BLUE_POOL -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseDrainingBluePool},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G NODE_POOL_SOAKING -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseNodePoolSoaking},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G DELETING_BLUE_POOL -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseDeletingBluePool},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G ROLLBACK_STARTED -> scale-up-only mode for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseRollbackStarted},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G CREATING_GREEN_POOL -> scale-up-only mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseCreatingGreenPool},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G CORDONING_BLUE_POOL -> scale-up-only mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseCordoningBluePool},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G DRAINING_BLUE_POOL -> scale-up-only mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseDrainingBluePool},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: true,
		},
		"Regular B/G NODE_POOL_SOAKING -> regular mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseNodePoolSoaking},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: false,
		},
		"Regular B/G DELETING_BLUE_POOL -> regular mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseDeletingBluePool},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: false,
		},
		"Regular B/G ROLLBACK_STARTED -> scaling completely blocked for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseRollbackStarted},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Autoscaled B/G UPDATE_STARTED -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseUpdateStarted, IsAutoScaled: true},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Autoscaled B/G CREATING_GREEN_POOL -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseCreatingGreenPool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Autoscaled B/G CORDONING_BLUE_POOL -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseCordoningBluePool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Autoscaled B/G WAITING_TO_DRAIN_BLUE_POOL -> scale-down-only mode for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseWaitingToDrainBluePool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: false,
		},
		"Autoscaled B/G DRAINING_BLUE_POOL -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseDrainingBluePool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Autoscaled B/G DELETING_BLUE_POOL -> scaling completely blocked for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseDeletingBluePool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
		"Autoscaled B/G ROLLBACK_STARTED -> regular mode for Blue MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseRollbackStarted, IsAutoScaled: true},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: false,
		},
		"Autoscaled B/G CREATING_GREEN_POOL -> regular mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseCreatingGreenPool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: false,
		},
		"Autoscaled B/G CORDONING_BLUE_POOL -> regular mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseCordoningBluePool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: false,
		},
		"Autoscaled B/G WAITING_TO_DRAIN_BLUE_POOL -> regular mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseWaitingToDrainBluePool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: false,
		},
		"Autoscaled B/G DRAINING_BLUE_POOL -> regular mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseDrainingBluePool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: false,
		},
		"Autoscaled B/G DELETING_BLUE_POOL -> regular mode for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseDeletingBluePool, IsAutoScaled: true},
			wantShouldBlockScaleUp:   false,
			wantShouldBlockScaleDown: false,
		},
		"Autoscaled B/G ROLLBACK_STARTED -> scaling completely blocked for Green MIG": {
			migBgi:                   &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseRollbackStarted},
			wantShouldBlockScaleUp:   true,
			wantShouldBlockScaleDown: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			gotShouldBlockScaleUp, gotShouldBlockScaleDown := ShouldBlockScaling(tc.migBgi)
			if gotShouldBlockScaleUp != tc.wantShouldBlockScaleUp {
				t.Errorf("shouldBlockScaleUp: want %v, got %v", tc.wantShouldBlockScaleUp, gotShouldBlockScaleUp)
			}
			if gotShouldBlockScaleDown != tc.wantShouldBlockScaleDown {
				t.Errorf("shouldBlockScaleDown: want %v, got %v", tc.wantShouldBlockScaleUp, gotShouldBlockScaleUp)
			}
		})
	}
}

type mockCloudProvider struct {
	migs []*gke.GkeMig
}

func (p *mockCloudProvider) GetGkeMigs() []*gke.GkeMig {
	return p.migs
}

func TestBlockedMigs(t *testing.T) {
	var (
		scalingBlockedBgi  = &gke.MigBlueGreenInfo{Color: gke.BlueMig, Phase: gkeclient.PhaseCreatingGreenPool}
		scaleUpOnlyModeBgi = &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseCreatingGreenPool}
		regularModeBgi     = &gke.MigBlueGreenInfo{Color: gke.GreenMig, Phase: gkeclient.PhaseDeletingBluePool}
		noUpdateMig        = gke.NewTestGkeMigBuilder().SetGceRefName("noUpdateMig").Build()
		scalingBlockedMig  = gke.NewTestGkeMigBuilder().SetGceRefName("scalingBlockedMig").SetBlueGreenInfo(scalingBlockedBgi).Build()
		scalingBlockedMig2 = gke.NewTestGkeMigBuilder().SetGceRefName("scalingBlockedMig2").SetBlueGreenInfo(scalingBlockedBgi).Build()
		scaleUpOnlyModeMig = gke.NewTestGkeMigBuilder().SetGceRefName("scaleUpOnlyModeMig").SetBlueGreenInfo(scaleUpOnlyModeBgi).Build()
		regularModeMig     = gke.NewTestGkeMigBuilder().SetGceRefName("regularModeMig").SetBlueGreenInfo(regularModeBgi).Build()
	)
	for tn, tc := range map[string]struct {
		migs            []*gke.GkeMig
		wantBlockedMigs scaleblocking.BlockedMigs
	}{
		"no MIGs -> no blocked MIGs": {
			migs:            nil,
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"non-update MIGs are not blocked": {
			migs:            []*gke.GkeMig{noUpdateMig},
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"regular mode MIGs are not blocked": {
			migs:            []*gke.GkeMig{regularModeMig},
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"scale-up-only mode MIGs have scale-down blocked": {
			migs: []*gke.GkeMig{scaleUpOnlyModeMig},
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{
					scaleUpOnlyModeMig.Id(): {BlockedMigBlueGreen: true},
				},
			},
		},
		"MIGs with scaling completely blocked are correctly blocked": {
			migs: []*gke.GkeMig{scalingBlockedMig},
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{
					scalingBlockedMig.Id(): {BlockedMigBlueGreen: true},
				},
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					scalingBlockedMig.Id(): {BlockedMigBlueGreen: true},
				},
			},
		},
		"multiple MIGs are correctly handled together": {
			migs: []*gke.GkeMig{noUpdateMig, regularModeMig, scaleUpOnlyModeMig, scalingBlockedMig, scalingBlockedMig2},
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{
					scaleUpOnlyModeMig.Id(): {BlockedMigBlueGreen: true},
					scalingBlockedMig.Id():  {BlockedMigBlueGreen: true},
					scalingBlockedMig2.Id(): {BlockedMigBlueGreen: true},
				},
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					scalingBlockedMig.Id():  {BlockedMigBlueGreen: true},
					scalingBlockedMig2.Id(): {BlockedMigBlueGreen: true},
				},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := &mockCloudProvider{migs: tc.migs}
			source := NewBlockedMigsSource(provider)
			gotBlockedMigs := source.BlockedMigs()
			if diff := cmp.Diff(tc.wantBlockedMigs, gotBlockedMigs, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("BlockedMigs diff (-want +got):\n%s", diff)
			}
		})
	}
}
