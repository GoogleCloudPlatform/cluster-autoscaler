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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
)

type mockWatcher struct {
	noScaleUp   sets.Set[string]
	noScaleDown sets.Set[string]
}

func (sw *mockWatcher) NoScaleUpNodePools() sets.Set[string] {
	return sw.noScaleUp
}

func (sw *mockWatcher) NoScaleDownNodePools() sets.Set[string] {
	return sw.noScaleDown
}

func (sw *mockWatcher) Run(_ context.Context) {
}

type mockCloudProvider struct {
	migs []*gke.GkeMig
}

func (p *mockCloudProvider) GetGkeMigs() []*gke.GkeMig {
	return p.migs
}

func TestBlockedMigsSource(t *testing.T) {
	var (
		poolA    = "poolA"
		poolB    = "poolB"
		migA1    = gke.NewTestGkeMigBuilder().SetGceRefName("migA1").SetNodePoolName(poolA).Build()
		migA2    = gke.NewTestGkeMigBuilder().SetGceRefName("migA1").SetNodePoolName(poolA).Build()
		migB1    = gke.NewTestGkeMigBuilder().SetGceRefName("migB1").SetNodePoolName(poolB).Build()
		migB2    = gke.NewTestGkeMigBuilder().SetGceRefName("migB2").SetNodePoolName(poolB).Build()
		testMigs = []*gke.GkeMig{migA1, migA2, migB1, migB2}
	)
	for tn, tc := range map[string]struct {
		noScaleUpNodePools   []string
		noScaleDownNodePools []string
		migs                 []*gke.GkeMig
		wantBlockedMigs      scaleblocking.BlockedMigs
	}{
		"no snowflakes": {
			migs: testMigs,
		},
		"noScaleUp only": {
			noScaleUpNodePools: []string{poolA},
			migs:               testMigs,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					migA1.Id(): {BlockedMigSnowflaked: true},
					migA2.Id(): {BlockedMigSnowflaked: true},
				},
			},
		},
		"noScaleDown only": {
			noScaleDownNodePools: []string{poolA},
			migs:                 testMigs,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{
					migA1.Id(): {BlockedMigSnowflaked: true},
					migA2.Id(): {BlockedMigSnowflaked: true},
				},
			},
		},
		"noScaleUp+noScaleDown": {
			noScaleDownNodePools: []string{poolA},
			noScaleUpNodePools:   []string{poolA},
			migs:                 testMigs,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{
					migA1.Id(): {BlockedMigSnowflaked: true},
					migA2.Id(): {BlockedMigSnowflaked: true},
				},
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					migA1.Id(): {BlockedMigSnowflaked: true},
					migA2.Id(): {BlockedMigSnowflaked: true},
				},
			},
		},
		"multiple node pools noScaleUp+noScaleDown": {
			noScaleDownNodePools: []string{poolA, poolB},
			noScaleUpNodePools:   []string{poolA, poolB},
			migs:                 testMigs,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{
					migA1.Id(): {BlockedMigSnowflaked: true},
					migA2.Id(): {BlockedMigSnowflaked: true},
					migB1.Id(): {BlockedMigSnowflaked: true},
					migB2.Id(): {BlockedMigSnowflaked: true},
				},
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					migA1.Id(): {BlockedMigSnowflaked: true},
					migA2.Id(): {BlockedMigSnowflaked: true},
					migB1.Id(): {BlockedMigSnowflaked: true},
					migB2.Id(): {BlockedMigSnowflaked: true},
				},
			},
		},
		"multiple node pools noScaleUp/noScaleDown": {
			noScaleDownNodePools: []string{poolA},
			noScaleUpNodePools:   []string{poolB},
			migs:                 testMigs,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleDownMigs: map[string]scaleblocking.BlockedMigReasonSet{
					migA1.Id(): {BlockedMigSnowflaked: true},
					migA2.Id(): {BlockedMigSnowflaked: true},
				},
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					migB1.Id(): {BlockedMigSnowflaked: true},
					migB2.Id(): {BlockedMigSnowflaked: true},
				},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			watcher := &mockWatcher{
				noScaleUp:   sets.New(tc.noScaleUpNodePools...),
				noScaleDown: sets.New(tc.noScaleDownNodePools...),
			}
			provider := &mockCloudProvider{migs: tc.migs}
			source := NewBlockedMigsSource(provider, watcher)
			gotBlockedMigs := source.BlockedMigs()
			if diff := cmp.Diff(tc.wantBlockedMigs, gotBlockedMigs, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("BlockedMigs diff (-want +got): %s", diff)
			}
		})
	}
}
