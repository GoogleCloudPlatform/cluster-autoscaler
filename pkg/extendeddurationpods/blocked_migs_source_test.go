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

package extendeddurationpods

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
)

func TestShouldBlockScaling(t *testing.T) {
	for tn, tc := range map[string]struct {
		mig                    *gke.GkeMig
		clusterVersion         string
		wantShouldBlockScaleUp bool
	}{
		"gke mig is nil": {
			mig:                    nil,
			clusterVersion:         "1.24.0-gke.1000",
			wantShouldBlockScaleUp: false,
		},
		"Node Config is nil": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(nil).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build(),
			clusterVersion:         "1.24.0-gke.1000",
			wantShouldBlockScaleUp: false,
		},
		"Spec is nil": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: "1.24.0-gke.1000"}).SetSpec(nil).SetExist(true).Build(),
			clusterVersion:         "1.24.1-gke.1000",
			wantShouldBlockScaleUp: false,
		},
		"Mig in un-real": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: "1.24.0-gke.1000"}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(false).Build(),
			clusterVersion:         "1.24.1-gke.1000",
			wantShouldBlockScaleUp: false,
		},
		"No extended duration pods": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: "1.24.0-gke.1000"}).SetSpec(&gkeclient.NodePoolSpec{}).SetExist(true).Build(),
			clusterVersion:         "1.24.1-gke.1000",
			wantShouldBlockScaleUp: false,
		},
		"empty mig version": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: ""}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build(),
			clusterVersion:         "1.24.0-gke.1000",
			wantShouldBlockScaleUp: false,
		},
		"Bad mig version": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: "172-adf"}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build(),
			clusterVersion:         "1.24.0-gke.1000",
			wantShouldBlockScaleUp: false,
		},
		"empty cluster version": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: "1.24.0-gke.1000"}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build(),
			clusterVersion:         "",
			wantShouldBlockScaleUp: false,
		},
		"Bad cluster version": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: "1.24.0-gke.1000"}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build(),
			clusterVersion:         "123-abc",
			wantShouldBlockScaleUp: false,
		},
		"same cluster and mig version": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: "1.24.0-gke.1000"}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build(),
			clusterVersion:         "1.24.0-gke.1000",
			wantShouldBlockScaleUp: false,
		},
		"cluster is lower than mig version": { // although this should not happen in practice
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: "1.24.0-gke.1000"}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build(),
			clusterVersion:         "1.23.0-gke.1000",
			wantShouldBlockScaleUp: false,
		},
		"cluster is higher than mig version": {
			mig:                    gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: "1.24.0-gke.1000"}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build(),
			clusterVersion:         "1.24.1-gke.1000",
			wantShouldBlockScaleUp: true,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			gotShouldBlockScaleUp := shouldBlockScalingUp(tc.mig, tc.clusterVersion)
			if gotShouldBlockScaleUp != tc.wantShouldBlockScaleUp {
				t.Errorf("shouldBlockScaleUp: want %v, got %v", tc.wantShouldBlockScaleUp, gotShouldBlockScaleUp)
			}
		})
	}
}

type mockCloudProvider struct {
	migs           []*gke.GkeMig
	clusterVersion string
}

func (p *mockCloudProvider) GetGkeMigs() []*gke.GkeMig {
	return p.migs
}

func (p *mockCloudProvider) GetClusterVersion() string {
	return p.clusterVersion
}

func TestBlockedMigs(t *testing.T) {
	var (
		version1 = "1.24.0-gke.1000"
		version2 = "1.24.1-gke.1000"

		badConfiguredMig   = gke.NewTestGkeMigBuilder().SetGceRefName("badConfigured").Build()
		nonRealEdpMig      = gke.NewTestGkeMigBuilder().SetGceRefName("nonEdpMig").SetNodeConfig(&gke.NodeConfig{Version: version1}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).Build()
		nonEdpMig          = gke.NewTestGkeMigBuilder().SetGceRefName("nonEdpMig").SetNodeConfig(&gke.NodeConfig{Version: version1}).SetExist(true).Build()
		lowVersionEDPMig1  = gke.NewTestGkeMigBuilder().SetGceRefName("mig-1").SetNodeConfig(&gke.NodeConfig{Version: version1}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build()
		lowVersionEDPMig2  = gke.NewTestGkeMigBuilder().SetGceRefName("mig-2").SetNodeConfig(&gke.NodeConfig{Version: version1}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build()
		highVersionEDPMig1 = gke.NewTestGkeMigBuilder().SetGceRefName("mig-high-1").SetNodeConfig(&gke.NodeConfig{Version: version2}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build()
		highVersionEDPMig2 = gke.NewTestGkeMigBuilder().SetGceRefName("mig-high-2").SetNodeConfig(&gke.NodeConfig{Version: version2}).SetSpec(&gkeclient.NodePoolSpec{ExtendedDurationPods: "1"}).SetExist(true).Build()
	)
	for tn, tc := range map[string]struct {
		migs            []*gke.GkeMig
		clusterVersion  string
		wantBlockedMigs scaleblocking.BlockedMigs
	}{
		"no MIGs -> no blocked MIGs": {
			migs:            nil,
			clusterVersion:  version1,
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"bad configured": {
			migs:            []*gke.GkeMig{badConfiguredMig},
			clusterVersion:  version1,
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"MIGs are not edp": {
			migs:            []*gke.GkeMig{nonEdpMig},
			clusterVersion:  version1,
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"MIGs are not real": {
			migs:            []*gke.GkeMig{nonRealEdpMig},
			clusterVersion:  version1,
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"cluster version is not set": {
			migs:            []*gke.GkeMig{lowVersionEDPMig1},
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"all edp migs are same cluster version": {
			migs:            []*gke.GkeMig{lowVersionEDPMig1, lowVersionEDPMig2},
			clusterVersion:  version1,
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"all edp migs are lower than cluster version ": {
			migs:           []*gke.GkeMig{lowVersionEDPMig1, lowVersionEDPMig2},
			clusterVersion: version2,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					lowVersionEDPMig1.Id(): {BlockedMigEDPUpgrade: true},
					lowVersionEDPMig2.Id(): {BlockedMigEDPUpgrade: true},
				},
			},
		},
		"mixed migs but non-lower than cluster version ": {
			migs:            []*gke.GkeMig{lowVersionEDPMig1, lowVersionEDPMig2, nonEdpMig, badConfiguredMig},
			clusterVersion:  version1,
			wantBlockedMigs: scaleblocking.BlockedMigs{},
		},
		"all edp migs, some lower than cluster version ": {
			migs:           []*gke.GkeMig{lowVersionEDPMig1, highVersionEDPMig1, highVersionEDPMig2},
			clusterVersion: version2,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					lowVersionEDPMig1.Id(): {BlockedMigEDPUpgrade: true},
				},
			},
		},
		"some edp migs are lower than cluster version with other migs": {
			migs:           []*gke.GkeMig{lowVersionEDPMig1, lowVersionEDPMig2, highVersionEDPMig1, highVersionEDPMig1, nonEdpMig, badConfiguredMig},
			clusterVersion: version2,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					lowVersionEDPMig1.Id(): {BlockedMigEDPUpgrade: true},
					lowVersionEDPMig2.Id(): {BlockedMigEDPUpgrade: true},
				},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			provider := &mockCloudProvider{migs: tc.migs, clusterVersion: tc.clusterVersion}
			source := NewBlockedMigsSource(provider)
			gotBlockedMigs := source.BlockedMigs()
			if diff := cmp.Diff(tc.wantBlockedMigs, gotBlockedMigs, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("BlockedMigs diff (-want +got):\n%s", diff)
			}
		})
	}
}
