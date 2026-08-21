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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/processors/scaleblocking"
)

func TestBlockedMigs(t *testing.T) {
	const (
		project  = "gke-test"
		location = "us-central1-c"
	)
	var (
		standardMig1 = gke.NewTestGkeMigBuilder().SetNodePoolName("std-pool-1").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "std-pool-1-mig",
		}).Build()
		standardMig2 = gke.NewTestGkeMigBuilder().SetNodePoolName("std-pool-2").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "std-pool-2-mig",
		}).Build()
		flexStartNonQueuedMig1 = gke.NewTestGkeMigBuilder().SetNodePoolName("fsnq-pool-3").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "fsnq-pool-3-mig",
		}).SetSpec(&gkeclient.NodePoolSpec{FlexStart: true}).Build()
		queuedProvisioningMig = gke.NewTestGkeMigBuilder().SetNodePoolName("qp-pool-4").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "qp-pool-4-mig",
		}).SetQueuedProvisioning(true).Build()
		flexStartQueuedProvisioningMig = gke.NewTestGkeMigBuilder().SetNodePoolName("fsqp-pool-5").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "fsqp-pool-5-mig",
		}).SetSpec(&gkeclient.NodePoolSpec{FlexStart: true}).SetQueuedProvisioning(true).Build()
		flexStartNonQueuedMig2 = gke.NewTestGkeMigBuilder().SetNodePoolName("fsnq-pool-6").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "fsnq-pool-6-mig",
		}).SetSpec(&gkeclient.NodePoolSpec{FlexStart: true}).Build()
	)

	for desc, tc := range map[string]struct {
		flexStartNonQueuedExpEnabled bool
		migs                         []*gke.GkeMig
		wantBlockedMigs              scaleblocking.BlockedMigs
	}{
		"expEnabled_do_not_blockScaleUp": {
			flexStartNonQueuedExpEnabled: true,
			migs: []*gke.GkeMig{
				standardMig1,
				standardMig2,
				flexStartNonQueuedMig1,
				queuedProvisioningMig,
				flexStartQueuedProvisioningMig,
				flexStartNonQueuedMig2,
			},
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{},
			},
		},
		"expDisabled_blockScaleUp_for_FlexStartNonQueued_migs": {
			flexStartNonQueuedExpEnabled: false,
			migs: []*gke.GkeMig{
				standardMig1,
				standardMig2,
				flexStartNonQueuedMig1,
				queuedProvisioningMig,
				flexStartQueuedProvisioningMig,
				flexStartNonQueuedMig2,
			},
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					flexStartNonQueuedMig1.Id(): {FlexStartNonQueuedExpDisabled: true},
					flexStartNonQueuedMig2.Id(): {FlexStartNonQueuedExpDisabled: true},
				},
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			enabledFeatures := []string{}
			if tc.flexStartNonQueuedExpEnabled {
				enabledFeatures = append(enabledFeatures, experiments.FlexStartNonQueuedEnabledFlag)
			}

			source := NewBlockedMigsSource(&mockCloudProvider{migs: tc.migs}, experiments.NewMockManager(enabledFeatures...))
			if diff := cmp.Diff(tc.wantBlockedMigs, source.BlockedMigs(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("BlockedMigs diff (-want +got):\n%s", diff)
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
