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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/placement"
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
		flexNonQueuedMig1 = gke.NewTestGkeMigBuilder().SetNodePoolName("fsnq-pool-3").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "fsnq-pool-3-mig",
		}).SetSpec(&gkeclient.NodePoolSpec{FlexStart: true}).Build()
		queuedMig = gke.NewTestGkeMigBuilder().SetNodePoolName("qp-pool-4").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "qp-pool-4-mig",
		}).SetQueuedProvisioning(true).Build()
		flexQueuedMig = gke.NewTestGkeMigBuilder().SetNodePoolName("fsqp-pool-5").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "fsqp-pool-5-mig",
		}).SetSpec(&gkeclient.NodePoolSpec{FlexStart: true}).SetQueuedProvisioning(true).Build()
		flexNonQueuedMig2 = gke.NewTestGkeMigBuilder().SetNodePoolName("fsnq-pool-6").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "fsnq-pool-6-mig",
		}).SetSpec(&gkeclient.NodePoolSpec{FlexStart: true}).Build()
		a4xFlexNonQueued = gke.NewTestGkeMigBuilder().SetNodePoolName("a4x-pool-1").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "a4x-pool-1-mig",
		}).SetSpec(&gkeclient.NodePoolSpec{MachineType: "a4x-highgpu-4g", FlexStart: true, PlacementGroup: placement.Spec{Policy: "a4x-policy"}}).Build()
		a4xFlexQueued = gke.NewTestGkeMigBuilder().SetNodePoolName("a4x-pool-2").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "a4x-pool-2-mig",
		}).SetQueuedProvisioning(true).SetSpec(&gkeclient.NodePoolSpec{MachineType: "a4x-highgpu-4g", FlexStart: true, QueuedProvisioning: true, PlacementGroup: placement.Spec{Policy: "a4x-policy"}}).Build()
		a4xNonFlexNonQueued = gke.NewTestGkeMigBuilder().SetNodePoolName("a4x-pool-3").SetGceRef(gce.GceRef{
			Project: project,
			Zone:    location,
			Name:    "a4x-pool-3-mig",
		}).SetSpec(&gkeclient.NodePoolSpec{MachineType: "a4x-highgpu-4g", FlexStart: false, PlacementGroup: placement.Spec{Policy: "a4x-policy"}}).Build()
	)

	availableMigs := []*gke.GkeMig{
		standardMig1,
		standardMig2,
		flexNonQueuedMig1,
		queuedMig,
		flexQueuedMig,
		flexNonQueuedMig2,
		a4xFlexNonQueued,
		a4xFlexQueued,
		a4xNonFlexNonQueued,
	}
	for desc, tc := range map[string]struct {
		enabledFeatures []string
		migs            []*gke.GkeMig
		wantBlockedMigs scaleblocking.BlockedMigs
	}{
		"expsEnabled_do_not_blockScaleUp": {
			enabledFeatures: []string{experiments.FlexStartNonQueuedBulkMigsFlag, experiments.ProvisioningRequestBulkMigsFlag},
			migs:            availableMigs,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{},
			},
		},
		"bulkFSNQEnabled_do_not_blockScaleUp": {
			enabledFeatures: []string{experiments.FlexStartNonQueuedBulkMigsFlag},
			migs:            availableMigs,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					a4xFlexQueued.Id(): {FlexStartQueuedBulkMigsExpDisabled: true},
				},
			},
		},
		"bulkFSQEnabled_do_not_blockScaleUp": {
			enabledFeatures: []string{experiments.ProvisioningRequestBulkMigsFlag},
			migs:            availableMigs,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					a4xFlexNonQueued.Id(): {FlexStartNonQueuedBulkMigsExpDisabled: true},
				},
			},
		},
		"expsDisabled_blockScaleUp_for_a4xFlexBulk_migs": {
			migs: availableMigs,
			wantBlockedMigs: scaleblocking.BlockedMigs{
				NoScaleUpMigs: map[string]scaleblocking.BlockedMigReasonSet{
					a4xFlexNonQueued.Id(): {FlexStartNonQueuedBulkMigsExpDisabled: true},
					a4xFlexQueued.Id():    {FlexStartQueuedBulkMigsExpDisabled: true},
				},
			},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			source := NewBlockedMigsSource(&mockCloudProvider{migs: tc.migs}, experiments.NewMockManager(tc.enabledFeatures...))
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
