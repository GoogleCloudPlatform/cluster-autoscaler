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

package reservations

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

const (
	testBlockPath   = "projects/test-project/reservations/test-reservation/reservationBlocks/test-block"
	testSubBlock25  = testBlockPath + "/reservationSubBlocks/test-block-sub-block-0025"
	testSubBlock26  = testBlockPath + "/reservationSubBlocks/test-block-sub-block-0026"
	testClusterProj = "my-cluster-project"
)

// nodeGroupWithoutSpec does not implement gke.NodeGroup, standing in for a non-GKE node group.
type nodeGroupWithoutSpec struct {
	cloudprovider.NodeGroup
}

func TestTargetedSubBlock(t *testing.T) {
	testCases := []struct {
		name           string
		clusterProject string
		nodeGroup      cloudprovider.NodeGroup
		want           string
	}{
		{
			name:      "specific affinity scoped to a sub-block",
			nodeGroup: NewTestSubBlockMig(SpecificSubBlockSpec(testSubBlock25), ""),
			want:      testSubBlock25,
		},
		{
			name:           "same-project long-form sub-block path is canonicalized to short form",
			clusterProject: testClusterProj,
			nodeGroup:      NewTestSubBlockMig(SpecificSubBlockSpec("projects/"+testClusterProj+"/reservations/"+testSubBlock25), ""),
			want:           testSubBlock25,
		},
		{
			name:           "cross-project long-form sub-block path retains its project prefix",
			clusterProject: testClusterProj,
			nodeGroup:      NewTestSubBlockMig(SpecificSubBlockSpec("projects/other-project/reservations/"+testSubBlock25), ""),
			want:           "projects/other-project/reservations/" + testSubBlock25,
		},
		{
			name:      "specific affinity scoped only to a block",
			nodeGroup: NewTestSubBlockMig(SpecificSubBlockSpec(testBlockPath), ""),
			want:      "",
		},
		{
			name:      "specific affinity scoped only to a reservation",
			nodeGroup: NewTestSubBlockMig(SpecificSubBlockSpec("test-reservation"), ""),
			want:      "",
		},
		{
			name:      "any affinity is not pinned to a sub-block",
			nodeGroup: NewTestSubBlockMig(ReservationAffinitySpec(gkeclient.ReservationAffinityAny, testSubBlock25), ""),
			want:      "",
		},
		{
			name:      "none affinity is not pinned to a sub-block",
			nodeGroup: NewTestSubBlockMig(ReservationAffinitySpec(gkeclient.ReservationAffinityNone), ""),
			want:      "",
		},
		{
			name:      "ambiguous multi-value affinity is ignored by TargetedSubBlock",
			nodeGroup: NewTestSubBlockMig(SpecificSubBlockSpec(testSubBlock25, testSubBlock26), ""),
			want:      "",
		},
		{
			name:      "no reservation affinity",
			nodeGroup: NewTestSubBlockMig(&gkeclient.NodePoolSpec{}, ""),
			want:      "",
		},
		{
			name:      "nil spec",
			nodeGroup: NewTestSubBlockMig(nil, ""),
			want:      "",
		},
		{
			name:      "node group without a spec",
			nodeGroup: &nodeGroupWithoutSpec{},
			want:      "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := TargetedSubBlock(tc.nodeGroup, tc.clusterProject); got != tc.want {
				t.Errorf("TargetedSubBlock() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRequiresWholeSubBlock(t *testing.T) {
	testCases := []struct {
		name      string
		nodeGroup cloudprovider.NodeGroup
		want      bool
	}{
		{
			name:      "PROVISION_ONLY node group requires the whole sub-block",
			nodeGroup: NewTestSubBlockMig(&gkeclient.NodePoolSpec{}, gceclient.AcceleratorTopologyModeProvisionOnly),
			want:      true,
		},
		{
			name:      "AUTO_CONNECT static slice does not require the whole sub-block",
			nodeGroup: NewTestSubBlockMig(&gkeclient.NodePoolSpec{}, "AUTO_CONNECT"),
			want:      false,
		},
		{
			name:      "node group without workload policy does not require the whole sub-block",
			nodeGroup: NewTestSubBlockMig(&gkeclient.NodePoolSpec{}, ""),
			want:      false,
		},
		{
			name:      "non-GKE node group",
			nodeGroup: &nodeGroupWithoutSpec{},
			want:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RequiresWholeSubBlock(tc.nodeGroup); got != tc.want {
				t.Errorf("RequiresWholeSubBlock() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClaimedSubBlocks(t *testing.T) {
	testSubBlock27 := testBlockPath + "/reservationSubBlocks/test-block-sub-block-0027"
	nodeGroups := []cloudprovider.NodeGroup{
		// Claims are kind-agnostic: a static slice occupying one or multiple sub-blocks counts too.
		NewTestSubBlockMig(SpecificSubBlockSpec(testSubBlock25), ""),
		NewTestSubBlockMig(SpecificSubBlockSpec("projects/"+testClusterProj+"/reservations/"+testSubBlock26), ""),
		NewTestSubBlockMig(SpecificSubBlockSpec(testSubBlock25, testSubBlock27), ""), // multi-sub-block static slice
		NewTestSubBlockMig(SpecificSubBlockSpec(testBlockPath), ""),                  // block only
		NewTestSubBlockMig(nil, ""),
		&nodeGroupWithoutSpec{},
	}
	want := sets.New(testSubBlock25, testSubBlock26, testSubBlock27)
	if got := ClaimedSubBlocks(nodeGroups, testClusterProj); !got.Equal(want) {
		t.Errorf("ClaimedSubBlocks() = %v, want %v", sets.List(got), sets.List(want))
	}
}
