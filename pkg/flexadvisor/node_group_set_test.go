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

package flexadvisor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	testprovider "sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	base_backoff "sigs.k8s.io/cluster-autoscaler/pkg/utils/backoff"
	tu "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func newTestUncreatedCandidate() *gke.GkeMig {
	return gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "p", Zone: "zone-a", Name: "nap-temporary-mig"}).SetExist(false).SetAutoprovisioned(true).Build()
}

func TestNodeGroupSet_UncreatedCandidate(t *testing.T) {
	ctx := context.TODO()

	for name, tc := range map[string]struct {
		locations          []string
		err                error
		nilProvider        bool
		wantZones          []string
		wantTargetZonesErr bool
	}{
		"planned location order is preserved, representative is reused for its own zone": {
			locations: []string{"zone-b", "zone-a", "zone-c"},
			wantZones: []string{"zone-b", "zone-a", "zone-c"},
		},
		"representative zone missing from planned locations is not evaluated": {
			locations: []string{"zone-b", "zone-c"},
			wantZones: []string{"zone-b", "zone-c"},
		},
		"single planned zone returns only the representative mig": {
			locations: []string{"zone-a"},
			wantZones: []string{"zone-a"},
		},
		"duplicate and empty planned locations are sanitized": {
			locations: []string{"", "zone-b", "zone-b"},
			wantZones: []string{"zone-b"},
		},
		"empty planned locations fall back to the representative zone": {
			locations: nil,
			wantZones: []string{"zone-a"},
		},
		"provider error falls back to representative mig in NodeGroups and returns error in TargetZones": {
			err:                errors.New("boom"),
			wantZones:          []string{"zone-a"},
			wantTargetZonesErr: true,
		},
		"nil provider evaluates only the representative zone": {
			nilProvider: true,
			wantZones:   []string{"zone-a"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			mig := newTestUncreatedCandidate()
			var provider gke.PlannedLocationsProvider
			gkeManagerMock := &gke.GkeManagerMock{}
			if !tc.nilProvider {
				gkeManagerMock.On("PlannedNodePoolLocations", mig).Return(tc.locations, tc.err).Once()
				provider = gkeManagerMock
			}

			sentinel := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "p", Zone: "zone-d", Name: "sentinel"}).Build()
			backing := []cloudprovider.NodeGroup{sentinel}
			emptySimilar := backing[:0]

			ngs := NewNodeGroupSet(ctx, mig, emptySimilar, provider)
			got := ngs.NodeGroups()

			gkeManagerMock.AssertExpectations(t)
			assert.Same(t, mig, ngs.Representative())
			assert.Same(t, sentinel, backing[0], "caller's similarNodeGroups backing array must not be modified")

			var gotZones []string
			ids := map[string]bool{}
			for _, ng := range got {
				m := ng.(*gke.GkeMig)
				gotZones = append(gotZones, m.GceRef().Zone)
				assert.False(t, ids[m.Id()], "duplicate Id %s", m.Id())
				ids[m.Id()] = true
				assert.Equal(t, mig.GceRef().Name, m.GceRef().Name)
				assert.Equal(t, mig.Spec(), m.Spec())
				// Ids must match the per-zone copies used as backoff keys.
				assert.Equal(t, mig.ShallowCopyInZone(m.GceRef().Zone).Id(), m.Id())
				if m.GceRef().Zone == mig.GceRef().Zone {
					assert.Same(t, mig, m, "the representative must be reused for its own zone")
				}
			}
			assert.Equal(t, tc.wantZones, gotZones)
			assert.Len(t, mig.NodePool().Migs(), 1, "planned migs must not be registered in the node pool")

			targetZones, err := ngs.TargetZones(nil, nil)
			if tc.wantTargetZonesErr {
				assert.Error(t, err)
				assert.Nil(t, targetZones)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantZones, targetZones, "TargetZones must agree with NodeGroups when nothing is backed off")
			}
		})
	}
}

// zoneBackoff reports a node group as backed off if its zone is in backedOffZones. It records the node
// infos it was queried with, keyed by node group id.
type zoneBackoff struct {
	backedOffZones map[string]bool
	seenNodeInfos  map[string]*framework.NodeInfo
}

func newZoneBackoff(zones ...string) *zoneBackoff {
	b := &zoneBackoff{backedOffZones: map[string]bool{}, seenNodeInfos: map[string]*framework.NodeInfo{}}
	for _, z := range zones {
		b.backedOffZones[z] = true
	}
	return b
}

func (b *zoneBackoff) Backoff(cloudprovider.NodeGroup, *framework.NodeInfo, cloudprovider.InstanceErrorInfo, time.Time) time.Time {
	return time.Time{}
}

func (b *zoneBackoff) BackoffStatus(ng cloudprovider.NodeGroup, nodeInfo *framework.NodeInfo, _ time.Time) base_backoff.Status {
	b.seenNodeInfos[ng.Id()] = nodeInfo
	gkeNg, ok := ng.(gke.NodeGroup)
	return base_backoff.Status{IsBackedOff: ok && b.backedOffZones[gkeNg.GceRef().Zone]}
}

func (b *zoneBackoff) RemoveBackoff(cloudprovider.NodeGroup, *framework.NodeInfo) {}

func (b *zoneBackoff) RemoveStaleBackoffData(time.Time) {}

func TestNodeGroupSet_TargetZonesExcludeBackedOffZones(t *testing.T) {
	ctx := context.TODO()

	for name, tc := range map[string]struct {
		backedOffZones []string
		wantZones      []string
	}{
		"no zone backed off":        {wantZones: []string{"zone-a", "zone-b", "zone-c"}},
		"non-representative zone":   {backedOffZones: []string{"zone-b"}, wantZones: []string{"zone-a", "zone-c"}},
		"representative zone":       {backedOffZones: []string{"zone-a"}, wantZones: []string{"zone-b", "zone-c"}},
		"all zones return no zones": {backedOffZones: []string{"zone-a", "zone-b", "zone-c"}, wantZones: nil},
	} {
		t.Run(name, func(t *testing.T) {
			mig := newTestUncreatedCandidate()
			gkeManagerMock := &gke.GkeManagerMock{}
			gkeManagerMock.On("PlannedNodePoolLocations", mig).Return([]string{"zone-a", "zone-b", "zone-c"}, nil).Once()

			node := tu.BuildTestNode("template", 1000, 1000)
			node.Labels = map[string]string{apiv1.LabelTopologyZone: "zone-a"}
			nodeInfos := map[string]*framework.NodeInfo{mig.Id(): framework.NewTestNodeInfo(node)}

			backoff := newZoneBackoff(tc.backedOffZones...)
			ngs := NewNodeGroupSet(ctx, mig, nil, gkeManagerMock)
			gotZones, err := ngs.TargetZones(backoff, nodeInfos)

			assert.NoError(t, err)
			assert.Equal(t, tc.wantZones, gotZones)
			// NodeGroups is not backoff-aware and must still return all planned zones.
			assert.Len(t, ngs.NodeGroups(), 3)

			// Backoff must be queried with the per-zone MIG and a node info relabeled to that zone.
			for _, zone := range []string{"zone-a", "zone-b", "zone-c"} {
				zonalId := mig.ShallowCopyInZone(zone).Id()
				if assert.Contains(t, backoff.seenNodeInfos, zonalId) {
					assert.Equal(t, zone, backoff.seenNodeInfos[zonalId].Node().Labels[apiv1.LabelTopologyZone])
				}
			}
		})
	}
}

func TestNodeGroupSet_TargetZonesWithoutNodeInfo(t *testing.T) {
	mig := newTestUncreatedCandidate()
	gkeManagerMock := &gke.GkeManagerMock{}
	gkeManagerMock.On("PlannedNodePoolLocations", mig).Return([]string{"zone-a", "zone-b"}, nil).Once()

	backoff := newZoneBackoff("zone-b")
	gotZones, err := NewNodeGroupSet(context.TODO(), mig, nil, gkeManagerMock).TargetZones(backoff, nil)

	assert.NoError(t, err)
	assert.Equal(t, []string{"zone-a"}, gotZones)
	assert.Nil(t, backoff.seenNodeInfos[mig.ShallowCopyInZone("zone-b").Id()], "nil node info must be passed through")
}

func TestNodeGroupSet_ExistingNodePool(t *testing.T) {
	ctx := context.TODO()
	existingA := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "p", Zone: "zone-a", Name: "mig-a"}).SetExist(true).Build()
	existingB := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "p", Zone: "zone-b", Name: "mig-b"}).SetExist(true).Build()
	sentinel := gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "p", Zone: "zone-c", Name: "sentinel"}).SetExist(true).Build()
	backing := []cloudprovider.NodeGroup{existingB, sentinel}
	similar := backing[:1] // spare capacity: a naive append would overwrite backing[1]

	gkeManagerMock := &gke.GkeManagerMock{}
	ngs := NewNodeGroupSet(ctx, existingA, similar, gkeManagerMock)

	gkeManagerMock.AssertNotCalled(t, "PlannedNodePoolLocations", mock.Anything)
	assert.Equal(t, []cloudprovider.NodeGroup{existingA, existingB}, ngs.NodeGroups())
	assert.Same(t, sentinel, backing[1], "caller's similarNodeGroups backing array must not be modified")

	// Backoff is not consulted for existing node pools (ComputeSimilarNodeGroups already excludes backed-off MIGs).
	targetZones, err := ngs.TargetZones(newZoneBackoff("zone-a", "zone-b"), nil)
	assert.NoError(t, err)
	assert.Equal(t, []string{"zone-a", "zone-b"}, targetZones)
}

func TestNodeGroupSet_TargetZonesErrors(t *testing.T) {
	_, err := NewNodeGroupSet(context.Background(), nil, nil, nil).TargetZones(nil, nil)
	assert.ErrorContains(t, err, "nil node group")

	notGke := testprovider.NewTestNodeGroup("not-gke", 0, 0, 0, false, true, "", nil, nil)
	_, err = NewNodeGroupSet(context.Background(), notGke, nil, nil).TargetZones(nil, nil)
	assert.ErrorContains(t, err, "not a GKE node group")
}

func TestNodeGroupSet_OnlyExpandsUncreatedCandidate(t *testing.T) {
	ctx := context.TODO()
	for name, tc := range map[string]struct {
		ng         func() cloudprovider.NodeGroup
		wantExpand bool
	}{
		"autoprovisioned, not existing, not upcoming": {
			ng: func() cloudprovider.NodeGroup {
				return gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "p", Zone: "zone-a", Name: "mig"}).SetExist(false).SetAutoprovisioned(true).Build()
			},
			wantExpand: true,
		},
		"not autoprovisioned": {
			ng: func() cloudprovider.NodeGroup {
				return gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "p", Zone: "zone-a", Name: "mig"}).SetExist(false).SetAutoprovisioned(false).Build()
			},
			wantExpand: false,
		},
		"existing": {
			ng: func() cloudprovider.NodeGroup {
				return gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "p", Zone: "zone-a", Name: "mig"}).SetExist(true).SetAutoprovisioned(true).Build()
			},
			wantExpand: false,
		},
		"upcoming": {
			ng: func() cloudprovider.NodeGroup {
				return gke.NewTestGkeMigBuilder().SetGceRef(gce.GceRef{Project: "p", Zone: "zone-a", Name: "mig"}).SetUpcoming().SetAutoprovisioned(true).Build()
			},
			wantExpand: false,
		},
		"not a GKE node group": {
			ng: func() cloudprovider.NodeGroup {
				return testprovider.NewTestNodeGroup("not-gke", 0, 0, 0, false, true, "", nil, nil)
			},
			wantExpand: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ng := tc.ng()
			gkeManagerMock := &gke.GkeManagerMock{}
			if tc.wantExpand {
				gkeManagerMock.On("PlannedNodePoolLocations", ng).Return([]string{"zone-a", "zone-b"}, nil).Once()
			}
			ngs := NewNodeGroupSet(ctx, ng, nil, gkeManagerMock)
			gkeManagerMock.AssertExpectations(t)
			if tc.wantExpand {
				assert.Len(t, ngs.NodeGroups(), 2)
			} else {
				assert.Equal(t, []cloudprovider.NodeGroup{ng}, ngs.NodeGroups())
			}
		})
	}
}

func TestSanitizeZones(t *testing.T) {
	testCases := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "empty",
			input:    []string{},
			expected: nil,
		},
		{
			name:     "no duplicates or empty strings",
			input:    []string{"us-central1-a", "us-central1-b", "us-central1-c"},
			expected: []string{"us-central1-a", "us-central1-b", "us-central1-c"},
		},
		{
			name:     "deduplicates zones while preserving order",
			input:    []string{"us-central1-a", "us-central1-a", "us-central1-b", "us-central1-a"},
			expected: []string{"us-central1-a", "us-central1-b"},
		},
		{
			name:     "filters out empty strings",
			input:    []string{"", "us-central1-a", "", "us-central1-b", ""},
			expected: []string{"us-central1-a", "us-central1-b"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeZones(tc.input)
			assert.Equal(t, tc.expected, got)
		})
	}
}
