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

package scaleblocking

import (
	"reflect"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/autoscaler/cluster-autoscaler/processors/callbacks"

	"github.com/google/go-cmp/cmp"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
)

type mockBlockedMigsSource struct {
	noScaleUpMigs   []string
	noScaleDownMigs []string
	reason          BlockedMigReason
	extraReasons    BlockedMigReasonSet
}

func (s *mockBlockedMigsSource) BlockedMigs() BlockedMigs {
	return BlockedMigs{
		NoScaleUpMigs:   s.noScaleMigs(s.noScaleUpMigs),
		NoScaleDownMigs: s.noScaleMigs(s.noScaleDownMigs),
	}
}

func (s *mockBlockedMigsSource) noScaleMigs(migs []string) map[string]BlockedMigReasonSet {
	result := map[string]BlockedMigReasonSet{}
	for _, migId := range migs {
		reasonSet := BlockedMigReasonSet{}
		if s.reason != "" {
			reasonSet[s.reason] = true
		}
		for extraReason := range s.extraReasons {
			reasonSet[extraReason] = true
		}
		result[migId] = reasonSet
	}
	return result
}

func (s *mockBlockedMigsSource) CleanUp() {
}

type mockCloudProvider struct {
	migsForNodes map[string]*gke.GkeMig
}

func (p *mockCloudProvider) GkeMigForNode(node *apiv1.Node) (*gke.GkeMig, error) {
	return p.migsForNodes[node.Name], nil
}

func TestFilterNoScaleNodeGroups(t *testing.T) {
	var (
		mig1 = gke.NewTestGkeMigBuilder().SetGceRefName("mig1").SetNodePoolName("pool1").Build()
		mig2 = gke.NewTestGkeMigBuilder().SetGceRefName("mig2").SetNodePoolName("pool2").Build()
	)
	for tn, tc := range map[string]struct {
		noScaleUpMigs   []string
		noScaleDownMigs []string
		nodeGroups      []cloudprovider.NodeGroup
		wantScaleUp     []cloudprovider.NodeGroup
		wantScaleDown   []cloudprovider.NodeGroup
	}{
		"no blocked MIGs": {
			nodeGroups:    []cloudprovider.NodeGroup{mig1, mig2},
			wantScaleUp:   []cloudprovider.NodeGroup{mig1, mig2},
			wantScaleDown: []cloudprovider.NodeGroup{mig1, mig2},
		},
		"single noScaleUp MIG": {
			noScaleUpMigs: []string{mig2.Id()},
			nodeGroups:    []cloudprovider.NodeGroup{mig1, mig2},
			wantScaleDown: []cloudprovider.NodeGroup{mig1, mig2},
			wantScaleUp:   []cloudprovider.NodeGroup{mig1},
		},
		"single noScaleDown MIG": {
			noScaleDownMigs: []string{mig2.Id()},
			nodeGroups:      []cloudprovider.NodeGroup{mig1, mig2},
			wantScaleDown:   []cloudprovider.NodeGroup{mig1},
			wantScaleUp:     []cloudprovider.NodeGroup{mig1, mig2},
		},
		"single NoScaleUp+NoScaleDown MIG": {
			noScaleDownMigs: []string{mig2.Id()},
			noScaleUpMigs:   []string{mig2.Id()},
			nodeGroups:      []cloudprovider.NodeGroup{mig1, mig2},
			wantScaleUp:     []cloudprovider.NodeGroup{mig1},
			wantScaleDown:   []cloudprovider.NodeGroup{mig1},
		},
		"multiple NoScaleUp/NoScaleDown MIGs": {
			noScaleDownMigs: []string{mig1.Id()},
			noScaleUpMigs:   []string{mig2.Id()},
			nodeGroups:      []cloudprovider.NodeGroup{mig1, mig2},
			wantScaleUp:     []cloudprovider.NodeGroup{mig1},
			wantScaleDown:   []cloudprovider.NodeGroup{mig2},
		},
		"multiple NoScaleUp+NoScaleDown MIGs": {
			noScaleDownMigs: []string{mig1.Id(), mig2.Id()},
			noScaleUpMigs:   []string{mig1.Id(), mig2.Id()},
			nodeGroups:      []cloudprovider.NodeGroup{mig1, mig2},
			wantScaleUp:     nil,
			wantScaleDown:   nil,
		},
	} {
		t.Run(tn, func(t *testing.T) {
			ctx := &context.AutoscalingContext{ProcessorCallbacks: callbacks.NewTestProcessorCallbacks()}
			source := &mockBlockedMigsSource{noScaleUpMigs: tc.noScaleUpMigs, noScaleDownMigs: tc.noScaleDownMigs, reason: BlockedMigReason("test-reason")}
			processor := NewProcessor(nil, []BlockedMigsSource{source})
			compareAllUnexportedOpt := cmp.Exporter(func(r reflect.Type) bool { return true })
			gotScaleUp := processor.FilterNoScaleUpNodeGroups(ctx, tc.nodeGroups)
			if diff := cmp.Diff(tc.wantScaleUp, gotScaleUp, compareAllUnexportedOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("FilterNoScaleUpNodeGroups diff (-want +got): %s", diff)
			}
			gotScaleDown := processor.FilterNoScaleDownNodeGroups(ctx, tc.nodeGroups)
			if diff := cmp.Diff(tc.wantScaleDown, gotScaleDown, compareAllUnexportedOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("FilterNoScaleDownNodeGroups diff (-want +got): %s", diff)
			}
		})
	}
}

func TestFilterNoScaleDownNodes(t *testing.T) {
	var (
		mig1     = gke.NewTestGkeMigBuilder().SetGceRefName("mig1").SetNodePoolName("pool1").Build()
		mig2     = gke.NewTestGkeMigBuilder().SetGceRefName("mig2").SetNodePoolName("pool2").Build()
		node1    = apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
		node2a   = apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2a"}}
		node2b   = apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2b"}}
		node3    = apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node3"}}
		provider = &mockCloudProvider{
			migsForNodes: map[string]*gke.GkeMig{
				"node1":  mig1,
				"node2a": mig2,
				"node2b": mig2,
			},
		}
	)
	for tn, tc := range map[string]struct {
		noScaleDownMigs         []string
		nodes                   []*apiv1.Node
		wantScaleDownCandidates []*apiv1.Node
	}{
		"no blocked MIGs": {
			nodes:                   []*apiv1.Node{&node1, &node2a, &node2b},
			wantScaleDownCandidates: []*apiv1.Node{&node1, &node2a, &node2b},
		},
		"single blocked MIG, no nodes": {
			noScaleDownMigs:         []string{"imaginary-pool"},
			nodes:                   []*apiv1.Node{&node1, &node2a, &node2b},
			wantScaleDownCandidates: []*apiv1.Node{&node1, &node2a, &node2b},
		},
		"single blocked MIG, single node": {
			noScaleDownMigs:         []string{mig1.Id()},
			nodes:                   []*apiv1.Node{&node1, &node2a, &node2b},
			wantScaleDownCandidates: []*apiv1.Node{&node2a, &node2b},
		},
		"single blocked MIG, multiple nodes": {
			noScaleDownMigs:         []string{mig2.Id()},
			nodes:                   []*apiv1.Node{&node1, &node2a, &node2b},
			wantScaleDownCandidates: []*apiv1.Node{&node1},
		},
		"multiple blocked MIGs": {
			noScaleDownMigs:         []string{mig1.Id(), mig2.Id()},
			nodes:                   []*apiv1.Node{&node1, &node2a, &node2b},
			wantScaleDownCandidates: nil,
		},
		"no blocked MIGs, non-autoscaled node present (shouldn't panic)": {
			nodes:                   []*apiv1.Node{&node1, &node2a, &node2b, &node3},
			wantScaleDownCandidates: []*apiv1.Node{&node1, &node2a, &node2b, &node3},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			ctx := &context.AutoscalingContext{ProcessorCallbacks: callbacks.NewTestProcessorCallbacks()}
			source := &mockBlockedMigsSource{noScaleDownMigs: tc.noScaleDownMigs, reason: BlockedMigReason("test-reason")}
			processor := NewProcessor(provider, []BlockedMigsSource{source})
			gotScaleDownCandidates, err := processor.GetScaleDownCandidates(ctx, tc.nodes)
			if err != nil {
				t.Errorf("GetScaleDownCandidates unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantScaleDownCandidates, gotScaleDownCandidates); diff != "" {
				t.Errorf("GetScaleDownCandidates diff (-want +got): %s", diff)
			}
			gotPodDestinationCandidates, err := processor.GetPodDestinationCandidates(ctx, tc.nodes)
			if err != nil {
				t.Errorf("GetPodDestinationCandidates unexpected error: %v", err)
			}
			// We don't want the processor to change pod destination candidates, only scale-down candidates.
			if diff := cmp.Diff(tc.nodes, gotPodDestinationCandidates); diff != "" {
				t.Errorf("GetPodDestinationCandidates diff (-want +got): %s", diff)
			}
		})
	}
}

func TestGetBlockedMigs(t *testing.T) {
	for tn, tc := range map[string]struct {
		sources []BlockedMigsSource
		want    BlockedMigs
	}{
		"no sources -> no blocked MIGs": {
			sources: nil,
			want:    BlockedMigs{},
		},
		"single source": {
			sources: []BlockedMigsSource{
				&mockBlockedMigsSource{
					noScaleUpMigs:   []string{"mig1", "mig2"},
					noScaleDownMigs: []string{"mig3", "mig4"},
					reason:          BlockedMigReason("test-reason"),
				},
			},
			want: BlockedMigs{
				NoScaleUpMigs: map[string]BlockedMigReasonSet{
					"mig1": {BlockedMigReason("test-reason"): true},
					"mig2": {BlockedMigReason("test-reason"): true},
				},
				NoScaleDownMigs: map[string]BlockedMigReasonSet{
					"mig3": {BlockedMigReason("test-reason"): true},
					"mig4": {BlockedMigReason("test-reason"): true},
				},
			},
		},
		"multiple sources, different MIGs": {
			sources: []BlockedMigsSource{
				&mockBlockedMigsSource{
					noScaleUpMigs:   []string{"mig1"},
					noScaleDownMigs: []string{"mig2"},
					reason:          BlockedMigReason("test-reason-1"),
				},
				&mockBlockedMigsSource{
					noScaleUpMigs:   []string{"mig3"},
					noScaleDownMigs: []string{"mig4"},
					reason:          BlockedMigReason("test-reason-2"),
				},
			},
			want: BlockedMigs{
				NoScaleUpMigs: map[string]BlockedMigReasonSet{
					"mig1": {BlockedMigReason("test-reason-1"): true},
					"mig3": {BlockedMigReason("test-reason-2"): true},
				},
				NoScaleDownMigs: map[string]BlockedMigReasonSet{
					"mig2": {BlockedMigReason("test-reason-1"): true},
					"mig4": {BlockedMigReason("test-reason-2"): true},
				},
			},
		},
		"multiple sources, same MIG, different up/down": {
			sources: []BlockedMigsSource{
				&mockBlockedMigsSource{
					noScaleUpMigs: []string{"mig1"},
					reason:        BlockedMigReason("test-reason-1"),
				},
				&mockBlockedMigsSource{
					noScaleDownMigs: []string{"mig1"},
					reason:          BlockedMigReason("test-reason-2"),
				},
			},
			want: BlockedMigs{
				NoScaleUpMigs: map[string]BlockedMigReasonSet{
					"mig1": {BlockedMigReason("test-reason-1"): true},
				},
				NoScaleDownMigs: map[string]BlockedMigReasonSet{
					"mig1": {BlockedMigReason("test-reason-2"): true},
				},
			},
		},
		"multiple sources, some MIGs overlapping": {
			sources: []BlockedMigsSource{
				&mockBlockedMigsSource{
					noScaleUpMigs:   []string{"mig1", "mig2"},
					noScaleDownMigs: []string{"mig3", "mig4"},
					reason:          BlockedMigReason("test-reason-1"),
				},
				&mockBlockedMigsSource{
					noScaleUpMigs:   []string{"mig5", "mig2"},
					noScaleDownMigs: []string{"mig6", "mig3"},
					reason:          BlockedMigReason("test-reason-2"),
				},
			},
			want: BlockedMigs{
				NoScaleUpMigs: map[string]BlockedMigReasonSet{
					"mig1": {BlockedMigReason("test-reason-1"): true},
					"mig2": {BlockedMigReason("test-reason-1"): true, BlockedMigReason("test-reason-2"): true},
					"mig5": {BlockedMigReason("test-reason-2"): true},
				},
				NoScaleDownMigs: map[string]BlockedMigReasonSet{
					"mig3": {BlockedMigReason("test-reason-1"): true, BlockedMigReason("test-reason-2"): true},
					"mig4": {BlockedMigReason("test-reason-1"): true},
					"mig6": {BlockedMigReason("test-reason-2"): true},
				},
			},
		},
		"single source with multiple reasons per MIG": {
			sources: []BlockedMigsSource{
				&mockBlockedMigsSource{
					noScaleUpMigs:   []string{"mig1", "mig2"},
					noScaleDownMigs: []string{"mig3", "mig4"},
					reason:          BlockedMigReason("test-reason"),
					extraReasons:    map[BlockedMigReason]bool{"additional-reason": true},
				},
			},
			want: BlockedMigs{
				NoScaleUpMigs: map[string]BlockedMigReasonSet{
					"mig1": {BlockedMigReason("test-reason"): true, BlockedMigReason("additional-reason"): true},
					"mig2": {BlockedMigReason("test-reason"): true, BlockedMigReason("additional-reason"): true},
				},
				NoScaleDownMigs: map[string]BlockedMigReasonSet{
					"mig3": {BlockedMigReason("test-reason"): true, BlockedMigReason("additional-reason"): true},
					"mig4": {BlockedMigReason("test-reason"): true, BlockedMigReason("additional-reason"): true},
				},
			},
		},
		"multiple sources with multiple reasons per MIG": {
			sources: []BlockedMigsSource{
				&mockBlockedMigsSource{
					noScaleUpMigs:   []string{"mig1"},
					noScaleDownMigs: []string{"mig2"},
					reason:          BlockedMigReason("test-reason"),
					extraReasons:    map[BlockedMigReason]bool{"additional-reason": true},
				},
				&mockBlockedMigsSource{
					noScaleUpMigs:   []string{"mig1"},
					noScaleDownMigs: []string{"mig3"},
					reason:          BlockedMigReason("test-reason-2"),
					extraReasons:    map[BlockedMigReason]bool{"additional-reason": true},
				},
			},
			want: BlockedMigs{
				NoScaleUpMigs: map[string]BlockedMigReasonSet{
					"mig1": {BlockedMigReason("test-reason"): true, BlockedMigReason("test-reason-2"): true, BlockedMigReason("additional-reason"): true},
				},
				NoScaleDownMigs: map[string]BlockedMigReasonSet{
					"mig2": {BlockedMigReason("test-reason"): true, BlockedMigReason("additional-reason"): true},
					"mig3": {BlockedMigReason("test-reason-2"): true, BlockedMigReason("additional-reason"): true},
				},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			processor := NewProcessor(nil, tc.sources)
			got := processor.getBlockedMigs(callbacks.NewTestProcessorCallbacks())
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("getBlockedMigs() diff (-want +got): %s", diff)
			}
		})
	}
}

func TestGetBlockedMigsConsistency(t *testing.T) {
	var (
		testReason          = BlockedMigReason("test-reason")
		testReasonSingleton = map[BlockedMigReason]bool{testReason: true}
	)
	// loopAssertion represents an assertion for this test during a single loop - even if blocked MIGs in
	// the underlying sources change, getBlockedMigs should return and report a consistent snapshot during
	// a single loop.
	type loopAssertion struct {
		noScaleUpMigChanges [][]string
		wantBlockedMigs     BlockedMigs
	}
	type testCase []loopAssertion
	for tn, loopAssertions := range map[string]testCase{
		"no blocked MIGs, no changes": {
			{ // LOOP 1.
				noScaleUpMigChanges: [][]string{
					nil, // nil means no change
					nil, // nil means no change
				},
				wantBlockedMigs: BlockedMigs{},
			},
		},
		"source changes don't change the result during 1 loop": {
			{ // LOOP 1.
				noScaleUpMigChanges: [][]string{
					{"mig1", "mig2"},
					{"mig3", "mig4"},
					{"mig5", "mig6"},
				},
				wantBlockedMigs: BlockedMigs{
					NoScaleUpMigs: map[string]BlockedMigReasonSet{
						"mig1": testReasonSingleton,
						"mig2": testReasonSingleton,
					},
				},
			},
		},
		"source changes are reflected between loops": {
			{ // LOOP 1.
				noScaleUpMigChanges: [][]string{
					{"mig1", "mig2"},
					{"mig3", "mig4"},
				},
				wantBlockedMigs: BlockedMigs{
					NoScaleUpMigs: map[string]BlockedMigReasonSet{
						"mig1": testReasonSingleton,
						"mig2": testReasonSingleton,
					},
				},
			},
			{ // LOOP 2.
				noScaleUpMigChanges: [][]string{
					{"mig5", "mig6"},
					{"mig7", "mig8"},
				},
				wantBlockedMigs: BlockedMigs{
					NoScaleUpMigs: map[string]BlockedMigReasonSet{
						"mig5": testReasonSingleton,
						"mig6": testReasonSingleton,
					},
				},
			},
		},
		"changes from 1 loop are picked up in a subsequent loop": {
			{ // LOOP 1.
				noScaleUpMigChanges: [][]string{
					{"mig1", "mig2"},
					{"mig3", "mig4"},
				},
				wantBlockedMigs: BlockedMigs{
					NoScaleUpMigs: map[string]BlockedMigReasonSet{
						"mig1": testReasonSingleton,
						"mig2": testReasonSingleton,
					},
				},
			},
			{ // LOOP 2.
				noScaleUpMigChanges: [][]string{
					nil, // nil means no change
					nil, // nil means no change
				},
				wantBlockedMigs: BlockedMigs{
					NoScaleUpMigs: map[string]BlockedMigReasonSet{
						"mig3": testReasonSingleton,
						"mig4": testReasonSingleton,
					},
				},
			},
		},
		"blocked MIGs are cleared correctly between loops": {
			{ // LOOP 1.
				noScaleUpMigChanges: [][]string{
					{"mig1", "mig2"},
					{"mig3", "mig4"},
				},
				wantBlockedMigs: BlockedMigs{
					NoScaleUpMigs: map[string]BlockedMigReasonSet{
						"mig1": testReasonSingleton,
						"mig2": testReasonSingleton,
					},
				},
			},
			{ // LOOP 2.
				noScaleUpMigChanges: [][]string{
					{},
					nil, // nil means no change
				},
				wantBlockedMigs: BlockedMigs{},
			},
		},
		"blocked MIGs are cleared correctly in a subsequent loop": {
			{ // LOOP 1.
				noScaleUpMigChanges: [][]string{
					{"mig1", "mig2"},
					{"mig3", "mig4"},
					{},
				},
				wantBlockedMigs: BlockedMigs{
					NoScaleUpMigs: map[string]BlockedMigReasonSet{
						"mig1": testReasonSingleton,
						"mig2": testReasonSingleton,
					},
				},
			},
			{ // LOOP 2.
				noScaleUpMigChanges: [][]string{
					nil, // nil means no change
					nil, // nil means no change
				},
				wantBlockedMigs: BlockedMigs{},
			},
		},
	} {
		t.Run(tn, func(t *testing.T) {
			cb := callbacks.NewTestProcessorCallbacks()
			source := &mockBlockedMigsSource{noScaleUpMigs: []string{}, reason: BlockedMigReason("test-reason")}
			processor := NewProcessor(nil, []BlockedMigsSource{source})
			for loopIndex, loopAssertion := range loopAssertions {
				for changeIndex, noScaleUpMigChange := range loopAssertion.noScaleUpMigChanges {
					if noScaleUpMigChange != nil {
						source.noScaleUpMigs = noScaleUpMigChange
					}
					gotBlockedMigs := processor.getBlockedMigs(cb)
					if diff := cmp.Diff(loopAssertion.wantBlockedMigs, gotBlockedMigs, cmpopts.EquateEmpty()); diff != "" {
						t.Errorf("getBlockedMigs (loop %d, change %d) diff (-want +got): %s", loopIndex+1, changeIndex+1, diff)
					}
					reportedBlockedMigs, found := cb.GetExtraValue(BlockedMigsContextKey)
					if !found {
						t.Errorf("getBlockedMigs (loop %d, change %d) called, but nothing was reported in ProcessorCallbacks", loopIndex+1, changeIndex+1)
					}
					if diff := cmp.Diff(loopAssertion.wantBlockedMigs, reportedBlockedMigs.(BlockedMigs), cmpopts.EquateEmpty()); diff != "" {
						t.Errorf("ProcessorCallbacks.BlockedMigsContextKey (loop %d, change %d) diff (-want +got): %s", loopIndex+1, changeIndex+1, diff)
					}
				}
				cb.Reset()
			}
		})
	}
}
