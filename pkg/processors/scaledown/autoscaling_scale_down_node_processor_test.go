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

package scaledown

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

// fakeScaleDownNodeProcessor rejects nodes whose names match any of the given rejects.
type fakeScaleDownNodeProcessor struct {
	rejects map[string]bool
}

func newFakeProcessor(rejects ...string) *fakeScaleDownNodeProcessor {
	m := make(map[string]bool, len(rejects))
	for _, r := range rejects {
		m[r] = true
	}
	return &fakeScaleDownNodeProcessor{rejects: m}
}

func (f *fakeScaleDownNodeProcessor) GetScaleDownCandidates(_ context.Context, _ *ca_context.AutoscalingContext, allNodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	var candidates []*apiv1.Node
	for _, node := range allNodes {
		if !f.rejects[node.Name] {
			candidates = append(candidates, node)
		}
	}
	return candidates, nil
}

func (f *fakeScaleDownNodeProcessor) GetPodDestinationCandidates(_ *ca_context.AutoscalingContext, allNodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return allNodes, nil
}

func (f *fakeScaleDownNodeProcessor) CleanUp() {}

type fakeDefragAwareScaleDownNodeProcessor struct {
	fakeScaleDownNodeProcessor
	defragVariant nodes.ScaleDownNodeProcessor
}

func (f *fakeDefragAwareScaleDownNodeProcessor) DefragScaleDownNodeProcessor() nodes.ScaleDownNodeProcessor {
	return f.defragVariant
}

// droppingProcessor removes every node whose name is in drop.
type droppingProcessor struct {
	drop map[string]bool
	err  errors.AutoscalerError
}

func (p *droppingProcessor) GetPodDestinationCandidates(_ *ca_context.AutoscalingContext, nodes []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	return nodes, nil
}

func (p *droppingProcessor) GetScaleDownCandidates(_ context.Context, _ *ca_context.AutoscalingContext, candidates []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError) {
	if p.err != nil {
		return nil, p.err
	}
	var kept []*apiv1.Node
	for _, node := range candidates {
		if !p.drop[node.Name] {
			kept = append(kept, node)
		}
	}
	return kept, nil
}

func (p *droppingProcessor) CleanUp() {}

func TestNewDefragScaleDownNodeProcessor(t *testing.T) {
	allNodes := buildNodes("shared-reject", "scale-down-only-reject", "defrag-only-reject", "keep")

	testCases := []struct {
		name           string
		buildProcessor func([]nodes.ScaleDownNodeProcessor) *GkeInternalAutoscalingScaleDownNodeProcessor
		wantCandidates []string
	}{
		{
			name:           "scale down chain uses regular processor variant",
			buildProcessor: NewGkeInternalAutoscalingScaleDownNodeProcessor,
			wantCandidates: []string{"defrag-only-reject", "keep"},
		},
		{
			name:           "defrag chain swaps defrag-aware processor",
			buildProcessor: NewDefragScaleDownNodeProcessor,
			wantCandidates: []string{"scale-down-only-reject", "keep"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			processors := []nodes.ScaleDownNodeProcessor{
				newFakeProcessor("shared-reject"),
				&fakeDefragAwareScaleDownNodeProcessor{
					fakeScaleDownNodeProcessor: *newFakeProcessor("scale-down-only-reject"),
					defragVariant:              newFakeProcessor("defrag-only-reject"),
				},
			}

			composite := tc.buildProcessor(processors)
			candidates, err := composite.GetScaleDownCandidates(context.Background(), nil, allNodes)

			assert.NoError(t, err)
			assert.Equal(t, tc.wantCandidates, candidateNodeNames(candidates))
		})
	}
}

func TestGetScaleDownCandidatesWithStages(t *testing.T) {
	testCases := []struct {
		name          string
		processors    []nodes.ScaleDownNodeProcessor
		input         []string
		wantSurvivors []string
		// wantDroppedBy maps node name to the index of the processor that dropped it.
		wantDroppedBy map[string]int
	}{
		{
			name:          "no processors keeps everything",
			processors:    nil,
			input:         []string{"a", "b"},
			wantSurvivors: []string{"a", "b"},
			wantDroppedBy: map[string]int{},
		},
		{
			name:          "single processor drops are attributed to it",
			processors:    []nodes.ScaleDownNodeProcessor{&droppingProcessor{drop: map[string]bool{"b": true}}},
			input:         []string{"a", "b", "c"},
			wantSurvivors: []string{"a", "c"},
			wantDroppedBy: map[string]int{"b": 0},
		},
		{
			name: "drops are attributed to the processor that made them",
			processors: []nodes.ScaleDownNodeProcessor{
				&droppingProcessor{drop: map[string]bool{"a": true}},
				&droppingProcessor{drop: map[string]bool{"b": true}},
				&droppingProcessor{drop: map[string]bool{"c": true}},
			},
			input:         []string{"a", "b", "c", "d"},
			wantSurvivors: []string{"d"},
			wantDroppedBy: map[string]int{"a": 0, "b": 1, "c": 2},
		},
		{
			// A node dropped by an early processor must be attributed to that
			// processor even though later processors would also have dropped it.
			name: "first dropper wins",
			processors: []nodes.ScaleDownNodeProcessor{
				&droppingProcessor{drop: map[string]bool{"a": true}},
				&droppingProcessor{drop: map[string]bool{"a": true, "b": true}},
			},
			input:         []string{"a", "b", "c"},
			wantSurvivors: []string{"c"},
			wantDroppedBy: map[string]int{"a": 0, "b": 1},
		},
		{
			name: "everything dropped",
			processors: []nodes.ScaleDownNodeProcessor{
				&droppingProcessor{drop: map[string]bool{"a": true, "b": true}},
			},
			input:         []string{"a", "b"},
			wantSurvivors: nil,
			wantDroppedBy: map[string]int{"a": 0, "b": 0},
		},
		{
			name:          "empty input",
			processors:    []nodes.ScaleDownNodeProcessor{&droppingProcessor{drop: map[string]bool{"a": true}}},
			input:         nil,
			wantSurvivors: nil,
			wantDroppedBy: map[string]int{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewGkeInternalAutoscalingScaleDownNodeProcessor(tc.processors)
			got, stages, err := p.GetScaleDownCandidatesWithStages(context.Background(), nil, buildNodes(tc.input...))
			if err != nil {
				t.Fatalf("GetScaleDownCandidatesWithStages() returned error: %v", err)
			}

			gotNames := candidateNodeNames(got)
			if len(gotNames) != len(tc.wantSurvivors) {
				t.Fatalf("survivors = %v, want %v", gotNames, tc.wantSurvivors)
			}
			for i, name := range tc.wantSurvivors {
				if gotNames[i] != name {
					t.Errorf("survivors = %v, want %v", gotNames, tc.wantSurvivors)
					break
				}
			}

			if len(stages.DroppedBy) != len(tc.wantDroppedBy) {
				t.Fatalf("DroppedBy = %v, want %v", stages.DroppedBy, tc.wantDroppedBy)
			}
			for name, wantIdx := range tc.wantDroppedBy {
				gotIdx, ok := stages.DroppedBy[name]
				if !ok {
					t.Errorf("node %q missing from DroppedBy %v", name, stages.DroppedBy)
					continue
				}
				if gotIdx != wantIdx {
					t.Errorf("DroppedBy[%q] = %d, want %d", name, gotIdx, wantIdx)
				}
				if _, ok := stages.ProcessorFor(name); !ok {
					t.Errorf("ProcessorFor(%q) reported no processor", name)
				}
			}
			if len(stages.ProcessorNames) != len(tc.processors) {
				t.Errorf("ProcessorNames has %d entries, want %d", len(stages.ProcessorNames), len(tc.processors))
			}
		})
	}
}

// GetScaleDownCandidatesWithStages must agree with GetScaleDownCandidates on
// the surviving set, so that enabling attribution cannot change autoscaler
// behaviour.
func TestGetScaleDownCandidatesWithStagesMatchesPlainVariant(t *testing.T) {
	processors := []nodes.ScaleDownNodeProcessor{
		&droppingProcessor{drop: map[string]bool{"a": true}},
		&droppingProcessor{drop: map[string]bool{"c": true}},
	}
	input := []string{"a", "b", "c", "d"}

	plain, err := NewGkeInternalAutoscalingScaleDownNodeProcessor(processors).GetScaleDownCandidates(context.Background(), nil, buildNodes(input...))
	if err != nil {
		t.Fatalf("GetScaleDownCandidates() returned error: %v", err)
	}
	staged, _, err := NewGkeInternalAutoscalingScaleDownNodeProcessor(processors).GetScaleDownCandidatesWithStages(context.Background(), nil, buildNodes(input...))
	if err != nil {
		t.Fatalf("GetScaleDownCandidatesWithStages() returned error: %v", err)
	}

	plainNames, stagedNames := candidateNodeNames(plain), candidateNodeNames(staged)
	if len(plainNames) != len(stagedNames) {
		t.Fatalf("staged survivors = %v, plain survivors = %v", stagedNames, plainNames)
	}
	for i := range plainNames {
		if plainNames[i] != stagedNames[i] {
			t.Fatalf("staged survivors = %v, plain survivors = %v", stagedNames, plainNames)
		}
	}
}

func TestGetScaleDownCandidatesWithStagesPropagatesError(t *testing.T) {
	wantErr := errors.NewAutoscalerErrorf(errors.InternalError, "boom")
	processors := []nodes.ScaleDownNodeProcessor{
		&droppingProcessor{drop: map[string]bool{"a": true}},
		&droppingProcessor{err: wantErr},
	}

	_, stages, err := NewGkeInternalAutoscalingScaleDownNodeProcessor(processors).
		GetScaleDownCandidatesWithStages(context.Background(), nil, buildNodes("a", "b"))
	if err == nil {
		t.Fatal("GetScaleDownCandidatesWithStages() returned no error, want one")
	}
	// Attribution gathered before the failure must still be usable.
	if got, ok := stages.DroppedBy["a"]; !ok || got != 0 {
		t.Errorf("DroppedBy[a] = (%d, %v), want (0, true)", got, ok)
	}
	// The processor that failed excluded nothing, so a node that was still a
	// candidate when it failed must not be reported as dropped by it.
	if got, ok := stages.DroppedBy["b"]; ok {
		t.Errorf("DroppedBy[b] = %d, want no attribution: b survived every processor that completed", got)
	}
}

func TestProcessorForNilStages(t *testing.T) {
	var stages *ScaleDownCandidateStages
	if _, ok := stages.ProcessorFor("a"); ok {
		t.Error("ProcessorFor() on nil stages reported a processor")
	}
}

func buildNodes(names ...string) []*apiv1.Node {
	nodes := make([]*apiv1.Node, len(names))
	for i, name := range names {
		nodes[i] = &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	return nodes
}

func candidateNodeNames(nodes []*apiv1.Node) []string {
	names := make([]string, len(nodes))
	for i, node := range nodes {
		names[i] = node.Name
	}
	return names
}
