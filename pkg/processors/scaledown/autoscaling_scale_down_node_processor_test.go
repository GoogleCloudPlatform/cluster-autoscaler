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
