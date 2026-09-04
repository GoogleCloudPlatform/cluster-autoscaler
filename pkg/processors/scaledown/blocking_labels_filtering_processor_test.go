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
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"

	gke_labels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

// buildSliceNode builds a ComputeClass node optionally bound to a TPU slice via
// the gke-tpu-slice label.
func buildSliceNode(name, slice string) *apiv1.Node {
	node := buildCCNode(name, testCC)
	if slice != "" {
		node.Labels[gke_labels.TPUSliceLabel] = slice
	}
	return node
}

// buildLabeledNode builds a ComputeClass node carrying an arbitrary label, used
// to verify configurable scale-down-blocking labels.
func buildLabeledNode(name, key, value string) *apiv1.Node {
	node := buildCCNode(name, testCC)
	node.Labels[key] = value
	return node
}

func TestBlockingLabelsFilteringProcessor_FilterUnremovableNodes(t *testing.T) {
	testCases := []struct {
		name            string
		blockingLabels  []string
		groups          []testNodeGroup
		wantRemovable   []string
		wantUnremovable []string
	}{
		{
			name:           "bound node filtered, sibling passes through",
			blockingLabels: []string{gke_labels.TPUSliceLabel},
			groups: []testNodeGroup{
				{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildSliceNode("ng1-a", "slice-1"), buildCCNode("ng1-b", testCC)}},
				{id: "ng2", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng2-a", testCC), buildCCNode("ng2-b", testCC)}},
			},
			wantRemovable:   []string{"ng1-b", "ng2-a", "ng2-b"},
			wantUnremovable: []string{"ng1-a"},
		},
		{
			name:           "no bound nodes, all pass through",
			blockingLabels: []string{gke_labels.TPUSliceLabel},
			groups: []testNodeGroup{
				{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng1-a", testCC), buildCCNode("ng1-b", testCC)}},
				{id: "ng2", atomic: true, nodes: []*apiv1.Node{buildCCNode("ng2-a", testCC), buildCCNode("ng2-b", testCC)}},
			},
			wantRemovable:   []string{"ng1-a", "ng1-b", "ng2-a", "ng2-b"},
			wantUnremovable: nil,
		},
		{
			name:           "non-atomic bound node kept, unbound node removable",
			blockingLabels: []string{gke_labels.TPUSliceLabel},
			groups: []testNodeGroup{
				{id: "ng1", atomic: false, nodes: []*apiv1.Node{buildSliceNode("ng1-a", "slice-1"), buildCCNode("ng1-b", testCC)}},
			},
			wantRemovable:   []string{"ng1-b"},
			wantUnremovable: []string{"ng1-a"},
		},
		{
			name:           "empty label value is treated as unbound",
			blockingLabels: []string{gke_labels.TPUSliceLabel},
			groups: []testNodeGroup{
				{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildSliceNode("ng1-a", ""), buildCCNode("ng1-b", testCC)}},
			},
			wantRemovable:   []string{"ng1-a", "ng1-b"},
			wantUnremovable: nil,
		},
		{
			name:           "custom blocking label filters matching node",
			blockingLabels: []string{"example.com/block-scale-down"},
			groups: []testNodeGroup{
				{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildLabeledNode("ng1-a", "example.com/block-scale-down", "yes"), buildCCNode("ng1-b", testCC)}},
			},
			wantRemovable:   []string{"ng1-b"},
			wantUnremovable: []string{"ng1-a"},
		},
		{
			name:           "no blocking labels configured, all pass through",
			blockingLabels: nil,
			groups: []testNodeGroup{
				{id: "ng1", atomic: true, nodes: []*apiv1.Node{buildSliceNode("ng1-a", "slice-1"), buildCCNode("ng1-b", testCC)}},
			},
			wantRemovable:   []string{"ng1-a", "ng1-b"},
			wantUnremovable: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := setupAtomicMinCapacityCtx(t, tc.groups)
			processor := NewBlockingLabelsFilteringProcessor(tc.blockingLabels)

			removable, unremovable := processor.FilterUnremovableNodes(context.Background(), ctx, nodes.NewDefaultScaleDownContext(), candidatesFromGroups(tc.groups))

			assert.ElementsMatch(t, tc.wantRemovable, nodeNames(removable))
			assert.ElementsMatch(t, tc.wantUnremovable, unremovableNames(unremovable))
		})
	}
}
