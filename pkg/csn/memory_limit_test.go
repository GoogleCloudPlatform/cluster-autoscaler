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

package csn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/metadata"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestNewMemoryLimit(t *testing.T) {
	tests := []struct {
		name      string
		flagValue string
		flags     map[string]string
		want      int64
	}{
		{
			name:  "flag unset",
			flags: map[string]string{},
			want:  defaultMinUnsupportedMemoryGB,
		},
		{
			name:  "flag set",
			flags: flagsWithLimit("150"),
			want:  150,
		},
		{
			name:  "flag unparsable",
			flags: flagsWithLimit("not-a-number"),
			want:  defaultMinUnsupportedMemoryGB,
		},
		{
			name:  "flag zero",
			flags: flagsWithLimit("0"),
			want:  defaultMinUnsupportedMemoryGB,
		},
		{
			name:  "flag negative",
			flags: flagsWithLimit("-5"),
			want:  defaultMinUnsupportedMemoryGB,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			experimentsManager := experiments.NewMockManagerWithOptions(version.Version{}, map[string]bool{}, tc.flags)
			assert.Equal(t, tc.want, NewMemoryLimit(experimentsManager).GB())
		})
	}
}

func TestMemoryLimitGBZeroValue(t *testing.T) {
	assert.Equal(t, int64(defaultMinUnsupportedMemoryGB), MemoryLimit{}.GB())
}

func TestMemoryLimitExceededByPodRequest(t *testing.T) {
	tests := []struct {
		name   string
		limit  MemoryLimit
		memReq int64
		want   bool
	}{
		{
			name:   "well below the limit",
			memReq: 1 * units.GiB,
			want:   false,
		},
		{
			name: "just below the limit",
			// The limit is in decimal GB, so 208 GB fits and 209 GB does not.
			memReq: 208 * units.GB,
			want:   false,
		},
		{
			name:   "exactly at the limit",
			memReq: 209 * units.GB,
			want:   true,
		},
		{
			name:   "above the limit",
			memReq: 300 * units.GB,
			want:   true,
		},
		{
			name: "binary and decimal units are not interchangeable",
			// 195 GiB is 209.4 GB, which is over the limit even though 195 < 209.
			memReq: 195 * units.GiB,
			want:   true,
		},
		{
			name:   "non-default limit",
			limit:  MemoryLimit{minUnsupportedGB: 129},
			memReq: 130 * units.GB,
			want:   true,
		},
		{
			name:   "non-default limit not reached",
			limit:  MemoryLimit{minUnsupportedGB: 300},
			memReq: 250 * units.GB,
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.limit.ExceededByPodRequest(test.BuildTestPod("some-pod", 1000, tc.memReq)))
		})
	}
}

func TestMemoryLimitExceededByPodRequestNilPod(t *testing.T) {
	assert.False(t, MemoryLimit{}.ExceededByPodRequest(nil))
}

func TestMemoryLimitBlocksPodOnAnyNode(t *testing.T) {
	const bufferId = "ns/buffer"

	// A node that satisfies everything MakePodCSN asks for except, optionally, the memory limit.
	suspendableNodeLabels := map[string]string{
		metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
		metadata.BufferAssignmentKey:       "ns_buffer",
	}

	tests := []struct {
		name       string
		limit      MemoryLimit
		nodeLabels map[string]string
		want       bool
	}{
		{
			name:       "node over the limit is blocked by the limit alone",
			nodeLabels: nodeLabels(suspendableNodeLabels, labels.MemoryScalingLevelLabel, "256"),
			want:       true,
		},
		{
			name:       "node exactly at the limit is blocked",
			nodeLabels: nodeLabels(suspendableNodeLabels, labels.MemoryScalingLevelLabel, "209"),
			want:       true,
		},
		{
			name:       "node under the limit is not blocked",
			nodeLabels: nodeLabels(suspendableNodeLabels, labels.MemoryScalingLevelLabel, "64"),
			want:       false,
		},
		{
			name:       "node without the label is not blocked",
			nodeLabels: suspendableNodeLabels,
			want:       false,
		},
		{
			name:       "node with an unparsable label is not blamed on the limit",
			nodeLabels: nodeLabels(suspendableNodeLabels, labels.MemoryScalingLevelLabel, "lots"),
			want:       false,
		},
		{
			name: "node rejected by the pod's other requirements is not blamed on the limit",
			// Over the limit, but missing the buffer assignment label, so it would not have
			// worked even if standby buffers supported nodes this large.
			nodeLabels: map[string]string{
				metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
				labels.MemoryScalingLevelLabel:     "256",
			},
			want: false,
		},
		{
			name:       "non-default limit makes a smaller node blocking",
			limit:      MemoryLimit{minUnsupportedGB: 129},
			nodeLabels: nodeLabels(suspendableNodeLabels, labels.MemoryScalingLevelLabel, "200"),
			want:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := test.BuildTestPod("some-pod", 1000, units.GiB)
			MakePodCSN(pod, bufferId, WithMemoryLimit(tc.limit))
			node := test.BuildTestNode("node", 4*units.GiB, 8000, test.WithNodeLabels(tc.nodeLabels))
			assert.Equal(t, tc.want, tc.limit.BlocksPodOnAnyNode(pod, node))
		})
	}
}

func TestMemoryLimitBlocksPodOnAnyNodeLeavesNodeUnchanged(t *testing.T) {
	pod := &apiv1.Pod{}
	MakePodCSN(pod, "ns/buffer")
	originalLabels := map[string]string{
		metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
		metadata.BufferAssignmentKey:       "ns_buffer",
		labels.MemoryScalingLevelLabel:     "256",
	}
	node := &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: originalLabels}}

	assert.True(t, MemoryLimit{}.BlocksPodOnAnyNode(pod, node))
	assert.Equal(t, "256", node.Labels[labels.MemoryScalingLevelLabel])
}

func TestMemoryLimitBlocksPodOnAnyNodeNilArguments(t *testing.T) {
	node := &apiv1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}
	assert.False(t, MemoryLimit{}.BlocksPodOnAnyNode(nil, node))
	assert.False(t, MemoryLimit{}.BlocksPodOnAnyNode(&apiv1.Pod{}, nil))
	assert.False(t, MemoryLimit{}.BlocksPodOnAnyNode(&apiv1.Pod{}))
}

func TestMemoryLimitBlocksPodOnAnyNodeScansAllNodes(t *testing.T) {
	pod := &apiv1.Pod{}
	MakePodCSN(pod, "ns/buffer")
	suspendable := map[string]string{
		metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
		metadata.BufferAssignmentKey:       "ns_buffer",
	}
	node := func(name string, labels map[string]string) *apiv1.Node {
		return test.BuildTestNode(name, 4000, 8*units.GB, test.WithNodeLabels(labels))
	}
	// Neither of these is blocked by the limit: one is small enough, the other is over the limit
	// but does not carry the buffer assignment the pod asks for.
	small := node("small", nodeLabels(suspendable, labels.MemoryScalingLevelLabel, "64"))
	unrelated := node("unrelated", map[string]string{labels.MemoryScalingLevelLabel: "256"})
	blocked := node("blocked", nodeLabels(suspendable, labels.MemoryScalingLevelLabel, "256"))

	assert.False(t, MemoryLimit{}.BlocksPodOnAnyNode(pod, small, unrelated))
	// The blocking node is found even when it is not the first one examined.
	assert.True(t, MemoryLimit{}.BlocksPodOnAnyNode(pod, small, unrelated, blocked))
}

// nodeLabels returns a copy of base with an extra label set.
func nodeLabels(base map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(base)+1)
	for k, v := range base {
		result[k] = v
	}
	result[key] = value
	return result
}

func flagsWithLimit(val string) map[string]string {
	return map[string]string{experiments.ColdStandbyNodesMinUnsupportedMemoryGBFlag: val}
}
