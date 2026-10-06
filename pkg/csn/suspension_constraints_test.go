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
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/util/version"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/csn/metadata"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/experiments"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestSuspensionConstraintsBlocksPodRequest(t *testing.T) {
	constraints := NewSuspensionConstraints(experiments.NewMockManagerWithOptions(version.Version{}, nil, nil))

	overMemoryPod := test.BuildTestPod("over-mem", 100, 300*units.GB)
	MakePodCSN(overMemoryPod, "default/buffer")
	msg, blocked := constraints.BlocksPodRequest(overMemoryPod)
	assert.True(t, blocked)
	assert.Contains(t, msg, "209 GB of memory")

	lssdPod := test.BuildTestPod("lssd", 100, 1*units.GB)
	lssdPod.Spec.NodeSelector = map[string]string{
		labels.EphemeralLocalSsdLabel: labels.EphemeralLocalSsdEnabledValue,
	}
	MakePodCSN(lssdPod, "default/buffer")
	msg, blocked = constraints.BlocksPodRequest(lssdPod)
	assert.True(t, blocked)
	assert.Contains(t, msg, "Local SSDs")

	bothPod := test.BuildTestPod("both", 100, 300*units.GB)
	bothPod.Spec.NodeSelector = map[string]string{
		labels.EphemeralLocalSsdLabel: labels.EphemeralLocalSsdEnabledValue,
	}
	MakePodCSN(bothPod, "default/buffer")
	msg, blocked = constraints.BlocksPodRequest(bothPod)
	assert.True(t, blocked)
	assert.Contains(t, msg, "209 GB of memory")

	normalPod := test.BuildTestPod("normal", 100, 1*units.GB)
	MakePodCSN(normalPod, "default/buffer")
	_, blocked = constraints.BlocksPodRequest(normalPod)
	assert.False(t, blocked)

	_, blocked = constraints.BlocksPodRequest(nil)
	assert.False(t, blocked)
}

func TestSuspensionConstraintsBlocksPodOnAnyNode(t *testing.T) {
	const bufferID = "default/buffer"
	constraints := NewSuspensionConstraints(experiments.NewMockManagerWithOptions(version.Version{}, nil, nil))

	pod := test.BuildTestPod("pod", 100, 1*units.GB)
	MakePodCSN(pod, bufferID)

	csnNodeWithLabels := func(name string, extraLabels map[string]string) *apiv1.Node {
		nodeLabels := map[string]string{
			metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
			metadata.BufferAssignmentKey:       "default_buffer",
		}
		for k, v := range extraLabels {
			nodeLabels[k] = v
		}
		return test.BuildTestNode(name, 4000, 8*units.GB, test.WithNodeLabels(nodeLabels))
	}

	smallNode := csnNodeWithLabels("small", map[string]string{
		labels.MemoryScalingLevelLabel: "64",
	})
	noLabelsCSNNode := csnNodeWithLabels("no-labels", nil)
	unparsableMemNode := csnNodeWithLabels("unparsable-mem", map[string]string{
		labels.MemoryScalingLevelLabel: "lots",
	})
	exactLimitNode := csnNodeWithLabels("exact-limit", map[string]string{
		labels.MemoryScalingLevelLabel: "209",
	})
	largeNode := csnNodeWithLabels("large", map[string]string{
		labels.MemoryScalingLevelLabel: "256",
	})
	lssdDisabledNode := csnNodeWithLabels("lssd-false", map[string]string{
		labels.EphemeralLocalSsdLabel: "false",
	})
	lssdNode := csnNodeWithLabels("lssd", map[string]string{
		labels.MemoryScalingLevelLabel: "64",
		labels.EphemeralLocalSsdLabel:  labels.EphemeralLocalSsdEnabledValue,
	})
	largeLSSDNode := csnNodeWithLabels("large-lssd", map[string]string{
		labels.MemoryScalingLevelLabel: "256",
		labels.EphemeralLocalSsdLabel:  labels.EphemeralLocalSsdEnabledValue,
	})
	// Violates both constraints, but lacks the buffer assignment label required by the pod.
	unrelatedNode := test.BuildTestNode("unrelated", 4000, 8*units.GB, test.WithNodeLabels(map[string]string{
		metadata.SoftWorkloadSeparationKey: metadata.SoftWorkloadSeparationValue,
		labels.MemoryScalingLevelLabel:     "256",
		labels.EphemeralLocalSsdLabel:      labels.EphemeralLocalSsdEnabledValue,
	}))

	_, blocked := constraints.BlocksPodOnAnyNode(pod, smallNode, noLabelsCSNNode, unparsableMemNode, lssdDisabledNode, unrelatedNode)
	assert.False(t, blocked)

	msg, blocked := constraints.BlocksPodOnAnyNode(pod, exactLimitNode)
	assert.True(t, blocked)
	assert.Contains(t, msg, "209 GB of memory")

	msg, blocked = constraints.BlocksPodOnAnyNode(pod, largeNode)
	assert.True(t, blocked)
	assert.Contains(t, msg, "209 GB of memory")

	msg, blocked = constraints.BlocksPodOnAnyNode(pod, lssdNode)
	assert.True(t, blocked)
	assert.Contains(t, msg, "Local SSDs")

	msg, blocked = constraints.BlocksPodOnAnyNode(pod, largeLSSDNode)
	assert.True(t, blocked)
	assert.Contains(t, msg, "209 GB of memory")

	// Even when the Local SSD node appears before the large memory node in the slice,
	// constraint priority order (memory limit first) is deterministic.
	msg, blocked = constraints.BlocksPodOnAnyNode(pod, lssdNode, largeNode)
	assert.True(t, blocked)
	assert.Contains(t, msg, "209 GB of memory")

	// Scans past non-blocking and unrelated nodes to find a blocking node.
	msg, blocked = constraints.BlocksPodOnAnyNode(pod, smallNode, unrelatedNode, lssdNode)
	assert.True(t, blocked)
	assert.Contains(t, msg, "Local SSDs")

	// Pod explicitly selecting Local SSD is still attributed to the Local SSD restriction on a matching node.
	lssdSelectorPod := test.BuildTestPod("lssd-selector", 100, 1*units.GB)
	lssdSelectorPod.Spec.NodeSelector = map[string]string{
		labels.EphemeralLocalSsdLabel: labels.EphemeralLocalSsdEnabledValue,
	}
	MakePodCSN(lssdSelectorPod, bufferID)
	msg, blocked = constraints.BlocksPodOnAnyNode(lssdSelectorPod, lssdNode)
	assert.True(t, blocked)
	assert.Contains(t, msg, "Local SSDs")

	// Pod with user-defined NodeAffinity terms (exercises len(strippedTerms) > 0):
	// matches when the rejected node satisfies the user's NodeAffinity term, and does not
	// match when the rejected node fails the user's NodeAffinity term.
	zoneAffinityPod := test.BuildTestPod("zone-affinity-pod", 100, 1*units.GB)
	zoneAffinityPod.Spec.Affinity = &apiv1.Affinity{
		NodeAffinity: &apiv1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
				NodeSelectorTerms: []apiv1.NodeSelectorTerm{
					{
						MatchExpressions: []apiv1.NodeSelectorRequirement{
							{
								Key:      "topology.kubernetes.io/zone",
								Operator: apiv1.NodeSelectorOpIn,
								Values:   []string{"us-central1-a"},
							},
						},
					},
				},
			},
		},
	}
	MakePodCSN(zoneAffinityPod, bufferID)
	lssdNodeInZoneA := csnNodeWithLabels("lssd-zone-a", map[string]string{
		labels.EphemeralLocalSsdLabel: labels.EphemeralLocalSsdEnabledValue,
		"topology.kubernetes.io/zone": "us-central1-a",
	})
	lssdNodeInZoneB := csnNodeWithLabels("lssd-zone-b", map[string]string{
		labels.EphemeralLocalSsdLabel: labels.EphemeralLocalSsdEnabledValue,
		"topology.kubernetes.io/zone": "us-central1-b",
	})
	_, blocked = constraints.BlocksPodOnAnyNode(zoneAffinityPod, lssdNodeInZoneB)
	assert.False(t, blocked)
	msg, blocked = constraints.BlocksPodOnAnyNode(zoneAffinityPod, lssdNodeInZoneA)
	assert.True(t, blocked)
	assert.Contains(t, msg, "Local SSDs")

	// Non-default memory limit via experiment flag.
	customConstraints := NewSuspensionConstraints(experiments.NewMockManagerWithOptions(version.Version{}, nil, flagsWithLimit("129")))
	customPod := test.BuildTestPod("custom-pod", 100, 1*units.GB)
	MakePodCSN(customPod, bufferID, WithMemoryLimit(MemoryLimit{minUnsupportedGB: 129}))
	node200 := csnNodeWithLabels("node-200", map[string]string{
		labels.MemoryScalingLevelLabel: "200",
	})
	msg, blocked = customConstraints.BlocksPodOnAnyNode(customPod, node200)
	assert.True(t, blocked)
	assert.Contains(t, msg, "129 GB of memory")

	// Non-CSN pod without affinity is not blocked by required.Match(largeNode).
	plainPod := test.BuildTestPod("plain-pod", 100, 1*units.GB)
	_, blocked = constraints.BlocksPodOnAnyNode(plainPod, largeNode)
	assert.False(t, blocked)

	// Nil/empty arguments.
	_, blocked = constraints.BlocksPodOnAnyNode(nil, largeNode)
	assert.False(t, blocked)
	_, blocked = constraints.BlocksPodOnAnyNode(pod, nil)
	assert.False(t, blocked)
	_, blocked = constraints.BlocksPodOnAnyNode(pod)
	assert.False(t, blocked)
}
