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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestPodRequestsAccelerator(t *testing.T) {
	zoneRequirement := nodeSelectorRequirement("topology.kubernetes.io/zone", apiv1.NodeSelectorOpIn, "us-central1-a")
	withResource := func(name apiv1.ResourceName) func(*apiv1.Pod) {
		return func(pod *apiv1.Pod) {
			pod.Spec.Containers[0].Resources.Requests[name] = resource.MustParse("1")
		}
	}

	tests := []struct {
		name string
		pod  *apiv1.Pod
		want bool
	}{
		{
			name: "no accelerator request, selector or affinity",
			pod:  test.BuildTestPod("pod", 1000, units.GiB),
			want: false,
		},
		{
			name: "GPU request",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withResource("nvidia.com/gpu")),
			want: true,
		},
		{
			name: "TPU request",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withResource("google.com/tpu")),
			want: true,
		},
		{
			name: "non-accelerator extended resource request",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withResource("example.com/widget")),
			want: false,
		},
		{
			name: "nodeSelector on GPU label",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withNodeSelector(labels.GPULabel, "nvidia-l4")),
			want: true,
		},
		{
			name: "nodeSelector on TPU label",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withNodeSelector(labels.TPULabel, "tpu-v5-lite-podslice")),
			want: true,
		},
		{
			name: "nodeSelector on unrelated label",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withNodeSelector("cloud.google.com/compute-class", "gpu-class")),
			want: false,
		},
		{
			name: "nodeAffinity with empty nodeSelectorTerms",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms()),
			want: false,
		},
		{
			name: "nodeAffinity GPU In in single term",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.GPULabel, apiv1.NodeSelectorOpIn, "nvidia-l4")))),
			want: true,
		},
		{
			name: "nodeAffinity TPU Exists in single term",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.TPULabel, apiv1.NodeSelectorOpExists)))),
			want: true,
		},
		{
			// podrequirements reads only In and Exists expressions.
			name: "nodeAffinity GPU Gt in single term",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.GPULabel, apiv1.NodeSelectorOpGt, "1")))),
			want: false,
		},
		{
			name: "nodeAffinity TPU Lt in single term",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.TPULabel, apiv1.NodeSelectorOpLt, "1")))),
			want: false,
		},
		{
			name: "nodeAffinity requires an accelerator in all terms",
			pod: test.BuildTestPod("pod", 1000, units.GiB, withTerms(
				nodeSelectorTerm(zoneRequirement, nodeSelectorRequirement(labels.GPULabel, apiv1.NodeSelectorOpIn, "nvidia-l4")),
				nodeSelectorTerm(nodeSelectorRequirement(labels.TPULabel, apiv1.NodeSelectorOpExists)),
			)),
			want: true,
		},
		{
			name: "nodeAffinity requires an accelerator in only one of two terms",
			pod: test.BuildTestPod("pod", 1000, units.GiB, withTerms(
				nodeSelectorTerm(nodeSelectorRequirement(labels.GPULabel, apiv1.NodeSelectorOpIn, "nvidia-l4")),
				nodeSelectorTerm(zoneRequirement),
			)),
			// Requirements are merged across terms, matching how NAP provisions for the pod.
			want: true,
		},
		{
			name: "nodeAffinity GPU DoesNotExist",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.GPULabel, apiv1.NodeSelectorOpDoesNotExist)))),
			want: false,
		},
		{
			name: "nodeAffinity TPU NotIn",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.TPULabel, apiv1.NodeSelectorOpNotIn, "tpu-v5-lite-podslice")))),
			want: false,
		},
		{
			name: "nodeAffinity Exists on unrelated label",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement("example.com/accelerator", apiv1.NodeSelectorOpExists)))),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, podRequestsAccelerator(tc.pod))
			// The injected DoesNotExist requirements must not change the verdict.
			MakePodCSN(tc.pod, "ns/buffer")
			assert.Equal(t, tc.want, podRequestsAccelerator(tc.pod))
		})
	}
}

func TestPodRequestsAcceleratorNilPod(t *testing.T) {
	assert.False(t, podRequestsAccelerator(nil))
}

func TestHasAccelerator(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{
			name:   "GPU label",
			labels: map[string]string{labels.GPULabel: "nvidia-l4"},
			want:   true,
		},
		{
			name:   "TPU label",
			labels: map[string]string{labels.TPULabel: "tpu-v5-lite-podslice"},
			want:   true,
		},
		{
			name:   "empty GPU label value",
			labels: map[string]string{labels.GPULabel: ""},
			want:   true,
		},
		{
			name:   "no accelerator labels",
			labels: map[string]string{"topology.kubernetes.io/zone": "us-central1-a"},
			want:   false,
		},
		{
			name:   "nil labels",
			labels: nil,
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := test.BuildTestNode("node", 4000, 8*units.GB)
			node.Labels = tc.labels
			assert.Equal(t, tc.want, hasAccelerator(node))
		})
	}
}
