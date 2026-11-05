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
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/units"
)

func TestPodRequestsLocalSSD(t *testing.T) {
	localSSDRequirement := nodeSelectorRequirement(labels.EphemeralLocalSsdLabel, apiv1.NodeSelectorOpIn, labels.EphemeralLocalSsdEnabledValue)
	disabledLocalSSDRequirement := nodeSelectorRequirement(labels.EphemeralLocalSsdLabel, apiv1.NodeSelectorOpIn, "false")
	zoneRequirement := nodeSelectorRequirement("topology.kubernetes.io/zone", apiv1.NodeSelectorOpIn, "us-central1-a")

	tests := []struct {
		name string
		pod  *apiv1.Pod
		want bool
	}{
		{
			name: "no local SSD selector or affinity",
			pod:  test.BuildTestPod("pod", 1000, units.GiB),
			want: false,
		},
		{
			name: "nodeSelector requests local SSD",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withNodeSelector(labels.EphemeralLocalSsdLabel, labels.EphemeralLocalSsdEnabledValue)),
			want: true,
		},
		{
			name: "nodeSelector disables local SSD",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withNodeSelector(labels.EphemeralLocalSsdLabel, "false")),
			want: false,
		},
		{
			name: "nodeSelector on unrelated label",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withNodeSelector("cloud.google.com/compute-class", "lssd-class")),
			want: false,
		},
		{
			name: "nodeAffinity with empty nodeSelectorTerms",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms()),
			want: false,
		},
		{
			name: "nodeAffinity requires local SSD in single term",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(localSSDRequirement))),
			want: true,
		},
		{
			name: "nodeAffinity disables local SSD in single term",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(disabledLocalSSDRequirement))),
			want: false,
		},
		{
			name: "nodeAffinity In with enabled and disabled values",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.EphemeralLocalSsdLabel, apiv1.NodeSelectorOpIn, labels.EphemeralLocalSsdEnabledValue, "false")))),
			want: true,
		},
		{
			name: "nodeAffinity local SSD Exists in single term",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.EphemeralLocalSsdLabel, apiv1.NodeSelectorOpExists)))),
			want: true,
		},
		{
			// podrequirements reads only In and Exists expressions.
			name: "nodeAffinity local SSD Gt in single term",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.EphemeralLocalSsdLabel, apiv1.NodeSelectorOpGt, "0")))),
			want: false,
		},
		{
			name: "nodeAffinity local SSD Lt in single term",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.EphemeralLocalSsdLabel, apiv1.NodeSelectorOpLt, "1")))),
			want: false,
		},
		{
			name: "nodeAffinity requires local SSD in all terms",
			pod: test.BuildTestPod("pod", 1000, units.GiB, withTerms(
				nodeSelectorTerm(localSSDRequirement, zoneRequirement),
				nodeSelectorTerm(localSSDRequirement),
			)),
			want: true,
		},
		{
			name: "nodeAffinity requires local SSD in only one of two terms",
			pod: test.BuildTestPod("pod", 1000, units.GiB, withTerms(
				nodeSelectorTerm(localSSDRequirement),
				nodeSelectorTerm(zoneRequirement),
			)),
			// Single In requirement is preserved across terms, matching how NAP provisions for the pod.
			want: true,
		},
		{
			// podrequirements resolves multiple definitions of the same key by keeping the last In
			// expression and letting In affinity override nodeSelector (see b/235823013).
			name: "nodeAffinity multiple In terms: last In disables local SSD",
			pod: test.BuildTestPod("pod", 1000, units.GiB, withTerms(
				nodeSelectorTerm(localSSDRequirement),
				nodeSelectorTerm(disabledLocalSSDRequirement),
			)),
			want: false,
		},
		{
			name: "nodeAffinity multiple In terms: last In enables local SSD",
			pod: test.BuildTestPod("pod", 1000, units.GiB, withTerms(
				nodeSelectorTerm(disabledLocalSSDRequirement),
				nodeSelectorTerm(localSSDRequirement),
			)),
			want: true,
		},
		{
			name: "nodeSelector enabled overridden by In affinity disabling local SSD",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withNodeSelector(labels.EphemeralLocalSsdLabel, labels.EphemeralLocalSsdEnabledValue), withTerms(nodeSelectorTerm(disabledLocalSSDRequirement))),
			want: false,
		},
		{
			name: "nodeSelector disabled overridden by In affinity enabling local SSD",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withNodeSelector(labels.EphemeralLocalSsdLabel, "false"), withTerms(nodeSelectorTerm(localSSDRequirement))),
			want: true,
		},
		{
			name: "nodeAffinity local SSD DoesNotExist",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.EphemeralLocalSsdLabel, apiv1.NodeSelectorOpDoesNotExist)))),
			want: false,
		},
		{
			name: "nodeAffinity local SSD NotIn",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement(labels.EphemeralLocalSsdLabel, apiv1.NodeSelectorOpNotIn, labels.EphemeralLocalSsdEnabledValue)))),
			want: false,
		},
		{
			name: "nodeAffinity Exists on unrelated label",
			pod:  test.BuildTestPod("pod", 1000, units.GiB, withTerms(nodeSelectorTerm(nodeSelectorRequirement("example.com/local-ssd", apiv1.NodeSelectorOpExists)))),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, podRequestsLocalSSD(tc.pod))
			// The injected NotIn requirement must not change the verdict.
			MakePodCSN(tc.pod, "ns/buffer")
			assert.Equal(t, tc.want, podRequestsLocalSSD(tc.pod))
		})
	}
}

func TestPodRequestsLocalSSDNilPod(t *testing.T) {
	assert.False(t, podRequestsLocalSSD(nil))
}
