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

package extendeddurationpods

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestExtendedDurationPodsPriority(t *testing.T) {
	p1 := test.BuildTestPod("p1", 1, 1)
	p2 := test.BuildTestPod("p2", 2, 1)
	p3 := test.BuildTestPod("p3", 2, 100)

	p1Edp := test.BuildTestPod("p4", 2, 10)
	p1Edp.Spec.NodeSelector = map[string]string{labels.ExtendedDurationPodsLabel: "2"}

	p2Edp := test.BuildTestPod("p5", 2, 110)
	p2Edp.Spec.NodeSelector = map[string]string{labels.ExtendedDurationPodsLabel: "2"}

	node := test.BuildTestNode("node1", 4000, 6000)
	nodeInfo := framework.NewTestNodeInfo(node)

	nodeWithExtendedDuration := test.BuildTestNode("node2", 4000, 6000)
	nodeWithExtendedDuration.Labels = map[string]string{labels.ExtendedDurationPodsLabel: "2"}
	nodeInfo2 := framework.NewTestNodeInfo(nodeWithExtendedDuration)

	testCases := map[string]struct {
		inputPodGroups    []estimator.PodEquivalenceGroup
		expectedPodGroups []estimator.PodEquivalenceGroup
		inputNodeTemplate *framework.NodeInfo
	}{
		"extended duration with unsorted pods": {
			inputPodGroups:    singlePodGroups([]*v1.Pod{p2, p1, p3}),
			expectedPodGroups: singlePodGroups([]*v1.Pod{p3, p2, p1}),
			inputNodeTemplate: nodeInfo,
		},
		"extended duration with edp-node with no edp pods": {
			inputPodGroups:    singlePodGroups([]*v1.Pod{p2, p1, p3}),
			expectedPodGroups: singlePodGroups([]*v1.Pod{p3, p2, p1}),
			inputNodeTemplate: nodeInfo2,
		},
		"extended duration with no edp-node with mixed pods": {
			inputPodGroups:    singlePodGroups([]*v1.Pod{p1, p2, p3, p1Edp, p2Edp}),
			expectedPodGroups: singlePodGroups([]*v1.Pod{p2Edp, p3, p1Edp, p2, p1}),
			inputNodeTemplate: nodeInfo,
		},
		"extended duration with edp-node with only sorted edp pods": {
			inputPodGroups:    singlePodGroups([]*v1.Pod{p2Edp, p1Edp}),
			expectedPodGroups: singlePodGroups([]*v1.Pod{p2Edp, p1Edp}),
			inputNodeTemplate: nodeInfo2,
		},
		"extended duration with edp-node with only unsorted edp pods": {
			inputPodGroups:    singlePodGroups([]*v1.Pod{p1Edp, p2Edp}),
			expectedPodGroups: singlePodGroups([]*v1.Pod{p2Edp, p1Edp}),
			inputNodeTemplate: nodeInfo2,
		},
		"extended duration with edp-node with mixed pods": {
			inputPodGroups:    singlePodGroups([]*v1.Pod{p1, p2, p3, p1Edp, p2Edp}),
			expectedPodGroups: singlePodGroups([]*v1.Pod{p2Edp, p1Edp, p3, p2, p1}),
			inputNodeTemplate: nodeInfo2,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			orderer := NewExtendedDurationPodsFirstDecreasingPodOrderer()
			actual := orderer.Order(tc.inputPodGroups, tc.inputNodeTemplate, nil)
			assert.Equal(t, tc.expectedPodGroups, actual)
		})
	}
}

func singlePodGroups(pods []*v1.Pod) []estimator.PodEquivalenceGroup {
	groups := []estimator.PodEquivalenceGroup{}
	for _, pod := range pods {
		groups = append(groups, estimator.PodEquivalenceGroup{Pods: []*v1.Pod{pod}})
	}

	return groups
}
