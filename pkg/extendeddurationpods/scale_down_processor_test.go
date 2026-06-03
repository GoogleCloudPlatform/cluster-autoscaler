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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	gkelabels "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestScaleDownProcessorGetScaleDownCandidates(t *testing.T) {
	n1 := test.BuildTestNode("n1", 100, 100)
	n2 := test.BuildTestNode("n2", 100, 100)
	n3 := test.BuildTestNode("n3", 100, 100)

	// Set very high tGPS on one of the pods to verify that just high tGPS is not enough to block scale-down, the pod has to have the
	// EDP node selector/affinity.
	veryHighTgps := int64(9999999)
	highTgpsButNotEdp := test.BuildTestPod("highTgpsButNotEdp", 1, 1)
	highTgpsButNotEdp.Spec.TerminationGracePeriodSeconds = &veryHighTgps
	regular1 := test.BuildTestPod("regular1", 1, 1)
	regular2 := test.BuildTestPod("regular2", 1, 1)

	edp1 := test.BuildTestPod("edp1", 1, 1)
	edp1.Spec.NodeSelector = map[string]string{gkelabels.ExtendedDurationPodsLabel: "1"}

	edp2 := test.BuildTestPod("edp2", 1, 1)
	edp2.Spec.Affinity = &apiv1.Affinity{
		NodeAffinity: &apiv1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
				NodeSelectorTerms: []apiv1.NodeSelectorTerm{
					{
						MatchExpressions: []apiv1.NodeSelectorRequirement{
							{Key: gkelabels.ExtendedDurationPodsLabel, Operator: apiv1.NodeSelectorOpIn, Values: []string{"1"}},
						},
					},
				},
			},
		},
	}

	testCases := map[string]struct {
		snapshot       func() clustersnapshot.ClusterSnapshot
		candidateNodes []*apiv1.Node
		expectedNodes  []*apiv1.Node
	}{
		"no node can be filtered": {
			snapshot: func() clustersnapshot.ClusterSnapshot {
				snapshot := testsnapshot.NewTestSnapshotOrDie(t)
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(n1, highTgpsButNotEdp))
				assert.NoError(t, err)
				err = snapshot.AddNodeInfo(framework.NewTestNodeInfo(n2, regular1, regular2))
				assert.NoError(t, err)
				return snapshot
			},
			candidateNodes: []*apiv1.Node{n1, n2},
			expectedNodes:  []*apiv1.Node{n1, n2},
		},
		"all nodes can be filtered": {
			snapshot: func() clustersnapshot.ClusterSnapshot {
				snapshot := testsnapshot.NewTestSnapshotOrDie(t)
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(n1, edp1))
				assert.NoError(t, err)
				err = snapshot.AddNodeInfo(framework.NewTestNodeInfo(n2, edp2))
				assert.NoError(t, err)
				return snapshot
			},
			candidateNodes: []*apiv1.Node{n1, n2},
			expectedNodes:  []*apiv1.Node{},
		},
		"some nodes can be filtered": {
			snapshot: func() clustersnapshot.ClusterSnapshot {
				snapshot := testsnapshot.NewTestSnapshotOrDie(t)
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(n1, edp1))
				assert.NoError(t, err)
				err = snapshot.AddNodeInfo(framework.NewTestNodeInfo(n2, edp2))
				assert.NoError(t, err)
				err = snapshot.AddNodeInfo(framework.NewTestNodeInfo(n3, regular2))
				assert.NoError(t, err)

				return snapshot
			},
			candidateNodes: []*apiv1.Node{n1, n2, n3},
			expectedNodes:  []*apiv1.Node{n3},
		},
		"some nodes can be filtered with mixed pods": {
			snapshot: func() clustersnapshot.ClusterSnapshot {
				snapshot := testsnapshot.NewTestSnapshotOrDie(t)
				err := snapshot.AddNodeInfo(framework.NewTestNodeInfo(n1, edp1, regular1))
				assert.NoError(t, err)
				err = snapshot.AddNodeInfo(framework.NewTestNodeInfo(n3, regular2))
				assert.NoError(t, err)

				return snapshot
			},
			candidateNodes: []*apiv1.Node{n1, n3},
			expectedNodes:  []*apiv1.Node{n3},
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			processor := NewScaleDownProcessor()
			nodes, err := processor.GetScaleDownCandidates(&context.AutoscalingContext{ClusterSnapshot: testCase.snapshot()}, testCase.candidateNodes)
			assert.NoError(t, err)
			assert.ElementsMatch(t, testCase.expectedNodes, nodes)
		})
	}
}

func TestGetPodDestinationCandidates(t *testing.T) {
	notLabeledHigherVersionNode := test.BuildTestNode("notLabeledHigherVersionNode", 100, 100)
	notLabeledLowerVersionNode := test.BuildTestNode("notLabeledLowerVersionNode", 100, 100)
	labeledHigherVersionNode := test.BuildTestNode("labeledHigherVersionNode", 100, 100)
	labeledLowerVersionNode1 := test.BuildTestNode("labeledLowerVersionNode1", 100, 100)
	labeledLowerVersionNode2 := test.BuildTestNode("labeledLowerVersionNode2", 100, 100)

	notLabeledHigherVersionNode.Status.NodeInfo.KubeletVersion = "1.26.0"
	labeledHigherVersionNode.Status.NodeInfo.KubeletVersion = "1.26.0"
	notLabeledLowerVersionNode.Status.NodeInfo.KubeletVersion = "1.24.0"
	labeledLowerVersionNode1.Status.NodeInfo.KubeletVersion = "1.24.0"
	labeledLowerVersionNode2.Status.NodeInfo.KubeletVersion = "1.24.0"

	labeledHigherVersionNode.Labels = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "1000m",
	}
	labeledLowerVersionNode1.Labels = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "1000m",
	}
	labeledLowerVersionNode2.Labels = map[string]string{
		gkelabels.ExtendedDurationPodsLabel: "1000m",
	}

	testCases := map[string]struct {
		candidateNodes []*apiv1.Node
		expectedNodes  []*apiv1.Node
	}{
		"labeled node with lower version": {
			candidateNodes: []*apiv1.Node{labeledLowerVersionNode1},
			expectedNodes:  []*apiv1.Node{},
		},
		"labeled node with higher version": {
			candidateNodes: []*apiv1.Node{labeledHigherVersionNode},
			expectedNodes:  []*apiv1.Node{labeledHigherVersionNode},
		},
		"not labeled node with lower version": {
			candidateNodes: []*apiv1.Node{notLabeledLowerVersionNode},
			expectedNodes:  []*apiv1.Node{notLabeledLowerVersionNode},
		},
		"not labeled node with higher version": {
			candidateNodes: []*apiv1.Node{notLabeledHigherVersionNode},
			expectedNodes:  []*apiv1.Node{notLabeledHigherVersionNode},
		},
		"Some nodes have both the label and a lower node version": {
			candidateNodes: []*apiv1.Node{notLabeledHigherVersionNode, labeledLowerVersionNode1, labeledHigherVersionNode, notLabeledLowerVersionNode, labeledLowerVersionNode2},
			expectedNodes:  []*apiv1.Node{notLabeledHigherVersionNode, labeledHigherVersionNode, notLabeledLowerVersionNode},
		},
	}
	cp := &gke.GkeCloudProviderMock{}
	cp.On("GetClusterVersion").Return("1.25.0")
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			processor := NewScaleDownProcessor()
			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			ctx := context.AutoscalingContext{
				ClusterSnapshot: snapshot,
				CloudProvider:   cp,
			}
			err := snapshot.SetClusterState(testCase.candidateNodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot())
			assert.NoError(t, err)
			nodes, err := processor.GetPodDestinationCandidates(&ctx, testCase.candidateNodes)
			assert.NoError(t, err)
			assert.EqualValues(t, testCase.expectedNodes, nodes)
		})
	}
}
