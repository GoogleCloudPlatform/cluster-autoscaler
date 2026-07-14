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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

func TestNewUpgradeNodeTaintingProcessor(t *testing.T) {
	n1 := framework.NewTestNodeInfo(setTaint(test.BuildTestNode("node1", 100, 0), notTargetGkeVersionTaint))
	n2 := framework.NewTestNodeInfo(setTaint(test.BuildTestNode("node2", 100, 0), notTargetGkeVersionTaint))
	n3 := framework.NewTestNodeInfo(test.BuildTestNode("node3", 100, 0))
	n4 := framework.NewTestNodeInfo(test.BuildTestNode("node4", 100, 0))

	testCases := map[string]struct {
		nodes    []*framework.NodeInfo
		maxCount int
		expected int
	}{
		"No nodes": {
			nodes:    nil,
			maxCount: 1,
			expected: 0,
		},
		"all node with taint already existing": {
			nodes:    []*framework.NodeInfo{n1, n2},
			maxCount: 1,
			expected: 0,
		},
		"eligible nodes less than max count": {
			nodes:    []*framework.NodeInfo{n1, n2, n3},
			maxCount: 2,
			expected: 1,
		},
		"eligible nodes more than max count": {
			nodes:    []*framework.NodeInfo{n1, n2, n3, n4},
			maxCount: 1,
			expected: 1,
		},
	}
	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			processor := NewUpgradeNodeTaintingProcessor(tc.maxCount)
			actual := processor.getFilteredNodes(tc.nodes)
			assert.Equal(t, tc.expected, len(actual))
		})
	}
}

func TestUpgradeNodeTaintingProcessor_Process(t *testing.T) {
	n1 := setVersion(setLabel(test.BuildTestNode("node1", 100, 0), labels.ExtendedDurationPodsLabel), "1.24.0")
	n2 := setVersion(setLabel(test.BuildTestNode("node2", 100, 0), labels.ExtendedDurationPodsLabel), "1.24.1")

	fakeClient := buildFakeClient(t, n2, n1)

	snapshot := testsnapshot.NewTestSnapshotOrDie(t)
	cp := &gke.GkeCloudProviderMock{}
	ctx := &ca_context.AutoscalingContext{
		ClusterSnapshot: snapshot,
		CloudProvider:   cp,
		AutoscalingKubeClients: ca_context.AutoscalingKubeClients{
			ClientSet: fakeClient,
		},
	}

	assert.NoError(t, snapshot.SetClusterState([]*v1.Node{n1, n2}, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))
	cp.On("GetClusterVersion").Return("1.24.1")
	edpProcessor := NewUpgradeNodeTaintingProcessor(10)
	err := edpProcessor.Process(ctx, nil, time.Now())
	assert.NoError(t, err)

	assert.Equal(t, 1, len(getNode(t, fakeClient, "node1").Spec.Taints))
	assert.Equal(t, *notTargetGkeVersionTaint, getNode(t, fakeClient, "node1").Spec.Taints[0])
	assert.Equal(t, 0, len(getNode(t, fakeClient, "node2").Spec.Taints))

}

func setTaint(node *v1.Node, taint *v1.Taint) *v1.Node {
	node.Spec.Taints = append(node.Spec.Taints, *taint)
	return node
}

func buildFakeClient(t *testing.T, nodes ...*v1.Node) *fake.Clientset {
	t.Helper()
	fakeClient := fake.NewSimpleClientset()

	for _, node := range nodes {
		_, err := fakeClient.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
		assert.NoError(t, err)
	}

	return fakeClient
}

func getNode(t *testing.T, client kube_client.Interface, name string) *v1.Node {
	t.Helper()
	node, err := client.CoreV1().Nodes().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to retrieve node %v: %v", name, err)
	}
	return node
}
