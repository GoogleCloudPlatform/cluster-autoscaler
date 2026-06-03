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
	"k8s.io/autoscaler/cluster-autoscaler/utils/annotations"

	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/testsnapshot"
	csisnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/csi/snapshot"
	drasnapshot "k8s.io/autoscaler/cluster-autoscaler/simulator/dynamicresources/snapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
)

// TestEligibleNodes checks in the integration of the pipeline
func TestEligibleNodes(t *testing.T) {
	n1 := setVersion(setLabel(test.BuildTestNode("node1", 100, 0), labels.ExtendedDurationPodsLabel), "1.24.0")
	n2 := setVersion(setLabel(test.BuildTestNode("node2", 100, 0), labels.ExtendedDurationPodsLabel), "1.24.1")
	n3 := setVersion(test.BuildTestNode("node3", 100, 0), "1.24.0")
	n4 := setVersion(test.BuildTestNode("node4", 100, 0), "1.24.1")

	testCases := map[string]struct {
		nodes            []*v1.Node
		cloudProviderNil bool
		clusterVersion   string
		expected         int
	}{
		"nil cloud provider": {
			nodes:            nil,
			cloudProviderNil: true,
			expected:         0,
		},
		"no nodes in the snapshot": {
			nodes:    nil,
			expected: 0,
		},
		"no nodes on lower version": {
			nodes:          []*v1.Node{n2, n4},
			expected:       0,
			clusterVersion: "1.24.1",
		},
		"no extended duration nodes": {
			nodes:          []*v1.Node{n3, n4},
			expected:       0,
			clusterVersion: "1.24.1",
		},
		"some eligible node": {
			nodes:          []*v1.Node{n1, n2, n3, n4},
			expected:       1,
			clusterVersion: "1.24.1",
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {

			var cp *gke.GkeCloudProviderMock
			if !tc.cloudProviderNil {
				cp = &gke.GkeCloudProviderMock{}
			}

			snapshot := testsnapshot.NewTestSnapshotOrDie(t)
			ctx := &context.AutoscalingContext{
				ClusterSnapshot: snapshot,
				CloudProvider:   cp,
			}
			if !tc.cloudProviderNil {
				cp.On("GetClusterVersion").Return(tc.clusterVersion)
			}
			if tc.nodes != nil {
				err := snapshot.SetClusterState(tc.nodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot())
				assert.NoError(t, err)
			}
			actual := UpgradeEligibleEdpNodes(ctx)
			assert.Equal(t, len(actual), tc.expected)
		})
	}
}

func TestIsNodeEligibleForUpgrade(t *testing.T) {

	testCases := map[string]struct {
		node           *v1.Node
		clusterVersion string
		kubeletVersion string
		expected       bool
	}{
		"nil node should not be eligible": {
			node:           nil,
			clusterVersion: "1.24.0",
			expected:       false,
		},
		"no edp label set": {
			node:           test.BuildTestNode("node1", 100, 0),
			clusterVersion: "1.24.0",
			kubeletVersion: "1.23.0",
			expected:       false,
		},
		"un real node should not be eligible": {
			node:           setAnnotation(setLabel(test.BuildTestNode("node1", 100, 0), labels.ExtendedDurationPodsLabel), annotations.NodeUpcomingAnnotation),
			clusterVersion: "1.24.0",
			kubeletVersion: "1.23.0",
			expected:       false,
		},
		"in-correct cluster version": {
			node:           setLabel(test.BuildTestNode("node1", 100, 0), labels.ExtendedDurationPodsLabel),
			clusterVersion: "1.240-abc",
			expected:       false,
		},
		"in-correct node version": {
			node:           setLabel(test.BuildTestNode("node1", 100, 0), labels.ExtendedDurationPodsLabel),
			clusterVersion: "1.24.0",
			kubeletVersion: "123-abe",
			expected:       false,
		},
		"cluster and node version is same": {
			node:           setLabel(test.BuildTestNode("node1", 100, 0), labels.ExtendedDurationPodsLabel),
			clusterVersion: "1.24.1",
			kubeletVersion: "1.24.1",
			expected:       false,
		},
		"cluster is smaller than node version": { // in practice this case shouldn't be possible
			node:           setLabel(test.BuildTestNode("node1", 100, 0), labels.ExtendedDurationPodsLabel),
			clusterVersion: "1.24.0",
			kubeletVersion: "1.24.1",
			expected:       false,
		},
		"cluster is greater than node version": {
			node:           setLabel(test.BuildTestNode("node1", 100, 0), labels.ExtendedDurationPodsLabel),
			clusterVersion: "1.24.1",
			kubeletVersion: "1.24.0",
			expected:       true,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			nodeInfo := framework.NewTestNodeInfo(nil)
			if tc.node != nil {
				setVersion(tc.node, tc.kubeletVersion)
				nodeInfo.SetNode(tc.node)
			}
			isEligible := IsNodeEligibleForUpgrade(nodeInfo, tc.clusterVersion)
			assert.Equal(t, tc.expected, isEligible)
		})
	}
}

func setAnnotation(node *v1.Node, annotationKey string) *v1.Node {
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[annotationKey] = ""
	return node
}

func setLabel(node *v1.Node, labelKey string) *v1.Node {
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Labels[labelKey] = ""
	return node
}

func setVersion(node *v1.Node, version string) *v1.Node {
	node.Status.NodeInfo.KubeletVersion = version
	return node
}
