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

package processors

import (
	"testing"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupset"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"

	"github.com/stretchr/testify/assert"
)

func TestIsGkeNodeInfoSimilar(t *testing.T) {
	n1 := BuildTestNode("node1", 1000, 2000)
	n1.ObjectMeta.Labels["test-label"] = "test-value"
	n1.ObjectMeta.Labels["character"] = "winnie the pooh"
	n2 := BuildTestNode("node2", 1000, 2000)
	n2.ObjectMeta.Labels["test-label"] = "test-value"
	// No node-pool labels.
	checkNodesSimilar(t, n1, n2, IsGkeNodeInfoSimilar, false)
	// Empty node-pool labels
	n1.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = ""
	n2.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = ""
	checkNodesSimilar(t, n1, n2, IsGkeNodeInfoSimilar, false)
	// Only one non empty
	n1.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = ""
	n2.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = "blah"
	checkNodesSimilar(t, n1, n2, IsGkeNodeInfoSimilar, false)
	// Only one present
	delete(n1.ObjectMeta.Labels, "cloud.google.com/gke-nodepool")
	n2.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = "blah"
	checkNodesSimilar(t, n1, n2, IsGkeNodeInfoSimilar, false)
	// Different vales
	n1.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = "blah1"
	n2.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = "blah2"
	checkNodesSimilar(t, n1, n2, IsGkeNodeInfoSimilar, false)
	// Same values
	n1.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = "blah"
	n2.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = "blah"
	checkNodesSimilar(t, n1, n2, IsGkeNodeInfoSimilar, true)
}

func TestFindSimilarNodeGroupsGkeBasic(t *testing.T) {
	processor := &nodegroupset.BalancingNodeGroupSetProcessor{Comparator: IsGkeNodeInfoSimilar}
	context := &context.AutoscalingContext{}

	n1 := BuildTestNode("n1", 1000, 1000)
	n2 := BuildTestNode("n2", 1000, 1000)
	n3 := BuildTestNode("n3", 2000, 2000)
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("ng1", 1, 10, 1)
	provider.AddNodeGroup("ng2", 1, 10, 1)
	provider.AddNodeGroup("ng3", 1, 10, 1)
	provider.AddNode("ng1", n1)
	provider.AddNode("ng2", n2)
	provider.AddNode("ng3", n3)

	ni1 := framework.NewTestNodeInfo(n1)
	ni2 := framework.NewTestNodeInfo(n2)
	ni3 := framework.NewTestNodeInfo(n3)

	nodeInfosForGroups := map[string]*framework.NodeInfo{
		"ng1": ni1, "ng2": ni2, "ng3": ni3,
	}

	ng1, _ := provider.NodeGroupForNode(n1)
	ng2, _ := provider.NodeGroupForNode(n2)
	ng3, _ := provider.NodeGroupForNode(n3)
	context.CloudProvider = provider

	similar, err := processor.FindSimilarNodeGroups(context, ng1, nodeInfosForGroups)
	assert.NoError(t, err)
	assert.Equal(t, similar, []cloudprovider.NodeGroup{ng2})

	similar, err = processor.FindSimilarNodeGroups(context, ng2, nodeInfosForGroups)
	assert.NoError(t, err)
	assert.Equal(t, similar, []cloudprovider.NodeGroup{ng1})

	similar, err = processor.FindSimilarNodeGroups(context, ng3, nodeInfosForGroups)
	assert.NoError(t, err)
	assert.Equal(t, similar, []cloudprovider.NodeGroup{})
}

func TestFindSimilarNodeGroupsGkeByLabel(t *testing.T) {
	processor := &nodegroupset.BalancingNodeGroupSetProcessor{Comparator: IsGkeNodeInfoSimilar}
	context := &context.AutoscalingContext{}

	n1 := BuildTestNode("n1", 1000, 1000)
	n2 := BuildTestNode("n2", 2000, 2000)

	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("ng1", 1, 10, 1)
	provider.AddNodeGroup("ng2", 1, 10, 1)
	provider.AddNode("ng1", n1)
	provider.AddNode("ng2", n2)

	ni1 := framework.NewTestNodeInfo(n1)
	ni2 := framework.NewTestNodeInfo(n2)

	nodeInfosForGroups := map[string]*framework.NodeInfo{
		"ng1": ni1, "ng2": ni2,
	}

	ng1, _ := provider.NodeGroupForNode(n1)
	ng2, _ := provider.NodeGroupForNode(n2)
	context.CloudProvider = provider

	// Groups with different cpu and mem are not similar
	similar, err := processor.FindSimilarNodeGroups(context, ng1, nodeInfosForGroups)
	assert.NoError(t, err)
	assert.Equal(t, similar, []cloudprovider.NodeGroup{})

	// Unless we give them nodepool label
	n1.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = "blah"
	n2.ObjectMeta.Labels["cloud.google.com/gke-nodepool"] = "blah"
	similar, err = processor.FindSimilarNodeGroups(context, ng1, nodeInfosForGroups)
	assert.NoError(t, err)
	assert.Equal(t, similar, []cloudprovider.NodeGroup{ng2})
}

func checkNodesSimilar(t *testing.T, n1, n2 *apiv1.Node, comparator nodegroupset.NodeInfoComparator, shouldEqual bool) {
	checkNodesSimilarWithPods(t, n1, n2, []*apiv1.Pod{}, []*apiv1.Pod{}, comparator, shouldEqual)
}

func checkNodesSimilarWithPods(t *testing.T, n1, n2 *apiv1.Node, pods1, pods2 []*apiv1.Pod, comparator nodegroupset.NodeInfoComparator, shouldEqual bool) {
	ni1 := framework.NewTestNodeInfo(n1, pods1...)
	ni2 := framework.NewTestNodeInfo(n2, pods2...)
	assert.Equal(t, shouldEqual, comparator(ni1, ni2))
}
