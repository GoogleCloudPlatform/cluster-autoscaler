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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gkeclient"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/labels"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/ekvms/size"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodeinfosprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/testsnapshot"
	csisnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/csi/snapshot"
	drasnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/snapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestShortLivedUpgradeNodeInfoProvider(t *testing.T) {
	const nodeGroupName = "ng"
	node := readyTestNode(test.BuildTestNode("tn", 1000, 200*size.MiB))
	unreadyNode := unreadyTestNode(test.BuildTestNode("tn", 1000, 200*size.MiB))
	migTemplateNode := readyTestNode(test.BuildTestNode("tn", 127, 127*size.MiB))
	if migTemplateNode.Annotations == nil {
		migTemplateNode.Annotations = map[string]string{}
	}
	migTemplateNode.Annotations[labels.NodeGeneratedFromTemplateAnnotation] = "true"

	testCases := []struct {
		name                        string
		node                        *v1.Node
		queuedProvisioning          bool
		flexStart                   bool
		shortLivedUpgradeInProgress bool
		wantFakeNodeInfo            bool
	}{
		{
			name:                        "non-QueuedProvisioning && non-FlexStart MIG - unchanged real nodeInfo",
			node:                        node,
			queuedProvisioning:          false,
			flexStart:                   false,
			shortLivedUpgradeInProgress: false,
			wantFakeNodeInfo:            false,
		},
		{
			name:                        "QueuedProvisioning MIG, no upgrade in progress - unchanged real nodeInfo",
			node:                        node,
			queuedProvisioning:          true,
			shortLivedUpgradeInProgress: false,
			wantFakeNodeInfo:            false,
		},
		{
			name:                        "QueuedProvisioning MIG, upgrade in progress, real nodeInfo - overwrite nodeInfo with fake one",
			node:                        node,
			queuedProvisioning:          true,
			shortLivedUpgradeInProgress: true,
			wantFakeNodeInfo:            true,
		},
		{
			name:                        "QueuedProvisioning MIG, upgrade in progress, unready node, so fake nodeInfo - unchanged fake nodeInfo",
			node:                        unreadyNode,
			queuedProvisioning:          true,
			shortLivedUpgradeInProgress: true,
			wantFakeNodeInfo:            true,
		},
		{
			name:                        "FlexStart Non-Queued MIG, no upgrade in progress - unchanged real nodeInfo",
			node:                        node,
			flexStart:                   true,
			queuedProvisioning:          false,
			shortLivedUpgradeInProgress: false,
			wantFakeNodeInfo:            false,
		},
		{
			name:                        "FlexStart Non-Queued MIG, upgrade in progress, real nodeInfo - overwrite nodeInfo with fake one",
			node:                        node,
			flexStart:                   true,
			queuedProvisioning:          false,
			shortLivedUpgradeInProgress: true,
			wantFakeNodeInfo:            true,
		},
		{
			name:                        "FlexStart Non-Queued MIG, upgrade in progress, unready node, so fake nodeInfo - unchanged fake nodeInfo",
			node:                        unreadyNode,
			flexStart:                   true,
			queuedProvisioning:          false,
			shortLivedUpgradeInProgress: true,
			wantFakeNodeInfo:            true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defaultProvider := nodeinfosprovider.NewMixedTemplateNodeInfoProvider(nil, false)

			nodes := []*v1.Node{tc.node}
			gkeManager := &gke.GkeManagerMock{}
			cloudProvider := &gke.GkeCloudProviderMock{}
			mig := gke.NewTestGkeMigBuilder().
				SetGceRefName(nodeGroupName).
				SetGkeManager(gkeManager).
				SetNodePoolName(nodeGroupName).
				SetQueuedProvisioning(tc.queuedProvisioning).
				SetSpec(&gkeclient.NodePoolSpec{FlexStart: tc.flexStart}).
				SetShortLivedUpgradeInProgress(tc.shortLivedUpgradeInProgress).
				Build()

			cloudProvider.On("NodeGroups").Return([]cloudprovider.NodeGroup{mig})
			cloudProvider.On("NodeGroupForNode", mock.AnythingOfType("*v1.Node")).Return(mig, nil)

			podLister := kube_util.NewTestPodLister([]*v1.Pod{})
			registry := kube_util.NewListerRegistry(nil, nil, podLister, nil, nil, nil, nil, nil, nil)
			ctx := ca_context.AutoscalingContext{
				CloudProvider: cloudProvider,
				AutoscalingKubeClients: ca_context.AutoscalingKubeClients{
					ListerRegistry: registry,
				},
				ClusterSnapshot: testsnapshot.NewTestSnapshotOrDie(t),
			}
			assert.NoError(t, ctx.ClusterSnapshot.SetClusterState(context.TODO(), nodes, nil, drasnapshot.NewEmptySnapshot(), csisnapshot.NewEmptySnapshot()))
			provReqNodeInfoProvider := NewShortLivedUpgradeNodeInfoProvider(defaultProvider)

			var wantNode *v1.Node
			if tc.wantFakeNodeInfo {
				wantNode = migTemplateNode
				gkeManager.On("GetMigTemplateNodeInfo", mock.AnythingOfType("*gke.GkeMig")).Return(framework.NewTestNodeInfo(migTemplateNode), nil).Once()
			} else {
				wantNode = tc.node
			}

			nodeInfos, err := provReqNodeInfoProvider.Process(context.TODO(), &ctx, nodes, []*appsv1.DaemonSet{}, taints.TaintConfig{}, time.Now())
			assert.NoError(t, err)
			mock.AssertExpectationsForObjects(t, gkeManager)
			assert.Len(t, nodeInfos, 1)

			url := gce.GenerateMigUrl("", mig.GceRef())
			assert.NotNil(t, nodeInfos[url].Node())
			assert.Equal(t, !tc.wantFakeNodeInfo, isNodeInfoReal(nodeInfos[url]))

			gotNodeInfo := nodeInfos[url]
			gotNode := gotNodeInfo.Node()

			wantNode.Name = ""
			wantNode.UID = ""
			if wantNode.Labels == nil {
				wantNode.Labels = make(map[string]string)
			}
			delete(wantNode.Labels, v1.LabelHostname)
			wantNode.Labels["cluster-autoscaler.kubernetes.io/template-node"] = "true"
			gotNode.Name = ""
			gotNode.UID = ""
			delete(gotNode.Labels, v1.LabelHostname)
			assert.Equal(t, wantNode, gotNode)
		})
	}
}

func unreadyTestNode(node *v1.Node) *v1.Node {
	test.SetNodeReadyState(node, false, time.Now().Add(-2*time.Minute))
	return node
}

func readyTestNode(node *v1.Node) *v1.Node {
	test.SetNodeReadyState(node, true, time.Now().Add(-2*time.Minute))
	return node
}
