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

package initialization

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/gce"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/utils"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	nodegroupchange "k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupconfig"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroups/asyncnodegroups"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke"
	resizerequestclient "k8s.io/gke-autoscaling/cluster-autoscaler/pkg/cloudprovider/gke/gceclient/resizerequest"
)

func TestRecoverPendingScaleUps(t *testing.T) {
	now := time.Date(2023, 6, 30, 12, 33, 4, 20, time.UTC)
	tests := []struct {
		name                  string
		nodeProvisionDuration time.Duration
		migTemplates          []migTemplate
		wantErr               bool
	}{
		{
			name: "simple queued node pool",
			migTemplates: []migTemplate{
				{
					name:               "np",
					queuedProvisioning: true,
					resizeRequests: []resizerequestclient.ResizeRequestStatus{
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateAccepted,
							CreationTime: now.Add(-10 * time.Minute),
						},
					},
					targetSize:      12,
					existingNodes:   2,
					wantIsScalingUp: true,
				},
			},
		},
		{
			name: "multiple resize requests",
			migTemplates: []migTemplate{
				{
					name:               "np",
					queuedProvisioning: true,
					resizeRequests: []resizerequestclient.ResizeRequestStatus{
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateAccepted,
							CreationTime: now.Add(-10 * time.Minute),
						},
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateProvisioning,
							CreationTime: now.Add(-10 * time.Minute),
						},
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateSucceeded,
							CreationTime: now.Add(-10 * time.Minute),
						},
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateSucceeded,
							CreationTime: now.Add(-10 * time.Minute),
						},
					},
					targetSize:      22,
					existingNodes:   2,
					wantIsScalingUp: true,
				},
			},
		},
		{
			name: "no accepted resize requests",
			migTemplates: []migTemplate{
				{
					name:               "np",
					queuedProvisioning: true,
					resizeRequests: []resizerequestclient.ResizeRequestStatus{
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateProvisioning,
							CreationTime: now.Add(-10 * time.Minute),
						},
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateSucceeded,
							CreationTime: now.Add(-10 * time.Minute),
						},
					},
					targetSize:    2,
					existingNodes: 2,
				},
			},
		},
		{
			name: "multiple node pools",
			migTemplates: []migTemplate{
				{
					name:               "np-queued",
					queuedProvisioning: true,
					resizeRequests: []resizerequestclient.ResizeRequestStatus{
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateAccepted,
							CreationTime: now.Add(-10 * time.Minute),
						},
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateSucceeded,
							CreationTime: now.Add(-10 * time.Minute),
						},
					},
					targetSize:      12,
					existingNodes:   2,
					wantIsScalingUp: true,
				},
				{
					name: "np-not-queued1",
					resizeRequests: []resizerequestclient.ResizeRequestStatus{
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateProvisioning,
							CreationTime: now.Add(-10 * time.Minute),
						},
					},
					targetSize:    2,
					existingNodes: 2,
				},
				{
					name: "np-not-queued2",
					resizeRequests: []resizerequestclient.ResizeRequestStatus{
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateProvisioning,
							CreationTime: now.Add(-10 * time.Minute),
						},
					},
					targetSize:    2,
					existingNodes: 2,
				},
			},
		},
		{
			name: "listing resize requests fails",
			migTemplates: []migTemplate{
				{
					name:                "np-failed",
					queuedProvisioning:  true,
					resizeRequestsError: errors.New("test-error"),
					targetSize:          2,
					existingNodes:       2,
				},
			},
			wantErr: true,
		},
		{
			name: "listing resize requests fails, but another is successful and is marked as such",
			migTemplates: []migTemplate{
				{
					name:                "np-failed1",
					queuedProvisioning:  true,
					resizeRequestsError: errors.New("test-error"),
					targetSize:          2,
					existingNodes:       2,
				},
				{
					name:               "np-queued1",
					queuedProvisioning: true,
					resizeRequests: []resizerequestclient.ResizeRequestStatus{
						{
							ResizeBy:     10,
							State:        resizerequestclient.ResizeRequestStateAccepted,
							CreationTime: now.Add(-10 * time.Minute),
						},
					},
					targetSize:      12,
					existingNodes:   2,
					wantIsScalingUp: true,
				},
				{
					name:                "np-failed2",
					queuedProvisioning:  true,
					resizeRequestsError: errors.New("test-error"),
					targetSize:          2,
					existingNodes:       2,
				},
				{
					name:               "np-queued",
					queuedProvisioning: true,
					resizeRequests: []resizerequestclient.ResizeRequestStatus{
						{
							ResizeBy:     2,
							State:        resizerequestclient.ResizeRequestStateAccepted,
							CreationTime: now.Add(-10 * time.Minute),
						},
					},
					targetSize:      4,
					existingNodes:   2,
					wantIsScalingUp: true,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build mock cluster state registry
			fakeClient := &fake.Clientset{}
			fakeLogRecorder, _ := utils.NewStatusMapRecorder(fakeClient, "kube-system", kube_record.NewFakeRecorder(5), false, "my-cool-configmap")
			provider, gkeMigs, nodes := buildMockMigsAndProvider(tt.migTemplates, now)
			nodeGroupConfigProcessor := nodegroupconfig.NewDefaultNodeGroupConfigProcessor(config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 15 * time.Minute})
			clusterStateRegistry := clusterstate.NewClusterStateRegistry(
				provider,
				fakeLogRecorder,
				backoff.NewIdBasedExponentialBackoff(5*time.Minute, 10*time.Minute, 15*time.Minute),
				nodeGroupConfigProcessor,
				nil, // No templates
				clusterstate.WithAsyncNodeGroupStateChecker(asyncnodegroups.NewDefaultAsyncNodeGroupStateChecker()),
				clusterstate.WithConfig(clusterstate.ClusterStateRegistryConfig{
					MaxTotalUnreadyPercentage: 10,
					OkTotalUnreadyCount:       1,
				}),
				clusterstate.WithScaleStateNotifier(nodegroupchange.NewNodeGroupChangeObserversList()),
			)
			context := &ca_context.AutoscalingContext{
				ClusterStateRegistry: clusterStateRegistry,
			}

			// UpdateNodes in the cluster state registry
			err := clusterStateRegistry.UpdateNodes(nodes, now)
			if err != nil {
				t.Fatalf("clusterStateRegistry.UpdateNodes(%v, nil, %v) returned error: %v", nodes, now, err)
			}

			// Run the Recovery
			gotFunc := RecoverPendingScaleUps(context, provider)
			if gotFunc == nil {
				t.Fatalf("RecoverPendingScaleUps() returned nil")
			}
			gotErr := gotFunc()
			if (gotErr != nil) != tt.wantErr {
				t.Errorf("RecoverPendingScaleUps()() error = %v, wantErr %v", gotErr, tt.wantErr)
			}

			// Compare expected scale-ups
			for i, mig := range gkeMigs {
				wantIsScalingUp := tt.migTemplates[i].wantIsScalingUp
				gotIsScalingUp := clusterStateRegistry.IsNodeGroupScalingUp(mig.Id())
				// TODO(b/290035119): also verify the timestamp of the scale-up
				if gotIsScalingUp != wantIsScalingUp {
					t.Errorf("IsNodeGroupScalingUp(%q) returned %t, but want %t", mig.Id(), gotIsScalingUp, wantIsScalingUp)
				}
			}
		})
	}
}

type migTemplate struct {
	name                string
	queuedProvisioning  bool
	resizeRequests      []resizerequestclient.ResizeRequestStatus
	resizeRequestsError error
	wantIsScalingUp     bool
	targetSize          int
	existingNodes       int
}

func buildMockMigsAndProvider(migTemplates []migTemplate, now time.Time) (*gke.GkeCloudProviderMock, []*gke.GkeMig, []*apiv1.Node) {
	gkeManager := &gke.GkeManagerMock{}
	provider := &gke.GkeCloudProviderMock{}
	gkeMigs := []*gke.GkeMig{}
	nodeGroups := []cloudprovider.NodeGroup{}
	nodes := []*apiv1.Node{}
	for _, mt := range migTemplates {
		// Build mock mig
		mig := gke.NewTestGkeMigBuilder().
			SetGceRefName(mt.name).
			SetGkeManager(gkeManager).
			SetExist(true).
			SetQueuedProvisioning(mt.queuedProvisioning).
			Build()
		gkeMigs = append(gkeMigs, mig)
		nodeGroups = append(nodeGroups, mig)
		gkeManager.On("ResizeRequests", mig).Return(mt.resizeRequests, mt.resizeRequestsError)
		gkeManager.On("GetMigSize", mig).Return(int64(mt.targetSize), nil)
		gkeManager.On("ScaleDownUnreadyTimeOverride", mig).Return(time.Duration(0), false).Maybe()

		// Build mock nodes
		for i := 0; i < mt.existingNodes; i++ {
			node := test.BuildTestNode(fmt.Sprintf("%s-%d", mt.name, i), 1000, 1000)
			test.SetNodeReadyState(node, true, now.Add(-time.Minute))
			nodes = append(nodes, node)
		}

		// Build mock cloud instances
		instances := []gce.GceInstance{}
		for i := 0; i < mt.targetSize; i++ {
			name := fmt.Sprintf("%s-%d", mt.name, i)
			state := cloudprovider.InstanceRunning
			if i >= mt.existingNodes {
				state = cloudprovider.InstanceCreating
			}
			instances = append(instances, gce.GceInstance{
				Instance: cloudprovider.Instance{
					Id: name,
					Status: &cloudprovider.InstanceStatus{
						State: state,
					},
				},
			})
			provider.On("HasInstance", mock.MatchedBy(func(node *apiv1.Node) bool { return node.Name == name })).Return(true, nil)
			provider.On("NodeGroupForNode", mock.MatchedBy(func(node *apiv1.Node) bool { return node.Name == name })).Return(mig, nil)
		}
		gkeManager.On("GetMigNodes", mig).Return(instances, nil)
	}
	provider.On("GetGkeMigs").Return(gkeMigs)
	provider.On("NodeGroups").Return(nodeGroups)

	return provider, gkeMigs, nodes
}
